"""Writing data files: Arrow in, Parquet out, a `DataFile` record back.

`write_data_files` takes Arrow `RecordBatch`es, aligns their columns to the
table's schema **by name** (falling back to the field id when the batch carries
one), partitions each batch by the spec's transforms, writes one Parquet file
per partition per batch through parquet.mojo's writer, and returns the
`data_file` records a manifest needs — record count, file size, split offsets,
and the per-column sizes, value counts, null counts and Appendix-D bounds that
`InclusiveMetricsEvaluator` prunes on.

The statistics come from the Parquet footer the writer just produced rather
than from a second pass over the values: whatever the writer decided the
min and max were is what the manifest reports, which is the only way the two
can agree. Bounds are truncated the way `write.metadata.metrics.default`'s
`truncate(16)` says — a lower bound is cut, an upper bound is cut and then
incremented so it stays an upper bound, and dropped when it cannot be.
"""

from std.collections import Dict

from parquet import (
    ByteSink,
    ListSink,
    ParquetReader,
    ParquetWriter,
    RecordBatch,
    WriterOptions,
    WrittenFile,
    build_schema,
)
from parquet.arrow import ArrayArena, ArrayData, ArrowType
from parquet_full import AllCodecs
from parquet.stats import (
    SV_BOOL,
    SV_BYTES,
    SV_FLOAT,
    SV_INT,
    SV_NONE,
    SV_UINT,
    ScalarValue,
)
from parquet.stats import decode_stats
from thrift import CompressionCodec, FileMetaData, read_footer

from .expressions import ColumnMetrics
from .io import FileIO, join_path
from .kernels import (
    arrow_type_for,
    cast_array,
    constant_array,
    filter_array,
)
from .nested import (
    ColumnTree,
    cast_tree,
    default_tree,
    filter_tree,
    find_struct_path,
    flatten_leaf,
    subtree_copy,
    tree_matches,
)
from .manifest import CONTENT_DATA, DataFile
from .read import _extract
from .schema import Schema
from .transforms import (
    PartitionSpec,
    T_DAY,
    T_HOUR,
    T_MONTH,
    T_VOID,
    T_YEAR,
)
from .types import (
    NestedField,
    P_BINARY,
    P_BOOLEAN,
    P_DATE,
    P_DECIMAL,
    P_DOUBLE,
    P_FIXED,
    P_FLOAT,
    P_INT,
    P_LONG,
    P_STRING,
    P_TIME,
    P_TIMESTAMP,
    P_TIMESTAMPTZ,
    P_TIMESTAMPTZ_NS,
    P_TIMESTAMP_NS,
    P_UUID,
    TK_PRIMITIVE,
)
from .util import uuid4, zero_pad
from .values import (
    Datum,
    civil_from_days,
    datum_to_bytes,
    floor_div,
    int64_to_be_twos,
)


comptime DEFAULT_METRICS_TRUNCATE = 16
comptime HIVE_NULL = String("__HIVE_DEFAULT_PARTITION__")

comptime PROP_COMPRESSION = String("write.parquet.compression-codec")
comptime PROP_COMPRESSION_LEVEL = String("write.parquet.compression-level")
"""Iceberg's name for the codec level. Honoured for ZSTD (1 to 22); the other
codecs have no level here and ignore it."""
comptime PROP_DICT_SIZE = String("write.parquet.dict-size-bytes")
"""Iceberg's dictionary-page cap. parquet.mojo sizes its dictionary by value
count rather than bytes, so only the one answer that means the same thing in
both is honoured: `0` switches dictionary encoding off, anything else leaves it
on, as the writer decides per column chunk whether a dictionary pays."""
comptime PROP_PAGE_SIZE = String("write.parquet.page-size-bytes")
comptime PROP_ROW_GROUP_ROWS = String("write.parquet.row-group-size-rows")
"""Not an Iceberg property.

Iceberg spells the row-group target in *bytes*
(`write.parquet.row-group-size-bytes`), and parquet.mojo's writer splits a
batch by a *row count*. Converting between them means guessing a compression
ratio before compressing, which would make the setting mean something
different from what it says, so the byte property is ignored and this one —
under a name no reader will mistake for the spec's — is honoured instead."""


def codec_value(name: String) raises -> Int32:
    var n = name.lower()
    if n == "zstd":
        return CompressionCodec.ZSTD.value
    if n == "snappy":
        return CompressionCodec.SNAPPY.value
    if n == "gzip":
        return CompressionCodec.GZIP.value
    if n == "lz4":
        return CompressionCodec.LZ4_RAW.value
    if n == "uncompressed" or n == "none":
        return CompressionCodec.UNCOMPRESSED.value
    raise Error(
        "iceberg: '"
        + name
        + "' is not a Parquet codec this build can write (zstd, snappy, gzip,"
        " lz4, uncompressed)"
    )


@fieldwise_init
struct WriteOptions(Copyable, Defaultable, Movable):
    """How to write data files."""

    var codec: String
    var row_group_rows: Int
    """Rows per row group, which is what parquet.mojo's writer splits on."""
    var page_size: Int
    """Target *uncompressed* bytes of values per data page."""
    var compression_level: Int
    """The codec's level: `0` is the codec's own default. ZSTD takes 1 to 22
    and is the only codec that reads it. A higher level writes a smaller file
    more slowly and changes nothing for readers."""
    var use_dictionary: Bool
    """Whether column chunks may be dictionary-encoded. On, the writer still
    falls back to plain for any chunk where a dictionary would not pay."""
    var truncate: Int
    """`write.metadata.metrics.default = truncate(N)`; 0 disables truncation."""
    var num_workers: Int
    """Threads that encode each data file's pages (parquet.mojo's
    `WriterOptions.num_workers`): 1, the default, encodes on the calling
    thread, 0 on every core. The files are the same either way."""

    def __init__(out self):
        self.codec = String("zstd")
        self.row_group_rows = 1024 * 1024
        self.page_size = 1024 * 1024
        self.compression_level = 0
        self.use_dictionary = True
        self.truncate = DEFAULT_METRICS_TRUNCATE
        self.num_workers = 1

    @staticmethod
    def from_properties(properties: Dict[String, String]) raises -> Self:
        var o = Self()
        if PROP_COMPRESSION in properties:
            o.codec = properties[PROP_COMPRESSION]
        if PROP_ROW_GROUP_ROWS in properties:
            o.row_group_rows = Int(properties[PROP_ROW_GROUP_ROWS])
        if PROP_PAGE_SIZE in properties:
            o.page_size = Int(properties[PROP_PAGE_SIZE])
        if PROP_COMPRESSION_LEVEL in properties:
            o.compression_level = Int(properties[PROP_COMPRESSION_LEVEL])
        if PROP_DICT_SIZE in properties:
            o.use_dictionary = Int(properties[PROP_DICT_SIZE]) != 0
        return o^


# ── bound truncation ────────────────────────────────────────────────────────
def truncate_lower(raw: List[UInt8], kind: UInt8, n: Int) -> List[UInt8]:
    """A lower bound cut to `n` units — code points for a string, bytes
    otherwise. Cutting a lower bound can only lower it, which is safe."""
    if n <= 0:
        return raw.copy()
    var limit = _unit_cut(raw, kind, n)
    if limit >= len(raw):
        return raw.copy()
    var out = List[UInt8](capacity=limit)
    for k in range(limit):
        out.append(raw[k])
    return out^


def truncate_upper(
    raw: List[UInt8], kind: UInt8, n: Int
) raises -> Tuple[List[UInt8], Bool]:
    """An upper bound cut to `n` units and then incremented.

    Truncation lowers a value, so an upper bound has to be nudged back up or
    it stops bounding. For a string that means incrementing the last code
    point that is not the maximum; for binary, the last byte that is not
    `0xFF`. When every unit is at its maximum the bound cannot be truncated at
    all and the second element of the result is `False` — the spec's answer is
    to omit it.
    """
    if n <= 0:
        return (raw.copy(), True)
    var limit = _unit_cut(raw, kind, n)
    if limit >= len(raw):
        return (raw.copy(), True)
    var cut = List[UInt8](capacity=limit)
    for k in range(limit):
        cut.append(raw[k])
    if kind == P_STRING:
        return _increment_utf8(cut)
    for k in range(len(cut) - 1, -1, -1):
        if cut[k] != 0xFF:
            cut[k] += 1
            var out = List[UInt8](capacity=k + 1)
            for j in range(k + 1):
                out.append(cut[j])
            return (out^, True)
    return (List[UInt8](), False)


def _unit_cut(raw: List[UInt8], kind: UInt8, n: Int) -> Int:
    """The byte length of the first `n` units of `raw`."""
    if kind != P_STRING:
        return n
    var seen = 0
    var k = 0
    while k < len(raw):
        if seen == n:
            return k
        var c = raw[k]
        var width = 1
        if c >= 0xF0:
            width = 4
        elif c >= 0xE0:
            width = 3
        elif c >= 0xC0:
            width = 2
        k += width
        seen += 1
    return len(raw)


def _increment_utf8(cut: List[UInt8]) raises -> Tuple[List[UInt8], Bool]:
    """Increment the last code point of a UTF-8 string, dropping any that are
    already at the maximum."""
    var starts = List[Int]()
    var k = 0
    while k < len(cut):
        starts.append(k)
        var c = cut[k]
        if c >= 0xF0:
            k += 4
        elif c >= 0xE0:
            k += 3
        elif c >= 0xC0:
            k += 2
        else:
            k += 1
    for s in range(len(starts) - 1, -1, -1):
        var start = starts[s]
        var end = starts[s + 1] if s + 1 < len(starts) else len(cut)
        var cp = _decode_cp(cut, start, end)
        if cp < 0x10FFFF and not (cp >= 0xD7FF and cp < 0xE000):
            var next = cp + 1
            if next == 0xD800:
                next = 0xE000
            var out = List[UInt8](capacity=start + 4)
            for j in range(start):
                out.append(cut[j])
            _encode_cp(out, next)
            return (out^, True)
    return (List[UInt8](), False)


def _decode_cp(b: List[UInt8], start: Int, end: Int) -> Int:
    var c = Int(b[start])
    if c < 0x80:
        return c
    var cp: Int
    var n: Int
    if c >= 0xF0:
        cp = c & 0x07
        n = 3
    elif c >= 0xE0:
        cp = c & 0x0F
        n = 2
    else:
        cp = c & 0x1F
        n = 1
    for k in range(1, n + 1):
        if start + k >= end:
            break
        cp = (cp << 6) | Int(b[start + k] & 0x3F)
    return cp


def _encode_cp(mut out: List[UInt8], cp: Int):
    if cp < 0x80:
        out.append(UInt8(cp))
    elif cp < 0x800:
        out.append(UInt8(0xC0 | (cp >> 6)))
        out.append(UInt8(0x80 | (cp & 0x3F)))
    elif cp < 0x10000:
        out.append(UInt8(0xE0 | (cp >> 12)))
        out.append(UInt8(0x80 | ((cp >> 6) & 0x3F)))
        out.append(UInt8(0x80 | (cp & 0x3F)))
    else:
        out.append(UInt8(0xF0 | (cp >> 18)))
        out.append(UInt8(0x80 | ((cp >> 12) & 0x3F)))
        out.append(UInt8(0x80 | ((cp >> 6) & 0x3F)))
        out.append(UInt8(0x80 | (cp & 0x3F)))


# ── a Parquet statistic as an Appendix-D bound ──────────────────────────────
def bound_bytes(
    v: ScalarValue, kind: UInt8, precision: Int, scale: Int
) raises -> Tuple[List[UInt8], Bool]:
    if v.kind == SV_NONE:
        return (List[UInt8](), False)
    if kind == P_BOOLEAN:
        var out = List[UInt8]()
        out.append(UInt8(1) if v.i != 0 else UInt8(0))
        return (out^, True)
    if kind == P_FLOAT:
        return (datum_to_bytes(Datum.float_(v.f)), True)
    if kind == P_DOUBLE:
        return (datum_to_bytes(Datum.double_(v.f)), True)
    if kind == P_STRING:
        return (v.b.copy(), True)
    if kind == P_BINARY or kind == P_FIXED or kind == P_UUID:
        return (v.b.copy(), True)
    if kind == P_DECIMAL:
        if v.kind == SV_BYTES:
            return (_minimal_be(v.b), True)
        return (int64_to_be_twos(v.i), True)
    if v.kind == SV_BYTES:
        return (v.b.copy(), True)
    if v.kind == SV_FLOAT:
        return (datum_to_bytes(Datum.integral(kind, Int64(v.f))), True)
    return (datum_to_bytes(Datum.integral(kind, v.i)), True)


def _minimal_be(b: List[UInt8]) -> List[UInt8]:
    """Strip redundant sign bytes from a big-endian two's complement value."""
    var start = 0
    while start + 1 < len(b):
        var lead = b[start]
        var next = b[start + 1]
        if lead == 0 and (next & 0x80) == 0:
            start += 1
            continue
        if lead == 0xFF and (next & 0x80) != 0:
            start += 1
            continue
        break
    var out = List[UInt8](capacity=len(b) - start)
    for k in range(start, len(b)):
        out.append(b[k])
    return out^


# ── partition paths ─────────────────────────────────────────────────────────
def human_partition_value(d: Datum, transform_kind: UInt8) raises -> String:
    """The Hive-style rendering of one partition value."""
    if not d.valid:
        return HIVE_NULL
    if transform_kind == T_YEAR:
        return zero_pad(Int(d.i) + 1970, 4)
    if transform_kind == T_MONTH:
        var y = 1970 + Int(floor_div(d.i, 12))
        var m = Int(d.i - floor_div(d.i, 12) * 12) + 1
        return zero_pad(y, 4) + "-" + zero_pad(m, 2)
    if transform_kind == T_DAY:
        var ymd = civil_from_days(d.i)
        return (
            zero_pad(Int(ymd[0]), 4)
            + "-"
            + zero_pad(Int(ymd[1]), 2)
            + "-"
            + zero_pad(Int(ymd[2]), 2)
        )
    if transform_kind == T_HOUR:
        var days = floor_div(d.i, 24)
        var hour = d.i - days * 24
        var ymd = civil_from_days(days)
        return (
            zero_pad(Int(ymd[0]), 4)
            + "-"
            + zero_pad(Int(ymd[1]), 2)
            + "-"
            + zero_pad(Int(ymd[2]), 2)
            + "-"
            + zero_pad(Int(hour), 2)
        )
    if d.kind == P_STRING:
        return d.s
    if d.kind == P_BOOLEAN:
        return String("true") if d.i != 0 else String("false")
    if d.kind == P_DATE:
        var ymd = civil_from_days(d.i)
        return (
            zero_pad(Int(ymd[0]), 4)
            + "-"
            + zero_pad(Int(ymd[1]), 2)
            + "-"
            + zero_pad(Int(ymd[2]), 2)
        )
    if d.kind == P_FLOAT or d.kind == P_DOUBLE:
        return String(d.f)
    if d.kind == P_BINARY or d.kind == P_FIXED or d.kind == P_UUID:
        return _hex(d.b)
    return String(d.i)


comptime _HEXD = String("0123456789abcdef")


def _hex(b: List[UInt8]) -> String:
    var out = String("")
    for k in range(len(b)):
        out += _HEXD[byte=Int(b[k] >> 4)]
        out += _HEXD[byte=Int(b[k] & 0x0F)]
    return out^


def escape_path(s: String) -> String:
    """Percent-escape everything a Hive path may not contain."""
    var out = String("")
    var b = s.as_bytes()
    for k in range(len(b)):
        var c = b[k]
        var ok = (
            (c >= 48 and c <= 57)
            or (c >= 65 and c <= 90)
            or (c >= 97 and c <= 122)
            or c == 95
            or c == 45
            or c == 46
        )
        if ok:
            out += String(StringSlice(unsafe_from_utf8=Span(b)[k : k + 1]))
        else:
            out += "%"
            out += _HEXD[byte=Int(c >> 4)].upper()
            out += _HEXD[byte=Int(c & 0x0F)].upper()
    return out^


def partition_path(spec: PartitionSpec, values: List[Datum]) raises -> String:
    var out = String("")
    for k in range(len(spec.fields)):
        if k > 0:
            out += "/"
        var d = values[k].copy() if k < len(values) else Datum.none()
        out += escape_path(spec.fields[k].name)
        out += "="
        out += escape_path(
            human_partition_value(d, spec.fields[k].transform.kind)
        )
    return out^


# ── aligning a batch to the table's schema ──────────────────────────────────
def _column_for(
    batch: RecordBatch, field: NestedField, name: String
) raises -> Int:
    for c in range(batch.num_columns()):
        if batch.column(c).field_id == Int32(field.id):
            return c
    for c in range(batch.num_columns()):
        if batch.name(c) == name:
            return c
    return -1


def align_batch(batch: RecordBatch, schema: Schema) raises -> List[ColumnTree]:
    """The batch's columns in the table's schema order, at the table's types.

    A nested column is matched child by child **by field id**, so a batch
    whose struct has its fields in another order, under other names, or one
    field short still lines up with the table — the same resolution the read
    path does, run the other way.

    Every column comes back in an arena of its own, so this copies each one
    once. `write_data_files` does not go through here: it uses `_align`,
    which leaves a column that already matches where it is.
    """
    var out = List[ColumnTree]()
    var cols = schema.columns()
    for k in range(len(cols)):
        ref f = cols[k]
        var at = _column_for(batch, f, f.name)
        var arena = ArrayArena()
        var r: Int
        if at < 0:
            r = _missing_column(arena, schema, f, batch.num_rows)
        else:
            r = cast_tree(
                batch.arena,
                batch.roots[at],
                schema.store,
                f.type,
                f.name,
                f.id,
                not f.required,
                arena,
            )
        out.append(ColumnTree(arena^, r))
    return out^


def _missing_column(
    mut arena: ArrayArena, schema: Schema, f: NestedField, rows: Int
) raises -> Int:
    if f.required:
        raise Error(
            "iceberg: the batch has no column for required field '"
            + f.name
            + "'"
        )
    return default_tree(arena, schema.store, f, rows)


struct _Aligned(Movable):
    """A batch lined up with the table, copying only what had to change.

    Column `k` is `roots[k]` in the batch's own arena when `borrowed[k]`, and
    in `own` otherwise: a column whose names, ids and types already match the
    schema (`tree_matches`) is written from where it is, and only a column
    that has to be cast, or is missing and gets its default, is built anew.
    """

    var own: ArrayArena
    var borrowed: List[Bool]
    var roots: List[Int]

    def __init__(out self):
        self.own = ArrayArena()
        self.borrowed = List[Bool]()
        self.roots = List[Int]()

    def __init__(out self, *, deinit move: Self):
        self.own = move.own^
        self.borrowed = move.borrowed^
        self.roots = move.roots^

    def all_borrowed(self) -> Bool:
        for b in self.borrowed:
            if not b:
                return False
        return True

    def none_borrowed(self) -> Bool:
        for b in self.borrowed:
            if b:
                return False
        return True

    def adopt_borrowed(mut self, batch: RecordBatch) raises:
        """Copy the borrowed columns into `own`, so that every column is in
        one arena — what a single Parquet file needs when some columns had
        to be cast and some did not. Only the borrowed columns are copied."""
        for k in range(len(self.roots)):
            if self.borrowed[k]:
                self.roots[k] = subtree_copy(
                    batch.arena, self.roots[k], self.own
                )
                self.borrowed[k] = False


def _align(
    batch: RecordBatch, schema: Schema, borrow: Bool = True
) raises -> _Aligned:
    """`align_batch` without the copies: see `_Aligned`. `borrow=False` casts
    every column, which is the reference the borrowing path is tested
    against."""
    var out = _Aligned()
    var cols = schema.columns()
    for k in range(len(cols)):
        ref f = cols[k]
        var at = _column_for(batch, f, f.name)
        if at < 0:
            out.roots.append(
                _missing_column(out.own, schema, f, batch.num_rows)
            )
            out.borrowed.append(False)
            continue
        var src = batch.roots[at]
        if borrow and tree_matches(
            batch.arena,
            src,
            schema.store,
            f.type,
            f.name,
            f.id,
            not f.required,
        ):
            out.roots.append(src)
            out.borrowed.append(True)
            continue
        out.roots.append(
            cast_tree(
                batch.arena,
                src,
                schema.store,
                f.type,
                f.name,
                f.id,
                not f.required,
                out.own,
            )
        )
        out.borrowed.append(False)
    return out^


# ── writing one Parquet file ────────────────────────────────────────────────
def write_parquet(
    columns: List[ColumnTree], schema: Schema, options: WriteOptions
) raises -> List[UInt8]:
    var arena = ArrayArena()
    var roots = List[Int]()
    for k in range(len(columns)):
        roots.append(subtree_copy(columns[k].arena, columns[k].root, arena))
    return encode_parquet(arena, roots, schema, options)


def encode_parquet(
    arena: ArrayArena, roots: List[Int], schema: Schema, options: WriteOptions
) raises -> List[UInt8]:
    """One Parquet file of the arrays at `roots`, which are already aligned
    to `schema`. The arena may hold other arrays too; only `roots` and what
    they reach are written."""
    var sink = ListSink()
    _ = write_parquet_to(arena, roots, schema, options, sink)
    var out = List[UInt8]()
    swap(out, sink.bytes)
    return out^


def write_parquet_to[
    S: ByteSink
](
    arena: ArrayArena,
    roots: List[Int],
    schema: Schema,
    options: WriteOptions,
    mut sink: S,
) raises -> WrittenFile:
    """`encode_parquet`, handing each row group to `sink` as soon as it is
    encoded, and returning the footer the writer wrote — which is what the
    `data_file` is built from."""
    var opts = WriterOptions()
    opts.codec = codec_value(options.codec)
    opts.row_group_size = options.row_group_rows
    opts.data_page_size = options.page_size
    opts.compression_level = options.compression_level
    opts.use_dictionary = options.use_dictionary
    opts.num_workers = options.num_workers
    var writer = ParquetWriter[AllCodecs](opts^)
    writer.add_metadata("iceberg.schema", schema.to_json())
    writer.write_batch_to(arena, roots, sink)
    return writer^.finish_to(sink)


def data_file_from_parquet(
    data: List[UInt8],
    path: String,
    schema: Schema,
    spec: PartitionSpec,
    partition: List[Datum],
    sort_order_id: Int,
    options: WriteOptions,
) raises -> DataFile:
    """Read back the footer of a file already in memory and turn it into a
    `data_file`. Only the footer is parsed, from `data` where it lies."""
    return data_file_from_footer(
        read_footer(Span(data)),
        len(data),
        path,
        schema,
        spec,
        partition,
        sort_order_id,
        options,
    )


def data_file_from_footer(
    meta: FileMetaData,
    file_size: Int,
    path: String,
    schema: Schema,
    spec: PartitionSpec,
    partition: List[Datum],
    sort_order_id: Int,
    options: WriteOptions,
) raises -> DataFile:
    """A `data_file` from a Parquet footer: the one the writer hands back
    from `finish_to`, so nothing is read back out of the file at all."""
    var leaves = build_schema(meta.schema).leaves.copy()
    var sizes = List[ColumnMetrics]()
    var metrics = List[ColumnMetrics]()
    for leaf in range(len(leaves)):
        ref lc = leaves[leaf]
        var fid = Int(lc.field_id)
        if fid < 0:
            continue
        if not schema.has_field(fid):
            continue
        var af = schema.find_field(fid)
        ref t = schema.store.nodes[af.type]
        var size: Int64 = 0
        var values: Int64 = 0
        var nulls: Int64 = 0
        var has_nulls = False
        var lo = ScalarValue()
        var hi = ScalarValue()
        var have_bounds = False
        for rg in range(len(meta.row_groups)):
            ref chunk = meta.row_groups[rg].columns[leaf]
            if not chunk.meta_data:
                continue
            ref cm = chunk.meta_data.value()
            size += cm.total_compressed_size
            values += cm.num_values
            if not cm.statistics:
                continue
            var st = decode_stats(lc, cm.statistics.value())
            if st.has_null_count:
                nulls += st.null_count
                has_nulls = True
            if st.has_min_max:
                if not have_bounds:
                    lo = st.min.copy()
                    hi = st.max.copy()
                    have_bounds = True
                else:
                    if _scalar_less(st.min, lo):
                        lo = st.min.copy()
                    if _scalar_less(hi, st.max):
                        hi = st.max.copy()
        var cs = ColumnMetrics.blank(fid)
        cs.value_count = size
        cs.has_value_count = True
        sizes.append(cs^)

        var m = ColumnMetrics.blank(fid)
        m.value_count = values
        m.has_value_count = True
        if has_nulls:
            m.null_value_count = nulls
            m.has_null_value_count = True
        if have_bounds:
            var lower = bound_bytes(lo, t.prim, t.precision, t.scale)
            if lower[1]:
                m.lower_bound = truncate_lower(
                    lower[0], t.prim, options.truncate
                )
                m.has_lower = True
            var upper = bound_bytes(hi, t.prim, t.precision, t.scale)
            if upper[1]:
                var cut = truncate_upper(upper[0], t.prim, options.truncate)
                if cut[1]:
                    m.upper_bound = cut[0].copy()
                    m.has_upper = True
        metrics.append(m^)

    return DataFile(
        CONTENT_DATA,
        path,
        String("PARQUET"),
        partition.copy(),
        Int64(meta.num_rows),
        Int64(file_size),
        sizes^,
        metrics^,
        List[UInt8](),
        False,
        _split_offsets(meta),
        List[Int](),
        sort_order_id,
        True,
        0,
        False,
        String(""),
        False,
        0,
        False,
        0,
        False,
    )


def _split_offsets(meta: FileMetaData) -> List[Int64]:
    """Iceberg's `data_file.split_offsets`: where each row group starts —
    what `ParquetReader.split_offsets` reports, from the footer alone."""
    var out = List[Int64](capacity=len(meta.row_groups))
    for rg in meta.row_groups:
        if rg.file_offset:
            out.append(rg.file_offset.value())
        elif len(rg.columns) and rg.columns[0].meta_data:
            ref cm = rg.columns[0].meta_data.value()
            var start = Int64(cm.data_page_offset)
            if cm.dictionary_page_offset:
                var d = cm.dictionary_page_offset.value()
                if d > 0 and d < start:
                    start = d
            out.append(start)
        else:
            out.append(0)
    return out^


def _scalar_less(a: ScalarValue, b: ScalarValue) raises -> Bool:
    if a.kind == SV_BYTES and b.kind == SV_BYTES:
        var n = len(a.b) if len(a.b) < len(b.b) else len(b.b)
        for k in range(n):
            if a.b[k] != b.b[k]:
                return a.b[k] < b.b[k]
        return len(a.b) < len(b.b)
    if a.kind == SV_FLOAT or b.kind == SV_FLOAT:
        return a.f < b.f
    if a.kind == SV_UINT and b.kind == SV_UINT:
        return a.u < b.u
    return a.i < b.i


# ── partitioning and the whole job ──────────────────────────────────────────
def _partition_key(values: List[Datum]) raises -> String:
    var out = String("")
    for k in range(len(values)):
        out += "\x01"
        if not values[k].valid:
            out += "N"
            continue
        out += "V"
        out += _hex(datum_to_bytes(values[k]))
    return out^


def write_data_files(
    io: FileIO,
    location: String,
    batches: List[RecordBatch],
    schema: Schema,
    spec: PartitionSpec,
    sort_order_id: Int,
    options: WriteOptions,
) raises -> List[DataFile]:
    """Write every batch out, one Parquet file per partition per batch."""
    var out = List[DataFile]()
    var cols = schema.columns()
    var counter = 0
    for b in range(len(batches)):
        ref batch = batches[b]
        var aligned = _align(batch, schema)
        var n = batch.num_rows
        if n == 0:
            continue

        # ── which rows go where ────────────────────────────────────────────
        var keys = List[String]()
        var groups = Dict[String, Int]()
        var group_values = List[List[Datum]]()
        var group_keep = List[List[Bool]]()
        var group_count = List[Int]()
        if len(spec.fields) == 0:
            keys.append(String(""))
            groups[String("")] = 0
            group_values.append(List[Datum]())
            group_keep.append(List[Bool](length=n, fill=True))
            group_count.append(n)
        else:
            # A partition source may be a field nested in a struct
            # (`identity(a.b)`, `bucket[4](a.b)`), which the spec allows: the
            # leaf is flattened out of its column once per batch, with the
            # structs above it contributing their nulls.
            var source_arrays = List[ArrayData]()
            var source_types = List[Int]()
            for f in range(len(spec.fields)):
                var src_id = spec.fields[f].source_id
                var top = schema.top_ancestor_id(src_id)
                var slot = -1
                for k in range(len(cols)):
                    if cols[k].id == top:
                        slot = k
                        break
                if slot < 0:
                    raise Error(
                        "iceberg: partition field '"
                        + spec.fields[f].name
                        + "' has no source column in the schema"
                    )
                var path = List[Int]()
                if src_id != top and not find_struct_path(
                    schema.store, cols[slot].type, src_id, path
                ):
                    raise Error(
                        "iceberg: partition field '"
                        + spec.fields[f].name
                        + "' is inside a list or a map, which cannot be"
                        " partitioned on"
                    )
                if aligned.borrowed[slot]:
                    source_arrays.append(
                        flatten_leaf(batch.arena, aligned.roots[slot], path)
                    )
                else:
                    source_arrays.append(
                        flatten_leaf(aligned.own, aligned.roots[slot], path)
                    )
                source_types.append(schema.find_field(src_id).type)
            for r in range(n):
                var values = List[Datum]()
                for f in range(len(spec.fields)):
                    ref t = schema.store.nodes[source_types[f]]
                    var v = _extract(
                        source_arrays[f],
                        r,
                        t.prim,
                        t.precision,
                        t.scale,
                        t.length,
                    )
                    values.append(spec.fields[f].transform.apply(v))
                var key = _partition_key(values)
                if key in groups:
                    var g = groups[key]
                    group_keep[g][r] = True
                    group_count[g] += 1
                else:
                    var g = len(keys)
                    groups[key] = g
                    keys.append(key)
                    group_values.append(values^)
                    group_keep.append(List[Bool](length=n, fill=False))
                    group_keep[g][r] = True
                    group_count.append(1)

        # ── one file per group ─────────────────────────────────────────────
        for g in range(len(keys)):
            var sink = ListSink()
            var written: WrittenFile
            if group_count[g] == n:
                # The whole batch is one file: write the columns from where
                # they are. That is the batch's own arena when every column
                # already matched the schema, and no copy is made at all.
                if not aligned.all_borrowed() and not aligned.none_borrowed():
                    aligned.adopt_borrowed(batch)
                if aligned.all_borrowed():
                    written = write_parquet_to(
                        batch.arena, aligned.roots, schema, options, sink
                    )
                else:
                    written = write_parquet_to(
                        aligned.own, aligned.roots, schema, options, sink
                    )
            else:
                var arena = ArrayArena()
                var roots = List[Int]()
                for k in range(len(aligned.roots)):
                    if aligned.borrowed[k]:
                        roots.append(
                            filter_tree(
                                batch.arena,
                                aligned.roots[k],
                                group_keep[g],
                                group_count[g],
                                arena,
                            )
                        )
                    else:
                        roots.append(
                            filter_tree(
                                aligned.own,
                                aligned.roots[k],
                                group_keep[g],
                                group_count[g],
                                arena,
                            )
                        )
                written = write_parquet_to(arena, roots, schema, options, sink)
            var dir = join_path(location, "data")
            var rel = partition_path(spec, group_values[g])
            if rel != "":
                dir = join_path(dir, rel)
            var name = uuid4() + "-" + zero_pad(counter, 5) + ".parquet"
            counter += 1
            var path = join_path(dir, name)
            io.write_all(path, Span(sink.bytes))
            out.append(
                data_file_from_footer(
                    written.metadata,
                    written.file_size,
                    path,
                    schema,
                    spec,
                    group_values[g],
                    sort_order_id,
                    options,
                )
            )
    return out^
