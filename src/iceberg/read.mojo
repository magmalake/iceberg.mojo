"""Reading rows: `TableScan.to_table()` and `to_batches()`.

This is the half of a scan that `plan_files()` stops short of. For each
`FileScanTask` it opens the data file through the `FileIO`, decodes it with
[parquet.mojo](https://github.com/magmalake/parquet.mojo), resolves every
projected column **by field id**, applies the deletes the planner associated,
evaluates whatever is left of the filter, and appends the surviving rows.

Column projection follows the spec's order exactly. For each projected field
id, in turn:

1. the column with that id in the data file;
2. otherwise, the value from the manifest's partition tuple, when an identity
   partition field is defined on that source column — the "metadata-only Hive
   migration" case;
3. otherwise, the column the table property `schema.name-mapping.default` maps
   that id onto, for files written without field ids at all;
4. otherwise, the field's `initial-default`;
5. otherwise, null.

Schema evolution falls out of that: a renamed column is found by id under its
new name, a column added after the file was written resolves to its default or
null, and a promoted column (`int`→`long`, `float`→`double`, a decimal whose
precision grew) is read at the file's physical width and produced at the
table's current type, because the value, not the encoding, is what is
projected.

Deletes are applied in the order the spec implies. A deletion vector, when one
applies, replaces every position delete file for that data file; otherwise
position delete files are read and their `pos` values collected for this file
path. Equality deletes are matched last, on the columns named by
`equality_ids`, with `null` equal to `null` as the spec requires. The planner
has already scoped every delete by sequence number and partition.

Values are carried as `Datum`, the same tagged value the rest of this library
compares and encodes, which is what makes one code path serve every Iceberg
primitive. `ScanResult.to_batch()` converts a result to parquet.mojo's Arrow
`RecordBatch`, which `export_c` hands to anything speaking the Arrow C Data
Interface.
"""

from std.collections import Dict
from std.memory import bitcast

from parquet import ParquetReader, Predicate, RecordBatch
from parquet.reader import (
    OP_EQ as OP_EQ_PQ,
    OP_GE as OP_GE_PQ,
    OP_GT as OP_GT_PQ,
    OP_LE as OP_LE_PQ,
    OP_LT as OP_LT_PQ,
)
from parquet.stats import SV_NONE, ScalarValue
from parquet.arrow import (
    AT_BINARY,
    AT_BOOL,
    AT_DATE32,
    AT_DECIMAL128,
    AT_FIXED_SIZE_BINARY,
    AT_FLOAT32,
    AT_FLOAT64,
    AT_INT16,
    AT_INT32,
    AT_INT64,
    AT_INT8,
    AT_LARGE_BINARY,
    AT_LARGE_UTF8,
    AT_TIME32,
    AT_TIME64,
    AT_TIMESTAMP,
    AT_UINT16,
    AT_UINT32,
    AT_UINT64,
    AT_UINT8,
    AT_UTF8,
    TU_MICRO,
    TU_NANO,
    ArrayArena,
    ArrayData,
    ArrowType,
    bit_get,
    bit_set,
    load_f32,
    load_f64,
    load_i32,
    load_i64,
)
from parquet.ext_full import AllCodecs

from .expressions import (
    Expr,
    OP_AND,
    OP_EQ,
    OP_FALSE,
    OP_GT,
    OP_GT_EQ,
    OP_IN,
    OP_IS_NAN,
    OP_IS_NULL,
    OP_LT,
    OP_LT_EQ,
    OP_NOT,
    OP_NOT_EQ,
    OP_NOT_IN,
    OP_NOT_NAN,
    OP_NOT_NULL,
    OP_NOT_STARTS_WITH,
    OP_OR,
    OP_STARTS_WITH,
    OP_TRUE,
    bind,
    parse_filter,
)
from .io import FileIO
from .json import Json, json_quote, parse_json, substr
from .manifest import DataFile
from .metadata import TableMetadata
from .puffin import deleted_positions
from .schema import Schema
from .transforms import PartitionSpec, T_IDENTITY
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
    P_UNKNOWN,
    P_UUID,
    TK_PRIMITIVE,
    TypeStore,
)
from .values import (
    Datum,
    compare,
    datum_from_json_prim,
    int64_to_be_twos,
)


# ── reserved ids ────────────────────────────────────────────────────────────
comptime POS_DELETE_FILE_PATH_ID = 2147483546
comptime POS_DELETE_POS_ID = 2147483545

comptime META_FILE = String("_file")
comptime META_POS = String("_pos")
comptime META_SPEC_ID = String("_spec_id")
comptime META_PARTITION = String("_partition")
comptime META_ROW_ID = String("_row_id")
comptime META_LAST_UPDATED = String("_last_updated_sequence_number")

comptime NAME_MAPPING_PROPERTY = String("schema.name-mapping.default")


def is_metadata_column(name: String) -> Bool:
    return (
        name == META_FILE
        or name == META_POS
        or name == META_SPEC_ID
        or name == META_PARTITION
        or name == META_ROW_ID
        or name == META_LAST_UPDATED
    )


# ── a scan's rows ───────────────────────────────────────────────────────────
@fieldwise_init
struct ScanColumn(Copyable, Movable):
    """One output column: its name, its Iceberg type, and its values."""

    var name: String
    var field_id: Int
    var kind: UInt8
    var precision: Int
    var scale: Int
    var length: Int
    var values: List[Datum]


struct ScanResult(Copyable, Defaultable, Movable):
    """The rows a scan returned, column-major."""

    var columns: List[ScanColumn]

    def __init__(out self):
        self.columns = []

    def __init__(out self, var columns: List[ScanColumn]):
        self.columns = columns^

    def num_columns(self) -> Int:
        return len(self.columns)

    def num_rows(self) -> Int:
        if len(self.columns) == 0:
            return 0
        return len(self.columns[0].values)

    def name(self, i: Int) -> String:
        return self.columns[i].name

    def value(self, row: Int, col: Int) -> Datum:
        return self.columns[col].values[row].copy()

    def append(mut self, other: ScanResult) raises:
        """Concatenate another result with the same columns."""
        if len(self.columns) == 0:
            self.columns = other.columns.copy()
            return
        if len(self.columns) != len(other.columns):
            raise Error("iceberg: cannot append results with different shapes")
        for k in range(len(self.columns)):
            for j in range(len(other.columns[k].values)):
                self.columns[k].values.append(other.columns[k].values[j].copy())

    # ── output ─────────────────────────────────────────────────────────────
    def to_csv(self, header: Bool = True) raises -> String:
        var out = String("")
        if header:
            for c in range(len(self.columns)):
                if c > 0:
                    out += ","
                out += _csv_cell(self.columns[c].name)
            out += "\n"
        for r in range(self.num_rows()):
            for c in range(len(self.columns)):
                if c > 0:
                    out += ","
                ref d = self.columns[c].values[r]
                out += "" if not d.valid else _csv_cell(d.repr_())
            out += "\n"
        return out^

    def to_json(self) raises -> String:
        """One JSON object per row, in an array — the Appendix-D encoding."""
        var out = String("[")
        for r in range(self.num_rows()):
            if r > 0:
                out += ","
            out += "{"
            for c in range(len(self.columns)):
                if c > 0:
                    out += ","
                out += json_quote(self.columns[c].name) + ":"
                out += self.columns[c].values[r].to_json()
            out += "}"
        out += "]"
        return out^

    def to_batch(self) raises -> RecordBatch:
        """The same rows as an Arrow `RecordBatch`, ready for `export_c`."""
        var batch = RecordBatch()
        var n = self.num_rows()
        batch.num_rows = n
        for c in range(len(self.columns)):
            ref col = self.columns[c]
            var node = _build_array(col, n)
            batch.roots.append(batch.arena.add(node^))
        return batch^


def _csv_cell(s: String) -> String:
    var needs = (
        s.find(",") >= 0
        or s.find('"') >= 0
        or s.find("\n") >= 0
        or s.find("\r") >= 0
    )
    if not needs:
        return s
    var out = String('"')
    var b = s.as_bytes()
    for k in range(len(b)):
        if b[k] == 34:
            out += '""'
        else:
            out += String(StringSlice(unsafe_from_utf8=Span(b)[k : k + 1]))
    out += '"'
    return out^


# ── Iceberg type -> Arrow type ──────────────────────────────────────────────
def arrow_type_of(
    kind: UInt8, precision: Int, scale: Int, length: Int
) raises -> ArrowType:
    var t: ArrowType
    if kind == P_BOOLEAN:
        t = ArrowType(AT_BOOL)
    elif kind == P_INT:
        t = ArrowType(AT_INT32)
    elif kind == P_FLOAT:
        t = ArrowType(AT_FLOAT32)
    elif kind == P_DOUBLE:
        t = ArrowType(AT_FLOAT64)
    elif kind == P_DATE:
        t = ArrowType(AT_DATE32)
    elif kind == P_TIME:
        t = ArrowType(AT_TIME64)
        t.unit = TU_MICRO
    elif kind == P_TIMESTAMP or kind == P_TIMESTAMPTZ:
        t = ArrowType(AT_TIMESTAMP)
        t.unit = TU_MICRO
        if kind == P_TIMESTAMPTZ:
            t.tz = String("UTC")
    elif kind == P_TIMESTAMP_NS or kind == P_TIMESTAMPTZ_NS:
        t = ArrowType(AT_TIMESTAMP)
        t.unit = TU_NANO
        if kind == P_TIMESTAMPTZ_NS:
            t.tz = String("UTC")
    elif kind == P_STRING:
        t = ArrowType(AT_UTF8)
    elif kind == P_UUID:
        t = ArrowType(AT_FIXED_SIZE_BINARY)
        t.byte_width = 16
    elif kind == P_FIXED:
        t = ArrowType(AT_FIXED_SIZE_BINARY)
        t.byte_width = length
    elif kind == P_BINARY:
        t = ArrowType(AT_BINARY)
    elif kind == P_DECIMAL:
        t = ArrowType(AT_DECIMAL128)
        t.precision = precision
        t.scale = scale
    elif kind == P_LONG:
        t = ArrowType(AT_INT64)
    else:
        # `unknown`, `variant`, geometry/geography: carried as binary, which is
        # what they are on the wire.
        t = ArrowType(AT_BINARY)
    return t^


def _build_array(col: ScanColumn, n: Int) raises -> ArrayData:
    var t = arrow_type_of(col.kind, col.precision, col.scale, col.length)
    var a = ArrayData(t^, col.name)
    a.length = n
    a.nullable = True
    a.field_id = Int32(col.field_id)
    var width = a.type.fixed_width()
    var is_binary = (
        a.type.id == AT_UTF8
        or a.type.id == AT_BINARY
        or a.type.id == AT_LARGE_UTF8
        or a.type.id == AT_LARGE_BINARY
    )
    if is_binary:
        a.offsets.append(0)
    for r in range(n):
        ref d = col.values[r]
        bit_set(a.validity, r, d.valid)
        if not d.valid:
            a.null_count += 1
        if a.type.id == AT_BOOL:
            while len(a.values) <= r // 8:
                a.values.append(0)
            if d.valid and d.i != 0:
                a.values[r // 8] |= UInt8(1) << UInt8(r % 8)
        elif is_binary:
            if d.valid:
                if col.kind == P_STRING:
                    var b = d.s.as_bytes()
                    for k in range(len(b)):
                        a.values.append(b[k])
                else:
                    for k in range(len(d.b)):
                        a.values.append(d.b[k])
            a.offsets.append(Int32(len(a.values)))
        elif a.type.id == AT_DECIMAL128:
            var le = _decimal_le16(d)
            for k in range(16):
                a.values.append(le[k])
        elif a.type.id == AT_FIXED_SIZE_BINARY:
            for k in range(width):
                a.values.append(d.b[k] if d.valid and k < len(d.b) else 0)
        elif a.type.id == AT_FLOAT32:
            _append_le(
                a.values, UInt64(UInt32(_f32_bits(d.f if d.valid else 0.0))), 4
            )
        elif a.type.id == AT_FLOAT64:
            _append_le(a.values, _f64_bits(d.f if d.valid else 0.0), 8)
        else:
            _append_le(a.values, UInt64(Int64(d.i if d.valid else 0)), width)
    # A bitmap has to cover every row even when the last byte is partial.
    while len(a.validity) < (n + 7) // 8:
        a.validity.append(0)
    if a.type.id == AT_BOOL:
        while len(a.values) < (n + 7) // 8:
            a.values.append(0)
    return a^


def _f32_bits(v: Float64) -> UInt32:
    return bitcast[DType.uint32](Float32(v))


def _f64_bits(v: Float64) -> UInt64:
    return bitcast[DType.uint64](v)


def _append_le(mut out: List[UInt8], v: UInt64, width: Int):
    for k in range(width):
        out.append(UInt8((v >> UInt64(8 * k)) & 0xFF))


def _decimal_le16(d: Datum) -> List[UInt8]:
    """A decimal's unscaled value as Arrow's little-endian 16-byte two's
    complement."""
    var out = List[UInt8]()
    if not d.valid or len(d.b) == 0:
        for _ in range(16):
            out.append(0)
        return out^
    var sign = UInt8(0xFF) if (d.b[0] & 0x80) != 0 else UInt8(0)
    for k in range(len(d.b)):
        out.append(d.b[len(d.b) - 1 - k])
    while len(out) < 16:
        out.append(sign)
    while len(out) > 16:
        _ = out.pop()
    return out^


# ── reading one value out of an Arrow array ─────────────────────────────────
def _int_at(a: ArrayData, i: Int) raises -> Int64:
    """The integral value at `i`, whatever fixed width it was stored at.

    This is where `int` -> `long` promotion happens: the file's physical width
    is read, the table's current type is what comes out.
    """
    var id = a.type.id
    if id == AT_BOOL:
        return 1 if bit_get(Span(a.values), i) else 0
    if id == AT_INT8:
        return Int64(bitcast[DType.int8](a.values[i]))
    if id == AT_UINT8:
        return Int64(a.values[i])
    if id == AT_INT16 or id == AT_UINT16:
        var v = UInt16(a.values[2 * i]) | (UInt16(a.values[2 * i + 1]) << 8)
        if id == AT_INT16:
            return Int64(bitcast[DType.int16](v))
        return Int64(v)
    if id == AT_INT32 or id == AT_DATE32 or id == AT_TIME32:
        return Int64(load_i32(Span(a.values), i))
    if id == AT_UINT32:
        return Int64(UInt32(bitcast[DType.uint32](load_i32(Span(a.values), i))))
    if id == AT_INT64 or id == AT_TIME64 or id == AT_TIMESTAMP:
        return load_i64(Span(a.values), i)
    if id == AT_UINT64:
        return load_i64(Span(a.values), i)
    if id == AT_FLOAT32:
        return Int64(load_f32(Span(a.values), i))
    if id == AT_FLOAT64:
        return Int64(load_f64(Span(a.values), i))
    raise Error(
        "iceberg: cannot read an integer from Arrow type " + String(a.type)
    )


def _float_at(a: ArrayData, i: Int) raises -> Float64:
    var id = a.type.id
    if id == AT_FLOAT32:
        return Float64(load_f32(Span(a.values), i))
    if id == AT_FLOAT64:
        return load_f64(Span(a.values), i)
    return Float64(_int_at(a, i))


def _bytes_at(a: ArrayData, i: Int) raises -> List[UInt8]:
    var out = List[UInt8]()
    var id = a.type.id
    var start: Int
    var end: Int
    if id == AT_UTF8 or id == AT_BINARY:
        start = Int(a.offsets[i])
        end = Int(a.offsets[i + 1])
    elif id == AT_LARGE_UTF8 or id == AT_LARGE_BINARY:
        start = Int(a.large_offsets[i])
        end = Int(a.large_offsets[i + 1])
    elif id == AT_FIXED_SIZE_BINARY or id == AT_DECIMAL128:
        var w = a.type.fixed_width()
        start = w * i
        end = start + w
    else:
        raise Error(
            "iceberg: cannot read bytes from Arrow type " + String(a.type)
        )
    for k in range(start, end):
        out.append(a.values[k])
    return out^


def _extract(
    a: ArrayData, i: Int, kind: UInt8, precision: Int, scale: Int, length: Int
) raises -> Datum:
    """One value, typed as the table's current schema says it is."""
    if not a.is_valid(i):
        return Datum.none()
    if kind == P_BOOLEAN:
        return Datum.bool_(_int_at(a, i) != 0)
    if kind == P_FLOAT:
        return Datum.float_(Float64(Float32(_float_at(a, i))))
    if kind == P_DOUBLE:
        return Datum.double_(_float_at(a, i))
    if kind == P_STRING:
        var b = _bytes_at(a, i)
        return Datum.string_(String(StringSlice(unsafe_from_utf8=Span(b))))
    if kind == P_UUID:
        var b = _bytes_at(a, i)
        return Datum.uuid_(b^)
    if kind == P_FIXED:
        var b = _bytes_at(a, i)
        return Datum.fixed_(b^)
    if kind == P_BINARY or kind == P_UNKNOWN:
        var b = _bytes_at(a, i)
        return Datum.binary_(b^)
    if kind == P_DECIMAL:
        # Arrow decimal128 is little-endian; Iceberg's is big-endian minimal.
        var le = _bytes_at(a, i)
        var be = List[UInt8]()
        for k in range(len(le)):
            be.append(le[len(le) - 1 - k])
        return Datum.decimal_(be^, precision, scale)
    # int, long, date, time, timestamp and their nanosecond forms.
    return Datum.integral(kind, _int_at(a, i))


# ── column plans ────────────────────────────────────────────────────────────
comptime SRC_FILE = 0
comptime SRC_CONSTANT = 1
comptime SRC_POS = 2
comptime SRC_ROW_ID = 3


@fieldwise_init
struct _ColumnPlan(Copyable, Movable):
    var name: String
    var field_id: Int
    var kind: UInt8
    var precision: Int
    var scale: Int
    var length: Int
    var source: Int
    var batch_index: Int
    """Which column of the Parquet batch, for `SRC_FILE`."""
    var constant: Datum


@fieldwise_init
struct NameMapping(Copyable, Defaultable, Movable):
    """`schema.name-mapping.default`: fallback ids for files without them."""

    var names: List[String]
    var ids: List[Int]

    def __init__(out self):
        self.names = []
        self.ids = []

    @staticmethod
    def parse(text: String) raises -> Self:
        var doc = parse_json(text)
        var out = Self()
        out._collect(doc, doc.root, "")
        return out^

    def _collect(mut self, doc: Json, node: Int, prefix: String) raises:
        for k in range(doc.size(node)):
            var e = doc.at(node, k)
            var id = -1
            var fi = doc.get(e, "field-id")
            if fi >= 0 and not doc.is_null(fi):
                id = Int(doc.as_int(fi))
            var names = doc.get(e, "names")
            var first = String("")
            if names >= 0:
                for j in range(doc.size(names)):
                    var n = doc.as_string(doc.at(names, j))
                    var full = n if prefix == "" else prefix + "." + n
                    if id >= 0:
                        self.names.append(full)
                        self.ids.append(id)
                    if j == 0:
                        first = full^
            var kids = doc.get(e, "fields")
            if kids >= 0 and not doc.is_null(kids):
                self._collect(doc, kids, first)

    def id_for(self, name: String) -> Int:
        for k in range(len(self.names)):
            if self.names[k] == name:
                return self.ids[k]
        return -1

    def name_for(self, id: Int) -> String:
        for k in range(len(self.ids)):
            if self.ids[k] == id:
                return self.names[k]
        return String("")


# ── row-level predicate evaluation ──────────────────────────────────────────
def collect_field_ids(e: Expr, i: Int, mut out: List[Int]):
    """Every column the expression touches, by id."""
    if i < 0:
        return
    ref n = e.nodes[i]
    if n.op == OP_AND or n.op == OP_OR:
        collect_field_ids(e, n.left, out)
        collect_field_ids(e, n.right, out)
        return
    if n.op == OP_NOT:
        collect_field_ids(e, n.left, out)
        return
    if n.op == OP_TRUE or n.op == OP_FALSE:
        return
    if n.field_id < 0:
        return
    for k in range(len(out)):
        if out[k] == n.field_id:
            return
    out.append(n.field_id)


def eval_row(e: Expr, i: Int, row: List[Datum], ids: List[Int]) raises -> Bool:
    """Evaluate a *bound* expression against one row.

    `row[k]` is the value of field `ids[k]`. A predicate on a null value is
    false for everything except `is-null` and the negations that the planner's
    `rewrite_not` has already pushed down, which is Iceberg's three-valued
    logic collapsed the way a filter needs it.
    """
    if i < 0:
        return True
    ref n = e.nodes[i]
    if n.op == OP_TRUE:
        return True
    if n.op == OP_FALSE:
        return False
    if n.op == OP_AND:
        return eval_row(e, n.left, row, ids) and eval_row(e, n.right, row, ids)
    if n.op == OP_OR:
        return eval_row(e, n.left, row, ids) or eval_row(e, n.right, row, ids)
    if n.op == OP_NOT:
        return not eval_row(e, n.left, row, ids)

    var slot = -1
    for k in range(len(ids)):
        if ids[k] == n.field_id:
            slot = k
            break
    if slot < 0:
        # A predicate on a column that is not being read: it was already
        # applied by the planner, or it cannot be evaluated here.
        return True
    ref v = row[slot]

    if n.op == OP_IS_NULL:
        return not v.valid
    if n.op == OP_NOT_NULL:
        return v.valid
    if n.op == OP_IS_NAN:
        return v.valid and v.is_nan()
    if n.op == OP_NOT_NAN:
        return v.valid and not v.is_nan()
    if not v.valid:
        return False

    if n.op == OP_STARTS_WITH:
        return v.s.startswith(n.lits[0].s)
    if n.op == OP_NOT_STARTS_WITH:
        return not v.s.startswith(n.lits[0].s)
    if n.op == OP_IN or n.op == OP_NOT_IN:
        var found = False
        for k in range(len(n.lits)):
            if compare(v, n.lits[k]) == 0:
                found = True
                break
        return found if n.op == OP_IN else not found

    var c = compare(v, n.lits[0])
    if n.op == OP_EQ:
        return c == 0
    if n.op == OP_NOT_EQ:
        return c != 0
    if n.op == OP_LT:
        return c < 0
    if n.op == OP_LT_EQ:
        return c <= 0
    if n.op == OP_GT:
        return c > 0
    if n.op == OP_GT_EQ:
        return c >= 0
    raise Error("iceberg: cannot evaluate operator " + String(n.op))


# ── scan options ────────────────────────────────────────────────────────────
@fieldwise_init
struct ScanOptions(Copyable, Defaultable, Movable):
    """How to read, as opposed to what."""

    var batch_size: Int
    var limit: Int
    """Stop after this many rows; -1 for all of them."""
    var verify_crc: Bool
    var prune: Bool
    """Use the residual to drop row groups and pages before decoding."""
    var lazy: Bool
    """Fetch the footer and only the surviving row groups, instead of the
    whole file. Worth it over the network, pointless on a local disk."""

    def __init__(out self):
        self.batch_size = 8192
        self.limit = -1
        self.verify_crc = False
        self.prune = True
        self.lazy = False


# ── deletes ─────────────────────────────────────────────────────────────────
def _position_delete_bitmap(
    io: FileIO,
    data_path: String,
    record_count: Int64,
    deletes: List[DataFile],
    options: ScanOptions,
) raises -> List[Bool]:
    """Which positions of this data file are deleted.

    A deletion vector, when one applies, already contains every delete for the
    file — the spec requires a writer adding one to fold in the position
    delete files it replaces — so the older files are skipped, which is also
    what the planner's association guarantees.
    """
    var n = Int(record_count)
    var out = List[Bool](length=n if n > 0 else 0, fill=False)
    var has_vector = False
    for k in range(len(deletes)):
        if deletes[k].is_deletion_vector():
            has_vector = True
            break

    for k in range(len(deletes)):
        ref d = deletes[k]
        if d.is_deletion_vector():
            var positions = deleted_positions(
                io, d.file_path, d.content_offset, d.content_size_in_bytes
            )
            for j in range(len(positions)):
                var p = Int(positions[j])
                if p >= 0 and p < len(out):
                    out[p] = True
            continue
        if has_vector or not d.is_position_delete():
            continue
        _apply_position_delete_file(io, d, data_path, out, options)
    return out^


def _apply_position_delete_file(
    io: FileIO,
    delete_file: DataFile,
    data_path: String,
    mut out: List[Bool],
    options: ScanOptions,
) raises:
    """Read a position delete file and mark the rows it removes.

    The two columns are addressed by the reserved ids the spec assigns them,
    2147483546 (`file_path`) and 2147483545 (`pos`), never by name.
    """
    var data = io.read_all(delete_file.file_path)
    var reader = ParquetReader[AllCodecs](data^)
    reader.batch_size = options.batch_size
    reader.verify_crc = options.verify_crc
    var names = List[String]()
    var path_idx = reader.schema.field_by_id(Int32(POS_DELETE_FILE_PATH_ID))
    var pos_idx = reader.schema.field_by_id(Int32(POS_DELETE_POS_ID))
    if path_idx < 0 or pos_idx < 0:
        # Some writers omit the field ids; the spec fixes the names too.
        path_idx = reader.schema.field_by_name("file_path")
        pos_idx = reader.schema.field_by_name("pos")
    if path_idx < 0 or pos_idx < 0:
        raise Error(
            "iceberg: position delete file '"
            + delete_file.file_path
            + "' has no file_path/pos columns"
        )
    names.append(reader.schema.fields[path_idx].name)
    names.append(reader.schema.fields[pos_idx].name)
    reader.select_columns(names)
    while reader.has_next():
        var batch = reader.read_batch()
        var paths = batch.column(0).copy()
        var positions = batch.column(1).copy()
        for r in range(batch.num_rows):
            if not paths.is_valid(r):
                continue
            var raw = _bytes_at(paths, r)
            var p = String(StringSlice(unsafe_from_utf8=Span(raw)))
            if p != data_path:
                continue
            var at = Int(_int_at(positions, r))
            if at >= 0 and at < len(out):
                out[at] = True


@fieldwise_init
struct _EqualityDeletes(Copyable, Defaultable, Movable):
    """The rows of every equality delete file that applies, by column id."""

    var ids: List[Int]
    var rows: List[List[Datum]]

    def __init__(out self):
        self.ids = []
        self.rows = []

    def matches(self, values: List[Datum]) raises -> Bool:
        """`values` are this row's values for `ids`, in order.

        A delete row matches when every delete column is equal, and `null`
        equals `null` — the spec's "a null value in a delete column matches a
        row if the row's value is null".
        """
        for k in range(len(self.rows)):
            ref cand = self.rows[k]
            var all_equal = True
            for c in range(len(self.ids)):
                ref a = cand[c]
                ref b = values[c]
                if not a.valid or not b.valid:
                    if a.valid != b.valid:
                        all_equal = False
                        break
                    continue
                if compare(a, b) != 0:
                    all_equal = False
                    break
            if all_equal:
                return True
        return False


def _read_equality_deletes(
    io: FileIO,
    deletes: List[DataFile],
    schema: Schema,
    options: ScanOptions,
) raises -> List[_EqualityDeletes]:
    """One `_EqualityDeletes` per delete file, since each names its own
    `equality_ids`."""
    var out = List[_EqualityDeletes]()
    for k in range(len(deletes)):
        ref d = deletes[k]
        if not d.is_equality_delete():
            continue
        if len(d.equality_ids) == 0:
            raise Error(
                "iceberg: equality delete file '"
                + d.file_path
                + "' declares no equality_ids"
            )
        var eq = _EqualityDeletes()
        eq.ids = d.equality_ids.copy()
        var data = io.read_all(d.file_path)
        var reader = ParquetReader[AllCodecs](data^)
        reader.batch_size = options.batch_size
        reader.verify_crc = options.verify_crc
        var names = List[String]()
        var kinds = List[UInt8]()
        var precisions = List[Int]()
        var scales = List[Int]()
        var lengths = List[Int]()
        for j in range(len(eq.ids)):
            var idx = reader.schema.field_by_id(Int32(eq.ids[j]))
            if idx < 0:
                raise Error(
                    "iceberg: equality delete file is missing column "
                    + String(eq.ids[j])
                )
            names.append(reader.schema.fields[idx].name)
            var f = schema.find_field(eq.ids[j])
            ref t = schema.store.nodes[f.type]
            kinds.append(t.prim)
            precisions.append(t.precision)
            scales.append(t.scale)
            lengths.append(t.length)
        reader.select_columns(names)
        while reader.has_next():
            var batch = reader.read_batch()
            for r in range(batch.num_rows):
                var row = List[Datum]()
                for c in range(len(eq.ids)):
                    row.append(
                        _extract(
                            batch.column(c),
                            r,
                            kinds[c],
                            precisions[c],
                            scales[c],
                            lengths[c],
                        )
                    )
                eq.rows.append(row^)
        out.append(eq^)
    return out^


# ── the data-file reader ────────────────────────────────────────────────────
def _identity_partition_value(
    spec: PartitionSpec, partition: List[Datum], source_id: Int
) -> Datum:
    """The partition value that stands in for a column missing from a file.

    Only an identity transform can supply one: every other transform loses
    information, so it cannot reconstruct the column.
    """
    for k in range(len(spec.fields)):
        if spec.fields[k].transform.kind != T_IDENTITY:
            continue
        if spec.fields[k].source_id != source_id:
            continue
        if k < len(partition):
            return partition[k].copy()
    return Datum.none()


def _partition_json(spec: PartitionSpec, partition: List[Datum]) -> String:
    var out = String("{")
    for k in range(len(spec.fields)):
        if k > 0:
            out += ","
        out += json_quote(spec.fields[k].name) + ":"
        if k < len(partition):
            out += partition[k].to_json()
        else:
            out += "null"
    out += "}"
    return out^


def read_data_file(
    io: FileIO,
    data_file: DataFile,
    delete_files: List[DataFile],
    data_sequence_number: Int64,
    spec: PartitionSpec,
    schema: Schema,
    projected: List[Int],
    meta_columns: List[String],
    mapping: NameMapping,
    residual_dsl: String,
    case_sensitive: Bool,
    options: ScanOptions,
) raises -> ScanResult:
    """Read one data file's surviving rows, projected to `projected`."""
    if data_file.file_format.lower() != "parquet":
        raise Error(
            "iceberg: only Parquet data files can be read; '"
            + data_file.file_path
            + "' is "
            + data_file.file_format
        )

    var residual = bind(parse_filter(residual_dsl), schema, case_sensitive)
    var filter_ids = List[Int]()
    collect_field_ids(residual, residual.root, filter_ids)

    var equality = _read_equality_deletes(io, delete_files, schema, options)

    # Everything that must be *read*: the projection, the columns the residual
    # still needs, and the columns every equality delete matches on.
    var read_ids = projected.copy()
    for k in range(len(filter_ids)):
        if not _contains(read_ids, filter_ids[k]):
            read_ids.append(filter_ids[k])
    for k in range(len(equality)):
        for j in range(len(equality[k].ids)):
            if not _contains(read_ids, equality[k].ids[j]):
                read_ids.append(equality[k].ids[j])

    var data = _load_bytes(io, data_file, options)
    var reader = ParquetReader[AllCodecs](data^)
    reader.batch_size = options.batch_size
    reader.verify_crc = options.verify_crc

    # ── column projection, in the spec's order ─────────────────────────────
    var plans = List[_ColumnPlan]()
    var file_names = List[String]()
    for k in range(len(read_ids)):
        var id = read_ids[k]
        var f = schema.find_field(id)
        ref t = schema.store.nodes[f.type]
        if t.kind != TK_PRIMITIVE:
            raise Error(
                "iceberg: cannot read nested column '" + f.name + "' yet"
            )
        var plan = _ColumnPlan(
            f.name,
            id,
            t.prim,
            t.precision,
            t.scale,
            t.length,
            SRC_CONSTANT,
            -1,
            Datum.none(),
        )
        # 1. the column with this id in the file.
        var idx = reader.schema.field_by_id(Int32(id))
        # 2. an identity partition value.
        if idx < 0:
            var pv = _identity_partition_value(spec, data_file.partition, id)
            if pv.valid:
                plan.constant = pv^
                plans.append(plan^)
                continue
        # 3. `schema.name-mapping.default`.
        if idx < 0:
            var mapped = mapping.name_for(id)
            if mapped != "":
                idx = reader.schema.field_by_name(mapped)
        if idx >= 0:
            plan.source = SRC_FILE
            plan.batch_index = len(file_names)
            file_names.append(reader.schema.fields[idx].name)
            plans.append(plan^)
            continue
        # 4. `initial-default`, then 5. null.
        var nf = _nested_field(schema, id)
        if nf.has_initial_default:
            plan.constant = datum_from_json_prim(
                t.prim,
                t.precision,
                t.scale,
                t.length,
                parse_json(nf.initial_default),
                parse_json(nf.initial_default).root,
            )
        plans.append(plan^)

    # Metadata columns are appended after the real ones, in the order asked.
    for k in range(len(meta_columns)):
        var name = meta_columns[k]
        var plan = _ColumnPlan(
            name, -1, P_LONG, 0, 0, 0, SRC_CONSTANT, -1, Datum.none()
        )
        if name == META_FILE:
            plan.kind = P_STRING
            plan.constant = Datum.string_(data_file.file_path)
        elif name == META_POS:
            plan.source = SRC_POS
        elif name == META_SPEC_ID:
            plan.kind = P_INT
            plan.constant = Datum.int_(Int64(spec.spec_id))
        elif name == META_PARTITION:
            plan.kind = P_STRING
            plan.constant = Datum.string_(
                _partition_json(spec, data_file.partition)
            )
        elif name == META_ROW_ID:
            # v3 row lineage: `_row_id` is the file's `first_row_id` plus the
            # row's position, and null when the file has no `first_row_id`.
            if data_file.has_first_row_id:
                plan.source = SRC_ROW_ID
                plan.constant = Datum.long_(data_file.first_row_id)
        elif name == META_LAST_UPDATED:
            plan.constant = Datum.long_(data_sequence_number)
        else:
            raise Error("iceberg: unknown metadata column '" + name + "'")
        plans.append(plan^)

    if len(file_names) > 0:
        reader.select_columns(file_names)
    else:
        reader.select_columns(List[String]())

    # ── deletes and pruning ────────────────────────────────────────────────
    var deleted = _position_delete_bitmap(
        io, data_file.file_path, data_file.record_count, delete_files, options
    )
    var has_position_deletes = False
    for k in range(len(deleted)):
        if deleted[k]:
            has_position_deletes = True
            break
    var need_positions = has_position_deletes or _wants_positions(meta_columns)

    var groups = List[Int]()
    for g in range(reader.num_row_groups()):
        groups.append(g)
    if options.prune and len(file_names) > 0:
        var preds = _predicates_for(residual, residual.root, reader, schema)
        if len(preds) > 0:
            _ = reader.prune_row_groups(preds)
            groups = reader._row_groups.copy()

    var starts = List[Int64]()
    var at: Int64 = 0
    for g in range(reader.num_row_groups()):
        starts.append(at)
        at += reader.meta.row_groups[g].num_rows

    # ── read ───────────────────────────────────────────────────────────────
    var out = List[ScanColumn]()
    for k in range(len(plans)):
        out.append(
            ScanColumn(
                plans[k].name,
                plans[k].field_id,
                plans[k].kind,
                plans[k].precision,
                plans[k].scale,
                plans[k].length,
                List[Datum](),
            )
        )
    var kept = 0
    for gi in range(len(groups)):
        var g = groups[gi]
        reader.select_row_groups([g])
        if options.prune and not need_positions and len(file_names) > 0:
            var preds = _predicates_for(residual, residual.root, reader, schema)
            if len(preds) > 0:
                _ = reader.prune_pages(preds)
        var pos = starts[g]
        while reader.has_next():
            var batch = reader.read_batch()
            var cols = List[ArrayData]()
            for q in range(batch.num_columns()):
                cols.append(batch.column(q).copy())
            var rows_in_batch = batch.num_rows
            for r in range(rows_in_batch):
                var row_pos = pos
                pos += 1
                if Int(row_pos) < len(deleted) and deleted[Int(row_pos)]:
                    continue
                var row = List[Datum]()
                for c in range(len(read_ids)):
                    ref p = plans[c]
                    if p.source == SRC_FILE:
                        row.append(
                            _extract(
                                cols[p.batch_index],
                                r,
                                p.kind,
                                p.precision,
                                p.scale,
                                p.length,
                            )
                        )
                    else:
                        row.append(p.constant.copy())
                if not eval_row(residual, residual.root, row, read_ids):
                    continue
                var dropped = False
                for e in range(len(equality)):
                    var key = List[Datum]()
                    for j in range(len(equality[e].ids)):
                        var slot = _index_of(read_ids, equality[e].ids[j])
                        key.append(row[slot].copy())
                    if equality[e].matches(key):
                        dropped = True
                        break
                if dropped:
                    continue
                for c in range(len(read_ids)):
                    out[c].values.append(row[c].copy())
                for c in range(len(read_ids), len(plans)):
                    ref p = plans[c]
                    if p.source == SRC_POS:
                        out[c].values.append(Datum.long_(row_pos))
                    elif p.source == SRC_ROW_ID:
                        out[c].values.append(
                            Datum.long_(p.constant.i + row_pos)
                        )
                    else:
                        out[c].values.append(p.constant.copy())
                kept += 1
                if options.limit >= 0 and kept >= options.limit:
                    return _trim(out^, len(projected), len(meta_columns))
    return _trim(out^, len(projected), len(meta_columns))


def _trim(
    var columns: List[ScanColumn], projected: Int, meta: Int
) -> ScanResult:
    """Drop the columns that were only read to evaluate a filter or a delete."""
    var out = List[ScanColumn]()
    for k in range(projected):
        out.append(columns[k].copy())
    for k in range(len(columns) - meta, len(columns)):
        out.append(columns[k].copy())
    return ScanResult(out^)


def _wants_positions(meta_columns: List[String]) -> Bool:
    for k in range(len(meta_columns)):
        if meta_columns[k] == META_POS or meta_columns[k] == META_ROW_ID:
            return True
    return False


def _contains(l: List[Int], v: Int) -> Bool:
    for k in range(len(l)):
        if l[k] == v:
            return True
    return False


def _index_of(l: List[Int], v: Int) -> Int:
    for k in range(len(l)):
        if l[k] == v:
            return k
    return -1


def _nested_field(schema: Schema, id: Int) raises -> NestedField:
    var cols = schema.columns()
    for k in range(len(cols)):
        if cols[k].id == id:
            return cols[k].copy()
    return NestedField.simple(id, String(""), False, 0)


# ── loading the bytes ───────────────────────────────────────────────────────
def _load_bytes(
    io: FileIO, data_file: DataFile, options: ScanOptions
) raises -> List[UInt8]:
    """The file's bytes, or just the parts of them a scan will touch.

    `lazy` fetches the footer and then only the byte ranges of the row groups
    that survive statistics pruning, into a buffer the size of the file with
    everything else left zero. Parquet addresses everything by absolute file
    offset, so a sparse buffer decodes exactly like the whole file — and over
    a network this is the difference between one range request per row group
    and downloading the object.
    """
    if not options.lazy:
        return io.read_all(data_file.file_path)

    var size = Int(data_file.file_size_in_bytes)
    if size <= 0:
        size = io.length(data_file.file_path)
    if size < 12:
        return io.read_all(data_file.file_path)
    var tail = io.read_range(data_file.file_path, size - 8, 8)
    if len(tail) != 8 or tail[4] != 0x50 or tail[7] != 0x31:
        # Not a footer we recognise; fall back rather than guess.
        return io.read_all(data_file.file_path)
    var footer_len = (
        Int(tail[0])
        | (Int(tail[1]) << 8)
        | (Int(tail[2]) << 16)
        | (Int(tail[3]) << 24)
    )
    var footer_start = size - 8 - footer_len
    if footer_start < 4 or footer_len <= 0:
        return io.read_all(data_file.file_path)

    var buf = List[UInt8](length=size, fill=0)
    buf[0] = 0x50
    buf[1] = 0x41
    buf[2] = 0x52
    buf[3] = 0x31
    var footer = io.read_range(
        data_file.file_path, footer_start, footer_len + 8
    )
    for k in range(len(footer)):
        buf[footer_start + k] = footer[k]

    # A footer-only reader is enough to say where each row group lives.
    var probe = ParquetReader[AllCodecs](buf.copy())
    for g in range(probe.num_row_groups()):
        var extent = _row_group_extent(probe, g)
        var start = extent[0]
        var length = extent[1]
        if length <= 0 or start < 0 or start + length > size:
            continue
        var chunk = io.read_range(data_file.file_path, start, length)
        for k in range(len(chunk)):
            buf[start + k] = chunk[k]
    return buf^


def _row_group_extent(
    reader: ParquetReader[AllCodecs], g: Int
) raises -> Tuple[Int, Int]:
    """`(start, length)` of one row group's column data.

    A row group's first byte is the first page of its first column chunk —
    the dictionary page when there is one — and `total_compressed_size` on the
    column chunks covers the rest.
    """
    ref rg = reader.meta.row_groups[g]
    var start = -1
    var end = -1
    for c in range(len(rg.columns)):
        ref chunk = rg.columns[c]
        if not chunk.meta_data:
            continue
        ref md = chunk.meta_data.value()
        var at = Int(md.data_page_offset)
        if md.dictionary_page_offset:
            var dp = Int(md.dictionary_page_offset.value())
            if dp > 0 and dp < at:
                at = dp
        var stop = at + Int(md.total_compressed_size)
        if start < 0 or at < start:
            start = at
        if stop > end:
            end = stop
    if start < 0:
        return (0, 0)
    return (start, end - start)


# ── statistics predicates ───────────────────────────────────────────────────
def _predicates_for(
    e: Expr, i: Int, reader: ParquetReader[AllCodecs], schema: Schema
) raises -> List[Predicate]:
    """The conjunction of simple comparisons the Parquet reader can prune on.

    Only the top-level `AND` chain contributes: a disjunction proves nothing
    about a page. `!=` and `not-in` are excluded for the same reason the
    metrics evaluator excludes them — a bound is not a value that occurs.
    """
    var out = List[Predicate]()
    _collect_predicates(e, i, reader, schema, out)
    return out^


def _collect_predicates(
    e: Expr,
    i: Int,
    reader: ParquetReader[AllCodecs],
    schema: Schema,
    mut out: List[Predicate],
) raises:
    if i < 0:
        return
    ref n = e.nodes[i]
    if n.op == OP_AND:
        _collect_predicates(e, n.left, reader, schema, out)
        _collect_predicates(e, n.right, reader, schema, out)
        return
    var op: Int
    if n.op == OP_EQ:
        op = OP_EQ_PQ
    elif n.op == OP_LT:
        op = OP_LT_PQ
    elif n.op == OP_LT_EQ:
        op = OP_LE_PQ
    elif n.op == OP_GT:
        op = OP_GT_PQ
    elif n.op == OP_GT_EQ:
        op = OP_GE_PQ
    else:
        return
    if n.field_id < 0 or len(n.lits) == 0:
        return
    var idx = reader.schema.field_by_id(Int32(n.field_id))
    if idx < 0:
        return
    var name = reader.schema.fields[idx].name
    if reader.schema.leaf_by_path(name) < 0:
        return
    var v = _scalar_of(n.lits[0])
    if v.kind == SV_NONE:
        return
    out.append(Predicate(name, op, v^))


def _scalar_of(d: Datum) raises -> ScalarValue:
    if not d.valid:
        return ScalarValue()
    if d.kind == P_BOOLEAN:
        return ScalarValue.of_bool(d.i != 0)
    if d.kind == P_FLOAT or d.kind == P_DOUBLE:
        return ScalarValue.of_float(d.f)
    if d.kind == P_STRING:
        return ScalarValue.of_bytes(d.s.as_bytes())
    if d.kind == P_BINARY or d.kind == P_FIXED or d.kind == P_UUID:
        return ScalarValue.of_bytes(Span(d.b))
    if d.kind == P_DECIMAL:
        return ScalarValue()
    if (
        d.kind == P_INT
        or d.kind == P_LONG
        or d.kind == P_DATE
        or d.kind == P_TIME
        or d.kind == P_TIMESTAMP
        or d.kind == P_TIMESTAMPTZ
        or d.kind == P_TIMESTAMP_NS
        or d.kind == P_TIMESTAMPTZ_NS
    ):
        return ScalarValue.of_int(d.i)
    return ScalarValue()
