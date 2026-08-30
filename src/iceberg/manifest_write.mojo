"""Writing manifests and manifest lists.

The two Avro files a commit produces, byte-compatible with what iceberg-rust,
PyIceberg and Java write:

* a **manifest** (`<uuid>-m<n>.avro`) — one `manifest_entry` per file, with the
  `data_file` struct the spec's Appendix A fixes for the format version, and
  the file-level metadata (`schema`, `schema-id`, `partition-spec`,
  `partition-spec-id`, `format-version`, `content`) that tells a reader how to
  type the `partition` column;
* a **manifest list** (`snap-<id>-<n>-<uuid>.avro`) — one `manifest_file` per
  manifest, with the counts and the per-partition-field summaries that let a
  scan skip a manifest without opening it, plus the file-level metadata
  (`snapshot-id`, `parent-snapshot-id`, `sequence-number`, `format-version`).

The Avro schema for each is generated per format version rather than
hard-coded once, because the field sets genuinely differ: v1 has a required
`snapshot_id` and a required `block_size_in_bytes` and no `content`; v2 adds
`content`, `sequence_number`, `file_sequence_number` and `equality_ids`; v3
adds `first_row_id`, `referenced_data_file`, `content_offset` and
`content_size_in_bytes` to `data_file` and `first_row_id` to `manifest_file`.
`avro.DataFileWriter.set_schema_json` writes the generated JSON verbatim, so
the header is exactly what was intended rather than a printed approximation.

The `partition` field is a record typed by the *writing* spec, which is what
makes a partition tuple readable at all: the manifest's own file metadata is
the only place that typing exists.
"""

from avro import DataFileWriter, Schema as AvroSchema, Value, parse_schema

from .expressions import ColumnMetrics, FieldSummary
from .json import json_quote
from .manifest import (
    CONTENT_DATA,
    CONTENT_EQUALITY_DELETES,
    CONTENT_POSITION_DELETES,
    DataFile,
    Manifest,
    ManifestEntry,
    ManifestFile,
    MANIFEST_CONTENT_DATA,
    MANIFEST_CONTENT_DELETES,
    STATUS_ADDED,
    STATUS_DELETED,
    STATUS_EXISTING,
)
from .schema import Schema
from .transforms import PartitionSpec
from .types import (
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
    TypeStore,
)
from .values import Datum, compare, datum_from_bytes_prim, datum_to_bytes


# ── the partition struct's typing ───────────────────────────────────────────
struct PartitionTyping(Copyable, Defaultable, Movable, Sized):
    """The result type of every field of a partition spec, flattened."""

    var names: List[String]
    var field_ids: List[Int]
    var kinds: List[UInt8]
    var precisions: List[Int]
    var scales: List[Int]
    var lengths: List[Int]

    def __init__(out self):
        self.names = []
        self.field_ids = []
        self.kinds = []
        self.precisions = []
        self.scales = []
        self.lengths = []

    def __init__(out self, *, copy: Self):
        self.names = copy.names.copy()
        self.field_ids = copy.field_ids.copy()
        self.kinds = copy.kinds.copy()
        self.precisions = copy.precisions.copy()
        self.scales = copy.scales.copy()
        self.lengths = copy.lengths.copy()

    def __init__(out self, *, deinit move: Self):
        self.names = move.names^
        self.field_ids = move.field_ids^
        self.kinds = move.kinds^
        self.precisions = move.precisions^
        self.scales = move.scales^
        self.lengths = move.lengths^

    def __len__(self) -> Int:
        return len(self.kinds)

    @staticmethod
    def of(spec: PartitionSpec, schema: Schema) raises -> Self:
        var pt = spec.partition_type(schema)
        var out = Self()
        var cols = pt.columns()
        for k in range(len(cols)):
            ref t = pt.store.nodes[cols[k].type]
            out.names.append(cols[k].name)
            out.field_ids.append(cols[k].id)
            out.kinds.append(t.prim)
            out.precisions.append(t.precision)
            out.scales.append(t.scale)
            out.lengths.append(t.length)
        return out^


def decimal_byte_width(precision: Int) -> Int:
    """The fixed width Avro stores a decimal of this precision in."""
    var n = 1
    while n < 40:
        # 8n - 1 bits of magnitude hold up to 10^p - 1 decimal digits, and
        # log10(2) ~= 0.30103.
        var digits = ((8 * n - 1) * 30103) // 100000
        if digits >= precision:
            return n
        n += 1
    return 16


# ── Avro type JSON for one Iceberg primitive ────────────────────────────────
def avro_prim_json(
    kind: UInt8, precision: Int, scale: Int, length: Int, hint: String
) -> String:
    if kind == P_BOOLEAN:
        return String('"boolean"')
    if kind == P_INT:
        return String('"int"')
    if kind == P_LONG:
        return String('"long"')
    if kind == P_FLOAT:
        return String('"float"')
    if kind == P_DOUBLE:
        return String('"double"')
    if kind == P_DATE:
        return String('{"type":"int","logicalType":"date"}')
    if kind == P_TIME:
        return String('{"type":"long","logicalType":"time-micros"}')
    if kind == P_TIMESTAMP:
        return String(
            '{"type":"long","logicalType":"timestamp-micros",'
            '"adjust-to-utc":false}'
        )
    if kind == P_TIMESTAMPTZ:
        return String(
            '{"type":"long","logicalType":"timestamp-micros",'
            '"adjust-to-utc":true}'
        )
    if kind == P_TIMESTAMP_NS:
        return String(
            '{"type":"long","logicalType":"timestamp-nanos",'
            '"adjust-to-utc":false}'
        )
    if kind == P_TIMESTAMPTZ_NS:
        return String(
            '{"type":"long","logicalType":"timestamp-nanos",'
            '"adjust-to-utc":true}'
        )
    if kind == P_STRING:
        return String('"string"')
    if kind == P_UUID:
        return String(
            '{"type":"fixed","size":16,"name":"'
            + hint
            + '_uuid","logicalType":"uuid"}'
        )
    if kind == P_FIXED:
        return String(
            '{"type":"fixed","size":'
            + String(length)
            + ',"name":"'
            + hint
            + '_fixed"}'
        )
    if kind == P_DECIMAL:
        return String(
            '{"type":"fixed","size":'
            + String(decimal_byte_width(precision))
            + ',"name":"'
            + hint
            + '_decimal","logicalType":"decimal","precision":'
            + String(precision)
            + ',"scale":'
            + String(scale)
            + "}"
        )
    # binary, unknown, variant, geometry: bytes on the wire.
    return String('"bytes"')


def _sanitize(name: String) -> String:
    """An Avro name: letters, digits and underscores only."""
    var out = String("")
    var b = name.as_bytes()
    for k in range(len(b)):
        var c = b[k]
        var ok = (
            (c >= 48 and c <= 57)
            or (c >= 65 and c <= 90)
            or (c >= 97 and c <= 122)
            or c == 95
        )
        out += String(
            StringSlice(unsafe_from_utf8=Span(b)[k : k + 1])
        ) if ok else "_"
    if out == "":
        return String("_")
    var first = out.as_bytes()[0]
    if first >= 48 and first <= 57:
        return "_" + out
    return out^


def partition_record_json(typing: PartitionTyping) -> String:
    var out = String('{"type":"record","name":"r102","fields":[')
    for k in range(len(typing)):
        if k > 0:
            out += ","
        var hint = _sanitize(typing.names[k])
        out += '{"name":' + json_quote(typing.names[k])
        out += ',"type":["null",'
        out += avro_prim_json(
            typing.kinds[k],
            typing.precisions[k],
            typing.scales[k],
            typing.lengths[k],
            hint,
        )
        out += '],"default":null,"field-id":' + String(typing.field_ids[k])
        out += "}"
    out += "]}"
    return out^


def _map_json(
    name: String,
    field_id: Int,
    key_id: Int,
    value_id: Int,
    value_type: String,
) -> String:
    return String(
        '{"name":"'
        + name
        + '","type":["null",{"type":"array","items":{"type":"record","name":"k'
        + String(key_id)
        + "_v"
        + String(value_id)
        + '","fields":[{"name":"key","type":"int","field-id":'
        + String(key_id)
        + '},{"name":"value","type":'
        + value_type
        + ',"field-id":'
        + String(value_id)
        + '}]},"logicalType":"map"}],"default":null,"field-id":'
        + String(field_id)
        + "}"
    )


def _opt_json(name: String, field_id: Int, t: String) -> String:
    return String(
        '{"name":"'
        + name
        + '","type":["null",'
        + t
        + '],"default":null,"field-id":'
        + String(field_id)
        + "}"
    )


def _req_json(name: String, field_id: Int, t: String) -> String:
    return String(
        '{"name":"'
        + name
        + '","type":'
        + t
        + ',"field-id":'
        + String(field_id)
        + "}"
    )


def data_file_record_json(
    format_version: Int, typing: PartitionTyping
) -> String:
    var f = List[String]()
    if format_version >= 2:
        f.append('{"name":"content","type":"int","default":0,"field-id":134}')
    f.append(_req_json("file_path", 100, '"string"'))
    f.append(_req_json("file_format", 101, '"string"'))
    f.append(_req_json("partition", 102, partition_record_json(typing)))
    f.append(_req_json("record_count", 103, '"long"'))
    f.append(_req_json("file_size_in_bytes", 104, '"long"'))
    if format_version == 1:
        f.append(_req_json("block_size_in_bytes", 105, '"long"'))
    f.append(_map_json("column_sizes", 108, 117, 118, '"long"'))
    f.append(_map_json("value_counts", 109, 119, 120, '"long"'))
    f.append(_map_json("null_value_counts", 110, 121, 122, '"long"'))
    f.append(_map_json("nan_value_counts", 137, 138, 139, '"long"'))
    f.append(_map_json("lower_bounds", 125, 126, 127, '"bytes"'))
    f.append(_map_json("upper_bounds", 128, 129, 130, '"bytes"'))
    f.append(_opt_json("key_metadata", 131, '"bytes"'))
    f.append(
        _opt_json(
            "split_offsets",
            132,
            '{"type":"array","items":"long","element-id":133}',
        )
    )
    if format_version >= 2:
        f.append(
            _opt_json(
                "equality_ids",
                135,
                '{"type":"array","items":"int","element-id":136}',
            )
        )
    f.append(_opt_json("sort_order_id", 140, '"int"'))
    if format_version >= 3:
        f.append(_opt_json("first_row_id", 142, '"long"'))
        f.append(_opt_json("referenced_data_file", 143, '"string"'))
        f.append(_opt_json("content_offset", 144, '"long"'))
        f.append(_opt_json("content_size_in_bytes", 145, '"long"'))
    var out = String('{"type":"record","name":"r2","fields":[')
    for k in range(len(f)):
        if k > 0:
            out += ","
        out += f[k]
    out += "]}"
    return out^


def manifest_entry_schema_json(
    format_version: Int, typing: PartitionTyping
) -> String:
    var f = List[String]()
    f.append(_req_json("status", 0, '"int"'))
    if format_version == 1:
        f.append(_req_json("snapshot_id", 1, '"long"'))
    else:
        f.append(_opt_json("snapshot_id", 1, '"long"'))
        f.append(_opt_json("sequence_number", 3, '"long"'))
        f.append(_opt_json("file_sequence_number", 4, '"long"'))
    f.append(
        _req_json("data_file", 2, data_file_record_json(format_version, typing))
    )
    var out = String('{"type":"record","name":"manifest_entry","fields":[')
    for k in range(len(f)):
        if k > 0:
            out += ","
        out += f[k]
    out += "]}"
    return out^


comptime _FIELD_SUMMARY_JSON = String(
    '{"type":"record","name":"r508","fields":['
    '{"name":"contains_null","type":"boolean","field-id":509},'
    '{"name":"contains_nan","type":["null","boolean"],"default":null,'
    '"field-id":518},'
    '{"name":"lower_bound","type":["null","bytes"],"default":null,'
    '"field-id":510},'
    '{"name":"upper_bound","type":["null","bytes"],"default":null,'
    '"field-id":511}]}'
)


def manifest_list_schema_json(format_version: Int) -> String:
    var f = List[String]()
    f.append(_req_json("manifest_path", 500, '"string"'))
    f.append(_req_json("manifest_length", 501, '"long"'))
    f.append(_req_json("partition_spec_id", 502, '"int"'))
    if format_version >= 2:
        f.append(_req_json("content", 517, '"int"'))
        f.append(_req_json("sequence_number", 515, '"long"'))
        f.append(_req_json("min_sequence_number", 516, '"long"'))
    f.append(_req_json("added_snapshot_id", 503, '"long"'))
    if format_version == 1:
        f.append(_opt_json("added_files_count", 504, '"int"'))
        f.append(_opt_json("existing_files_count", 505, '"int"'))
        f.append(_opt_json("deleted_files_count", 506, '"int"'))
        f.append(_opt_json("added_rows_count", 512, '"long"'))
        f.append(_opt_json("existing_rows_count", 513, '"long"'))
        f.append(_opt_json("deleted_rows_count", 514, '"long"'))
    else:
        f.append(_req_json("added_files_count", 504, '"int"'))
        f.append(_req_json("existing_files_count", 505, '"int"'))
        f.append(_req_json("deleted_files_count", 506, '"int"'))
        f.append(_req_json("added_rows_count", 512, '"long"'))
        f.append(_req_json("existing_rows_count", 513, '"long"'))
        f.append(_req_json("deleted_rows_count", 514, '"long"'))
    f.append(
        _opt_json(
            "partitions",
            507,
            '{"type":"array","element-id":508,"items":'
            + _FIELD_SUMMARY_JSON
            + "}",
        )
    )
    f.append(_opt_json("key_metadata", 519, '"bytes"'))
    if format_version >= 3:
        f.append(_opt_json("first_row_id", 520, '"long"'))
    var out = String('{"type":"record","name":"manifest_file","fields":[')
    for k in range(len(f)):
        if k > 0:
            out += ","
        out += f[k]
    out += "]}"
    return out^


def put(
    mut names: List[String], mut values: List[Value], n: String, var v: Value
):
    names.append(n)
    values.append(v^)


# ── Datum -> Avro value ─────────────────────────────────────────────────────
def _sign_extend(b: List[UInt8], width: Int) -> List[UInt8]:
    var pad = UInt8(0xFF) if (len(b) > 0 and (b[0] & 0x80) != 0) else UInt8(0)
    var out = List[UInt8](capacity=width)
    var skip = len(b) - width
    if skip < 0:
        skip = 0
    for _ in range(width - (len(b) - skip)):
        out.append(pad)
    for k in range(skip, len(b)):
        out.append(b[k])
    return out^


def partition_value(d: Datum, kind: UInt8, precision: Int) raises -> Value:
    if not d.valid:
        return Value.null()
    if kind == P_BOOLEAN:
        return Value.boolean(d.i != 0)
    if kind == P_INT or kind == P_DATE:
        return Value.int(d.i)
    if (
        kind == P_LONG
        or kind == P_TIME
        or kind == P_TIMESTAMP
        or kind == P_TIMESTAMPTZ
        or kind == P_TIMESTAMP_NS
        or kind == P_TIMESTAMPTZ_NS
    ):
        return Value.long(d.i)
    if kind == P_FLOAT:
        return Value.float(Float32(d.f))
    if kind == P_DOUBLE:
        return Value.double(d.f)
    if kind == P_STRING:
        return Value.string(d.s)
    if kind == P_UUID or kind == P_FIXED:
        return Value.fixed(Span(d.b))
    if kind == P_DECIMAL:
        return Value.fixed(
            Span(_sign_extend(d.b, decimal_byte_width(precision)))
        )
    return Value.bytes(Span(d.b))


def _int_map_value(entries: List[ColumnMetrics], which: Int) raises -> Value:
    """`which`: 0 = column size, 1 = value count, 2 = null count, 3 = nan."""
    var items = List[Value]()
    for k in range(len(entries)):
        ref m = entries[k]
        var present: Bool
        var v: Int64
        if which == 0 or which == 1:
            present = m.has_value_count
            v = m.value_count
        elif which == 2:
            present = m.has_null_value_count
            v = m.null_value_count
        else:
            present = m.has_nan_value_count
            v = m.nan_value_count
        if not present:
            continue
        var names: List[String] = [String("key"), String("value")]
        items.append(
            Value.record(names^, [Value.int(Int64(m.field_id)), Value.long(v)])
        )
    if len(items) == 0:
        return Value.null()
    return Value.array(items)


def _bytes_map_value(entries: List[ColumnMetrics], upper: Bool) raises -> Value:
    var items = List[Value]()
    for k in range(len(entries)):
        ref m = entries[k]
        var present = m.has_upper if upper else m.has_lower
        if not present:
            continue
        var raw = m.upper_bound.copy() if upper else m.lower_bound.copy()
        var names: List[String] = [String("key"), String("value")]
        items.append(
            Value.record(
                names^, [Value.int(Int64(m.field_id)), Value.bytes(Span(raw))]
            )
        )
    if len(items) == 0:
        return Value.null()
    return Value.array(items)


def data_file_value(
    df: DataFile, format_version: Int, typing: PartitionTyping
) raises -> Value:
    var names = List[String]()
    var values = List[Value]()
    if format_version >= 2:
        put(names, values, String("content"), Value.int(Int64(df.content)))
    put(names, values, String("file_path"), Value.string(df.file_path))
    put(names, values, String("file_format"), Value.string(df.file_format))

    var pnames = List[String]()
    var pvalues = List[Value]()
    for k in range(len(typing)):
        pnames.append(typing.names[k])
        var d = (
            df.partition[k].copy() if k < len(df.partition) else Datum.none()
        )
        pvalues.append(
            partition_value(d, typing.kinds[k], typing.precisions[k])
        )
    put(names, values, String("partition"), Value.record(pnames^, pvalues))

    put(names, values, String("record_count"), Value.long(df.record_count))
    put(
        names,
        values,
        String("file_size_in_bytes"),
        Value.long(df.file_size_in_bytes),
    )
    if format_version == 1:
        put(names, values, String("block_size_in_bytes"), Value.long(67108864))
    put(
        names,
        values,
        String("column_sizes"),
        _int_map_value(df.column_sizes, 0),
    )
    put(names, values, String("value_counts"), _int_map_value(df.metrics, 1))
    put(
        names,
        values,
        String("null_value_counts"),
        _int_map_value(df.metrics, 2),
    )
    put(
        names, values, String("nan_value_counts"), _int_map_value(df.metrics, 3)
    )
    put(
        names,
        values,
        String("lower_bounds"),
        _bytes_map_value(df.metrics, False),
    )
    put(
        names,
        values,
        String("upper_bounds"),
        _bytes_map_value(df.metrics, True),
    )
    put(
        names,
        values,
        String("key_metadata"),
        Value.bytes(
            Span(df.key_metadata)
        ) if df.has_key_metadata else Value.null(),
    )
    if len(df.split_offsets) > 0:
        var offs = List[Value]()
        for k in range(len(df.split_offsets)):
            offs.append(Value.long(df.split_offsets[k]))
        put(names, values, String("split_offsets"), Value.array(offs))
    else:
        put(names, values, String("split_offsets"), Value.null())
    if format_version >= 2:
        if len(df.equality_ids) > 0:
            var eq = List[Value]()
            for k in range(len(df.equality_ids)):
                eq.append(Value.int(Int64(df.equality_ids[k])))
            put(names, values, String("equality_ids"), Value.array(eq))
        else:
            put(names, values, String("equality_ids"), Value.null())
    put(
        names,
        values,
        String("sort_order_id"),
        Value.int(
            Int64(df.sort_order_id)
        ) if df.has_sort_order_id else Value.null(),
    )
    if format_version >= 3:
        put(
            names,
            values,
            String("first_row_id"),
            Value.long(
                df.first_row_id
            ) if df.has_first_row_id else Value.null(),
        )
        put(
            names,
            values,
            String("referenced_data_file"),
            Value.string(
                df.referenced_data_file
            ) if df.has_referenced_data_file else Value.null(),
        )
        put(
            names,
            values,
            String("content_offset"),
            Value.long(
                df.content_offset
            ) if df.has_content_offset else Value.null(),
        )
        put(
            names,
            values,
            String("content_size_in_bytes"),
            Value.long(
                df.content_size_in_bytes
            ) if df.has_content_size_in_bytes else Value.null(),
        )
    return Value.record(names^, values)


def manifest_entry_value(
    entry: ManifestEntry,
    format_version: Int,
    snapshot_id: Int64,
    typing: PartitionTyping,
) raises -> Value:
    var names = List[String]()
    var values = List[Value]()
    names.append(String("status"))
    values.append(Value.int(Int64(entry.status)))
    names.append(String("snapshot_id"))
    var sid = entry.snapshot_id if entry.has_snapshot_id else snapshot_id
    if format_version == 1:
        values.append(Value.long(sid))
    else:
        values.append(Value.long(sid))
        names.append(String("sequence_number"))
        # An ADDED entry inherits both from the manifest list, and the spec
        # requires them to be null so that inheritance can happen.
        names.append(String("file_sequence_number"))
        if entry.status == STATUS_ADDED:
            values.append(Value.null())
            values.append(Value.null())
        else:
            values.append(Value.long(entry.sequence_number))
            values.append(Value.long(entry.file_sequence_number))
    names.append(String("data_file"))
    values.append(data_file_value(entry.data_file, format_version, typing))
    return Value.record(names^, values)


# ── the written artefacts ───────────────────────────────────────────────────
struct WrittenManifest(Copyable, Defaultable, Movable):
    """A manifest's bytes and everything its manifest-list entry needs."""

    var bytes: List[UInt8]
    var added_files_count: Int
    var existing_files_count: Int
    var deleted_files_count: Int
    var added_rows_count: Int64
    var existing_rows_count: Int64
    var deleted_rows_count: Int64
    var min_sequence_number: Int64
    var partitions: List[FieldSummary]
    var content: Int

    def __init__(out self):
        self.bytes = []
        self.added_files_count = 0
        self.existing_files_count = 0
        self.deleted_files_count = 0
        self.added_rows_count = 0
        self.existing_rows_count = 0
        self.deleted_rows_count = 0
        self.min_sequence_number = 0
        self.partitions = []
        self.content = MANIFEST_CONTENT_DATA

    def __init__(out self, *, copy: Self):
        self.bytes = copy.bytes.copy()
        self.added_files_count = copy.added_files_count
        self.existing_files_count = copy.existing_files_count
        self.deleted_files_count = copy.deleted_files_count
        self.added_rows_count = copy.added_rows_count
        self.existing_rows_count = copy.existing_rows_count
        self.deleted_rows_count = copy.deleted_rows_count
        self.min_sequence_number = copy.min_sequence_number
        self.partitions = copy.partitions.copy()
        self.content = copy.content

    def __init__(out self, *, deinit move: Self):
        self.bytes = move.bytes^
        self.added_files_count = move.added_files_count
        self.existing_files_count = move.existing_files_count
        self.deleted_files_count = move.deleted_files_count
        self.added_rows_count = move.added_rows_count
        self.existing_rows_count = move.existing_rows_count
        self.deleted_rows_count = move.deleted_rows_count
        self.min_sequence_number = move.min_sequence_number
        self.partitions = move.partitions.copy()
        self.content = move.content

    def to_manifest_file(
        self,
        path: String,
        spec_id: Int,
        snapshot_id: Int64,
        sequence_number: Int64,
        first_row_id: Int64,
        has_first_row_id: Bool,
    ) -> ManifestFile:
        return ManifestFile(
            path,
            Int64(len(self.bytes)),
            spec_id,
            self.content,
            sequence_number,
            self.min_sequence_number,
            snapshot_id,
            self.added_files_count,
            True,
            self.existing_files_count,
            True,
            self.deleted_files_count,
            True,
            self.added_rows_count,
            True,
            self.existing_rows_count,
            True,
            self.deleted_rows_count,
            True,
            self.partitions.copy(),
            len(self.partitions) > 0,
            List[UInt8](),
            first_row_id,
            has_first_row_id,
        )


def _spec_fields_json(spec: PartitionSpec) -> String:
    """Just the `fields` array — what a manifest's `partition-spec` key holds.
    """
    var out = String("[")
    for k in range(len(spec.fields)):
        if k > 0:
            out += ","
        ref f = spec.fields[k]
        out += '{"source-id":' + String(f.source_id)
        out += ',"field-id":' + String(f.field_id)
        out += ',"name":' + json_quote(f.name)
        out += ',"transform":' + json_quote(f.transform.raw)
        out += "}"
    out += "]"
    return out^


def _summarise(
    mut summaries: List[FieldSummary],
    typing: PartitionTyping,
    partition: List[Datum],
) raises:
    while len(summaries) < len(typing):
        summaries.append(
            FieldSummary(False, False, False, [], False, [], False)
        )
    for k in range(len(typing)):
        var d = partition[k].copy() if k < len(partition) else Datum.none()
        if not d.valid:
            summaries[k].contains_null = True
            continue
        if d.is_nan():
            summaries[k].contains_nan = True
            summaries[k].has_contains_nan = True
            continue
        if not summaries[k].has_contains_nan:
            summaries[k].contains_nan = False
            summaries[k].has_contains_nan = True
        var raw = datum_to_bytes(d)
        if not summaries[k].has_lower:
            summaries[k].lower_bound = raw.copy()
            summaries[k].has_lower = True
            summaries[k].upper_bound = raw.copy()
            summaries[k].has_upper = True
            continue
        # Compare on the decoded value, not the bytes: only string and binary
        # encodings sort the same either way.
        var lo = _decode_bound(summaries[k].lower_bound, typing, k)
        var hi = _decode_bound(summaries[k].upper_bound, typing, k)
        if compare(d, lo) < 0:
            summaries[k].lower_bound = raw.copy()
        if compare(d, hi) > 0:
            summaries[k].upper_bound = raw.copy()


def _decode_bound(
    raw: List[UInt8], typing: PartitionTyping, k: Int
) raises -> Datum:
    return datum_from_bytes_prim(
        typing.kinds[k],
        typing.precisions[k],
        typing.scales[k],
        typing.lengths[k],
        raw,
    )


def write_manifest(
    format_version: Int,
    schema: Schema,
    spec: PartitionSpec,
    content: Int,
    snapshot_id: Int64,
    sequence_number: Int64,
    entries: List[ManifestEntry],
    codec: String = String("deflate"),
) raises -> WrittenManifest:
    """One manifest, as bytes plus the summary its list entry needs."""
    var typing = PartitionTyping.of(spec, schema)
    var schema_json = manifest_entry_schema_json(format_version, typing)
    var avro_schema = parse_schema(schema_json)
    var writer = DataFileWriter(avro_schema^, codec)
    writer.set_schema_json(schema_json)
    writer.set_metadata_string("schema", schema.to_json())
    writer.set_metadata_string("schema-id", String(schema.schema_id))
    writer.set_metadata_string("partition-spec", _spec_fields_json(spec))
    writer.set_metadata_string("partition-spec-id", String(spec.spec_id))
    writer.set_metadata_string("format-version", String(format_version))
    if format_version >= 2:
        writer.set_metadata_string(
            "content",
            "deletes" if content == MANIFEST_CONTENT_DELETES else "data",
        )

    var out = WrittenManifest()
    out.content = content
    out.min_sequence_number = sequence_number
    var seen_min = False
    for k in range(len(entries)):
        ref e = entries[k]
        writer.append(
            manifest_entry_value(e, format_version, snapshot_id, typing)
        )
        if e.status == STATUS_ADDED:
            out.added_files_count += 1
            out.added_rows_count += e.data_file.record_count
        elif e.status == STATUS_EXISTING:
            out.existing_files_count += 1
            out.existing_rows_count += e.data_file.record_count
        else:
            out.deleted_files_count += 1
            out.deleted_rows_count += e.data_file.record_count
        if e.status != STATUS_DELETED:
            var seq = (
                sequence_number if e.status
                == STATUS_ADDED else e.sequence_number
            )
            if not seen_min or seq < out.min_sequence_number:
                out.min_sequence_number = seq
                seen_min = True
        _summarise(out.partitions, typing, e.data_file.partition)
    out.bytes = writer.bytes()
    return out^


def manifest_file_value(mf: ManifestFile, format_version: Int) raises -> Value:
    var names = List[String]()
    var values = List[Value]()
    names.append(String("manifest_path"))
    values.append(Value.string(mf.manifest_path))
    names.append(String("manifest_length"))
    values.append(Value.long(mf.manifest_length))
    names.append(String("partition_spec_id"))
    values.append(Value.int(Int64(mf.partition_spec_id)))
    if format_version >= 2:
        names.append(String("content"))
        values.append(Value.int(Int64(mf.content)))
        names.append(String("sequence_number"))
        values.append(Value.long(mf.sequence_number))
        names.append(String("min_sequence_number"))
        values.append(Value.long(mf.min_sequence_number))
    names.append(String("added_snapshot_id"))
    values.append(Value.long(mf.added_snapshot_id))
    names.append(String("added_files_count"))
    values.append(Value.int(Int64(mf.added_files_count)))
    names.append(String("existing_files_count"))
    values.append(Value.int(Int64(mf.existing_files_count)))
    names.append(String("deleted_files_count"))
    values.append(Value.int(Int64(mf.deleted_files_count)))
    names.append(String("added_rows_count"))
    values.append(Value.long(mf.added_rows_count))
    names.append(String("existing_rows_count"))
    values.append(Value.long(mf.existing_rows_count))
    names.append(String("deleted_rows_count"))
    values.append(Value.long(mf.deleted_rows_count))
    names.append(String("partitions"))
    if mf.has_partitions and len(mf.partitions) > 0:
        var parts = List[Value]()
        for k in range(len(mf.partitions)):
            ref s = mf.partitions[k]
            var fnames: List[String] = [
                String("contains_null"),
                String("contains_nan"),
                String("lower_bound"),
                String("upper_bound"),
            ]
            var fvalues = List[Value]()
            fvalues.append(Value.boolean(s.contains_null))
            fvalues.append(
                Value.boolean(
                    s.contains_nan
                ) if s.has_contains_nan else Value.null()
            )
            fvalues.append(
                Value.bytes(
                    Span(s.lower_bound)
                ) if s.has_lower else Value.null()
            )
            fvalues.append(
                Value.bytes(
                    Span(s.upper_bound)
                ) if s.has_upper else Value.null()
            )
            parts.append(Value.record(fnames^, fvalues))
        values.append(Value.array(parts))
    else:
        values.append(Value.null())
    names.append(String("key_metadata"))
    values.append(
        Value.bytes(Span(mf.key_metadata)) if len(mf.key_metadata)
        > 0 else Value.null()
    )
    if format_version >= 3:
        names.append(String("first_row_id"))
        values.append(
            Value.long(mf.first_row_id) if mf.has_first_row_id else Value.null()
        )
    return Value.record(names^, values)


def write_manifest_list(
    format_version: Int,
    snapshot_id: Int64,
    parent_snapshot_id: Int64,
    has_parent: Bool,
    sequence_number: Int64,
    manifests: List[ManifestFile],
    codec: String = String("deflate"),
) raises -> List[UInt8]:
    var schema_json = manifest_list_schema_json(format_version)
    var avro_schema = parse_schema(schema_json)
    var writer = DataFileWriter(avro_schema^, codec)
    writer.set_schema_json(schema_json)
    writer.set_metadata_string("snapshot-id", String(snapshot_id))
    writer.set_metadata_string(
        "parent-snapshot-id",
        String(parent_snapshot_id) if has_parent else String("null"),
    )
    writer.set_metadata_string("format-version", String(format_version))
    if format_version >= 2:
        writer.set_metadata_string("sequence-number", String(sequence_number))
    for k in range(len(manifests)):
        writer.append(manifest_file_value(manifests[k], format_version))
    return writer.bytes()
