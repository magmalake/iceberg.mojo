"""The test suite for iceberg.mojo. Run with `pixi run test` from the repo root.

Everything that touches `tests/fixtures/` is a *parity* test: the expected
values were produced by iceberg-rust 0.10.1 (through iceberg-rs.mojo) or by
PyIceberg, never by this implementation. See `tests/fixtures/PROVENANCE.md`.
"""

from std.testing import TestSuite, assert_equal, assert_true, assert_false, assert_raises

from iceberg.json import parse_json, Json, substr
from iceberg.schema import Schema
from iceberg.metadata import TableMetadata, Snapshot, SnapshotRef
from iceberg.types import (
    TypeStore,
    P_INT,
    P_LONG,
    P_STRING,
    P_DATE,
    P_TIME,
    P_TIMESTAMP,
    P_TIMESTAMPTZ,
    P_DECIMAL,
    P_UUID,
    P_FIXED,
    P_BINARY,
    P_BOOLEAN,
    P_DOUBLE,
)
from iceberg.values import (
    Datum,
    compare,
    decimal_from_text,
    decimal_text,
    hex_bytes,
    hex_text,
    uuid_bytes,
    uuid_text,
    parse_iso,
    iso_text,
    civil_from_days,
    days_from_civil,
    datum_from_bytes_prim,
    datum_to_bytes,
    int64_to_be_twos,
    be_twos_to_int64,
)
from iceberg.expressions import (
    Expr,
    parse_filter,
    bind,
    rewrite_not,
    project_inclusive,
    project_strict,
    ManifestEvaluator,
    InclusiveMetricsEvaluator,
    ResidualEvaluator,
    FieldSummary,
    ColumnMetrics,
    OP_EQ,
    OP_LT_EQ,
    OP_GT_EQ,
    OP_IN,
    OP_TRUE,
    OP_FALSE,
    OP_NOT_EQ,
    op_name,
)
from iceberg.transforms import (
    Transform,
    parse_transform,
    bucket_of,
    iceberg_hash,
    truncate_codepoints,
    PartitionSpec,
    T_IDENTITY,
    T_BUCKET,
    T_TRUNCATE,
    T_YEAR,
    T_MONTH,
    T_DAY,
    T_HOUR,
    T_VOID,
    T_UNKNOWN,
)


comptime FIXTURES = String("tests/fixtures")


def read_file(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


# ══ JSON ════════════════════════════════════════════════════════════════════
def test_json_scalars() raises:
    var d = parse_json('{"a":9223372036854775807,"b":-9223372036854775808}')
    assert_equal(d.as_int(d.get(d.root, "a")), 9223372036854775807)
    assert_equal(d.as_int(d.get(d.root, "b")), -9223372036854775808)


def test_json_round_trip() raises:
    var texts = [
        String('{"a":1,"b":[1,2.5,true,null,"x"],"c":{"d":-0.5}}'),
        String("[]"),
        String("{}"),
        String('"\\u00e9\\ud83d\\ude00"'),
        String('{"k":"a\\"b\\\\c\\nd"}'),
    ]
    for k in range(len(texts)):
        var d = parse_json(texts[k])
        var again = parse_json(d.dump_root())
        assert_equal(d.dump_root(), again.dump_root())


def test_json_escapes() raises:
    var d = parse_json('{"k":"a\\"b\\\\c\\nd\\u0041"}')
    var want = String('a"b') + String("\\") + String("c\ndA")
    assert_equal(d.req_string(d.root, "k"), want)


def test_json_errors() raises:
    with assert_raises():
        _ = parse_json("{")
    with assert_raises():
        _ = parse_json("[1,]x")
    with assert_raises():
        _ = parse_json("tru")


# ══ types and schema ════════════════════════════════════════════════════════
def test_schema_nested_names() raises:
    var s = Schema.parse(
        '{"type":"struct","schema-id":3,"identifier-field-ids":[1],"fields":['
        '{"id":1,"name":"id","required":true,"type":"long"},'
        '{"id":2,"name":"d","required":false,"type":"decimal(9, 2)"},'
        '{"id":3,"name":"n","required":false,"type":{"type":"struct","fields":['
        '{"id":4,"name":"x","required":false,"type":"string"}]}},'
        '{"id":5,"name":"l","required":false,"type":{"type":"list",'
        '"element-id":6,"element":"int","element-required":true}},'
        '{"id":7,"name":"m","required":false,"type":{"type":"map","key-id":8,'
        '"key":"string","value-id":9,"value":"double","value-required":false}}]}'
    )
    assert_equal(s.schema_id, 3)
    assert_equal(s.find_by_name("n.x").id, 4)
    assert_equal(s.find_by_name("l.element").id, 6)
    assert_equal(s.find_by_name("m.key").id, 8)
    assert_equal(s.find_by_name("m.value").id, 9)
    assert_equal(s.name_of(4), "n.x")
    assert_equal(s.store.type_name(s.find_field(2).type), "decimal(9, 2)")
    assert_equal(s.identifier_field_ids[0], 1)
    assert_equal(s.highest_field_id(), 9)


def test_schema_all_primitives() raises:
    var names = [
        String("boolean"), String("int"), String("long"), String("float"),
        String("double"), String("date"), String("time"), String("timestamp"),
        String("timestamptz"), String("timestamp_ns"), String("timestamptz_ns"),
        String("string"), String("uuid"), String("binary"), String("unknown"),
        String("variant"), String("fixed[16]"), String("decimal(38, 10)"),
        String("geometry"), String("geometry(srid:3857)"), String("geography"),
        String("geography(srid:4269, vincenty)"),
    ]
    var store = TypeStore()
    for k in range(len(names)):
        var i = store.parse_primitive(names[k])
        assert_equal(store.type_name(i), names[k])


def test_schema_unknown_type_tolerated() raises:
    # The spec requires readers to tolerate types from a newer version.
    var s = Schema.parse(
        '{"type":"struct","schema-id":0,"fields":['
        '{"id":1,"name":"weird","required":false,"type":"quaternion"}]}'
    )
    assert_equal(s.store.type_name(s.find_field(1).type), "quaternion")


def test_schema_defaults_round_trip() raises:
    var text = String(
        '{"type":"struct","schema-id":0,"fields":['
        '{"id":1,"name":"a","required":false,"type":"int",'
        '"initial-default":7,"write-default":9,"doc":"hello"}]}'
    )
    var s = Schema.parse(text)
    var back = parse_json(s.to_json())
    var f = back.at(back.get(back.root, "fields"), 0)
    assert_equal(back.as_int(back.get(f, "initial-default")), 7)
    assert_equal(back.as_int(back.get(f, "write-default")), 9)
    assert_equal(back.req_string(f, "doc"), "hello")


def test_schema_select() raises:
    var s = Schema.parse(
        '{"type":"struct","schema-id":0,"identifier-field-ids":[1],"fields":['
        '{"id":1,"name":"a","required":true,"type":"long"},'
        '{"id":2,"name":"b","required":false,"type":"string"}]}'
    )
    var p = s.select([1])
    assert_equal(len(p.columns()), 1)
    assert_true(p.has_field(1))
    assert_false(p.has_field(2))
    assert_equal(len(p.identifier_field_ids), 1)


# ══ values ══════════════════════════════════════════════════════════════════
def test_decimal_round_trip() raises:
    var cases = [
        String("0.00"), String("14.20"), String("-14.20"), String("10.65"),
        String("-0.01"), String("99999999.99"),
    ]
    for k in range(len(cases)):
        var d = decimal_from_text(cases[k], 10, 2)
        assert_equal(decimal_text(d.b, 2), cases[k])


def test_decimal_128_bit() raises:
    # decimal(38, 10) needs more than 64 bits of unscaled value.
    var t = String("12345678901234567890.1234567890")
    var d = decimal_from_text(t, 38, 10)
    assert_equal(decimal_text(d.b, 10), t)
    var neg = decimal_from_text("-" + t, 38, 10)
    assert_equal(decimal_text(neg.b, 10), "-" + t)


def test_twos_complement_minimal() raises:
    assert_equal(hex_text(int64_to_be_twos(0)), "00")
    assert_equal(hex_text(int64_to_be_twos(1)), "01")
    assert_equal(hex_text(int64_to_be_twos(127)), "7f")
    assert_equal(hex_text(int64_to_be_twos(128)), "0080")
    assert_equal(hex_text(int64_to_be_twos(-1)), "ff")
    assert_equal(hex_text(int64_to_be_twos(-128)), "80")
    assert_equal(hex_text(int64_to_be_twos(-129)), "ff7f")
    assert_equal(hex_text(int64_to_be_twos(1420)), "058c")
    assert_equal(be_twos_to_int64(int64_to_be_twos(-9223372036854775808)), -9223372036854775808)


def test_uuid_text() raises:
    var u = String("f79c3e09-677c-4bbd-a479-3f349cb785e7")
    assert_equal(uuid_text(uuid_bytes(u)), u)
    assert_equal(hex_text(uuid_bytes(u)), "f79c3e09677c4bbda4793f349cb785e7")


def test_civil_days() raises:
    assert_equal(days_from_civil(1970, 1, 1), 0)
    assert_equal(days_from_civil(2017, 11, 16), 17486)
    assert_equal(days_from_civil(1969, 12, 31), -1)
    assert_equal(days_from_civil(1969, 1, 1), -365)
    var c = civil_from_days(-1)
    assert_equal(c[0], 1969)
    assert_equal(c[1], 12)
    assert_equal(c[2], 31)


def test_iso_round_trip() raises:
    assert_equal(parse_iso(P_DATE, "2017-11-16"), 17486)
    assert_equal(iso_text(P_DATE, 17486), "2017-11-16")
    assert_equal(parse_iso(P_TIME, "22:31:08"), 81068000000)
    assert_equal(parse_iso(P_TIMESTAMP, "2017-11-16T22:31:08"), 1510871468000000)
    assert_equal(parse_iso(P_TIMESTAMPTZ, "2017-11-16T14:31:08-08:00"), 1510871468000000)
    assert_equal(iso_text(P_TIMESTAMP, 1510871468000000), "2017-11-16T22:31:08")
    # Pre-epoch values must floor, not truncate toward zero.
    assert_equal(parse_iso(P_TIMESTAMP, "1969-12-31T23:59:59.999999"), -1)
    assert_equal(iso_text(P_TIMESTAMP, -1), "1969-12-31T23:59:59.999999")


def test_appendix_d_binary() raises:
    assert_equal(hex_text(datum_to_bytes(Datum.int_(1))), "01000000")
    assert_equal(hex_text(datum_to_bytes(Datum.long_(1))), "0100000000000000")
    assert_equal(hex_text(datum_to_bytes(Datum.double_(1.0))), "000000000000f03f")
    assert_equal(hex_text(datum_to_bytes(Datum.string_("iceberg"))), "69636562657267")
    assert_equal(datum_from_bytes_prim(P_INT, 0, 0, 0, hex_bytes("01000000")).i, 1)
    assert_equal(datum_from_bytes_prim(P_BOOLEAN, 0, 0, 0, hex_bytes("01")).i, 1)
    assert_equal(
        datum_from_bytes_prim(P_STRING, 0, 0, 0, hex_bytes("69636562657267")).s,
        "iceberg",
    )


def test_appendix_d_promoted_lengths() raises:
    """Bounds written before a promotion keep the *old* type's byte length."""
    # int -> long: 4 bytes for a long column.
    assert_equal(datum_from_bytes_prim(P_LONG, 0, 0, 0, hex_bytes("ffffffff")).i, -1)
    assert_equal(datum_from_bytes_prim(P_LONG, 0, 0, 0, hex_bytes("ffffffffffffffff")).i, -1)
    # float -> double: 4 bytes for a double column.
    var f = datum_from_bytes_prim(P_DOUBLE, 0, 0, 0, hex_bytes("0000803f"))
    assert_equal(f.f, 1.0)
    # date -> timestamp: 4 bytes of days become microseconds.
    var ts = datum_from_bytes_prim(P_TIMESTAMP, 0, 0, 0, hex_bytes("4e440000"))
    assert_equal(ts.i, 17486 * 86400000000)


def test_datum_compare() raises:
    assert_equal(compare(Datum.long_(1), Datum.long_(2)), -1)
    assert_equal(compare(Datum.long_(2), Datum.long_(2)), 0)
    assert_equal(compare(Datum.string_("a"), Datum.string_("b")), -1)
    # NaN sorts last; -0.0 equals 0.0.
    var nan = Datum.double_(Float64(0.0) / Float64(0.0))
    assert_equal(compare(nan, Datum.double_(1e300)), 1)
    assert_equal(compare(Datum.double_(-0.0), Datum.double_(0.0)), 0)
    # Decimals compare through their bytes, so 128-bit values work.
    var big = decimal_from_text("12345678901234567890.1234567890", 38, 10)
    var small = decimal_from_text("1.0000000000", 38, 10)
    assert_equal(compare(small, big), -1)
    assert_equal(compare(big, small), 1)


# ══ transforms ══════════════════════════════════════════════════════════════
def test_transform_parsing() raises:
    assert_equal(parse_transform("identity").kind, T_IDENTITY)
    assert_equal(parse_transform("bucket[16]").param, 16)
    assert_equal(parse_transform("truncate[4]").param, 4)
    assert_equal(parse_transform("year").kind, T_YEAR)
    assert_equal(parse_transform("void").kind, T_VOID)
    var u = parse_transform("chronomancy[3]")
    assert_equal(u.kind, T_UNKNOWN)
    assert_false(u.can_project())
    assert_equal(u.raw, "chronomancy[3]")


def test_truncate_codepoints() raises:
    # Truncation counts code points, not bytes.
    assert_equal(truncate_codepoints("iceberg", 3), "ice")
    assert_equal(truncate_codepoints("измерение", 3), "изм")
    assert_equal(truncate_codepoints("abc", 10), "abc")
    assert_equal(truncate_codepoints("abc", 0), "")


def datum_for(type_name: String, doc: Json, node: Int, int_node: Int) raises -> Datum:
    """Build a Datum from one transform-vector row."""
    if type_name == "int":
        return Datum.int_(doc.as_int(node))
    if type_name == "long":
        return Datum.long_(doc.as_int(node))
    if type_name == "string":
        return Datum.string_(doc.as_string(node))
    if type_name == "uuid":
        return Datum.uuid_(uuid_bytes(doc.as_string(node)))
    if type_name == "binary":
        return Datum.binary_(hex_bytes(doc.as_string(node)))
    if type_name.startswith("fixed"):
        return Datum.fixed_(hex_bytes(doc.as_string(node)))
    if type_name.startswith("decimal"):
        var ps = type_name.find("(")
        var comma = type_name.find(",")
        var close = type_name.find(")")
        var p = Int(String(substr(type_name, ps + 1, comma).strip()))
        var sc = Int(String(substr(type_name, comma + 1, close).strip()))
        return decimal_from_text(doc.as_string(node), p, sc)
    if type_name == "date":
        return Datum.integral(P_DATE, doc.as_int(int_node))
    if type_name == "time":
        return Datum.integral(P_TIME, doc.as_int(int_node))
    if type_name == "timestamp":
        return Datum.integral(P_TIMESTAMP, doc.as_int(int_node))
    if type_name == "timestamptz":
        return Datum.integral(P_TIMESTAMPTZ, doc.as_int(int_node))
    raise Error("test: unhandled vector type '" + type_name + "'")


def test_transform_vectors() raises:
    """Gate (c): every transform vector produced by PyIceberg + the spec."""
    var doc = parse_json(read_file(FIXTURES + "/transform_vectors.json"))
    var n = doc.size(doc.root)
    assert_true(n > 200, "expected the full vector set, got " + String(n))
    var checked = 0
    var hashes_checked = 0
    for k in range(n):
        var row = doc.at(doc.root, k)
        var tname = doc.req_string(row, "transform")
        var type_name = doc.req_string(row, "type")
        var t = parse_transform(tname)
        var v = datum_for(
            type_name, doc, doc.get(row, "input"), doc.get(row, "input_int")
        )
        # The published 32-bit hash, where the generator recorded one.
        var hnode = doc.get(row, "hash")
        if hnode >= 0 and not doc.is_null(hnode):
            assert_equal(
                Int64(Int(iceberg_hash(v))),
                doc.as_int(hnode),
                "hash mismatch for " + tname + " " + type_name,
            )
            hashes_checked += 1
        var out = t.apply(v)
        var want = doc.get(row, "output")
        var what = tname + " " + type_name + " #" + String(k)
        if t.kind == T_VOID:
            assert_false(out.valid, what)
        elif t.kind == T_BUCKET:
            assert_equal(out.i, doc.as_int(want), what)
        elif t.kind == T_IDENTITY:
            assert_equal(out.repr_(), v.repr_(), what)
        elif t.kind == T_TRUNCATE:
            if type_name == "int" or type_name == "long":
                assert_equal(out.i, doc.as_int(want), what)
            elif type_name.startswith("decimal"):
                assert_equal(decimal_text(out.b, out.scale), doc.as_string(want), what)
            elif type_name == "string":
                assert_equal(out.s, doc.as_string(want), what)
            else:
                assert_equal(hex_text(out.b), doc.as_string(want), what)
        else:
            assert_equal(out.i, doc.as_int(want), what)
        checked += 1
    print("    transform vectors:", checked, "checked,", hashes_checked, "with hashes")


def test_bucket_spec_vectors() raises:
    """Appendix B's own published test values, independent of the fixtures."""
    assert_equal(Int(iceberg_hash(Datum.int_(34))), 2017239379)
    assert_equal(Int(iceberg_hash(Datum.long_(34))), 2017239379)
    assert_equal(Int(iceberg_hash(Datum.string_("iceberg"))), 1210000089)
    assert_equal(
        Int(iceberg_hash(Datum.uuid_(uuid_bytes("f79c3e09-677c-4bbd-a479-3f349cb785e7")))),
        1488055340,
    )
    assert_equal(Int(iceberg_hash(Datum.binary_(hex_bytes("00010203")))), -188683207)
    assert_equal(Int(iceberg_hash(decimal_from_text("14.20", 9, 2))), -500754589)
    assert_equal(Int(iceberg_hash(Datum.integral(P_DATE, 17486))), -653330422)
    assert_equal(Int(iceberg_hash(Datum.integral(P_TIME, 81068000000))), -662762989)
    assert_equal(
        Int(iceberg_hash(Datum.integral(P_TIMESTAMP, 1510871468000000))), -2047944441
    )
    # (2017239379 & Integer.MAX_VALUE) % 16 == 3
    assert_equal(bucket_of(Datum.int_(34), 16), 3)
    assert_equal(bucket_of(Datum.string_("iceberg"), 16), 9)


def test_partition_spec_json() raises:
    var doc = parse_json(
        '{"spec-id":1,"fields":[{"source-id":2,"field-id":1000,'
        '"transform":"bucket[4]","name":"id_bucket"},'
        '{"source-id":3,"field-id":1001,"transform":"day","name":"ts_day"}]}'
    )
    var spec = PartitionSpec.from_json(doc, doc.root)
    assert_equal(spec.spec_id, 1)
    assert_equal(len(spec.fields), 2)
    assert_equal(spec.fields[0].transform.kind, T_BUCKET)
    assert_equal(spec.fields[0].transform.param, 4)
    assert_equal(spec.fields[1].transform.kind, T_DAY)
    var back = parse_json(spec.to_json())
    var s2 = PartitionSpec.from_json(back, back.root)
    assert_equal(s2.fields[1].name, "ts_day")


def test_partition_type() raises:
    var schema = Schema.parse(
        '{"type":"struct","schema-id":0,"fields":['
        '{"id":1,"name":"id","required":true,"type":"long"},'
        '{"id":2,"name":"ts","required":false,"type":"timestamp"},'
        '{"id":3,"name":"s","required":false,"type":"string"}]}'
    )
    var doc = parse_json(
        '{"spec-id":0,"fields":['
        '{"source-id":1,"field-id":1000,"transform":"bucket[4]","name":"b"},'
        '{"source-id":2,"field-id":1001,"transform":"day","name":"d"},'
        '{"source-id":3,"field-id":1002,"transform":"truncate[3]","name":"t"},'
        '{"source-id":1,"field-id":1003,"transform":"identity","name":"i"}]}'
    )
    var spec = PartitionSpec.from_json(doc, doc.root)
    var pt = spec.partition_type(schema)
    # bucket -> int, day -> date, truncate -> source type, identity -> source.
    assert_equal(pt.store.type_name(pt.find_field(1000).type), "int")
    assert_equal(pt.store.type_name(pt.find_field(1001).type), "date")
    assert_equal(pt.store.type_name(pt.find_field(1002).type), "string")
    assert_equal(pt.store.type_name(pt.find_field(1003).type), "long")



# ══ table metadata ══════════════════════════════════════════════════════════
comptime FIXTURE_TABLES = String(
    "unpartitioned,ident_part,bucket_part,day_part,trunc_part,evolved,deletes_v2"
)


def fixture_table_names() -> List[String]:
    var out = List[String]()
    var t = FIXTURE_TABLES
    var start = 0
    while True:
        var c = t.find(",", start)
        if c < 0:
            out.append(substr(t, start, t.byte_length()))
            break
        out.append(substr(t, start, c))
        start = c + 1
    return out^


def fixture_index() raises -> Json:
    return parse_json(read_file(FIXTURES + "/index.json"))


def current_metadata_path(idx: Json, table: String) raises -> String:
    var e = idx.get(idx.root, table)
    return (
        FIXTURES + "/" + table + "/metadata/"
        + idx.req_string(e, "current_metadata")
    )


def load_fixture_metadata(table: String) raises -> TableMetadata:
    var idx = fixture_index()
    var m = TableMetadata.parse(read_file(current_metadata_path(idx, table)))
    return m^


def metadata_diff(a: TableMetadata, b: TableMetadata) raises -> String:
    """First field on which two TableMetadata values differ, or "".

    This is the field-by-field comparison gate (a) asks for: `a` comes from the
    fixture file, `b` from the oracle's own rendering of the same table.
    """
    if a.format_version != b.format_version:
        return "format-version"
    if a.table_uuid != b.table_uuid:
        return "table-uuid"
    if a.location != b.location:
        return "location"
    if a.last_sequence_number != b.last_sequence_number:
        return "last-sequence-number"
    if a.last_updated_ms != b.last_updated_ms:
        return "last-updated-ms"
    if a.last_column_id != b.last_column_id:
        return "last-column-id"
    if a.current_schema_id != b.current_schema_id:
        return "current-schema-id"
    if len(a.schemas) != len(b.schemas):
        return "schemas (count)"
    for k in range(len(a.schemas)):
        if a.schemas[k].to_json() != b.schemas[k].to_json():
            return "schemas[" + String(k) + "]"
    if a.default_spec_id != b.default_spec_id:
        return "default-spec-id"
    if len(a.partition_specs) != len(b.partition_specs):
        return "partition-specs (count)"
    for k in range(len(a.partition_specs)):
        if a.partition_specs[k].to_json() != b.partition_specs[k].to_json():
            return "partition-specs[" + String(k) + "]"
    if a.last_partition_id != b.last_partition_id:
        return "last-partition-id"
    if a.default_sort_order_id != b.default_sort_order_id:
        return "default-sort-order-id"
    if len(a.sort_orders) != len(b.sort_orders):
        return "sort-orders (count)"
    for k in range(len(a.sort_orders)):
        if a.sort_orders[k].to_json() != b.sort_orders[k].to_json():
            return "sort-orders[" + String(k) + "]"
    if a.has_current_snapshot != b.has_current_snapshot:
        return "current-snapshot-id (presence)"
    if a.has_current_snapshot and a.current_snapshot_id != b.current_snapshot_id:
        return "current-snapshot-id"
    if len(a.snapshots) != len(b.snapshots):
        return "snapshots (count)"
    for k in range(len(a.snapshots)):
        if a.snapshots[k].to_json() != b.snapshots[k].to_json():
            return "snapshots[" + String(k) + "]"
    if len(a.snapshot_log) != len(b.snapshot_log):
        return "snapshot-log (count)"
    for k in range(len(a.snapshot_log)):
        if (
            a.snapshot_log[k].snapshot_id != b.snapshot_log[k].snapshot_id
            or a.snapshot_log[k].timestamp_ms != b.snapshot_log[k].timestamp_ms
        ):
            return "snapshot-log[" + String(k) + "]"
    if len(a.refs) != len(b.refs):
        return "refs (count)"
    for k in range(len(a.refs)):
        var j = b.ref_index(a.refs[k].name)
        if j < 0 or a.refs[k].to_json() != b.refs[j].to_json():
            return "refs['" + a.refs[k].name + "']"
    if len(a.properties) != len(b.properties):
        return "properties (count)"
    for e in a.properties.items():
        if e.key not in b.properties:
            return "properties['" + e.key + "'] (missing)"
        if b.properties[e.key] != e.value:
            return "properties['" + e.key + "']"
    if len(a.statistics) != len(b.statistics):
        return "statistics (count)"
    if len(a.partition_statistics) != len(b.partition_statistics):
        return "partition-statistics (count)"
    if a.has_next_row_id != b.has_next_row_id:
        return "next-row-id (presence)"
    if a.has_next_row_id and a.next_row_id != b.next_row_id:
        return "next-row-id"
    if len(a.encryption_keys) != len(b.encryption_keys):
        return "encryption-keys (count)"
    return ""


def test_metadata_round_trip() raises:
    """Gate (a), first half: parse -> serialize -> parse is lossless."""
    var tables = fixture_table_names()
    var idx = fixture_index()
    var n = 0
    for k in range(len(tables)):
        var path = current_metadata_path(idx, tables[k])
        var m1 = TableMetadata.parse(read_file(path))
        var m2 = TableMetadata.parse(m1.to_json())
        var d = metadata_diff(m1, m2)
        assert_equal(d, "", tables[k] + ": round trip lost " + d)
        # And a second pass is byte-identical, so serialization is stable.
        assert_equal(m1.to_json(), m2.to_json(), tables[k] + ": unstable output")
        n += 1
    print("    metadata round-trip:", n, "tables")


def test_metadata_matches_oracle() raises:
    """Gate (a), second half: field-by-field against the oracle rendering."""
    var tables = fixture_table_names()
    var idx = fixture_index()
    var n = 0
    for k in range(len(tables)):
        var mine = TableMetadata.parse(
            read_file(current_metadata_path(idx, tables[k]))
        )
        var theirs = TableMetadata.parse(
            read_file(FIXTURES + "/" + tables[k] + "/oracle/metadata.json")
        )
        var d = metadata_diff(mine, theirs)
        assert_equal(d, "", tables[k] + ": differs from the oracle at " + d)
        n += 1
    print("    metadata vs oracle:", n, "tables, field by field")


def test_metadata_full_field_coverage() raises:
    """Every field of the spec's table-metadata table, on a synthetic v3 file.

    The fixtures are all v2, so the v3-only fields (`next-row-id`,
    `encryption-keys`, snapshot `first-row-id`/`added-rows`/`key-id`) and the
    optional statistics blocks need a hand-written case.
    """
    var text = String(
        '{"format-version":3,"table-uuid":"9c12d441-03fe-4693-9a96-a0705ddf69c1",'
        '"location":"s3://b/t","last-sequence-number":34,'
        '"last-updated-ms":1602638573590,"last-column-id":3,'
        '"current-schema-id":1,"schemas":[{"type":"struct","schema-id":1,'
        '"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}],'
        '"default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":['
        '{"source-id":1,"field-id":1000,"transform":"identity","name":"x"}]}],'
        '"last-partition-id":1000,"default-sort-order-id":3,'
        '"sort-orders":[{"order-id":3,"fields":[{"transform":"identity",'
        '"source-id":1,"direction":"desc","null-order":"nulls-last"}]}],'
        '"properties":{"read.split.target.size":"134217728"},'
        '"current-snapshot-id":3055729675574597004,'
        '"refs":{"main":{"snapshot-id":3055729675574597004,"type":"branch"},'
        '"tag1":{"snapshot-id":3051729675574597004,"type":"tag",'
        '"max-ref-age-ms":10000000}},'
        '"snapshots":[{"snapshot-id":3051729675574597004,"timestamp-ms":1515100955770,'
        '"sequence-number":0,"summary":{"operation":"append"},'
        '"manifest-list":"s3://b/t/m1.avro","first-row-id":0,"added-rows":100},'
        '{"snapshot-id":3055729675574597004,"parent-snapshot-id":3051729675574597004,'
        '"timestamp-ms":1555100955770,"sequence-number":1,'
        '"summary":{"operation":"append"},"manifest-list":"s3://b/t/m2.avro",'
        '"schema-id":1,"first-row-id":100,"added-rows":50,"key-id":"k1"}],'
        '"statistics":[{"snapshot-id":3055729675574597004,'
        '"statistics-path":"s3://b/t/s.puffin","file-size-in-bytes":413,'
        '"file-footer-size-in-bytes":42,"blob-metadata":[{"type":"ndv",'
        '"snapshot-id":3055729675574597004,"sequence-number":1,"fields":[1]}]}],'
        '"partition-statistics":[{"snapshot-id":3055729675574597004,'
        '"statistics-path":"s3://b/t/p.parquet","file-size-in-bytes":43}],'
        '"snapshot-log":[{"snapshot-id":3051729675574597004,"timestamp-ms":1515100955770},'
        '{"snapshot-id":3055729675574597004,"timestamp-ms":1555100955770}],'
        '"metadata-log":[{"metadata-file":"s3://b/t/m/v1.json","timestamp-ms":1515100}],'
        '"next-row-id":150,"encryption-keys":[{"key-id":"k1",'
        '"encrypted-key-metadata":"AAAA","encrypted-by-id":"kek"}],'
        '"unknown-future-field":{"a":1}}'
    )
    var m = TableMetadata.parse(text)
    assert_equal(m.format_version, 3)
    assert_equal(m.table_uuid, "9c12d441-03fe-4693-9a96-a0705ddf69c1")
    assert_equal(m.location, "s3://b/t")
    assert_equal(m.last_sequence_number, 34)
    assert_equal(m.last_updated_ms, 1602638573590)
    assert_equal(m.last_column_id, 3)
    assert_equal(m.current_schema_id, 1)
    assert_equal(m.default_spec_id, 0)
    assert_equal(m.last_partition_id, 1000)
    assert_equal(m.default_sort_order_id, 3)
    assert_equal(m.property_("read.split.target.size", ""), "134217728")
    assert_equal(m.current_snapshot_id, 3055729675574597004)
    assert_equal(len(m.snapshots), 2)
    assert_equal(len(m.refs), 2)
    assert_equal(len(m.statistics), 1)
    assert_equal(len(m.statistics[0].blob_metadata), 1)
    assert_equal(m.statistics[0].blob_metadata[0].type, "ndv")
    assert_equal(len(m.partition_statistics), 1)
    assert_equal(len(m.snapshot_log), 2)
    assert_equal(len(m.metadata_log), 1)
    assert_true(m.has_next_row_id)
    assert_equal(m.next_row_id, 150)
    assert_equal(len(m.encryption_keys), 1)
    assert_equal(m.encryption_keys[0].encrypted_key_metadata, "AAAA")
    assert_equal(m.encryption_keys[0].encrypted_by_id, "kek")
    # v3 snapshot fields.
    var cur = m.current_snapshot()
    assert_true(cur.has_first_row_id)
    assert_equal(cur.first_row_id, 100)
    assert_equal(cur.added_rows, 50)
    assert_equal(cur.key_id, "k1")
    # Unknown top-level keys survive the round trip.
    assert_equal(len(m.extra_keys), 1)
    var back = TableMetadata.parse(m.to_json())
    assert_equal(metadata_diff(m, back), "")
    var bd = parse_json(back.to_json())
    assert_true(bd.has(bd.root, "unknown-future-field"))


def test_metadata_v1_forms() raises:
    """v1's singular `schema` and bare `partition-spec` array."""
    var text = String(
        '{"format-version":1,"table-uuid":"1517a9b0-0000-0000-0000-000000000000",'
        '"location":"file:///t","last-updated-ms":1,"last-column-id":2,'
        '"schema":{"type":"struct","schema-id":7,"fields":['
        '{"id":1,"name":"a","required":true,"type":"int"},'
        '{"id":2,"name":"b","required":false,"type":"string"}]},'
        '"partition-spec":[{"source-id":2,"transform":"identity","name":"b"},'
        '{"source-id":1,"transform":"bucket[8]","name":"a_bucket"}],'
        '"properties":{},"current-snapshot-id":-1,"snapshots":[]}'
    )
    var m = TableMetadata.parse(text)
    assert_equal(m.format_version, 1)
    assert_equal(len(m.schemas), 1)
    assert_equal(m.current_schema_id, 7)
    assert_equal(len(m.partition_specs), 1)
    # v1 assigned partition field ids sequentially from 1000.
    assert_equal(m.partition_specs[0].fields[0].field_id, 1000)
    assert_equal(m.partition_specs[0].fields[1].field_id, 1001)
    assert_equal(m.last_partition_id, 1001)
    assert_false(m.has_current_snapshot)
    # A v1 file with no sort orders gets the reserved unsorted order 0.
    assert_equal(len(m.sort_orders), 1)
    assert_equal(m.sort_orders[0].order_id, 0)


def test_metadata_rejects_future_version() raises:
    with assert_raises():
        _ = TableMetadata.parse(
            '{"format-version":9,"location":"x","last-updated-ms":0,'
            '"last-column-id":0,"schemas":[],"current-schema-id":0}'
        )


def test_snapshot_selection() raises:
    """Gate (b): current / by-id / by-ref / as-of against the oracle."""
    var tables = fixture_table_names()
    var idx = fixture_index()
    var checked = 0
    for k in range(len(tables)):
        var table = tables[k]
        var m = load_fixture_metadata(table)
        var oracle = parse_json(
            read_file(FIXTURES + "/" + table + "/oracle/snapshots.json")
        )
        var n = oracle.size(oracle.root)
        assert_equal(
            len(m.snapshots), n, table + ": snapshot count"
        )
        # The oracle lists snapshots oldest first.
        var last_id: Int64 = 0
        for j in range(n):
            var o = oracle.at(oracle.root, j)
            var id = oracle.req_int(o, "snapshot-id")
            var s = m.snapshot_by_id(id)
            assert_equal(
                s.sequence_number,
                oracle.req_int(o, "sequence-number"),
                table + ": sequence-number of " + String(id),
            )
            assert_equal(
                s.timestamp_ms,
                oracle.req_int(o, "timestamp-ms"),
                table + ": timestamp-ms of " + String(id),
            )
            assert_equal(
                s.manifest_list,
                oracle.req_string(o, "manifest-list"),
                table + ": manifest-list of " + String(id),
            )
            assert_equal(
                s.operation(),
                oracle.req_string(o, "operation"),
                table + ": operation of " + String(id),
            )
            var p = oracle.get(o, "parent-snapshot-id")
            if p >= 0 and not oracle.is_null(p):
                assert_true(s.has_parent, table + ": missing parent")
                assert_equal(
                    s.parent_snapshot_id,
                    oracle.as_int(p),
                    table + ": parent of " + String(id),
                )
            else:
                assert_false(s.has_parent, table + ": unexpected parent")
            # as-of at this snapshot's own timestamp must select it.
            var asof = m.snapshot_as_of(s.timestamp_ms)
            assert_equal(
                asof.snapshot_id, id, table + ": as-of at " + String(s.timestamp_ms)
            )
            last_id = id
            checked += 1
        # The newest snapshot is the current one and the head of `main`.
        var cur = m.current_snapshot()
        assert_equal(cur.snapshot_id, last_id, table + ": current snapshot")
        var main = m.snapshot_for_ref("main")
        assert_equal(main.snapshot_id, last_id, table + ": main branch head")
        # A timestamp before the first snapshot has no answer.
        with assert_raises():
            _ = m.snapshot_as_of(m.snapshots[0].timestamp_ms - 1000000)
        # An unknown ref and an unknown id are errors, not silent nulls.
        with assert_raises():
            _ = m.snapshot_for_ref("no-such-branch")
        with assert_raises():
            _ = m.snapshot_by_id(1)
    print("    snapshot selection:", checked, "snapshots across", len(tables), "tables")


def test_snapshot_ref_and_schema_selection() raises:
    var m = load_fixture_metadata("evolved")
    # Schema evolution: several schemas, and each snapshot names the one that
    # was current when it was written.
    assert_true(len(m.schemas) >= 2, "evolved should have several schemas")
    for k in range(len(m.snapshots)):
        var s = m.schema_for_snapshot(m.snapshots[k])
        assert_true(len(s.columns()) > 0)
    var cur = m.schema()
    assert_equal(cur.schema_id, m.current_schema_id)



# ══ expressions ═════════════════════════════════════════════════════════════
comptime FILTER_SCHEMA = String(
    '{"type":"struct","schema-id":0,"fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"region","required":true,"type":"string"},'
    '{"id":3,"name":"amount","required":false,"type":"double"},'
    '{"id":4,"name":"ts","required":false,"type":"timestamp"},'
    '{"id":5,"name":"ok","required":false,"type":"boolean"},'
    '{"id":6,"name":"cnt","required":false,"type":"int"},'
    '{"id":7,"name":"d","required":false,"type":"decimal(9, 2)"}]}'
)


def bound_filter(text: String) raises -> Expr:
    var schema = Schema.parse(FILTER_SCHEMA)
    return bind(parse_filter(text), schema)


def test_filter_dsl_parsing() raises:
    var cases = [
        String('["true"]'), String('["false"]'),
        String('["=","region","eu"]'), String('["!=","id",3]'),
        String('["<","id",5]'), String('["<=","id",5]'),
        String('[">","id",5]'), String('[">=","id",5]'),
        String('["is-null","amount"]'), String('["not-null","amount"]'),
        String('["is-nan","amount"]'), String('["not-nan","amount"]'),
        String('["in","region",["eu","us"]]'),
        String('["not-in","region",["eu","us"]]'),
        String('["starts-with","region","e"]'),
        String('["not-starts-with","region","e"]'),
        String('["and",[">","id",1],["is-null","amount"]]'),
        String('["or",[">","id",1],["<","id",0]]'),
        String('["not",["=","region","eu"]]'),
        String('["and",[">","id",1],["<","id",9],["=","region","eu"]]'),
    ]
    for k in range(len(cases)):
        var e = parse_filter(cases[k])
        assert_true(e.root >= 0, "failed to parse " + cases[k])
        _ = bound_filter(cases[k])
    with assert_raises():
        _ = parse_filter('["nope","id",1]')
    with assert_raises():
        _ = parse_filter("[]")


def test_literal_typing() raises:
    """A literal is typed by the column, not by its JSON shape."""
    var e = bound_filter('["=","id",3]')
    assert_equal(e.nodes[e.root].lits[0].kind, P_LONG)
    var f = bound_filter('["=","cnt",3]')
    assert_equal(f.nodes[f.root].lits[0].kind, P_INT)
    # ISO strings on temporal columns.
    var g = bound_filter('["<","ts","2017-11-16T22:31:08"]')
    assert_equal(g.nodes[g.root].lits[0].i, 1510871468000000)
    # and the raw integer, which the oracle DSL also allows.
    var h = bound_filter('["<","ts",1510871468000000]')
    assert_equal(h.nodes[h.root].lits[0].i, 1510871468000000)
    # Decimal text at the column's scale.
    var i = bound_filter('["=","d","14.20"]')
    assert_equal(i.nodes[i.root].lits[0].i, 1420)


def test_bind_folds_impossible_predicates() raises:
    # `region` is required, so it is never null.
    var a = bound_filter('["is-null","region"]')
    assert_equal(a.nodes[a.root].op, OP_FALSE)
    var b = bound_filter('["not-null","region"]')
    assert_equal(b.nodes[b.root].op, OP_TRUE)
    # `id` is a long, so it is never NaN.
    var c = bound_filter('["is-nan","id"]')
    assert_equal(c.nodes[c.root].op, OP_FALSE)
    # A column that is not in the schema can never match...
    var d = bound_filter('["=","nope","x"]')
    assert_equal(d.nodes[d.root].op, OP_FALSE)
    # ...but reads as all-null.
    var e = bound_filter('["is-null","nope"]')
    assert_equal(e.nodes[e.root].op, OP_TRUE)
    # A one-element `in` collapses to `=`.
    var f = bound_filter('["in","region",["eu"]]')
    assert_equal(f.nodes[f.root].op, OP_EQ)
    # starts-with on a non-string column is an error, not a silent mismatch.
    with assert_raises():
        _ = bound_filter('["starts-with","id","x"]')


def test_rewrite_not() raises:
    var e = rewrite_not(bound_filter('["not",["=","region","eu"]]'))
    assert_equal(e.nodes[e.root].op, OP_NOT_EQ)
    var f = rewrite_not(
        bound_filter('["not",["and",[">","id",1],["<","id",9]]]')
    )
    # not(a and b) becomes (not a) or (not b).
    assert_equal(op_name(f.nodes[f.root].op), "or")
    var g = rewrite_not(bound_filter('["not",["not",["=","region","eu"]]]'))
    assert_equal(g.nodes[g.root].op, OP_EQ)


def spec_from(text: String) raises -> PartitionSpec:
    var d = parse_json(text)
    return PartitionSpec.from_json(d, d.root)


def test_inclusive_projection_identity() raises:
    var schema = Schema.parse(FILTER_SCHEMA)
    var spec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":2,"field-id":1000,'
        '"transform":"identity","name":"region"}]}'
    )
    var p = project_inclusive(
        rewrite_not(bound_filter('["=","region","eu"]')), spec, schema
    )
    assert_equal(p.nodes[p.root].op, OP_EQ)
    assert_equal(p.nodes[p.root].field_id, 1000)
    assert_equal(p.nodes[p.root].lits[0].s, "eu")
    # A predicate on a column that is not a partition source says nothing.
    var q = project_inclusive(
        rewrite_not(bound_filter('[">","id",3]')), spec, schema
    )
    assert_equal(q.nodes[q.root].op, OP_TRUE)


def test_inclusive_projection_bucket() raises:
    var schema = Schema.parse(FILTER_SCHEMA)
    var spec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,'
        '"transform":"bucket[4]","name":"id_bucket"}]}'
    )
    # Equality survives bucketing...
    var p = project_inclusive(
        rewrite_not(bound_filter('["=","id",34]')), spec, schema
    )
    assert_equal(p.nodes[p.root].op, OP_EQ)
    assert_equal(p.nodes[p.root].lits[0].i, Int64(bucket_of(Datum.long_(34), 4)))
    # ...as does `in`, mapped to the set of buckets.
    var q = project_inclusive(
        rewrite_not(bound_filter('["in","id",[1,4,7]]')), spec, schema
    )
    assert_equal(q.nodes[q.root].op, OP_IN)
    assert_true(len(q.nodes[q.root].lits) <= 3)
    # Ordering does not: bucketing is not monotonic.
    var r = project_inclusive(
        rewrite_not(bound_filter('[">","id",3]')), spec, schema
    )
    assert_equal(r.nodes[r.root].op, OP_TRUE)


def test_inclusive_projection_truncate() raises:
    var schema = Schema.parse(FILTER_SCHEMA)
    var spec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":2,"field-id":1000,'
        '"transform":"truncate[3]","name":"region_trunc"}]}'
    )
    # A prefix no longer than the width stays a prefix predicate.
    var p = project_inclusive(
        rewrite_not(bound_filter('["starts-with","region","eu"]')), spec, schema
    )
    assert_equal(op_name(p.nodes[p.root].op), "starts-with")
    # A longer prefix becomes equality on its own truncation.
    var q = project_inclusive(
        rewrite_not(bound_filter('["starts-with","region","europe"]')),
        spec, schema,
    )
    assert_equal(q.nodes[q.root].op, OP_EQ)
    assert_equal(q.nodes[q.root].lits[0].s, "eur")
    # Truncation preserves order, so `<` becomes `<=` on the truncation.
    var r = project_inclusive(
        rewrite_not(bound_filter('["<","region","euxyz"]')), spec, schema
    )
    assert_equal(r.nodes[r.root].op, OP_LT_EQ)


def test_inclusive_projection_time() raises:
    var schema = Schema.parse(FILTER_SCHEMA)
    var spec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":4,"field-id":1000,'
        '"transform":"day","name":"ts_day"}]}'
    )
    var p = project_inclusive(
        rewrite_not(bound_filter('[">=","ts","2017-11-16T00:00:00"]')),
        spec, schema,
    )
    assert_equal(p.nodes[p.root].op, OP_GT_EQ)
    assert_equal(p.nodes[p.root].lits[0].i, 17486)
    assert_equal(p.nodes[p.root].lits[0].kind, P_DATE)
    # `<` on a day-partitioned column bounds the day of the value one micro
    # earlier, which is still the same day here.
    var q = project_inclusive(
        rewrite_not(bound_filter('["<","ts","2017-11-17T00:00:00"]')),
        spec, schema,
    )
    assert_equal(q.nodes[q.root].op, OP_LT_EQ)
    assert_equal(q.nodes[q.root].lits[0].i, 17486)


def test_projection_ignores_unknown_transform() raises:
    """v3 requires readers to ignore partition fields they cannot interpret."""
    var schema = Schema.parse(FILTER_SCHEMA)
    var spec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":2,"field-id":1000,'
        '"transform":"chronomancy[7]","name":"weird"}]}'
    )
    var p = project_inclusive(
        rewrite_not(bound_filter('["=","region","eu"]')), spec, schema
    )
    assert_equal(p.nodes[p.root].op, OP_TRUE)


def test_strict_projection() raises:
    var schema = Schema.parse(FILTER_SCHEMA)
    var spec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":2,"field-id":1000,'
        '"transform":"identity","name":"region"}]}'
    )
    # Identity: every row of the partition matches exactly when the partition
    # value does.
    var p = project_strict(
        rewrite_not(bound_filter('["=","region","eu"]')), spec, schema
    )
    assert_equal(p.nodes[p.root].op, OP_EQ)
    # Bucketing can never guarantee a whole partition matches.
    var bspec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,'
        '"transform":"bucket[4]","name":"b"}]}'
    )
    var q = project_strict(
        rewrite_not(bound_filter('["=","id",34]')), bspec, schema
    )
    assert_equal(q.nodes[q.root].op, OP_FALSE)


def summary(var lo: List[UInt8], var hi: List[UInt8], nulls: Bool) -> FieldSummary:
    return FieldSummary(nulls, False, False, lo^, True, hi^, True)


def test_manifest_evaluator() raises:
    var schema = Schema.parse(FILTER_SCHEMA)
    var spec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":2,"field-id":1000,'
        '"transform":"identity","name":"region"}]}'
    )
    # This manifest holds partitions from "eu" through "us".
    var s = List[FieldSummary]()
    s.append(summary(hex_bytes("6575"), hex_bytes("7573"), False))
    var keep = ManifestEvaluator(bound_filter('["=","region","eu"]'), spec, schema)
    assert_true(keep.eval(s))
    var drop = ManifestEvaluator(bound_filter('["=","region","zz"]'), spec, schema)
    assert_false(drop.eval(s))
    var below = ManifestEvaluator(bound_filter('["<","region","aa"]'), spec, schema)
    assert_false(below.eval(s))
    var above = ManifestEvaluator(bound_filter('[">","region","zz"]'), spec, schema)
    assert_false(above.eval(s))
    var isnull = ManifestEvaluator(bound_filter('["is-null","amount"]'), spec, schema)
    assert_true(isnull.eval(s), "a non-partition predicate must not prune")
    # A summary that says the field has no nulls kills an is-null on it.
    var pspec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":3,"field-id":1000,'
        '"transform":"identity","name":"amount"}]}'
    )
    var s2 = List[FieldSummary]()
    s2.append(
        summary(
            hex_bytes("0000000000001a40"), hex_bytes("0000000000001a40"), False
        )
    )
    var nulls = ManifestEvaluator(bound_filter('["is-null","amount"]'), pspec, schema)
    assert_false(nulls.eval(s2))


def metric(
    field_id: Int, lo: String, hi: String, values: Int64, nulls: Int64
) raises -> ColumnMetrics:
    var lob = hex_bytes(lo)
    var hib = hex_bytes(hi)
    return ColumnMetrics(
        field_id, values, True, nulls, True, 0, False,
        lob^, True, hib^, True,
    )


def test_inclusive_metrics_evaluator() raises:
    var schema = Schema.parse(FILTER_SCHEMA)
    # id in [1, 5]; amount all null.
    var m = List[ColumnMetrics]()
    m.append(metric(1, "0100000000000000", "0500000000000000", 5, 0))
    m.append(ColumnMetrics(3, 5, True, 5, True, 0, False, [], False, [], False))
    var e1 = InclusiveMetricsEvaluator(bound_filter('["=","id",3]'), schema)
    assert_true(e1.eval(5, m))
    var e2 = InclusiveMetricsEvaluator(bound_filter('["=","id",9]'), schema)
    assert_false(e2.eval(5, m))
    var e3 = InclusiveMetricsEvaluator(bound_filter('[">","id",5]'), schema)
    assert_false(e3.eval(5, m))
    var e4 = InclusiveMetricsEvaluator(bound_filter('[">=","id",5]'), schema)
    assert_true(e4.eval(5, m))
    var e5 = InclusiveMetricsEvaluator(bound_filter('["<","id",1]'), schema)
    assert_false(e5.eval(5, m))
    var e6 = InclusiveMetricsEvaluator(bound_filter('["in","id",[7,8,9]]'), schema)
    assert_false(e6.eval(5, m))
    var e7 = InclusiveMetricsEvaluator(bound_filter('["in","id",[7,3]]'), schema)
    assert_true(e7.eval(5, m))
    # amount is entirely null.
    var e8 = InclusiveMetricsEvaluator(bound_filter('["not-null","amount"]'), schema)
    assert_false(e8.eval(5, m))
    var e9 = InclusiveMetricsEvaluator(bound_filter('["is-null","amount"]'), schema)
    assert_true(e9.eval(5, m))
    var e10 = InclusiveMetricsEvaluator(bound_filter('[">","amount",1.0]'), schema)
    assert_false(e10.eval(5, m), "an all-null column matches no value predicate")
    # An empty file matches nothing.
    assert_false(e1.eval(0, m))


def test_residual_evaluator() raises:
    var schema = Schema.parse(FILTER_SCHEMA)
    var spec = spec_from(
        '{"spec-id":0,"fields":[{"source-id":2,"field-id":1000,'
        '"transform":"identity","name":"region"}]}'
    )
    var r = ResidualEvaluator(bound_filter('["=","region","eu"]'), spec, schema)
    # In the "eu" partition every row matches, so nothing is left to check.
    var eu = List[Datum]()
    eu.append(Datum.string_("eu"))
    assert_true(r.selects(eu))
    var res = r.residual_for(eu)
    assert_true(res.is_true(res.root))
    # The "us" partition is excluded outright.
    var us = List[Datum]()
    us.append(Datum.string_("us"))
    assert_false(r.selects(us))
    # A filter that partitioning cannot decide survives into the residual.
    var r2 = ResidualEvaluator(bound_filter('[">","id",3]'), spec, schema)
    assert_true(r2.selects(eu))
    var res2 = r2.residual_for(eu)
    assert_false(res2.is_true(res2.root))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
