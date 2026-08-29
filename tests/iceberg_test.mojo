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


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
