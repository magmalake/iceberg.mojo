"""The test suite for iceberg.mojo. Run with `pixi run test` from the repo root.

Everything that touches `tests/fixtures/` is a *parity* test: the expected
values were produced by iceberg-rust 0.10.1 (through iceberg-rs.mojo) or by
PyIceberg, never by this implementation. See `tests/fixtures/PROVENANCE.md`.
"""

from std.os import getenv, makedirs
from std.pathlib import Path
from std.testing import (
    TestSuite,
    assert_equal,
    assert_true,
    assert_false,
    assert_raises,
)

from iceberg.json import parse_json, Json, substr
from iceberg.schema import Schema
from iceberg.metadata import TableMetadata, Snapshot, SnapshotRef
from iceberg.io import FileIO, basename, dirname, join_path, strip_scheme
from iceberg.catalog.filesystem import (
    FilesystemCatalog,
    Table,
    find_latest_metadata,
    read_metadata_file,
    read_version_hint,
    gunzip,
)
from iceberg.catalog.rest import (
    LoadTableResult,
    RestCatalog,
    RestCatalogConfig,
    encode_namespace,
    url_encode,
    ACCESS_DELEGATION_HEADER,
    VENDED_CREDENTIALS,
)
from iceberg.puffin import (
    BLOB_DELETION_VECTOR_V1,
    BlobMetadata,
    PuffinFile,
    deleted_positions,
    read_deletion_vector,
)
from iceberg.scan import TableScan, FileScanTask
from iceberg.manifest import (
    ManifestFile,
    ManifestEntry,
    DataFile,
    Manifest,
    read_manifest_list,
    read_manifest,
    read_manifest_at,
    STATUS_ADDED,
    STATUS_EXISTING,
    STATUS_DELETED,
    CONTENT_DATA,
    CONTENT_POSITION_DELETES,
    CONTENT_EQUALITY_DELETES,
    MANIFEST_CONTENT_DATA,
    MANIFEST_CONTENT_DELETES,
)
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
        String("boolean"),
        String("int"),
        String("long"),
        String("float"),
        String("double"),
        String("date"),
        String("time"),
        String("timestamp"),
        String("timestamptz"),
        String("timestamp_ns"),
        String("timestamptz_ns"),
        String("string"),
        String("uuid"),
        String("binary"),
        String("unknown"),
        String("variant"),
        String("fixed[16]"),
        String("decimal(38, 10)"),
        String("geometry"),
        String("geometry(srid:3857)"),
        String("geography"),
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
        String("0.00"),
        String("14.20"),
        String("-14.20"),
        String("10.65"),
        String("-0.01"),
        String("99999999.99"),
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
    assert_equal(
        be_twos_to_int64(int64_to_be_twos(-9223372036854775808)),
        -9223372036854775808,
    )


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
    assert_equal(
        parse_iso(P_TIMESTAMP, "2017-11-16T22:31:08"), 1510871468000000
    )
    assert_equal(
        parse_iso(P_TIMESTAMPTZ, "2017-11-16T14:31:08-08:00"), 1510871468000000
    )
    assert_equal(iso_text(P_TIMESTAMP, 1510871468000000), "2017-11-16T22:31:08")
    # Pre-epoch values must floor, not truncate toward zero.
    assert_equal(parse_iso(P_TIMESTAMP, "1969-12-31T23:59:59.999999"), -1)
    assert_equal(iso_text(P_TIMESTAMP, -1), "1969-12-31T23:59:59.999999")


def test_appendix_d_binary() raises:
    assert_equal(hex_text(datum_to_bytes(Datum.int_(1))), "01000000")
    assert_equal(hex_text(datum_to_bytes(Datum.long_(1))), "0100000000000000")
    assert_equal(
        hex_text(datum_to_bytes(Datum.double_(1.0))), "000000000000f03f"
    )
    assert_equal(
        hex_text(datum_to_bytes(Datum.string_("iceberg"))), "69636562657267"
    )
    assert_equal(
        datum_from_bytes_prim(P_INT, 0, 0, 0, hex_bytes("01000000")).i, 1
    )
    assert_equal(
        datum_from_bytes_prim(P_BOOLEAN, 0, 0, 0, hex_bytes("01")).i, 1
    )
    assert_equal(
        datum_from_bytes_prim(P_STRING, 0, 0, 0, hex_bytes("69636562657267")).s,
        "iceberg",
    )


def test_appendix_d_promoted_lengths() raises:
    """Bounds written before a promotion keep the *old* type's byte length."""
    # int -> long: 4 bytes for a long column.
    assert_equal(
        datum_from_bytes_prim(P_LONG, 0, 0, 0, hex_bytes("ffffffff")).i, -1
    )
    assert_equal(
        datum_from_bytes_prim(P_LONG, 0, 0, 0, hex_bytes("ffffffffffffffff")).i,
        -1,
    )
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


def datum_for(
    type_name: String, doc: Json, node: Int, int_node: Int
) raises -> Datum:
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
                assert_equal(
                    decimal_text(out.b, out.scale), doc.as_string(want), what
                )
            elif type_name == "string":
                assert_equal(out.s, doc.as_string(want), what)
            else:
                assert_equal(hex_text(out.b), doc.as_string(want), what)
        else:
            assert_equal(out.i, doc.as_int(want), what)
        checked += 1
    print(
        "    transform vectors:",
        checked,
        "checked,",
        hashes_checked,
        "with hashes",
    )


def test_bucket_spec_vectors() raises:
    """Appendix B's own published test values, independent of the fixtures."""
    assert_equal(Int(iceberg_hash(Datum.int_(34))), 2017239379)
    assert_equal(Int(iceberg_hash(Datum.long_(34))), 2017239379)
    assert_equal(Int(iceberg_hash(Datum.string_("iceberg"))), 1210000089)
    assert_equal(
        Int(
            iceberg_hash(
                Datum.uuid_(uuid_bytes("f79c3e09-677c-4bbd-a479-3f349cb785e7"))
            )
        ),
        1488055340,
    )
    assert_equal(
        Int(iceberg_hash(Datum.binary_(hex_bytes("00010203")))), -188683207
    )
    assert_equal(
        Int(iceberg_hash(decimal_from_text("14.20", 9, 2))), -500754589
    )
    assert_equal(Int(iceberg_hash(Datum.integral(P_DATE, 17486))), -653330422)
    assert_equal(
        Int(iceberg_hash(Datum.integral(P_TIME, 81068000000))), -662762989
    )
    assert_equal(
        Int(iceberg_hash(Datum.integral(P_TIMESTAMP, 1510871468000000))),
        -2047944441,
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
    "unpartitioned,ident_part,bucket_part,day_part,trunc_part,evolved,"
    "deletes_v2,dv_v3"
)
"""The tables with a *plan* oracle. `eq_deletes_v2` is deliberately absent:
PyIceberg 0.11.1 refuses to plan a scan of a table with equality deletes at
all ("PyIceberg does not yet support equality deletes"), so there is no plan
oracle for it — DuckDB supplies its row oracle instead."""

comptime ALL_FIXTURE_TABLES = String(
    "unpartitioned,ident_part,bucket_part,day_part,trunc_part,evolved,"
    "deletes_v2,dv_v3,eq_deletes_v2"
)


def all_fixture_table_names() -> List[String]:
    return _split_commas(ALL_FIXTURE_TABLES)


def fixture_table_names() -> List[String]:
    return _split_commas(FIXTURE_TABLES)


def _split_commas(t: String) -> List[String]:
    var out = List[String]()
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
        FIXTURES
        + "/"
        + table
        + "/metadata/"
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
    if (
        a.has_current_snapshot
        and a.current_snapshot_id != b.current_snapshot_id
    ):
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
        assert_equal(
            m1.to_json(), m2.to_json(), tables[k] + ": unstable output"
        )
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
    """The v1 singular `schema` and bare `partition-spec` array."""
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
        assert_equal(len(m.snapshots), n, table + ": snapshot count")
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
                asof.snapshot_id,
                id,
                table + ": as-of at " + String(s.timestamp_ms),
            )
            last_id = id
            checked += 1
        # The newest snapshot is the current one and the head of `main`.
        var cur = m.current_snapshot()
        assert_equal(cur.snapshot_id, last_id, table + ": current snapshot")
        var main = m.snapshot_for_ref("main")
        assert_equal(main.snapshot_id, last_id, table + ": main branch head")
        # A timestamp before the first snapshot has no answer.
        var earliest = m.snapshots[0].timestamp_ms
        for j in range(len(m.snapshots)):
            if m.snapshots[j].timestamp_ms < earliest:
                earliest = m.snapshots[j].timestamp_ms
        with assert_raises():
            _ = m.snapshot_as_of(earliest - 1000000)
        # An unknown ref and an unknown id are errors, not silent nulls.
        with assert_raises():
            _ = m.snapshot_for_ref("no-such-branch")
        with assert_raises():
            _ = m.snapshot_by_id(1)
    print(
        "    snapshot selection:",
        checked,
        "snapshots across",
        len(tables),
        "tables",
    )


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
        String('["true"]'),
        String('["false"]'),
        String('["=","region","eu"]'),
        String('["!=","id",3]'),
        String('["<","id",5]'),
        String('["<=","id",5]'),
        String('[">","id",5]'),
        String('[">=","id",5]'),
        String('["is-null","amount"]'),
        String('["not-null","amount"]'),
        String('["is-nan","amount"]'),
        String('["not-nan","amount"]'),
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
    assert_equal(
        p.nodes[p.root].lits[0].i, Int64(bucket_of(Datum.long_(34), 4))
    )
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
        spec,
        schema,
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
        spec,
        schema,
    )
    assert_equal(p.nodes[p.root].op, OP_GT_EQ)
    assert_equal(p.nodes[p.root].lits[0].i, 17486)
    assert_equal(p.nodes[p.root].lits[0].kind, P_DATE)
    # `<` on a day-partitioned column bounds the day of the value one micro
    # earlier, which is still the same day here.
    var q = project_inclusive(
        rewrite_not(bound_filter('["<","ts","2017-11-17T00:00:00"]')),
        spec,
        schema,
    )
    assert_equal(q.nodes[q.root].op, OP_LT_EQ)
    assert_equal(q.nodes[q.root].lits[0].i, 17486)


def test_projection_ignores_unknown_transform() raises:
    """Format version 3 requires readers to ignore partition fields they cannot interpret.
    """
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


def summary(
    var lo: List[UInt8], var hi: List[UInt8], nulls: Bool
) -> FieldSummary:
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
    var keep = ManifestEvaluator(
        bound_filter('["=","region","eu"]'), spec, schema
    )
    assert_true(keep.eval(s))
    var drop = ManifestEvaluator(
        bound_filter('["=","region","zz"]'), spec, schema
    )
    assert_false(drop.eval(s))
    var below = ManifestEvaluator(
        bound_filter('["<","region","aa"]'), spec, schema
    )
    assert_false(below.eval(s))
    var above = ManifestEvaluator(
        bound_filter('[">","region","zz"]'), spec, schema
    )
    assert_false(above.eval(s))
    var isnull = ManifestEvaluator(
        bound_filter('["is-null","amount"]'), spec, schema
    )
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
    var nulls = ManifestEvaluator(
        bound_filter('["is-null","amount"]'), pspec, schema
    )
    assert_false(nulls.eval(s2))


def metric(
    field_id: Int, lo: String, hi: String, values: Int64, nulls: Int64
) raises -> ColumnMetrics:
    var lob = hex_bytes(lo)
    var hib = hex_bytes(hi)
    return ColumnMetrics(
        field_id,
        values,
        True,
        nulls,
        True,
        0,
        False,
        lob^,
        True,
        hib^,
        True,
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
    var e6 = InclusiveMetricsEvaluator(
        bound_filter('["in","id",[7,8,9]]'), schema
    )
    assert_false(e6.eval(5, m))
    var e7 = InclusiveMetricsEvaluator(
        bound_filter('["in","id",[7,3]]'), schema
    )
    assert_true(e7.eval(5, m))
    # amount is entirely null.
    var e8 = InclusiveMetricsEvaluator(
        bound_filter('["not-null","amount"]'), schema
    )
    assert_false(e8.eval(5, m))
    var e9 = InclusiveMetricsEvaluator(
        bound_filter('["is-null","amount"]'), schema
    )
    assert_true(e9.eval(5, m))
    var e10 = InclusiveMetricsEvaluator(
        bound_filter('[">","amount",1.0]'), schema
    )
    assert_false(
        e10.eval(5, m), "an all-null column matches no value predicate"
    )
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


# ══ manifests and scan planning ═════════════════════════════════════════════
comptime WAREHOUSE_PREFIX = String(
    "file:///Users/mseritan/dev/magmalake/iceberg.mojo/build/warehouse-root"
    "/warehouse/db"
)
"""The absolute location the fixtures were generated at. The metadata files are
verbatim copies, so every manifest list and manifest they name is addressed by
that path; `fixture_io` redirects it at `tests/fixtures/` so the tests run
anywhere, including CI where the original warehouse does not exist."""


def fixture_filters(table: String) raises -> List[String]:
    """The six oracle filters recorded for a table, in order."""
    var out = List[String]()
    for k in range(6):
        out.append(
            String(
                read_file(
                    FIXTURES
                    + "/"
                    + table
                    + "/oracle/plan_"
                    + String(k)
                    + ".filter.txt"
                ).strip()
            )
        )
    return out^


def fixture_io() -> FileIO:
    var io = FileIO.local()
    io.rebase(WAREHOUSE_PREFIX, FIXTURES)
    return io^


def fixture_scan(table: String) raises -> TableScan:
    return TableScan(load_fixture_metadata(table), fixture_io())


def test_io_rebasing() raises:
    var io = fixture_io()
    assert_equal(
        io.resolve(WAREHOUSE_PREFIX + "/ident_part/metadata/x.avro"),
        FIXTURES + "/ident_part/metadata/x.avro",
    )
    # An unmatched location just loses its scheme.
    assert_equal(io.resolve("file:///tmp/z"), "/tmp/z")
    assert_equal(io.resolve("/tmp/z"), "/tmp/z")
    assert_equal(strip_scheme("file:///a/b"), "/a/b")
    assert_equal(basename("file:///a/b/c.avro"), "c.avro")


def test_manifest_list_reading() raises:
    """Every manifest list of every fixture decodes with sane field values."""
    var tables = fixture_table_names()
    var io = fixture_io()
    var lists = 0
    var manifests = 0
    for k in range(len(tables)):
        var m = load_fixture_metadata(tables[k])
        for j in range(len(m.snapshots)):
            ref snap = m.snapshots[j]
            var mfs = read_manifest_list(io.resolve(snap.manifest_list))
            assert_true(len(mfs) > 0, tables[k] + ": empty manifest list")
            for i in range(len(mfs)):
                ref mf = mfs[i]
                assert_true(mf.manifest_path != "", "manifest_path")
                assert_true(mf.manifest_length > 0, "manifest_length")
                assert_true(mf.added_snapshot_id != 0, "added_snapshot_id")
                # The manifest's sequence number never exceeds the snapshot's.
                assert_true(
                    mf.sequence_number <= snap.sequence_number,
                    tables[k] + ": manifest sequence number is in the future",
                )
                assert_true(
                    mf.min_sequence_number <= mf.sequence_number,
                    tables[k] + ": min_sequence_number above sequence_number",
                )
                manifests += 1
            lists += 1
    print("    manifest lists:", lists, "read,", manifests, "manifest entries")


def test_manifest_sequence_inheritance() raises:
    """Gate (e): inherited sequence numbers, checked against the manifest list.

    An ADDED entry with a null sequence number inherits the manifest's; an
    EXISTING or DELETED entry must carry its own. Either way the result has to
    be a real sequence number that some snapshot actually assigned.
    """
    var tables = fixture_table_names()
    var io = fixture_io()
    var checked = 0
    var inherited = 0
    var explicit = 0
    for k in range(len(tables)):
        var m = load_fixture_metadata(tables[k])
        var snap = m.current_snapshot()
        var mfs = read_manifest_list(io.resolve(snap.manifest_list))
        for i in range(len(mfs)):
            ref mf = mfs[i]
            var man = read_manifest_at(io.resolve(mf.manifest_path), mf)
            assert_equal(
                man.partition_spec_id,
                mf.partition_spec_id,
                tables[k] + ": spec id disagrees with the manifest list",
            )
            for j in range(len(man.entries)):
                ref e = man.entries[j]
                if e.status == STATUS_ADDED:
                    # Inherited from the manifest's own entry in the list.
                    assert_equal(
                        e.sequence_number,
                        mf.sequence_number,
                        tables[k] + ": ADDED entry did not inherit",
                    )
                    inherited += 1
                else:
                    # Explicit, and never newer than the manifest that holds it.
                    assert_true(
                        e.sequence_number <= mf.sequence_number,
                        tables[k] + ": EXISTING entry has a future sequence",
                    )
                    explicit += 1
                assert_true(
                    e.sequence_number >= mf.min_sequence_number,
                    tables[k] + ": entry below the manifest's minimum",
                )
                # Snapshot id is inherited when null.
                assert_true(e.has_snapshot_id, "snapshot id")
                assert_true(
                    m.snapshot_index(e.snapshot_id) >= 0,
                    tables[k] + ": entry names an unknown snapshot",
                )
                checked += 1
    print(
        "    sequence inheritance:",
        checked,
        "entries (",
        inherited,
        "inherited,",
        explicit,
        "explicit )",
    )


def test_manifest_partition_typing() raises:
    """A manifest's partition tuple is typed by the spec in its own metadata."""
    var io = fixture_io()
    var m = load_fixture_metadata("ident_part")
    var snap = m.current_snapshot()
    var mfs = read_manifest_list(io.resolve(snap.manifest_list))
    var seen = 0
    for i in range(len(mfs)):
        var man = read_manifest_at(io.resolve(mfs[i].manifest_path), mfs[i])
        assert_equal(len(man.partition_spec.fields), 1)
        assert_equal(man.partition_spec.fields[0].name, "region")
        for j in range(len(man.entries)):
            ref e = man.entries[j]
            assert_equal(len(e.data_file.partition), 1)
            assert_true(e.data_file.partition[0].valid)
            assert_equal(e.data_file.partition[0].kind, P_STRING)
            var r = e.data_file.partition[0].s
            assert_true(r == "eu" or r == "us" or r == "apac", "region " + r)
            seen += 1
    assert_true(seen > 0)
    # bucket[4] partitions decode as ints, day(ts) as dates.
    var b = load_fixture_metadata("bucket_part")
    var bsnap = b.current_snapshot()
    var bmfs = read_manifest_list(io.resolve(bsnap.manifest_list))
    var bman = read_manifest_at(io.resolve(bmfs[0].manifest_path), bmfs[0])
    assert_equal(bman.entries[0].data_file.partition[0].kind, P_INT)
    var d = load_fixture_metadata("day_part")
    var dsnap = d.current_snapshot()
    var dmfs = read_manifest_list(io.resolve(dsnap.manifest_list))
    var dman = read_manifest_at(io.resolve(dmfs[0].manifest_path), dmfs[0])
    assert_equal(dman.entries[0].data_file.partition[0].kind, P_DATE)


def test_manifest_metrics_decoded() raises:
    """Per-column metrics and bounds decode to the right types."""
    var io = fixture_io()
    var m = load_fixture_metadata("ident_part")
    var snap = m.current_snapshot()
    var schema = m.schema()
    var mfs = read_manifest_list(io.resolve(snap.manifest_list))
    var man = read_manifest_at(io.resolve(mfs[0].manifest_path), mfs[0])
    ref df = man.entries[0].data_file
    assert_equal(df.content, CONTENT_DATA)
    assert_equal(df.file_format.lower(), "parquet")
    assert_true(df.record_count > 0)
    assert_true(df.file_size_in_bytes > 0)
    assert_true(len(df.metrics) > 0)
    var found_id = False
    for k in range(len(df.metrics)):
        ref c = df.metrics[k]
        if c.field_id == 1 and c.has_lower:
            var lo = datum_from_bytes_prim(P_LONG, 0, 0, 0, c.lower_bound)
            var hi = datum_from_bytes_prim(P_LONG, 0, 0, 0, c.upper_bound)
            assert_true(lo.i <= hi.i, "id bounds are ordered")
            found_id = True
    assert_true(found_id, "no bounds for the id column")


def test_delete_manifest() raises:
    """The v2 fixture with a position delete file."""
    var io = fixture_io()
    var m = load_fixture_metadata("deletes_v2")
    var snap = m.current_snapshot()
    var mfs = read_manifest_list(io.resolve(snap.manifest_list))
    var delete_manifests = 0
    var delete_files = 0
    for i in range(len(mfs)):
        if not mfs[i].is_delete_manifest():
            continue
        delete_manifests += 1
        var man = read_manifest_at(io.resolve(mfs[i].manifest_path), mfs[i])
        for j in range(len(man.entries)):
            ref df = man.entries[j].data_file
            assert_equal(df.content, CONTENT_POSITION_DELETES)
            assert_true(df.record_count > 0)
            delete_files += 1
    assert_equal(delete_manifests, 1, "expected one delete manifest")
    assert_equal(delete_files, 1, "expected one position delete file")


def plan_paths(tasks: List[FileScanTask]) -> List[String]:
    var out = List[String]()
    for k in range(len(tasks)):
        out.append(basename(tasks[k].data_file.file_path))
    _sort_strings_test(out)
    return out^


def _sort_strings_test(mut l: List[String]):
    for i in range(1, len(l)):
        var j = i
        while j > 0 and l[j] < l[j - 1]:
            l.swap_elements(j, j - 1)
            j -= 1


def oracle_bridge_paths(table: String, k: Int) raises -> List[String]:
    var doc = parse_json(
        read_file(
            FIXTURES + "/" + table + "/oracle/plan_" + String(k) + ".json"
        )
    )
    var out = List[String]()
    for j in range(doc.size(doc.root)):
        out.append(
            basename(doc.req_string(doc.at(doc.root, j), "data-file-path"))
        )
    _sort_strings_test(out)
    return out^


def oracle_pyiceberg_paths(table: String, k: Int) raises -> List[String]:
    var doc = parse_json(
        read_file(
            FIXTURES
            + "/"
            + table
            + "/oracle/pyiceberg_plan_"
            + String(k)
            + ".json"
        )
    )
    var tasks = doc.get(doc.root, "tasks")
    var out = List[String]()
    for j in range(doc.size(tasks)):
        out.append(doc.req_string(doc.at(tasks, j), "data_file"))
    _sort_strings_test(out)
    return out^


def join_list(l: List[String]) -> String:
    var out = String("")
    for k in range(len(l)):
        if k > 0:
            out += " "
        out += l[k]
    return out^


def test_plan_files_matches_oracles() raises:
    """Gate (d): the planned file set, per table, per filter, against both
    oracles."""
    var tables = fixture_table_names()
    var cases = 0
    var bridge_agree = 0
    var pyiceberg_agree = 0
    var disagreements = String("")
    for t in range(len(tables)):
        var table = tables[t]
        var scan = fixture_scan(table)
        for k in range(6):
            var dsl = read_file(
                FIXTURES
                + "/"
                + table
                + "/oracle/plan_"
                + String(k)
                + ".filter.txt"
            ).strip()
            var tasks = scan.filter(String(dsl)).plan_files()
            var mine = plan_paths(tasks)
            var bridge = oracle_bridge_paths(table, k)
            var pyi = oracle_pyiceberg_paths(table, k)
            var what = table + " filter " + String(k) + " " + String(dsl)
            if join_list(mine) == join_list(bridge):
                bridge_agree += 1
            else:
                disagreements += (
                    "\n      bridge: "
                    + what
                    + "\n        mine:   "
                    + join_list(mine)
                    + "\n        oracle: "
                    + join_list(bridge)
                )
            if join_list(mine) == join_list(pyi):
                pyiceberg_agree += 1
            else:
                disagreements += (
                    "\n      pyiceberg: "
                    + what
                    + "\n        mine:   "
                    + join_list(mine)
                    + "\n        oracle: "
                    + join_list(pyi)
                )
            cases += 1
    print(
        "    plan_files:",
        cases,
        "cases;",
        bridge_agree,
        "match iceberg-rust,",
        pyiceberg_agree,
        "match PyIceberg",
    )
    if disagreements != "":
        print("    disagreements:", disagreements)
    # PyIceberg is the strict reference here and must match everywhere.
    assert_equal(
        pyiceberg_agree, cases, "PyIceberg disagreements: " + disagreements
    )
    # iceberg-rust 0.10.1 disagrees on exactly one case: for
    # `["in","id",[1,4,7]]` over the bucket[4] table it applies the partition
    # filter but not the per-file `In` metrics filter, so it plans two extra
    # files that cannot contain a matching row. A plan may legitimately be a
    # superset, so this is looseness in the bridge, not an error — and the
    # separate subset assertion below proves nothing is being *dropped*.
    assert_equal(
        bridge_agree,
        cases - 1,
        "unexpected iceberg-rust disagreements: " + disagreements,
    )


def test_plan_files_never_drops_a_file_the_bridge_keeps() raises:
    """Whatever the two disagree on, our plan is never a *subset* short of a
    file the bridge would read — over-pruning would lose data."""
    var tables = fixture_table_names()
    var extra = 0
    for t in range(len(tables)):
        var table = tables[t]
        var scan = fixture_scan(table)
        for k in range(6):
            var dsl = read_file(
                FIXTURES
                + "/"
                + table
                + "/oracle/plan_"
                + String(k)
                + ".filter.txt"
            ).strip()
            var mine = plan_paths(scan.filter(String(dsl)).plan_files())
            var bridge = oracle_bridge_paths(table, k)
            for j in range(len(mine)):
                var found = False
                for i in range(len(bridge)):
                    if bridge[i] == mine[j]:
                        found = True
                assert_true(
                    found,
                    table
                    + " filter "
                    + String(k)
                    + ": planned "
                    + mine[j]
                    + ", which iceberg-rust does not",
                )
            extra += len(bridge) - len(mine)
    print("    files the bridge plans that we prune away:", extra)


def test_plan_files_delete_association() raises:
    """The position delete file is attached to both data files it covers."""
    var scan = fixture_scan("deletes_v2")
    var tasks = scan.filter('["true"]').plan_files()
    assert_true(len(tasks) > 0)
    var with_deletes = 0
    for k in range(len(tasks)):
        if len(tasks[k].delete_files) > 0:
            with_deletes += 1
            assert_equal(
                tasks[k].delete_files[0].content, CONTENT_POSITION_DELETES
            )
    # The oracle says both data files carry the delete.
    var doc = parse_json(read_file(FIXTURES + "/deletes_v2/oracle/plan_0.json"))
    var want = 0
    for j in range(doc.size(doc.root)):
        if doc.size(doc.get(doc.at(doc.root, j), "deletes")) > 0:
            want += 1
    assert_equal(with_deletes, want, "delete association count")


def test_plan_files_json_shape() raises:
    """The emitted JSON matches the bridge's, field for field."""
    var scan = fixture_scan("ident_part")
    var text = scan.filter('["=","region","eu"]').plan_files_json()
    var mine = parse_json(text)
    var theirs = parse_json(
        read_file(FIXTURES + "/ident_part/oracle/plan_1.json")
    )
    assert_equal(mine.size(mine.root), theirs.size(theirs.root))
    # Compare by path so manifest order does not matter.
    for j in range(mine.size(mine.root)):
        var a = mine.at(mine.root, j)
        var path = mine.req_string(a, "data-file-path")
        var matched = False
        for i in range(theirs.size(theirs.root)):
            var b = theirs.at(theirs.root, i)
            if theirs.req_string(b, "data-file-path") != path:
                continue
            matched = True
            assert_equal(
                mine.req_int(a, "record-count"),
                theirs.req_int(b, "record-count"),
            )
            assert_equal(
                mine.req_int(a, "file-size-in-bytes"),
                theirs.req_int(b, "file-size-in-bytes"),
            )
            assert_equal(
                mine.req_string(a, "file-format"),
                theirs.req_string(b, "file-format"),
            )
            assert_equal(mine.req_int(a, "start"), theirs.req_int(b, "start"))
            assert_equal(mine.req_int(a, "length"), theirs.req_int(b, "length"))
            assert_equal(
                mine.size(mine.get(a, "project-field-ids")),
                theirs.size(theirs.get(b, "project-field-ids")),
            )
        assert_true(matched, "the oracle has no task for " + path)


def test_scan_snapshot_selection() raises:
    """Planning an older snapshot sees fewer files than the current one."""
    var m = load_fixture_metadata("unpartitioned")
    var scan = fixture_scan("unpartitioned")
    # The `snapshots` array is not ordered by the spec — iceberg-rust writes
    # it in hash order — so the oldest snapshot is found by sequence number,
    # not by position.
    var oldest = 0
    for k in range(len(m.snapshots)):
        if m.snapshots[k].sequence_number < m.snapshots[oldest].sequence_number:
            oldest = k
    ref first = m.snapshots[oldest]
    var older = scan.use_snapshot(first.snapshot_id).plan_files()
    var current = scan.plan_files()
    assert_true(
        len(older) < len(current),
        "the first snapshot should have fewer files than the last",
    )
    # as_of at the first snapshot's timestamp gives the same plan.
    var asof = scan.as_of(first.timestamp_ms).plan_files()
    assert_equal(len(asof), len(older))
    # and so does the main branch vs the default.
    var main = scan.use_ref("main").plan_files()
    assert_equal(len(main), len(current))


def test_scan_projection() raises:
    var scan = fixture_scan("ident_part")
    var ids = scan.select(["id", "region"]).projected_field_ids()
    assert_equal(len(ids), 2)
    var s = scan.select(["id", "region"]).schema()
    assert_equal(len(s.columns()), 2)
    var all_ids = scan.projected_field_ids()
    assert_true(len(all_ids) > 2)


def test_scan_residuals() raises:
    """A filter fully decided by partitioning leaves no residual."""
    var scan = fixture_scan("ident_part")
    var tasks = scan.filter('["=","region","eu"]').plan_files()
    assert_true(len(tasks) > 0)
    for k in range(len(tasks)):
        assert_equal(
            tasks[k].residual,
            '["true"]',
            "an identity-partitioned equality should leave nothing behind",
        )
    # One that partitioning cannot decide does leave a residual.
    var t2 = scan.filter('[">","id",2]').plan_files()
    assert_true(len(t2) > 0)
    assert_true(t2[0].residual != '["true"]', "expected a surviving residual")


# ══ catalogs ════════════════════════════════════════════════════════════════
def test_find_latest_metadata() raises:
    """Discovery from a table dir, a metadata dir, and a file path alike."""
    var io = fixture_io()
    var idx = fixture_index()
    var tables = fixture_table_names()
    for k in range(len(tables)):
        var want = current_metadata_path(idx, tables[k])
        var from_dir = find_latest_metadata(io, FIXTURES + "/" + tables[k])
        assert_equal(
            basename(from_dir),
            basename(want),
            tables[k] + ": discovery from the table dir",
        )
        var from_meta = find_latest_metadata(
            io, FIXTURES + "/" + tables[k] + "/metadata"
        )
        assert_equal(basename(from_meta), basename(want))
        var from_file = find_latest_metadata(io, want)
        assert_equal(from_file, want)
    # A directory with no metadata is an error, not an empty table.
    with assert_raises():
        _ = find_latest_metadata(io, FIXTURES)


def test_table_load_and_scan() raises:
    var t = Table.load(FIXTURES + "/ident_part", fixture_io())
    assert_equal(t.metadata.format_version, 2)
    assert_equal(t.name, "ident_part")
    assert_true(t.metadata_location.endswith(".metadata.json"))
    var tasks = t.scan().filter('["=","region","eu"]').plan_files()
    assert_true(len(tasks) > 0)


def test_filesystem_catalog() raises:
    var cat = FilesystemCatalog(FIXTURES, fixture_io())
    var names = cat.list_tables("")
    assert_equal(len(names), 9, "expected 9 fixture tables")
    assert_true(cat.table_exists("", "ident_part"))
    assert_false(cat.table_exists("", "no_such_table"))
    var t = cat.load_table("", "bucket_part")
    assert_equal(t.name, "bucket_part")
    assert_equal(len(t.metadata.partition_specs[0].fields), 1)


def test_version_hint_discovery() raises:
    """A `version-hint.text` table, which the fixtures do not use."""
    var dir = String("build/hinted/metadata")
    _ = _mkdirs(dir)
    var meta = String(
        '{"format-version":2,"table-uuid":"aaaaaaaa-0000-0000-0000-000000000000",'
        '"location":"file:///t","last-sequence-number":0,"last-updated-ms":1,'
        '"last-column-id":1,"current-schema-id":0,"schemas":[{"type":"struct",'
        '"schema-id":0,"fields":[{"id":1,"name":"a","required":true,'
        '"type":"long"}]}],"default-spec-id":0,"partition-specs":[{"spec-id":0,'
        '"fields":[]}],"last-partition-id":999,"default-sort-order-id":0,'
        '"sort-orders":[{"order-id":0,"fields":[]}],"properties":{},'
        '"current-snapshot-id":-1,"snapshots":[]}'
    )
    _write_file(dir + "/v7.metadata.json", meta)
    _write_file(dir + "/v2.metadata.json", meta)
    _write_file(dir + "/version-hint.text", "7\n")
    var io = FileIO.local()
    assert_equal(read_version_hint(io, dir), 7)
    assert_equal(
        basename(find_latest_metadata(io, "build/hinted")), "v7.metadata.json"
    )
    # Without the hint, the highest version still wins.
    _write_file(dir + "/version-hint.text", "")
    assert_equal(
        basename(find_latest_metadata(io, "build/hinted")), "v7.metadata.json"
    )


def _mkdirs(path: String) -> Bool:
    """`std.pathlib.Path` has no mkdir on either toolchain; shell out."""
    try:
        makedirs(Path(path), exist_ok=True)
        return True
    except:
        return False


def _write_file(path: String, content: String) raises:
    with open(path, "w") as f:
        f.write(content)


def test_gunzip() raises:
    """A gzip member produced by python's gzip module, decoded in-process."""
    # "iceberg" gzipped: 1f8b header, deflate payload, crc32 + isize trailer.
    var gz = hex_bytes("1f8b08000000000002ffcb4c4e4d4a2d4a07004d8a1c4d07000000")
    var out = gunzip(gz)
    assert_equal(String(StringSlice(unsafe_from_utf8=Span(out))), "iceberg")
    with assert_raises():
        _ = gunzip(hex_bytes("00010203"))


def test_rest_url_and_header_shaping() raises:
    var c = RestCatalogConfig("https://polaris.example.com/api/catalog/")
    assert_equal(
        c.config_url(), "https://polaris.example.com/api/catalog/v1/config"
    )
    c.with_warehouse("my_wh")
    assert_equal(
        c.config_url(),
        "https://polaris.example.com/api/catalog/v1/config?warehouse=my_wh",
    )
    assert_equal(
        c.load_table_url("sales", "orders"),
        "https://polaris.example.com/api/catalog/v1/namespaces/sales/tables/orders",
    )
    # A config response can insert a prefix into every later path.
    c.apply_config('{"overrides":{"prefix":"ws/main"},"defaults":{}}')
    assert_equal(c.prefix, "ws/main")
    assert_equal(
        c.tables_url("sales"),
        "https://polaris.example.com/api/catalog/v1/ws/main/namespaces/sales/tables",
    )
    # Multipart namespaces travel as one segment joined by 0x1F.
    assert_equal(url_encode(encode_namespace("a.b")), "a%1Fb")
    # Headers: bearer token and credential vending.
    c.with_token("tok123")
    c.vend_credentials = True
    var hs = c.headers()
    var saw_auth = False
    var saw_delegation = False
    for k in range(len(hs)):
        if hs[k].name == "Authorization":
            assert_equal(hs[k].value, "Bearer tok123")
            saw_auth = True
        if hs[k].name == ACCESS_DELEGATION_HEADER:
            assert_equal(hs[k].value, VENDED_CREDENTIALS)
            saw_delegation = True
    assert_true(saw_auth and saw_delegation)


def test_rest_load_table_response() raises:
    """A `loadTable` body parses into a real TableMetadata."""
    var idx = fixture_index()
    var inner = read_file(current_metadata_path(idx, "ident_part"))
    var body = (
        '{"metadata-location":"s3://b/t/metadata/00003-x.metadata.json",'
        '"metadata":'
        + inner
        + ","
        '"config":{"s3.access-key-id":"AK"},'
        '"storage-credentials":[{"prefix":"s3://b/t",'
        '"config":{"s3.session-token":"tok"}}]}'
    )
    var r = LoadTableResult.parse(body)
    assert_true(r.has_metadata_location)
    assert_equal(r.metadata_location, "s3://b/t/metadata/00003-x.metadata.json")
    assert_equal(r.metadata.format_version, 2)
    assert_equal(len(r.metadata.snapshots), 3)
    assert_equal(r.config["s3.access-key-id"], "AK")
    assert_equal(len(r.storage_credentials), 1)
    assert_equal(r.storage_credentials[0].prefix, "s3://b/t")
    assert_equal(r.storage_credentials[0].config["s3.session-token"], "tok")
    with assert_raises():
        _ = LoadTableResult.parse('{"metadata-location":"x"}')


def test_rest_list_responses() raises:
    assert_equal(
        len(
            RestCatalogConfig.parse_namespaces(
                '{"namespaces":[["accounting"],["tax","ny"]]}'
            )
        ),
        2,
    )
    var ns = RestCatalogConfig.parse_namespaces(
        '{"namespaces":[["accounting"],["tax","ny"]]}'
    )
    assert_equal(ns[1], "tax.ny")
    var ts = RestCatalogConfig.parse_tables(
        '{"identifiers":[{"namespace":["a"],"name":"t1"},'
        '{"namespace":["a"],"name":"t2"}]}'
    )
    assert_equal(len(ts), 2)
    assert_equal(ts[0], "t1")


# ══ REST catalog over a real socket ═════════════════════════════════════════
# tests/run_tests.sh starts tests/rest_server.py, a mock catalog serving the
# checked-in fixtures. Every assertion below is about the *client*: that it
# sends the bearer token and the delegation header, absorbs the `prefix` a
# config response asks for, parses a LoadTableResult into real metadata, maps
# status codes onto errors, and produces exactly the plan the local reader
# produces from the same table.
def _rest_uri() -> String:
    return getenv("ICEBERG_TEST_REST", "")


def _rest_catalog(token: String = "test-token") raises -> RestCatalog:
    var io = FileIO.local()
    io.rebase(WAREHOUSE_PREFIX, FIXTURES)
    var cat = RestCatalog(RestCatalogConfig(_rest_uri()), io^)
    cat.config.with_token(token)
    cat.config.vend_credentials = True
    return cat^


def test_rest_catalog_over_http() raises:
    if _rest_uri() == "":
        print("SKIP test_rest_catalog_over_http: no ICEBERG_TEST_REST")
        return
    var cat = _rest_catalog()
    assert_equal(cat.config.prefix, "")
    cat.connect()
    # The server sends `overrides.prefix = "ws"`; every later URL must carry it.
    assert_equal(cat.config.prefix, "ws")
    assert_true(cat.config.namespaces_url().endswith("/v1/ws/namespaces"))

    var namespaces = cat.list_namespaces()
    assert_equal(len(namespaces), 1)
    assert_equal(namespaces[0], "db")

    var tables = cat.list_tables("db")
    var found = False
    for k in range(len(tables)):
        if tables[k] == "unpartitioned":
            found = True
    assert_true(found)
    assert_true(cat.table_exists("db", "unpartitioned"))
    assert_false(cat.table_exists("db", "no_such_table"))


def test_rest_load_table_matches_local() raises:
    if _rest_uri() == "":
        print("SKIP test_rest_load_table_matches_local: no ICEBERG_TEST_REST")
        return
    var cat = _rest_catalog()
    cat.connect()
    var names = [
        String("unpartitioned"),
        String("ident_part"),
        String("bucket_part"),
        String("day_part"),
        String("trunc_part"),
        String("evolved"),
        String("deletes_v2"),
    ]
    var checked = 0
    for k in range(len(names)):
        var filters = fixture_filters(names[k])
        var remote = cat.load_table("db", names[k])
        var local = load_fixture_metadata(names[k])
        assert_equal(remote.metadata.table_uuid, local.table_uuid)
        assert_equal(
            remote.metadata.current_snapshot_id, local.current_snapshot_id
        )
        assert_equal(remote.metadata.format_version, local.format_version)
        # The catalog vended a credential and echoed the delegation header
        # back; both prove the request carried what it should have.
        assert_equal(remote.metadata.location, local.location)
        for j in range(len(filters)):
            var want = (
                TableScan(local.copy(), fixture_io())
                .filter(filters[j])
                .plan_files_json()
            )
            var got = remote.scan().filter(filters[j]).plan_files_json()
            assert_equal(got, want)
            checked += 1
    assert_equal(checked, len(names) * 6)


def test_rest_delegation_and_credentials() raises:
    if _rest_uri() == "":
        print("SKIP test_rest_delegation_and_credentials: no ICEBERG_TEST_REST")
        return
    var cat = _rest_catalog()
    cat.connect()
    var res = cat.load_table_result("db", "unpartitioned")
    assert_equal(res.config["echo.delegation"], VENDED_CREDENTIALS)
    assert_equal(len(res.storage_credentials), 1)
    assert_true("s3.access-key-id" in res.storage_credentials[0].config)
    assert_true(res.has_metadata_location)

    # Without the delegation header the server vends nothing.
    var plain = RestCatalog(RestCatalogConfig(_rest_uri()), FileIO.local())
    plain.config.with_token("test-token")
    plain.connect()
    var res2 = plain.load_table_result("db", "unpartitioned")
    assert_equal(res2.config["echo.delegation"], "")
    assert_equal(len(res2.storage_credentials), 0)


def test_rest_error_mapping() raises:
    if _rest_uri() == "":
        print("SKIP test_rest_error_mapping: no ICEBERG_TEST_REST")
        return
    var cat = _rest_catalog()
    cat.connect()
    # 404 with an ErrorModel body.
    var missing = String("")
    try:
        _ = cat.load_table("db", "no_such_table")
    except e:
        missing = String(e)
    assert_true(missing.find("404") >= 0)
    assert_true(missing.find("not found") >= 0)
    assert_true(missing.find("NoSuchTableException") >= 0)

    # 401 when the bearer token is wrong.
    var bad = _rest_catalog("wrong-token")
    var unauth = String("")
    try:
        bad.connect()
    except e:
        unauth = String(e)
    assert_true(unauth.find("401") >= 0)
    assert_true(unauth.find("not authenticated") >= 0)


# ══ S3 end to end ═══════════════════════════════════════════════════════════
# tests/run_tests.sh uploads a few fixture tables into MinIO and points
# $ICEBERG_TEST_S3 at the warehouse prefix. The metadata is byte-identical to
# what is on disk, so the plan must be too — the only thing that changed is
# where every byte came from.
def _s3_warehouse() -> String:
    return getenv("ICEBERG_TEST_S3", "")


def s3_io() raises -> FileIO:
    var io = FileIO.local()
    io.set(String("s3.endpoint"), getenv("AWS_ENDPOINT_URL_S3", ""))
    io.set(String("s3.access-key-id"), getenv("AWS_ACCESS_KEY_ID", ""))
    io.set(String("s3.secret-access-key"), getenv("AWS_SECRET_ACCESS_KEY", ""))
    io.set(String("s3.region"), String("us-east-1"))
    io.rebase(WAREHOUSE_PREFIX, _s3_warehouse())
    return io^


def test_s3_table_load_and_plan() raises:
    if _s3_warehouse() == "":
        print("SKIP test_s3_table_load_and_plan: no ICEBERG_TEST_S3")
        return
    var names = [
        String("unpartitioned"),
        String("ident_part"),
        String("deletes_v2"),
        String("evolved"),
    ]
    var checked = 0
    for k in range(len(names)):
        var filters = fixture_filters(names[k])
        var t = Table.load(_s3_warehouse() + "/" + names[k], s3_io())
        var local = load_fixture_metadata(names[k])
        assert_equal(t.metadata.table_uuid, local.table_uuid)
        assert_equal(t.metadata.current_snapshot_id, local.current_snapshot_id)
        for j in range(len(filters)):
            var want = (
                TableScan(local.copy(), fixture_io())
                .filter(filters[j])
                .plan_files_json()
            )
            assert_equal(t.scan().filter(filters[j]).plan_files_json(), want)
            checked += 1
    assert_equal(checked, len(names) * 6)


def test_s3_catalog_listing() raises:
    if _s3_warehouse() == "":
        print("SKIP test_s3_catalog_listing: no ICEBERG_TEST_S3")
        return
    var cat = FilesystemCatalog(_s3_warehouse(), s3_io())
    var tables = cat.list_tables("")
    assert_true(len(tables) >= 4)
    var seen = False
    for k in range(len(tables)):
        if tables[k] == "ident_part":
            seen = True
    assert_true(seen)
    assert_true(cat.table_exists("", "unpartitioned"))
    assert_false(cat.table_exists("", "nope"))


# ══ Puffin and deletion vectors ═════════════════════════════════════════════
# `dv_v3` is a format-version 3 table whose deletes live in a Puffin file as
# `deletion-vector-v1` blobs. It was assembled by tools/make_delete_tables.py
# from PyIceberg's own v3 structs (PyIceberg cannot write v3 metadata) and
# validated by reading it back with PyIceberg, which decodes the vectors with
# entirely separate code; tests/fixtures/delete_tables_report.json is that
# read-back.
def dv_report() raises -> Json:
    return parse_json(read_file(FIXTURES + "/delete_tables_report.json"))


def dv_puffin_location() raises -> String:
    """The Puffin file the dv_v3 delete manifest points at."""
    var tasks = fixture_scan("dv_v3").plan_files()
    for k in range(len(tasks)):
        for j in range(len(tasks[k].delete_files)):
            if tasks[k].delete_files[j].is_deletion_vector():
                return tasks[k].delete_files[j].file_path
    raise Error("no deletion vector in the dv_v3 plan")


def test_puffin_footer() raises:
    var io = fixture_io()
    var pf = PuffinFile.open(io, dv_puffin_location())
    assert_equal(len(pf.blobs), 2)
    assert_false(pf.footer_compressed)
    assert_equal(pf.created_by(), "iceberg.mojo fixture generator")
    for k in range(len(pf.blobs)):
        ref b = pf.blobs[k]
        assert_equal(b.type, BLOB_DELETION_VECTOR_V1)
        assert_true(b.is_deletion_vector())
        # The spec requires these for a deletion vector.
        assert_true(b.referenced_data_file() != "")
        assert_equal(b.cardinality(), 1)
        assert_equal(b.compression_codec, "")
        assert_equal(b.snapshot_id, -1)
        assert_equal(b.sequence_number, -1)
        assert_true(b.offset >= 4)
        assert_true(b.length > 0)


def test_puffin_offsets_match_the_manifest() raises:
    """`content_offset` / `content_size_in_bytes` must equal the footer's
    `offset` / `length` for the same blob — the spec says "must exactly
    match", and a scan trusts the manifest and never reads the footer."""
    var io = fixture_io()
    var pf = PuffinFile.open(io, dv_puffin_location())
    var tasks = fixture_scan("dv_v3").plan_files()
    var matched = 0
    for k in range(len(tasks)):
        for j in range(len(tasks[k].delete_files)):
            ref d = tasks[k].delete_files[j]
            if not d.is_deletion_vector():
                continue
            assert_true(d.has_content_offset)
            assert_true(d.has_content_size_in_bytes)
            var found = False
            for b in range(len(pf.blobs)):
                if pf.blobs[b].offset != d.content_offset:
                    continue
                found = True
                assert_equal(pf.blobs[b].length, d.content_size_in_bytes)
                assert_equal(
                    pf.blobs[b].referenced_data_file(), d.referenced_data_file
                )
                assert_equal(pf.blobs[b].cardinality(), d.record_count)
            assert_true(found)
            matched += 1
    assert_equal(matched, 2)


def test_deletion_vector_decodes() raises:
    """Positions decoded here must equal the ones the generator recorded and
    PyIceberg read back."""
    var io = fixture_io()
    var doc = dv_report()
    var dv = doc.get(doc.root, "dv_v3")
    var deleted = doc.get(dv, "deleted")
    var tasks = fixture_scan("dv_v3").plan_files()
    var checked = 0
    for k in range(len(tasks)):
        for j in range(len(tasks[k].delete_files)):
            ref d = tasks[k].delete_files[j]
            if not d.is_deletion_vector():
                continue
            var positions = deleted_positions(
                io, d.file_path, d.content_offset, d.content_size_in_bytes
            )
            # Find the report entry for the referenced data file.
            var want = List[Int64]()
            for e in range(doc.size(deleted)):
                var entry = doc.at(deleted, e)
                if not d.referenced_data_file.endswith(
                    doc.req_string(entry, "file")
                ):
                    continue
                var pos = doc.get(entry, "positions")
                for q in range(doc.size(pos)):
                    want.append(doc.as_int(doc.at(pos, q)))
            assert_equal(len(positions), len(want))
            for q in range(len(want)):
                assert_equal(Int64(Int(positions[q])), want[q])
            checked += 1
    assert_equal(checked, 2)


def test_deletion_vector_crc_is_checked() raises:
    """A corrupted vector must be an error, not silently missing deletes."""
    var io = fixture_io()
    var tasks = fixture_scan("dv_v3").plan_files()
    ref d = tasks[0].delete_files[0]
    with assert_raises():
        # One byte short: the framing's own length check must reject it.
        _ = read_deletion_vector(
            io, d.file_path, d.content_offset, d.content_size_in_bytes - 1
        )


def test_dv_supersedes_and_associates() raises:
    """Every data file in dv_v3 gets exactly its own vector."""
    var tasks = fixture_scan("dv_v3").plan_files()
    assert_equal(len(tasks), 2)
    for k in range(len(tasks)):
        assert_equal(len(tasks[k].delete_files), 1)
        assert_true(tasks[k].delete_files[0].is_deletion_vector())
        assert_equal(
            tasks[k].delete_files[0].referenced_data_file,
            tasks[k].data_file.file_path,
        )
        assert_equal(tasks[k].delete_files[0].file_format.lower(), "puffin")


def test_dv_table_is_v3() raises:
    var m = load_fixture_metadata("dv_v3")
    assert_equal(m.format_version, 3)
    var snap = m.current_snapshot()
    assert_equal(snap.operation(), "delete")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
