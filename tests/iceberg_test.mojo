"""The test suite for iceberg.mojo. Run with `pixi run test` from the repo root.

Everything that touches `tests/fixtures/` is a *parity* test: the expected
values were produced by iceberg-rust 0.10.1 (through iceberg-rs.mojo) or by
PyIceberg, never by this implementation. See `tests/fixtures/PROVENANCE.md`.
"""

from std.collections import Dict
from std.memory import bitcast
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
    commit_append_body,
    RestCatalog,
    RestCatalogConfig,
    encode_namespace,
    url_encode,
    ACCESS_DELEGATION_HEADER,
    VENDED_CREDENTIALS,
)
from iceberg.catalog.sql import (
    NamespacePropertiesUpdateSummary,
    SqlCatalog,
    sqlite_path_from_uri,
)
from iceberg.puffin import (
    BLOB_DELETION_VECTOR_V1,
    BlobMetadata,
    PuffinFile,
    PuffinWriter,
    decompress_blob,
    deleted_positions,
    read_blob_bytes,
    read_deletion_vector,
)
from roaring import Bitmap64
from iceberg.read import (
    read_data_file,
    read_data_file_table,
    META_FILE,
    META_LAST_UPDATED,
    META_PARTITION,
    META_POS,
    META_ROW_ID,
    META_SPEC_ID,
    NameMapping,
    ScanOptions,
    ScanResult,
    arrow_type_of,
)
from iceberg.append import (
    metadata_file_name,
    next_metadata_version,
    prepare_append,
)
from iceberg.delete import (
    MODE_COPY_ON_WRITE,
    MODE_MERGE_ON_READ,
    PROP_DELETE_MODE,
    delete_mode_of,
    plan_row_deletes,
)
from iceberg.batch import (
    ColumnBuilder,
    NestedBuilder,
    batch_of,
    batch_of_columns,
)
from iceberg.util import now_ms
from iceberg.manifest_write import (
    manifest_entry_schema_json,
    manifest_list_schema_json,
    PartitionTyping,
)
from iceberg.scan import TableScan, FileScanTask
from iceberg.transforms import parse_transform
from iceberg.write import (
    WriteOptions,
    write_data_files,
    escape_path,
    human_partition_value,
    partition_path,
    truncate_lower,
    truncate_upper,
)
from parquet import RecordBatch
from parquet.arrow import AT_LIST, AT_MAP, AT_STRUCT
from iceberg.manifest import (
    ManifestCache,
    ManifestFile,
    ManifestEntry,
    DataFile,
    Manifest,
    read_manifest_list,
    read_manifest_list_io,
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
    P_FLOAT,
    P_UNKNOWN,
)
from iceberg.values import (
    Datum,
    decimal_text,
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
    PartitionField,
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
    "deletes_v2,dv_v3,nested_v2,nested_evo_v2,nested_part_v2"
)
"""The tables with a *plan* oracle. `eq_deletes_v2` is deliberately absent:
PyIceberg 0.11.1 refuses to plan a scan of a table with equality deletes at
all ("PyIceberg does not yet support equality deletes"), so there is no plan
oracle for it — DuckDB supplies its row oracle instead."""

comptime ALL_FIXTURE_TABLES = String(
    "unpartitioned,ident_part,bucket_part,day_part,trunc_part,evolved,"
    "deletes_v2,dv_v3,eq_deletes_v2,nested_v2,nested_evo_v2,nested_part_v2"
)

comptime NESTED_FIXTURE_TABLES = String(
    "nested_v2,nested_evo_v2,nested_part_v2"
)
"""The three fixtures with structs, lists and maps in them, written by
PyIceberg 0.11.1 — see tools/make_nested_tables.py."""


def nested_fixture_table_names() -> List[String]:
    return _split_commas(NESTED_FIXTURE_TABLES)


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
    assert_equal(len(names), 12, "expected 12 fixture tables")
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
    # `db` is the read-only fixture corpus; `wr` is the scratch namespace the
    # commit tests write into.
    assert_equal(len(namespaces), 2)
    assert_equal(namespaces[0], "db")
    assert_equal(namespaces[1], "wr")

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
        # And the rows themselves, through the FileIO the catalog handed back.
        var local_rows = encoded_rows(
            TableScan(local.copy(), fixture_io()).to_table()
        )
        var remote_rows = encoded_rows(remote.scan().to_table())
        assert_equal(
            _diff(remote_rows, local_rows), "", "REST rows for " + names[k]
        )
        assert_equal(len(remote_rows), len(local_rows))
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


def test_s3_read_matches_local() raises:
    """Gate (f): the same rows over `s3://` as on disk, eagerly and lazily.

    The lazy path is the one that matters here: it fetches the footer and then
    only the byte ranges of the row groups it needs, which is a different set
    of HTTP requests entirely.
    """
    if _s3_warehouse() == "":
        print("SKIP test_s3_read_matches_local: no ICEBERG_TEST_S3")
        return
    var names = [
        String("unpartitioned"),
        String("ident_part"),
        String("deletes_v2"),
        String("evolved"),
    ]
    var lazy = ScanOptions()
    lazy.lazy = True
    var rows = 0
    for k in range(len(names)):
        var want = encoded_rows(fixture_scan(names[k]).to_table())
        var t = Table.load(_s3_warehouse() + "/" + names[k], s3_io())
        var got = encoded_rows(t.scan().to_table())
        assert_equal(_diff(got, want), "", "s3 rows for " + names[k])
        assert_equal(len(got), len(want))
        var got_lazy = encoded_rows(t.scan().to_table(lazy))
        assert_equal(_diff(got_lazy, want), "", "s3 lazy rows for " + names[k])
        rows += len(got)
    print("    s3 rows:", rows, "over", len(names), "tables")


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


def _dv_writer_dir() raises -> String:
    var root = WRITE_ROOT + "/puffin"
    var io = FileIO.local()
    try:
        var existing = io.list(root)
        for k in range(len(existing)):
            io.delete(existing[k])
    except:
        pass
    makedirs(root, exist_ok=True)
    return root^


def test_puffin_writer_round_trips_through_the_reader() raises:
    """Write a Puffin file, then read it back with the reader that has only
    ever read other people's files.

    Two deletion vectors and one opaque blob, so the footer exercises the
    fields that differ: a DV carries `snapshot-id`/`sequence-number` of -1 and
    no `compression-codec`, and the other blob carries real ones.
    """
    var dir = _dv_writer_dir()
    var io = FileIO.local()
    var a = Bitmap64()
    a.add(UInt64(0))
    a.add(UInt64(2))
    a.add(UInt64(9))
    var b = Bitmap64()
    b.add(UInt64(1))

    var w = PuffinWriter()
    var i0 = w.add_deletion_vector(String("file:///t/data/a.parquet"), a)
    var i1 = w.add_deletion_vector(String("file:///t/data/b.parquet"), b)
    var payload = List[UInt8]()
    for k in range(64):
        payload.append(UInt8(k))
    var i2 = w.add_blob(
        String("apache-datasketches-theta-v1"),
        [1],
        Int64(77),
        Int64(5),
        Span(payload),
        String("lz4"),
        [String("ndv")],
        [String("12")],
    )
    assert_equal(i0, 0)
    assert_equal(i1, 1)
    assert_equal(i2, 2)
    var bytes = w.finish()
    var path = dir + "/written.puffin"
    io.write_all(path, Span(bytes))

    var pf = PuffinFile.open(io, path)
    assert_equal(len(pf.blobs), 3)
    assert_false(pf.footer_compressed)
    assert_equal(pf.created_by(), "iceberg.mojo")
    # Blob 0: the first vector, right after the leading magic.
    assert_equal(pf.blobs[0].type, BLOB_DELETION_VECTOR_V1)
    assert_equal(pf.blobs[0].offset, 4)
    assert_equal(pf.blobs[0].snapshot_id, -1)
    assert_equal(pf.blobs[0].sequence_number, -1)
    assert_equal(pf.blobs[0].compression_codec, "")
    assert_equal(pf.blobs[0].cardinality(), 3)
    assert_equal(pf.blobs[0].referenced_data_file(), "file:///t/data/a.parquet")
    # Blobs are laid end to end, so each offset is the previous one's end.
    assert_equal(pf.blobs[1].offset, pf.blobs[0].offset + pf.blobs[0].length)
    assert_equal(pf.blobs[2].offset, pf.blobs[1].offset + pf.blobs[1].length)
    assert_equal(pf.blobs[1].cardinality(), 1)

    # The vectors decode to exactly the positions that went in.
    var got = deleted_positions(
        io, path, pf.blobs[0].offset, pf.blobs[0].length
    )
    assert_equal(len(got), 3)
    assert_equal(Int(got[0]), 0)
    assert_equal(Int(got[1]), 2)
    assert_equal(Int(got[2]), 9)
    var got_b = deleted_positions(
        io, path, pf.blobs[1].offset, pf.blobs[1].length
    )
    assert_equal(len(got_b), 1)
    assert_equal(Int(got_b[0]), 1)

    # And the compressed blob is really an LZ4 frame, with its own metadata.
    assert_equal(pf.blobs[2].type, "apache-datasketches-theta-v1")
    assert_equal(pf.blobs[2].compression_codec, "lz4")
    assert_equal(pf.blobs[2].snapshot_id, 77)
    assert_equal(pf.blobs[2].sequence_number, 5)
    assert_equal(len(pf.blobs[2].fields), 1)
    assert_equal(pf.blobs[2].fields[0], 1)
    assert_equal(pf.blobs[2].property("ndv"), "12")
    var raw = read_blob_bytes(io, path, pf.blobs[2].offset, pf.blobs[2].length)
    var plain = decompress_blob(raw^, pf.blobs[2].compression_codec)
    assert_equal(len(plain), 64)
    for k in range(64):
        assert_equal(Int(plain[k]), k)


def test_puffin_writer_compresses_the_footer() raises:
    """`Flags` bit 0, and an LZ4-framed footer payload — the only footer
    compression the format defines.

    Thirty vectors, because a footer is worth compressing only once it is
    repetitive: at one blob the frame's own header costs more than the JSON
    it saves, which is why this library leaves the footer plain by default.
    """
    var dir = _dv_writer_dir()
    var io = FileIO.local()
    var w = PuffinWriter()
    for k in range(30):
        var bm = Bitmap64()
        bm.add(UInt64(k))
        bm.add(UInt64(k + 100))
        _ = w.add_deletion_vector(
            "file:///warehouse/db/t/data/00000-0-part-"
            + String(k)
            + ".parquet",
            bm,
        )
    var plain = w.finish(False)
    var packed = w.finish(True)
    assert_true(
        len(packed) < len(plain),
        "a repetitive footer compresses: "
        + String(len(packed))
        + " vs "
        + String(len(plain)),
    )
    var path = dir + "/packed.puffin"
    io.write_all(path, Span(packed))
    var pf = PuffinFile.open(io, path)
    assert_true(pf.footer_compressed)
    assert_equal(len(pf.blobs), 30)
    for k in range(30):
        assert_equal(pf.blobs[k].cardinality(), 2)
        var got = deleted_positions(
            io, path, pf.blobs[k].offset, pf.blobs[k].length
        )
        assert_equal(len(got), 2)
        assert_equal(Int(got[0]), k)
        assert_equal(Int(got[1]), k + 100)


def test_puffin_writer_refuses_an_unknown_codec() raises:
    var w = PuffinWriter()
    var payload = List[UInt8]()
    payload.append(UInt8(1))
    with assert_raises():
        _ = w.add_blob(
            String("apache-datasketches-theta-v1"),
            List[Int](),
            Int64(-1),
            Int64(-1),
            Span(payload),
            String("zstd"),
        )


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


# ══ reading rows ════════════════════════════════════════════════════════════
# The row oracles were produced by PyIceberg (all six filters, eight tables)
# and by DuckDB's iceberg extension (unfiltered, eight tables); see
# tools/oracle_rows.py for why neither covers all nine. Cells are compared in
# the oracle's exact encoding — doubles as their IEEE-754 bits, timestamps as
# integers — so a match is a byte match, not a formatting opinion.
comptime HEXDIGITS = String("0123456789abcdef")


def hex_of(bytes: List[UInt8]) -> String:
    var out = String("")
    for k in range(len(bytes)):
        out += String(HEXDIGITS[byte=Int(bytes[k] >> 4)])
        out += String(HEXDIGITS[byte=Int(bytes[k] & 0xF)])
    return out^


def oracle_cell(d: Datum) raises -> String:
    """The encoding tools/oracle_rows.py writes, so the two can be compared."""
    if not d.valid:
        return String("\x00null")  # a sentinel that no real cell can produce
    if d.kind == P_BOOLEAN:
        return String("true") if d.i != 0 else String("false")
    if d.kind == P_FLOAT:
        var bits = UInt32(bitcast[DType.uint32](Float32(d.f)))
        var b = List[UInt8]()
        for k in range(4):
            b.append(UInt8((bits >> UInt32(8 * (3 - k))) & 0xFF))
        return hex_of(b)
    if d.kind == P_DOUBLE:
        var bits = UInt64(bitcast[DType.uint64](d.f))
        var b = List[UInt8]()
        for k in range(8):
            b.append(UInt8((bits >> UInt64(8 * (7 - k))) & 0xFF))
        return hex_of(b)
    if d.kind == P_STRING:
        return d.s
    if d.kind == P_UUID:
        return uuid_text(d.b)
    if d.kind == P_BINARY or d.kind == P_FIXED:
        return hex_of(d.b)
    if d.kind == P_DECIMAL:
        return decimal_text(d.b, d.scale)
    return String(d.i)


def encoded_rows(result: ScanResult) raises -> List[String]:
    """Every row as one comparable string, sorted the way the oracle sorts."""
    var rows = List[String]()
    for r in range(result.num_rows()):
        var line = String("")
        for c in range(result.num_columns()):
            if c > 0:
                line += "\x01"
            line += oracle_cell(result.value(r, c))
        rows.append(line^)
    _sort_strings(rows)
    return rows^


def oracle_rows(path: String) raises -> Tuple[List[String], List[String]]:
    """`(columns, rows)` from one oracle file, in the same encoding."""
    var doc = parse_json(read_file(path))
    var cols = List[String]()
    var ci = doc.get(doc.root, "columns")
    for k in range(doc.size(ci)):
        cols.append(doc.as_string(doc.at(ci, k)))
    var rows = List[String]()
    var ri = doc.get(doc.root, "rows")
    for k in range(doc.size(ri)):
        var row = doc.at(ri, k)
        var line = String("")
        for c in range(doc.size(row)):
            if c > 0:
                line += "\x01"
            var cell = doc.at(row, c)
            if doc.is_null(cell):
                line += String("\x00null")
            else:
                line += doc.as_string(cell)
        rows.append(line^)
    _sort_strings(rows)
    return (cols^, rows^)


def _sort_strings(mut l: List[String]):
    for i in range(1, len(l)):
        var j = i
        while j > 0 and l[j] < l[j - 1]:
            l.swap_elements(j, j - 1)
            j -= 1


def _diff(mine: List[String], theirs: List[String]) -> String:
    var out = String("")
    var n = len(mine) if len(mine) < len(theirs) else len(theirs)
    for k in range(n):
        if mine[k] != theirs[k]:
            out += "\n    mine:   " + mine[k]
            out += "\n    oracle: " + theirs[k]
            return out^
    if len(mine) != len(theirs):
        out += (
            "\n    counts differ: "
            + String(len(mine))
            + " vs "
            + String(len(theirs))
        )
    return out^


def test_to_table_matches_pyiceberg_rows() raises:
    """Gate (a): every table x every filter, row for row, against PyIceberg."""
    var tables = all_fixture_table_names()
    var cases = 0
    var rows_compared = 0
    var failures = String("")
    for t in range(len(tables)):
        var table = tables[t]
        for k in range(6):
            var path = (
                FIXTURES
                + "/"
                + table
                + "/oracle/rows_pyiceberg_"
                + String(k)
                + ".json"
            )
            if not _file_exists(path):
                continue
            var want = oracle_rows(path)
            var dsl = read_file(
                FIXTURES
                + "/"
                + table
                + "/oracle/plan_"
                + String(k)
                + ".filter.txt"
            ).strip()
            var got = fixture_scan(table).filter(String(dsl)).to_table()
            var mine = encoded_rows(got)
            cases += 1
            rows_compared += len(mine)
            if len(mine) != len(want[1]) or _diff(mine, want[1]) != "":
                failures += (
                    "\n  "
                    + table
                    + " filter "
                    + String(k)
                    + " "
                    + String(dsl)
                    + _diff(mine, want[1])
                )
    print(
        "    rows vs PyIceberg:",
        cases,
        "cases,",
        rows_compared,
        "rows",
    )
    assert_equal(failures, "", "row mismatches vs PyIceberg:" + failures)
    assert_equal(
        cases,
        65,
        (
            "expected 11 tables x 6 filters, less the 1\n"
            "        PyIceberg refuses (is-null on a list)"
        ),
    )


def test_to_table_matches_duckdb_rows() raises:
    """Gate (a), second oracle: DuckDB's iceberg extension, unfiltered."""
    var tables = all_fixture_table_names()
    var cases = 0
    var rows_compared = 0
    var failures = String("")
    for t in range(len(tables)):
        var table = tables[t]
        var path = FIXTURES + "/" + table + "/oracle/rows_duckdb.json"
        if not _file_exists(path):
            continue
        var want = oracle_rows(path)
        var got = fixture_scan(table).to_table()
        var mine = encoded_rows(got)
        cases += 1
        rows_compared += len(mine)
        if len(mine) != len(want[1]) or _diff(mine, want[1]) != "":
            failures += "\n  " + table + _diff(mine, want[1])
    print("    rows vs DuckDB:", cases, "tables,", rows_compared, "rows")
    assert_equal(failures, "", "row mismatches vs DuckDB:" + failures)
    assert_equal(cases, 12, "expected 12 tables with a DuckDB oracle")


def test_nested_rows_match_duckdb_filters() raises:
    """Gate (a), nested: three tables x six filters, against DuckDB 1.5.5.

    DuckDB reads the nested columns and evaluates the predicate itself, so
    this is a second and wholly independent answer — and the only one for
    `is-null` on a list, which PyIceberg 0.11.1 cannot even project
    ("Cannot explicitly project List or Map types").
    """
    var tables = nested_fixture_table_names()
    var cases = 0
    var rows_compared = 0
    var failures = String("")
    for t in range(len(tables)):
        var table = tables[t]
        for k in range(6):
            var path = (
                FIXTURES
                + "/"
                + table
                + "/oracle/rows_duckdb_"
                + String(k)
                + ".json"
            )
            if not _file_exists(path):
                continue
            var want = oracle_rows(path)
            var dsl = read_file(
                FIXTURES
                + "/"
                + table
                + "/oracle/plan_"
                + String(k)
                + ".filter.txt"
            ).strip()
            var got = fixture_scan(table).filter(String(dsl)).to_table()
            var mine = encoded_rows(got)
            cases += 1
            rows_compared += len(mine)
            if len(mine) != len(want[1]) or _diff(mine, want[1]) != "":
                failures += (
                    "\n  "
                    + table
                    + " filter "
                    + String(k)
                    + " "
                    + String(dsl)
                    + _diff(mine, want[1])
                )
    print("    nested rows vs DuckDB:", cases, "cases,", rows_compared, "rows")
    assert_equal(failures, "", "nested row mismatches vs DuckDB:" + failures)
    assert_equal(cases, 18, "expected 3 nested tables x 6 filters")


def test_nested_subfield_projection_matches_pyiceberg() raises:
    """Gate (a), projection: `select(["a.b", "c"])` gives back the column `a`
    holding only `b`, with the same cells PyIceberg produces.

    PyIceberg returns the columns in schema order whatever order they were
    asked in, so the comparison is by name: each oracle column is looked up
    in our result and its cells compared.
    """
    var tables = nested_fixture_table_names()
    var cases = 0
    var failures = String("")
    for t in range(len(tables)):
        var table = tables[t]
        for k in range(8):
            var path = (
                FIXTURES
                + "/"
                + table
                + "/oracle/rows_project_"
                + String(k)
                + ".json"
            )
            if not _file_exists(path):
                continue
            var doc = parse_json(read_file(path))
            var sel = List[String]()
            var si = doc.get(doc.root, "select")
            for j in range(doc.size(si)):
                sel.append(doc.as_string(doc.at(si, j)))
            var want = oracle_rows(path)
            var got = fixture_scan(table).select(sel.copy()).to_table()
            # Same columns, whatever order each side puts them in.
            var mine_names = List[String]()
            for c in range(got.num_columns()):
                mine_names.append(got.name(c))
            _sort_strings(mine_names)
            var want_names = want[0].copy()
            _sort_strings(want_names)
            var same_names = len(mine_names) == len(want_names)
            if same_names:
                for c in range(len(mine_names)):
                    if mine_names[c] != want_names[c]:
                        same_names = False
            if not same_names:
                failures += (
                    "\n  "
                    + table
                    + " projection "
                    + String(k)
                    + ": columns differ"
                )
                cases += 1
                continue
            var mine = _rows_in_oracle_order(got, want[0])
            cases += 1
            if len(mine) != len(want[1]) or _diff(mine, want[1]) != "":
                failures += (
                    "\n  "
                    + table
                    + " projection "
                    + String(k)
                    + _diff(mine, want[1])
                )
    print("    nested sub-field projections vs PyIceberg:", cases, "cases")
    assert_equal(failures, "", "projection mismatches:" + failures)
    assert_equal(cases, 7, "expected 7 sub-field projections")


def _rows_in_oracle_order(
    result: ScanResult, order: List[String]
) raises -> List[String]:
    """`encoded_rows`, with the columns permuted into the oracle's order."""
    var slots = List[Int]()
    for k in range(len(order)):
        var at = -1
        for c in range(result.num_columns()):
            if result.name(c) == order[k]:
                at = c
                break
        slots.append(at)
    var rows = List[String]()
    for r in range(result.num_rows()):
        var line = String("")
        for k in range(len(slots)):
            if k > 0:
                line += "\x01"
            line += oracle_cell(result.value(r, slots[k]))
        rows.append(line^)
    _sort_strings(rows)
    return rows^


def test_nested_batches_export_over_the_c_data_interface() raises:
    """Gate (c): a nested scan batch is exportable as it stands.

    A struct, a list and a map each come out of the scan as a tree in the
    column's own arena; `to_batch` renumbers them into the batch's arena and
    `export_c` walks the children. The format strings are the Arrow ones —
    `+s`, `+l`, `+m` — and a map's child is the two-field `entries` struct.
    """
    var batches = fixture_scan("nested_v2").to_batches()
    assert_true(len(batches) > 0)
    var rows = 0
    for k in range(len(batches)):
        ref b = batches[k]
        assert_equal(b.num_columns(), 8)
        rows += b.num_rows
        for c in range(b.num_columns()):
            var exported = b.export_c(c)
            assert_true(exported.array != 0, "ArrowArray address")
            assert_true(exported.schema != 0, "ArrowSchema address")
        assert_equal(b.type(1).id, AT_STRUCT)
        assert_equal(b.type(2).id, AT_LIST)
        assert_equal(b.type(3).id, AT_MAP)
        assert_equal(len(b.column(3).children), 1)
        assert_equal(len(b.child(b.roots[3], 0).children), 2)
    assert_equal(rows, fixture_scan("nested_v2").to_table().num_rows())


def test_nested_is_null_on_containers() raises:
    """`is-null` binds against a struct, a list and a map; anything else on a
    container is refused, and so is any predicate inside one."""
    var scan = fixture_scan("nested_v2")
    assert_equal(scan.filter('["is-null","tags"]').to_table().num_rows(), 2)
    assert_equal(scan.filter('["is-null","props"]').to_table().num_rows(), 1)
    assert_equal(scan.filter('["is-null","addr"]').to_table().num_rows(), 2)
    assert_equal(scan.filter('["not-null","deep"]').to_table().num_rows(), 7)
    with assert_raises():
        _ = scan.filter('["=","tags","a"]').to_table()
    with assert_raises():
        _ = scan.filter('["=","tags.element","a"]').to_table()
    with assert_raises():
        _ = scan.filter('["=","props.value",1]').to_table()


def test_nested_schema_evolution_inside_a_struct() raises:
    """A field added, a field renamed and an `int` promoted to `long`, all
    inside a struct, resolved by field id against the older file."""
    var schema = fixture_scan("nested_evo_v2").current_schema()
    assert_equal(
        schema.store.type_name(schema.find_by_name("addr.zip").type), "long"
    )
    assert_true(schema.has_name("addr.town"))
    assert_true(schema.has_name("addr.country"))
    assert_false(schema.has_name("addr.city"))

    var rows = fixture_scan("nested_evo_v2").to_table()
    var by_id = Dict[Int64, String]()
    for r in range(rows.num_rows()):
        by_id[rows.value(r, 0).i] = rows.cell(r, 1)
    # Written before the change: `country` is absent from the file, so it
    # reads as null; `town` is the renamed `city`; `zip` was an int32.
    assert_equal(by_id[1], '{"town":"eu","zip":10,"country":null}')
    assert_equal(by_id[3], "null")
    # Written after: a value no int32 could hold.
    assert_equal(by_id[7], '{"town":"apac","zip":7000000000,"country":"sg"}')


def test_nested_partition_source_is_read_and_pruned() raises:
    """`identity(addr.city)` — a partition field whose source is nested."""
    var t = load_fixture_metadata("nested_part_v2")
    var spec = t.spec_by_id(t.default_spec_id)
    assert_equal(len(spec.fields), 1)
    var schema = t.schema()
    assert_equal(schema.name_of(spec.fields[0].source_id), "addr.city")

    # The metrics and partition evaluators still prune on the nested leaf.
    var all_files = len(fixture_scan("nested_part_v2").plan_files())
    var eu = fixture_scan("nested_part_v2").filter('["=","addr.city","eu"]')
    assert_true(len(eu.plan_files()) < all_files)
    assert_equal(eu.to_table().num_rows(), 3)


def test_nested_sub_field_projection_prunes_the_read() raises:
    """Selecting `addr.city` gives the column `addr` with only `city` in it."""
    var rows = (
        fixture_scan("nested_v2")
        .select([String("addr.city"), String("id")])
        .to_table()
    )
    assert_equal(rows.num_columns(), 2)
    assert_equal(rows.name(0), "addr")
    assert_equal(rows.name(1), "id")
    var seen = String("")
    for r in range(rows.num_rows()):
        if rows.value(r, 0).i == 0:
            pass
        seen += rows.cell(r, 0) + ";"
    assert_true(seen.find('"zip"') < 0, "zip must not be read: " + seen)
    assert_true(seen.find('{"city":"eu"}') >= 0, seen)

    # The whole column, for comparison.
    var whole = fixture_scan("nested_v2").select([String("addr")]).to_table()
    assert_equal(whole.num_columns(), 1)
    assert_true(whole.cell(0, 0).find('"zip"') >= 0)

    # Metadata columns sit beside a nested one exactly as beside a flat one.
    var meta = (
        fixture_scan("nested_v2")
        .select(
            [
                String("tags"),
                String("addr.zip"),
                String("_file"),
                String("_pos"),
            ]
        )
        .to_table()
    )
    assert_equal(meta.num_columns(), 4)
    assert_equal(meta.name(2), "_file")
    assert_equal(meta.name(3), "_pos")
    assert_equal(meta.num_rows(), 8)
    for r in range(meta.num_rows()):
        assert_true(meta.value(r, 2).s.endswith(".parquet"))
        assert_true(meta.value(r, 3).i >= 0)
        assert_true(
            meta.cell(r, 1).find('"city"') < 0, "city was not asked for"
        )


def test_nested_cells_render_as_json_and_csv() raises:
    """A nested cell is JSON in `to_json`, and the same JSON as a CSV field."""
    var rows = (
        fixture_scan("nested_v2")
        .filter('["=","id",1]')
        .select([String("id"), String("props"), String("matrix")])
        .to_table()
    )
    assert_equal(rows.num_rows(), 1)
    assert_equal(rows.cell(0, 1), '{"keys":["x","y"],"values":[1,2]}')
    assert_equal(rows.cell(0, 2), "[[1,2],[3]]")
    assert_equal(
        rows.to_json(),
        (
            '[{"id":1,"props":{"keys":["x","y"],"values":[1,2]},'
            '"matrix":[[1,2],[3]]}]'
        ),
    )
    var csv = rows.to_csv()
    assert_true(
        csv.find('"{""keys"":[""x"",""y""],""values"":[1,2]}"') >= 0, csv
    )


def _file_exists(path: String) -> Bool:
    try:
        with open(path, "r") as f:
            _ = f.read_bytes(1)
        return True
    except:
        return False


def test_position_deletes_remove_rows() raises:
    """Gate (b): the position-delete table, against the generator's report."""
    var doc = parse_json(
        read_file(FIXTURES + "/deletes_v2/oracle_delete_report.json")
    )
    var after = doc.get(doc.root, "rows_after_delete")
    var want = List[Int64]()
    for k in range(doc.size(after)):
        want.append(doc.as_int(doc.at(after, k)))

    var undeleted = fixture_scan("deletes_v2").to_table()
    var ids = List[Int64]()
    for r in range(undeleted.num_rows()):
        ids.append(undeleted.value(r, 0).i)
    _sort_int64(ids)
    assert_equal(len(ids), len(want))
    for k in range(len(want)):
        assert_equal(ids[k], want[k])

    # And the deleted rows really are gone: the data files hold more rows than
    # the scan returns.
    var tasks = fixture_scan("deletes_v2").plan_files()
    var stored: Int64 = 0
    var with_deletes = 0
    for k in range(len(tasks)):
        stored += tasks[k].data_file.record_count
        if len(tasks[k].delete_files) > 0:
            with_deletes += 1
    assert_true(stored > Int64(len(ids)))
    assert_true(with_deletes > 0)


def test_equality_deletes_match_duckdb() raises:
    """Gate (c): equality deletes, including `null` matching `null`.

    PyIceberg 0.11.1 cannot read this table at all, so DuckDB is the only
    oracle — and it is a real one: it returns ids 1, 3 and 4, which is what
    the spec says survives two delete files, one on `id` and one whose single
    `amount` value is NULL.
    """
    var want = oracle_rows(FIXTURES + "/eq_deletes_v2/oracle/rows_duckdb.json")
    var got = fixture_scan("eq_deletes_v2").to_table()
    var mine = encoded_rows(got)
    assert_equal(len(mine), 3)
    assert_equal(_diff(mine, want[1]), "")

    # The delete files are what we think they are.
    var tasks = fixture_scan("eq_deletes_v2").plan_files()
    var eq_files = 0
    var ids_seen = List[Int]()
    for k in range(len(tasks)):
        for j in range(len(tasks[k].delete_files)):
            if not tasks[k].delete_files[j].is_equality_delete():
                continue
            eq_files += 1
            for q in range(len(tasks[k].delete_files[j].equality_ids)):
                ids_seen.append(tasks[k].delete_files[j].equality_ids[q])
    assert_true(eq_files >= 2)
    var has_id = False
    var has_amount = False
    for k in range(len(ids_seen)):
        if ids_seen[k] == 1:
            has_id = True
        if ids_seen[k] == 3:
            has_amount = True
    assert_true(has_id)
    assert_true(has_amount)


def test_deletion_vector_removes_rows() raises:
    """Gate (d): the v3 deletion-vector table, against PyIceberg and DuckDB."""
    var pyi = oracle_rows(FIXTURES + "/dv_v3/oracle/rows_pyiceberg_0.json")
    var duck = oracle_rows(FIXTURES + "/dv_v3/oracle/rows_duckdb.json")
    var got = fixture_scan("dv_v3").to_table()
    var mine = encoded_rows(got)
    assert_equal(len(mine), 4)
    assert_equal(_diff(mine, pyi[1]), "")
    assert_equal(_diff(mine, duck[1]), "")
    # Six rows were written; two vectors removed one each.
    var tasks = fixture_scan("dv_v3").plan_files()
    var stored: Int64 = 0
    for k in range(len(tasks)):
        stored += tasks[k].data_file.record_count
    assert_equal(stored, 6)


def test_schema_evolution_reads() raises:
    """Gate (e): renamed, promoted and added columns."""
    var r = fixture_scan("evolved").to_table()
    assert_equal(r.num_columns(), 5)
    assert_equal(r.name(0), "id")
    assert_equal(r.name(1), "label")  # renamed from `name`
    assert_equal(r.name(2), "cnt")  # promoted int -> long
    assert_equal(r.name(4), "extra")  # added after the first snapshot
    var promoted = False
    var missing_extra = 0
    for row in range(r.num_rows()):
        # 5_000_000_000 does not fit in an int: reading it proves the
        # promotion happened rather than a truncation.
        if r.value(row, 2).i == 5000000000:
            promoted = True
        if not r.value(row, 4).valid:
            missing_extra += 1
    assert_true(promoted)
    # The three rows written before `extra` existed read as null, not as an
    # error and not as a default.
    assert_true(missing_extra >= 3)

    # Projection by the *new* name reaches the same column.
    var one = (
        fixture_scan("evolved")
        .select([String("label"), String("cnt")])
        .filter('["=","label","alpha"]')
        .to_table()
    )
    assert_equal(one.num_columns(), 2)
    assert_equal(one.num_rows(), 1)
    assert_equal(one.value(0, 0).s, "alpha")
    assert_equal(one.value(0, 1).i, 10)


def test_identity_partition_projection() raises:
    """A partition column is readable even though the file has it too."""
    var r = (
        fixture_scan("ident_part")
        .select([String("region"), String("id")])
        .filter('["=","region","apac"]')
        .to_table()
    )
    assert_equal(r.num_columns(), 2)
    assert_true(r.num_rows() > 0)
    for row in range(r.num_rows()):
        assert_equal(r.value(row, 0).s, "apac")


def test_metadata_columns() raises:
    var r = (
        fixture_scan("unpartitioned")
        .select(
            [
                String("id"),
                META_FILE,
                META_POS,
                META_SPEC_ID,
                META_PARTITION,
                META_LAST_UPDATED,
            ]
        )
        .to_table()
    )
    assert_equal(r.num_columns(), 6)
    assert_equal(r.name(1), META_FILE)
    for row in range(r.num_rows()):
        assert_true(r.value(row, 1).s.endswith(".parquet"))
        assert_true(r.value(row, 2).i >= 0)
        assert_equal(r.value(row, 3).i, 0)
        assert_equal(r.value(row, 4).s, "{}")
        assert_true(r.value(row, 5).i > 0)


def test_v3_row_lineage_columns() raises:
    """`_row_id` comes from the data file's `first_row_id` plus the position.

    The dv_v3 fixture assigns row ids on the first post-upgrade snapshot, so
    every surviving row has one and they are distinct.
    """
    var r = (
        fixture_scan("dv_v3")
        .select([String("id"), META_ROW_ID, META_POS])
        .to_table()
    )
    assert_equal(r.num_rows(), 4)
    var seen = List[Int64]()
    for row in range(r.num_rows()):
        assert_true(r.value(row, 1).valid, "every row should have a _row_id")
        var v = r.value(row, 1).i
        for k in range(len(seen)):
            assert_true(seen[k] != v, "_row_id values must be distinct")
        seen.append(v)


def test_scan_limit_and_batches() raises:
    var opts = ScanOptions()
    opts.limit = 2
    var r = fixture_scan("unpartitioned").to_table(opts)
    assert_equal(r.num_rows(), 2)

    var batches = fixture_scan("unpartitioned").to_batches()
    var total = 0
    for k in range(len(batches)):
        total += batches[k].num_rows
        assert_equal(batches[k].num_columns(), 6)
    assert_equal(total, 7)


def test_to_batch_round_trips() raises:
    """The Arrow batch carries the same values the result does."""
    var r = fixture_scan("unpartitioned").to_table()
    var batch = r.to_batch()
    assert_equal(batch.num_rows, r.num_rows())
    assert_equal(batch.num_columns(), r.num_columns())
    var ids = batch.column_i64(0)
    assert_equal(len(ids[0]), r.num_rows())
    for row in range(r.num_rows()):
        assert_equal(ids[0][row], r.value(row, 0).i)
    var regions = batch.column_str(1)
    for row in range(r.num_rows()):
        assert_equal(regions[0][row], r.value(row, 1).s)
    var amounts = batch.column_f64(2)
    for row in range(r.num_rows()):
        assert_equal(amounts[1][row], r.value(row, 2).valid)
    var oks = batch.column_bool(4)
    for row in range(r.num_rows()):
        if r.value(row, 4).valid:
            assert_equal(oks[0][row], r.value(row, 4).i != 0)


def test_lazy_read_matches_eager() raises:
    """Fetching only the footer and the surviving row groups reads the same
    rows as downloading the file."""
    var tables = all_fixture_table_names()
    var opts = ScanOptions()
    opts.lazy = True
    var checked = 0
    for t in range(len(tables)):
        var eager = encoded_rows(fixture_scan(tables[t]).to_table())
        var lazy = encoded_rows(fixture_scan(tables[t]).to_table(opts))
        assert_equal(len(lazy), len(eager), "lazy row count for " + tables[t])
        assert_equal(_diff(lazy, eager), "", "lazy rows for " + tables[t])
        checked += 1
    assert_equal(checked, 12)


def test_name_mapping_parses() raises:
    var m = NameMapping.parse(
        '[{"field-id":1,"names":["id","identifier"]},'
        '{"field-id":2,"names":["data"],"fields":['
        '{"field-id":3,"names":["inner"]}]}]'
    )
    assert_equal(m.id_for("id"), 1)
    assert_equal(m.id_for("identifier"), 1)
    assert_equal(m.id_for("data"), 2)
    assert_equal(m.id_for("data.inner"), 3)
    assert_equal(m.id_for("nope"), -1)
    assert_equal(m.name_for(2), "data")


def test_result_output_formats() raises:
    var r = (
        fixture_scan("unpartitioned")
        .select([String("id"), String("region")])
        .filter('["=","id",1]')
        .to_table()
    )
    assert_equal(r.to_csv(), "id,region\n1,eu\n")
    assert_equal(r.to_json(), '[{"id":1,"region":"eu"}]')


def _sort_int64(mut l: List[Int64]):
    for i in range(1, len(l)):
        var j = i
        while j > 0 and l[j] < l[j - 1]:
            l.swap_elements(j, j - 1)
            j -= 1


# ══ the projection fallbacks ════════════════════════════════════════════════
# Rules 2 and 3 of the spec's column projection — an identity partition value,
# and `schema.name-mapping.default` — cannot be reached through a fixture whose
# data files carry the right field ids, because rule 1 always wins. They are
# reached here by handing `read_data_file` a schema whose ids are deliberately
# *wrong* for the file, which is exactly the situation both rules exist for.
comptime SHIFTED_SCHEMA = String(
    '{"type":"struct","schema-id":0,"fields":['
    '{"id":101,"name":"id","required":true,"type":"long"},'
    '{"id":102,"name":"region","required":true,"type":"string"},'
    '{"id":103,"name":"amount","required":false,"type":"double"},'
    '{"id":104,"name":"missing","required":false,"type":"string",'
    '"initial-default":"filled-in"},'
    '{"id":105,"name":"absent","required":false,"type":"int"}]}'
)
"""The `ident_part` columns under ids no data file has ever heard of."""


def _one_ident_task() raises -> FileScanTask:
    var tasks = (
        fixture_scan("ident_part").filter('["=","region","apac"]').plan_files()
    )
    assert_true(len(tasks) > 0)
    return tasks[0].copy()


def test_projection_falls_back_to_name_mapping() raises:
    """Rule 3: a file whose ids do not match is read through the name mapping.
    """
    var schema = Schema.parse(SHIFTED_SCHEMA)
    var mapping = NameMapping.parse(
        '[{"field-id":101,"names":["id"]},'
        '{"field-id":102,"names":["region"]},'
        '{"field-id":103,"names":["amount"]}]'
    )
    var task = _one_ident_task()
    var rows = read_data_file_table(
        fixture_io(),
        task.data_file,
        List[DataFile](),
        task.data_sequence_number,
        PartitionSpec.unpartitioned(0),
        schema,
        [101, 102, 103, 104, 105],
        List[String](),
        mapping,
        String('["true"]'),
        True,
        ScanOptions(),
    )
    assert_true(rows.num_rows() > 0)
    assert_equal(rows.num_columns(), 5)
    for r in range(rows.num_rows()):
        # 101/102/103 came through the mapping and really hold values.
        assert_true(rows.value(r, 0).valid)
        assert_equal(rows.value(r, 1).s, "apac")
        # Rule 4: a column the file lacks, with an `initial-default`.
        assert_equal(rows.value(r, 3).s, "filled-in")
        # Rule 5: a column the file lacks, with no default at all.
        assert_false(rows.value(r, 4).valid)


def test_projection_falls_back_to_partition_value() raises:
    """Rule 2: an identity partition value stands in for a missing column.

    This is the metadata-only Hive migration case — the data file never had
    the partition column in it, and the manifest's partition tuple is the only
    place the value exists.
    """
    var schema = Schema.parse(SHIFTED_SCHEMA)
    var task = _one_ident_task()
    # `region` at id 102, identity-partitioned, with no name mapping: the only
    # way to resolve it is the partition tuple.
    var spec = PartitionSpec(
        0,
        [
            PartitionField.single(
                102, 1000, String("region"), Transform.identity()
            )
        ],
    )
    var rows = read_data_file_table(
        fixture_io(),
        task.data_file,
        List[DataFile](),
        task.data_sequence_number,
        spec,
        schema,
        [102, 101],
        List[String](),
        NameMapping(),
        String('["true"]'),
        True,
        ScanOptions(),
    )
    assert_true(rows.num_rows() > 0)
    for r in range(rows.num_rows()):
        assert_equal(rows.value(r, 0).s, "apac", "from the partition tuple")
        # 101 has neither a matching id, a mapping, nor a default: null.
        assert_false(rows.value(r, 1).valid)


# ── the write path ──────────────────────────────────────────────────────────
#
# Everything below writes into build/write-tests/<case>/ and reads it back
# through this library. The *external* gate — PyIceberg 0.11.1 and DuckDB
# 1.5.5 reading the same tables, and PyIceberg appending to them — lives in
# tools/verify_written.py, which `pixi run verify-writes` runs; it needs a
# PyIceberg venv, which the unit suite deliberately does not.
comptime WRITE_ROOT = String("build/write-tests")

comptime WRITE_SCHEMA = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"region","required":true,"type":"string"},'
    '{"id":3,"name":"amount","required":false,"type":"double"},'
    '{"id":4,"name":"ts","required":false,"type":"timestamp"},'
    '{"id":5,"name":"ok","required":false,"type":"boolean"}]}'
)

comptime WRITE_DAY: Int64 = 19723
comptime WRITE_MICROS_PER_DAY: Int64 = 86400000000


def write_region(i: Int) raises -> String:
    var names = String("eu,us,apac,latam,emea").split(",")
    return String(names[i % 5])


def write_batch(schema: Schema, start: Int, n: Int) raises -> RecordBatch:
    var ids = ColumnBuilder.of(schema, 1)
    var region = ColumnBuilder.of(schema, 2)
    var amount = ColumnBuilder.of(schema, 3)
    var ts = ColumnBuilder.of(schema, 4)
    var ok = ColumnBuilder.of(schema, 5)
    for k in range(n):
        var i = start + k
        ids.add(Datum.long_(Int64(i)))
        region.add(Datum.string_(write_region(i)))
        if i % 4 == 0:
            amount.add_null()
        else:
            amount.add(Datum.double_(Float64(i) * 1.5))
        ts.add(
            Datum.integral(
                ts.kind,
                (WRITE_DAY + Int64(i % 3)) * WRITE_MICROS_PER_DAY
                + Int64(i) * 1000,
            )
        )
        if i % 5 == 0:
            ok.add_null()
        else:
            ok.add(Datum.bool_(i % 2 == 0))
    return batch_of([ids^, region^, amount^, ts^, ok^])


def fresh_sql_catalog(scenario: String) raises -> SqlCatalog:
    """A SQL catalog of this test's own: a fresh sqlite file and warehouse."""
    var root = WRITE_ROOT + "/sql-" + scenario
    var io = FileIO.local()
    try:
        var existing = io.list(root)
        for k in range(len(existing)):
            io.delete(existing[k])
    except:
        pass
    makedirs(root, exist_ok=True)
    var db_path = root + "/catalog.db"
    try:
        io.delete(db_path)
    except:
        pass
    return SqlCatalog.local(
        "default", "sqlite:///" + db_path, root + "/warehouse"
    )


def test_sqlite_path_from_uri() raises:
    assert_equal(
        sqlite_path_from_uri("sqlite:///rel/catalog.db"), "rel/catalog.db"
    )
    assert_equal(
        sqlite_path_from_uri("sqlite:////abs/catalog.db"), "/abs/catalog.db"
    )
    assert_equal(sqlite_path_from_uri(":memory:"), ":memory:")
    assert_equal(
        sqlite_path_from_uri("/already/a/path.db"), "/already/a/path.db"
    )


def test_sql_catalog_namespaces_and_properties() raises:
    var cat = fresh_sql_catalog("namespaces")
    assert_false(cat.namespace_exists("db"))
    cat.create_namespace("db")
    assert_true(cat.namespace_exists("db"))
    with assert_raises():
        cat.create_namespace("db")

    cat.create_namespace("db.sub", {"owner": "marius"})
    assert_true(cat.namespace_exists("db.sub"))
    # "db.sub" must not be confused with a namespace merely prefixed by "db",
    # e.g. "db_other" — the point of escaping "_" in the LIKE pattern.
    cat.create_namespace("db_other")
    assert_false(cat.namespace_exists("db.sub.nope"))

    var top = cat.list_namespaces()
    _sort_strings(top)
    assert_equal(len(top), 2, "db and db_other at the top level")
    assert_equal(top[0], "db")
    assert_equal(top[1], "db_other")

    var children = cat.list_namespaces("db")
    assert_equal(len(children), 1)
    assert_equal(children[0], "db.sub")

    var props = cat.load_namespace_properties("db.sub")
    assert_equal(props["owner"], "marius")

    var summary = cat.update_namespace_properties(
        "db.sub", List[String](), {"owner": "someone-else", "team": "lake"}
    )
    assert_equal(len(summary.updated), 2)
    props = cat.load_namespace_properties("db.sub")
    assert_equal(props["owner"], "someone-else")
    assert_equal(props["team"], "lake")

    var removed_summary = cat.update_namespace_properties(
        "db.sub", ["team", "nonexistent"]
    )
    assert_equal(len(removed_summary.removed), 1)
    assert_equal(len(removed_summary.missing), 1)
    assert_false("team" in cat.load_namespace_properties("db.sub"))

    # `drop_namespace` only refuses a namespace with *direct* tables — the
    # same "not empty" check PyIceberg's `SqlCatalog` makes, which does not
    # look at child namespaces at all. A table living directly in "db"
    # exercises that; "db.sub" having no table of its own does not block it.
    var schema = Schema.parse(WRITE_SCHEMA)
    _ = cat.create_table("db", "t", schema)
    with assert_raises():
        cat.drop_namespace("db")  # "db" itself has a direct table
    cat.drop_table("db", "t")
    cat.drop_namespace("db.sub")
    cat.drop_namespace("db")
    assert_false(cat.namespace_exists("db"))


def test_sql_catalog_tables_crud() raises:
    var cat = fresh_sql_catalog("tables")
    cat.create_namespace("db")
    var schema = Schema.parse(WRITE_SCHEMA)
    assert_false(cat.table_exists("db", "orders"))

    var created = cat.create_table("db", "orders", schema)
    assert_equal(created.metadata.format_version, 2)
    assert_false(created.metadata.has_current_snapshot)
    assert_true(cat.table_exists("db", "orders"))

    with assert_raises():
        _ = cat.create_table("db", "orders", schema)
    with assert_raises():
        _ = cat.create_table("nonexistent-ns", "t", schema)

    var names = cat.list_tables("db")
    assert_equal(len(names), 1)
    assert_equal(names[0], "orders")

    var loaded = cat.load_table("db", "orders")
    assert_equal(loaded.metadata.table_uuid, created.metadata.table_uuid)

    var renamed = cat.rename_table("db", "orders", "db", "orders2")
    assert_equal(renamed.metadata.table_uuid, created.metadata.table_uuid)
    assert_false(cat.table_exists("db", "orders"))
    assert_true(cat.table_exists("db", "orders2"))
    with assert_raises():
        _ = cat.rename_table("db", "nonexistent", "db", "orders3")

    cat.drop_table("db", "orders2")
    assert_false(cat.table_exists("db", "orders2"))
    with assert_raises():
        cat.drop_table("db", "orders2")


def test_sql_catalog_append_delete_overwrite() raises:
    var cat = fresh_sql_catalog("commit")
    cat.create_namespace("db")
    var schema = Schema.parse(WRITE_SCHEMA)
    _ = cat.create_table(
        "db",
        "t",
        schema,
        PartitionSpec.unpartitioned(),
        Dict[String, String](),
        3,
    )

    var batches = List[RecordBatch]()
    batches.append(write_batch(schema, 0, 12))
    var after_append = cat.append("db", "t", batches)
    assert_equal(len(after_append.metadata.snapshots), 1)
    assert_equal(cat.load_table("db", "t").scan().to_table().num_rows(), 12)

    var after_delete = cat.delete_where(
        "db", "t", String('["=","region","eu"]'), MODE_MERGE_ON_READ
    )
    assert_equal(after_delete.metadata.current_snapshot().operation(), "delete")
    var back = cat.load_table("db", "t")
    assert_equal(back.scan().to_table().num_rows(), 9)
    var deletes = delete_files_of(back)
    assert_equal(len(deletes), 1)
    assert_true(deletes[0].is_deletion_vector())

    var fresh = List[RecordBatch]()
    fresh.append(write_batch(schema, 50, 2))
    var after_overwrite = cat.overwrite("db", "t", fresh)
    assert_equal(
        after_overwrite.metadata.current_snapshot().operation(), "overwrite"
    )
    var final = cat.load_table("db", "t")
    assert_equal(final.scan().to_table().num_rows(), 2)
    assert_equal(len(delete_files_of(final)), 0)


def test_sql_catalog_dynamic_partition_overwrite() raises:
    var cat = fresh_sql_catalog("dynamic")
    cat.create_namespace("db")
    var schema = Schema.parse(WRITE_SCHEMA)
    var spec = PartitionSpec(
        0,
        [
            PartitionField.single(
                2, 1000, String("region"), parse_transform("identity")
            )
        ],
    )
    _ = cat.create_table("db", "t", schema, spec)
    var first = List[RecordBatch]()
    first.append(write_batch(schema, 0, 10))
    _ = cat.append("db", "t", first)
    assert_equal(cat.load_table("db", "t").scan().to_table().num_rows(), 10)

    # id 0 -> region "eu" (i % 5 == 0) and nothing else, so only the "eu"
    # partition is touched.
    var only_eu = List[RecordBatch]()
    only_eu.append(write_batch(schema, 0, 1))
    var after = cat.dynamic_partition_overwrite("db", "t", only_eu)
    assert_equal(after.metadata.current_snapshot().operation(), "overwrite")
    # The other four partitions' eight original rows survive untouched; "eu"'s
    # original two rows are replaced by the one new row: 8 + 1 = 9.
    assert_equal(cat.load_table("db", "t").scan().to_table().num_rows(), 9)


def test_sql_catalog_guarded_commit_rejects_a_stale_pointer() raises:
    """The atomic swap PyIceberg's `SqlCatalog` performs: an `UPDATE ... WHERE
    metadata_location = <the value this attempt read>` that a concurrent
    writer's completed commit makes affect zero rows.

    This simulates exactly that race — one writer reads the table, a second
    (this test, standing in for it) commits first and moves the pointer, then
    the first writer's guarded swap is attempted against the now-stale
    location it originally read. It must fail without touching the row a
    successful commit just wrote.
    """
    var cat = fresh_sql_catalog("conflict")
    cat.create_namespace("db")
    var schema = Schema.parse(WRITE_SCHEMA)
    _ = cat.create_table("db", "t", schema)
    var stale = cat.load_table("db", "t")  # metadata_location == v0

    var batches = List[RecordBatch]()
    batches.append(write_batch(schema, 0, 6))
    var after = cat.append("db", "t", batches)  # v0 -> v1, the real commit

    # The stale reader's swap must be refused: v0 no longer matches the row.
    var swapped = cat._guarded_swap(
        "db", "t", stale.metadata_location, "bogus-location-must-not-land"
    )
    assert_false(swapped, "a guarded swap against a stale pointer must fail")

    # And the real commit's pointer must be exactly what it was, untouched.
    var still = cat.load_table("db", "t")
    assert_equal(still.metadata_location, after.metadata_location)
    assert_equal(still.scan().to_table().num_rows(), 6)


def test_sql_catalog_retries_past_its_own_stale_read() raises:
    """`commit_append` reloads and retries on a lost race rather than
    surfacing it — the same shape as the REST catalog's 409 handling, just
    against the guarded `UPDATE` instead of a status code."""
    var cat = fresh_sql_catalog("retry")
    cat.create_namespace("db")
    var schema = Schema.parse(WRITE_SCHEMA)
    _ = cat.create_table("db", "t", schema)

    var b1 = List[RecordBatch]()
    b1.append(write_batch(schema, 0, 3))
    _ = cat.append("db", "t", b1)
    var b2 = List[RecordBatch]()
    b2.append(write_batch(schema, 3, 3))
    _ = cat.append("db", "t", b2)

    var final = cat.load_table("db", "t")
    assert_equal(len(final.metadata.snapshots), 2)
    assert_equal(final.scan().to_table().num_rows(), 6)


def fresh_catalog(scenario: String) raises -> FilesystemCatalog:
    """A warehouse of this test's own, emptied first."""
    var root = WRITE_ROOT + "/" + scenario
    var io = FileIO.local()
    try:
        var existing = io.list(root)
        for k in range(len(existing)):
            io.delete(existing[k])
    except:
        pass
    makedirs(root, exist_ok=True)
    return FilesystemCatalog.local(root)


def build_written_table(
    scenario: String, shape: String, format_version: Int, appends: Int = 3
) raises -> Table:
    var catalog = fresh_catalog(scenario)
    var schema = Schema.parse(WRITE_SCHEMA)
    var spec = PartitionSpec.unpartitioned()
    if shape == "ident":
        spec = PartitionSpec(
            0,
            [
                PartitionField.single(
                    2, 1000, String("region"), parse_transform("identity")
                )
            ],
        )
    elif shape == "bucket":
        spec = PartitionSpec(
            0,
            [
                PartitionField.single(
                    1, 1000, String("id_bucket"), parse_transform("bucket[4]")
                )
            ],
        )
    elif shape == "day":
        spec = PartitionSpec(
            0,
            [
                PartitionField.single(
                    4, 1000, String("ts_day"), parse_transform("day")
                )
            ],
        )
    var table = catalog.create_table(
        String("db"),
        String("t"),
        schema,
        spec,
        Dict[String, String](),
        format_version,
    )
    for b in range(appends):
        var batches = List[RecordBatch]()
        batches.append(write_batch(schema, b * 6, 6))
        var tx = table.new_append()
        tx.add_batches(batches)
        _ = tx.commit()
        table.refresh()
    return table^


comptime NESTED_WRITE_SCHEMA = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"addr","required":false,"type":{"type":"struct","fields":['
    '{"id":10,"name":"city","required":false,"type":"string"},'
    '{"id":11,"name":"zip","required":false,"type":"int"}]}},'
    '{"id":3,"name":"tags","required":false,"type":'
    '{"type":"list","element-id":20,"element":"string",'
    '"element-required":false}},'
    '{"id":4,"name":"props","required":false,"type":'
    '{"type":"map","key-id":30,"key":"string","value-id":31,"value":"long",'
    '"value-required":false}}]}'
)


def nested_write_batch(
    schema: Schema, start: Int, n: Int
) raises -> RecordBatch:
    """`n` rows from `start`, with a null struct, an empty list and a null map
    scattered through them so the containers are never all alike."""
    var ids = ColumnBuilder.of(schema, 1)
    var addr = NestedBuilder.of(schema, 2)
    var tags = NestedBuilder.of(schema, 3)
    var props = NestedBuilder.of(schema, 4)
    for k in range(n):
        var i = start + k
        ids.add(Datum.long_(Int64(i)))
        if i % 5 == 3:
            addr.add_null()
        else:
            addr.add(
                '{"city":"'
                + write_region(i)
                + '","zip":'
                + String(i * 10)
                + "}"
            )
        if i % 4 == 0:
            tags.add("[]")
        elif i % 4 == 1:
            tags.add_null()
        else:
            tags.add('["t' + String(i) + '","u"]')
        if i % 3 == 0:
            props.add_null()
        else:
            props.add('{"keys":["k"],"values":[' + String(i) + "]}")
    return batch_of_columns(
        [ids^.build_tree(), addr^.build(), tags^.build(), props^.build()]
    )


def nested_written_table(
    scenario: String, format_version: Int, shape: String = String("")
) raises -> Table:
    """A table with a struct, a list and a map, three appends of six rows."""
    var catalog = fresh_catalog(scenario)
    var schema = Schema.parse(NESTED_WRITE_SCHEMA)
    var spec = PartitionSpec.unpartitioned()
    if shape == "ident":
        # `identity(addr.city)` — the spec lets a partition source be nested.
        spec = PartitionSpec(
            0,
            [
                PartitionField.single(
                    10, 1000, String("city"), parse_transform("identity")
                )
            ],
        )
    elif shape == "bucket":
        spec = PartitionSpec(
            0,
            [
                PartitionField.single(
                    11,
                    1000,
                    String("zip_bucket"),
                    parse_transform("bucket[4]"),
                )
            ],
        )
    var table = catalog.create_table(
        String("db"),
        String("t"),
        schema,
        spec,
        Dict[String, String](),
        format_version,
    )
    for b in range(3):
        var batches = List[RecordBatch]()
        batches.append(nested_write_batch(schema, b * 6, 6))
        var tx = table.new_append()
        tx.add_batches(batches)
        _ = tx.commit()
        table.refresh()
    return table^


def nested_cells(table: Table) raises -> Dict[Int64, String]:
    """`{id: the row's other columns, as JSON}` — the shape a cell prints."""
    var rows = table.scan().to_table()
    var out = Dict[Int64, String]()
    for r in range(rows.num_rows()):
        var line = String("")
        for c in range(1, rows.num_columns()):
            if c > 1:
                line += "|"
            line += rows.cell(r, c)
        out[rows.value(r, 0).i] = line^
    return out^


def test_nested_append_round_trips_through_our_own_reader() raises:
    """Written by this library, read back by it: 18 rows of struct, list and
    map, with the null and the empty containers still distinct."""
    var table = nested_written_table(String("nest_append"), 2)
    var cells = nested_cells(table)
    assert_equal(len(cells), 18)
    assert_equal(
        cells[0],
        '{"city":"eu","zip":0}|[]|null',
    )
    assert_equal(
        cells[1],
        '{"city":"us","zip":10}|null|{"keys":["k"],"values":[1]}',
    )
    # id 3: a null struct beside a present list and a null map.
    assert_equal(cells[3], 'null|["t3","u"]|null')
    # The manifest carries metrics for every nested *leaf*, by its own id.
    var tasks = table.scan().plan_files()
    assert_true(len(tasks) > 0)
    ref df = tasks[0].data_file
    var ids = List[Int]()
    for k in range(len(df.metrics)):
        ids.append(df.metrics[k].field_id)
    for want in [1, 10, 11, 20, 30, 31]:
        assert_true(
            _contains_int(ids, want),
            "metrics for leaf " + String(want),
        )
    for absent in [2, 3, 4]:
        assert_false(
            _contains_int(ids, absent),
            "a container is not a leaf: " + String(absent),
        )


def _contains_int(l: List[Int], v: Int) -> Bool:
    for k in range(len(l)):
        if l[k] == v:
            return True
    return False


def test_nested_merge_on_read_delete() raises:
    """A v3 deletion vector over a table with nested columns: the surviving
    rows keep their structs, lists and maps intact."""
    var table = nested_written_table(String("nest_mor"), 3)
    var before = nested_cells(table)
    # v3 row lineage: three appends of six rows, so `next-row-id` is 18 and a
    # merge-on-read delete moves no row.
    assert_equal(table.metadata.next_row_id, 18)
    var removed = table.delete_where(
        String('["=","addr.city","eu"]'), MODE_MERGE_ON_READ
    )
    assert_equal(removed, 4)
    table.refresh()
    assert_equal(table.metadata.next_row_id, 18)
    var deletes = delete_files_of(table)
    assert_true(len(deletes) > 0)
    assert_true(deletes[0].is_deletion_vector())
    var after = nested_cells(table)
    assert_equal(len(after), 14)
    for entry in after.items():
        assert_equal(entry.value, before[entry.key], "row " + String(entry.key))
    for gone in [Int64(0), Int64(5), Int64(10), Int64(15)]:
        assert_false(gone in after, "id " + String(gone) + " is deleted")


def test_nested_copy_on_write_delete_rewrites_nested_files() raises:
    """A copy-on-write delete rewrites the data files, which means the writer
    has to put the struct, list and map back together."""
    var table = nested_written_table(String("nest_cow"), 2)
    var before = nested_cells(table)
    var removed = table.delete_where(String('["=","addr.city","eu"]'))
    assert_equal(removed, 4)
    table.refresh()
    assert_equal(len(delete_files_of(table)), 0, "copy-on-write leaves none")
    var after = nested_cells(table)
    assert_equal(len(after), 14)
    for entry in after.items():
        assert_equal(entry.value, before[entry.key], "row " + String(entry.key))


def test_nested_overwrite_and_partition_by_a_struct_leaf() raises:
    """`identity(addr.city)` partitions by a nested leaf, and an overwrite
    replaces the table with fresh nested rows."""
    var table = nested_written_table(String("nest_part"), 2, String("ident"))
    assert_equal(table.scan().to_table().num_rows(), 18)
    # The partition tuple is the struct leaf's value, and pruning uses it.
    var eu = table.scan().filter('["=","addr.city","eu"]')
    assert_true(len(eu.plan_files()) < len(table.scan().plan_files()))
    assert_equal(eu.to_table().num_rows(), 4)

    var schema = Schema.parse(NESTED_WRITE_SCHEMA)
    var fresh = List[RecordBatch]()
    fresh.append(nested_write_batch(schema, 100, 3))
    _ = table.overwrite(fresh)
    table.refresh()
    var cells = nested_cells(table)
    assert_equal(len(cells), 3)
    assert_equal(
        cells[100],
        '{"city":"eu","zip":1000}|[]|{"keys":["k"],"values":[100]}',
    )


def test_nested_bucket_partition_on_a_struct_leaf() raises:
    """`bucket[4](addr.zip)` — the other transform the spec allows on a
    nested source."""
    var table = nested_written_table(String("nest_bucket"), 2, String("bucket"))
    assert_equal(table.scan().to_table().num_rows(), 18)
    var spec = table.metadata.spec_by_id(table.metadata.default_spec_id)
    assert_equal(spec.fields[0].source_id, 11)
    var paths = List[String]()
    var tasks = table.scan().plan_files()
    for k in range(len(tasks)):
        var dir = tasks[k].data_file.file_path
        var at = dir.rfind("/")
        paths.append(substr(dir, 0, at))
    var buckets = 0
    for k in range(len(paths)):
        var seen = False
        for j in range(k):
            if paths[j] == paths[k]:
                seen = True
        if not seen:
            buckets += 1
    assert_true(buckets > 1, "the rows land in more than one bucket")


def test_create_table_writes_a_v2_metadata_file() raises:
    var catalog = fresh_catalog(String("create"))
    var schema = Schema.parse(WRITE_SCHEMA)
    var table = catalog.create_table(String("db"), String("t"), schema)
    assert_equal(table.metadata.format_version, 2)
    assert_true(table.metadata.table_uuid != "")
    assert_false(table.metadata.has_current_snapshot)
    assert_equal(table.metadata.last_column_id, 5)
    assert_equal(len(table.metadata.schemas), 1)
    assert_equal(len(table.metadata.partition_specs), 1)
    assert_equal(len(table.metadata.sort_orders), 1)
    assert_true(
        basename(table.metadata_location).startswith("00000-"),
        "the first metadata file is 00000-<uuid>.metadata.json",
    )
    # Discoverable again from the directory alone, and empty.
    var again = catalog.load_table(String("db"), String("t"))
    assert_equal(again.metadata.table_uuid, table.metadata.table_uuid)
    assert_equal(again.scan().to_table().num_rows(), 0)
    assert_equal(
        read_version_hint(catalog.io, dirname(table.metadata_location)), 0
    )
    # A second create must not silently take over the first.
    with assert_raises():
        _ = catalog.create_table(String("db"), String("t"), schema)


def test_append_round_trips_through_our_own_reader() raises:
    var table = build_written_table(
        String("append_v2"), String("unpartitioned"), 2
    )
    assert_equal(len(table.metadata.snapshots), 3)
    assert_equal(table.metadata.last_sequence_number, 3)
    var rows = table.scan().to_table()
    assert_equal(rows.num_rows(), 18)
    assert_equal(rows.num_columns(), 5)

    # Every id 0..17 exactly once, with the values the batches carried.
    var seen = List[Bool](length=18, fill=False)
    for r in range(rows.num_rows()):
        var id = Int(rows.value(r, 0).i)
        assert_false(seen[id], "id " + String(id) + " came back twice")
        seen[id] = True
        assert_equal(rows.value(r, 1).s, write_region(id))
        if id % 4 == 0:
            assert_false(rows.value(r, 2).valid, "amount should be null")
        else:
            assert_equal(rows.value(r, 2).f, Float64(id) * 1.5)
        assert_equal(
            rows.value(r, 3).i,
            (WRITE_DAY + Int64(id % 3)) * WRITE_MICROS_PER_DAY
            + Int64(id) * 1000,
        )
        if id % 5 == 0:
            assert_false(rows.value(r, 4).valid, "ok should be null")
        else:
            assert_equal(rows.value(r, 4).i != 0, id % 2 == 0)
    for k in range(18):
        assert_true(seen[k], "id " + String(k) + " is missing")

    # Three snapshots, chained, each an append whose counts accumulate.
    var parent_seen = False
    for k in range(len(table.metadata.snapshots)):
        ref s = table.metadata.snapshots[k]
        assert_equal(s.operation(), "append")
        assert_equal(s.summary_int("added-records", -1), 6)
        assert_true(s.summary_int("added-files-size", -1) > 0)
        assert_equal(s.summary_int("total-records", -1), Int64(6 * (k + 1)))
        assert_equal(s.sequence_number, Int64(k + 1))
        if s.has_parent:
            parent_seen = True
    assert_true(parent_seen, "later snapshots must name a parent")
    assert_equal(len(table.metadata.snapshot_log), 3)
    assert_equal(len(table.metadata.metadata_log), 3)
    assert_equal(table.metadata.ref_index(String("main")) >= 0, True)

    # Each snapshot's own scan sees only what had been committed by then.
    for k in range(len(table.metadata.snapshots)):
        var at = table.scan().use_snapshot(
            table.metadata.snapshots[k].snapshot_id
        )
        assert_equal(
            at.to_table().num_rows(),
            Int(table.metadata.snapshots[k].summary_int("total-records", 0)),
        )


def test_append_carries_the_parent_manifests_forward() raises:
    var table = build_written_table(String("carry"), String("unpartitioned"), 2)
    var snap = table.metadata.current_snapshot()
    var manifests = read_manifest_list_io(table.io, snap.manifest_list)
    assert_equal(len(manifests), 3, "one manifest per append, all live")
    var seqs = List[Int64]()
    for k in range(len(manifests)):
        assert_equal(manifests[k].added_files_count, 1)
        assert_equal(manifests[k].added_rows_count, 6)
        assert_equal(manifests[k].existing_files_count, 0)
        assert_equal(manifests[k].deleted_files_count, 0)
        assert_false(manifests[k].is_delete_manifest())
        seqs.append(manifests[k].sequence_number)
    # The carried-over manifests keep the sequence numbers they were
    # committed with; only the newest has the newest.
    var found_1 = False
    var found_3 = False
    for k in range(len(seqs)):
        if seqs[k] == 1:
            found_1 = True
        if seqs[k] == 3:
            found_3 = True
    assert_true(found_1, "the first commit's manifest keeps sequence 1")
    assert_true(found_3, "the newest manifest has sequence 3")


def test_written_manifest_has_the_statistics_it_claims() raises:
    var table = build_written_table(String("stats"), String("unpartitioned"), 2)
    var tasks = table.scan().plan_files()
    assert_equal(len(tasks), 3)
    for k in range(len(tasks)):
        ref df = tasks[k].data_file
        assert_equal(df.record_count, 6)
        assert_true(df.file_size_in_bytes > 0)
        assert_equal(df.file_format.lower(), "parquet")
        assert_true(len(df.split_offsets) > 0, "split_offsets")
        assert_true(len(df.column_sizes) > 0, "column_sizes")
        assert_equal(len(df.metrics), 5, "one metric entry per column")
        for c in range(len(df.metrics)):
            ref m = df.metrics[c]
            assert_true(m.has_value_count, "value_count")
            assert_equal(m.value_count, 6)
            assert_true(m.has_null_value_count, "null_value_count")
            assert_true(m.has_lower, "lower_bound")
            assert_true(m.has_upper, "upper_bound")
            if m.field_id == 1:
                assert_equal(m.null_value_count, 0)
                var lo = datum_from_bytes_prim(P_LONG, 0, 0, 0, m.lower_bound)
                var hi = datum_from_bytes_prim(P_LONG, 0, 0, 0, m.upper_bound)
                assert_true(lo.i <= hi.i, "id bounds are ordered")
                assert_equal(hi.i - lo.i, 5)
            if m.field_id == 3:
                # `amount` is null wherever id % 4 == 0.
                assert_true(m.null_value_count >= 1)


def test_append_partitions_by_the_spec() raises:
    var table = build_written_table(String("ident_part"), String("ident"), 2)
    var rows = table.scan().to_table()
    assert_equal(rows.num_rows(), 18)
    var tasks = table.scan().plan_files()
    # Five regions across three batches of six rows: 15 files.
    assert_equal(len(tasks), 15)
    for k in range(len(tasks)):
        ref t = tasks[k]
        assert_equal(len(t.data_file.partition), 1)
        assert_true(t.data_file.partition[0].valid)
        var region = t.data_file.partition[0].s
        assert_true(
            t.data_file.file_path.find("/region=" + region + "/") > 0,
            "the file path should carry its partition: "
            + t.data_file.file_path,
        )
        # Every row in the file really is in that partition.
        var one = read_data_file_table(
            table.io,
            t.data_file,
            List[DataFile](),
            t.data_sequence_number,
            table.metadata.spec(),
            table.metadata.schema(),
            [1, 2],
            List[String](),
            NameMapping(),
            String('["true"]'),
            True,
            ScanOptions(),
        )
        for r in range(one.num_rows()):
            assert_equal(one.value(r, 1).s, region)

    # A partition predicate must prune to exactly that partition's files.
    var eu = table.scan().filter('["=","region","eu"]')
    assert_equal(len(eu.plan_files()), 3)
    var eu_rows = eu.to_table()
    assert_true(eu_rows.num_rows() > 0)
    for r in range(eu_rows.num_rows()):
        assert_equal(eu_rows.value(r, 1).s, "eu")


def test_append_bucket_and_day_partitions() raises:
    var bucket = build_written_table(String("bucket"), String("bucket"), 2)
    assert_equal(bucket.scan().to_table().num_rows(), 18)
    var btasks = bucket.scan().plan_files()
    for k in range(len(btasks)):
        var v = btasks[k].data_file.partition[0].copy()
        assert_true(v.valid and v.i >= 0 and v.i < 4, "bucket in 0..3")
        assert_true(
            btasks[k].data_file.file_path.find("/id_bucket=") > 0,
            btasks[k].data_file.file_path,
        )

    var day = build_written_table(String("day"), String("day"), 2)
    assert_equal(day.scan().to_table().num_rows(), 18)
    var dtasks = day.scan().plan_files()
    # Three distinct days across three batches.
    assert_equal(len(dtasks), 9)
    for k in range(len(dtasks)):
        var v = dtasks[k].data_file.partition[0].copy()
        assert_true(v.valid)
        assert_true(
            v.i >= WRITE_DAY and v.i <= WRITE_DAY + 2,
            "day partition value " + String(v.i),
        )
        assert_true(
            dtasks[k].data_file.file_path.find("/ts_day=2024-01-0") > 0,
            dtasks[k].data_file.file_path,
        )


def test_v3_append_assigns_row_lineage() raises:
    var table = build_written_table(String("v3"), String("unpartitioned"), 3)
    assert_equal(table.metadata.format_version, 3)
    assert_true(table.metadata.has_next_row_id)
    assert_equal(table.metadata.next_row_id, 18)
    var at: Int64 = 0
    for k in range(len(table.metadata.snapshots)):
        ref s = table.metadata.snapshots[k]
        assert_true(s.has_first_row_id, "a v3 snapshot carries first-row-id")
        assert_true(s.has_added_rows, "a v3 snapshot carries added-rows")
        assert_equal(s.added_rows, 6)
    # The three ranges tile 0..18 with no overlap.
    var firsts = List[Int64]()
    for k in range(len(table.metadata.snapshots)):
        firsts.append(table.metadata.snapshots[k].first_row_id)
    for i in range(1, len(firsts)):
        var j = i
        while j > 0 and firsts[j] < firsts[j - 1]:
            firsts.swap_elements(j, j - 1)
            j -= 1
    for k in range(len(firsts)):
        assert_equal(firsts[k], at)
        at += 6
    assert_equal(at, table.metadata.next_row_id)

    # And the reader's inheritance produces exactly those ids.
    var rows = table.scan().select([String("id"), String("_row_id")]).to_table()
    assert_equal(rows.num_rows(), 18)
    for r in range(rows.num_rows()):
        assert_true(rows.value(r, 1).valid, "every v3 row has a _row_id")
        assert_equal(rows.value(r, 1).i, rows.value(r, 0).i)

    # A v2 table must not carry row lineage at all.
    var v2 = build_written_table(
        String("v2_lineage"), String("unpartitioned"), 2, 1
    )
    assert_false(v2.metadata.has_next_row_id)
    assert_false(v2.metadata.snapshots[0].has_first_row_id)


def test_append_retries_when_another_writer_wins() raises:
    """Two transactions opened against the same version; both must land."""
    var catalog = fresh_catalog(String("race"))
    var schema = Schema.parse(WRITE_SCHEMA)
    var table = catalog.create_table(String("db"), String("t"), schema)
    var a = table.new_append()
    var b = table.new_append()
    a.add(write_batch(schema, 0, 6))
    b.add(write_batch(schema, 6, 6))
    assert_equal(a.commit(), 6)
    # `b` still believes the table has no snapshot; its create-mode write of
    # 00001-*.metadata.json collides, so it reloads and retries.
    assert_equal(b.commit(), 6)
    var loaded = catalog.load_table(String("db"), String("t"))
    assert_equal(len(loaded.metadata.snapshots), 2)
    assert_equal(loaded.scan().to_table().num_rows(), 12)
    assert_equal(loaded.metadata.last_sequence_number, 2)


def test_written_tables_reread_by_the_cli_layout() raises:
    """`version-hint.text` and the file listing must agree after a commit."""
    var table = build_written_table(String("hint"), String("unpartitioned"), 2)
    var dir = dirname(table.metadata_location)
    assert_equal(read_version_hint(table.io, dir), 3)
    var found = find_latest_metadata(table.io, dir)
    assert_equal(basename(found), basename(table.metadata_location))
    # And from the table directory, which is what the CLI is given.
    var from_root = find_latest_metadata(table.io, dirname(dir))
    assert_equal(basename(from_root), basename(table.metadata_location))


def test_next_metadata_version_reads_the_file_name() raises:
    var m = TableMetadata()
    m.metadata_file_location = String(
        "/w/db/t/metadata/00007-abc.metadata.json"
    )
    assert_equal(next_metadata_version(m), 8)
    m.metadata_file_location = String("/w/db/t/metadata/v3.metadata.json")
    assert_equal(next_metadata_version(m), 1, "no leading digits: fall back")
    m.metadata_file_location = String("")
    assert_equal(next_metadata_version(m), 1)
    assert_true(metadata_file_name(12).startswith("00012-"))
    assert_true(metadata_file_name(12).endswith(".metadata.json"))


def test_manifest_schemas_match_the_format_version() raises:
    var schema = Schema.parse(WRITE_SCHEMA)
    var spec = PartitionSpec(
        0,
        [
            PartitionField.single(
                2, 1000, String("region"), parse_transform("identity")
            )
        ],
    )
    var typing = PartitionTyping.of(spec, schema)
    assert_equal(len(typing), 1)
    assert_equal(typing.kinds[0], P_STRING)

    var v1 = manifest_entry_schema_json(1, typing)
    var v2 = manifest_entry_schema_json(2, typing)
    var v3 = manifest_entry_schema_json(3, typing)

    # v1: required snapshot_id, block_size_in_bytes, no content.
    assert_true(v1.find('"block_size_in_bytes"') > 0)
    assert_true(v1.find('"content"') < 0)
    assert_true(v1.find('"sequence_number"') < 0)
    assert_true(v1.find('"equality_ids"') < 0)
    # v2: content, sequence numbers, equality ids; no block size, no v3 fields.
    assert_true(v2.find('"content"') > 0)
    assert_true(v2.find('"sequence_number"') > 0)
    assert_true(v2.find('"file_sequence_number"') > 0)
    assert_true(v2.find('"equality_ids"') > 0)
    assert_true(v2.find('"block_size_in_bytes"') < 0)
    assert_true(v2.find('"first_row_id"') < 0)
    # v3 adds row lineage and the deletion-vector fields.
    assert_true(v3.find('"first_row_id"') > 0)
    assert_true(v3.find('"referenced_data_file"') > 0)
    assert_true(v3.find('"content_offset"') > 0)
    assert_true(v3.find('"content_size_in_bytes"') > 0)
    # The partition record is typed by the spec, under field id 102.
    assert_true(v2.find('"name":"r102"') > 0)
    assert_true(v2.find('"field-id":1000') > 0)

    var l1 = manifest_list_schema_json(1)
    var l2 = manifest_list_schema_json(2)
    var l3 = manifest_list_schema_json(3)
    assert_true(l1.find('"content"') < 0)
    assert_true(l1.find('"sequence_number"') < 0)
    assert_true(l2.find('"min_sequence_number"') > 0)
    assert_true(l2.find('"first_row_id"') < 0)
    assert_true(l3.find('"first_row_id"') > 0)
    # And every one of them parses as Avro.
    for k in range(3):
        var v = k + 1
        _ = parse_json(manifest_entry_schema_json(v, typing))
        _ = parse_json(manifest_list_schema_json(v))


def test_metric_bounds_are_truncated() raises:
    """`write.metadata.metrics.default = truncate(16)`, both directions."""
    var short_ = List[UInt8]()
    short_.extend(String("abc").as_bytes())
    assert_equal(len(truncate_lower(short_, P_STRING, 16)), 3)
    var kept = truncate_upper(short_, P_STRING, 16)
    assert_true(kept[1])
    assert_equal(len(kept[0]), 3, "a short value is not truncated at all")

    var long_ = List[UInt8]()
    long_.extend(String("abcdefghijklmnopqrstuvwxyz").as_bytes())
    var lower = truncate_lower(long_, P_STRING, 16)
    assert_equal(len(lower), 16)
    assert_equal(
        String(StringSlice(unsafe_from_utf8=Span(lower))), "abcdefghijklmnop"
    )
    var upper = truncate_upper(long_, P_STRING, 16)
    assert_true(upper[1])
    assert_equal(
        String(StringSlice(unsafe_from_utf8=Span(upper[0]))),
        "abcdefghijklmnoq",
        "an upper bound is incremented so it still bounds",
    )

    # Multi-byte code points count as one unit, and the increment stays valid
    # UTF-8.
    var utf8 = List[UInt8]()
    utf8.extend(String("ααααααααααααααααββ").as_bytes())
    var u_lower = truncate_lower(utf8, P_STRING, 16)
    assert_equal(len(u_lower), 32, "16 two-byte code points")
    var u_upper = truncate_upper(utf8, P_STRING, 16)
    assert_true(u_upper[1])
    assert_equal(
        String(StringSlice(unsafe_from_utf8=Span(u_upper[0]))),
        "αααααααααααααααβ",
    )

    # Binary increments the last byte below 0xFF; all-0xFF cannot be bounded.
    var bytes = List[UInt8]()
    for _ in range(20):
        bytes.append(0x41)
    var b_upper = truncate_upper(bytes, P_BINARY, 16)
    assert_true(b_upper[1])
    assert_equal(len(b_upper[0]), 16)
    assert_equal(b_upper[0][15], 0x42)
    var maxed = List[UInt8]()
    for _ in range(20):
        maxed.append(0xFF)
    var m_upper = truncate_upper(maxed, P_BINARY, 16)
    assert_false(m_upper[1], "an all-0xFF prefix has no truncated upper bound")


def test_partition_paths_are_hive_shaped() raises:
    assert_equal(escape_path(String("a b/c")), "a%20b%2Fc")
    assert_equal(
        human_partition_value(Datum.integral(P_DATE, 19723), 5), "2024-01-01"
    )
    assert_equal(
        human_partition_value(Datum.int_(54), 3), "2024", "year(1970+54)"
    )
    assert_equal(human_partition_value(Datum.int_(648), 4), "2024-01")
    assert_equal(human_partition_value(Datum.int_(473352), 6), "2024-01-01-00")
    assert_equal(
        human_partition_value(Datum.none(), 0), "__HIVE_DEFAULT_PARTITION__"
    )
    var spec = PartitionSpec(
        0,
        [
            PartitionField.single(
                2, 1000, String("region"), parse_transform("identity")
            )
        ],
    )
    assert_equal(
        partition_path(spec, [Datum.string_(String("eu"))]), "region=eu"
    )
    assert_equal(
        partition_path(spec, [Datum.none()]),
        "region=__HIVE_DEFAULT_PARTITION__",
    )


def test_scan_batches_export_over_the_c_data_interface() raises:
    """Every column of a scan batch must survive `export_c`.

    The struct pair is what any Arrow consumer imports; the reader's job is to
    produce arrays whose buffers and lengths are exportable without a copy or
    a conversion in between, which is only true because the batch *is* the
    decoded Parquet, cast in place.
    """
    var batches = (
        fixture_scan("unpartitioned")
        .select(
            [
                String("id"),
                String("region"),
                String("amount"),
                String("ok"),
                String("_file"),
                String("_pos"),
            ]
        )
        .to_batches()
    )
    assert_true(len(batches) > 0)
    var rows = 0
    for k in range(len(batches)):
        ref b = batches[k]
        assert_equal(b.num_columns(), 6)
        rows += b.num_rows
        for c in range(b.num_columns()):
            var exported = b.export_c(c)
            assert_true(exported.array != 0, "ArrowArray address")
            assert_true(exported.schema != 0, "ArrowSchema address")
            # `exported` releases both structs when it goes out of scope.
        # The metadata columns are real arrays, not a special case.
        assert_equal(b.name(4), "_file")
        assert_equal(b.name(5), "_pos")
        var files = b.column_str(4)
        var positions = b.column_i64(5)
        assert_equal(len(files[0]), b.num_rows)
        for r in range(b.num_rows):
            assert_true(files[0][r].endswith(".parquet"))
            assert_true(positions[0][r] >= 0)
    assert_equal(rows, fixture_scan("unpartitioned").to_table().num_rows())


def test_written_table_survives_a_rewrite_of_its_location() raises:
    """The metadata records absolute paths; a rebase must still read them."""
    var table = build_written_table(
        String("rebase"), String("unpartitioned"), 2, 1
    )
    var io = FileIO.local()
    io.rebase(table.metadata.location, table.metadata.location)
    var scan = table.scan().with_io(io^)
    assert_equal(scan.to_table().num_rows(), 6)


# ── REST commits ────────────────────────────────────────────────────────────
def rest_write_catalog() raises -> RestCatalog:
    var config = RestCatalogConfig(getenv("ICEBERG_TEST_REST", ""))
    config.with_token(getenv("ICEBERG_TEST_REST_TOKEN", "test-token"))
    var catalog = RestCatalog(config^, FileIO.local())
    catalog.connect()
    return catalog^


def test_rest_create_table_and_commit_append() raises:
    if getenv("ICEBERG_TEST_REST", "") == "":
        print("  (skipped: no ICEBERG_TEST_REST)")
        return
    var catalog = rest_write_catalog()
    var schema = Schema.parse(WRITE_SCHEMA)
    var spec = PartitionSpec(
        0,
        [
            PartitionField.single(
                2, 1000, String("region"), parse_transform("identity")
            )
        ],
    )
    for v in [2, 3]:
        var name = String("commit_v") + String(v)
        var created = catalog.create_table(
            String("wr"), name, schema, spec, Dict[String, String](), v
        )
        assert_equal(created.metadata.format_version, v)
        assert_false(created.metadata.has_current_snapshot)

        var total = 0
        for b in range(2):
            var batches = List[RecordBatch]()
            batches.append(write_batch(schema, b * 6, 6))
            var after = catalog.append(String("wr"), name, batches)
            total += 6
            assert_equal(len(after.metadata.snapshots), b + 1)
            assert_equal(after.metadata.last_sequence_number, Int64(b + 1))
            if v >= 3:
                # The server derives `next-row-id` from the snapshot, since
                # the spec has no TableUpdate that sets it.
                assert_equal(after.metadata.next_row_id, Int64(total))
        var back = catalog.load_table(String("wr"), name)
        var rows = back.scan().to_table()
        assert_equal(rows.num_rows(), 12)
        assert_equal(len(back.metadata.snapshots), 2)
        if v >= 3:
            # Row ids are assigned per *file*, in manifest order, so they do
            # not track `id` once the rows are spread over five partitions.
            # What must hold is that the twelve rows carry exactly the twelve
            # ids 0..11, each once.
            var lineage = (
                back.scan().select([String("id"), String("_row_id")]).to_table()
            )
            assert_equal(lineage.num_rows(), 12)
            var seen_row_id = List[Bool](length=12, fill=False)
            for r in range(lineage.num_rows()):
                assert_true(lineage.value(r, 1).valid, "_row_id is assigned")
                var rid = Int(lineage.value(r, 1).i)
                assert_true(
                    rid >= 0 and rid < 12,
                    "_row_id " + String(rid) + " in range",
                )
                assert_false(
                    seen_row_id[rid], "_row_id " + String(rid) + " twice"
                )
                seen_row_id[rid] = True


def test_rest_commit_retries_a_409() raises:
    if getenv("ICEBERG_TEST_REST", "") == "":
        print("  (skipped: no ICEBERG_TEST_REST)")
        return
    var catalog = rest_write_catalog()
    var schema = Schema.parse(WRITE_SCHEMA)
    # The mock answers 409 to this table's first commit, then accepts.
    var name = String("conflict_once_a")
    _ = catalog.create_table(
        String("wr"), name, schema, PartitionSpec.unpartitioned()
    )
    var batches = List[RecordBatch]()
    batches.append(write_batch(schema, 0, 6))
    var after = catalog.append(String("wr"), name, batches)
    assert_equal(len(after.metadata.snapshots), 1, "the retry must land")
    assert_equal(
        catalog.load_table(String("wr"), name).scan().to_table().num_rows(), 6
    )


def test_rest_delete_and_overwrite() raises:
    """A delete and an overwrite committed through the catalog.

    The `CommitTableRequest` is the same shape an append sends — the spec has
    no per-operation update action — so this proves the whole delete path
    survives a real optimistic commit, requirements checked by the server.
    """
    if getenv("ICEBERG_TEST_REST", "") == "":
        print("  (skipped: no ICEBERG_TEST_REST)")
        return
    var catalog = rest_write_catalog()
    var schema = Schema.parse(WRITE_SCHEMA)
    var name = String("rest_delete_a")
    _ = catalog.create_table(
        String("wr"),
        name,
        schema,
        PartitionSpec.unpartitioned(),
        Dict[String, String](),
        3,
    )
    var batches = List[RecordBatch]()
    batches.append(write_batch(schema, 0, 12))
    _ = catalog.append(String("wr"), name, batches)
    assert_equal(
        catalog.load_table(String("wr"), name).scan().to_table().num_rows(), 12
    )

    # Merge-on-read, so the rows stay put and a deletion vector records them.
    var after = catalog.delete_where(
        String("wr"), name, String('["=","region","eu"]'), MODE_MERGE_ON_READ
    )
    assert_equal(after.metadata.current_snapshot().operation(), "delete")
    var back = catalog.load_table(String("wr"), name)
    assert_equal(back.scan().to_table().num_rows(), 9)
    # One batch, so one data file, so one vector — with all three positions.
    var deletes = delete_files_of(back)
    assert_equal(len(deletes), 1)
    assert_true(deletes[0].is_deletion_vector())
    assert_equal(deletes[0].record_count, 3)

    # And an overwrite on top of the deletes, which rewrites what is left.
    var fresh = List[RecordBatch]()
    fresh.append(write_batch(schema, 50, 2))
    var done = catalog.overwrite(String("wr"), name, fresh)
    assert_equal(done.metadata.current_snapshot().operation(), "overwrite")
    var final = catalog.load_table(String("wr"), name)
    assert_equal(final.scan().to_table().num_rows(), 2)
    assert_equal(len(delete_files_of(final)), 0)


def test_rest_commit_recovers_from_5xx_with_idempotency_key() raises:
    """Applied, then 500 — and the retry replays the success.

    This is what the `Idempotency-Key` header is for, and why the REST spec
    has asked for it since 1.11.0. The mock applies this table's commit,
    records the answer under the key, and *then* returns 500. The HTTP client
    repeats the request — it may, precisely because the key is on it — and the
    server replays the recorded 200 instead of committing a second time.

    So the assertion is not just that the append succeeds: it is that the
    table ends up with **one** snapshot and six rows, not two snapshots and
    twelve.
    """
    if getenv("ICEBERG_TEST_REST", "") == "":
        print("  (skipped: no ICEBERG_TEST_REST)")
        return
    var catalog = rest_write_catalog()
    var schema = Schema.parse(WRITE_SCHEMA)
    var name = String("unknown_state_a")
    _ = catalog.create_table(
        String("wr"), name, schema, PartitionSpec.unpartitioned()
    )
    var batches = List[RecordBatch]()
    batches.append(write_batch(schema, 0, 6))
    var after = catalog.append(String("wr"), name, batches)
    assert_equal(
        len(after.metadata.snapshots), 1, "the replayed answer, not a re-commit"
    )
    var back = catalog.load_table(String("wr"), name)
    assert_equal(len(back.metadata.snapshots), 1, "exactly one snapshot")
    assert_equal(back.scan().to_table().num_rows(), 6, "no duplicated rows")


def test_rest_commit_state_unknown_on_5xx() raises:
    """A server that 5xxes and does *not* deduplicate on the key.

    `always_5xx` applies the first commit and then answers 500 to it and to
    every repeat, recording nothing. Every attempt fails the same way, and the
    client is left in exactly the state the spec calls `CommitStateUnknown`:
    the commit may or may not have landed — here it did — and the only honest
    thing to do is say so and let the caller reload.
    """
    if getenv("ICEBERG_TEST_REST", "") == "":
        print("  (skipped: no ICEBERG_TEST_REST)")
        return
    var catalog = rest_write_catalog()
    var schema = Schema.parse(WRITE_SCHEMA)
    var name = String("always_5xx_a")
    _ = catalog.create_table(
        String("wr"), name, schema, PartitionSpec.unpartitioned()
    )
    var batches = List[RecordBatch]()
    batches.append(write_batch(schema, 0, 6))
    var raised = String("")
    try:
        _ = catalog.append(String("wr"), name, batches)
    except e:
        raised = String(e)
    assert_true(
        raised.find("CommitStateUnknown") >= 0,
        "a 5xx the server will not deduplicate is reported as unknown state: "
        + raised,
    )
    # And it really is unknown: the server had applied it.
    assert_equal(
        catalog.load_table(String("wr"), name).scan().to_table().num_rows(), 6
    )


def test_s3_write_round_trips() raises:
    """Create, append and read back entirely over `s3://`.

    MinIO verifies the SigV4 signatures, so every PUT here is a real signed
    request. The commit itself is *not* safe over an object store — there is
    no atomic create-if-absent to lose a race against — which is exactly what
    the spec says about filesystem tables and why this only ever has one
    writer.
    """
    if _s3_warehouse() == "":
        print("SKIP test_s3_write_round_trips: no ICEBERG_TEST_S3")
        return
    var prefix = getenv("ICEBERG_TEST_S3_PREFIX", "")
    if prefix == "":
        return
    var io = FileIO.local()
    io.set(String("s3.endpoint"), getenv("AWS_ENDPOINT_URL_S3", ""))
    io.set(String("s3.access-key-id"), getenv("AWS_ACCESS_KEY_ID", ""))
    io.set(String("s3.secret-access-key"), getenv("AWS_SECRET_ACCESS_KEY", ""))
    io.set(String("s3.region"), String("us-east-1"))
    var warehouse = prefix + "written"
    var catalog = FilesystemCatalog(warehouse, io^)
    var schema = Schema.parse(WRITE_SCHEMA)
    var spec = PartitionSpec(
        0,
        [
            PartitionField.single(
                2, 1000, String("region"), parse_transform("identity")
            )
        ],
    )
    # A fresh table name each run: an object store has no atomic create, so a
    # leftover from a previous run would be indistinguishable from a race.
    var name = String("t") + String(now_ms())
    var table = catalog.create_table(String("db"), name, schema, spec)
    assert_true(table.metadata.location.startswith("s3://"))
    for b in range(2):
        var batches = List[RecordBatch]()
        batches.append(write_batch(schema, b * 6, 6))
        var tx = table.new_append()
        tx.add_batches(batches)
        assert_equal(tx.commit(), 6)
        table.refresh()
    assert_equal(len(table.metadata.snapshots), 2)

    var back = catalog.load_table(String("db"), name)
    var rows = back.scan().to_table()
    assert_equal(rows.num_rows(), 12)
    for r in range(rows.num_rows()):
        var id = Int(rows.value(r, 0).i)
        assert_equal(rows.value(r, 1).s, write_region(id))
    var tasks = back.scan().plan_files()
    for k in range(len(tasks)):
        assert_true(
            tasks[k].data_file.file_path.startswith("s3://"),
            tasks[k].data_file.file_path,
        )
    # And a partition filter still prunes over the network.
    var eu = back.scan().filter('["=","region","eu"]').to_table()
    assert_true(eu.num_rows() > 0)
    for r in range(eu.num_rows()):
        assert_equal(eu.value(r, 1).s, "eu")


def test_rest_commit_rejects_a_stale_requirement() raises:
    if getenv("ICEBERG_TEST_REST", "") == "":
        print("  (skipped: no ICEBERG_TEST_REST)")
        return
    var catalog = rest_write_catalog()
    var schema = Schema.parse(WRITE_SCHEMA)
    var name = String("requirements")
    var created = catalog.create_table(
        String("wr"), name, schema, PartitionSpec.unpartitioned()
    )
    # A hand-built body whose `assert-table-uuid` is wrong must be refused,
    # which is what proves the server is checking rather than accepting.
    var files = write_data_files(
        created.io,
        created.metadata.location,
        [write_batch(schema, 0, 3)],
        schema,
        PartitionSpec.unpartitioned(),
        0,
        WriteOptions(),
    )
    var result = prepare_append(
        created.io, created.metadata, files^, Dict[String, String]()
    )
    var stale = created.metadata.copy()
    stale.table_uuid = String("00000000-0000-4000-8000-000000000000")
    var body = commit_append_body(String("wr"), name, stale, result)
    var headers = catalog.config.headers()
    var resp = catalog.client.post(
        catalog.config.commit_table_url(String("wr"), name),
        body.as_bytes(),
        headers,
    )
    assert_equal(resp.status, 409, "a wrong table uuid is a conflict")
    assert_true(resp.text().find("assert-table-uuid") >= 0, resp.text())


# ══ delete, overwrite, and the manifest maintenance they need ═══════════════
# Every table below is written by this library, deleted from by this library,
# and read back through the scan path. The *external* gate — PyIceberg 0.11.1
# and DuckDB 1.5.5 reading the same tables — is tools/verify_written.py.
def delete_table(
    scenario: String,
    shape: String,
    format_version: Int,
    mode: String = String(""),
) raises -> Table:
    """Three appends of six rows (ids 0..17), then whatever the test does."""
    var table = build_written_table(scenario, shape, format_version)
    if mode != "":
        table.metadata.properties[PROP_DELETE_MODE] = mode
    return table^


def ids_of(table: Table) raises -> List[Int64]:
    var rows = table.scan().select([String("id")]).to_table()
    var out = List[Int64]()
    for r in range(rows.num_rows()):
        out.append(rows.value(r, 0).i)
    return out^


def has_id(ids: List[Int64], v: Int64) -> Bool:
    for k in range(len(ids)):
        if ids[k] == v:
            return True
    return False


def delete_files_of(table: Table) raises -> List[DataFile]:
    var out = List[DataFile]()
    var tasks = table.scan().plan_files()
    var seen = List[String]()
    for k in range(len(tasks)):
        for j in range(len(tasks[k].delete_files)):
            # A single Puffin file holds one vector per data file, so the
            # identity of a delete *file entry* is path plus offset.
            var path = (
                tasks[k].delete_files[j].file_path
                + "@"
                + String(tasks[k].delete_files[j].content_offset)
            )
            var dup = False
            for i in range(len(seen)):
                if seen[i] == path:
                    dup = True
                    break
            if dup:
                continue
            seen.append(path^)
            out.append(tasks[k].delete_files[j].copy())
    return out^


def test_delete_mode_defaults_to_copy_on_write() raises:
    var props = Dict[String, String]()
    assert_equal(delete_mode_of(props), MODE_COPY_ON_WRITE)
    props[PROP_DELETE_MODE] = String("merge-on-read")
    assert_equal(delete_mode_of(props), MODE_MERGE_ON_READ)
    props[PROP_DELETE_MODE] = String("Copy-On-Write")
    assert_equal(delete_mode_of(props), MODE_COPY_ON_WRITE)
    props[PROP_DELETE_MODE] = String("whatever")
    with assert_raises():
        _ = delete_mode_of(props)


def test_merge_on_read_delete_writes_deletion_vectors() raises:
    """v3 merge-on-read: one Puffin vector per data file that lost a row.

    Four `eu` rows (ids 0, 5, 10, 15) spread over the three files, so no file
    is emptied and all three get a vector.
    """
    var table = delete_table("del_mor_v3", "unpartitioned", 3)
    assert_equal(table.scan().to_table().num_rows(), 18)
    var removed = table.delete_where(
        String('["=","region","eu"]'), MODE_MERGE_ON_READ
    )
    assert_equal(removed, 4)
    table.refresh()
    var ids = ids_of(table)
    assert_equal(len(ids), 14)
    for v in [Int64(0), Int64(5), Int64(10), Int64(15)]:
        assert_false(has_id(ids, v), "id " + String(v) + " is deleted")

    # Three vectors, in one Puffin file, each pointing at its data file.
    var deletes = delete_files_of(table)
    assert_equal(len(deletes), 3)
    var puffin = String("")
    var total: Int64 = 0
    for k in range(len(deletes)):
        assert_true(deletes[k].is_deletion_vector())
        assert_equal(deletes[k].file_format.lower(), "puffin")
        assert_equal(deletes[k].content, 1)
        assert_true(deletes[k].has_referenced_data_file)
        assert_true(deletes[k].has_content_offset)
        assert_true(deletes[k].has_content_size_in_bytes)
        total += deletes[k].record_count
        if puffin == "":
            puffin = deletes[k].file_path
        assert_equal(deletes[k].file_path, puffin, "one Puffin file per commit")
    assert_equal(total, 4)

    # The footer agrees with the manifest, byte for byte.
    var pf = PuffinFile.open(table.io, puffin)
    assert_equal(len(pf.blobs), 3)
    for k in range(len(deletes)):
        var found = False
        for b in range(len(pf.blobs)):
            if pf.blobs[b].offset != deletes[k].content_offset:
                continue
            found = True
            assert_equal(pf.blobs[b].length, deletes[k].content_size_in_bytes)
            assert_equal(
                pf.blobs[b].referenced_data_file(),
                deletes[k].referenced_data_file,
            )
            assert_equal(pf.blobs[b].cardinality(), deletes[k].record_count)
            assert_equal(pf.blobs[b].compression_codec, "")
            assert_equal(pf.blobs[b].snapshot_id, -1)
            assert_equal(pf.blobs[b].sequence_number, -1)
        assert_true(found, "every vector is in the footer")

    var snap = table.metadata.current_snapshot()
    assert_equal(snap.operation(), "delete")
    assert_equal(snap.summary_int(String("added-delete-files"), -1), 3)
    assert_equal(snap.summary_int(String("added-dvs"), -1), 3)
    assert_equal(snap.summary_int(String("added-position-deletes"), -1), 4)
    assert_equal(snap.summary_int(String("total-delete-files"), -1), 3)
    assert_equal(snap.summary_int(String("total-position-deletes"), -1), 4)
    assert_equal(snap.summary_int(String("total-data-files"), -1), 3)
    assert_equal(snap.summary_int(String("total-records"), -1), 18)


def test_merge_on_read_delete_writes_position_deletes() raises:
    """v2 merge-on-read: a Parquet file of (file_path, pos), sorted."""
    var table = delete_table("del_mor_v2", "unpartitioned", 2)
    var removed = table.delete_where(
        String('["=","region","eu"]'), MODE_MERGE_ON_READ
    )
    assert_equal(removed, 4)
    table.refresh()
    assert_equal(len(ids_of(table)), 14)

    var deletes = delete_files_of(table)
    assert_equal(len(deletes), 1, "one file per partition, and there is one")
    assert_equal(deletes[0].file_format.lower(), "parquet")
    assert_equal(deletes[0].content, 1)
    assert_equal(deletes[0].record_count, 4)
    assert_false(deletes[0].has_referenced_data_file)
    assert_false(deletes[0].has_sort_order_id)
    assert_true(deletes[0].file_path.find("position-deletes") >= 0)

    var snap = table.metadata.current_snapshot()
    assert_equal(snap.operation(), "delete")
    assert_equal(snap.summary_int(String("added-delete-files"), -1), 1)
    assert_equal(snap.summary_int(String("added-position-deletes"), -1), 4)
    assert_equal(snap.summary_int(String("added-position-delete-files"), -1), 1)
    assert_equal(snap.summary_int(String("total-position-deletes"), -1), 4)


def test_a_second_delete_merges_the_deletion_vector() raises:
    """The spec's rule: a new vector absorbs every prior position delete for
    its data file, and the vector it replaces is marked DELETED."""
    var table = delete_table("del_mor_twice", "unpartitioned", 3)
    _ = table.delete_where(String('["=","id",0]'), MODE_MERGE_ON_READ)
    table.refresh()
    var first = delete_files_of(table)
    assert_equal(len(first), 1)
    assert_equal(first[0].record_count, 1)
    assert_equal(len(ids_of(table)), 17)

    _ = table.delete_where(String('["=","id",3]'), MODE_MERGE_ON_READ)
    table.refresh()
    var second = delete_files_of(table)
    assert_equal(len(second), 1, "the old vector is gone, not applied twice")
    assert_equal(second[0].record_count, 2, "the new vector absorbed the old")
    assert_true(second[0].file_path != first[0].file_path)
    var ids = ids_of(table)
    assert_equal(len(ids), 16)
    assert_false(has_id(ids, 0))
    assert_false(has_id(ids, 3))
    assert_true(has_id(ids, 1))

    var snap = table.metadata.current_snapshot()
    assert_equal(snap.summary_int(String("removed-dvs"), -1), 1)
    assert_equal(snap.summary_int(String("removed-delete-files"), -1), 1)
    assert_equal(snap.summary_int(String("total-delete-files"), -1), 1)
    assert_equal(snap.summary_int(String("total-position-deletes"), -1), 2)


def test_copy_on_write_delete_rewrites_the_file() raises:
    """The default mode: no delete file anywhere, the originals DELETED."""
    var table = delete_table("del_cow_v2", "unpartitioned", 2)
    var removed = table.delete_where(String('["=","region","eu"]'))
    assert_equal(removed, 4)
    table.refresh()
    var ids = ids_of(table)
    assert_equal(len(ids), 14)
    assert_false(has_id(ids, 5))
    assert_equal(len(delete_files_of(table)), 0, "copy-on-write leaves none")
    assert_equal(len(table.scan().plan_files()), 3, "three rewritten files")

    var snap = table.metadata.current_snapshot()
    assert_equal(snap.operation(), "overwrite")
    assert_equal(snap.summary_int(String("added-data-files"), -1), 3)
    assert_equal(snap.summary_int(String("added-records"), -1), 14)
    assert_equal(snap.summary_int(String("deleted-data-files"), -1), 3)
    assert_equal(snap.summary_int(String("deleted-records"), -1), 18)
    assert_equal(snap.summary_int(String("total-data-files"), -1), 3)
    assert_equal(snap.summary_int(String("total-records"), -1), 14)
    assert_equal(snap.summary_int(String("total-delete-files"), -1), 0)


def test_metadata_delete_removes_whole_files() raises:
    """A filter the partitioning already answers: no data file is read and
    none is rewritten — the matching files are simply marked DELETED."""
    var table = delete_table("del_meta", "ident", 2)
    var before = len(table.scan().plan_files())
    var removed = table.delete_where(String('["=","region","eu"]'))
    assert_equal(removed, 4)
    table.refresh()
    var ids = ids_of(table)
    assert_equal(len(ids), 14)
    assert_false(has_id(ids, 0))
    var snap = table.metadata.current_snapshot()
    assert_equal(
        snap.operation(), "delete", "nothing was added, so it is a delete"
    )
    assert_equal(snap.summary_int(String("deleted-records"), -1), 4)
    assert_equal(snap.summary_int(String("total-records"), -1), 14)
    assert_true(len(table.scan().plan_files()) < before)


def test_delete_everything_leaves_an_empty_table() raises:
    """Every manifest ends up with nothing live, so none is written at all."""
    var table = delete_table("del_all", "unpartitioned", 3)
    var removed = table.delete_where(String('["true"]'), MODE_MERGE_ON_READ)
    assert_equal(removed, 18)
    table.refresh()
    assert_equal(table.scan().to_table().num_rows(), 0)
    assert_equal(len(table.scan().plan_files()), 0)
    var snap = table.metadata.current_snapshot()
    assert_equal(snap.summary_int(String("total-records"), -1), 0)
    assert_equal(snap.summary_int(String("total-data-files"), -1), 0)
    assert_equal(snap.summary_int(String("total-delete-files"), -1), 0)


def test_delete_preserves_row_lineage() raises:
    """v3: a merge-on-read delete moves no row, so every surviving `_row_id`
    is the one the append assigned."""
    var table = delete_table("del_lineage", "unpartitioned", 3)
    var before = (
        table.scan().select([String("id"), String("_row_id")]).to_table()
    )
    var want = Dict[Int64, Int64]()
    for r in range(before.num_rows()):
        want[before.value(r, 0).i] = before.value(r, 1).i
    _ = table.delete_where(String('["=","region","eu"]'), MODE_MERGE_ON_READ)
    table.refresh()
    var after = (
        table.scan().select([String("id"), String("_row_id")]).to_table()
    )
    assert_equal(after.num_rows(), 14)
    for r in range(after.num_rows()):
        var id = after.value(r, 0).i
        assert_equal(
            after.value(r, 1).i, want[id], "_row_id of id " + String(id)
        )
    assert_equal(table.metadata.next_row_id, 18)


def test_overwrite_replaces_the_whole_table() raises:
    var table = delete_table("ovw_all", "unpartitioned", 2)
    var schema = Schema.parse(WRITE_SCHEMA)
    var batches = List[RecordBatch]()
    batches.append(write_batch(schema, 100, 3))
    var added = table.overwrite(batches)
    assert_equal(added, 3)
    table.refresh()
    var ids = ids_of(table)
    assert_equal(len(ids), 3)
    assert_true(has_id(ids, 100))
    assert_false(has_id(ids, 0))
    var snap = table.metadata.current_snapshot()
    assert_equal(snap.operation(), "overwrite")
    assert_equal(snap.summary_int(String("added-data-files"), -1), 1)
    assert_equal(snap.summary_int(String("deleted-data-files"), -1), 3)
    assert_equal(snap.summary_int(String("total-records"), -1), 3)
    assert_equal(snap.summary_int(String("total-data-files"), -1), 1)


def test_overwrite_with_a_filter() raises:
    """Only the matching rows go; the rest of each file is rewritten."""
    var table = delete_table("ovw_filter", "unpartitioned", 3)
    var schema = Schema.parse(WRITE_SCHEMA)
    var batches = List[RecordBatch]()
    batches.append(write_batch(schema, 200, 2))
    var added = table.overwrite(batches, String('[">","id",11]'))
    assert_equal(added, 2)
    table.refresh()
    var ids = ids_of(table)
    assert_equal(len(ids), 14, "12 kept, 6 deleted, 2 added")
    assert_true(has_id(ids, 11))
    assert_false(has_id(ids, 12))
    assert_true(has_id(ids, 200))
    assert_equal(table.metadata.current_snapshot().operation(), "overwrite")


def test_dynamic_partition_overwrite_touches_only_its_partitions() raises:
    """The partitions the new rows land in are replaced; the others are not."""
    var table = delete_table("ovw_dynamic", "ident", 2)
    var schema = Schema.parse(WRITE_SCHEMA)
    # ids 0 and 5 are both `eu`; this replaces that partition with one row.
    var region = ColumnBuilder.of(schema, 2)
    var ids_b = ColumnBuilder.of(schema, 1)
    var amount = ColumnBuilder.of(schema, 3)
    var ts = ColumnBuilder.of(schema, 4)
    var ok = ColumnBuilder.of(schema, 5)
    ids_b.add(Datum.long_(Int64(900)))
    region.add(Datum.string_(String("eu")))
    amount.add(Datum.double_(1.0))
    ts.add(Datum.integral(ts.kind, WRITE_DAY * WRITE_MICROS_PER_DAY))
    ok.add(Datum.bool_(True))
    var batches = List[RecordBatch]()
    batches.append(batch_of([ids_b^, region^, amount^, ts^, ok^]))
    var added = table.dynamic_partition_overwrite(batches)
    assert_equal(added, 1)
    table.refresh()
    var ids = ids_of(table)
    assert_equal(len(ids), 15, "18 - 4 eu rows + 1")
    assert_true(has_id(ids, 900))
    assert_false(has_id(ids, 0))
    assert_true(has_id(ids, 1), "the `us` partition is untouched")
    var eu = table.scan().filter('["=","region","eu"]').to_table().num_rows()
    assert_equal(eu, 1)


def test_rewritten_manifests_keep_their_sequence_numbers() raises:
    """Inheritance is ADDED-only, so an EXISTING entry a rewrite carries has
    to spell its numbers out or its rows get re-dated — which would silently
    change which deletes apply to it."""
    var table = delete_table("del_seq", "unpartitioned", 3)
    var before = table.scan().plan_files()
    var was = Dict[String, Int64]()
    for k in range(len(before)):
        was[before[k].data_file.file_path] = before[k].data_sequence_number
    _ = table.delete_where(String('["=","id",7]'), MODE_MERGE_ON_READ)
    table.refresh()
    var after = table.scan().plan_files()
    assert_equal(len(after), 3)
    for k in range(len(after)):
        assert_equal(
            after[k].data_sequence_number,
            was[after[k].data_file.file_path],
            "sequence number of " + basename(after[k].data_file.file_path),
        )


def test_equality_deletes_delete_by_value() raises:
    """A v2 equality delete file: values, not positions.

    PyIceberg 0.11.1 cannot plan equality deletes at all, so this library's
    own reader is the check here and DuckDB 1.5.5 is the external one, in
    tools/verify_written.py.
    """
    var table = delete_table("del_equality", "unpartitioned", 2)
    var schema = Schema.parse(WRITE_SCHEMA)
    var ids = ColumnBuilder.of(schema, 1)
    ids.add(Datum.long_(Int64(2)))
    ids.add(Datum.long_(Int64(9)))
    ids.add(Datum.long_(Int64(16)))
    var rows = batch_of([ids^])
    var written = table.delete_by_equality(rows, [1])
    assert_equal(written, 3)
    table.refresh()
    var left = ids_of(table)
    assert_equal(len(left), 15)
    assert_false(has_id(left, 2))
    assert_false(has_id(left, 9))
    assert_false(has_id(left, 16))
    assert_true(has_id(left, 3))

    var deletes = delete_files_of(table)
    assert_equal(len(deletes), 1)
    assert_true(deletes[0].is_equality_delete())
    assert_equal(deletes[0].record_count, 3)
    assert_equal(len(deletes[0].equality_ids), 1)
    assert_equal(deletes[0].equality_ids[0], 1)
    assert_equal(deletes[0].file_format.lower(), "parquet")

    var snap = table.metadata.current_snapshot()
    assert_equal(snap.operation(), "delete")
    assert_equal(snap.summary_int(String("added-equality-deletes"), -1), 3)
    assert_equal(snap.summary_int(String("added-equality-delete-files"), -1), 1)
    assert_equal(snap.summary_int(String("total-equality-deletes"), -1), 3)
    # Rows are not moved, so the data files still hold all eighteen.
    assert_equal(snap.summary_int(String("total-records"), -1), 18)


def test_equality_deletes_are_refused_where_they_do_not_belong() raises:
    var v3 = delete_table("del_eq_v3", "unpartitioned", 3)
    var schema = Schema.parse(WRITE_SCHEMA)
    var ids = ColumnBuilder.of(schema, 1)
    ids.add(Datum.long_(Int64(2)))
    var rows = batch_of([ids^])
    with assert_raises():
        _ = v3.delete_by_equality(rows, [1])
    var part = delete_table("del_eq_part", "ident", 2)
    var more = ColumnBuilder.of(schema, 1)
    more.add(Datum.long_(Int64(2)))
    var rows2 = batch_of([more^])
    with assert_raises():
        _ = part.delete_by_equality(rows2, [1])


def test_expire_snapshots_removes_the_files_nothing_points_at() raises:
    """A copy-on-write delete orphans the files it rewrote — but only once
    the snapshots that still list them are gone."""
    var table = delete_table("expire", "unpartitioned", 2)
    var originals = List[String]()
    var before = table.scan().plan_files()
    for k in range(len(before)):
        originals.append(before[k].data_file.file_path)
    _ = table.delete_where(String('["=","region","eu"]'))
    table.refresh()
    assert_equal(len(table.metadata.snapshots), 4)
    var live = List[String]()
    var after = table.scan().plan_files()
    for k in range(len(after)):
        live.append(after[k].data_file.file_path)
    assert_equal(len(live), 3)

    # A dry run decides and reports, and changes nothing at all.
    var dry = table.expire_snapshots(-1, 1, True)
    assert_equal(len(dry.expired), 3)
    assert_true(dry.dry_run)
    assert_true(dry.total_deleted() > 0)
    for k in range(len(dry.deleted_files)):
        assert_true(
            table.io.exists(dry.deleted_files[k]), "a dry run deletes nothing"
        )
    for k in range(len(live)):
        var doomed = False
        for j in range(len(dry.deleted_files)):
            if dry.deleted_files[j] == live[k]:
                doomed = True
        assert_false(doomed, "a live data file is never a candidate")
    assert_equal(len(table.metadata.snapshots), 4, "a dry run commits nothing")

    var done = table.expire_snapshots(-1, 1, False)
    assert_equal(len(done.expired), 3)
    assert_equal(len(done.retained), 1)
    table.refresh()
    assert_equal(len(table.metadata.snapshots), 1)
    assert_equal(len(table.metadata.snapshot_log), 1)
    # The rows are all still there, read through what is left.
    assert_equal(table.scan().to_table().num_rows(), 14)
    for k in range(len(live)):
        assert_true(table.io.exists(live[k]), "the live files stay")
    for k in range(len(originals)):
        assert_false(
            table.io.exists(originals[k]),
            "the rewritten original " + basename(originals[k]) + " is gone",
        )
    for k in range(len(done.deleted_manifests)):
        assert_false(table.io.exists(done.deleted_manifests[k]))


def test_expire_snapshots_keeps_what_it_is_told_to() raises:
    """`keep_last` is a floor, and the current snapshot is never a candidate."""
    var table = delete_table("expire_keep", "unpartitioned", 2)
    assert_equal(len(table.metadata.snapshots), 3)
    var kept = table.expire_snapshots(-1, 2, False)
    assert_equal(len(kept.expired), 1, "three snapshots, keep two")
    table.refresh()
    assert_equal(len(table.metadata.snapshots), 2)
    assert_equal(table.scan().to_table().num_rows(), 18)
    # Nothing older than the epoch, so nothing to do.
    var none = table.expire_snapshots(0, 1, False)
    assert_equal(len(none.expired), 0)
    table.refresh()
    assert_equal(len(table.metadata.snapshots), 2)
    # And an age cut that reaches exactly one of them.
    var at = table.metadata.snapshots[1].timestamp_ms
    var one = table.expire_snapshots(at, 1, False)
    assert_equal(len(one.expired), 1)
    table.refresh()
    assert_equal(len(table.metadata.snapshots), 1)
    assert_equal(table.scan().to_table().num_rows(), 18)


def test_expire_snapshots_after_a_merge_on_read_delete() raises:
    """The Puffin file a superseded vector lived in goes with its snapshot,
    and the one the current snapshot uses does not."""
    var table = delete_table("expire_mor", "unpartitioned", 3)
    _ = table.delete_where(String('["=","id",0]'), MODE_MERGE_ON_READ)
    table.refresh()
    var first = delete_files_of(table)[0].file_path
    _ = table.delete_where(String('["=","id",3]'), MODE_MERGE_ON_READ)
    table.refresh()
    var second = delete_files_of(table)[0].file_path
    assert_true(first != second)
    var done = table.expire_snapshots(-1, 1, False)
    assert_equal(len(done.expired), 4)
    table.refresh()
    assert_equal(table.scan().to_table().num_rows(), 16)
    assert_false(table.io.exists(first), "the superseded vector's file goes")
    assert_true(table.io.exists(second), "the live one stays")


def test_delete_that_matches_nothing_changes_nothing() raises:
    var table = delete_table("del_none", "unpartitioned", 2)
    var snapshots = len(table.metadata.snapshots)
    assert_equal(table.delete_where(String('["=","id",9999]')), 0)
    table.refresh()
    assert_equal(len(table.metadata.snapshots), snapshots)
    assert_equal(table.scan().to_table().num_rows(), 18)


# ── multi-worker scans ──────────────────────────────────────────────────────
def ordered_rows(result: ScanResult) raises -> List[String]:
    """Every row as one comparable string, in the order the scan produced it.

    `encoded_rows` sorts, because the oracles compare row *sets*. Here the
    order is the thing under test: a parallel scan must return the rows a
    sequential one does, in the same order, or it is not a drop-in.
    """
    var rows = List[String]()
    for r in range(result.num_rows()):
        var line = String("")
        for c in range(result.num_columns()):
            if c > 0:
                line += "\x01"
            line += oracle_cell(result.value(r, c))
        rows.append(line^)
    return rows^


def _join_rows(rows: List[String]) -> String:
    var out = String("")
    for k in range(len(rows)):
        out += rows[k] + "\x02"
    return out^


def test_multi_worker_to_table_is_identical() raises:
    """Every fixture table, read at 1, 2, 4 and 8 workers: same rows, same
    order.

    `bucket_part` is the one that carries the test — it has six data files, so
    six file scan tasks to hand out — but the delete fixtures matter too:
    position deletes, a deletion vector and equality deletes are all applied
    inside the per-file task, so they have to parallelise with it.
    """
    var tables = all_fixture_table_names()
    var workers = [2, 4, 8]
    var checked = 0
    for t in range(len(tables)):
        var one = ScanOptions()
        var want = _join_rows(
            ordered_rows(fixture_scan(tables[t]).to_table(one))
        )
        for w in range(len(workers)):
            var opts = ScanOptions()
            opts.num_workers = workers[w]
            var got = _join_rows(
                ordered_rows(fixture_scan(tables[t]).to_table(opts))
            )
            assert_equal(
                got,
                want,
                String(
                    workers[w],
                    " workers changed ",
                    tables[t],
                ),
            )
            checked += 1
    print("    multi-worker to_table:", checked, "comparisons")


def test_multi_worker_to_batches_is_identical() raises:
    """The Arrow fast path too: same batch count, same rows, same order."""
    var tables = all_fixture_table_names()
    var checked = 0
    for t in range(len(tables)):
        var one = ScanOptions()
        var base = fixture_scan(tables[t]).to_batches(one)
        var opts = ScanOptions()
        opts.num_workers = 4
        var got = fixture_scan(tables[t]).to_batches(opts)
        assert_equal(len(got), len(base), "batch count for " + tables[t])
        for b in range(len(base)):
            assert_equal(
                got[b].num_rows, base[b].num_rows, "batch rows " + tables[t]
            )
        checked += 1
    print("    multi-worker to_batches:", checked, "tables")


def test_multi_worker_scan_reports_a_failure() raises:
    """A task cannot raise, so a failure comes back through its slot. Point a
    scan at a file that is not there and the error still reaches the caller."""
    var table = load_fixture_metadata("bucket_part")
    var io = FileIO.local()
    io.rebase(WAREHOUSE_PREFIX, FIXTURES + "/nowhere")
    var opts = ScanOptions()
    opts.num_workers = 4
    with assert_raises():
        _ = TableScan(table^, io^).to_table(opts)


def test_num_workers_zero_uses_every_core() raises:
    """`num_workers = 0` is "one per core", not "no workers"."""
    var opts = ScanOptions()
    opts.num_workers = 0
    assert_equal(
        fixture_scan("bucket_part").to_table(opts).num_rows(),
        fixture_scan("bucket_part").to_table().num_rows(),
    )


# ── the per-scan manifest cache ────────────────────────────────────────────


def _plan_digest(tasks: List[FileScanTask]) raises -> String:
    """Everything a planned task says, rendered so two plans can be compared
    as one string — order included."""
    var out = String()
    for k in range(len(tasks)):
        ref t = tasks[k]
        out += String(
            t.data_file.file_path,
            "|",
            t.data_file.content,
            "|",
            t.data_file.record_count,
            "|",
            t.data_file.file_size_in_bytes,
            "|",
            t.residual,
            "|",
            t.spec_id,
            "|",
            t.data_sequence_number,
            "|",
            t.data_file.first_row_id,
            ":",
            t.data_file.has_first_row_id,
            "|parts=",
        )
        for j in range(len(t.data_file.partition)):
            out += String(t.data_file.partition[j], ",")
        out += "|metrics="
        for j in range(len(t.data_file.metrics)):
            ref m = t.data_file.metrics[j]
            out += String(
                m.field_id,
                ":",
                m.value_count,
                ":",
                m.null_value_count,
                ":",
                hex_of(m.lower_bound),
                ":",
                hex_of(m.upper_bound),
                ",",
            )
        out += "|deletes="
        for j in range(len(t.delete_files)):
            out += t.delete_files[j].file_path + ","
        out += "\n"
    return out^


def test_the_manifest_cache_is_transparent() raises:
    """`evolved` has manifests written under different table schemas, so one
    scan meets more than one manifest shape. Planning it with the cache and
    without has to give the same answer, byte for byte."""
    for name in fixture_table_names():
        var scan = fixture_scan(name)
        var warm = ManifestCache()
        var cold = ManifestCache.disabled()
        assert_equal(
            _plan_digest(scan.plan_files_with(warm)),
            _plan_digest(scan.plan_files_with(cold)),
            String("cached plan differs for ", name),
        )
        # A disabled cache stores nothing and never claims a hit.
        assert_equal(cold.hits, 0)
        assert_equal(len(cold.shape_keys), 0)
        assert_equal(len(cold.plans), 0)


def test_schema_evolution_gives_a_scan_more_than_one_manifest_shape() raises:
    var scan = fixture_scan("evolved")
    var cache = ManifestCache()
    var tasks = scan.plan_files_with(cache)
    assert_true(len(tasks) > 0)
    # Manifests written before and after the evolution carry different
    # `schema` metadata, so they cannot share a shape.
    assert_true(
        len(cache.shapes) >= 2,
        String("evolved cached only ", len(cache.shapes), " shape(s)"),
    )
    # ... and they are told apart by the bytes, not by the hash: force every
    # key to hash the same and the plan must not change.
    var collide = ManifestCache()
    collide.collide_all_hashes()
    assert_equal(
        _plan_digest(tasks), _plan_digest(scan.plan_files_with(collide))
    )
    assert_equal(len(collide.shapes), len(cache.shapes))


def test_one_cache_carries_across_snapshots() raises:
    """Time travel plans several snapshots of one table; the manifests they
    share are read through one cache."""
    var m = load_fixture_metadata("evolved")
    var scan = fixture_scan("evolved")
    var cache = ManifestCache()
    var planned = 0
    for k in range(len(m.snapshots)):
        var s = scan.use_snapshot(m.snapshots[k].snapshot_id)
        planned += len(s.plan_files_with(cache))
        # Each snapshot's plan is the same one a fresh cache produces.
        var fresh = ManifestCache.disabled()
        assert_equal(
            _plan_digest(s.plan_files_with(cache)),
            _plan_digest(s.plan_files_with(fresh)),
        )
    assert_true(planned > 0)
    assert_true(cache.hits > 0, "no manifest was read twice")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
