"""Fixture + oracle generator for the native Mojo Iceberg reader.

Creates tables 1-5 of the fixture set with the iceberg-rs.mojo bridge
(iceberg-rust 0.10.1), each with three appends (three snapshots), and writes
the bridge's oracle outputs next to them:

    <out>/<table>/oracle/metadata.json     t.metadata_json()
    <out>/<table>/oracle/snapshots.json    t.snapshots_json()
    <out>/<table>/oracle/plan_<k>.json     t.scan("", FILTER).plan_files()
    <out>/<table>/oracle/plan_<k>.filter.txt

The warehouse root comes from $FIXTURE_ROOT, the oracle output root from
$FIXTURE_OUT.  Both directories (including <out>/<table>/oracle) must already
exist; tools/make_fixtures.sh creates them.

Run from the iceberg-rs.mojo project dir:

    pixi run -e default mojo run -I src -I ../iceberg.mojo/tools \
        ../iceberg.mojo/tools/make_fixtures.mojo
"""

from std.os import getenv
from iceberg_rs import Catalog, Table, Batch, version


# All five bridge-built tables share one schema; only the partition spec
# differs.  Keeping it constant makes the fixtures easy to diff against each
# other and keeps the append helper below a single function.
comptime SCHEMA = String(
    '{"type":"struct","schema-id":0,"fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"region","required":true,"type":"string"},'
    '{"id":3,"name":"amount","required":false,"type":"double"},'
    '{"id":4,"name":"ts","required":false,"type":"timestamp"},'
    '{"id":5,"name":"ok","required":false,"type":"boolean"},'
    '{"id":6,"name":"cnt","required":false,"type":"int"}]}'
)

# 2023-11-14T00:00:00Z .. 2024-01-01T00:00:00Z, in microseconds since the epoch.
comptime D14 = Int64(1699920000000000)
comptime D15 = Int64(1700006400000000)
comptime D16 = Int64(1700136000000000)
comptime D17 = Int64(1700179200000000)
comptime D_DEC = Int64(1701388800000000)
comptime D_JAN = Int64(1704067200000000)


def write_text(path: String, text: String) raises:
    with open(path, "w") as f:
        f.write(text)


def append_rows(
    mut t: Table,
    imm ids: List[Int64],
    imm regions: List[String],
    imm amounts: List[Float64],
    imm amount_valid: List[Bool],
    imm ts: List[Int64],
    imm ok: List[Bool],
    imm cnt: List[Int64],
) raises -> Int64:
    var b = t.builder()
    b.int_col("id", ids)
    b.str_col("region", regions)
    b.float_col("amount", amounts, amount_valid)
    b.int_col("ts", ts)
    b.bool_col("ok", ok)
    b.int_col("cnt", cnt)
    t.append(b.build())
    return t.commit()


def emit_oracle(
    imm t: Table, out_dir: String, imm filters: List[String]
) raises:
    write_text(out_dir + "/metadata.json", t.metadata_json())
    write_text(out_dir + "/snapshots.json", t.snapshots_json())
    for k in range(len(filters)):
        var s = t.scan("", filters[k])
        write_text(out_dir + "/plan_" + String(k) + ".json", s.plan_files())
        write_text(
            out_dir + "/plan_" + String(k) + ".filter.txt", filters[k]
        )
    print("  oracle written:", out_dir, "(", len(filters), "filters )")


def fill(mut t: Table, imm regions1: List[String], imm regions2: List[String],
         imm regions3: List[String]) raises -> Int:
    """Three appends -> three snapshots.  Row ids 1..7."""
    var s1 = append_rows(
        t,
        [Int64(1), Int64(2), Int64(3)],
        regions1,
        [1.5, 0.0, 3.5],
        [True, False, True],
        [D14, D15, D16],
        [True, False, True],
        [Int64(10), Int64(20), Int64(30)],
    )
    var s2 = append_rows(
        t,
        [Int64(4), Int64(5)],
        regions2,
        [4.5, 5.5],
        [True, True],
        [D17, D_DEC],
        [False, True],
        [Int64(40), Int64(50)],
    )
    var s3 = append_rows(
        t,
        [Int64(6), Int64(7)],
        regions3,
        [6.5, 0.0],
        [True, False],
        [D_JAN, D14],
        [True, False],
        [Int64(60), Int64(70)],
    )
    print("  snapshots:", s1, s2, s3)
    return 3


def main() raises:
    print("iceberg crate version:", version())

    var root = getenv("FIXTURE_ROOT", "")
    var out = getenv("FIXTURE_OUT", "")
    if root == "" or out == "":
        raise Error("FIXTURE_ROOT and FIXTURE_OUT must be set")
    var db_uri = String("sqlite:") + root + "/catalog.db?mode=rwc"
    var warehouse = String("file://") + root + "/warehouse"
    print("catalog:  ", db_uri)
    print("warehouse:", warehouse)
    print("out:      ", out)

    var cat = Catalog.sql(db_uri, warehouse)
    cat.create_namespace("db")

    var short1: List[String] = ["eu", "us", "eu"]
    var short2: List[String] = ["us", "apac"]
    var short3: List[String] = ["eu", "apac"]

    var long1: List[String] = ["europe", "usa", "eurasia"]
    var long2: List[String] = ["usa", "apac-north"]
    var long3: List[String] = ["europe", "apac-south"]

    # ---- 1. unpartitioned -------------------------------------------------
    print("== unpartitioned ==")
    var t1 = cat.create_table("db", "unpartitioned", SCHEMA)
    _ = fill(t1, short1, short2, short3)
    var f1: List[String] = [
        String('["true"]'),
        String('["=","region","eu"]'),
        String('[">","id",2]'),
        String('["and",[">","id",1],["is-null","amount"]]'),
        String('["in","region",["eu","us"]]'),
        String('["starts-with","region","e"]'),
    ]
    emit_oracle(t1, out + "/unpartitioned/oracle", f1)

    # ---- 2. ident_part ----------------------------------------------------
    print("== ident_part ==")
    var spec2 = String(
        '{"spec-id":0,"fields":['
        '{"source-id":2,"name":"region","transform":"identity"}]}'
    )
    var t2 = cat.create_table("db", "ident_part", SCHEMA, spec2)
    _ = fill(t2, short1, short2, short3)
    emit_oracle(t2, out + "/ident_part/oracle", f1)

    # ---- 3. bucket_part ---------------------------------------------------
    print("== bucket_part ==")
    var spec3 = String(
        '{"spec-id":0,"fields":['
        '{"source-id":1,"name":"id_bucket","transform":"bucket[4]"}]}'
    )
    var t3 = cat.create_table("db", "bucket_part", SCHEMA, spec3)
    _ = fill(t3, short1, short2, short3)
    var f3: List[String] = [
        String('["true"]'),
        String('["=","id",3]'),
        String('[">","id",2]'),
        String('["and",["=","id",1],["=","region","eu"]]'),
        String('["in","id",[1,4,7]]'),
        String('["not-null","amount"]'),
    ]
    emit_oracle(t3, out + "/bucket_part/oracle", f3)

    # ---- 4. day_part ------------------------------------------------------
    print("== day_part ==")
    var spec4 = String(
        '{"spec-id":0,"fields":['
        '{"source-id":4,"name":"ts_day","transform":"day"}]}'
    )
    var t4 = cat.create_table("db", "day_part", SCHEMA, spec4)
    _ = fill(t4, short1, short2, short3)
    var f4: List[String] = [
        String('["true"]'),
        String('[">=","ts","2023-11-16T00:00:00"]'),
        String('["<","ts","2023-11-16T00:00:00"]'),
        String('["and",[">=","ts","2023-11-15T00:00:00"],["<","ts","2023-11-18T00:00:00"]]'),
        String('["is-null","ts"]'),
        String('["=","region","eu"]'),
    ]
    emit_oracle(t4, out + "/day_part/oracle", f4)

    # ---- 5. trunc_part ----------------------------------------------------
    print("== trunc_part ==")
    var spec5 = String(
        '{"spec-id":0,"fields":['
        '{"source-id":2,"name":"region_trunc","transform":"truncate[3]"}]}'
    )
    var t5 = cat.create_table("db", "trunc_part", SCHEMA, spec5)
    _ = fill(t5, long1, long2, long3)
    var f5: List[String] = [
        String('["true"]'),
        String('["=","region","europe"]'),
        String('["starts-with","region","eur"]'),
        String('["in","region",["europe","usa"]]'),
        String('["and",["starts-with","region","apac"],[">","id",4]]'),
        String('["not-in","region",["europe"]]'),
    ]
    emit_oracle(t5, out + "/trunc_part/oracle", f5)

    print("BRIDGE FIXTURES OK")
