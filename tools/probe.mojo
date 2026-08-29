from std.os import getenv
from iceberg_rs import Catalog, Table, Batch, version


def main() raises:
    print("iceberg crate version:", version())
    var root = getenv("FIXTURE_ROOT", "/tmp/probe")
    var db_uri = String("sqlite:") + root + "/catalog.db?mode=rwc"
    var warehouse = String("file://") + root + "/warehouse"
    var cat = Catalog.sql(db_uri, warehouse)
    cat.create_namespace("db")

    var schema = String(
        '{"type":"struct","schema-id":0,"fields":['
        '{"id":1,"name":"id","required":true,"type":"long"},'
        '{"id":2,"name":"region","required":true,"type":"string"},'
        '{"id":3,"name":"amount","required":false,"type":"double"},'
        '{"id":4,"name":"ts","required":false,"type":"timestamp"}]}'
    )

    print("-- bucket --")
    var bspec = String(
        '{"spec-id":0,"fields":['
        '{"source-id":1,"name":"id_bucket","transform":"bucket[4]"}]}'
    )
    var bt = cat.create_table("db", "bucket_probe", schema, bspec)
    print(bt.partition_spec_json())
    var b = bt.builder()
    b.int_col("id", [Int64(1), Int64(2), Int64(3), Int64(4)])
    b.str_col("region", ["eu", "us", "eu", "apac"])
    b.float_col("amount", [1.5, 2.5, 3.5, 4.5])
    b.int_col("ts", [Int64(1700000000000000), Int64(1700086400000000), Int64(1700172800000000), Int64(1700259200000000)])
    bt.append(b.build())
    print("snap", bt.commit())
    var ps = bt.scan("", "")
    print("PLAN:", ps.plan_files())

    print("-- day --")
    var dspec = String(
        '{"spec-id":0,"fields":['
        '{"source-id":4,"name":"ts_day","transform":"day"}]}'
    )
    var dt = cat.create_table("db", "day_probe", schema, dspec)
    print(dt.partition_spec_json())
    var d = dt.builder()
    d.int_col("id", [Int64(1), Int64(2)])
    d.str_col("region", ["eu", "us"])
    d.float_col("amount", [1.5, 2.5])
    d.int_col("ts", [Int64(1700000000000000), Int64(1700259200000000)])
    dt.append(d.build())
    print("snap", dt.commit())

    print("-- truncate --")
    var tspec = String(
        '{"spec-id":0,"fields":['
        '{"source-id":2,"name":"region_trunc","transform":"truncate[3]"}]}'
    )
    var tt = cat.create_table("db", "trunc_probe", schema, tspec)
    print(tt.partition_spec_json())
    var tb = tt.builder()
    tb.int_col("id", [Int64(1), Int64(2)])
    tb.str_col("region", ["europe", "usa"])
    tb.float_col("amount", [1.5, 2.5])
    tb.int_col("ts", [Int64(1700000000000000), Int64(1700259200000000)])
    tt.append(tb.build())
    print("snap", tt.commit())
    var s2 = tt.scan("", '["starts-with","region","eu"]')
    print("PLAN2:", s2.plan_files())
    print("SNAPS:", tt.snapshots_json())
    print("PROBE OK")
