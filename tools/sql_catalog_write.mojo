"""Write a table through `SqlCatalog`, for cross-implementation parity checks.

`tools/verify_sql_catalog.sh` drives this, then reads the same sqlite file
back with PyIceberg's `SqlCatalog` (and, separately, has PyIceberg write into
a fresh catalog for this repo's own `SqlCatalog` — and `iceberg-mojo cat
--sql` — to read back). This tool prints one `KEY value` line per fact the
shell script or `tools/verify_sql_catalog.py` needs to check, so nothing has
to be re-derived from stdout parsing beyond splitting on the first space.

    sql-catalog-write <sqlite-db-path> <warehouse-dir>
"""

from std.sys import argv

from parquet import RecordBatch

from iceberg.batch import ColumnBuilder, batch_of
from iceberg.values import Datum
from iceberg.catalog.sql import SqlCatalog
from iceberg.schema import Schema
from iceberg.transforms import PartitionSpec


comptime SCHEMA_JSON = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"region","required":true,"type":"string"},'
    '{"id":3,"name":"amount","required":false,"type":"double"}]}'
)
comptime REGIONS = String("eu,us,apac,latam,emea")


def region_of(i: Int) raises -> String:
    var parts = REGIONS.split(",")
    return String(parts[i % 5])


def make_batch(schema: Schema, start: Int, n: Int) raises -> RecordBatch:
    var ids = ColumnBuilder.of(schema, 1)
    var region = ColumnBuilder.of(schema, 2)
    var amount = ColumnBuilder.of(schema, 3)
    for k in range(n):
        var i = start + k
        ids.add(Datum.long_(Int64(i)))
        region.add(Datum.string_(region_of(i)))
        amount.add(Datum.double_(Float64(i) * 1.5))
    return batch_of([ids^, region^, amount^])


def main() raises:
    var args = argv()
    if len(args) < 3:
        print("usage: sql-catalog-write <sqlite-db-path> <warehouse-dir>")
        return
    var db_path = String(args[1])
    var warehouse = String(args[2])

    var cat = SqlCatalog.local("default", "sqlite:///" + db_path, warehouse)
    cat.create_namespace("db", {"owner": "marius"})

    var schema = Schema.parse(SCHEMA_JSON)
    var created = cat.create_table(
        "db",
        "rt",
        schema,
        PartitionSpec.unpartitioned(),
        Dict[String, String](),
        2,
    )
    print("NAMESPACE db")
    print("TABLE db.rt")
    print("METADATA_LOCATION_CREATED " + created.metadata_location)

    var b1 = List[RecordBatch]()
    b1.append(make_batch(schema, 0, 6))
    var after1 = cat.append("db", "rt", b1)
    print("METADATA_LOCATION_AFTER_APPEND1 " + after1.metadata_location)
    print("SNAPSHOTS_AFTER_APPEND1 " + String(len(after1.metadata.snapshots)))

    var b2 = List[RecordBatch]()
    b2.append(make_batch(schema, 6, 6))
    var after2 = cat.append("db", "rt", b2)
    print("METADATA_LOCATION_AFTER_APPEND2 " + after2.metadata_location)
    print("SNAPSHOTS_AFTER_APPEND2 " + String(len(after2.metadata.snapshots)))
    print("ROWS_AFTER_APPEND2 12")

    # A scratch table this run drops, and a rename, so the Python side can
    # confirm both operations landed in the same database.
    _ = cat.create_table("db", "scratch", schema)
    cat.drop_table("db", "scratch")
    print("DROPPED db.scratch")

    _ = cat.rename_table("db", "rt", "db", "rt_renamed")
    print("RENAMED db.rt_renamed")

    print("OK")
