"""Build the tables the write path is verified against.

Ten tables — five partitioning shapes x format versions 2 and 3 — each created
empty and then appended to three times, so each has three snapshots. Everything
here goes through the public API: `FilesystemCatalog.create_table`,
`Table.new_append()`, `commit()`.

`tools/verify_written.py` then reads every one of them with PyIceberg 0.11.1
and DuckDB 1.5.5 and compares rows, snapshots, partition values and statistics
against the manifest this writer produced.

    write-tables <warehouse-dir>
"""

from std.memory import bitcast
from std.sys import argv

from parquet import RecordBatch

from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.io import FileIO
from iceberg.schema import Schema
from iceberg.transforms import PartitionField, PartitionSpec, parse_transform
from iceberg.values import Datum


comptime SCHEMA_JSON = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"region","required":true,"type":"string"},'
    '{"id":3,"name":"amount","required":false,"type":"double"},'
    '{"id":4,"name":"ts","required":false,"type":"timestamp"},'
    '{"id":5,"name":"ok","required":false,"type":"boolean"}]}'
)

comptime DAY_2024_01_01: Int64 = 19723
comptime MICROS_PER_DAY: Int64 = 86400000000
comptime REGIONS = String("eu,us,apac,latam,emea")


def region_of(i: Int) raises -> String:
    var parts = REGIONS.split(",")
    return String(parts[i % 5])


def make_batch(schema: Schema, start: Int, n: Int) raises -> RecordBatch:
    """`n` rows starting at `start`, with nulls in `amount` and in `ok`."""
    var ids = ColumnBuilder.of(schema, 1)
    var region = ColumnBuilder.of(schema, 2)
    var amount = ColumnBuilder.of(schema, 3)
    var ts = ColumnBuilder.of(schema, 4)
    var ok = ColumnBuilder.of(schema, 5)
    for k in range(n):
        var i = start + k
        ids.add(Datum.long_(Int64(i)))
        region.add(Datum.string_(region_of(i)))
        if i % 4 == 0:
            amount.add_null()
        else:
            amount.add(Datum.double_(Float64(i) * 1.5))
        ts.add(
            Datum.integral(
                ts.kind,
                (DAY_2024_01_01 + Int64(i % 3)) * MICROS_PER_DAY
                + Int64(i) * 1000,
            )
        )
        if i % 5 == 0:
            ok.add_null()
        else:
            ok.add(Datum.bool_(i % 2 == 0))
    return batch_of([ids^, region^, amount^, ts^, ok^])


def spec_for(kind: String) raises -> PartitionSpec:
    if kind == "unpartitioned":
        return PartitionSpec.unpartitioned()
    if kind == "ident":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    2, 1000, String("region"), parse_transform("identity")
                )
            ],
        )
    if kind == "bucket":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    1, 1000, String("id_bucket"), parse_transform("bucket[4]")
                )
            ],
        )
    if kind == "day":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    4, 1000, String("ts_day"), parse_transform("day")
                )
            ],
        )
    if kind == "trunc":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    2,
                    1000,
                    String("region_trunc"),
                    parse_transform("truncate[3]"),
                )
            ],
        )
    raise Error("unknown partition shape: " + kind)


def main() raises:
    var args = argv()
    if len(args) < 2:
        print("usage: write-tables <warehouse-dir>")
        return
    var warehouse = String(args[1])
    var catalog = FilesystemCatalog.local(warehouse)
    var schema = Schema.parse(SCHEMA_JSON)
    var shapes: List[String] = [
        String("unpartitioned"),
        String("ident"),
        String("bucket"),
        String("day"),
        String("trunc"),
    ]
    var versions: List[Int] = [2, 3]
    for vi in range(len(versions)):
        var v = versions[vi]
        for si in range(len(shapes)):
            var name = shapes[si] + "_v" + String(v)
            var table = catalog.create_table(
                String("db"),
                name,
                schema,
                spec_for(shapes[si]),
                Dict[String, String](),
                v,
            )
            var total: Int64 = 0
            for b in range(3):
                var batches = List[RecordBatch]()
                batches.append(make_batch(schema, b * 6, 6))
                var tx = table.new_append()
                tx.add_batches(batches)
                total += tx.commit()
                table.refresh()
            print(
                "wrote db." + name,
                "v" + String(v),
                String(total) + " rows",
                String(len(table.metadata.snapshots)) + " snapshots",
                "next-row-id=" + String(table.metadata.next_row_id),
            )
