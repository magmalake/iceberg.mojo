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
from parquet.arrow import (
    AT_BOOL,
    AT_FLOAT64,
    AT_INT64,
    AT_TIMESTAMP,
    AT_UTF8,
    TU_MICRO,
    ArrayData,
    ArrowType,
    bit_set,
)

from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.io import FileIO
from iceberg.schema import Schema
from iceberg.transforms import PartitionField, PartitionSpec, parse_transform


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


def _finish(mut a: ArrayData, n: Int):
    while len(a.validity) < (n + 7) // 8:
        a.validity.append(0)


def i64_col(
    name: String, fid: Int, values: List[Int64], valid: List[Bool]
) raises -> ArrayData:
    var a = ArrayData(ArrowType(AT_INT64), name)
    a.field_id = Int32(fid)
    a.length = len(values)
    for k in range(len(values)):
        bit_set(a.validity, k, valid[k])
        if not valid[k]:
            a.null_count += 1
        var v = UInt64(values[k])
        for b in range(8):
            a.values.append(UInt8((v >> UInt64(8 * b)) & 0xFF))
    _finish(a, len(values))
    return a^


def ts_col(
    name: String, fid: Int, values: List[Int64], valid: List[Bool]
) raises -> ArrayData:
    var a = i64_col(name, fid, values, valid)
    var t = ArrowType(AT_TIMESTAMP)
    t.unit = TU_MICRO
    a.type = t^
    return a^


def f64_col(
    name: String, fid: Int, values: List[Float64], valid: List[Bool]
) raises -> ArrayData:
    var a = ArrayData(ArrowType(AT_FLOAT64), name)
    a.field_id = Int32(fid)
    a.length = len(values)
    for k in range(len(values)):
        bit_set(a.validity, k, valid[k])
        if not valid[k]:
            a.null_count += 1
        var bits = UInt64(0)
        if valid[k]:
            bits = bitcast[DType.uint64](values[k])
        for b in range(8):
            a.values.append(UInt8((bits >> UInt64(8 * b)) & 0xFF))
    _finish(a, len(values))
    return a^


def bool_col(
    name: String, fid: Int, values: List[Bool], valid: List[Bool]
) raises -> ArrayData:
    var a = ArrayData(ArrowType(AT_BOOL), name)
    a.field_id = Int32(fid)
    a.length = len(values)
    for k in range(len(values)):
        bit_set(a.validity, k, valid[k])
        if not valid[k]:
            a.null_count += 1
        while len(a.values) <= k // 8:
            a.values.append(0)
        if valid[k] and values[k]:
            a.values[k // 8] |= UInt8(1) << UInt8(k % 8)
    while len(a.values) < (len(values) + 7) // 8:
        a.values.append(0)
    _finish(a, len(values))
    return a^


def str_col(
    name: String, fid: Int, values: List[String], valid: List[Bool]
) raises -> ArrayData:
    var a = ArrayData(ArrowType(AT_UTF8), name)
    a.field_id = Int32(fid)
    a.length = len(values)
    a.offsets.append(0)
    for k in range(len(values)):
        bit_set(a.validity, k, valid[k])
        if valid[k]:
            a.values.extend(values[k].as_bytes())
        else:
            a.null_count += 1
        a.offsets.append(Int32(len(a.values)))
    _finish(a, len(values))
    return a^


comptime REGIONS = String("eu,us,apac,latam,emea")


def region_of(i: Int) raises -> String:
    var parts = REGIONS.split(",")
    return String(parts[i % 5])


def make_batch(start: Int, n: Int) raises -> RecordBatch:
    """`n` rows starting at `start`, with a null in `amount` and in `ok`."""
    var ids = List[Int64]()
    var regions = List[String]()
    var amounts = List[Float64]()
    var amount_valid = List[Bool]()
    var timestamps = List[Int64]()
    var ts_valid = List[Bool]()
    var oks = List[Bool]()
    var ok_valid = List[Bool]()
    var all_valid = List[Bool]()
    for k in range(n):
        var i = start + k
        ids.append(Int64(i))
        regions.append(region_of(i))
        amounts.append(Float64(i) * 1.5)
        amount_valid.append(i % 4 != 0)
        timestamps.append((DAY_2024_01_01 + Int64(i % 3)) * MICROS_PER_DAY + Int64(i) * 1000)
        ts_valid.append(True)
        oks.append(i % 2 == 0)
        ok_valid.append(i % 5 != 0)
        all_valid.append(True)

    var batch = RecordBatch()
    batch.num_rows = n
    batch.roots.append(
        batch.arena.add(i64_col(String("id"), 1, ids, all_valid))
    )
    batch.roots.append(
        batch.arena.add(str_col(String("region"), 2, regions, all_valid))
    )
    batch.roots.append(
        batch.arena.add(f64_col(String("amount"), 3, amounts, amount_valid))
    )
    batch.roots.append(
        batch.arena.add(ts_col(String("ts"), 4, timestamps, ts_valid))
    )
    batch.roots.append(
        batch.arena.add(bool_col(String("ok"), 5, oks, ok_valid))
    )
    return batch^


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
                batches.append(make_batch(b * 6, 6))
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
