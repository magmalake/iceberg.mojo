"""Append benchmark: a million rows into a fresh table, timed end to end.

Everything a commit does is inside the measurement — building the Arrow
batches, writing the Parquet data files, reading their footers back for the
statistics, writing the manifest and the manifest list, and writing the new
`metadata.json`. `tools/bench_pyiceberg_append.py` does the same thing with
PyIceberg 0.11.1 on the same rows for comparison.

The table is rebuilt from scratch each run, under
`$ICEBERG_APPEND_BENCH_ROOT` (default `build/append-bench`).
"""

from std.collections import Dict
from std.os import getenv, makedirs
from std.time import perf_counter_ns

from parquet import RecordBatch

from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.io import FileIO
from iceberg.schema import Schema
from iceberg.transforms import PartitionSpec
from iceberg.values import Datum


comptime SCHEMA_JSON = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"region","required":true,"type":"string"},'
    '{"id":3,"name":"amount","required":false,"type":"double"},'
    '{"id":4,"name":"ts","required":false,"type":"timestamp"},'
    '{"id":5,"name":"ok","required":false,"type":"boolean"},'
    '{"id":6,"name":"cnt","required":false,"type":"long"}]}'
)

comptime BASE_TS: Int64 = 1704067200000000
comptime REGIONS = String("eu,us,apac,latam,emea")


def batch(schema: Schema, start: Int, n: Int) raises -> RecordBatch:
    var names = REGIONS.split(",")
    var ids = ColumnBuilder.of(schema, 1)
    var region = ColumnBuilder.of(schema, 2)
    var amount = ColumnBuilder.of(schema, 3)
    var ts = ColumnBuilder.of(schema, 4)
    var ok = ColumnBuilder.of(schema, 5)
    var cnt = ColumnBuilder.of(schema, 6)
    for k in range(n):
        var i = start + k
        ids.add(Datum.long_(Int64(i)))
        region.add(Datum.string_(String(names[i % 5])))
        if i % 7 == 0:
            amount.add_null()
        else:
            amount.add(Datum.double_(Float64(i) * 0.5))
        ts.add(Datum.integral(ts.kind, BASE_TS + Int64(i % 86400) * 1000000))
        if i % 11 == 0:
            ok.add_null()
        else:
            ok.add(Datum.bool_(i % 3 == 0))
        cnt.add(Datum.long_(Int64(i % 1000)))
    return batch_of([ids^, region^, amount^, ts^, ok^, cnt^])


def _clear(io: FileIO, root: String) raises:
    try:
        var existing = io.list(root)
        for k in range(len(existing)):
            io.delete(existing[k])
    except:
        pass


def main() raises:
    var root = getenv("ICEBERG_APPEND_BENCH_ROOT", "build/append-bench")
    var rows = 1000000
    var chunk_text = getenv("ICEBERG_APPEND_BENCH_CHUNK", "250000")
    var chunk = Int(chunk_text)
    var rows_text = getenv("ICEBERG_APPEND_BENCH_ROWS", "")
    if rows_text != "":
        rows = Int(rows_text)

    var io = FileIO.local()
    _clear(io, root)
    makedirs(root, exist_ok=True)
    var catalog = FilesystemCatalog.local(root)
    var schema = Schema.parse(SCHEMA_JSON)
    var props = Dict[String, String]()
    props[String("write.parquet.compression-codec")] = String("zstd")
    var table = catalog.create_table(
        String("db"),
        String("bench"),
        schema,
        PartitionSpec.unpartitioned(),
        props^,
        2,
    )

    var t0 = perf_counter_ns()
    var written = 0
    while written < rows:
        var n = chunk if written + chunk <= rows else rows - written
        var batches = List[RecordBatch]()
        batches.append(batch(schema, written, n))
        var tx = table.new_append()
        tx.add_batches(batches)
        _ = tx.commit()
        table.refresh()
        written += n
    var ns = perf_counter_ns() - t0

    var bytes: Int64 = 0
    var tasks = table.scan().plan_files()
    for k in range(len(tasks)):
        bytes += tasks[k].data_file.file_size_in_bytes
    var seconds = Float64(ns) / 1.0e9
    print(
        "iceberg.mojo append:",
        written,
        "rows in",
        Int(seconds * 1000),
        "ms —",
        Int(Float64(written) / seconds),
        "rows/s,",
        Int(Float64(bytes) / seconds / 1048576.0),
        "MB/s of Parquet,",
        len(table.metadata.snapshots),
        "snapshots,",
        len(tasks),
        "files,",
        Int(bytes / 1048576),
        "MB",
    )
