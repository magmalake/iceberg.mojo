"""Scan planning over many manifests — the read path that is pure Avro.

Planning a scan opens the snapshot's manifest list and then every manifest it
does not skip, decoding one `manifest_entry` per data file. No Parquet is
touched, so this measures the Avro reader and the inheritance rules on top of
it and nothing else.

The table is built once under `$ICEBERG_PLANNING_BENCH_ROOT` (default
`build/planning-bench`) by committing `$ICEBERG_PLANNING_APPENDS` times —
each commit writes its own manifest, which is how a table ends up with
hundreds of them. It is not checked in.
"""

from std.collections import Dict
from std.os import getenv, makedirs
from std.time import perf_counter_ns

from parquet import RecordBatch

from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.io import FileIO
from iceberg.manifest import read_manifest_io, read_manifest_list_io
from iceberg.schema import Schema
from iceberg.transforms import PartitionSpec
from iceberg.values import Datum

comptime SCHEMA_JSON = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"region","required":true,"type":"string"},'
    '{"id":3,"name":"amount","required":false,"type":"double"},'
    '{"id":4,"name":"ok","required":false,"type":"boolean"}]}'
)

comptime REGIONS = String("eu,us,apac,latam,emea")


def small_batch(schema: Schema, start: Int, n: Int) raises -> RecordBatch:
    var names = REGIONS.split(",")
    var ids = ColumnBuilder.of(schema, 1)
    var region = ColumnBuilder.of(schema, 2)
    var amount = ColumnBuilder.of(schema, 3)
    var ok = ColumnBuilder.of(schema, 4)
    for k in range(n):
        var i = start + k
        ids.add(Datum.long_(Int64(i)))
        region.add(Datum.string_(String(names[i % 5])))
        amount.add(Datum.double_(Float64(i) * 0.5))
        ok.add(Datum.bool_(i % 3 == 0))
    return batch_of([ids^, region^, amount^, ok^])


def build_table(root: String, appends: Int, files: Int) raises:
    var io = FileIO.local()
    try:
        var existing = io.list(root)
        for k in range(len(existing)):
            io.delete(existing[k])
    except:
        pass
    makedirs(root, exist_ok=True)
    var catalog = FilesystemCatalog.local(root)
    var schema = Schema.parse(SCHEMA_JSON)
    var props = Dict[String, String]()
    var table = catalog.create_table(
        String("db"),
        String("planning"),
        schema,
        PartitionSpec.unpartitioned(),
        props^,
        2,
    )
    var at = 0
    for _a in range(appends):
        var batches = List[RecordBatch]()
        for _f in range(files):
            batches.append(small_batch(schema, at, 4))
            at += 4
        var tx = table.new_append()
        tx.add_batches(batches)
        _ = tx.commit()
        table.refresh()


def best_ms(t: Table, reps: Int) raises -> Tuple[Float64, Int]:
    var best = 0
    var tasks = 0
    for r in range(reps):
        var t0 = perf_counter_ns()
        var plan = t.scan().plan_files()
        var ns = perf_counter_ns() - t0
        tasks = len(plan)
        if r == 0 or ns < best:
            best = ns
    return (Float64(best) / 1.0e6, tasks)


def two_dp(v: Float64) -> String:
    """Two decimals without pulling in a formatter."""
    var h = Int(v * 100.0 + 0.5)
    return String(h // 100, ".", (h % 100) // 10, h % 10)


def main() raises:
    var root = getenv("ICEBERG_PLANNING_BENCH_ROOT", "build/planning-bench")
    var appends = Int(getenv("ICEBERG_PLANNING_APPENDS", "500"))
    var files = Int(getenv("ICEBERG_PLANNING_FILES", "4"))
    var reps = Int(getenv("ICEBERG_PLANNING_REPS", "5"))

    var io = FileIO.local()
    var have = True
    try:
        _ = io.read_all(String(root, "/built.txt"))
    except:
        have = False
    if not have:
        print(
            "== building the planning table (",
            appends,
            " appends x ",
            files,
            " files)",
            sep="",
        )
        var t0 = perf_counter_ns()
        build_table(root, appends, files)
        io.write_all(String(root, "/built.txt"), String("ok").as_bytes())
        print("   built in", (perf_counter_ns() - t0) // 1000000, "ms")

    var catalog = FilesystemCatalog.local(root)
    var table = catalog.load_table(String("db"), String("planning"))

    var manifests = len(
        read_manifest_list_io(
            io, table.metadata.current_snapshot().manifest_list
        )
    )
    var result = best_ms(table, reps)
    print(
        "plan_files over ",
        manifests,
        " manifests -> ",
        result[1],
        " file tasks: ",
        two_dp(result[0]),
        " ms (",
        Int(Float64(result[1]) / (result[0] / 1000.0)),
        " entries/s)",
        sep="",
    )
