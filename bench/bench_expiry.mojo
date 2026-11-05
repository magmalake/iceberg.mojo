"""Snapshot expiry and commit cost on a table with a long history.

Builds a table the shape of an hourly ingest — one commit per "hour", one
manifest per commit, and every `--replace-every` commits an overwrite that
replaces one earlier "day" (24 hours' files) the way a rerun does — then
expires it down to the last `--keep` snapshots, and reports what each step
cost in object-store requests (`FileIO.counts()`) and in time.

The data files are entries only: one real Parquet file is written and its
`DataFile` is registered under a new path for every commit. Neither a commit
nor an expiry reads data files, so the metadata is exactly what a real table
of that history would have, without writing one.

    bench-expiry <warehouse-dir> <snapshots> [--keep N] [--replace-every N]
                 [--build-only] [--expire-only]

Uses only the API iceberg-mojo has had since expiry existed, so the same file
measures an older build too.
"""

from std.collections import Dict
from std.os import makedirs
from std.sys import argv
from std.time import perf_counter_ns

from parquet import RecordBatch

from iceberg.append import prepare_append
from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.commit import FileChanges, prepare_commit
from iceberg.io import FileIO
from iceberg.manifest import DataFile
from iceberg.schema import Schema
from iceberg.transforms import PartitionSpec
from iceberg.values import Datum
from iceberg.write import WriteOptions, write_data_files


comptime SCHEMA_JSON = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"}]}'
)


def _ms(t0: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / 1e6


def _median(var xs: List[Float64]) -> Float64:
    if len(xs) == 0:
        return 0.0
    for i in range(1, len(xs)):
        var j = i
        while j > 0 and xs[j] < xs[j - 1]:
            xs.swap_elements(j, j - 1)
            j -= 1
    return xs[len(xs) // 2]


def _template(table: Table) raises -> DataFile:
    var schema = Schema.parse(SCHEMA_JSON)
    var ids = ColumnBuilder.of(schema, 1)
    ids.add(Datum.long_(Int64(1)))
    var batches = List[RecordBatch]()
    batches.append(batch_of([ids^]))
    var files = write_data_files(
        table.io,
        table.metadata.location,
        batches,
        schema,
        PartitionSpec.unpartitioned(),
        0,
        WriteOptions.from_properties(table.metadata.properties),
    )
    return files[0].copy()


def _hour_file(t: DataFile, location: String, hour: Int, gen: Int) -> DataFile:
    var f = t.copy()
    f.file_path = (
        location + "/data/h" + String(hour) + "-g" + String(gen) + ".parquet"
    )
    return f^


def build(root: String, snapshots: Int, replace_every: Int) raises -> Table:
    makedirs(root, exist_ok=True)
    var catalog = FilesystemCatalog.local(root)
    var schema = Schema.parse(SCHEMA_JSON)
    var table = catalog.create_table(
        String("db"), String("events"), schema, PartitionSpec.unpartitioned()
    )
    var t = _template(table)
    var loc = table.metadata.location
    var gen = Dict[Int, Int]()  # hour -> its files' current generation
    var commit_ms = List[Float64]()
    var hours = 0
    print(
        "commits,median_commit_ms,last_commit_requests,last_commit_lists,metadata_kb"
    )
    for i in range(snapshots):
        var before = table.io.counts()
        var t0 = perf_counter_ns()
        if (
            replace_every > 0
            and i > 0
            and i % replace_every == 0
            and hours >= 48
        ):
            # Rerun an earlier day: replace its 24 hours' files.
            var day = (hours // 24) // 2
            var changes = FileChanges()
            for h in range(day * 24, day * 24 + 24):
                var g = gen.get(h, 0)
                changes.removed.append(_hour_file(t, loc, h, g).file_path)
                changes.added_data.append(_hour_file(t, loc, h, g + 1))
                gen[h] = g + 1
            var res = prepare_commit(
                table.io,
                table.metadata,
                String("overwrite"),
                changes^,
                Dict[String, String](),
            )
            table.commit(res)
        else:
            var files = List[DataFile]()
            files.append(_hour_file(t, loc, hours, 0))
            var res = prepare_append(
                table.io, table.metadata, files^, Dict[String, String]()
            )
            table.commit(res)
            hours += 1
        commit_ms.append(_ms(t0))
        if (i + 1) % 250 == 0 or i + 1 == snapshots:
            var after = table.io.counts()
            print(
                String(i + 1)
                + ","
                + String(_median(commit_ms.copy()))
                + ","
                + String(after.requests() - before.requests())
                + ","
                + String(after.lists - before.lists)
                + ","
                + String(table.metadata.to_json().byte_length() // 1024)
            )
            commit_ms = List[Float64]()
    return table^


def main() raises:
    var args = argv()
    if len(args) < 3:
        print(
            "usage: bench-expiry <warehouse-dir> <snapshots> [--keep N]"
            " [--replace-every N] [--build-only] [--expire-only]"
        )
        return
    var root = String(args[1])
    var snapshots = Int(String(args[2]))
    var keep = 168
    var replace_every = 200
    var build_only = False
    var expire_only = False
    var k = 3
    while k < len(args):
        var a = String(args[k])
        if a == "--keep":
            keep = Int(String(args[k + 1]))
            k += 1
        elif a == "--replace-every":
            replace_every = Int(String(args[k + 1]))
            k += 1
        elif a == "--build-only":
            build_only = True
        elif a == "--expire-only":
            expire_only = True
        k += 1

    var table: Table
    if expire_only:
        table = FilesystemCatalog.local(root).load_table(
            String("db"), String("events")
        )
    else:
        var t0 = perf_counter_ns()
        table = build(root, snapshots, replace_every)
        print(
            "built",
            len(table.metadata.snapshots),
            "snapshots in",
            _ms(t0),
            "ms",
        )
    if build_only:
        return

    var rows_before = table.scan().plan_files()
    table.io.reset_counts()
    var t1 = perf_counter_ns()
    var done = table.expire_snapshots(-1, keep, False)
    var took = _ms(t1)
    var io = table.io.counts()
    print(
        "expired",
        len(done.expired),
        "kept",
        len(done.retained),
        "deleted files",
        len(done.deleted_files),
        "manifests+lists",
        len(done.deleted_manifests),
    )
    print("expiry ms", took)
    print("expiry io", io)
    table.refresh()
    var rows_after = table.scan().plan_files()
    print(
        "live files before",
        len(rows_before),
        "after",
        len(rows_after),
        "snapshots now",
        len(table.metadata.snapshots),
    )
    if len(rows_before) != len(rows_after):
        raise Error("expiry changed the current snapshot's files")

    # What a commit costs once the history is bounded again.
    var t = _template(table)
    var after_ms = List[Float64]()
    for i in range(50):
        var files = List[DataFile]()
        files.append(_hour_file(t, table.metadata.location, 1_000_000 + i, 0))
        var t2 = perf_counter_ns()
        var res = prepare_append(
            table.io, table.metadata, files^, Dict[String, String]()
        )
        table.commit(res)
        after_ms.append(_ms(t2))
    print("median commit ms after expiry", _median(after_ms^))
