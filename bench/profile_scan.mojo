"""Per-stage scan profile. `pixi run profile`.

Splits a full scan of the one-million-row bench table into the stages that
actually do work, so an optimisation can be aimed at the stage that dominates
rather than guessed at. The stages mirror what `TableScan.to_table` does:

| stage | what it covers |
|---|---|
| load | `Table.load` — find the metadata file, parse the JSON |
| plan | `plan_files` — manifest list, manifests, entries, residuals |
| io | `FileIO.read_all` per data file |
| open | `ParquetReader` construction: the Thrift footer and the schema |
| decode | `read_table()` — parquet's own decode, nothing of Iceberg's |
| read | `read_data_file` — decode plus cast, deletes, residual, assembly |
| concat | `ScanResult.append` of every batch into one result |
| total | `Table.load` .. `to_table`, the number the bench prints |

`read` minus `decode` is what the Iceberg layer costs on top of the decode it
wraps; `decode` is measured at the same `batch_size` the scan uses, so batch
chopping shows up where it happens.
"""

from std.os import getenv
from std.time import perf_counter_ns

from parquet import ParquetReader
from parquet.ext_full import AllCodecs

from iceberg.catalog.filesystem import Table
from iceberg.io import FileIO
from iceberg.read import ScanOptions, read_data_file
from iceberg.transforms import PartitionSpec


def read_text(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


def _us(ns: Int) -> String:
    var us = ns // 1000
    return String(us // 1000, ".", (us % 1000) // 100, (us % 100) // 10)


def _row(name: StringSlice, ns: Int, total: Int) -> String:
    var pct = 0
    if total > 0:
        pct = (ns * 1000) // total
    var pad = String()
    for _ in range(10 - name.byte_length()):
        pad += " "
    var ms = _us(ns)
    var mpad = String()
    for _ in range(8 - ms.byte_length()):
        mpad += " "
    return String(
        "  ", name, pad, mpad, ms, " ms   ", pct // 10, ".", pct % 10, " %"
    )


def profile(
    meta: String,
    label: String,
    filter: String,
    columns: List[String],
    options: ScanOptions,
) raises:
    # One untimed pass first: the page cache and the allocator warming up are
    # not what this is measuring, and they land entirely on `io` and `decode`.
    var warm = Table.load(meta, FileIO.local())
    var warm_scan = warm.scan().filter(filter)
    if len(columns) > 0:
        warm_scan = warm_scan.select(columns.copy())
    _ = warm_scan.to_batches(options)

    var t_total = perf_counter_ns()

    var t0 = perf_counter_ns()
    var table = Table.load(meta, FileIO.local())
    var load_ns = perf_counter_ns() - t0

    var scan = table.scan().filter(filter)
    if len(columns) > 0:
        scan = scan.select(columns.copy())
    var t1 = perf_counter_ns()
    var tasks = scan.plan_files()
    var plan_ns = perf_counter_ns() - t1

    var schema = scan.current_schema()
    var split = scan._split_selection()
    var ids = split[0].copy()
    var meta_columns = split[1].copy()
    var mapping = scan.name_mapping()

    var io_ns = 0
    var open_ns = 0
    var decode_ns = 0
    var read_ns = 0
    var concat_ns = 0
    var rows = 0
    var batches = 0

    # The stages parquet owns, timed on the same files with the same options.
    for k in range(len(tasks)):
        ref t = tasks[k]
        var t2 = perf_counter_ns()
        var bytes = table.io.read_all(t.data_file.file_path)
        io_ns += perf_counter_ns() - t2
        var t3 = perf_counter_ns()
        var reader = ParquetReader[AllCodecs](bytes^)
        reader.batch_size = options.batch_size
        reader.verify_crc = False
        open_ns += perf_counter_ns() - t3
        var t4 = perf_counter_ns()
        var tbl = reader.read_table()
        decode_ns += perf_counter_ns() - t4
        batches += len(tbl.batches)

    # `read_data_file`, the whole Iceberg read of the same files.
    from iceberg.read import ScanResult

    var out = ScanResult()
    for k in range(len(tasks)):
        ref t = tasks[k]
        var spec = PartitionSpec.unpartitioned(t.spec_id)
        if table.metadata.has_spec(t.spec_id):
            spec = table.metadata.spec_by_id(t.spec_id)
        var t5 = perf_counter_ns()
        var parts = read_data_file(
            table.io,
            t.data_file,
            t.delete_files,
            t.data_sequence_number,
            spec,
            schema,
            ids,
            meta_columns,
            mapping,
            t.residual,
            True,
            options,
        )
        read_ns += perf_counter_ns() - t5
        var t6 = perf_counter_ns()
        for j in range(len(parts)):
            out.append(parts[j])
        concat_ns += perf_counter_ns() - t6
    rows = out.num_rows()

    var total_ns = perf_counter_ns() - t_total

    print()
    print(
        label,
        "— batch_size",
        options.batch_size,
        "—",
        len(tasks),
        "file(s),",
        batches,
        "parquet batches,",
        rows,
        "rows",
    )
    print(_row("load", load_ns, total_ns))
    print(_row("plan", plan_ns, total_ns))
    print(_row("io", io_ns, total_ns))
    print(_row("open", open_ns, total_ns))
    print(_row("decode", decode_ns, total_ns))
    print(_row("read", read_ns, total_ns))
    print(_row("  iceberg", read_ns - decode_ns, total_ns))
    print(_row("concat", concat_ns, total_ns))
    print(_row("total", total_ns, total_ns))


def main() raises:
    var root = getenv("ICEBERG_BENCH_ROOT", "build/bench-warehouse")
    var meta = read_text(root + "/metadata_location.txt").strip()
    print("bench table:", meta)

    var eager = ScanOptions()
    var lazy = ScanOptions()
    lazy.lazy = True
    var chopped = ScanOptions()
    chopped.batch_size = 8192

    profile(String(meta), String("full scan"), String('["true"]'), [], eager)
    profile(
        String(meta),
        String("full scan, 8192-row batches"),
        String('["true"]'),
        [],
        chopped,
    )
    profile(
        String(meta),
        String("projection id,region"),
        String('["true"]'),
        [String("id"), String("region")],
        eager,
    )
    profile(
        String(meta),
        String("filter region=eu"),
        String('["=","region","eu"]'),
        [],
        eager,
    )
    profile(
        String(meta),
        String("filter id>900000"),
        String('[">","id",900000]'),
        [],
        eager,
    )
    profile(
        String(meta), String("full scan, lazy io"), String('["true"]'), [], lazy
    )
