"""End-to-end scan benchmark: metadata, plan, decode, deletes, rows out.

Times whole scans over a one-million-row table built by
tools/make_bench_table.py, which is what `pixi run bench` regenerates into
`build/bench-warehouse`. tools/bench_pyiceberg.py runs the same four scans
through PyIceberg for comparison.

Two shapes are timed for each scan:

* `to_batches()` — the **Arrow fast path**. Columns come out of parquet.mojo
  as Arrow buffers, are cast, filtered and assembled by the columnar kernels
  in `iceberg.kernels`, and are handed back as `RecordBatch`es. This is the
  comparable number: PyIceberg's `to_arrow()` does the same thing.
* `to_table()` — the same rows concatenated into one `ScanResult`, which costs
  a copy of every buffer.

Nothing here is a micro-benchmark: every number includes reading the metadata,
planning the scan, and decoding Parquet.
"""

from std.os import getenv
from std.time import perf_counter_ns

from iceberg.catalog.filesystem import Table
from iceberg.io import FileIO
from iceberg.read import ScanOptions


def _pad(s: String, width: Int) -> String:
    var out = s
    while out.byte_length() < width:
        out += " "
    return out^


def read_text(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


def _report(label: String, rows: Int, ns: Int) raises:
    var seconds = Float64(ns) / 1.0e9
    var per_second = 0
    if seconds > 0:
        per_second = Int(Float64(rows) / seconds)
    print(
        _pad(label, 30),
        _pad(String(rows), 10),
        "rows",
        _pad(
            String(Int(seconds * 10000) // 10, ".", Int(seconds * 10000) % 10),
            7,
        ),
        "ms",
        _pad(String(per_second), 11),
        "rows/s",
    )


def timed(
    label: String,
    location: String,
    filter: String,
    columns: List[String],
    options: ScanOptions,
) raises -> Int:
    """Best of three, warm — the same shape `tools/bench_pyiceberg.py` uses.

    A single cold run measures the page cache and the allocator warming up as
    much as it measures the scan, and it measures them differently for the two
    implementations. Both sides take the best of three so the comparison is of
    the code.
    """
    var best = 0
    var n = 0
    for _ in range(3):
        var t0 = perf_counter_ns()
        var table = Table.load(location, FileIO.local())
        var scan = table.scan().filter(filter)
        if len(columns) > 0:
            scan = scan.select(columns.copy())
        var batches = scan.to_batches(options)
        var ns = perf_counter_ns() - t0
        n = 0
        for k in range(len(batches)):
            n += batches[k].num_rows
        if best == 0 or ns < best:
            best = ns
    _report(label + " (arrow)", n, best)

    var best2 = 0
    var rows = 0
    for _ in range(3):
        var t1 = perf_counter_ns()
        var table2 = Table.load(location, FileIO.local())
        var scan2 = table2.scan().filter(filter)
        if len(columns) > 0:
            scan2 = scan2.select(columns.copy())
        var result = scan2.to_table(options)
        var ns = perf_counter_ns() - t1
        rows = result.num_rows()
        if best2 == 0 or ns < best2:
            best2 = ns
    _report(label + " (to_table)", rows, best2)
    return n


def main() raises:
    var root = getenv("ICEBERG_BENCH_ROOT", "build/bench-warehouse")
    var meta = read_text(root + "/metadata_location.txt").strip()
    print("bench table:", meta)
    print()

    var eager = ScanOptions()
    var lazy = ScanOptions()
    lazy.lazy = True

    _ = timed(String("full scan"), String(meta), String('["true"]'), [], eager)
    _ = timed(
        String("projection id,region"),
        String(meta),
        String('["true"]'),
        [String("id"), String("region")],
        eager,
    )
    _ = timed(
        String("filter region=eu"),
        String(meta),
        String('["=","region","eu"]'),
        [],
        eager,
    )
    _ = timed(
        String("filter id>900000"),
        String(meta),
        String('[">","id",900000]'),
        [],
        eager,
    )
    _ = timed(
        String("full scan (lazy io)"),
        String(meta),
        String('["true"]'),
        [],
        lazy,
    )

    print()
    print("four workers (ScanOptions.num_workers = 4)")
    var par = ScanOptions()
    par.num_workers = 4
    _ = timed(String("full scan"), String(meta), String('["true"]'), [], par)
    _ = timed(
        String("projection id,region"),
        String(meta),
        String('["true"]'),
        [String("id"), String("region")],
        par,
    )
    _ = timed(
        String("filter region=eu"),
        String(meta),
        String('["=","region","eu"]'),
        [],
        par,
    )
    _ = timed(
        String("filter id>900000"),
        String(meta),
        String('[">","id",900000]'),
        [],
        par,
    )
