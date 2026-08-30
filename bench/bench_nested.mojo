"""Scan benchmark for nested columns: a struct and a list, 200k rows.

The same four shapes `bench_scan.mojo` times on flat columns, over a table
built by tools/make_nested_bench_table.py — `id long`, `addr struct<city
string, zip int>` and `tags list<string>` averaging two elements a row.
tools/bench_pyiceberg_nested.py runs the same four through PyIceberg.

The one that is not in the flat benchmark is the **sub-field projection**:
`select(["addr.city", "id"])` prunes the Parquet read to the leaves under
`addr.city`, so `addr.zip` and the whole `tags` column are never decoded. It
should be markedly faster than the full scan, and that gap is the point of
the feature.
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
        _pad(label, 32),
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
) raises -> Int:
    """Best of three, warm — the same shape the flat benchmark uses."""
    var options = ScanOptions()
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
    var root = getenv("ICEBERG_NESTED_BENCH_ROOT", "build/nested-bench")
    var meta = read_text(root + "/metadata_location.txt").strip()
    print("nested bench table:", meta)
    print()

    _ = timed(
        String("full scan (struct+list)"), String(meta), String('["true"]'), []
    )
    _ = timed(
        String("projection addr.city,id"),
        String(meta),
        String('["true"]'),
        [String("addr.city"), String("id")],
    )
    _ = timed(
        String("filter addr.city=eu"),
        String(meta),
        String('["=","addr.city","eu"]'),
        [],
    )
    _ = timed(
        String("filter addr.zip>90000"),
        String(meta),
        String('[">","addr.zip",90000]'),
        [],
    )
