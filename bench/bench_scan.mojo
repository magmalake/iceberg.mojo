"""End-to-end scan benchmark: metadata, plan, decode, deletes, rows out.

Times whole `to_table()` calls over a one-million-row table built by
tools/make_bench_table.py, which is what `pixi run bench` regenerates into
`build/bench-warehouse`. tools/bench_pyiceberg.py runs the same four scans
through PyIceberg for comparison.

Nothing here is a micro-benchmark: every number includes reading the
metadata, planning the scan, decoding Parquet and materialising values.
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


def timed(
    label: String,
    location: String,
    filter: String,
    columns: List[String],
    options: ScanOptions,
) raises -> Int:
    var t0 = perf_counter_ns()
    var table = Table.load(location, FileIO.local())
    var scan = table.scan().filter(filter)
    if len(columns) > 0:
        scan = scan.select(columns.copy())
    var rows = scan.to_table(options)
    var ns = perf_counter_ns() - t0
    var seconds = Float64(ns) / 1.0e9
    var per_second = 0
    if seconds > 0:
        per_second = Int(Float64(rows.num_rows()) / seconds)
    print(
        _pad(label, 24),
        _pad(String(rows.num_rows()), 10),
        "rows",
        _pad(String(Int(seconds * 1000)), 7),
        "ms",
        _pad(String(per_second), 11),
        "rows/s",
    )
    return rows.num_rows()


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
