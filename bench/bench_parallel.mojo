"""Multi-worker scans: `to_table` and `to_batches` at 1, 2, 4 and 8 workers.

File scan tasks are shared-nothing — each decodes, casts, applies its deletes
and evaluates its residual into its own arena — so a scan over many files is
the obvious place for a second core. `ScanOptions.num_workers` says how many:
1 (the default) reads on the calling thread, 0 means one per core.

The table is the one `tools/make_bench_table.py` builds with eight appends,
so eight data files of 250k rows each. Every run checks the row count, and
checks that the rows come back in the same order they do at one worker: the
merge is by task index, not by whichever worker finished first.

Read the numbers with the memory ceiling in mind. Decoding Parquet is mostly
moving bytes, and threads.mojo's own memcpy benchmark tops out near 4x on this
class of machine; so should this.
"""

from std.os import getenv
from std.time import perf_counter_ns

from iceberg.catalog.filesystem import Table
from iceberg.io import FileIO
from iceberg.read import ScanOptions, ScanResult
from threads import num_cpus


def read_text(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


def _pad(s: String, width: Int) -> String:
    var out = s
    while out.byte_length() < width:
        out += " "
    return out^


def _ms(ns: Int) -> String:
    var us = ns // 1000
    return String(us // 1000, ".", (us % 1000) // 100)


def fingerprint(r: ScanResult) raises -> String:
    """Enough of the result to notice a reordering: the first and last cell of
    every column, plus the row count."""
    var out = String(r.num_rows())
    var n = r.num_rows()
    for c in range(r.num_columns()):
        out += "|" + r.cell(0, c) + ".." + r.cell(n - 1, c)
    return out^


def main() raises:
    var root = getenv("ICEBERG_PARALLEL_BENCH_ROOT", "build/parallel-bench")
    var meta = read_text(root + "/metadata_location.txt").strip()
    print("bench table:", meta, "—", num_cpus(), "logical CPUs")
    print()

    var workers = [1, 2, 4, 8]
    var base = String("")
    var base_batches = 0
    print(
        _pad(String("workers"), 9),
        _pad(String("to_table"), 12),
        _pad(String("speedup"), 9),
        _pad(String("to_batches"), 12),
        _pad(String("speedup"), 9),
        "rows",
    )
    var t1_table = 0
    var t1_batches = 0
    for w in range(len(workers)):
        var options = ScanOptions()
        options.num_workers = workers[w]

        var best_table = 0
        var rows = 0
        var print_ = String("")
        for _ in range(3):
            var t0 = perf_counter_ns()
            var table = Table.load(String(meta), FileIO.local())
            var result = table.scan().to_table(options)
            var ns = perf_counter_ns() - t0
            rows = result.num_rows()
            print_ = fingerprint(result)
            if best_table == 0 or ns < best_table:
                best_table = ns

        var best_batches = 0
        var nb = 0
        for _ in range(3):
            var t0 = perf_counter_ns()
            var table = Table.load(String(meta), FileIO.local())
            var batches = table.scan().to_batches(options)
            var ns = perf_counter_ns() - t0
            nb = len(batches)
            if best_batches == 0 or ns < best_batches:
                best_batches = ns

        if base == "":
            base = print_
            base_batches = nb
            t1_table = best_table
            t1_batches = best_batches
        elif print_ != base:
            raise Error(
                "iceberg: "
                + String(workers[w])
                + " workers changed the rows or their order"
            )
        elif nb != base_batches:
            raise Error(
                "iceberg: "
                + String(workers[w])
                + " workers changed the batch count"
            )

        var sp_t = Float64(t1_table) / Float64(best_table)
        var sp_b = Float64(t1_batches) / Float64(best_batches)
        print(
            _pad(String(workers[w]), 9),
            _pad(_ms(best_table) + " ms", 12),
            _pad(String(Int(sp_t * 100) // 100, ".", Int(sp_t * 100) % 100), 9),
            _pad(_ms(best_batches) + " ms", 12),
            _pad(String(Int(sp_b * 100) // 100, ".", Int(sp_b * 100) % 100), 9),
            rows,
        )
