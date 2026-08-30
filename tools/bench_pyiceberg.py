#!/usr/bin/env python3
"""PyIceberg's numbers for the same scans, so the README can be honest.

Best of three, warm, which is what bench/bench_scan.mojo does: a single cold
run measures the page cache and the interpreter warming up as much as it
measures the scan. PyIceberg's `to_arrow()` hands each file to pyarrow, which
uses its own thread pool — so this is one PyIceberg *process*, not one core.

Usage: bench_pyiceberg.py <metadata.json>
"""
import json
import sys
import time

from pyiceberg.expressions import EqualTo, GreaterThan
from pyiceberg.table import StaticTable

meta = sys.argv[1]
table = StaticTable.from_metadata(meta)

cases = [
    ("full scan", None, None),
    ("projection id,region", None, ["id", "region"]),
    ("filter region=eu", EqualTo("region", "eu"), None),
    ("filter id>900000", GreaterThan("id", 900000), None),
]

out = {}
for name, expr, cols in cases:
    scan = table.scan()
    if expr is not None:
        scan = table.scan(row_filter=expr)
    if cols:
        scan = scan.select(*cols)
    dt = None
    for _ in range(3):
        scan = table.scan(row_filter=expr) if expr is not None else table.scan()
        if cols:
            scan = scan.select(*cols)
        t0 = time.perf_counter()
        arrow = scan.to_arrow()
        one = time.perf_counter() - t0
        if dt is None or one < dt:
            dt = one
    out[name] = {
        "rows": arrow.num_rows,
        "seconds": round(dt, 4),
        "rows_per_second": int(arrow.num_rows / dt) if dt > 0 else 0,
    }
    print("%-24s %9d rows  %7.3f s  %10d rows/s"
          % (name, arrow.num_rows, dt, out[name]["rows_per_second"]))
print(json.dumps(out))
