#!/usr/bin/env python3
"""PyIceberg's numbers for the nested scans, so the README can be honest.

Best of three, warm, which is what bench/bench_nested.mojo does.

Usage: bench_pyiceberg_nested.py <metadata.json>
"""
import json
import sys
import time

from pyiceberg.expressions import EqualTo, GreaterThan
from pyiceberg.table import StaticTable

meta = sys.argv[1]
table = StaticTable.from_metadata(meta)

cases = [
    ("full scan (struct+list)", None, None),
    ("projection addr.city,id", None, ["addr.city", "id"]),
    ("filter addr.city=eu", EqualTo("addr.city", "eu"), None),
    ("filter addr.zip>90000", GreaterThan("addr.zip", 90000), None),
]

out = {}
for name, expr, cols in cases:
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
    print("%-26s %9d rows  %7.3f s  %10d rows/s"
          % (name, arrow.num_rows, dt, out[name]["rows_per_second"]))
print(json.dumps(out))
