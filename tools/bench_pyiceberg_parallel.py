#!/usr/bin/env python3
"""PyIceberg's number for the multi-file scan, single process.

PyIceberg has no worker knob of its own: `to_arrow()` hands each file to
pyarrow, which uses its own thread pool. That is the point of the comparison —
what one PyIceberg process does with the machine, against what iceberg.mojo
does at 1, 2, 4 and 8 workers.

Usage: bench_pyiceberg_parallel.py <metadata.json>
"""
import json
import sys
import time

from pyiceberg.table import StaticTable

meta = sys.argv[1]
table = StaticTable.from_metadata(meta)

best = None
rows = 0
for _ in range(3):
    t0 = time.perf_counter()
    arrow = table.scan().to_arrow()
    dt = time.perf_counter() - t0
    rows = arrow.num_rows
    if best is None or dt < best:
        best = dt
print("%-24s %9d rows  %7.3f s  %10d rows/s"
      % ("full scan (to_arrow)", rows, best, int(rows / best)))
print(json.dumps({"rows": rows, "seconds": round(best, 4)}))
