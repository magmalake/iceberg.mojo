#!/usr/bin/env python3
"""The same append benchmark, through PyIceberg 0.11.1.

A million rows into a fresh SQL-catalog table, in the same chunks, with the
same schema and the same values. Building the Arrow table is *outside* the
measurement on both sides; the commit is inside.

Usage:  bench_pyiceberg_append.py <root> [rows] [chunk]
"""
import datetime as dt
import os
import shutil
import sys
import time

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

root = os.path.abspath(sys.argv[1])
rows = int(sys.argv[2]) if len(sys.argv) > 2 else 1_000_000
chunk = int(sys.argv[3]) if len(sys.argv) > 3 else 250_000
if os.path.isdir(root):
    shutil.rmtree(root)
os.makedirs(os.path.join(root, "warehouse"))

catalog = SqlCatalog(
    "appendbench",
    **{"uri": f"sqlite:///{root}/catalog.db", "warehouse": f"file://{root}/warehouse"},
)
catalog.create_namespace("db")
schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "region", StringType(), required=True),
    NestedField(3, "amount", DoubleType(), required=False),
    NestedField(4, "ts", TimestampType(), required=False),
    NestedField(5, "ok", BooleanType(), required=False),
    NestedField(6, "cnt", LongType(), required=False),
)
t = catalog.create_table(
    "db.bench",
    schema=schema,
    properties={"format-version": "2", "write.parquet.compression-codec": "zstd"},
)

REGIONS = ["eu", "us", "apac", "latam", "emea"]
BASE = dt.datetime(2024, 1, 1)
arrow_schema = t.schema().as_arrow()
chunks = []
for start in range(0, rows, chunk):
    n = min(chunk, rows - start)
    ids = list(range(start, start + n))
    chunks.append(
        pa.Table.from_pydict(
            {
                "id": ids,
                "region": [REGIONS[i % 5] for i in ids],
                "amount": [None if i % 7 == 0 else i * 0.5 for i in ids],
                "ts": [BASE + dt.timedelta(seconds=i % 86400) for i in ids],
                "ok": [None if i % 11 == 0 else (i % 3 == 0) for i in ids],
                "cnt": [i % 1000 for i in ids],
            },
            schema=arrow_schema,
        )
    )

t0 = time.perf_counter()
for c in chunks:
    t.append(c)
elapsed = time.perf_counter() - t0

t = catalog.load_table("db.bench")
files = list(t.scan().plan_files())
size = sum(f.file.file_size_in_bytes for f in files)
print(
    "PyIceberg append: %d rows in %d ms — %d rows/s, %d MB/s of Parquet, "
    "%d snapshots, %d files, %d MB"
    % (
        rows,
        elapsed * 1000,
        rows / elapsed,
        size / elapsed / 1048576,
        len(t.metadata.snapshots),
        len(files),
        size / 1048576,
    )
)
