#!/usr/bin/env python3
"""Build the benchmark table: one million rows, in build/bench-warehouse.

Not checked in — it is ~15 MB of Parquet — and not needed by the test suite.
`pixi run bench` regenerates it if it is missing.

Six columns of the same shapes the fixtures use, so the benchmark measures the
same decode paths the parity gates cover: two integers, a string, a double, a
timestamp and a boolean, with nulls in two of them.

Usage: make_bench_table.py <warehouse-dir> [rows]
"""
import datetime as dt
import os
import shutil
import sys

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
if os.path.isdir(root):
    shutil.rmtree(root)
os.makedirs(os.path.join(root, "warehouse"))

catalog = SqlCatalog(
    "bench",
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
    "db.bench", schema=schema, properties={"format-version": "2"}
)

REGIONS = ["eu", "us", "apac", "latam", "emea"]
base = dt.datetime(2024, 1, 1)
chunk = 250_000
for start in range(0, rows, chunk):
    n = min(chunk, rows - start)
    ids = list(range(start, start + n))
    t.append(
        pa.Table.from_pydict(
            {
                "id": ids,
                "region": [REGIONS[i % 5] for i in ids],
                "amount": [None if i % 7 == 0 else i * 0.5 for i in ids],
                "ts": [base + dt.timedelta(seconds=i % 86400) for i in ids],
                "ok": [None if i % 11 == 0 else (i % 3 == 0) for i in ids],
                "cnt": [i % 1000 for i in ids],
            },
            schema=t.schema().as_arrow(),
        )
    )

t = catalog.load_table("db.bench")
meta = t.metadata_location.replace("file://", "")
with open(os.path.join(root, "metadata_location.txt"), "w") as fh:
    fh.write(meta + "\n")
print("bench table: %d rows, %d data files" % (
    rows, len(list(t.scan().plan_files()))))
print("metadata:", meta)
