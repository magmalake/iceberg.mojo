#!/usr/bin/env python3
"""Build the nested benchmark table: 200k rows with a struct and a list.

Not checked in — `pixi run bench` regenerates it if it is missing. The shape
is the one a nested scan actually costs something on: a struct of two leaves
and a `list<string>` averaging two elements a row, so the Dremel assembly and
the offsets buffer are both in the measurement.

Usage: make_nested_bench_table.py <warehouse-dir> [rows]
"""
import os
import shutil
import sys

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    StructType,
)

root = os.path.abspath(sys.argv[1])
rows = int(sys.argv[2]) if len(sys.argv) > 2 else 200_000
if os.path.isdir(root):
    shutil.rmtree(root)
os.makedirs(os.path.join(root, "warehouse"))

catalog = SqlCatalog(
    "nbench",
    **{"uri": f"sqlite:///{root}/catalog.db", "warehouse": f"file://{root}/warehouse"},
)
catalog.create_namespace("db")
schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(
        2,
        "addr",
        StructType(
            NestedField(10, "city", StringType(), required=False),
            NestedField(11, "zip", IntegerType(), required=False),
        ),
        required=False,
    ),
    NestedField(
        3,
        "tags",
        ListType(element_id=20, element_type=StringType(), element_required=False),
        required=False,
    ),
)
t = catalog.create_table(
    "db.nbench", schema=schema, properties={"format-version": "2"}
)

CITIES = ["eu", "us", "apac", "latam", "emea"]
TAGS = ["alpha", "beta", "gamma", "delta", "epsilon"]
sa = t.schema().as_arrow()
chunk = 50_000
for start in range(0, rows, chunk):
    n = min(chunk, rows - start)
    ids = list(range(start, start + n))
    t.append(
        pa.table(
            {
                "id": pa.array(ids, type=sa.field("id").type),
                "addr": pa.array(
                    [
                        None if i % 13 == 0
                        else {"city": CITIES[i % 5], "zip": i % 100000}
                        for i in ids
                    ],
                    type=sa.field("addr").type,
                ),
                "tags": pa.array(
                    [
                        None if i % 17 == 0 else [TAGS[i % 5]] * (1 + i % 3)
                        for i in ids
                    ],
                    type=sa.field("tags").type,
                ),
            },
            schema=sa,
        )
    )

t = catalog.load_table("db.nbench")
meta = t.metadata_location.replace("file://", "")
with open(os.path.join(root, "metadata_location.txt"), "w") as fh:
    fh.write(meta + "\n")
print("nested bench table: %d rows, %d data files"
      % (rows, len(list(t.scan().plan_files()))))
print("metadata:", meta)
