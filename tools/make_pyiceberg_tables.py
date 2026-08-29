#!/usr/bin/env python3
"""Create the two fixture tables the Rust bridge cannot make.

  * ``evolved``    — schema evolution: add a column, rename a column, promote
                     an int column to long, with an append before and after.
  * ``deletes_v2`` — a format-version-2 table with merge-on-read delete modes,
                     so ``table.delete(...)`` leaves a *position delete* file
                     behind instead of rewriting the data files.

Both are written into the same sqlite JDBC catalog and the same warehouse the
Rust bridge used, so the fixture set is one coherent warehouse.

Env: FIXTURE_ROOT (warehouse root dir), FIXTURE_OUT (oracle output root).
"""

import datetime as dt
import json
import os
import sys

import pyarrow as pa
import pyiceberg
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

ROOT = os.environ["FIXTURE_ROOT"]
OUT = os.environ["FIXTURE_OUT"]

catalog = SqlCatalog(
    "sql",
    **{
        "uri": f"sqlite:///{ROOT}/catalog.db",
        "warehouse": f"file://{ROOT}/warehouse",
    },
)
try:
    catalog.create_namespace("db")
except Exception:
    pass

TS = [
    dt.datetime(2023, 11, 14, 0, 0, 0),
    dt.datetime(2023, 11, 15, 0, 0, 0),
    dt.datetime(2023, 11, 16, 12, 0, 0),
    dt.datetime(2023, 11, 17, 0, 0, 0),
    dt.datetime(2023, 12, 1, 0, 0, 0),
    dt.datetime(2024, 1, 1, 0, 0, 0),
    dt.datetime(2023, 11, 14, 0, 0, 0),
]


def drop(name):
    try:
        catalog.drop_table(f"db.{name}")
    except Exception:
        pass


def arrow(tbl, rows):
    """Build a pyarrow table matching the *current* iceberg schema."""
    schema = tbl.schema().as_arrow()
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    return pa.Table.from_pydict(cols, schema=schema)


# ---------------------------------------------------------------- evolved ---
def make_evolved():
    drop("evolved")
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "cnt", IntegerType(), required=False),
        NestedField(4, "amount", DoubleType(), required=False),
    )
    t = catalog.create_table(
        "db.evolved", schema=schema, properties={"format-version": "2"}
    )

    # snapshot 1 — original schema
    t.append(
        arrow(
            t,
            [
                {"id": 1, "name": "alpha", "cnt": 10, "amount": 1.5},
                {"id": 2, "name": "beta", "cnt": 20, "amount": None},
                {"id": 3, "name": "gamma", "cnt": 30, "amount": 3.5},
            ],
        )
    )

    # schema evolution: add "extra", rename name -> label, promote cnt to long
    with t.update_schema() as us:
        us.add_column("extra", StringType(), required=False)
        us.rename_column("name", "label")
        us.update_column("cnt", LongType())
    t = catalog.load_table("db.evolved")

    # snapshot 2 — evolved schema
    t.append(
        arrow(
            t,
            [
                {
                    "id": 4,
                    "label": "delta",
                    "cnt": 40,
                    "amount": 4.5,
                    "extra": "x4",
                },
                {
                    "id": 5,
                    "label": "epsilon",
                    "cnt": 5000000000,
                    "amount": None,
                    "extra": None,
                },
            ],
        )
    )

    # snapshot 3
    t.append(
        arrow(
            t,
            [
                {
                    "id": 6,
                    "label": "zeta",
                    "cnt": 60,
                    "amount": 6.5,
                    "extra": "x6",
                }
            ],
        )
    )
    t = catalog.load_table("db.evolved")
    print(
        "evolved: schemas=%d snapshots=%d"
        % (len(t.metadata.schemas), len(t.metadata.snapshots))
    )
    return t


# ------------------------------------------------------------- deletes_v2 ---
def make_deletes_v2():
    drop("deletes_v2")
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "region", StringType(), required=True),
        NestedField(3, "amount", DoubleType(), required=False),
        NestedField(4, "ts", TimestampType(), required=False),
        NestedField(5, "ok", BooleanType(), required=False),
    )
    t = catalog.create_table(
        "db.deletes_v2",
        schema=schema,
        properties={
            "format-version": "2",
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
            "write.merge.mode": "merge-on-read",
        },
    )
    rows1 = [
        {"id": 1, "region": "eu", "amount": 1.5, "ts": TS[0], "ok": True},
        {"id": 2, "region": "us", "amount": None, "ts": TS[1], "ok": False},
        {"id": 3, "region": "eu", "amount": 3.5, "ts": TS[2], "ok": True},
    ]
    rows2 = [
        {"id": 4, "region": "us", "amount": 4.5, "ts": TS[3], "ok": False},
        {"id": 5, "region": "apac", "amount": 5.5, "ts": TS[4], "ok": True},
    ]
    t.append(arrow(t, rows1))
    t.append(arrow(t, rows2))

    # The delete that should become a positional delete file.
    t.delete(EqualTo("id", 3))
    t = catalog.load_table("db.deletes_v2")

    # Report exactly what came out.
    snap = t.current_snapshot()
    report = {
        "snapshots": len(t.metadata.snapshots),
        "last_operation": snap.summary.operation.value
        if snap and snap.summary
        else None,
        "last_summary": dict(snap.summary) if snap and snap.summary else {},
        "delete_files": [],
        "data_files": [],
    }
    from pyiceberg.manifest import ManifestContent

    io = t.io
    for mf in snap.manifests(io):
        for entry in mf.fetch_manifest_entry(io, discard_deleted=True):
            d = entry.data_file
            rec = {
                "manifest": mf.manifest_path.rsplit("/", 1)[-1],
                "manifest_content": mf.content.name,
                "path": d.file_path,
                "content": d.content.name,
                "format": d.file_format.name,
                "record_count": d.record_count,
            }
            if d.content.name == "DATA":
                report["data_files"].append(rec)
            else:
                report["delete_files"].append(rec)
    print("deletes_v2 report:", json.dumps(report, indent=2, default=str))
    with open(os.path.join(OUT, "deletes_v2_report.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)
    return t


if __name__ == "__main__":
    print("pyiceberg", pyiceberg.__version__, "pyarrow", pa.__version__)
    make_evolved()
    make_deletes_v2()
    print("PYICEBERG FIXTURES OK")
