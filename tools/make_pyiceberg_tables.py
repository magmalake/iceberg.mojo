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

    # Schema evolution, one change per commit so the metadata carries a chain
    # of schemas (0 original, 1 +extra, 2 renamed, 3 promoted) rather than one
    # collapsed diff.
    with t.update_schema() as us:
        us.add_column("extra", StringType(), required=False)
    t = catalog.load_table("db.evolved")
    with t.update_schema() as us:
        us.rename_column("name", "label")
    t = catalog.load_table("db.evolved")
    with t.update_schema() as us:
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
#
# PyIceberg 0.11.1 cannot produce merge-on-read deletes: `Table.delete()` warns
# "Merge on read is not yet supported, falling back to copy-on-write", and both
# ManifestWriterV1.content() and ManifestWriterV2.content() are hard-wired to
# ManifestContent.DATA, so there is no delete-manifest writer at all.
#
# So the position-delete snapshot below is assembled from PyIceberg's own
# internals: a real position-delete Parquet file (file_path/pos with the
# reserved field ids 2147483546/2147483545), a ManifestWriterV2 subclass that
# reports ManifestContent.DELETES and stamps `content: deletes` into the Avro
# metadata, and a _FastAppendFiles subclass that writes the added files into
# that delete manifest and keeps the parent snapshot's data manifests.
# Everything else - manifest list, snapshot summary, metadata commit - is
# stock PyIceberg.

import uuid as _uuid

import pyarrow.parquet as pq
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
)
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.update.snapshot import _FastAppendFiles
from pyiceberg.typedef import Record

POS_DELETE_ARROW_SCHEMA = pa.schema(
    [
        pa.field(
            "file_path",
            pa.string(),
            nullable=False,
            metadata={b"PARQUET:field_id": b"2147483546"},
        ),
        pa.field(
            "pos",
            pa.int64(),
            nullable=False,
            metadata={b"PARQUET:field_id": b"2147483545"},
        ),
    ]
)


class _DeleteManifestWriterV2(ManifestWriterV2):
    """A v2 manifest writer whose content is DELETES rather than DATA."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self):
        meta = dict(super()._meta)
        meta["content"] = "deletes"
        return meta


class _AppendDeleteFiles(_FastAppendFiles):
    """Fast-append, but the added files go into a *delete* manifest."""

    def _manifests(self):
        tm = self._transaction.table_metadata
        out = []
        if self._added_data_files:
            with _DeleteManifestWriterV2(
                spec=tm.spec(),
                schema=tm.schema(),
                output_file=self.new_manifest_output(),
                snapshot_id=self._snapshot_id,
                avro_compression=self._compression,
            ) as writer:
                for df in self._added_data_files:
                    writer.add(
                        ManifestEntry.from_args(
                            status=ManifestEntryStatus.ADDED,
                            snapshot_id=self._snapshot_id,
                            sequence_number=None,
                            file_sequence_number=None,
                            data_file=df,
                        )
                    )
            out.append(writer.to_manifest_file())
        return out + self._existing_manifests()


def write_position_delete_file(t, deletes):
    """`deletes` is [(data_file_path, pos), ...] - sorted, spec order."""
    deletes = sorted(deletes)
    path = "%s/data/00000-0-position-deletes-%s.parquet" % (
        t.location().rstrip("/"),
        _uuid.uuid4(),
    )
    tbl = pa.Table.from_pydict(
        {
            "file_path": [d[0] for d in deletes],
            "pos": [d[1] for d in deletes],
        },
        schema=POS_DELETE_ARROW_SCHEMA,
    )
    out = t.io.new_output(path)
    with out.create(overwrite=True) as fh:
        pq.write_table(tbl, fh)
    size = len(t.io.new_input(path))
    return DataFile.from_args(
        _table_format_version=2,
        content=DataFileContent.POSITION_DELETES,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=len(deletes),
        file_size_in_bytes=size,
        spec_id=t.metadata.default_spec_id,
        sort_order_id=None,
        equality_ids=None,
        key_metadata=None,
        column_sizes={},
        value_counts={},
        null_value_counts={},
        nan_value_counts={},
        lower_bounds={},
        upper_bounds={},
        split_offsets=[],
    )


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
    t = catalog.load_table("db.deletes_v2")

    # Which rows to shadow: id=3 (file 1, position 2) and id=4 (file 2,
    # position 0).  One delete file referencing two data files.
    files = sorted(
        task.file.file_path for task in t.scan().plan_files()
    )
    if len(files) != 2:
        raise SystemExit("expected 2 data files, got %r" % (files,))
    by_first_id = {}
    for f in files:
        tbl = pq.read_table(f.replace("file://", ""))
        by_first_id[tbl.column("id")[0].as_py()] = f
    deletes = [(by_first_id[1], 2), (by_first_id[4], 0)]

    delete_file = write_position_delete_file(t, deletes)
    # Drive the snapshot producer directly: UpdateSnapshot has no hook for a
    # custom producer class.
    tx = t.transaction()
    producer = _AppendDeleteFiles(
        operation=Operation.DELETE,
        transaction=tx,
        io=t.io,
        snapshot_properties={},
    )
    producer.append_data_file(delete_file)
    updates, requirements = producer._commit()
    tx._apply(updates, requirements)
    tx.commit_transaction()
    t = catalog.load_table("db.deletes_v2")

    # Report exactly what came out.
    snap = t.current_snapshot()
    report = {
        "pyiceberg_delete_api": (
            "PyIceberg 0.11.1 Table.delete() falls back to copy-on-write "
            "(UserWarning: 'Merge on read is not yet supported, falling back "
            "to copy-on-write'); the position delete below was written with a "
            "ManifestWriterV2 subclass reporting ManifestContent.DELETES."
        ),
        "snapshots": len(t.metadata.snapshots),
        "format_version": t.metadata.format_version,
        "last_operation": snap.summary.operation.value,
        "last_summary": {k: v for k, v in snap.summary.additional_properties.items()},
        "delete_files": [],
        "data_files": [],
        "manifests": [],
    }
    io = t.io
    for mf in snap.manifests(io):
        report["manifests"].append(
            {
                "path": mf.manifest_path.rsplit("/", 1)[-1],
                "content": mf.content.name,
                "added_files_count": mf.added_files_count,
                "existing_files_count": mf.existing_files_count,
                "sequence_number": mf.sequence_number,
            }
        )
        for entry in mf.fetch_manifest_entry(io, discard_deleted=True):
            d = entry.data_file
            rec = {
                "manifest": mf.manifest_path.rsplit("/", 1)[-1],
                "manifest_content": mf.content.name,
                "path": d.file_path,
                "content": d.content.name,
                "format": d.file_format.name,
                "record_count": d.record_count,
                "file_size_in_bytes": d.file_size_in_bytes,
                "sequence_number": entry.sequence_number,
            }
            if d.content == DataFileContent.DATA:
                report["data_files"].append(rec)
            else:
                report["delete_files"].append(rec)
                dt_ = pq.read_table(d.file_path.replace("file://", ""))
                rec["rows"] = dt_.to_pylist()

    # Prove the delete is actually applied on read.
    report["rows_after_delete"] = sorted(
        r["id"] for r in t.scan().to_arrow().to_pylist()
    )
    report["plan_delete_files"] = [
        [df.file_path for df in task.delete_files] for task in t.scan().plan_files()
    ]
    print("deletes_v2 report:", json.dumps(report, indent=2, default=str))
    with open(os.path.join(OUT, "deletes_v2_report.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)
    return t


if __name__ == "__main__":
    print("pyiceberg", pyiceberg.__version__, "pyarrow", pa.__version__)
    make_evolved()
    make_deletes_v2()
    print("PYICEBERG FIXTURES OK")
