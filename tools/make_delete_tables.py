#!/usr/bin/env python3
"""Create the two delete fixtures PyIceberg's public API cannot produce.

  * ``eq_deletes_v2`` — a v2 table with **equality delete files**, including
    one whose delete column value is NULL (the spec's "a null value in a
    delete column matches a row if the row's value is null").
  * ``dv_v3``        — a **format-version 3** table with a **deletion vector**
    in a Puffin file, tracked by a v3 delete manifest with
    ``referenced_data_file`` / ``content_offset`` / ``content_size_in_bytes``.

Neither is reachable from PyIceberg 0.11.1's public API:

  * ``Table.delete()`` falls back to copy-on-write ("Merge on read is not yet
    supported"), and there is no equality-delete writer at all;
  * ``write_manifest``/``write_manifest_list`` raise for version 3, and
    ``TableMetadataV3.model_dump_json`` raises ``NotImplementedError:
    Writing V3 is not yet supported`` (apache/iceberg-python#1551).

*Reading* all of it is supported, though — ``TableMetadataV3`` parses, the v3
Avro structs are all present in ``pyiceberg.manifest``, and
``pyiceberg.table.puffin.PuffinFile`` decodes a deletion vector. So the files
below are assembled from PyIceberg's own structs and then read back with
PyIceberg as an independent check, with DuckDB's iceberg extension as a second.

Both tables are added to the SAME sqlite catalog and warehouse the rest of the
fixtures live in; nothing existing is touched.

Env: FIXTURE_ROOT (warehouse root dir), FIXTURE_OUT (oracle output root).
"""

from __future__ import annotations

import binascii
import datetime as dt
import json
import os
import struct
import sqlite3
import uuid as _uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pyiceberg
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DATA_FILE_TYPE,
    MANIFEST_ENTRY_SCHEMAS,
    MANIFEST_LIST_FILE_SCHEMAS,
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    ManifestWriter,
    ManifestWriterV2,
    ManifestListWriter,
)
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.update.snapshot import _FastAppendFiles
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)
from pyroaring import BitMap

ROOT = os.environ["FIXTURE_ROOT"]
OUT = os.environ["FIXTURE_OUT"]
WAREHOUSE = f"{ROOT}/warehouse"

catalog = SqlCatalog(
    "sql",
    **{"uri": f"sqlite:///{ROOT}/catalog.db", "warehouse": f"file://{WAREHOUSE}"},
)
try:
    catalog.create_namespace("db")
except Exception:
    pass

TS = [
    dt.datetime(2023, 11, 14),
    dt.datetime(2023, 11, 15),
    dt.datetime(2023, 11, 16, 12),
    dt.datetime(2023, 11, 17),
    dt.datetime(2023, 12, 1),
    dt.datetime(2024, 1, 1),
]

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "region", StringType(), required=True),
    NestedField(3, "amount", DoubleType(), required=False),
    NestedField(4, "ts", TimestampType(), required=False),
    NestedField(5, "ok", BooleanType(), required=False),
)

ROWS_1 = [
    {"id": 1, "region": "eu", "amount": 1.5, "ts": TS[0], "ok": True},
    {"id": 2, "region": "us", "amount": None, "ts": TS[1], "ok": False},
    {"id": 3, "region": "eu", "amount": 3.5, "ts": TS[2], "ok": True},
]
ROWS_2 = [
    {"id": 4, "region": "us", "amount": 4.5, "ts": TS[3], "ok": False},
    {"id": 5, "region": "apac", "amount": None, "ts": TS[4], "ok": True},
    {"id": 6, "region": "apac", "amount": 6.5, "ts": TS[5], "ok": None},
]


def drop(name):
    try:
        catalog.drop_table(f"db.{name}")
    except Exception:
        pass


def arrow(tbl, rows):
    schema = tbl.schema().as_arrow()
    return pa.Table.from_pydict(
        {f.name: [r.get(f.name) for r in rows] for f in schema}, schema=schema
    )


def local(path):
    return path.replace("file://", "")


# ══ equality deletes ════════════════════════════════════════════════════════
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


def write_equality_delete_file(t, name, arrow_schema, columns, equality_ids):
    """One equality delete file: any row equal on `equality_ids` is deleted."""
    path = "%s/data/00000-0-equality-deletes-%s-%s.parquet" % (
        t.location().rstrip("/"),
        name,
        _uuid.uuid4(),
    )
    tbl = pa.Table.from_pydict(columns, schema=arrow_schema)
    out = t.io.new_output(path)
    with out.create(overwrite=True) as fh:
        pq.write_table(tbl, fh)
    return DataFile.from_args(
        _table_format_version=2,
        content=DataFileContent.EQUALITY_DELETES,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=tbl.num_rows,
        file_size_in_bytes=len(t.io.new_input(path)),
        spec_id=t.metadata.default_spec_id,
        sort_order_id=None,
        equality_ids=list(equality_ids),
        key_metadata=None,
        column_sizes={},
        value_counts={},
        null_value_counts={},
        nan_value_counts={},
        lower_bounds={},
        upper_bounds={},
        split_offsets=[],
    )


def make_eq_deletes_v2():
    drop("eq_deletes_v2")
    t = catalog.create_table(
        "db.eq_deletes_v2",
        schema=SCHEMA,
        properties={
            "format-version": "2",
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
            "write.merge.mode": "merge-on-read",
        },
    )
    t.append(arrow(t, ROWS_1))
    t.append(arrow(t, ROWS_2))
    t = catalog.load_table("db.eq_deletes_v2")

    # Two delete files in one commit, exercising both halves of the rule:
    #   * equality on `id` (a required column) removes ids 2 and 6;
    #   * equality on `amount` with a NULL value removes every row whose
    #     amount IS NULL — ids 2 and 5 — which is the spec's "a null value in
    #     a delete column matches a row if the row's value is null".
    # Row 2 is hit by both, which is legal and must not double-count.
    id_schema = pa.schema(
        [
            pa.field(
                "id", pa.int64(), nullable=False,
                metadata={b"PARQUET:field_id": b"1"},
            )
        ]
    )
    amount_schema = pa.schema(
        [
            pa.field(
                "amount", pa.float64(), nullable=True,
                metadata={b"PARQUET:field_id": b"3"},
            )
        ]
    )
    d1 = write_equality_delete_file(
        t, "id", id_schema, {"id": [2, 6]}, [1]
    )
    d2 = write_equality_delete_file(
        t, "amount", amount_schema, {"amount": [None]}, [3]
    )

    tx = t.transaction()
    producer = _AppendDeleteFiles(
        operation=Operation.DELETE, transaction=tx, io=t.io,
        snapshot_properties={},
    )
    producer.append_data_file(d1)
    producer.append_data_file(d2)
    updates, requirements = producer._commit()
    tx._apply(updates, requirements)
    tx.commit_transaction()
    t = catalog.load_table("db.eq_deletes_v2")
    print(
        "eq_deletes_v2: snapshots=%d format=%d"
        % (len(t.metadata.snapshots), t.metadata.format_version)
    )
    return t


# ══ deletion vector, format version 3 ═══════════════════════════════════════
DV_MAGIC = b"\xd1\xd3\x39\x64"
PUFFIN_MAGIC = b"PFA1"


def serialize_dv(positions):
    """The Iceberg `deletion-vector-v1` blob for a set of row positions."""
    by_key = {}
    for p in positions:
        by_key.setdefault(p >> 32, BitMap()).add(p & 0xFFFFFFFF)
    vector = struct.pack("<q", len(by_key))
    for key in sorted(by_key):
        vector += struct.pack("<I", key) + by_key[key].serialize()
    body = DV_MAGIC + vector
    return (
        struct.pack(">i", len(body))
        + body
        + struct.pack(">I", binascii.crc32(body) & 0xFFFFFFFF)
    )


def write_puffin(io, path, blobs):
    """`blobs` is [(referenced_data_file, [positions])]; returns metadata."""
    payload = PUFFIN_MAGIC
    metas = []
    for referenced, positions in blobs:
        blob = serialize_dv(positions)
        metas.append(
            {
                "type": "deletion-vector-v1",
                "fields": [],
                "snapshot-id": -1,
                "sequence-number": -1,
                "offset": len(payload),
                "length": len(blob),
                "properties": {
                    "referenced-data-file": referenced,
                    "cardinality": str(len(positions)),
                },
            }
        )
        payload += blob
    footer = json.dumps(
        {
            "blobs": metas,
            "properties": {"created-by": "iceberg.mojo fixture generator"},
        }
    ).encode("utf-8")
    payload += (
        PUFFIN_MAGIC
        + footer
        + struct.pack("<i", len(footer))
        + struct.pack("<i", 0)  # flags: footer not compressed
        + PUFFIN_MAGIC
    )
    out = io.new_output(path)
    with out.create(overwrite=True) as fh:
        fh.write(payload)
    return metas, len(payload)



def _v3_new_writer(self):
    """PyIceberg's `ManifestWriter.new_writer` builds the in-memory record
    schema from `DEFAULT_READ_VERSION` (2) while writing the file with the
    writer's own version. For a v3 manifest that silently drops every v3-only
    column — `first_row_id`, `referenced_data_file`, `content_offset`,
    `content_size_in_bytes` all come out null. Both schemas must be v3."""
    from pyiceberg.avro.file import AvroOutputFile

    return AvroOutputFile[ManifestEntry](
        output_file=self._output_file,
        file_schema=self._with_partition(3),
        record_schema=self._with_partition(3),
        schema_name="manifest_entry",
        metadata=self._meta,
    )


class ManifestWriterV3Data(ManifestWriter):
    """A v3 DATA manifest writer, for rewriting the pre-upgrade manifests.

    On the first snapshot after an upgrade to v3 the spec requires row IDs to
    be assigned to *existing* data files as well as added ones, and requires
    the inherited `first_row_id` to be written into the file metadata of
    EXISTING entries. The v2 manifests PyIceberg wrote have no such field at
    all, so they are rewritten here.
    """

    def content(self) -> ManifestContent:
        return ManifestContent.DATA

    @property
    def version(self):
        return 3

    @property
    def _meta(self):
        return {**super()._meta, "content": "data"}

    new_writer = _v3_new_writer

    def prepare_entry(self, entry):
        return entry


class ManifestWriterV3(ManifestWriter):
    """The v3 manifest writer PyIceberg 0.11.1 refuses to build.

    Identical to its V2 in everything but the version number and the record
    struct, both of which come from `pyiceberg.manifest`'s own v3 tables.
    """

    def __init__(self, spec, schema, output_file, snapshot_id, avro_compression):
        super().__init__(spec, schema, output_file, snapshot_id, avro_compression)

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def version(self):
        return 3

    @property
    def _meta(self):
        return {**super()._meta, "content": "deletes"}

    new_writer = _v3_new_writer

    def prepare_entry(self, entry):
        return entry


class ManifestListWriterV3(ManifestListWriter):
    """The v3 manifest-list writer PyIceberg 0.11.1 refuses to build."""

    def __init__(
        self, output_file, snapshot_id, parent_snapshot_id, sequence_number,
        avro_compression, first_row_ids,
    ):
        super().__init__(
            format_version=3,
            output_file=output_file,
            meta={
                "snapshot-id": str(snapshot_id),
                "parent-snapshot-id": str(parent_snapshot_id),
                "sequence-number": str(sequence_number),
                "format-version": "3",
            },
        )
        self._commit_snapshot_id = snapshot_id
        self._sequence_number = sequence_number
        self._compression = avro_compression
        # {manifest_path: first_row_id}. The spec requires every *data*
        # manifest in a post-upgrade snapshot to carry one, and requires it to
        # be null for delete manifests.
        self._first_row_ids = first_row_ids

    def prepare_manifest(self, manifest_file):
        wrapped = ManifestFile.from_args(
            _table_format_version=3,
            **{
                field.name: getattr(manifest_file, field.name, None)
                for field in MANIFEST_LIST_FILE_SCHEMAS[3].fields
                if field.name != "first_row_id"
            },
        )
        if wrapped.added_snapshot_id is None:
            wrapped.added_snapshot_id = self._commit_snapshot_id
        if wrapped.sequence_number == -1:
            wrapped.sequence_number = self._sequence_number
        if wrapped.min_sequence_number == -1:
            wrapped.min_sequence_number = self._sequence_number
        wrapped.first_row_id = self._first_row_ids.get(wrapped.manifest_path)
        return wrapped


def make_dv_v3():
    drop("dv_v3")
    # Two ordinary v2 append snapshots first: PyIceberg writes them, so the
    # data files and their manifests are stock output.
    t = catalog.create_table(
        "db.dv_v3", schema=SCHEMA, properties={"format-version": "2"}
    )
    t.append(arrow(t, ROWS_1))
    t.append(arrow(t, ROWS_2))
    t = catalog.load_table("db.dv_v3")

    parent = t.current_snapshot()
    io = t.io
    data_files = []
    for mf in parent.manifests(io):
        for entry in mf.fetch_manifest_entry(io, discard_deleted=True):
            if entry.data_file.content == DataFileContent.DATA:
                data_files.append(entry.data_file)
    data_files.sort(key=lambda d: d.file_path)

    # Delete one row from each data file, addressed by position: the row with
    # id=3 (last of the first file) and the row with id=4 (first of the
    # second). Positions are read back from the Parquet so the fixture cannot
    # drift from what was actually written.
    plan = []
    for d in data_files:
        ids = pq.read_table(local(d.file_path)).column("id").to_pylist()
        for wanted in (3, 4):
            if wanted in ids:
                plan.append((d, [ids.index(wanted)]))
    if len(plan) != 2:
        raise SystemExit("expected one deleted row in each of 2 files")

    puffin_path = "%s/data/00000-0-deletion-vectors-%s.puffin" % (
        t.location().rstrip("/"), _uuid.uuid4(),
    )
    metas, puffin_size = write_puffin(
        io, puffin_path, [(d.file_path, pos) for d, pos in plan]
    )

    snapshot_id = _uuid.uuid4().int >> 96
    sequence_number = parent.sequence_number + 1

    dv_files = []
    for (d, positions), meta in zip(plan, metas):
        dv_files.append(
            DataFile.from_args(
                _table_format_version=3,
                content=DataFileContent.POSITION_DELETES,
                file_path=puffin_path,
                file_format=FileFormat.PUFFIN,
                partition=Record(),
                record_count=len(positions),
                file_size_in_bytes=puffin_size,
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
                referenced_data_file=d.file_path,
                content_offset=meta["offset"],
                content_size_in_bytes=meta["length"],
                first_row_id=None,
            )
        )

    base = t.location().rstrip("/")
    delete_manifest_path = "%s/metadata/%s-m0.avro" % (base, _uuid.uuid4())
    with ManifestWriterV3(
        spec=t.metadata.spec(),
        schema=t.metadata.schema(),
        output_file=io.new_output(delete_manifest_path),
        snapshot_id=snapshot_id,
        avro_compression="null",
    ) as writer:
        for df in dv_files:
            writer.add(
                ManifestEntry.from_args(
                    _table_format_version=3,
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=snapshot_id,
                    sequence_number=None,
                    file_sequence_number=None,
                    data_file=df,
                )
            )
    delete_manifest = writer.to_manifest_file()

    manifest_list_path = "%s/metadata/snap-%d-0-%s.avro" % (
        base, snapshot_id, _uuid.uuid4(),
    )
    # Row lineage. This is the first snapshot after the (synthetic) upgrade to
    # v3, so the table's next-row-id is still 0: the snapshot's first-row-id is
    # 0 and every *data* manifest is assigned one, in list order, advancing by
    # that manifest's added + existing row counts. Delete manifests get null.
    parent_manifests = sorted(
        parent.manifests(io), key=lambda m: m.manifest_path
    )
    first_row_ids = {}
    next_row_id = 0
    rewritten = []
    for mf in parent_manifests:
        if mf.content != ManifestContent.DATA:
            rewritten.append(mf)
            continue
        manifest_first_row_id = next_row_id
        path = "%s/metadata/%s-m0.avro" % (base, _uuid.uuid4())
        row_id = manifest_first_row_id
        with ManifestWriterV3Data(
            spec=t.metadata.spec(),
            schema=t.metadata.schema(),
            output_file=io.new_output(path),
            snapshot_id=snapshot_id,
            avro_compression="null",
        ) as dw:
            for entry in mf.fetch_manifest_entry(io, discard_deleted=True):
                d = entry.data_file
                fields = {
                    f.name: getattr(d, f.name, None)
                    for f in DATA_FILE_TYPE[3].fields
                }
                fields["first_row_id"] = row_id
                row_id += d.record_count
                dw.add_entry(
                    ManifestEntry.from_args(
                        _table_format_version=3,
                        status=ManifestEntryStatus.EXISTING,
                        snapshot_id=entry.snapshot_id,
                        sequence_number=entry.sequence_number,
                        file_sequence_number=entry.file_sequence_number,
                        data_file=DataFile.from_args(
                            _table_format_version=3, **fields
                        ),
                    )
                )
        new_mf = dw.to_manifest_file()
        first_row_ids[new_mf.manifest_path] = manifest_first_row_id
        next_row_id = row_id
        rewritten.append(new_mf)
    parent_manifests = rewritten
    with ManifestListWriterV3(
        output_file=io.new_output(manifest_list_path),
        snapshot_id=snapshot_id,
        parent_snapshot_id=parent.snapshot_id,
        sequence_number=sequence_number,
        avro_compression="null",
        first_row_ids=first_row_ids,
    ) as lw:
        lw.add_manifests([delete_manifest] + parent_manifests)

    # ── the v3 metadata file, written by hand ──────────────────────────────
    meta_path = t.metadata_location.replace("file://", "")
    with open(meta_path) as fh:
        doc = json.load(fh)
    doc["format-version"] = 3
    doc["next-row-id"] = next_row_id
    doc["last-sequence-number"] = sequence_number
    now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
    doc["last-updated-ms"] = now_ms
    snapshot = {
        "snapshot-id": snapshot_id,
        "parent-snapshot-id": parent.snapshot_id,
        "sequence-number": sequence_number,
        "timestamp-ms": now_ms,
        "manifest-list": manifest_list_path,
        "schema-id": t.metadata.current_schema_id,
        "first-row-id": 0,
        "summary": {
            "operation": "delete",
            "added-delete-files": str(len(dv_files)),
            "added-dvs": str(len(dv_files)),
            "added-position-deletes": str(sum(len(p) for _, p in plan)),
            "total-delete-files": str(len(dv_files)),
            "total-position-deletes": str(sum(len(p) for _, p in plan)),
        },
    }
    doc["snapshots"].append(snapshot)
    doc["current-snapshot-id"] = snapshot_id
    doc.setdefault("snapshot-log", []).append(
        {"snapshot-id": snapshot_id, "timestamp-ms": now_ms}
    )
    doc["refs"] = {"main": {"snapshot-id": snapshot_id, "type": "branch"}}

    version = 1 + max(
        int(os.path.basename(p).split("-", 1)[0])
        for p in os.listdir(os.path.join(base.replace("file://", ""), "metadata"))
        if p.endswith(".metadata.json")
    )
    new_meta = "%s/metadata/%05d-%s.metadata.json" % (base, version, _uuid.uuid4())
    doc["metadata-log"] = doc.get("metadata-log", []) + [
        {"metadata-file": t.metadata_location, "timestamp-ms": doc["last-updated-ms"]}
    ]
    out = io.new_output(new_meta)
    with out.create(overwrite=True) as fh:
        fh.write(json.dumps(doc, indent=2).encode("utf-8"))

    # Point the catalog at it, so every reader that goes through the catalog
    # (including tools/oracle_pyiceberg.py) sees the v3 snapshot.
    conn = sqlite3.connect(f"{ROOT}/catalog.db")
    conn.execute(
        "UPDATE iceberg_tables SET metadata_location = ?, "
        "previous_metadata_location = ? WHERE table_name = 'dv_v3'",
        (new_meta, t.metadata_location),
    )
    conn.commit()
    conn.close()

    t = catalog.load_table("db.dv_v3")
    print(
        "dv_v3: format=%d snapshots=%d dv blobs=%d next-row-id=%d"
        % (
            t.metadata.format_version,
            len(t.metadata.snapshots),
            len(dv_files),
            next_row_id,
        )
    )
    return t, puffin_path, plan, first_row_ids


def verify(t_eq, t_dv, puffin_path, plan):
    """Read both tables back with PyIceberg, and record exactly what it did."""
    report = {
        "pyiceberg_version": pyiceberg.__version__,
        "notes": {
            "eq_deletes": (
                "PyIceberg 0.11.1 has no equality-delete writer and, on read, "
                "ignores equality deletes entirely (pyarrow._task_to_record_"
                "batches applies only positional deletes). Its row output is "
                "therefore recorded as UNFILTERED, and DuckDB is the oracle "
                "for the equality-delete gate."
            ),
            "dv": (
                "PyIceberg 0.11.1 cannot write v3 metadata "
                "(TableMetadataV3.model_dump_json raises NotImplementedError) "
                "but reads it, and pyiceberg.table.puffin.PuffinFile decodes "
                "deletion-vector-v1, so its row output IS the oracle here."
            ),
        },
    }
    # PyIceberg 0.11.1 refuses to *plan* a scan of a table with equality
    # deletes at all — `_plan_files_local` raises "PyIceberg does not yet
    # support equality deletes" — so the delete files are enumerated straight
    # from the manifests instead, and the row oracle is DuckDB's.
    eq = {
        "format_version": t_eq.metadata.format_version,
        "snapshots": len(t_eq.metadata.snapshots),
        "delete_files": [],
        "data_files": [],
    }
    snap = t_eq.current_snapshot()
    for mf in snap.manifests(t_eq.io):
        for entry in mf.fetch_manifest_entry(t_eq.io, discard_deleted=True):
            d = entry.data_file
            rec = {
                "path": d.file_path.rsplit("/", 1)[-1],
                "content": d.content.name,
                "record_count": d.record_count,
                "sequence_number": entry.sequence_number,
            }
            if d.content == DataFileContent.DATA:
                eq["data_files"].append(rec)
            else:
                rec["equality_ids"] = list(d.equality_ids or [])
                rec["rows"] = pq.read_table(local(d.file_path)).to_pylist()
                eq["delete_files"].append(rec)
    try:
        _ = t_eq.scan().plan_files()
        eq["pyiceberg_plan"] = "succeeded"
    except ValueError as e:
        eq["pyiceberg_plan"] = "refused: %s" % e
    report["eq_deletes_v2"] = eq
    report["dv_v3"] = {
        "format_version": t_dv.metadata.format_version,
        "snapshots": len(t_dv.metadata.snapshots),
        "puffin_file": puffin_path.rsplit("/", 1)[-1],
        "deleted": [
            {"file": d.file_path.rsplit("/", 1)[-1], "positions": pos}
            for d, pos in plan
        ],
        "pyiceberg_rows_after_dv": sorted(
            r["id"] for r in t_dv.scan().to_arrow().to_pylist()
        ),
        "manifest_first_row_ids": {
            k.rsplit("/", 1)[-1]: v for k, v in first_row_ids.items()
        },
        "next_row_id": t_dv.metadata.model_dump().get("next_row_id"),
    }
    with open(os.path.join(OUT, "delete_tables_report.json"), "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    print("pyiceberg", pyiceberg.__version__, "pyarrow", pa.__version__)
    t_eq = make_eq_deletes_v2()
    t_dv, puffin_path, plan, first_row_ids = make_dv_v3()
    verify(t_eq, t_dv, puffin_path, plan)
    print("DELETE FIXTURES OK")
