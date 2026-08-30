"""Scan planning: which data files a query must read, and which deletes apply.

`TableScan` walks the metadata tree exactly once per plan:

    metadata -> snapshot -> manifest list -> manifests -> manifest entries

pruning at every level with the strongest evidence available at that level:

1. **Manifest list.** A delete manifest is collected separately; a data
   manifest is dropped when `ManifestEvaluator` proves that no partition it
   summarises can match. The manifest's *own* partition spec is used, not the
   table's current one, because that is what its partition tuples were written
   with.
2. **Manifest entry.** DELETED entries are skipped outright. A live entry is
   dropped when its partition tuple fails the inclusive projection, or when
   `InclusiveMetricsEvaluator` proves its column bounds cannot match.
3. **Delete association.** Every surviving data file is paired with the delete
   files that apply to it under the spec's scope rules — the sequence-number
   comparisons differ between position and equality deletes, and a deletion
   vector supersedes any position delete file for the same data file.

Reading the data files themselves is out of scope until parquet.mojo exists;
`plan_files` stops at the task list, which is exactly what `ib_scan_plan_files_json`
returns, so the two can be diffed directly.
"""

from .expressions import (
    Expr,
    InclusiveMetricsEvaluator,
    ManifestEvaluator,
    ResidualEvaluator,
    bind,
    parse_filter,
    rewrite_not,
)
from .io import FileIO, basename
from .json import json_quote
from .manifest import (
    CONTENT_DATA,
    CONTENT_EQUALITY_DELETES,
    CONTENT_POSITION_DELETES,
    DataFile,
    Manifest,
    ManifestEntry,
    ManifestFile,
    MANIFEST_CONTENT_DELETES,
    STATUS_DELETED,
    read_manifest_io,
    read_manifest_list_io,
)
from .metadata import Snapshot, TableMetadata
from .schema import Schema
from .transforms import PartitionSpec
from .values import Datum, compare


@fieldwise_init
struct FileScanTask(Copyable, Movable, Writable):
    """One data file to read, with the deletes that apply and what is left of
    the filter after partitioning has been accounted for."""

    var data_file: DataFile
    var delete_files: List[DataFile]
    var residual: String
    """The residual filter, in the DSL. `["true"]` when nothing is left."""
    var start: Int64
    var length: Int64
    var spec_id: Int

    def write_to(self, mut writer: Some[Writer]):
        writer.write(
            "FileScanTask(",
            self.data_file.file_path,
            ", deletes=",
            len(self.delete_files),
            ")",
        )


@fieldwise_init
struct _PendingDelete(Copyable, Movable):
    """A delete file plus the sequence number and spec it was written with."""

    var file: DataFile
    var sequence_number: Int64
    var spec_id: Int
    var spec_unpartitioned: Bool


struct TableScan(Copyable, Movable):
    """A configured, not-yet-executed scan. Every setter returns a new scan."""

    var metadata: TableMetadata
    var io: FileIO
    var snapshot_id: Int64
    var has_snapshot: Bool
    var filter_dsl: String
    var selected: List[String]
    var case_sensitive: Bool

    def __init__(out self, var metadata: TableMetadata, var io: FileIO):
        self.metadata = metadata^
        self.io = io^
        self.snapshot_id = 0
        self.has_snapshot = False
        self.filter_dsl = '["true"]'
        self.selected = []
        self.case_sensitive = True

    @staticmethod
    def of(var metadata: TableMetadata) -> Self:
        return Self(metadata^, FileIO.local())

    # ── configuration ──────────────────────────────────────────────────────
    def use_snapshot(self, id: Int64) raises -> Self:
        var s = self.copy()
        _ = self.metadata.snapshot_by_id(id)
        s.snapshot_id = id
        s.has_snapshot = True
        return s^

    def use_ref(self, name: String) raises -> Self:
        var snap = self.metadata.snapshot_for_ref(name)
        return self.use_snapshot(snap.snapshot_id)

    def as_of(self, timestamp_ms: Int64) raises -> Self:
        var snap = self.metadata.snapshot_as_of(timestamp_ms)
        return self.use_snapshot(snap.snapshot_id)

    def filter(self, dsl: String) -> Self:
        var s = self.copy()
        s.filter_dsl = dsl
        return s^

    def select(self, var columns: List[String]) -> Self:
        var s = self.copy()
        s.selected = columns^
        return s^

    def with_io(self, var io: FileIO) -> Self:
        var s = self.copy()
        s.io = io^
        return s^

    def case_insensitive(self) -> Self:
        var s = self.copy()
        s.case_sensitive = False
        return s^

    # ── resolution ─────────────────────────────────────────────────────────
    def snapshot(self) raises -> Snapshot:
        if self.has_snapshot:
            return self.metadata.snapshot_by_id(self.snapshot_id)
        return self.metadata.current_snapshot()

    def schema(self) raises -> Schema:
        var s = self.snapshot()
        var full = self.metadata.schema_for_snapshot(s)
        if len(self.selected) == 0:
            return full^
        var ids = List[Int]()
        for k in range(len(self.selected)):
            ids.append(full.find_by_name(self.selected[k]).id)
        return full.select(ids)

    def projected_field_ids(self) raises -> List[Int]:
        var s = self.snapshot()
        var full = self.metadata.schema_for_snapshot(s)
        var out = List[Int]()
        if len(self.selected) == 0:
            var cols = full.columns()
            for k in range(len(cols)):
                out.append(cols[k].id)
            return out^
        for k in range(len(self.selected)):
            out.append(full.find_by_name(self.selected[k]).id)
        return out^

    # ── planning ───────────────────────────────────────────────────────────
    def plan_files(self) raises -> List[FileScanTask]:
        var snap = self.snapshot()
        var schema = self.metadata.schema_for_snapshot(snap)
        var row_filter = bind(
            parse_filter(self.filter_dsl), schema, self.case_sensitive
        )
        var metrics_eval = InclusiveMetricsEvaluator(row_filter, schema)

        if snap.manifest_list == "":
            # v1 allowed an inline manifest list; nothing else is supported.
            if len(snap.manifests) == 0:
                return List[FileScanTask]()
            return self._plan_from_manifests(
                snap, snap.manifests.copy(), schema, row_filter, metrics_eval
            )
        var manifests = read_manifest_list_io(self.io, snap.manifest_list)
        return self._plan(snap, manifests, schema, row_filter, metrics_eval)

    def _plan_from_manifests(
        self,
        snap: Snapshot,
        paths: List[String],
        schema: Schema,
        row_filter: Expr,
        metrics_eval: InclusiveMetricsEvaluator,
    ) raises -> List[FileScanTask]:
        """v1 tables that inline manifest paths carry no manifest-list summary,
        so every manifest is opened and nothing can be pruned at that level."""
        var mfs = List[ManifestFile]()
        for k in range(len(paths)):
            var mf = ManifestFile(
                paths[k],
                0,
                self.metadata.default_spec_id,
                0,
                0,
                0,
                snap.snapshot_id,
                0,
                False,
                0,
                False,
                0,
                False,
                0,
                False,
                0,
                False,
                0,
                False,
                [],
                False,
                [],
                0,
                False,
            )
            mfs.append(mf^)
        return self._plan(snap, mfs, schema, row_filter, metrics_eval)

    def _plan(
        self,
        snap: Snapshot,
        manifests: List[ManifestFile],
        schema: Schema,
        row_filter: Expr,
        metrics_eval: InclusiveMetricsEvaluator,
    ) raises -> List[FileScanTask]:
        # ── pass 1: every live delete file in the snapshot ─────────────────
        var deletes = List[_PendingDelete]()
        for k in range(len(manifests)):
            ref mf = manifests[k]
            if not mf.is_delete_manifest():
                continue
            var m = read_manifest_io(self.io, mf.manifest_path, mf)
            for j in range(len(m.entries)):
                ref e = m.entries[j]
                if not e.is_live():
                    continue
                if e.data_file.is_data():
                    continue
                deletes.append(
                    _PendingDelete(
                        e.data_file.copy(),
                        e.sequence_number,
                        m.partition_spec_id,
                        m.partition_spec.is_unpartitioned(),
                    )
                )

        # ── pass 2: data manifests, pruned by their partition summaries ────
        var tasks = List[FileScanTask]()
        var project = self.projected_field_ids()
        for k in range(len(manifests)):
            ref mf = manifests[k]
            if mf.is_delete_manifest():
                continue
            var spec = PartitionSpec.unpartitioned(mf.partition_spec_id)
            if self.metadata.has_spec(mf.partition_spec_id):
                spec = self.metadata.spec_by_id(mf.partition_spec_id)
            if mf.has_partitions and len(mf.partitions) > 0:
                var me = ManifestEvaluator(row_filter, spec, schema)
                if not me.eval(mf.partitions):
                    continue
            var m = read_manifest_io(self.io, mf.manifest_path, mf)
            # The manifest's own spec is authoritative for its tuples.
            var residuals = ResidualEvaluator(
                row_filter, m.partition_spec, schema
            )
            for j in range(len(m.entries)):
                ref e = m.entries[j]
                if not e.is_live():
                    continue
                if not e.data_file.is_data():
                    continue
                if not residuals.selects(e.data_file.partition):
                    continue
                if not metrics_eval.eval(
                    e.data_file.record_count, e.data_file.metrics
                ):
                    continue
                var res = residuals.residual_for(e.data_file.partition)
                var applicable = _deletes_for(
                    e.data_file, e.sequence_number, m.partition_spec_id, deletes
                )
                tasks.append(
                    FileScanTask(
                        e.data_file.copy(),
                        applicable^,
                        res.text(res.root),
                        0,
                        e.data_file.file_size_in_bytes,
                        m.partition_spec_id,
                    )
                )
        return tasks^

    # ── output ─────────────────────────────────────────────────────────────
    def plan_files_json(self) raises -> String:
        """The plan in the same shape `ib_scan_plan_files_json` emits."""
        var tasks = self.plan_files()
        var project = self.projected_field_ids()
        var out = String("[")
        for k in range(len(tasks)):
            if k > 0:
                out += ","
            ref t = tasks[k]
            out += '{"data-file-path":' + json_quote(t.data_file.file_path)
            out += ',"deletes":['
            for j in range(len(t.delete_files)):
                if j > 0:
                    out += ","
                out += json_quote(t.delete_files[j].file_path)
            out += "]"
            out += ',"file-format":' + json_quote(
                t.data_file.file_format.lower()
            )
            out += ',"file-size-in-bytes":' + String(
                t.data_file.file_size_in_bytes
            )
            out += ',"length":' + String(t.length)
            out += ',"project-field-ids":['
            for j in range(len(project)):
                if j > 0:
                    out += ","
                out += String(project[j])
            out += "]"
            out += ',"record-count":' + String(t.data_file.record_count)
            out += ',"start":' + String(t.start)
            out += "}"
        out += "]"
        return out^


def _deletes_for(
    data_file: DataFile,
    data_seq: Int64,
    data_spec_id: Int,
    deletes: List[_PendingDelete],
) raises -> List[DataFile]:
    """The delete files that apply to one data file, per the spec's scope rules.

    Position deletes (and deletion vectors) apply at `data_seq <= delete_seq`,
    so a delete committed alongside the data it removes still applies.
    Equality deletes need `data_seq < delete_seq` — they never remove rows from
    their own commit — but apply globally when written with an unpartitioned
    spec.
    """
    var out = List[DataFile]()
    # A deletion vector for this file supersedes every position delete file.
    var dv = -1
    for k in range(len(deletes)):
        ref d = deletes[k]
        if not d.file.is_deletion_vector():
            continue
        if d.file.referenced_data_file != data_file.file_path:
            continue
        if data_seq > d.sequence_number:
            continue
        if not _same_partition(data_file, data_spec_id, d):
            continue
        dv = k
        break
    if dv >= 0:
        out.append(deletes[dv].file.copy())

    for k in range(len(deletes)):
        ref d = deletes[k]
        if k == dv:
            continue
        if d.file.is_position_delete():
            if dv >= 0:
                # The vector already contains every position delete for this
                # file, so the older files must not be applied on top.
                continue
            if d.file.is_deletion_vector():
                continue
            if (
                d.file.has_referenced_data_file
                and d.file.referenced_data_file != data_file.file_path
            ):
                continue
            if data_seq > d.sequence_number:
                continue
            if not _same_partition(data_file, data_spec_id, d):
                continue
            out.append(d.file.copy())
        elif d.file.is_equality_delete():
            if data_seq >= d.sequence_number:
                continue
            if not d.spec_unpartitioned and not _same_partition(
                data_file, data_spec_id, d
            ):
                continue
            out.append(d.file.copy())
    return out^


def _same_partition(
    data_file: DataFile, data_spec_id: Int, d: _PendingDelete
) raises -> Bool:
    """Partition equality: same spec id and equal partition values.

    Unknown transforms are ignored for *filtering* but their values still count
    for equality, which is why this compares the tuples directly rather than
    re-deriving them.
    """
    if data_spec_id != d.spec_id:
        return False
    if len(data_file.partition) != len(d.file.partition):
        return False
    for k in range(len(data_file.partition)):
        ref a = data_file.partition[k]
        ref b = d.file.partition[k]
        if a.valid != b.valid:
            return False
        if a.valid and compare(a, b) != 0:
            return False
    return True
