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

from threads import OpaquePtr, num_cpus, opaque_ptr, parallel_for

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
from parquet import RecordBatch

from .read import (
    NameMapping,
    NAME_MAPPING_PROPERTY,
    ScanOptions,
    ScanResult,
    empty_scan_result,
    is_metadata_column,
    read_data_file,
)
from .json import json_quote
from .nested import concat_tree
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
from .values import compare


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
    var data_sequence_number: Int64
    """The data file's sequence number after inheritance — what the delete
    scope rules compare against, and what `_last_updated_sequence_number`
    reports for rows the file has not had rewritten."""

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


struct _FileScanCtx(Movable):
    """Everything a parallel file-scan task reads, and where it writes.

    `parallel_for` hands a task one `void *` and nothing else, so every input
    a `read_data_file` call needs is laid out here once and every task indexes
    it. The outputs are pre-sized to one slot per task, and a task only ever
    writes its own slot — the tasks share nothing, not even an arena, which is
    what makes this safe without a lock and deterministic without a sort.
    """

    var io: FileIO
    var tasks: List[FileScanTask]
    var specs: List[PartitionSpec]
    var schema: Schema
    var ids: List[Int]
    var meta_columns: List[String]
    var mapping: NameMapping
    var options: ScanOptions
    var case_sensitive: Bool
    var out: List[List[ScanResult]]
    var errors: List[String]

    def __init__(
        out self,
        var io: FileIO,
        var tasks: List[FileScanTask],
        var specs: List[PartitionSpec],
        var schema: Schema,
        var ids: List[Int],
        var meta_columns: List[String],
        var mapping: NameMapping,
        var options: ScanOptions,
        case_sensitive: Bool,
    ):
        var n = len(tasks)
        self.io = io^
        self.tasks = tasks^
        self.specs = specs^
        self.schema = schema^
        self.ids = ids^
        self.meta_columns = meta_columns^
        self.mapping = mapping^
        self.options = options^
        self.case_sensitive = case_sensitive
        self.out = List[List[ScanResult]]()
        self.errors = List[String]()
        for _ in range(n):
            self.out.append(List[ScanResult]())
            self.errors.append(String(""))

    def take_out(mut self) -> List[List[ScanResult]]:
        """The results, moved out; the context keeps an empty list in place."""
        var taken = self.out^
        self.out = List[List[ScanResult]]()
        return taken^

    def __init__(out self, *, deinit move: Self):
        self.io = move.io^
        self.tasks = move.tasks^
        self.specs = move.specs^
        self.schema = move.schema^
        self.ids = move.ids^
        self.meta_columns = move.meta_columns^
        self.mapping = move.mapping^
        self.options = move.options^
        self.case_sensitive = move.case_sensitive
        self.out = move.out^
        self.errors = move.errors^


def _drain_into(
    var parts: List[ScanResult], mut out: List[RecordBatch]
) raises -> Int:
    """Move every non-empty part into `out`, in order. Returns the rows moved.
    """
    var n = len(parts)
    var rev = List[ScanResult]()
    for _ in range(n):
        rev.append(parts.pop())
    var seen = 0
    for _ in range(n):
        var part = rev.pop()
        if part.num_rows() == 0:
            continue
        seen += part.num_rows()
        out.append(part^.take_batch())
    return seen


struct _ConcatCtx(Movable):
    """One column per task: the destination result, and the parts to append.

    Concatenating batches is per-column work — every column owns its own arena
    and its own buffers — so the loops invert cleanly: instead of walking parts
    and touching every column of each, walk columns and append every part's.
    Task `c` is the only writer of column `c`, and every part is read-only.
    """

    var out: ScanResult
    var rest: List[ScanResult]
    var errors: List[String]

    def __init__(out self, var out_: ScanResult, var rest: List[ScanResult]):
        var n = out_.num_columns()
        self.out = out_^
        self.rest = rest^
        self.errors = List[String]()
        for _ in range(n):
            self.errors.append(String(""))

    def take_out(mut self) -> ScanResult:
        var taken = self.out^
        self.out = ScanResult()
        return taken^

    def __init__(out self, *, deinit move: Self):
        self.out = move.out^
        self.rest = move.rest^
        self.errors = move.errors^


def _concat_one_column(c: Int, ctx: OpaquePtr) -> None:
    var x = UnsafePointer[_ConcatCtx, MutUntrackedOrigin](
        unsafe_from_address=Int(ctx)
    )
    try:
        for j in range(len(x[].rest)):
            concat_tree(
                x[].out.columns[c].arena,
                x[].out.columns[c].root,
                x[].rest[j].columns[c].arena,
                x[].rest[j].columns[c].root,
            )
    except e:
        x[].errors[c] = String(e)


def _concat_parts(
    var parts: List[ScanResult], workers: Int
) raises -> ScanResult:
    """Every part, concatenated into one result, in order."""
    var n = len(parts)
    var rev = List[ScanResult]()
    for _ in range(n):
        rev.append(parts.pop())
    var live = List[ScanResult]()
    for _ in range(n):
        var p = rev.pop()
        if p.num_rows() > 0:
            live.append(p^)
    if len(live) == 0:
        return ScanResult()
    if len(live) == 1:
        return live.pop()

    var m = len(live)
    var rev2 = List[ScanResult]()
    for _ in range(m):
        rev2.append(live.pop())
    var first = rev2.pop()
    var rest = List[ScanResult]()
    for _ in range(m - 1):
        rest.append(rev2.pop())

    var ncols = first.num_columns()
    for j in range(len(rest)):
        if rest[j].num_columns() != ncols:
            raise Error("iceberg: cannot append results with different shapes")

    if workers <= 1 or ncols <= 1:
        var out = first^
        for j in range(len(rest)):
            out.append(rest[j])
        return out^

    var ctx = _ConcatCtx(first^, rest^)
    var w = workers
    if w > ncols:
        w = ncols
    parallel_for[_concat_one_column](
        n_tasks=ncols,
        ctx=opaque_ptr(Int(UnsafePointer(to=ctx))),
        num_workers=w,
    )
    for k in range(len(ctx.errors)):
        if ctx.errors[k] != "":
            raise Error(ctx.errors[k])
    return ctx.take_out()


def _scan_one_file(i: Int, ctx: OpaquePtr) -> None:
    """One file scan task, on whichever worker drew index `i`.

    A `parallel_for` body cannot raise — pthread has no exception channel — so
    a failure lands in this task's own error slot and the caller re-raises it
    after the join, in task order, so the message a scan fails with does not
    depend on which worker lost.
    """
    var c = UnsafePointer[_FileScanCtx, MutUntrackedOrigin](
        unsafe_from_address=Int(ctx)
    )
    try:
        c[].out[i] = read_data_file(
            c[].io,
            c[].tasks[i].data_file,
            c[].tasks[i].delete_files,
            c[].tasks[i].data_sequence_number,
            c[].specs[i],
            c[].schema,
            c[].ids,
            c[].meta_columns,
            c[].mapping,
            c[].tasks[i].residual,
            c[].case_sensitive,
            c[].options,
        )
    except e:
        c[].errors[i] = String(e)


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
    def has_any_snapshot(self) -> Bool:
        """False for a table that has been created but never written to.

        Such a table is not an error to scan — it has a schema and no rows —
        so every path below falls back to the table's current schema rather
        than demanding a snapshot that does not exist.
        """
        if self.has_snapshot:
            return True
        return self.metadata.has_current_snapshot

    def snapshot(self) raises -> Snapshot:
        if self.has_snapshot:
            return self.metadata.snapshot_by_id(self.snapshot_id)
        return self.metadata.current_snapshot()

    def current_schema(self) raises -> Schema:
        if not self.has_any_snapshot():
            return self.metadata.schema()
        return self.metadata.schema_for_snapshot(self.snapshot())

    def schema(self) raises -> Schema:
        var full = self.current_schema()
        if len(self.selected) == 0:
            return full^
        var ids = List[Int]()
        for k in range(len(self.selected)):
            if is_metadata_column(self.selected[k]):
                continue
            ids.append(full.find_by_name(self.selected[k]).id)
        return full.select(ids)

    def projected_field_ids(self) raises -> List[Int]:
        var full = self.current_schema()
        var out = List[Int]()
        if len(self.selected) == 0:
            var cols = full.columns()
            for k in range(len(cols)):
                out.append(cols[k].id)
            return out^
        for k in range(len(self.selected)):
            if is_metadata_column(self.selected[k]):
                continue
            out.append(full.find_by_name(self.selected[k]).id)
        return out^

    # ── planning ───────────────────────────────────────────────────────────
    def plan_files(self) raises -> List[FileScanTask]:
        if not self.has_any_snapshot():
            return List[FileScanTask]()
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
                        e.sequence_number,
                    )
                )
        return tasks^

    # ── reading ────────────────────────────────────────────────────────────
    def _split_selection(self) raises -> Tuple[List[Int], List[String]]:
        """The selected columns, split into schema field ids and the metadata
        columns (`_file`, `_pos`, `_spec_id`, `_partition`, `_row_id`,
        `_last_updated_sequence_number`), which have no field id."""
        var full = self.current_schema()
        var ids = List[Int]()
        var meta = List[String]()
        if len(self.selected) == 0:
            var cols = full.columns()
            for k in range(len(cols)):
                ids.append(cols[k].id)
            return (ids^, meta^)
        for k in range(len(self.selected)):
            if is_metadata_column(self.selected[k]):
                meta.append(self.selected[k])
                continue
            ids.append(full.find_by_name(self.selected[k]).id)
        return (ids^, meta^)

    def name_mapping(self) raises -> NameMapping:
        """`schema.name-mapping.default`, or an empty mapping."""
        if NAME_MAPPING_PROPERTY in self.metadata.properties:
            return NameMapping.parse(
                self.metadata.properties[NAME_MAPPING_PROPERTY]
            )
        return NameMapping()

    def _specs_for(
        self, tasks: List[FileScanTask]
    ) raises -> List[PartitionSpec]:
        var out = List[PartitionSpec]()
        for k in range(len(tasks)):
            var id = tasks[k].spec_id
            if self.metadata.has_spec(id):
                out.append(self.metadata.spec_by_id(id))
            else:
                out.append(PartitionSpec.unpartitioned(id))
        return out^

    def _read_files(
        self,
        var tasks: List[FileScanTask],
        schema: Schema,
        ids: List[Int],
        meta_columns: List[String],
        mapping: NameMapping,
        options: ScanOptions,
    ) raises -> List[List[ScanResult]]:
        """Every planned file's batches, one slot per task, in task order.

        Sequential when `num_workers` is 1 — which is the default, so nothing
        that existed before this method changes shape — and otherwise one
        `parallel_for` task per file. Either way slot `k` holds the batches of
        `tasks[k]`, so the rows a scan returns and the order they come in are
        the same whichever path ran. A `limit` never takes the parallel path;
        `to_table`/`to_batches` handle that case themselves.
        """
        var specs = self._specs_for(tasks)
        var n = len(tasks)
        var workers = options.num_workers
        if workers == 0:
            workers = num_cpus()
        if workers > n:
            workers = n
        if workers <= 1 or n <= 1:
            var out = List[List[ScanResult]]()
            for k in range(n):
                out.append(
                    read_data_file(
                        self.io,
                        tasks[k].data_file,
                        tasks[k].delete_files,
                        tasks[k].data_sequence_number,
                        specs[k],
                        schema,
                        ids,
                        meta_columns,
                        mapping,
                        tasks[k].residual,
                        self.case_sensitive,
                        options,
                    )
                )
            return out^

        var ctx = _FileScanCtx(
            self.io.copy(),
            tasks^,
            specs^,
            schema.copy(),
            ids.copy(),
            meta_columns.copy(),
            mapping.copy(),
            options.copy(),
            self.case_sensitive,
        )
        parallel_for[_scan_one_file](
            n_tasks=n,
            ctx=opaque_ptr(Int(UnsafePointer(to=ctx))),
            num_workers=workers,
        )
        # `ctx` is mentioned here, after the join, on purpose: Mojo destroys a
        # value at its last use, and the workers read it until they are joined.
        for k in range(len(ctx.errors)):
            if ctx.errors[k] != "":
                raise Error(ctx.errors[k])
        return ctx.take_out()

    def to_table(
        self, options: ScanOptions = ScanOptions()
    ) raises -> ScanResult:
        """Read every planned file and return the rows, projected and filtered.
        """
        var schema = self.current_schema()
        var split = self._split_selection()
        var ids = split[0].copy()
        var meta_columns = split[1].copy()
        var mapping = self.name_mapping()
        var tasks = self.plan_files()
        var out = ScanResult()
        if options.limit >= 0:
            var specs = self._specs_for(tasks)
            var left = options.limit
            for k in range(len(tasks)):
                var opts = options.copy()
                opts.limit = left
                var parts = read_data_file(
                    self.io,
                    tasks[k].data_file,
                    tasks[k].delete_files,
                    tasks[k].data_sequence_number,
                    specs[k],
                    schema,
                    ids,
                    meta_columns,
                    mapping,
                    tasks[k].residual,
                    self.case_sensitive,
                    opts,
                )
                for j in range(len(parts)):
                    out.append(parts[j])
                left = options.limit - out.num_rows()
                if left <= 0:
                    break
        else:
            var workers = options.num_workers
            if workers == 0:
                workers = num_cpus()
            var all = self._read_files(
                tasks^, schema, ids, meta_columns, mapping, options
            )
            var flat = List[ScanResult]()
            var n = len(all)
            var rev = List[List[ScanResult]]()
            for _ in range(n):
                rev.append(all.pop())
            for _ in range(n):
                var parts = rev.pop()
                var m = len(parts)
                var prev = List[ScanResult]()
                for _ in range(m):
                    prev.append(parts.pop())
                for _ in range(m):
                    flat.append(prev.pop())
            out = _concat_parts(flat^, workers)
        if len(out.columns) == 0:
            # Nothing was read: still describe the shape of the result.
            out = empty_scan_result(schema, ids, meta_columns)
        return out^

    def to_batches(
        self, options: ScanOptions = ScanOptions()
    ) raises -> List[RecordBatch]:
        """The same rows, as Arrow `RecordBatch`es straight off the kernels.

        One batch per Parquet batch read, not one per data file: nothing is
        concatenated on the way out, which is what makes this the fast path.
        """
        var schema = self.current_schema()
        var split = self._split_selection()
        var ids = split[0].copy()
        var meta_columns = split[1].copy()
        var mapping = self.name_mapping()
        var tasks = self.plan_files()
        var out = List[RecordBatch]()
        if options.limit >= 0:
            var specs = self._specs_for(tasks)
            var left = options.limit
            var seen = 0
            for k in range(len(tasks)):
                var opts = options.copy()
                opts.limit = left
                var parts = read_data_file(
                    self.io,
                    tasks[k].data_file,
                    tasks[k].delete_files,
                    tasks[k].data_sequence_number,
                    specs[k],
                    schema,
                    ids,
                    meta_columns,
                    mapping,
                    tasks[k].residual,
                    self.case_sensitive,
                    opts,
                )
                seen += _drain_into(parts^, out)
                left = options.limit - seen
                if left <= 0:
                    break
            return out^
        var all = self._read_files(
            tasks^, schema, ids, meta_columns, mapping, options
        )
        var n = len(all)
        var rev = List[List[ScanResult]]()
        for _ in range(n):
            rev.append(all.pop())
        for _ in range(n):
            _ = _drain_into(rev.pop(), out)
        return out^

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
