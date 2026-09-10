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
    ManifestCache,
    read_manifest_io_cached,
    read_manifest_list_io_cached,
)
from .metadata import Snapshot, TableMetadata
from .schema import Schema
from .transforms import PartitionSpec
from .types import (
    P_BOOLEAN,
    P_DECIMAL,
    P_DOUBLE,
    P_FLOAT,
    P_UUID,
    TK_PRIMITIVE,
    is_integer_like,
)
from .values import compare


comptime TRUE_RESIDUAL = String('["true"]')
"""What `Expr.text` prints for a residual that has nothing left to check.

`ResidualEvaluator.residual_for` builds that residual as a single `OP_TRUE`
node, and `Expr.text` prints exactly this for it, so the comparison is against
a canonical string and not against something a user typed."""


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


def _worker_split(num_workers: Int, n_files: Int) -> Tuple[Int, Int]:
    """A scan's thread budget, divided into files-at-once and threads-per-file.

    A scan has two nested axes it can spend threads on: the planned data files,
    which this module fans out over, and the *(row group, leaf)* pairs inside
    one file, which `ParquetReader.num_workers` fans out over. The file axis is
    the coarser and the cheaper — tasks are shared-nothing whole files — so it
    is filled first, and only what it cannot use is passed inward. Splitting
    the budget evenly instead would be strictly worse at both ends: it would
    leave half the threads idle on a 24-file scan, where the file axis alone
    already saturates, and it would cap a one-file scan at half the machine.

    Concretely, with `w` resolved workers and `n` files:

    * `n >= w` — every worker gets a file and there is nothing left over, so
      each file is read on one thread. This is byte-for-byte the behaviour
      that existed before the second axis was wired up.
    * `n < w` — the file axis is `n` wide and `w - n` threads would otherwise
      idle, so each file is read with `w // n` of them. A single-file scan is
      the limiting case and gets the whole budget inside the reader, which is
      the case this exists for.

    The product `n_files_at_once * per_file` never exceeds `w`, so the nested
    `parallel_for`s cannot oversubscribe the machine between them.
    """
    var w = num_workers
    if w == 0:
        w = num_cpus()
    if w < 1:
        w = 1
    if n_files <= 0:
        return (0, w)
    var at_once = w if w < n_files else n_files
    return (at_once, w // at_once)


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
    var x = Pointer[_ConcatCtx, MutUntrackedOrigin](
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
        ctx=opaque_ptr(Int(Pointer(to=ctx))),
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
    var c = Pointer[_FileScanCtx, MutUntrackedOrigin](
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
        """The scan's file tasks, planned with a cache of its own.

        The cache lives exactly as long as the call: a snapshot's manifests
        were written together and share their schemas, so this is where the
        reuse is. A caller planning several snapshots — time travel, a diff
        between two refs — should hold one cache across them and use
        `plan_files_with`.
        """
        var cache = ManifestCache()
        return self.plan_files_with(cache)

    def plan_files_with(
        self, mut cache: ManifestCache
    ) raises -> List[FileScanTask]:
        """`plan_files`, reusing (and adding to) a cache the caller owns."""
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
                snap,
                snap.manifests.copy(),
                schema,
                row_filter,
                metrics_eval,
                cache,
            )
        var manifests = read_manifest_list_io_cached(
            self.io, snap.manifest_list, cache
        )
        return self._plan(
            snap, manifests, schema, row_filter, metrics_eval, cache
        )

    def _plan_from_manifests(
        self,
        snap: Snapshot,
        paths: List[String],
        schema: Schema,
        row_filter: Expr,
        metrics_eval: InclusiveMetricsEvaluator,
        mut cache: ManifestCache,
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
        return self._plan(snap, mfs, schema, row_filter, metrics_eval, cache)

    def _plan(
        self,
        snap: Snapshot,
        manifests: List[ManifestFile],
        schema: Schema,
        row_filter: Expr,
        metrics_eval: InclusiveMetricsEvaluator,
        mut cache: ManifestCache,
    ) raises -> List[FileScanTask]:
        # ── pass 1: every live delete file in the snapshot ─────────────────
        var deletes = List[_PendingDelete]()
        for k in range(len(manifests)):
            ref mf = manifests[k]
            if not mf.is_delete_manifest():
                continue
            var m = read_manifest_io_cached(
                self.io, mf.manifest_path, mf, cache
            )
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
            var m = read_manifest_io_cached(
                self.io, mf.manifest_path, mf, cache
            )
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

        The file axis is only as wide as the plan, so on its own it leaves the
        budget's remainder idle whenever a query touches fewer files than there
        are workers — a single-file scan ran on exactly one core however many
        were asked for. `_worker_split` hands that remainder to the reader,
        which spends it on the *(row group, leaf)* pairs inside each file; see
        its docstring for why the two are not simply halved.
        """
        var specs = self._specs_for(tasks)
        var n = len(tasks)
        var split = _worker_split(options.num_workers, n)
        var workers = split[0]
        var opts = options.copy()
        opts.num_workers = split[1]
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
                        opts,
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
            opts^,
            self.case_sensitive,
        )
        parallel_for[_scan_one_file](
            n_tasks=n,
            ctx=opaque_ptr(Int(Pointer(to=ctx))),
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

    def to_batches_for_paths(
        self,
        paths: List[String],
        options: ScanOptions = ScanOptions(),
    ) raises -> List[RecordBatch]:
        """The rows of just these data files, as this scan would return them.

        `to_batches` reads the whole plan. This reads the part of it named by
        `paths`, which is what lets the plan be split across processes: one
        planner hands out `plan_files()`, and each worker asks only for the
        files it was given.

        The filter is on the *planned* tasks, not on the table, so everything
        the plan decided still applies to the files that survive it —
        partition pruning, the residual, and the delete files attached to each
        task. A worker therefore returns exactly the rows the whole-table scan
        would have returned for those files, which is what makes the union of
        workers equal the whole.

        Paths that the plan does not contain are ignored rather than raising:
        a snapshot can be replaced between planning and reading, and a worker
        holding a stale ticket should come back empty rather than fail the
        query. A caller that needs to know can compare lengths.
        """
        var schema = self.current_schema()
        var split = self._split_selection()
        var ids = split[0].copy()
        var meta_columns = split[1].copy()
        var mapping = self.name_mapping()

        var wanted = List[FileScanTask]()
        for task in self.plan_files():
            for i in range(len(paths)):
                if task.data_file.file_path == paths[i]:
                    wanted.append(task.copy())
                    break

        var out = List[RecordBatch]()
        if len(wanted) == 0:
            return out^
        var all = self._read_files(
            wanted^, schema, ids, meta_columns, mapping, options
        )
        var n = len(all)
        var rev = List[List[ScanResult]]()
        for _ in range(n):
            rev.append(all.pop())
        for _ in range(n):
            _ = _drain_into(rev.pop(), out)
        return out^

    def to_batch_reader(
        self, options: ScanOptions = ScanOptions()
    ) raises -> BatchReader:
        """The same rows again, one batch at a time instead of all at once.

        `to_batches` holds the whole result before the caller sees the first
        batch, so a scan that returns most of a large table costs the whole
        table in memory. A `BatchReader` reads the plan in waves and hands the
        batches out as it goes, so a caller that folds — counts, sums, writes
        onward — holds one wave rather than one result. The rows and their
        order are the same as `to_batches`; see `BatchReader` for what a wave
        is and why the order survives `num_workers`.

        Planning happens here, not on the first batch, so a bad snapshot or an
        unreadable manifest raises from this call rather than from the loop.
        """
        return BatchReader(self, options)

    # ── counting ───────────────────────────────────────────────────────────
    def count(self, options: ScanOptions = ScanOptions()) raises -> Int64:
        """How many rows this scan would return, without reading the ones the
        manifests have already counted.

        A `DataFile` carries `record_count`, so a task whose every row survives
        to the caller contributes its count with the data file never opened —
        which is the whole point: an unfiltered `COUNT(*)` becomes a walk of
        the manifests instead of a decode of the table.
        `_countable_from_metadata` decides which tasks those are, and the rest
        are read and their surviving rows counted, so a scan that cannot be
        answered from metadata is still answered, only slowly.

        Adding the two halves is exact because the tasks partition the scan's
        rows: a row belongs to exactly one data file, and `to_batches` produces
        each task's rows independently of every other task's. The answer is
        therefore `len(to_table())` by construction, and the tests assert
        exactly that on every fixture that takes the reading path.

        This is not `select count(x)`: `select` narrows the columns and never
        the rows, so a projection is ignored here. Snapshot selection is not
        ignored — `plan_files` has already resolved `use_snapshot`, `use_ref`
        and `as_of`, so time travel, a branch and a tag each count their own
        snapshot, and a table that has never been written to plans no tasks and
        counts zero.
        """
        var tasks = self.plan_files()

        # A `limit` truncates the scan in task order, so the count has to be
        # accumulated in that order too and stopped where the reader would
        # stop. Only the tasks that are actually reached get read, which is
        # what makes `count` with a small limit cheap on a table it cannot
        # count from metadata at all.
        if options.limit >= 0:
            var lim = Int64(options.limit)
            var seen: Int64 = 0
            for k in range(len(tasks)):
                if seen >= lim:
                    break
                if _countable_from_metadata(tasks[k]):
                    seen += tasks[k].data_file.record_count
                    continue
                var opts = options.copy()
                opts.limit = Int(lim - seen)
                var one = List[FileScanTask]()
                one.append(tasks[k].copy())
                seen += self._count_by_reading(one^, opts)
            return lim if seen > lim else seen

        var total: Int64 = 0
        var to_read = List[FileScanTask]()
        for k in range(len(tasks)):
            if _countable_from_metadata(tasks[k]):
                total += tasks[k].data_file.record_count
            else:
                to_read.append(tasks[k].copy())
        return total + self._count_by_reading(to_read^, options)

    def _count_by_reading(
        self, var tasks: List[FileScanTask], options: ScanOptions
    ) raises -> Int64:
        """The rows these tasks really return, counted by reading them.

        Only ever called with tasks `_countable_from_metadata` rejected, so the
        work here is proportional to how much of the table the metadata could
        not answer for rather than to the table.

        `options.limit` is honoured per file, which is only the same thing as a
        scan-wide limit when there is one task — so `count` calls this one task
        at a time whenever a limit is set, and hands it every remaining task at
        once when there is none.
        """
        if len(tasks) == 0:
            return 0
        var schema = self.current_schema()
        var ids = _count_projection(schema)
        if len(ids) == 0:
            raise Error(
                "iceberg: cannot count '"
                + self.metadata.location
                + "' by reading: its schema has no columns, and the file"
                " metadata does not carry a usable record count"
            )
        var mapping = self.name_mapping()
        var all = self._read_files(
            tasks^, schema, ids, List[String](), mapping, options
        )
        var n: Int64 = 0
        for k in range(len(all)):
            for j in range(len(all[k])):
                n += Int64(all[k][j].num_rows())
        return n

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


struct BatchReader(Movable):
    """A scan being read a batch at a time, in the order `to_batches` returns.

    The plan is consumed in **waves**: the next `wave` file scan tasks go
    through `TableScan._read_files` together, their batches are handed out in
    task order, and only when the last of them has been yielded is the next
    wave started. Nothing beyond the wave in flight is held, so a fold over a
    scan costs one wave rather than one result — with the default
    `num_workers = 1` a wave is one data file.

    ## Ordering

    **Rows arrive in exactly the order `to_batches` returns them, for every
    `num_workers`.** That is the whole reason for the wave: within a wave the
    merge is `_read_files`' existing merge by task index, and waves are
    consumed in plan order, so thread scheduling cannot reach the output. The
    alternative — yielding each file as its worker finishes — would have made
    a scan's row order depend on which core won a race, and therefore differ
    between two runs of the same query on the same data. The `num_workers`
    docstring promises the opposite, and `to_table`/`to_batches` both depend
    on it; a streaming path that quietly broke it would be a worse API than
    one that buffers.

    The wave is `TableScan._worker_split`'s file-axis width, which is
    `min(num_workers, len(tasks))`, so every worker still gets a file and the
    parallel path is as wide as it was. What the wave costs instead is a
    barrier at its end: a wave is only done when its slowest file is, and a
    worker that finishes early idles until then, where `to_batches` would have
    moved it onto the next file. That is the price of the ordering guarantee,
    and it is paid in time, not in memory.

    A scan with a `limit` streams one file at a time — the file axis is not
    used at all, exactly as in `to_batches`, because stopping early is only
    meaningful in order.

    ## Errors

    `next_batch` is the primitive and raises what a read raises. `batches()`
    exists for `for batch in reader.batches():`, and a `for` loop cannot carry
    an error out: `Iterator.__next__` may raise only `StopIteration`. A read
    that fails inside the loop therefore *ends* the loop and parks the message
    on the reader, and **the caller must call `raise_if_failed` afterwards**
    or a truncated scan reads as a complete one:

    ```mojo
    var reader = table.scan().to_batch_reader(options)
    var rows = 0
    for batch in reader.batches():
        rows += batch.num_rows
    reader.raise_if_failed()
    ```
    """

    var scan: TableScan
    var tasks: List[FileScanTask]
    var schema: Schema
    var ids: List[Int]
    var meta_columns: List[String]
    var mapping: NameMapping
    var options: ScanOptions
    var wave: Int
    """How many data files a wave reads before any of them is handed out."""
    var next_task: Int
    """The first task of the wave that has not been read yet."""
    var pending: List[RecordBatch]
    """The current wave's undelivered batches, *reversed*: `pop()` takes the
    next one in task order and moves it, where indexing would copy an arena."""
    var rows: Int
    """Rows handed out, which is what a `limit` counts down."""
    var done: Bool
    var failure: String
    """What a read raised inside `batches()`, where it could not escape."""

    def __init__(
        out self, scan: TableScan, options: ScanOptions = ScanOptions()
    ) raises:
        self.scan = scan.copy()
        self.schema = scan.current_schema()
        var split = scan._split_selection()
        self.ids = split[0].copy()
        self.meta_columns = split[1].copy()
        self.mapping = scan.name_mapping()
        self.tasks = scan.plan_files()
        self.options = options.copy()
        # `_worker_split` on the whole plan, so the wave is the same file-axis
        # width `to_batches` would have used; `_read_files` recomputes the
        # split per wave and lands on the same numbers, because
        # `min(w, min(w, n)) == min(w, n)`.
        var budget = _worker_split(options.num_workers, len(self.tasks))
        self.wave = budget[0] if budget[0] > 0 else 1
        if options.limit >= 0:
            self.wave = 1
        self.next_task = 0
        self.pending = List[RecordBatch]()
        self.rows = 0
        self.done = False
        self.failure = String("")

    def __init__(out self, *, deinit move: Self):
        self.scan = move.scan^
        self.tasks = move.tasks^
        self.schema = move.schema^
        self.ids = move.ids^
        self.meta_columns = move.meta_columns^
        self.mapping = move.mapping^
        self.options = move.options^
        self.wave = move.wave
        self.next_task = move.next_task
        self.pending = move.pending^
        self.rows = move.rows
        self.done = move.done
        self.failure = move.failure^

    def num_tasks(self) -> Int:
        """How many data files the plan has — the scan's whole work list."""
        return len(self.tasks)

    def _fill(mut self) raises -> Bool:
        """Read waves until one produces a batch. False when nothing is left.

        The loop is needed because a wave can be empty — every row in it
        deleted, or filtered out by the residual — and an empty wave must not
        look like the end of the scan.
        """
        while self.next_task < len(self.tasks):
            if self.options.limit >= 0 and self.rows >= self.options.limit:
                return False
            var lo = self.next_task
            var hi = lo + self.wave
            if hi > len(self.tasks):
                hi = len(self.tasks)
            var wave_tasks = List[FileScanTask]()
            for k in range(lo, hi):
                wave_tasks.append(self.tasks[k].copy())
            var opts = self.options.copy()
            if self.options.limit >= 0:
                opts.limit = self.options.limit - self.rows
            var read = self.scan._read_files(
                wave_tasks^,
                self.schema,
                self.ids,
                self.meta_columns,
                self.mapping,
                opts,
            )
            self.next_task = hi
            var n = len(read)
            var rev = List[List[ScanResult]]()
            for _ in range(n):
                rev.append(read.pop())
            var ordered = List[RecordBatch]()
            for _ in range(n):
                _ = _drain_into(rev.pop(), ordered)
            var m = len(ordered)
            for _ in range(m):
                self.pending.append(ordered.pop())
            if m > 0:
                return True
        return False

    def next_batch(mut self) raises -> Optional[RecordBatch]:
        """The next batch, or nothing once the scan is finished.

        This is the API that reports a read failure honestly; `batches()` is
        the same walk with `for`-loop sugar and a deferred error.
        """
        if self.done:
            return None
        if len(self.pending) == 0:
            if not self._fill():
                self.done = True
                return None
        var batch = self.pending.pop()
        self.rows += batch.num_rows
        return Optional[RecordBatch](batch^)

    def batches(mut self) -> _BatchIter[origin_of(self)]:
        """A `for`-loop view of this reader. See the note on errors above: the
        loop ends on a read failure and `raise_if_failed` is what reports it.
        """
        return _BatchIter[origin_of(self)](Pointer(to=self))

    def failed(self) -> Bool:
        """Whether a read failed inside `batches()` and ended the loop early."""
        return self.failure != ""

    def raise_if_failed(self) raises:
        """Re-raise what `batches()` could not. A no-op after a clean scan."""
        if self.failure != "":
            raise Error(self.failure)


@fieldwise_init
struct _BatchIter[origin: Origin[mut=True]](
    ImplicitlyCopyable, Iterable, Iterator
):
    """Walks a `BatchReader` that outlives the loop, and never owns its state.

    The origin is mutable and tracked: the reader is what advances, and this
    is a borrow of it rather than a copy, so `raise_if_failed` on the reader
    the caller still holds sees what the loop hit. That is also why
    `BatchReader` is not itself `Iterable` — the trait's `IteratorType` has to
    be well-formed at an immutable origin too, and a copy of a reader would
    duplicate the plan and then swallow the error into the duplicate.
    """

    comptime Element = RecordBatch
    comptime IteratorType[
        iterable_mut: Bool, //, iterable_origin: Origin[mut=iterable_mut]
    ]: Iterator = Self

    var _reader: Pointer[BatchReader, Self.origin]

    def __iter__(ref self) -> Self.IteratorType[origin_of(self)]:
        """An iterator is its own iterable.

        Returns:
            A copy of this iterator, which is one pointer.
        """
        return self.copy()

    def __next__(mut self) raises StopIteration -> RecordBatch:
        """The next batch.

        Returns:
            The batch the reader is on.

        Raises:
            StopIteration: At the end of the scan, and also when a read
                failed — the failure is parked on the reader for
                `raise_if_failed`, because this signature admits no other
                error.
        """
        var got: Optional[RecordBatch]
        try:
            got = self._reader[].next_batch()
        except e:
            self._reader[].failure = String(e)
            raise StopIteration()
        if not got:
            raise StopIteration()
        return got.take()

    def bounds(self) -> Tuple[Int, Optional[Int]]:
        """How many batches are left, which a scan does not know until it has
        read them.

        Returns:
            `(0, None)` — no lower bound worth quoting and no upper one.
        """
        return (0, None)


def _countable_from_metadata(task: FileScanTask) -> Bool:
    """Whether `record_count` is exactly what this task will hand the caller.

    The question is never "is the metadata plausible" but the much narrower
    "will the reader return every row of this file, and only those rows". Each
    clause below is one way `read_data_file` can answer that with a different
    number, and a clause it cannot decide is answered `False`: being slow costs
    a read, being wrong costs the caller a number they cannot tell is wrong.

    1. **Any delete file at all.** `record_count` is what the writer put in the
       file, and every kind of delete removes rows from that afterwards: a v2
       position-delete file, a v3 deletion vector, and an equality delete all
       leave the file's own count untouched. It is not enough that a delete
       exists somewhere in the snapshot — `_deletes_for` has already narrowed
       them to the ones whose scope covers this data file — but a delete that
       is in scope may still remove nothing, and finding out costs a read of
       both files, so an in-scope delete is disqualifying on sight.
    2. **A residual that is not `true`.** The residual is what the filter still
       has to check per row, and the reader evaluates exactly this string. When
       it is `true` no row is dropped, so the file's rows and the scan's rows
       are the same set; anything else drops an unknown number of them. Keying
       off the residual rather than off the filter is what makes this widen on
       its own as `ResidualEvaluator` gets sharper — a boundary-aligned range
       over a partitioned table that reduces to `true` becomes countable here
       with no change to this function.
    3. **A task that is not the whole file.** `plan_files` emits one task per
       data file today, so `start` is 0 and `length` is the file's size, and
       `record_count` — a per-file number — is the count of exactly what the
       task covers. Were the planner to split a file across tasks, summing
       `record_count` per task would count every split file once per split, so
       the invariant that made this sound is asserted rather than assumed.
    4. **A record count that is not positive.** A count of zero is either a
       genuinely empty file, which reading answers correctly and instantly, or
       a manifest whose schema has no `record_count` field, which decodes to
       the same zero and would silently swallow the file's rows. The two are
       indistinguishable from here, so both are read.

    Not disqualifying, deliberately: a DELETED manifest entry, which
    `ManifestEntry.is_live` already dropped during planning and which therefore
    cannot reach a task; a partition pruned away whole, which produces no task
    and so contributes nothing to the sum; and the projection, which changes
    which columns come back and never how many rows.
    """
    if len(task.delete_files) > 0:
        return False
    if task.residual != TRUE_RESIDUAL:
        return False
    if task.start != 0 or task.length != task.data_file.file_size_in_bytes:
        return False
    if task.data_file.record_count <= 0:
        return False
    return True


def _count_projection(schema: Schema) raises -> List[Int]:
    """The one column to read when a count has to be counted the slow way.

    How many rows come back does not depend on which column is read, so this
    picks the one that costs least to decode and hands `read_data_file` only
    that: a fixed-width primitive is a memcpy per page, a string or a binary is
    an offset buffer and a heap of bytes, and a nested column has to assemble
    every leaf under it to produce one value. The reader adds back whatever the
    residual and the equality deletes still need to look at, so this is a floor
    on the columns read and not a ceiling.

    Being *present in the file* matters more than being narrow, which is why a
    required field wins over an optional one of the same shape: a column added
    by a later schema is absent from the older files and comes back as a
    constant null, which is correct but reads nothing and so cannot be checked
    against anything. The row count is right either way — a batch's row count
    comes from the row group, not from the columns selected out of it.
    """
    var cols = schema.columns()
    var best = -1
    var best_rank = 1 << 20
    for k in range(len(cols)):
        ref node = schema.store.nodes[cols[k].type]
        var rank = 4
        if node.kind == TK_PRIMITIVE:
            rank = 2 if _is_fixed_width(node.prim) else 3
        if cols[k].required:
            rank -= 2
        if rank < best_rank:
            best_rank = rank
            best = cols[k].id
    var out = List[Int]()
    if best >= 0:
        out.append(best)
    return out^


def _is_fixed_width(prim: UInt8) -> Bool:
    """True for the primitives that decode to a fixed number of bytes per row.

    `fixed` and `binary` are left out on purpose: the first is fixed-width but
    can be arbitrarily wide, and the second is not fixed-width at all."""
    return (
        is_integer_like(prim)
        or prim == P_BOOLEAN
        or prim == P_FLOAT
        or prim == P_DOUBLE
        or prim == P_DECIMAL
        or prim == P_UUID
    )


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
