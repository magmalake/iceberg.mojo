"""Row-level delete and overwrite: which rows go, and how they are recorded.

Two strategies, chosen by the table property `write.delete.mode`:

* **`copy-on-write`** (the default, as in Java, and the only thing PyIceberg
  can do) — every data file with a matching row is rewritten without those
  rows and the original is marked `DELETED`. The result needs no delete files
  at all, so *every* reader can read it, at the cost of rewriting whole files
  to remove one row.
* **`merge-on-read`** — the rows stay where they are and a delete file records
  their positions. Under **v3** that is a **deletion vector**: one Puffin
  `deletion-vector-v1` blob per data file, referenced by a manifest entry with
  `content=1`, `file_format=puffin`, `referenced_data_file` and the
  `content_offset`/`content_size_in_bytes` that must match the blob's footer
  entry exactly. Under **v2** it is a **position delete file**: a Parquet file
  of `(file_path, pos)` pairs — the two reserved field ids 2147483546 and
  2147483545 — sorted by path then position, one per partition.

Whichever strategy is in force, a data file whose rows are *all* deleted is
removed rather than emptied. That is the metadata-delete fast path: when the
scan's residual for a file is `true` — the partition predicate alone already
proves every row matches — the file is not read at all, and the delete costs
one manifest rewrite.

**Merging.** A deletion vector must contain every position deleted from its
data file, not just the new ones: the spec makes a new vector absorb the ones
it replaces, and a reader that finds a vector ignores every position delete
file for that data file. So a second delete against a file that already has a
vector reads the old vector, unions the new positions in, writes a fresh blob,
and marks the old vector's manifest entry `DELETED`. Position delete *files*
are left alone even when their positions are absorbed: one of them can cover
several data files, and removing it would resurrect rows in the others.

**Overwrite** is a delete and an append in one snapshot, always copy-on-write,
with `operation=overwrite`. `dynamic_partition_overwrite` derives the filter
from the data instead of taking one: it replaces exactly the partitions the
new rows land in, which needs no row-level work at all — a partition is
replaced by removing every data file in it.
"""

from std.collections import Dict

from parquet import RecordBatch
from parquet.arrow import ArrayData
from roaring import Bitmap64

from .append import AppendResult
from .batch import ColumnBuilder, batch_of
from .commit import FileChanges, OP_DELETE, OP_OVERWRITE, prepare_commit
from .expressions import ColumnMetrics, parse_filter
from .io import FileIO, join_path
from .kernels import filter_array, int_at
from .manifest import (
    CONTENT_EQUALITY_DELETES,
    CONTENT_POSITION_DELETES,
    DataFile,
)
from .metadata import TableMetadata
from .puffin import PuffinWriter
from .read import (
    META_POS,
    NameMapping,
    POS_DELETE_FILE_PATH_ID,
    POS_DELETE_POS_ID,
    ScanOptions,
    ScanResult,
    _position_delete_bitmap,
    read_data_file,
)
from .scan import FileScanTask, TableScan
from .schema import Schema
from .transforms import PartitionSpec
from .types import P_LONG, P_STRING
from .util import uuid4
from .values import Datum
from .nested import ColumnTree
from .write import (
    WriteOptions,
    _partition_key,
    align_batch,
    data_file_from_parquet,
    partition_path,
    write_data_files,
    write_parquet,
)


comptime PROP_DELETE_MODE = String("write.delete.mode")
comptime MODE_COPY_ON_WRITE = String("copy-on-write")
comptime MODE_MERGE_ON_READ = String("merge-on-read")

comptime POS_DELETE_SCHEMA_JSON = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":2147483546,"name":"file_path","required":true,"type":"string"},'
    '{"id":2147483545,"name":"pos","required":true,"type":"long"}]}'
)


def delete_mode_of(properties: Dict[String, String]) raises -> String:
    """`write.delete.mode`, defaulting to copy-on-write as Java does."""
    if PROP_DELETE_MODE not in properties:
        return MODE_COPY_ON_WRITE
    var mode = properties[PROP_DELETE_MODE].lower()
    if mode == MODE_COPY_ON_WRITE or mode == MODE_MERGE_ON_READ:
        return mode^
    raise Error(
        "iceberg: "
        + PROP_DELETE_MODE
        + " must be 'copy-on-write' or 'merge-on-read', not '"
        + mode
        + "'"
    )


struct RowDelete(Copyable, Movable):
    """One data file, and which of its rows this delete takes out."""

    var task: FileScanTask
    var positions: List[UInt64]
    """The positions the filter matched, ascending. Live rows only: a row a
    previous delete already removed is not matched again."""
    var existing: List[UInt64]
    """Positions already deleted by the delete files that apply to this data
    file — what a new deletion vector has to absorb."""
    var live_rows: Int64
    """Rows still alive in the file before this delete."""
    var whole_file: Bool
    """Every live row matched, so the file goes rather than being marked up."""
    var superseded: List[DataFile]
    """Delete files this commit replaces — the deletion vectors a new vector
    absorbs."""

    def __init__(
        out self,
        var task: FileScanTask,
        var positions: List[UInt64],
        var existing: List[UInt64],
        live_rows: Int64,
        whole_file: Bool,
        var superseded: List[DataFile],
    ):
        self.task = task^
        self.positions = positions^
        self.existing = existing^
        self.live_rows = live_rows
        self.whole_file = whole_file
        self.superseded = superseded^

    def __init__(out self, *, copy: Self):
        self.task = copy.task.copy()
        self.positions = copy.positions.copy()
        self.existing = copy.existing.copy()
        self.live_rows = copy.live_rows
        self.whole_file = copy.whole_file
        self.superseded = copy.superseded.copy()

    def __init__(out self, *, deinit move: Self):
        self.task = move.task^
        self.positions = move.positions^
        self.existing = move.existing^
        self.live_rows = move.live_rows
        self.whole_file = move.whole_file
        self.superseded = move.superseded^


def _pos_column(result: ScanResult) raises -> Int:
    for c in range(result.num_columns()):
        if result.name(c) == META_POS:
            return c
    raise Error("iceberg: the reader did not return a _pos column")


def _matched_positions(
    io: FileIO,
    task: FileScanTask,
    schema: Schema,
    spec: PartitionSpec,
    mapping: NameMapping,
    residual: String,
    case_sensitive: Bool,
    options: ScanOptions,
) raises -> List[UInt64]:
    """The positions of the rows in one data file that `residual` selects.

    Read through the ordinary scan path, so the deletes already on the file
    are applied first and a row that is gone is not deleted twice.
    """
    var parts = read_data_file(
        io,
        task.data_file,
        task.delete_files,
        task.data_sequence_number,
        spec,
        schema,
        List[Int](),
        [META_POS],
        mapping,
        residual,
        case_sensitive,
        options,
    )
    var out = List[UInt64]()
    for k in range(len(parts)):
        if parts[k].num_rows() == 0:
            continue
        var at = _pos_column(parts[k])
        ref column = parts[k].columns[at].array()
        for r in range(parts[k].num_rows()):
            out.append(UInt64(int_at(column, r)))
    return out^


def plan_row_deletes(
    scan: TableScan, filter_dsl: String, options: ScanOptions = ScanOptions()
) raises -> List[RowDelete]:
    """Which rows of which data files a `DELETE ... WHERE` takes out."""
    var planned = scan.filter(filter_dsl)
    var tasks = planned.plan_files()
    var schema = planned.current_schema()
    var mapping = planned.name_mapping()
    var out = List[RowDelete]()
    for k in range(len(tasks)):
        ref task = tasks[k]
        var spec = PartitionSpec.unpartitioned(task.spec_id)
        if scan.metadata.has_spec(task.spec_id):
            spec = scan.metadata.spec_by_id(task.spec_id)

        # Positions a previous delete already removed, and the vectors this
        # commit would replace.
        var already = _position_delete_bitmap(
            scan.io,
            task.data_file.file_path,
            task.data_file.record_count,
            task.delete_files,
            options,
        )
        var existing = List[UInt64]()
        for p in range(len(already)):
            if already[p]:
                existing.append(UInt64(p))
        var superseded = List[DataFile]()
        var has_equality = False
        for j in range(len(task.delete_files)):
            ref d = task.delete_files[j]
            if d.is_equality_delete():
                has_equality = True
            elif d.is_deletion_vector():
                superseded.append(d.copy())

        var residual = parse_filter(task.residual)
        var trivial = residual.is_true(residual.root)
        var positions = List[UInt64]()
        var live: Int64 = task.data_file.record_count - Int64(len(existing))
        if trivial and not has_equality:
            # The partition predicate alone proves every row matches: no read.
            positions = existing_complement(
                task.data_file.record_count, already
            )
        else:
            positions = _matched_positions(
                scan.io,
                task,
                schema,
                spec,
                mapping,
                task.residual,
                scan.case_sensitive,
                options,
            )
            if has_equality:
                var all_live = _matched_positions(
                    scan.io,
                    task,
                    schema,
                    spec,
                    mapping,
                    String('["true"]'),
                    scan.case_sensitive,
                    options,
                )
                live = Int64(len(all_live))
        if len(positions) == 0:
            continue
        var whole = Int64(len(positions)) >= live
        out.append(
            RowDelete(
                task.copy(), positions^, existing^, live, whole, superseded^
            )
        )
    return out^


def existing_complement(
    record_count: Int64, already: List[Bool]
) -> List[UInt64]:
    """Every position of a data file that is not already deleted."""
    var out = List[UInt64]()
    for p in range(Int(record_count)):
        if p < len(already) and already[p]:
            continue
        out.append(UInt64(p))
    return out^


def deleted_row_count(plans: List[RowDelete]) -> Int64:
    var n: Int64 = 0
    for k in range(len(plans)):
        n += Int64(len(plans[k].positions))
    return n


# ── merge-on-read: deletion vectors (v3) ────────────────────────────────────
def write_deletion_vectors(
    io: FileIO, location: String, plans: List[RowDelete]
) raises -> List[DataFile]:
    """One Puffin file holding a vector per partially-deleted data file.

    Every vector carries the file's *whole* delete set — the positions this
    commit adds, unioned with the ones already there — because a reader that
    finds a vector uses it alone.
    """
    var w = PuffinWriter()
    var which = List[Int]()
    for k in range(len(plans)):
        if plans[k].whole_file:
            continue
        var bitmap = Bitmap64()
        for j in range(len(plans[k].existing)):
            bitmap.add(plans[k].existing[j])
        for j in range(len(plans[k].positions)):
            bitmap.add(plans[k].positions[j])
        _ = w.add_deletion_vector(plans[k].task.data_file.file_path, bitmap)
        which.append(k)
    var out = List[DataFile]()
    if len(which) == 0:
        return out^
    var bytes = w.finish()
    var path = join_path(
        join_path(location, "data"), uuid4() + "-deletion-vectors.puffin"
    )
    io.write_all(path, Span(bytes))
    for b in range(len(which)):
        ref plan = plans[which[b]]
        var blob = w.blob(b)
        out.append(
            DataFile(
                CONTENT_POSITION_DELETES,
                path,
                String("PUFFIN"),
                plan.task.data_file.partition.copy(),
                blob.cardinality(),
                Int64(len(bytes)),
                List[ColumnMetrics](),
                List[ColumnMetrics](),
                List[UInt8](),
                False,
                List[Int64](),
                List[Int](),
                0,
                False,
                0,
                False,
                plan.task.data_file.file_path,
                True,
                blob.offset,
                True,
                blob.length,
                True,
            )
        )
    return out^


# ── merge-on-read: position delete files (v2) ───────────────────────────────
def write_position_deletes(
    io: FileIO,
    location: String,
    plans: List[RowDelete],
    spec: PartitionSpec,
    options: WriteOptions,
) raises -> List[DataFile]:
    """One Parquet file of `(file_path, pos)` per partition, sorted.

    The two columns are written under the reserved ids the spec assigns them,
    and the rows are sorted by path then position, which is what makes a
    reader able to merge them without buffering the whole file.
    """
    var delete_schema = Schema.parse(POS_DELETE_SCHEMA_JSON)
    var keys = List[String]()
    var partitions = List[List[Datum]]()
    var groups = List[List[Int]]()
    for k in range(len(plans)):
        if plans[k].whole_file:
            continue
        var key = _partition_key(plans[k].task.data_file.partition)
        var at = -1
        for j in range(len(keys)):
            if keys[j] == key:
                at = j
                break
        if at < 0:
            at = len(keys)
            keys.append(key^)
            partitions.append(plans[k].task.data_file.partition.copy())
            groups.append(List[Int]())
        groups[at].append(k)

    var out = List[DataFile]()
    var counter = 0
    for g in range(len(groups)):
        # Sorted by file path, then by position within a file.
        var order = groups[g].copy()
        for i in range(1, len(order)):
            var j = i
            while (
                j > 0
                and plans[order[j]].task.data_file.file_path
                < plans[order[j - 1]].task.data_file.file_path
            ):
                order.swap_elements(j, j - 1)
                j -= 1
        var paths = ColumnBuilder(
            String("file_path"),
            POS_DELETE_FILE_PATH_ID,
            P_STRING,
            0,
            0,
            0,
            False,
        )
        var positions = ColumnBuilder(
            String("pos"), POS_DELETE_POS_ID, P_LONG, 0, 0, 0, False
        )
        var rows = 0
        for o in range(len(order)):
            ref plan = plans[order[o]]
            for p in range(len(plan.positions)):
                paths.add(Datum.string_(plan.task.data_file.file_path))
                positions.add(Datum.long_(Int64(plan.positions[p])))
                rows += 1
        if rows == 0:
            continue
        var columns = List[ColumnTree]()
        columns.append(ColumnTree(paths.build()))
        columns.append(ColumnTree(positions.build()))
        var data = write_parquet(columns, delete_schema, options)
        var dir = join_path(location, "data")
        var rel = partition_path(spec, partitions[g])
        if rel != "":
            dir = join_path(dir, rel)
        var name = (
            "00000-"
            + String(counter)
            + "-position-deletes-"
            + uuid4()
            + ".parquet"
        )
        counter += 1
        var path = join_path(dir, name)
        io.write_all(path, Span(data))
        var df = data_file_from_parquet(
            data, path, delete_schema, spec, partitions[g], 0, options
        )
        df.content = CONTENT_POSITION_DELETES
        # A delete file is not sorted by any of the table's sort orders.
        df.has_sort_order_id = False
        out.append(df^)
    return out^


# ── copy-on-write: rewrite the files that lost rows ─────────────────────────
def surviving_batch(
    io: FileIO,
    plan: RowDelete,
    schema: Schema,
    spec: PartitionSpec,
    mapping: NameMapping,
    options: ScanOptions,
) raises -> RecordBatch:
    """Everything left of one data file once this delete has taken its rows.

    Read with no filter and with `_pos`, then dropped by position, rather than
    read with the *negation* of the filter: a three-valued `NOT` does not keep
    the rows a two-valued one drops, and "everything the delete did not match"
    is the only definition that cannot disagree with the delete itself.
    """
    var drop = List[Bool](
        length=Int(plan.task.data_file.record_count), fill=False
    )
    for k in range(len(plan.positions)):
        var p = Int(plan.positions[k])
        if p >= 0 and p < len(drop):
            drop[p] = True
    var ids = List[Int]()
    var cols = schema.columns()
    for k in range(len(cols)):
        ids.append(cols[k].id)
    var parts = read_data_file(
        io,
        plan.task.data_file,
        plan.task.delete_files,
        plan.task.data_sequence_number,
        spec,
        schema,
        ids,
        [META_POS],
        mapping,
        String('["true"]'),
        True,
        options,
    )
    var whole = ScanResult()
    for k in range(len(parts)):
        whole.append(parts[k])
    var batch = RecordBatch()
    if whole.num_columns() == 0:
        return batch^
    var pos_at = _pos_column(whole)
    var keep = List[Bool](length=whole.num_rows(), fill=False)
    var kept = 0
    ref positions = whole.columns[pos_at].array()
    for r in range(whole.num_rows()):
        var p = Int(int_at(positions, r))
        if p >= 0 and p < len(drop) and drop[p]:
            continue
        keep[r] = True
        kept += 1
    batch.num_rows = kept
    for c in range(whole.num_columns()):
        if c == pos_at:
            continue
        batch.roots.append(
            batch.arena.add(filter_array(whole.columns[c].array(), keep, kept))
        )
    return batch^


def rewrite_files(
    io: FileIO,
    metadata: TableMetadata,
    plans: List[RowDelete],
    options: ScanOptions,
) raises -> List[DataFile]:
    """The replacement data files for every partially-deleted file."""
    var schema = metadata.schema()
    var mapping = NameMapping()
    var batches = List[RecordBatch]()
    for k in range(len(plans)):
        if plans[k].whole_file:
            continue
        var spec = PartitionSpec.unpartitioned(plans[k].task.spec_id)
        if metadata.has_spec(plans[k].task.spec_id):
            spec = metadata.spec_by_id(plans[k].task.spec_id)
        var batch = surviving_batch(
            io, plans[k], schema, spec, mapping, options
        )
        if batch.num_rows > 0:
            batches.append(batch^)
    if len(batches) == 0:
        return List[DataFile]()
    var write_options = WriteOptions.from_properties(metadata.properties)
    return write_data_files(
        io,
        metadata.location,
        batches,
        schema,
        metadata.spec(),
        metadata.default_sort_order_id,
        write_options,
    )


# ── the whole operations ────────────────────────────────────────────────────
def prepare_delete(
    io: FileIO,
    metadata: TableMetadata,
    filter_dsl: String,
    mode: String = String(""),
    options: ScanOptions = ScanOptions(),
) raises -> AppendResult:
    """`DELETE FROM t WHERE <filter>`, as one prepared snapshot.

    Raises when the filter matches nothing, so a caller cannot commit an empty
    snapshot by accident.
    """
    var chosen = mode
    if chosen == "":
        chosen = delete_mode_of(metadata.properties)
    elif chosen != MODE_COPY_ON_WRITE and chosen != MODE_MERGE_ON_READ:
        raise Error(
            "iceberg: unknown delete mode '"
            + chosen
            + "' (copy-on-write, merge-on-read)"
        )
    var scan = TableScan(metadata.copy(), io.copy())
    var plans = plan_row_deletes(scan, filter_dsl, options)
    if len(plans) == 0:
        raise Error(
            "iceberg: the filter " + filter_dsl + " matches no live row"
        )
    return prepare_delete_from(io, metadata, plans, chosen, options)


def prepare_delete_from(
    io: FileIO,
    metadata: TableMetadata,
    plans: List[RowDelete],
    mode: String,
    options: ScanOptions = ScanOptions(),
) raises -> AppendResult:
    """`prepare_delete` with the planning already done — which is what a
    caller that needs the row count before it commits wants."""
    var chosen = mode
    if chosen == "":
        chosen = delete_mode_of(metadata.properties)
    var changes = FileChanges()
    var extra = Dict[String, String]()
    if chosen == MODE_MERGE_ON_READ:
        if metadata.format_version >= 3:
            changes.added_deletes = write_deletion_vectors(
                io, metadata.location, plans
            )
        else:
            changes.added_deletes = write_position_deletes(
                io,
                metadata.location,
                plans,
                metadata.spec(),
                WriteOptions.from_properties(metadata.properties),
            )
        # A vector this commit replaces goes; a position delete file does not,
        # because one of those can serve several data files.
        for k in range(len(plans)):
            if plans[k].whole_file:
                continue
            for j in range(len(plans[k].superseded)):
                changes.removed.append(plans[k].superseded[j].file_path)
    else:
        changes.added_data = rewrite_files(io, metadata, plans, options)
    for k in range(len(plans)):
        if plans[k].whole_file or chosen == MODE_COPY_ON_WRITE:
            changes.removed.append(plans[k].task.data_file.file_path)
    var operation = OP_DELETE if len(changes.added_data) == 0 else OP_OVERWRITE
    return prepare_commit(io, metadata, operation, changes^, extra^)


def prepare_overwrite(
    io: FileIO,
    metadata: TableMetadata,
    batches: List[RecordBatch],
    filter_dsl: String = String('["true"]'),
    options: ScanOptions = ScanOptions(),
) raises -> AppendResult:
    """Delete every row the filter matches and add `batches`, in one snapshot.

    Copy-on-write, always: an overwrite that left merge-on-read deletes behind
    would need the added rows to sort after the deletes it wrote in the same
    commit, and the spec's scope rules do not let a delete from one commit
    skip data added by the same commit.
    """
    var scan = TableScan(metadata.copy(), io.copy())
    var plans = plan_row_deletes(scan, filter_dsl, options)
    var changes = FileChanges()
    changes.added_data = rewrite_files(io, metadata, plans, options)
    for k in range(len(plans)):
        changes.removed.append(plans[k].task.data_file.file_path)
    if len(batches) > 0:
        var write_options = WriteOptions.from_properties(metadata.properties)
        var fresh = write_data_files(
            io,
            metadata.location,
            batches,
            metadata.schema(),
            metadata.spec(),
            metadata.default_sort_order_id,
            write_options,
        )
        for k in range(len(fresh)):
            changes.added_data.append(fresh[k].copy())
    if changes.is_empty():
        raise Error("iceberg: an overwrite must add or remove something")
    return prepare_commit(
        io, metadata, OP_OVERWRITE, changes^, Dict[String, String]()
    )


def prepare_dynamic_partition_overwrite(
    io: FileIO,
    metadata: TableMetadata,
    batches: List[RecordBatch],
) raises -> AppendResult:
    """Replace exactly the partitions the new rows land in.

    No row is examined: the new data files say which partitions they belong
    to, and every existing data file in one of those partitions is removed
    whole. A partition the new data does not touch is left alone, which is the
    difference between this and an unfiltered overwrite.
    """
    if len(metadata.spec().fields) == 0:
        raise Error(
            "iceberg: dynamic partition overwrite needs a partitioned table"
        )
    var write_options = WriteOptions.from_properties(metadata.properties)
    var fresh = write_data_files(
        io,
        metadata.location,
        batches,
        metadata.schema(),
        metadata.spec(),
        metadata.default_sort_order_id,
        write_options,
    )
    if len(fresh) == 0:
        raise Error("iceberg: dynamic partition overwrite got no rows")
    var keys = List[String]()
    for k in range(len(fresh)):
        var key = _partition_key(fresh[k].partition)
        var seen = False
        for j in range(len(keys)):
            if keys[j] == key:
                seen = True
                break
        if not seen:
            keys.append(key^)

    var changes = FileChanges()
    changes.added_data = fresh^
    var scan = TableScan(metadata.copy(), io.copy())
    var tasks = scan.plan_files()
    for k in range(len(tasks)):
        var key = _partition_key(tasks[k].data_file.partition)
        for j in range(len(keys)):
            if keys[j] == key:
                changes.removed.append(tasks[k].data_file.file_path)
                for d in range(len(tasks[k].delete_files)):
                    changes.removed.append(tasks[k].delete_files[d].file_path)
                break
    return prepare_commit(
        io, metadata, OP_OVERWRITE, changes^, Dict[String, String]()
    )


# ── equality deletes (v2) ───────────────────────────────────────────────────
def write_equality_deletes(
    io: FileIO,
    location: String,
    rows: RecordBatch,
    schema: Schema,
    equality_ids: List[Int],
    options: WriteOptions,
) raises -> DataFile:
    """One equality delete file: the values, and the ids they match on.

    A row is deleted when every one of the `equality_ids` columns equals the
    delete row's value for it, `null` included — the spec's "a null value in a
    delete column matches a row if the row's value is null". The file holds
    only those columns, under the table's own field ids, and the manifest
    entry carries `equality_ids` so a reader knows which they are.

    Written with the **unpartitioned** spec, which is what makes the file
    apply to the whole table: a partitioned equality delete only reaches its
    own partition, and pairing one with the right partition tuple is a job for
    a caller that knows the data. Format version 2 only — v3 replaced these
    with deletion vectors for positions and v4 is deprecating writing them.
    """
    if len(equality_ids) == 0:
        raise Error("iceberg: an equality delete needs at least one field id")
    var sub = schema.select(equality_ids)
    var columns = align_batch(rows, sub)
    if rows.num_rows == 0:
        raise Error("iceberg: an equality delete file needs at least one row")
    var data = write_parquet(columns, sub, options)
    var name = "00000-0-equality-deletes-" + uuid4() + ".parquet"
    var path = join_path(join_path(location, "data"), name)
    io.write_all(path, Span(data))
    var df = data_file_from_parquet(
        data,
        path,
        sub,
        PartitionSpec.unpartitioned(),
        List[Datum](),
        0,
        options,
    )
    df.content = CONTENT_EQUALITY_DELETES
    df.equality_ids = equality_ids.copy()
    df.has_sort_order_id = False
    return df^


def prepare_equality_delete(
    io: FileIO,
    metadata: TableMetadata,
    rows: RecordBatch,
    equality_ids: List[Int],
) raises -> AppendResult:
    """`DELETE FROM t WHERE (a, b) IN (<rows>)`, as one prepared snapshot."""
    if metadata.format_version != 2:
        raise Error(
            "iceberg: equality deletes are a v2 feature; this table is v"
            + String(metadata.format_version)
            + " (v3 uses deletion vectors, and v4 deprecates writing them)"
        )
    if len(metadata.spec().fields) != 0:
        raise Error(
            "iceberg: this build writes equality deletes only for"
            " unpartitioned tables, where one file applies to every data file"
        )
    var df = write_equality_deletes(
        io,
        metadata.location,
        rows,
        metadata.schema(),
        equality_ids,
        WriteOptions.from_properties(metadata.properties),
    )
    var changes = FileChanges()
    changes.added_deletes.append(df^)
    return prepare_commit(
        io, metadata, OP_DELETE, changes^, Dict[String, String]()
    )
