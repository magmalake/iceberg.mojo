"""A snapshot that adds *and* removes files, with the manifest maintenance
that implies.

`iceberg.append` only ever adds: its manifest list is the new manifest plus
the parent's, carried by reference. Everything else — delete, overwrite,
replace, compaction — has to *rewrite* the manifests that mention a file it is
removing, and that is what this module does.

    prepare_commit(io, metadata, operation, files, extra)

`files` says what the snapshot adds (data files, delete files) and what it
removes (by path). The parent's manifest list is then walked once:

* a manifest that mentions none of the removed paths is **carried by
  reference** — same path, same counts, same `sequence_number`,
  `min_sequence_number` and `first_row_id`. Nothing is rewritten that does not
  have to be, which is what keeps a delete from touching the whole table;
* a manifest that mentions one is **rewritten**. Every live entry it keeps
  becomes `EXISTING` with its `sequence_number`, `file_sequence_number`,
  `snapshot_id` and (v3) `first_row_id` written out **explicitly** — sequence
  inheritance is ADDED-only, so an EXISTING entry that left them null would be
  silently re-dated. Every live entry it drops becomes `DELETED`, with the
  same numbers, so an expiry can still find the file. Entries that were
  already `DELETED` in the parent belong to an older snapshot and are dropped;
* a rewritten manifest with nothing live left is **dropped from the list**
  entirely rather than written as an empty file.

The rewritten manifest's list entry is credited to the new snapshot
(`added_snapshot_id`, `sequence_number`) while its `min_sequence_number` stays
whatever its oldest surviving file says — which is what makes the delete scope
rules keep working across a rewrite.

**Totals.** The `total-*` summary keys are carried forward from the parent
snapshot and adjusted by what this commit added and removed, because the
manifest list does not record file *sizes* and a scan of every manifest to
recompute them would defeat the point of carrying most of them by reference.

**Row lineage (v3).** Added data files get the table's `next-row-id` through
their manifest, exactly as an append does. A rewritten manifest keeps the
`first_row_id` it was assigned, and its entries carry theirs explicitly, so no
range moves and none overlap. Rows in a data file this commit *rewrites*
(copy-on-write) do get new ids — see the README's gaps table.
"""

from std.collections import Dict

from .append import AppendResult, _metadata_dir, _total
from .io import FileIO, join_path
from .manifest import (
    CONTENT_DATA,
    CONTENT_EQUALITY_DELETES,
    CONTENT_POSITION_DELETES,
    DataFile,
    Manifest,
    ManifestEntry,
    ManifestFile,
    MANIFEST_CONTENT_DATA,
    MANIFEST_CONTENT_DELETES,
    STATUS_ADDED,
    STATUS_DELETED,
    STATUS_EXISTING,
    read_manifest_io,
    read_manifest_list_io,
)
from .manifest_write import write_manifest, write_manifest_list
from .metadata import (
    MAIN_BRANCH,
    REF_BRANCH,
    MetadataLogEntry,
    Snapshot,
    SnapshotLogEntry,
    SnapshotRef,
    TableMetadata,
)
from .schema import Schema
from .transforms import PartitionSpec
from .util import now_ms, random_long, uuid4
from .write import _partition_key


comptime OP_DELETE = String("delete")
comptime OP_OVERWRITE = String("overwrite")
comptime OP_REPLACE = String("replace")


def _is_dv(df: DataFile) -> Bool:
    return df.is_deletion_vector()


struct FileChanges(Copyable, Defaultable, Movable):
    """What one snapshot adds and what it takes away."""

    var added_data: List[DataFile]
    var added_deletes: List[DataFile]
    var removed: List[String]
    """Paths of files this snapshot removes — data or delete, either way the
    manifest entry that names them becomes `DELETED`."""

    def __init__(out self):
        self.added_data = []
        self.added_deletes = []
        self.removed = []

    def __init__(out self, *, copy: Self):
        self.added_data = copy.added_data.copy()
        self.added_deletes = copy.added_deletes.copy()
        self.removed = copy.removed.copy()

    def __init__(out self, *, deinit move: Self):
        self.added_data = move.added_data^
        self.added_deletes = move.added_deletes^
        self.removed = move.removed^

    def is_empty(self) -> Bool:
        return (
            len(self.added_data) == 0
            and len(self.added_deletes) == 0
            and len(self.removed) == 0
        )

    def removes(self, path: String) -> Bool:
        for k in range(len(self.removed)):
            if self.removed[k] == path:
                return True
        return False


struct _Removed(Copyable, Defaultable, Movable):
    """What the manifest rewrite actually found and marked DELETED."""

    var data_files: Int
    var records: Int64
    var files_size: Int64
    var delete_files: Int
    var position_deletes: Int64
    var equality_deletes: Int64
    var dvs: Int
    var partitions: List[String]
    """Partition keys this commit touched by removing something from them."""

    def __init__(out self):
        self.data_files = 0
        self.records = 0
        self.files_size = 0
        self.delete_files = 0
        self.position_deletes = 0
        self.equality_deletes = 0
        self.dvs = 0
        self.partitions = []

    def __init__(out self, *, copy: Self):
        self.data_files = copy.data_files
        self.records = copy.records
        self.files_size = copy.files_size
        self.delete_files = copy.delete_files
        self.position_deletes = copy.position_deletes
        self.equality_deletes = copy.equality_deletes
        self.dvs = copy.dvs
        self.partitions = copy.partitions.copy()

    def __init__(out self, *, deinit move: Self):
        self = Self(copy=move)

    def add(mut self, df: DataFile) raises:
        var key = _partition_key(df.partition)
        var seen = False
        for k in range(len(self.partitions)):
            if self.partitions[k] == key:
                seen = True
                break
        if not seen:
            self.partitions.append(key^)
        if df.is_data():
            self.data_files += 1
            self.records += df.record_count
            self.files_size += df.file_size_in_bytes
            return
        self.delete_files += 1
        if df.content == CONTENT_EQUALITY_DELETES:
            self.equality_deletes += df.record_count
        else:
            self.position_deletes += df.record_count
            if _is_dv(df):
                self.dvs += 1


def _count_added(
    files: List[DataFile], mut n_pos: Int64, mut n_eq: Int64, mut n_dv: Int
):
    for k in range(len(files)):
        ref d = files[k]
        if d.content == CONTENT_EQUALITY_DELETES:
            n_eq += d.record_count
        else:
            n_pos += d.record_count
            if _is_dv(d):
                n_dv += 1


def _entries_of(
    files: List[DataFile], snapshot_id: Int64, sequence_number: Int64
) -> List[ManifestEntry]:
    var out = List[ManifestEntry]()
    for k in range(len(files)):
        out.append(
            ManifestEntry(
                STATUS_ADDED,
                snapshot_id,
                True,
                sequence_number,
                sequence_number,
                files[k].copy(),
            )
        )
    return out^


def _distinct_partition_keys(
    a: List[DataFile], b: List[DataFile], var seen: List[String]
) raises -> Int:
    """How many partitions this commit touched, adding *or* removing."""
    for which in range(2):
        ref files = a if which == 0 else b
        for k in range(len(files)):
            var key = _partition_key(files[k].partition)
            var found = False
            for j in range(len(seen)):
                if seen[j] == key:
                    found = True
                    break
            if not found:
                seen.append(key^)
    return len(seen)


def prepare_commit(
    io: FileIO,
    metadata: TableMetadata,
    operation: String,
    var files: FileChanges,
    var extra_summary: Dict[String, String],
) raises -> AppendResult:
    """Write the manifests and manifest list for a commit, and build the new
    table metadata. Nothing is persisted: the caller commits it."""
    if files.is_empty():
        raise Error("iceberg: a commit must add or remove at least one file")
    var schema = metadata.schema()
    var spec = metadata.spec()
    var v = metadata.format_version
    var snapshot_id = random_long()
    var sequence_number: Int64 = 0
    if v > 1:
        sequence_number = metadata.last_sequence_number + 1
    var timestamp = now_ms()
    var dir = _metadata_dir(metadata)

    var manifests = List[ManifestFile]()
    var written_paths = List[String]()
    var first_row_id = metadata.next_row_id
    var added_rows: Int64 = 0
    var added_size: Int64 = 0

    # ── the manifest of data files this commit adds ────────────────────────
    if len(files.added_data) > 0:
        for k in range(len(files.added_data)):
            added_rows += files.added_data[k].record_count
            added_size += files.added_data[k].file_size_in_bytes
        var w = write_manifest(
            v,
            schema,
            spec,
            MANIFEST_CONTENT_DATA,
            snapshot_id,
            sequence_number,
            _entries_of(files.added_data, snapshot_id, sequence_number),
        )
        var path = join_path(dir, uuid4() + "-m0.avro")
        io.write_all(path, Span(w.bytes))
        written_paths.append(path)
        manifests.append(
            w.to_manifest_file(
                path,
                spec.spec_id,
                snapshot_id,
                sequence_number,
                first_row_id,
                v >= 3,
            )
        )

    # ── the manifest of delete files this commit adds ──────────────────────
    #
    # A delete manifest never assigns row ids: `first_row_id` is for rows a
    # manifest *adds*, and a deletion vector adds none.
    if len(files.added_deletes) > 0:
        for k in range(len(files.added_deletes)):
            added_size += files.added_deletes[k].file_size_in_bytes
        var w = write_manifest(
            v,
            schema,
            spec,
            MANIFEST_CONTENT_DELETES,
            snapshot_id,
            sequence_number,
            _entries_of(files.added_deletes, snapshot_id, sequence_number),
        )
        var path = join_path(dir, uuid4() + "-m0.avro")
        io.write_all(path, Span(w.bytes))
        written_paths.append(path)
        manifests.append(
            w.to_manifest_file(
                path, spec.spec_id, snapshot_id, sequence_number, 0, False
            )
        )

    # ── the parent's manifests: carried, rewritten, or dropped ─────────────
    var removed = _Removed()
    var parent_id: Int64 = 0
    var has_parent = False
    if metadata.has_current_snapshot:
        var parent = metadata.current_snapshot()
        parent_id = parent.snapshot_id
        has_parent = True
        var carried = List[ManifestFile]()
        if parent.manifest_list != "":
            carried = read_manifest_list_io(io, parent.manifest_list)
        for k in range(len(carried)):
            _carry_or_rewrite(
                io,
                dir,
                v,
                snapshot_id,
                sequence_number,
                carried[k],
                files,
                manifests,
                written_paths,
                removed,
            )
    if len(files.removed) > 0 and removed.data_files + removed.delete_files == 0:
        raise Error(
            "iceberg: none of the "
            + String(len(files.removed))
            + " files this commit removes is in the current snapshot"
        )

    # ── the manifest list ──────────────────────────────────────────────────
    var list_path = join_path(
        dir, "snap-" + String(snapshot_id) + "-0-" + uuid4() + ".avro"
    )
    var list_bytes = write_manifest_list(
        v, snapshot_id, parent_id, has_parent, sequence_number, manifests
    )
    io.write_all(list_path, Span(list_bytes))

    # ── the snapshot ───────────────────────────────────────────────────────
    var added_pos: Int64 = 0
    var added_eq: Int64 = 0
    var added_dvs = 0
    _count_added(files.added_deletes, added_pos, added_eq, added_dvs)

    var summary = Dict[String, String]()
    summary["operation"] = operation
    if len(files.added_data) > 0:
        summary["added-data-files"] = String(len(files.added_data))
        summary["added-records"] = String(added_rows)
    if added_size > 0:
        summary["added-files-size"] = String(added_size)
    if removed.data_files > 0:
        summary["deleted-data-files"] = String(removed.data_files)
        summary["deleted-records"] = String(removed.records)
        summary["removed-files-size"] = String(removed.files_size)
    if len(files.added_deletes) > 0:
        summary["added-delete-files"] = String(len(files.added_deletes))
    if added_dvs > 0:
        summary["added-dvs"] = String(added_dvs)
    if added_pos > 0:
        summary["added-position-deletes"] = String(added_pos)
        var pos_files = len(files.added_deletes) - _equality_files(
            files.added_deletes
        )
        if added_dvs == 0 and pos_files > 0:
            summary["added-position-delete-files"] = String(pos_files)
    if added_eq > 0:
        summary["added-equality-deletes"] = String(added_eq)
        summary["added-equality-delete-files"] = String(
            _equality_files(files.added_deletes)
        )
    if removed.delete_files > 0:
        summary["removed-delete-files"] = String(removed.delete_files)
    if removed.dvs > 0:
        summary["removed-dvs"] = String(removed.dvs)
    if removed.position_deletes > 0:
        summary["removed-position-deletes"] = String(removed.position_deletes)
    if removed.equality_deletes > 0:
        summary["removed-equality-deletes"] = String(removed.equality_deletes)
    summary["changed-partition-count"] = String(
        _distinct_partition_keys(
            files.added_data, files.added_deletes, removed.partitions.copy()
        )
    )
    summary["total-records"] = String(
        _total(metadata, String("total-records")) + added_rows - removed.records
    )
    summary["total-files-size"] = String(
        _total(metadata, String("total-files-size"))
        + added_size
        - removed.files_size
    )
    summary["total-data-files"] = String(
        _total(metadata, String("total-data-files"))
        + Int64(len(files.added_data))
        - Int64(removed.data_files)
    )
    summary["total-delete-files"] = String(
        _total(metadata, String("total-delete-files"))
        + Int64(len(files.added_deletes))
        - Int64(removed.delete_files)
    )
    summary["total-position-deletes"] = String(
        _total(metadata, String("total-position-deletes"))
        + added_pos
        - removed.position_deletes
    )
    summary["total-equality-deletes"] = String(
        _total(metadata, String("total-equality-deletes"))
        + added_eq
        - removed.equality_deletes
    )
    for entry in extra_summary.items():
        summary[entry.key] = entry.value

    var snapshot = Snapshot(
        snapshot_id,
        parent_id,
        has_parent,
        sequence_number,
        timestamp,
        list_path,
        List[String](),
        summary^,
        metadata.current_schema_id,
        True,
        first_row_id,
        v >= 3,
        added_rows,
        v >= 3,
        String(""),
        False,
    )

    # ── the new table metadata ─────────────────────────────────────────────
    var out = metadata.copy()
    out.last_sequence_number = sequence_number
    out.last_updated_ms = timestamp
    out.snapshots.append(snapshot^)
    out.current_snapshot_id = snapshot_id
    out.has_current_snapshot = True
    out.snapshot_log.append(SnapshotLogEntry(timestamp, snapshot_id))
    if metadata.metadata_file_location != "":
        out.metadata_log.append(
            MetadataLogEntry(
                metadata.last_updated_ms, metadata.metadata_file_location
            )
        )
    var at = out.ref_index(MAIN_BRANCH)
    if at >= 0:
        out.refs[at].snapshot_id = snapshot_id
    else:
        out.refs.append(
            SnapshotRef(
                MAIN_BRANCH, snapshot_id, REF_BRANCH, 0, False, 0, False, 0,
                False,
            )
        )
    if v >= 3:
        out.next_row_id = first_row_id + added_rows
        out.has_next_row_id = True

    var everything = files.added_data.copy()
    for k in range(len(files.added_deletes)):
        everything.append(files.added_deletes[k].copy())
    return AppendResult(
        out^, snapshot_id, list_path^, written_paths^, everything^
    )


def _equality_files(files: List[DataFile]) -> Int:
    var n = 0
    for k in range(len(files)):
        if files[k].content == CONTENT_EQUALITY_DELETES:
            n += 1
    return n


def _carry_or_rewrite(
    io: FileIO,
    dir: String,
    format_version: Int,
    snapshot_id: Int64,
    sequence_number: Int64,
    mf: ManifestFile,
    files: FileChanges,
    mut manifests: List[ManifestFile],
    mut written_paths: List[String],
    mut removed: _Removed,
) raises:
    """One parent manifest: carried untouched, rewritten, or dropped."""
    if len(files.removed) == 0:
        manifests.append(mf.copy())
        return
    var m = read_manifest_io(io, mf.manifest_path, mf)
    var touched = False
    for k in range(len(m.entries)):
        ref e = m.entries[k]
        if e.is_live() and files.removes(e.data_file.file_path):
            touched = True
            break
    if not touched:
        manifests.append(mf.copy())
        return

    var entries = List[ManifestEntry]()
    var live = 0
    for k in range(len(m.entries)):
        ref e = m.entries[k]
        # An entry that was already DELETED belongs to an older snapshot; the
        # file it names is gone from every live manifest and nothing reads it.
        if not e.is_live():
            continue
        if files.removes(e.data_file.file_path):
            removed.add(e.data_file)
            entries.append(
                ManifestEntry(
                    STATUS_DELETED,
                    snapshot_id,
                    True,
                    e.sequence_number,
                    e.file_sequence_number,
                    e.data_file.copy(),
                )
            )
            continue
        live += 1
        entries.append(
            ManifestEntry(
                STATUS_EXISTING,
                e.snapshot_id,
                True,
                e.sequence_number,
                e.file_sequence_number,
                e.data_file.copy(),
            )
        )
    if live == 0:
        # Nothing survives; the manifest is not written at all, which is what
        # keeps a table that has deleted everything from carrying a list of
        # empty files forever.
        return

    var w = write_manifest(
        format_version,
        m.schema,
        m.partition_spec,
        m.content,
        snapshot_id,
        sequence_number,
        entries,
    )
    var path = join_path(dir, uuid4() + "-m0.avro")
    io.write_all(path, Span(w.bytes))
    written_paths.append(path)
    # The rewritten manifest is credited to *this* snapshot, but its oldest
    # surviving file still dates it: `min_sequence_number` is what the delete
    # scope rules compare against and it must not move forward.
    var out = w.to_manifest_file(
        path,
        m.partition_spec_id,
        snapshot_id,
        sequence_number,
        mf.first_row_id,
        mf.has_first_row_id,
    )
    manifests.append(out^)
