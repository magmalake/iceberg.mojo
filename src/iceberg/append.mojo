"""Fast append: new data files, a new manifest, a new snapshot.

A fast append is the one write operation that never rewrites anything. It
adds a manifest holding the files it wrote, carries every manifest of the
parent snapshot over into the new manifest list **by reference** — same path,
same sequence numbers, same `first_row_id` — and commits a snapshot whose
`operation` is `append`.

What that produces, in order:

1. data files (`iceberg.write`), one Parquet file per partition per batch;
2. one manifest of ADDED entries, with null sequence numbers so that the
   manifest list's own numbers are inherited, and — under v3 — null
   `first_row_id`s for the same reason;
3. a manifest list: the new manifest first, then the parent's, each keeping
   the values it was committed with;
4. a `Snapshot` with the standard summary keys, and the metadata edits a
   commit implies: `last-sequence-number`, `last-updated-ms`, `snapshot-log`,
   `metadata-log`, `refs.main`, and under v3 `next-row-id`.

**Row lineage (v3).** The snapshot's `first-row-id` is the table's
`next-row-id` before the commit; the new manifest is assigned that same id in
the manifest list, and every data file in it inherits an id from it in order.
The table's new `next-row-id` is `first-row-id + added-rows`. Existing
manifests keep the ids they were assigned, so no two ranges overlap. That is
the rule the v3 fixture generator encodes and the reader already implements
for the other direction.

Overwrite, delete, replace and compaction are **not** here: they need a merge
of manifests rather than an append to them, and the iceberg-rs.mojo bridge
covers them.
"""

from std.collections import Dict

from parquet import RecordBatch

from .io import FileIO, join_path
from .json import substr
from .manifest import (
    DataFile,
    ManifestEntry,
    ManifestFile,
    MANIFEST_CONTENT_DATA,
    MANIFEST_CONTENT_DELETES,
    STATUS_ADDED,
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
from .util import now_ms, random_long, uuid4, zero_pad
from .write import WriteOptions, _partition_key, write_data_files


comptime OP_APPEND = String("append")


struct AppendResult(Copyable, Movable):
    """A prepared commit: the new metadata, and where its files went."""

    var metadata: TableMetadata
    var snapshot_id: Int64
    var manifest_list: String
    var manifest_paths: List[String]
    var data_files: List[DataFile]

    def __init__(
        out self,
        var metadata: TableMetadata,
        snapshot_id: Int64,
        var manifest_list: String,
        var manifest_paths: List[String],
        var data_files: List[DataFile],
    ):
        self.metadata = metadata^
        self.snapshot_id = snapshot_id
        self.manifest_list = manifest_list^
        self.manifest_paths = manifest_paths^
        self.data_files = data_files^

    def __init__(out self, *, copy: Self):
        self.metadata = copy.metadata.copy()
        self.snapshot_id = copy.snapshot_id
        self.manifest_list = copy.manifest_list.copy()
        self.manifest_paths = copy.manifest_paths.copy()
        self.data_files = copy.data_files.copy()

    def __init__(out self, *, deinit move: Self):
        self.metadata = move.metadata^
        self.snapshot_id = move.snapshot_id
        self.manifest_list = move.manifest_list^
        self.manifest_paths = move.manifest_paths^
        self.data_files = move.data_files^

    def added_records(self) -> Int64:
        var n: Int64 = 0
        for k in range(len(self.data_files)):
            n += self.data_files[k].record_count
        return n


def _metadata_dir(metadata: TableMetadata) -> String:
    return join_path(metadata.location, "metadata")


def _total(metadata: TableMetadata, key: String) -> Int64:
    """A `total-*` summary key of the current snapshot, or 0."""
    try:
        var s = metadata.current_snapshot()
        return s.summary_int(key, 0)
    except:
        return 0


def prepare_append(
    io: FileIO,
    metadata: TableMetadata,
    var data_files: List[DataFile],
    var extra_summary: Dict[String, String],
) raises -> AppendResult:
    """Write the manifest and manifest list for an append, and build the new
    table metadata. Nothing is committed: the caller persists the metadata."""
    if len(data_files) == 0:
        raise Error("iceberg: an append needs at least one data file")
    var schema = metadata.schema()
    var spec = metadata.spec()
    var v = metadata.format_version
    var snapshot_id = random_long()
    var sequence_number: Int64 = 0
    if v > 1:
        sequence_number = metadata.last_sequence_number + 1
    var timestamp = now_ms()

    # ── the manifest of everything this commit adds ────────────────────────
    var entries = List[ManifestEntry]()
    var added_rows: Int64 = 0
    for k in range(len(data_files)):
        added_rows += data_files[k].record_count
        entries.append(
            ManifestEntry(
                STATUS_ADDED,
                snapshot_id,
                True,
                sequence_number,
                sequence_number,
                data_files[k].copy(),
            )
        )
    var written = write_manifest(
        v,
        schema,
        spec,
        MANIFEST_CONTENT_DATA,
        snapshot_id,
        sequence_number,
        entries,
    )
    var manifest_path = join_path(_metadata_dir(metadata), uuid4() + "-m0.avro")
    io.write_all(manifest_path, Span(written.bytes))

    # ── the manifest list: ours, then the parent's, unchanged ──────────────
    var first_row_id = metadata.next_row_id
    var new_mf = written.to_manifest_file(
        manifest_path,
        spec.spec_id,
        snapshot_id,
        sequence_number,
        first_row_id,
        v >= 3,
    )
    var manifests = List[ManifestFile]()
    manifests.append(new_mf^)
    var parent_id: Int64 = 0
    var has_parent = False
    if metadata.has_current_snapshot:
        var parent = metadata.current_snapshot()
        parent_id = parent.snapshot_id
        has_parent = True
        if parent.manifest_list != "":
            var carried = read_manifest_list_io(io, parent.manifest_list)
            for k in range(len(carried)):
                manifests.append(carried[k].copy())

    var list_path = join_path(
        _metadata_dir(metadata),
        "snap-" + String(snapshot_id) + "-0-" + uuid4() + ".avro",
    )
    var list_bytes = write_manifest_list(
        v, snapshot_id, parent_id, has_parent, sequence_number, manifests
    )
    io.write_all(list_path, Span(list_bytes))

    # ── the snapshot ───────────────────────────────────────────────────────
    var added_size: Int64 = 0
    for k in range(len(data_files)):
        added_size += data_files[k].file_size_in_bytes
    var summary = Dict[String, String]()
    summary["operation"] = OP_APPEND
    summary["added-data-files"] = String(len(data_files))
    summary["added-records"] = String(added_rows)
    summary["added-files-size"] = String(added_size)
    summary["changed-partition-count"] = String(
        _distinct_partitions(data_files)
    )
    summary["total-records"] = String(
        _total(metadata, String("total-records")) + added_rows
    )
    summary["total-files-size"] = String(
        _total(metadata, String("total-files-size")) + added_size
    )
    summary["total-data-files"] = String(
        _total(metadata, String("total-data-files")) + Int64(len(data_files))
    )
    summary["total-delete-files"] = String(
        _total(metadata, String("total-delete-files"))
    )
    summary["total-position-deletes"] = String(
        _total(metadata, String("total-position-deletes"))
    )
    summary["total-equality-deletes"] = String(
        _total(metadata, String("total-equality-deletes"))
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
                MAIN_BRANCH,
                snapshot_id,
                REF_BRANCH,
                0,
                False,
                0,
                False,
                0,
                False,
            )
        )
    if v >= 3:
        out.next_row_id = first_row_id + added_rows
        out.has_next_row_id = True

    var paths = List[String]()
    paths.append(manifest_path)
    return AppendResult(out^, snapshot_id, list_path^, paths^, data_files^)


def _distinct_partitions(files: List[DataFile]) raises -> Int:
    var seen = List[String]()
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


def write_and_prepare_append(
    io: FileIO,
    metadata: TableMetadata,
    batches: List[RecordBatch],
) raises -> AppendResult:
    """Write `batches` as data files, then prepare the commit."""
    var options = WriteOptions.from_properties(metadata.properties)
    var files = write_data_files(
        io,
        metadata.location,
        batches,
        metadata.schema(),
        metadata.spec(),
        metadata.default_sort_order_id,
        options,
    )
    return prepare_append(io, metadata, files^, Dict[String, String]())


def next_metadata_version(metadata: TableMetadata) -> Int:
    """The `<V>` of the next `<V>-<uuid>.metadata.json`.

    Java derives it from the current file's own name, falling back to the
    length of the metadata log — which is what a table written by something
    else, or a brand new one, needs.
    """
    var name = metadata.metadata_file_location
    var slash = name.rfind("/")
    if slash >= 0:
        name = substr(name, slash + 1, name.byte_length())
    var dash = name.find("-")
    if dash > 0:
        var head = substr(name, 0, dash)
        var ok = head.byte_length() > 0
        var b = head.as_bytes()
        for k in range(len(b)):
            if b[k] < 48 or b[k] > 57:
                ok = False
                break
        if ok:
            try:
                return Int(head) + 1
            except:
                pass
    return len(metadata.metadata_log) + 1


def metadata_file_name(version: Int) -> String:
    return zero_pad(version, 5) + "-" + uuid4() + ".metadata.json"
