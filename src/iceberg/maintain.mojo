"""Expiring snapshots, and deleting the files that nothing points at any more.

Expiry is two decisions, in this order:

1. **Which snapshots go.** Never the current one, never one a branch or tag
   names, and never so many that fewer than `keep_last` are left. Of what
   remains, the ones older than `older_than_ms`.
2. **Which files go with them.** A file is deleted only when *no retained
   snapshot still lists it live*. That is stronger than "it was in an expired
   snapshot": a data file added five snapshots ago is in every snapshot since,
   and expiring the oldest of them must not touch it. So the retained
   snapshots are walked first and every manifest list, manifest and live entry
   they name is put in a keep set; only then is the expired snapshots' own
   inventory subtracted from it.

A `DELETED` entry does not protect a file — that is the entry that says it is
gone — but an older *retained* snapshot that still lists it `ADDED` or
`EXISTING` does, which is why the keep set is a union over all of them.

The new metadata is written **before** any file is deleted. A crash in between
leaves orphans, which cost storage; the other order leaves a snapshot pointing
at files that are not there, which costs the table. `dry_run` stops after the
decision and returns the list, which is the only safe way to find out what an
expiry would do.

Metadata files (`*.metadata.json`) are left alone. Java deletes them under
`write.metadata.delete-after-commit.enabled` with its own retention count, and
that is a separate policy from snapshot expiry: the metadata log is how a
reader reaches a snapshot by timestamp in the first place.
"""

from .io import FileIO
from .manifest import (
    STATUS_DELETED,
    read_manifest_io,
    read_manifest_list_io,
)
from .metadata import Snapshot, SnapshotLogEntry, TableMetadata
from .util import now_ms


struct ExpireResult(Copyable, Defaultable, Movable):
    """What an expiry decided, and what it did about it."""

    var expired: List[Int64]
    """Snapshot ids removed from the metadata."""
    var retained: List[Int64]
    var deleted_files: List[String]
    """Data and delete files nothing retained points at any more."""
    var deleted_manifests: List[String]
    """Manifests and manifest lists, likewise."""
    var dry_run: Bool

    def __init__(out self):
        self.expired = []
        self.retained = []
        self.deleted_files = []
        self.deleted_manifests = []
        self.dry_run = False

    def __init__(out self, *, copy: Self):
        self.expired = copy.expired.copy()
        self.retained = copy.retained.copy()
        self.deleted_files = copy.deleted_files.copy()
        self.deleted_manifests = copy.deleted_manifests.copy()
        self.dry_run = copy.dry_run

    def __init__(out self, *, deinit move: Self):
        self.expired = move.expired^
        self.retained = move.retained^
        self.deleted_files = move.deleted_files^
        self.deleted_manifests = move.deleted_manifests^
        self.dry_run = move.dry_run

    def total_deleted(self) -> Int:
        return len(self.deleted_files) + len(self.deleted_manifests)


def _has(l: List[String], v: String) -> Bool:
    for k in range(len(l)):
        if l[k] == v:
            return True
    return False


def _add(mut l: List[String], var v: String):
    if not _has(l, v):
        l.append(v^)


def _referenced_by(
    io: FileIO,
    snapshot: Snapshot,
    mut metadata_files: List[String],
    mut data_files: List[String],
    live_only: Bool,
) raises:
    """Every file one snapshot names, split into metadata and content.

    `live_only` is the difference between "what this snapshot needs" (a keep
    set) and "what this snapshot ever mentioned" (a candidate set): a DELETED
    entry names a file the snapshot does not need, but does know about.
    """
    if snapshot.manifest_list == "":
        return
    _add(metadata_files, snapshot.manifest_list)
    var manifests = read_manifest_list_io(io, snapshot.manifest_list)
    for k in range(len(manifests)):
        _add(metadata_files, manifests[k].manifest_path)
        var m = read_manifest_io(io, manifests[k].manifest_path, manifests[k])
        for j in range(len(m.entries)):
            ref e = m.entries[j]
            if live_only and e.status == STATUS_DELETED:
                continue
            _add(data_files, e.data_file.file_path)


def choose_expired(
    metadata: TableMetadata, older_than_ms: Int64, keep_last: Int
) raises -> List[Int64]:
    """The snapshot ids an expiry would remove, oldest first.

    The current snapshot and every snapshot a ref names are never candidates,
    and `keep_last` is a floor on how many survive in total.
    """
    var protected = List[Int64]()
    if metadata.has_current_snapshot:
        protected.append(metadata.current_snapshot_id)
    for k in range(len(metadata.refs)):
        protected.append(metadata.refs[k].snapshot_id)

    # Oldest first, so the ones expiry reaches for are at the front.
    var order = List[Int]()
    for k in range(len(metadata.snapshots)):
        order.append(k)
    for i in range(1, len(order)):
        var j = i
        while (
            j > 0
            and metadata.snapshots[order[j]].timestamp_ms
            < metadata.snapshots[order[j - 1]].timestamp_ms
        ):
            order.swap_elements(j, j - 1)
            j -= 1

    var floor = keep_last if keep_last > 1 else 1
    var surviving = len(metadata.snapshots)
    var out = List[Int64]()
    for i in range(len(order)):
        if surviving <= floor:
            break
        ref s = metadata.snapshots[order[i]]
        var is_protected = False
        for k in range(len(protected)):
            if protected[k] == s.snapshot_id:
                is_protected = True
                break
        if is_protected:
            continue
        if older_than_ms >= 0 and s.timestamp_ms >= older_than_ms:
            continue
        out.append(s.snapshot_id)
        surviving -= 1
    return out^


def expire_snapshots(
    io: FileIO,
    metadata: TableMetadata,
    older_than_ms: Int64 = -1,
    keep_last: Int = 1,
    dry_run: Bool = False,
) raises -> Tuple[TableMetadata, ExpireResult]:
    """Decide the expiry and, unless `dry_run`, carry out the deletions.

    Returns the pruned metadata and a report. The caller commits the metadata
    — and must do so *before* the files go, which is why deletion is the last
    thing this does rather than the first.
    """
    var result = ExpireResult()
    result.dry_run = dry_run
    result.expired = choose_expired(metadata, older_than_ms, keep_last)
    if len(result.expired) == 0:
        return (metadata.copy(), result^)

    var out = metadata.copy()
    out.snapshots = List[Snapshot]()
    var keep_metadata = List[String]()
    var keep_data = List[String]()
    for k in range(len(metadata.snapshots)):
        ref s = metadata.snapshots[k]
        var expired = False
        for j in range(len(result.expired)):
            if result.expired[j] == s.snapshot_id:
                expired = True
                break
        if expired:
            continue
        out.snapshots.append(s.copy())
        result.retained.append(s.snapshot_id)
        _referenced_by(io, s, keep_metadata, keep_data, True)

    # What the expired snapshots knew about, minus what a retained one needs.
    var stale_metadata = List[String]()
    var stale_data = List[String]()
    for k in range(len(metadata.snapshots)):
        ref s = metadata.snapshots[k]
        for j in range(len(result.expired)):
            if result.expired[j] != s.snapshot_id:
                continue
            _referenced_by(io, s, stale_metadata, stale_data, False)
            break
    for k in range(len(stale_data)):
        if not _has(keep_data, stale_data[k]):
            result.deleted_files.append(stale_data[k])
    for k in range(len(stale_metadata)):
        if not _has(keep_metadata, stale_metadata[k]):
            result.deleted_manifests.append(stale_metadata[k])

    # A file a retained snapshot marks DELETED, and no retained snapshot still
    # lists live, is an orphan too — the delete that produced it is history.
    for k in range(len(metadata.snapshots)):
        ref s = metadata.snapshots[k]
        var expired = False
        for j in range(len(result.expired)):
            if result.expired[j] == s.snapshot_id:
                expired = True
                break
        if expired:
            continue
        var mentioned = List[String]()
        var junk = List[String]()
        _referenced_by(io, s, junk, mentioned, False)
        for j in range(len(mentioned)):
            if _has(keep_data, mentioned[j]):
                continue
            if _has(result.deleted_files, mentioned[j]):
                continue
            result.deleted_files.append(mentioned[j])

    var log = List[SnapshotLogEntry]()
    for k in range(len(metadata.snapshot_log)):
        ref e = metadata.snapshot_log[k]
        var expired = False
        for j in range(len(result.expired)):
            if result.expired[j] == e.snapshot_id:
                expired = True
                break
        if not expired:
            log.append(e.copy())
    out.snapshot_log = log^
    out.last_updated_ms = now_ms()
    return (out^, result^)


def delete_expired_files(io: FileIO, result: ExpireResult) raises -> Int:
    """Remove what an expiry decided is unreachable. Content first.

    A file that is already gone is not an error: an expiry that was
    interrupted, or two of them racing, both end at the same place.
    """
    if result.dry_run:
        return 0
    var n = 0
    for k in range(len(result.deleted_files)):
        try:
            io.delete(result.deleted_files[k])
            n += 1
        except:
            pass
    for k in range(len(result.deleted_manifests)):
        try:
            io.delete(result.deleted_manifests[k])
            n += 1
        except:
            pass
    return n
