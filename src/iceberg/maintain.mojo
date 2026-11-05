"""Expiring snapshots, and deleting the files that nothing points at any more.

Expiry is two decisions, in this order:

1. **Which snapshots go.** The spec's retention rules, as Java applies them:
   every branch keeps its head, its last `min-snapshots-to-keep` ancestors and
   every ancestor newer than `max-snapshot-age-ms` (the branch's own values,
   or the table's `history.expire.*` properties, or the caller's); every tag
   keeps the snapshot it names; a branch or tag older than `max-ref-age-ms`
   is dropped first (never `main`). A snapshot no ref reaches is kept only
   while it is newer than the age cut. A caller can also pin snapshot ids
   outright (`retain`), which is how a scheduler keeps the snapshots a live
   run has read.
2. **Which files go with them.** A file is deleted only when *no retained
   snapshot still lists it live*.

The second decision is where expiry used to be quadratic: it read every
retained snapshot's manifest list and then every manifest on it, so a table
with one manifest per commit cost snapshots x manifests reads. It is now set
arithmetic on manifest *paths*, which is linear:

* Every manifest list is read once (they are batched on object storage).
  That gives the manifests the retained snapshots reference, and the ones the
  expired snapshots reference. A manifest is immutable and its path names it,
  so a manifest some retained snapshot lists is needed, whole, and is never
  opened to find that out.
* Only the manifests that expired snapshots list and no retained one does are
  deleted, and only they are opened: what they name are the candidates.
* A candidate data file is still needed if a retained manifest lists it live.
  It can only be listed there because a rewrite (a copy-on-write delete, a
  partition overwrite, a manifest merge, a compaction) carried it as an
  *existing* entry: a file is added once, by one snapshot, under a path no
  other file gets. The snapshot that rewrote a manifest is the one that
  dropped it from the list, and the manifest it wrote is credited to it. So
  on one line of history (every snapshot an ancestor of the current one,
  the retained ones the newest), only the retained manifests added by the
  snapshots that dropped an unreferenced manifest, and that carry existing
  or deleted entries, are opened to check the candidates against. With
  other branches or tags, or snapshots off the line, every retained
  manifest is opened once instead — still linear, never per snapshot.

A `DELETED` entry does not protect a file — that is the entry that says it is
gone. With every retained manifest opened, a file a retained snapshot marks
`DELETED` that no retained manifest lists live is an orphan and is deleted
too; with the shortcut it waits for the snapshot that deleted it to expire.

The new metadata is written **before** any file is deleted. A crash in between
leaves orphans, which cost storage; the other order leaves a snapshot pointing
at files that are not there, which costs the table. `dry_run` stops after the
decision and returns the list, which is the only safe way to find out what an
expiry would do.

Metadata files (`*.metadata.json`) are not this module's business: the
catalog's commit trims the `metadata-log` to `write.metadata.previous-versions-max`
and, with `write.metadata.delete-after-commit.enabled`, deletes the files that
fell off it.
"""

from std.collections import Dict

from .io import FileIO
from .manifest import (
    ManifestCache,
    ManifestFile,
    STATUS_DELETED,
    read_manifest_bytes_cached,
    read_manifest_list_bytes_cached,
)
from .metadata import (
    MAIN_BRANCH,
    Snapshot,
    SnapshotLogEntry,
    SnapshotRef,
    StatisticsFile,
    PartitionStatisticsFile,
    TableMetadata,
)
from .util import now_ms


comptime MAX_SNAPSHOT_AGE_MS = String("history.expire.max-snapshot-age-ms")
comptime MIN_SNAPSHOTS_TO_KEEP = String("history.expire.min-snapshots-to-keep")
comptime MAX_REF_AGE_MS = String("history.expire.max-ref-age-ms")
comptime DEFAULT_MAX_SNAPSHOT_AGE_MS: Int64 = 432000000
"""Five days, Java's default."""
comptime DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 1
comptime NO_AGE_LIMIT: Int64 = Int64.MAX
comptime _BATCH = 64
"""Files fetched per `read_all_many`: enough to keep S3 busy, little memory."""


struct ExpirePolicy(Copyable, Defaultable, Movable):
    """What an expiry keeps. The defaults expire nothing but history: every
    snapshot that is not a ref's head and not among the last one."""

    var older_than_ms: Int64
    """Snapshots at or after this timestamp are kept; `NO_AGE_LIMIT` lets age
    protect nothing (only `keep_last` and refs do)."""
    var keep_last: Int
    """The fewest ancestors each branch keeps, its head included."""
    var max_ref_age_ms: Int64
    """Branches and tags (not `main`) whose snapshot is older than this are
    removed; -1 keeps them all."""
    var now_ms: Int64
    var retain: List[Int64]
    """Snapshot ids never expired, whatever their age."""

    def __init__(out self):
        self.older_than_ms = NO_AGE_LIMIT
        self.keep_last = 1
        self.max_ref_age_ms = -1
        self.now_ms = now_ms()
        self.retain = []

    def __init__(out self, older_than_ms: Int64, keep_last: Int):
        """The two arguments `expire_snapshots` always took; a negative
        `older_than_ms` is no age limit."""
        self = Self()
        self.older_than_ms = (
            older_than_ms if older_than_ms >= 0 else NO_AGE_LIMIT
        )
        self.keep_last = keep_last

    @staticmethod
    def from_properties(metadata: TableMetadata) raises -> Self:
        """The table's own policy: `history.expire.max-snapshot-age-ms`
        (five days), `history.expire.min-snapshots-to-keep` (1) and
        `history.expire.max-ref-age-ms` (forever)."""
        var p = Self()
        var age = _int_property(
            metadata, MAX_SNAPSHOT_AGE_MS, DEFAULT_MAX_SNAPSHOT_AGE_MS
        )
        p.older_than_ms = p.now_ms - age if age >= 0 else NO_AGE_LIMIT
        p.keep_last = Int(
            _int_property(
                metadata, MIN_SNAPSHOTS_TO_KEEP, DEFAULT_MIN_SNAPSHOTS_TO_KEEP
            )
        )
        p.max_ref_age_ms = _int_property(metadata, MAX_REF_AGE_MS, -1)
        return p^


def _int_property(
    metadata: TableMetadata, key: String, dflt: Int64
) raises -> Int64:
    var v = metadata.property_(key, "")
    if v == "":
        return dflt
    try:
        return Int64(Int(v))
    except:
        raise Error(
            "iceberg: table property " + key + "=" + v + " is not an integer"
        )


struct ExpireResult(Copyable, Defaultable, Movable):
    """What an expiry decided, and what it did about it."""

    var expired: List[Int64]
    """Snapshot ids removed from the metadata."""
    var retained: List[Int64]
    var removed_refs: List[String]
    """Branches and tags dropped for `max-ref-age-ms`."""
    var deleted_files: List[String]
    """Data and delete files nothing retained points at any more."""
    var deleted_manifests: List[String]
    """Manifests and manifest lists, likewise."""
    var deleted_statistics: List[String]
    """Statistics files of expired snapshots."""
    var dry_run: Bool
    var manifest_lists_read: Int
    var manifests_read: Int
    var reachable: Bool
    """Whether every retained manifest had to be opened (other branches or
    tags, or a history that is not one line)."""

    def __init__(out self):
        self.expired = []
        self.retained = []
        self.removed_refs = []
        self.deleted_files = []
        self.deleted_manifests = []
        self.deleted_statistics = []
        self.dry_run = False
        self.manifest_lists_read = 0
        self.manifests_read = 0
        self.reachable = False

    def total_deleted(self) -> Int:
        return (
            len(self.deleted_files)
            + len(self.deleted_manifests)
            + len(self.deleted_statistics)
        )


struct ExpiryPlan(Copyable, Defaultable, Movable):
    var expired: List[Int64]
    var removed_refs: List[String]

    def __init__(out self):
        self.expired = []
        self.removed_refs = []


def _index_by_id(metadata: TableMetadata) -> Dict[Int64, Int]:
    var at = Dict[Int64, Int]()
    for k in range(len(metadata.snapshots)):
        at[metadata.snapshots[k].snapshot_id] = k
    return at^


def plan_expiry(
    metadata: TableMetadata, policy: ExpirePolicy
) raises -> ExpiryPlan:
    """The snapshots and refs an expiry would remove; snapshots oldest first."""
    var plan = ExpiryPlan()
    var at = _index_by_id(metadata)
    var keep = Dict[Int64, Bool]()
    for k in range(len(policy.retain)):
        keep[policy.retain[k]] = True

    # Refs first: one that is too old goes, and stops protecting anything.
    var refs = List[SnapshotRef]()
    for k in range(len(metadata.refs)):
        ref r = metadata.refs[k]
        var max_age = (
            r.max_ref_age_ms if r.has_max_ref_age_ms else policy.max_ref_age_ms
        )
        if r.name != MAIN_BRANCH and max_age >= 0 and r.snapshot_id in at:
            var ts = metadata.snapshots[at[r.snapshot_id]].timestamp_ms
            if policy.now_ms - ts > max_age:
                plan.removed_refs.append(r.name)
                continue
        refs.append(r.copy())
    var has_main = False
    for k in range(len(refs)):
        if refs[k].name == MAIN_BRANCH:
            has_main = True
    if not has_main and metadata.has_current_snapshot:
        # A table written without refs (v1, or an old writer): the current
        # snapshot is main's head.
        refs.append(
            SnapshotRef(
                MAIN_BRANCH,
                metadata.current_snapshot_id,
                String("branch"),
                0,
                False,
                0,
                False,
                0,
                False,
            )
        )
    if metadata.has_current_snapshot:
        keep[metadata.current_snapshot_id] = True

    # Every branch keeps its recent ancestors; every ref keeps its head.
    var reached = Dict[Int64, Bool]()
    for k in range(len(refs)):
        ref r = refs[k]
        keep[r.snapshot_id] = True
        if not r.is_branch():
            reached[r.snapshot_id] = True
            continue
        var min_keep = Int(
            r.min_snapshots_to_keep
        ) if r.has_min_snapshots_to_keep else policy.keep_last
        if min_keep < 1:
            min_keep = 1
        var cut = (
            policy.now_ms - r.max_snapshot_age_ms
        ) if r.has_max_snapshot_age_ms else policy.older_than_ms
        var id = r.snapshot_id
        var kept = 0
        var keeping = True
        while id in at:
            reached[id] = True
            ref s = metadata.snapshots[at[id]]
            if keeping and (
                kept < min_keep
                or (cut != NO_AGE_LIMIT and s.timestamp_ms >= cut)
            ):
                keep[id] = True
                kept += 1
            else:
                keeping = False
            if not s.has_parent:
                break
            id = s.parent_snapshot_id

    # A snapshot no ref reaches (rolled back, staged) survives on age alone.
    for k in range(len(metadata.snapshots)):
        ref s = metadata.snapshots[k]
        if s.snapshot_id in reached:
            continue
        if (
            policy.older_than_ms != NO_AGE_LIMIT
            and s.timestamp_ms >= policy.older_than_ms
        ):
            keep[s.snapshot_id] = True

    var order = List[Int]()
    for k in range(len(metadata.snapshots)):
        if metadata.snapshots[k].snapshot_id not in keep:
            order.append(k)
    _sort_by_time(metadata, order)
    for i in range(len(order)):
        plan.expired.append(metadata.snapshots[order[i]].snapshot_id)
    return plan^


def _sort_by_time(metadata: TableMetadata, mut order: List[Int]):
    """Oldest first; an insertion sort, as the lists are short and mostly
    in order already."""
    for i in range(1, len(order)):
        var j = i
        while (
            j > 0
            and metadata.snapshots[order[j]].timestamp_ms
            < metadata.snapshots[order[j - 1]].timestamp_ms
        ):
            order.swap_elements(j, j - 1)
            j -= 1


def choose_expired(
    metadata: TableMetadata, older_than_ms: Int64, keep_last: Int
) raises -> List[Int64]:
    """The snapshot ids an expiry would remove, oldest first.

    A ref's head is never a candidate, and each branch keeps at least
    `keep_last` of its ancestors. A negative `older_than_ms` is no age limit.
    """
    var plan = plan_expiry(metadata, ExpirePolicy(older_than_ms, keep_last))
    return plan.expired.copy()


def _is_pure_add(mf: ManifestFile) -> Bool:
    """Whether a manifest can only hold files its own snapshot added."""
    return (
        mf.has_existing_files_count
        and mf.existing_files_count == 0
        and mf.has_deleted_files_count
        and mf.deleted_files_count == 0
    )


def _read_many(
    io: FileIO, paths: List[String], start: Int, end: Int
) raises -> List[List[UInt8]]:
    var some = List[String](capacity=end - start)
    for k in range(start, end):
        some.append(paths[k])
    return io.read_all_many(some)


def _chain(metadata: TableMetadata, at: Dict[Int64, Int]) raises -> List[Int64]:
    """The current snapshot's ancestors, oldest first, ending with it."""
    var out = List[Int64]()
    if not metadata.has_current_snapshot:
        return out^
    var id = metadata.current_snapshot_id
    while id in at and len(out) <= len(metadata.snapshots):
        out.append(id)
        ref s = metadata.snapshots[at[id]]
        if not s.has_parent:
            break
        id = s.parent_snapshot_id
    out.reverse()
    return out^


def _linear_history(
    metadata: TableMetadata,
    chain: List[Int64],
    retained: Dict[Int64, Bool],
) -> Bool:
    """Whether every snapshot is an ancestor of the current one (or it), and
    the retained ones are the newest of them: one line of history, cut."""
    if len(chain) != len(metadata.snapshots):
        return False
    var seen_retained = False
    for k in range(len(chain)):
        if chain[k] in retained:
            seen_retained = True
        elif seen_retained:
            return False
    return True


def expire_snapshots(
    io: FileIO,
    metadata: TableMetadata,
    older_than_ms: Int64 = -1,
    keep_last: Int = 1,
    dry_run: Bool = False,
) raises -> Tuple[TableMetadata, ExpireResult]:
    """`expire_with_policy` with an age cut (negative: none) and a floor."""
    return expire_with_policy(
        io, metadata, ExpirePolicy(older_than_ms, keep_last), dry_run
    )


def expire_with_policy(
    io: FileIO,
    metadata: TableMetadata,
    policy: ExpirePolicy,
    dry_run: Bool = False,
) raises -> Tuple[TableMetadata, ExpireResult]:
    """Decide the expiry and the files it frees.

    Returns the pruned metadata and a report. The caller commits the metadata
    — and must do so *before* the files go (`delete_expired_files`), which is
    why nothing is deleted here.
    """
    var result = ExpireResult()
    result.dry_run = dry_run
    var plan = plan_expiry(metadata, policy)
    result.expired = plan.expired.copy()
    result.removed_refs = plan.removed_refs.copy()
    if len(result.expired) == 0 and len(result.removed_refs) == 0:
        return (metadata.copy(), result^)

    var gone = Dict[Int64, Bool]()
    for k in range(len(result.expired)):
        gone[result.expired[k]] = True
    var out = metadata.copy()
    out.snapshots = List[Snapshot]()
    var retained = Dict[Int64, Bool]()
    for k in range(len(metadata.snapshots)):
        ref s = metadata.snapshots[k]
        if s.snapshot_id in gone:
            continue
        out.snapshots.append(s.copy())
        result.retained.append(s.snapshot_id)
        retained[s.snapshot_id] = True
    var dropped_refs = Dict[String, Bool]()
    for k in range(len(plan.removed_refs)):
        dropped_refs[plan.removed_refs[k]] = True
    out.refs = List[SnapshotRef]()
    for k in range(len(metadata.refs)):
        if metadata.refs[k].name not in dropped_refs:
            out.refs.append(metadata.refs[k].copy())

    # The snapshot log has to stay a history: an entry for a snapshot that
    # is gone takes every entry before it along (Java does the same).
    var log = List[SnapshotLogEntry]()
    for k in range(len(metadata.snapshot_log)):
        ref e = metadata.snapshot_log[k]
        if e.snapshot_id in retained:
            log.append(e.copy())
        else:
            log = List[SnapshotLogEntry]()
    out.snapshot_log = log^

    # Statistics belong to a snapshot and go with it.
    out.statistics = List[StatisticsFile]()
    var kept_stats = Dict[String, Bool]()
    for k in range(len(metadata.statistics)):
        ref st = metadata.statistics[k]
        if st.snapshot_id in retained:
            out.statistics.append(st.copy())
            kept_stats[st.statistics_path] = True
    out.partition_statistics = List[PartitionStatisticsFile]()
    for k in range(len(metadata.partition_statistics)):
        ref st = metadata.partition_statistics[k]
        if st.snapshot_id in retained:
            out.partition_statistics.append(st.copy())
            kept_stats[st.statistics_path] = True
    for k in range(len(metadata.statistics)):
        ref st = metadata.statistics[k]
        if st.statistics_path not in kept_stats:
            kept_stats[st.statistics_path] = True
            result.deleted_statistics.append(st.statistics_path)
    for k in range(len(metadata.partition_statistics)):
        ref st = metadata.partition_statistics[k]
        if st.statistics_path not in kept_stats:
            kept_stats[st.statistics_path] = True
            result.deleted_statistics.append(st.statistics_path)
    out.last_updated_ms = now_ms()

    if len(result.expired) > 0:
        _files_freed(io, metadata, retained, result)
    return (out^, result^)


def _files_freed(
    io: FileIO,
    metadata: TableMetadata,
    retained: Dict[Int64, Bool],
    mut result: ExpireResult,
) raises:
    """Fill `deleted_files` and `deleted_manifests` (see the module doc)."""
    var at = _index_by_id(metadata)
    var chain = _chain(metadata, at)
    result.reachable = not _linear_history(metadata, chain, retained)
    var position = Dict[Int64, Int]()
    for k in range(len(chain)):
        position[chain[k]] = k

    # ── every manifest list, once ──────────────────────────────────────────
    var lists = List[String]()
    var list_retained = List[Bool]()
    var kept_lists = Dict[String, Bool]()
    var kept = Dict[String, ManifestFile]()
    var expired_only = Dict[String, ManifestFile]()
    var inline = Dict[String, Bool]()
    var list_position = List[Int]()
    # Each manifest's newest lister, by its position on the chain.
    var last_listed = Dict[String, Int]()
    for k in range(len(metadata.snapshots)):
        ref s = metadata.snapshots[k]
        var keep = s.snapshot_id in retained
        if s.manifest_list == "":
            # v1 inline manifests: no ManifestFile to open them with, so
            # they are treated as needed and never deleted.
            for j in range(len(s.manifests)):
                inline[s.manifests[j]] = True
            continue
        lists.append(s.manifest_list)
        list_retained.append(keep)
        list_position.append(position.get(s.snapshot_id, -1))
        if keep:
            kept_lists[s.manifest_list] = True
    var cache = ManifestCache()
    var start = 0
    while start < len(lists):
        var end = min(start + _BATCH, len(lists))
        var blobs = _read_many(io, lists, start, end)
        result.manifest_lists_read += end - start
        for i in range(end - start):
            var mfs = read_manifest_list_bytes_cached(blobs[i].copy(), cache)
            var at_pos = list_position[start + i]
            for j in range(len(mfs)):
                if at_pos > last_listed.get(mfs[j].manifest_path, -1):
                    last_listed[mfs[j].manifest_path] = at_pos
                if list_retained[start + i]:
                    if mfs[j].manifest_path in expired_only:
                        _ = expired_only.pop(mfs[j].manifest_path)
                    kept[mfs[j].manifest_path] = mfs[j].copy()
                elif (
                    mfs[j].manifest_path not in kept
                    and mfs[j].manifest_path not in inline
                ):
                    expired_only[mfs[j].manifest_path] = mfs[j].copy()
        start = end
    var seen_lists = Dict[String, Bool]()
    for k in range(len(lists)):
        if not list_retained[k] and lists[k] not in kept_lists:
            if lists[k] not in seen_lists:
                seen_lists[lists[k]] = True
                result.deleted_manifests.append(lists[k])

    # ── the manifests worth opening ────────────────────────────────────────
    var paths = List[String]()
    var infos = List[ManifestFile]()
    var is_kept = List[Bool]()
    for e in expired_only.items():
        result.deleted_manifests.append(e.key)
        paths.append(e.key)
        infos.append(e.value.copy())
        is_kept.append(False)
    if len(paths) == 0 and not result.reachable:
        return
    # A file of an unreferenced manifest that is still live somewhere was
    # carried, as an EXISTING entry, into a manifest written by the snapshot
    # that dropped that manifest from the list (a rewrite, a merge, a
    # compaction), and maybe carried again from there by the snapshot that
    # dropped *that* one. So on one line of history only the manifests those
    # snapshots added can hold a candidate live; the rest are not opened.
    var droppers = Dict[Int64, Bool]()
    for e in expired_only.items():
        var next = last_listed.get(e.key, -1) + 1
        if next > 0 and next < len(chain):
            droppers[chain[next]] = True
    for e in kept.items():
        if result.reachable or (
            e.value.added_snapshot_id in droppers and not _is_pure_add(e.value)
        ):
            paths.append(e.key)
            infos.append(e.value.copy())
            is_kept.append(True)

    var live = Dict[String, Bool]()
    var candidates = List[String]()
    var is_candidate = Dict[String, Bool]()
    start = 0
    while start < len(paths):
        var end = min(start + _BATCH, len(paths))
        var blobs = _read_many(io, paths, start, end)
        result.manifests_read += end - start
        for i in range(end - start):
            var m = read_manifest_bytes_cached(
                blobs[i].copy(), infos[start + i], cache
            )
            var mine = is_kept[start + i]
            for j in range(len(m.entries)):
                ref entry = m.entries[j]
                ref path = entry.data_file.file_path
                if mine and entry.status != STATUS_DELETED:
                    live[path] = True
                elif not mine or result.reachable:
                    if path not in is_candidate:
                        is_candidate[path] = True
                        candidates.append(path)
        start = end
    for k in range(len(candidates)):
        if candidates[k] not in live:
            result.deleted_files.append(candidates[k])


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
    for k in range(len(result.deleted_statistics)):
        try:
            io.delete(result.deleted_statistics[k])
            n += 1
        except:
            pass
    return n
