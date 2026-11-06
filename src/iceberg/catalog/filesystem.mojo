"""Filesystem tables: a table is a directory, no catalog service involved.

    <table>/metadata/version-hint.text     -> "4"
    <table>/metadata/v4.metadata.json      (or 00004-<uuid>.metadata.json)
    <table>/metadata/snap-<id>-<n>-<uuid>.avro
    <table>/data/...

`version-hint.text` is the fast path. When it is absent — the layout every
catalog-backed writer produces, including the fixtures in this repo — the
newest metadata file is found by listing the directory and taking the highest
version prefix, which is the same rule the reference implementations use.

Gzipped metadata (`*.gz.metadata.json`) is supported: gzip is a nine-byte
header, a raw deflate stream and an eight-byte trailer, and the header and
trailer are parsed here — `deflate.mojo` decodes the stream between them.
"""

from std.collections import Dict

from deflate import inflate
from parquet import RecordBatch

from ..append import (
    AppendResult,
    metadata_file_name,
    next_metadata_version,
    prepare_append,
    write_and_prepare_append,
)
from ..delete import (
    MODE_COPY_ON_WRITE,
    MODE_MERGE_ON_READ,
    delete_mode_of,
    deleted_row_count,
    plan_row_deletes,
    prepare_delete,
    prepare_delete_from,
    prepare_dynamic_partition_overwrite,
    prepare_equality_delete,
    prepare_overwrite,
)
from ..maintain import (
    ExpirePolicy,
    ExpireResult,
    delete_expired_files,
    expire_with_policy,
)
from ..metadata_log import (
    delete_after_commit,
    delete_metadata_files,
    record_previous,
    trim_metadata_log,
)
from ..read import ScanOptions
from ..io import FileIO, basename, dirname, join_path, strip_scheme
from ..json import substr
from ..manifest import DataFile
from ..metadata import (
    INITIAL_SORT_ORDER_ID,
    INITIAL_SPEC_ID,
    MAIN_BRANCH,
    SUPPORTED_FORMAT_VERSION,
    TableMetadata,
)
from ..scan import TableScan
from ..schema import Schema
from ..transforms import PartitionSpec, SortOrder
from ..update_schema import UpdateSchema, prepare_schema_update
from ..util import now_ms, uuid4, zero_pad
from ..write import WriteOptions, write_data_files


comptime VERSION_HINT = String("version-hint.text")
comptime METADATA_DIR = String("metadata")
comptime METADATA_SUFFIX = String(".metadata.json")
comptime GZ_METADATA_SUFFIX = String(".gz.metadata.json")


def gunzip(data: List[UInt8]) raises -> List[UInt8]:
    """Decompress a gzip member. Only the deflate method is defined by RFC 1952.
    """
    if len(data) < 18 or data[0] != 0x1F or data[1] != 0x8B:
        raise Error("iceberg: not a gzip stream")
    if data[2] != 8:
        raise Error(
            "iceberg: gzip compression method "
            + String(Int(data[2]))
            + " is not deflate"
        )
    var flg = data[3]
    var p = 10
    if (flg & 0x04) != 0:  # FEXTRA
        var xlen = Int(data[p]) | (Int(data[p + 1]) << 8)
        p += 2 + xlen
    if (flg & 0x08) != 0:  # FNAME
        while p < len(data) and data[p] != 0:
            p += 1
        p += 1
    if (flg & 0x10) != 0:  # FCOMMENT
        while p < len(data) and data[p] != 0:
            p += 1
        p += 1
    if (flg & 0x02) != 0:  # FHCRC
        p += 2
    if p >= len(data) - 8:
        raise Error("iceberg: truncated gzip stream")
    return inflate(Span(data)[p : len(data) - 8])


def read_metadata_file(io: FileIO, location: String) raises -> TableMetadata:
    """Read one metadata file, transparently un-gzipping a `*.gz.metadata.json`.
    """
    var text: String
    if location.endswith(".gz") or location.endswith(GZ_METADATA_SUFFIX):
        var raw = io.read_all(location)
        var plain = gunzip(raw)
        text = String(StringSlice(unsafe_from_utf8=Span(plain)))
    else:
        text = io.read_text(location)
    var m = TableMetadata.parse(text)
    m.metadata_file_location = location
    return m^


def read_version_hint(io: FileIO, metadata_dir: String) raises -> Int:
    """The integer in `version-hint.text`, or -1 when there is no hint."""
    var path = join_path(metadata_dir, VERSION_HINT)
    var text: String
    try:
        text = io.read_text(path)
    except:
        return -1
    var digits = String("")
    var b = text.as_bytes()
    for k in range(len(b)):
        if b[k] >= 48 and b[k] <= 57:
            digits += String(StringSlice(unsafe_from_utf8=Span(b)[k : k + 1]))
        elif digits != "":
            break
    if digits == "":
        return -1
    return Int(digits)


def _version_of(name: String) raises -> Int:
    """The leading version number of a metadata file name, or -1.

    Both spellings occur: `v4.metadata.json` from filesystem tables and
    `00004-<uuid>.metadata.json` from catalog-backed writers.
    """
    if not name.endswith(METADATA_SUFFIX):
        return -1
    var s = name
    if s.startswith("v"):
        s = substr(s, 1, s.byte_length())
    var digits = String("")
    var b = s.as_bytes()
    for k in range(len(b)):
        if b[k] >= 48 and b[k] <= 57:
            digits += String(StringSlice(unsafe_from_utf8=Span(b)[k : k + 1]))
        else:
            break
    if digits == "":
        return -1
    return Int(digits)


def find_latest_metadata(io: FileIO, table_location: String) raises -> String:
    """Locate the current metadata file for a filesystem table.

    `table_location` may be the table directory, its `metadata/` directory, or
    the metadata file itself.
    """
    var loc = table_location
    while loc.endswith("/"):
        loc = substr(loc, 0, loc.byte_length() - 1)
    if loc.endswith(METADATA_SUFFIX):
        return loc
    var metadata_dir = loc
    if basename(loc) != METADATA_DIR:
        var nested = join_path(loc, METADATA_DIR)
        if io.exists(join_path(nested, VERSION_HINT)) or _has_metadata(
            io, nested
        ):
            metadata_dir = nested

    var hint = read_version_hint(io, metadata_dir)
    if hint >= 0:
        var found = latest_from_version(io, metadata_dir, hint)
        if found != "":
            return found^

    # Highest version wins; ties are broken by `last-updated-ms`.
    #
    # Ties are not hypothetical. A metadata directory can hold files from more
    # than one table lifetime — drop and recreate a table under the same name
    # and the new `00000-<uuid>.metadata.json` sits beside the old one, with
    # every later version colliding too. The version prefix and the file name
    # cannot separate them (the uuid is random), but `last-updated-ms` can, and
    # it is the field the writer stamps immediately before writing.
    var names = io.list_names(metadata_dir)
    var best_v = -1
    var candidates = List[String]()
    for k in range(len(names)):
        var name = String(names[k])
        var v = _version_of(name)
        if v < 0:
            continue
        if v > best_v:
            best_v = v
            candidates = List[String]()
        if v == best_v:
            candidates.append(name^)
    if len(candidates) == 0:
        raise Error("iceberg: no *.metadata.json under '" + metadata_dir + "'")
    if len(candidates) == 1:
        return join_path(metadata_dir, candidates[0])
    _sort_names(candidates)
    var best = join_path(metadata_dir, candidates[0])
    var best_updated: Int64 = -1
    for k in range(len(candidates)):
        var path = join_path(metadata_dir, candidates[k])
        try:
            var m = read_metadata_file(io, path)
            if m.last_updated_ms > best_updated:
                best_updated = m.last_updated_ms
                best = path^
        except:
            # An unreadable candidate is not a reason to fail discovery.
            continue
    return best^


def _hadoop_names(version: Int) -> List[String]:
    return [
        "v" + String(version) + METADATA_SUFFIX,
        String(version) + METADATA_SUFFIX,
        "v" + String(version) + GZ_METADATA_SUFFIX,
    ]


def metadata_files_of_version(
    io: FileIO, metadata_dir: String, version: Int, hadoop: Bool = False
) raises -> List[String]:
    """The metadata files of one version, as paths: `<V>-<uuid>` names found
    with one prefix listing, and with `hadoop`, `v<V>.metadata.json` and its
    spellings checked by name too.

    One request (plus three with `hadoop`) however many versions the
    directory holds, which is what keeps a commit's cost flat as a table's
    history grows.
    """
    var out = List[String]()
    var names = io.list_names_with_prefix(
        metadata_dir, zero_pad(version, 5) + "-"
    )
    for k in range(len(names)):
        if _metadata_version_of(names[k]) == version:
            out.append(join_path(metadata_dir, names[k]))
    if hadoop:
        var spelled = _hadoop_names(version)
        for k in range(len(spelled)):
            var path = join_path(metadata_dir, spelled[k])
            if io.exists(path):
                out.append(path^)
    return out^


comptime CLAIM_SUFFIX = String(".claim")
comptime CLAIM_STALE_MS: Int64 = 60_000
"""How old a claim on a metadata version is when its writer is taken to
have died before finishing its commit."""


def claim_file_name(version: Int, at_ms: Int64) -> String:
    """`<V>-<uuid>.<ms>.claim`: a writer's claim on metadata version `V`,
    made at `at_ms`. Readers ignore it: it isn't a `*.metadata.json`."""
    return (
        zero_pad(version, 5)
        + "-"
        + uuid4()
        + "."
        + String(at_ms)
        + CLAIM_SUFFIX
    )


def _claim_ms(name: String) -> Int64:
    """When the claim `name` was made, or -1 if it isn't a claim."""
    if not name.endswith(CLAIM_SUFFIX):
        return -1
    var stem = substr(name, 0, name.byte_length() - CLAIM_SUFFIX.byte_length())
    var dot = stem.rfind(".")
    if dot < 0:
        return -1
    try:
        return Int64(Int(substr(stem, dot + 1, stem.byte_length())))
    except:
        return -1


def rivals_of_version(
    io: FileIO,
    metadata_dir: String,
    version: Int,
    hadoop: Bool,
    mine: String,
    now: Int64,
) raises -> Int:
    """What stands in the way of the claim `mine` on `version`: the other
    claims on it and its metadata files, found with one prefix listing (and
    with `hadoop`, `v<V>.metadata.json` and its spellings checked by name).
    Claims older than `CLAIM_STALE_MS` don't count, and are removed."""
    var n = 0
    var names = io.list_names_with_prefix(
        metadata_dir, zero_pad(version, 5) + "-"
    )
    for k in range(len(names)):
        if names[k] == mine:
            continue
        if _metadata_version_of(names[k]) == version:
            n += 1
            continue
        var at = _claim_ms(names[k])
        if at < 0:
            continue
        if now - at > CLAIM_STALE_MS:
            _remove_claim(io, join_path(metadata_dir, names[k]))
            continue
        n += 1
    if hadoop:
        var spelled = _hadoop_names(version)
        for k in range(len(spelled)):
            if io.exists(join_path(metadata_dir, spelled[k])):
                n += 1
    return n


def _remove_claim(io: FileIO, path: String):
    """Deletes a claim; one left behind is ignored once it's stale."""
    try:
        io.delete(path)
    except:
        pass


def _newest_of(io: FileIO, var paths: List[String]) -> String:
    """One version's files: the one with the latest `last-updated-ms`."""
    if len(paths) == 0:
        return String("")
    if len(paths) == 1:
        return paths[0]
    _sort_names(paths)
    var best = paths[0]
    var best_updated: Int64 = -1
    for k in range(len(paths)):
        try:
            var m = read_metadata_file(io, paths[k])
            if m.last_updated_ms > best_updated:
                best_updated = m.last_updated_ms
                best = paths[k]
        except:
            continue
    return best^


def _is_hadoop_name(name: String) -> Bool:
    return name.startswith("v") or name.find("-") < 0


def latest_from_version(
    io: FileIO, metadata_dir: String, version: Int
) raises -> String:
    """The newest metadata file at or after `version`, or "" when that
    version has no file.

    Versions are dense — every commit writes the one after the one it read —
    so the newest is found by stepping forward from a known one until a
    version is missing: a request or two per step, never a listing of the
    whole directory.
    """
    var found = _newest_of(
        io, metadata_files_of_version(io, metadata_dir, version, True)
    )
    if found == "":
        return found^
    var hadoop = _is_hadoop_name(basename(found))
    var v = version + 1
    while True:
        var next = _newest_of(
            io, metadata_files_of_version(io, metadata_dir, v, hadoop)
        )
        if next == "":
            return found^
        found = next^
        v += 1


def _metadata_version_of(name: String) -> Int:
    """The version prefix of a metadata file name, or -1."""
    try:
        return _version_of(name)
    except:
        return -1


def _sort_names(mut l: List[String]):
    """Sort so that an unreadable-candidate fallback stays deterministic."""
    for i in range(1, len(l)):
        var j = i
        while j > 0 and l[j] < l[j - 1]:
            l.swap_elements(j, j - 1)
            j -= 1


def _has_metadata(io: FileIO, dir: String) -> Bool:
    try:
        var names = io.list_names(dir)
        for k in range(len(names)):
            if names[k].endswith(METADATA_SUFFIX):
                return True
        return False
    except:
        return False


struct Table(Copyable, Movable):
    """A loaded table: its metadata, where that metadata came from, and the
    `FileIO` its locations resolve through."""

    var metadata: TableMetadata
    var metadata_location: String
    var io: FileIO
    var name: String

    def __init__(
        out self,
        var metadata: TableMetadata,
        var metadata_location: String,
        var io: FileIO,
        var name: String,
    ):
        self.metadata = metadata^
        self.metadata_location = metadata_location^
        self.io = io^
        self.name = name^

    @staticmethod
    def load(location: String, var io: FileIO) raises -> Table:
        """Load a filesystem table from a directory or a metadata file path."""
        var path = find_latest_metadata(io, location)
        var m = read_metadata_file(io, path)
        return Table(m^, path^, io^, basename(dirname(dirname(path))))

    @staticmethod
    def load_local(location: String) raises -> Table:
        return Table.load(location, FileIO.local())

    def scan(self) -> TableScan:
        return TableScan(self.metadata.copy(), self.io.copy())

    def refresh(mut self) raises:
        """Reload, stepping forward from the version this table already has
        (or from `version-hint.text`, when that is further on): no listing
        of the metadata directory."""
        var dir = dirname(self.metadata_location)
        var mine = _metadata_version_of(basename(self.metadata_location))
        var hint = read_version_hint(self.io, dir)
        var path = String("")
        if hint > mine:
            path = latest_from_version(self.io, dir, hint)
        if path == "" and mine >= 0:
            path = latest_from_version(self.io, dir, mine)
        if path == "":
            path = find_latest_metadata(self.io, dir)
        self.metadata = read_metadata_file(self.io, path)
        self.metadata_location = path^

    # ── writing ────────────────────────────────────────────────────────────
    def new_append(self) -> AppendFiles:
        """Start a fast-append transaction against this table."""
        return AppendFiles(self.copy())

    def append(mut self, batches: List[RecordBatch]) raises -> Int64:
        """Write `batches` and commit them as one snapshot. Rows appended."""
        var tx = self.new_append()
        tx.add_batches(batches)
        var n = tx.commit()
        self.refresh()
        return n

    def delete_where(
        mut self, filter_dsl: String, mode: String = String("")
    ) raises -> Int64:
        """`DELETE FROM t WHERE <filter>`. Returns the rows removed.

        The strategy comes from `write.delete.mode` unless `mode` overrides
        it: `copy-on-write` (the default) rewrites the affected data files,
        `merge-on-read` writes deletion vectors on v3 and position delete
        files on v2.
        """
        var attempt = 0
        while True:
            var scan = self.scan()
            var plans = plan_row_deletes(scan, filter_dsl, ScanOptions())
            var rows = deleted_row_count(plans)
            if rows == 0:
                return 0
            var result = prepare_delete_from(
                self.io, self.metadata, plans, mode
            )
            try:
                self.commit(result)
                return rows
            except e:
                attempt += 1
                if attempt > 4:
                    raise e
                self.refresh()

    def delete_by_equality(
        mut self, rows: RecordBatch, equality_ids: List[Int]
    ) raises -> Int64:
        """Write one equality delete file. Returns the delete rows written.

        The rows are values, not positions: every table row whose
        `equality_ids` columns match one of them is gone from then on. v2 and
        unpartitioned only — see `iceberg.delete.write_equality_deletes`.
        """
        var attempt = 0
        while True:
            var result = prepare_equality_delete(
                self.io, self.metadata, rows, equality_ids
            )
            try:
                self.commit(result)
                return Int64(rows.num_rows)
            except e:
                attempt += 1
                if attempt > 4:
                    raise e
                self.refresh()

    def overwrite(
        mut self,
        batches: List[RecordBatch],
        filter_dsl: String = String('["true"]'),
    ) raises -> Int64:
        """Delete every row the filter matches and add `batches`, in one
        `overwrite` snapshot. With no filter this replaces the whole table.
        Returns the rows added."""
        var rows: Int64 = 0
        for k in range(len(batches)):
            rows += Int64(batches[k].num_rows)
        var attempt = 0
        while True:
            var result = prepare_overwrite(
                self.io, self.metadata, batches, filter_dsl
            )
            try:
                self.commit(result)
                return rows
            except e:
                attempt += 1
                if attempt > 4:
                    raise e
                self.refresh()

    def dynamic_partition_overwrite(
        mut self, batches: List[RecordBatch]
    ) raises -> Int64:
        """Replace exactly the partitions the new rows land in. Rows added."""
        var rows: Int64 = 0
        for k in range(len(batches)):
            rows += Int64(batches[k].num_rows)
        var attempt = 0
        while True:
            var result = prepare_dynamic_partition_overwrite(
                self.io, self.metadata, batches
            )
            try:
                self.commit(result)
                return rows
            except e:
                attempt += 1
                if attempt > 4:
                    raise e
                self.refresh()

    def expire_snapshots(
        mut self,
        older_than_ms: Int64 = -1,
        keep_last: Int = 1,
        dry_run: Bool = False,
    ) raises -> ExpireResult:
        """Drop old snapshots and the files nothing points at any more.

        `older_than_ms` is an age cut (negative: none) and `keep_last` the
        fewest ancestors each branch keeps; refs keep their own settings.
        """
        return self.expire(ExpirePolicy(older_than_ms, keep_last), dry_run)

    def expire_by_properties(
        mut self, dry_run: Bool = False
    ) raises -> ExpireResult:
        """Expire by the table's own `history.expire.*` properties (Java's
        defaults where unset: five days, at least one snapshot)."""
        return self.expire(ExpirePolicy.from_properties(self.metadata), dry_run)

    def expire(
        mut self, policy: ExpirePolicy, dry_run: Bool = False
    ) raises -> ExpireResult:
        """Expire by `policy`.

        The pruned metadata is committed **first** and the files go after, so
        an interruption leaves orphans rather than a snapshot with holes in
        it. A concurrent commit makes this reload and decide again: the files
        to delete are only ever decided from the metadata that was committed.
        `dry_run` stops after the decision and returns the list.
        """
        var attempt = 0
        while True:
            var decided = expire_with_policy(
                self.io, self.metadata, policy, dry_run
            )
            var result = decided[1].copy()
            if dry_run or len(result.expired) + len(result.removed_refs) == 0:
                return result^
            try:
                self.commit_metadata(decided[0].copy())
            except e:
                attempt += 1
                if attempt > 4:
                    raise e
                self.refresh()
                continue
            _ = delete_expired_files(self.io, result)
            return result^

    def update_schema(mut self, update: UpdateSchema) raises -> Schema:
        """Commit schema changes; returns the new current schema.

        No data file is touched: one new metadata file with the new schema
        appended and made current. A concurrent commit makes this reload and
        apply the same changes to whatever schema is current then, so a
        column another writer added in the meantime is kept, and a clash
        (both adding `note`) raises instead of committing twice.
        """
        var attempt = 0
        while True:
            var c = prepare_schema_update(self.metadata, update)
            if not c.changed:
                return c.schema.copy()
            try:
                self.commit_metadata(c.metadata.copy())
            except e:
                attempt += 1
                if attempt > 4:
                    raise e
                self.refresh()
                continue
            return c.schema.copy()

    def commit(mut self, result: AppendResult) raises:
        """Persist a prepared commit as a new `<V>-<uuid>.metadata.json`."""
        self.commit_metadata(result.metadata.copy())

    def commit_metadata(mut self, var metadata: TableMetadata) raises:
        """Persist table metadata as a new `<V>-<uuid>.metadata.json`.

        Writers racing on version `V` settle it before any of them writes a
        metadata file: each writes a claim (`<V>-<uuid>.<ms>.claim`), lists
        the version's files, and goes on only if it finds no other claim and
        no metadata file of `V`; otherwise it removes its claim and raises,
        having written nothing a reader could load. Of two claims, the later
        one's listing sees the earlier, so at most one writer goes on; it
        writes its metadata file and then removes its claim.

        Readers never see a loser's metadata. They did when the metadata
        file was itself the claim: a reader could load it in the moment
        before its writer saw the clash and deleted it, and commit the next
        version on top of it, so a commit its writer retried landed twice,
        or one that won was lost (orca.mojo#164).

        A claim older than a minute is from a writer that died before
        finishing and is ignored (and removed). This is still a
        check-then-act protocol: it needs a listing that sees every write
        before it (a local filesystem, or S3 since 2020), and is unsafe over
        an object store that lacks one, as every filesystem-table writer is.
        """
        var dir = dirname(self.metadata_location)
        # The version we are about to write is derived from the file we read,
        # so that file has to still be the newest one: nobody may have written
        # the next version yet. This only saves a stale writer its claim; the
        # claim is what detects a concurrent commit.
        #
        # It used to list the whole metadata directory, twice per commit,
        # which made every commit slower than the last. Looking for one
        # version is one prefix listing, and `version-hint.text` covers the
        # case the listing cannot: a writer so far behind that the version
        # after its own was deleted by `delete-after-commit`.
        var version = next_metadata_version(metadata)
        var hadoop = _is_hadoop_name(basename(self.metadata_location))
        var ahead = metadata_files_of_version(self.io, dir, version, hadoop)
        var hint = read_version_hint(self.io, dir)
        if len(ahead) > 0 or hint >= version:
            raise Error(
                "iceberg: another writer committed version "
                + String(version)
                + " while this commit was based on "
                + basename(self.metadata_location)
            )
        record_previous(
            metadata, self.metadata_location, self.metadata.last_updated_ms
        )
        var forgotten = trim_metadata_log(metadata)
        var path = join_path(dir, metadata_file_name(version))
        var doc = metadata.to_json()
        var claim = claim_file_name(version, now_ms())
        var claim_path = join_path(dir, claim)
        self.io.write_new(claim_path, String("").as_bytes())
        var rivals: Int
        try:
            rivals = rivals_of_version(
                self.io, dir, version, hadoop, claim, now_ms()
            )
        except e:
            _remove_claim(self.io, claim_path)
            raise e^
        if rivals > 0:
            _remove_claim(self.io, claim_path)
            raise Error(
                "iceberg: "
                + String(rivals + 1)
                + " writers claimed version "
                + String(version)
                + "; reload and retry"
            )
        try:
            self.io.write_new(path, doc.as_bytes())
        except e:
            _remove_claim(self.io, claim_path)
            raise e^
        _remove_claim(self.io, claim_path)
        # `version-hint.text` is advisory: a reader that cannot find the file
        # it names falls back to listing, which is why it is written after.
        try:
            self.io.write_text(
                join_path(dir, VERSION_HINT), String(version) + "\n"
            )
        except:
            pass
        # Only now, with the new metadata in place, can what fell off the
        # metadata log go.
        if len(forgotten) > 0 and delete_after_commit(metadata):
            _ = delete_metadata_files(self.io, forgotten)
        self.metadata = metadata^
        self.metadata.metadata_file_location = path
        self.metadata_location = path^


struct AppendFiles(Movable):
    """A fast-append transaction: collect batches or data files, then commit.

    ```mojo
    var tx = table.new_append()
    tx.add(batch)
    _ = tx.commit()
    ```
    """

    var table: Table
    var batches: List[RecordBatch]
    var files: List[DataFile]
    var retries: Int
    """How many times to reload and retry when another writer got there first.
    """

    def __init__(out self, var table: Table):
        self.table = table^
        self.batches = []
        self.files = []
        self.retries = 4

    def __init__(out self, *, deinit move: Self):
        self.table = move.table^
        self.batches = move.batches^
        self.files = move.files^
        self.retries = move.retries

    def add(mut self, batch: RecordBatch):
        self.batches.append(batch.copy())

    def add_batches(mut self, batches: List[RecordBatch]):
        for k in range(len(batches)):
            self.batches.append(batches[k].copy())

    def add_data_file(mut self, file: DataFile):
        """Append a Parquet file that is already written and already in place.
        """
        self.files.append(file.copy())

    def commit(mut self) raises -> Int64:
        """Write the data files, then commit, reloading and retrying if
        another writer committed in between. Returns the rows appended."""
        var options = WriteOptions.from_properties(
            self.table.metadata.properties
        )
        var written = self.files.copy()
        if len(self.batches) > 0:
            var more = write_data_files(
                self.table.io,
                self.table.metadata.location,
                self.batches,
                self.table.metadata.schema(),
                self.table.metadata.spec(),
                self.table.metadata.default_sort_order_id,
                options,
            )
            for k in range(len(more)):
                written.append(more[k].copy())
        if len(written) == 0:
            return 0
        var rows: Int64 = 0
        for k in range(len(written)):
            rows += written[k].record_count
        var attempt = 0
        while True:
            var result = prepare_append(
                self.table.io,
                self.table.metadata,
                written.copy(),
                Dict[String, String](),
            )
            try:
                self.table.commit(result)
                return rows
            except e:
                attempt += 1
                if attempt > self.retries:
                    raise e
                # Somebody else took this version. Reload and try again — the
                # data files are already written and stay valid. The manifest
                # and manifest list of the failed attempt are orphaned, which
                # is what every Iceberg implementation does; nothing points at
                # them, and an orphan-file cleanup is a separate operation.
                self.table.refresh()


struct FilesystemCatalog(Copyable, Movable):
    """Tables addressed by path under a warehouse root: `<warehouse>/<ns>/<tbl>`.
    """

    var warehouse: String
    var io: FileIO

    def __init__(out self, var warehouse: String, var io: FileIO):
        self.warehouse = warehouse^
        self.io = io^

    @staticmethod
    def local(var warehouse: String) -> Self:
        return Self(warehouse^, FileIO.local())

    def table_location(self, namespace: String, name: String) -> String:
        if namespace == "":
            return join_path(self.warehouse, name)
        return join_path(join_path(self.warehouse, namespace), name)

    def load_table(self, namespace: String, name: String) raises -> Table:
        var loc = self.table_location(namespace, name)
        var t = Table.load(loc, self.io.copy())
        t.name = name if namespace == "" else namespace + "." + name
        return t^

    def table_exists(self, namespace: String, name: String) -> Bool:
        try:
            _ = find_latest_metadata(
                self.io, self.table_location(namespace, name)
            )
            return True
        except:
            return False

    def list_tables(self, namespace: String) raises -> List[String]:
        """Directories under the namespace that look like Iceberg tables."""
        var out = List[String]()
        var base = self.warehouse if namespace == "" else join_path(
            self.warehouse, namespace
        )
        var names = self.io.list_names(base)
        for k in range(len(names)):
            var n = names[k].copy()
            if _has_metadata(
                self.io, join_path(join_path(base, n), METADATA_DIR)
            ):
                out.append(n^)
        return out^

    def create_table(
        self,
        namespace: String,
        name: String,
        schema: Schema,
        spec: PartitionSpec = PartitionSpec.unpartitioned(),
        var properties: Dict[String, String] = Dict[String, String](),
        format_version: Int = 2,
    ) raises -> Table:
        """Create an empty table: one `00000-<uuid>.metadata.json`, no snapshot.
        """
        var loc = self.table_location(namespace, name)
        if self.table_exists(namespace, name):
            raise Error("iceberg: table '" + loc + "' already exists")
        var m = new_table_metadata(
            loc, schema, spec, properties^, format_version
        )
        var dir = join_path(loc, METADATA_DIR)
        var path = join_path(dir, metadata_file_name(0))
        self.io.write_new(path, m.to_json().as_bytes())
        try:
            self.io.write_text(join_path(dir, VERSION_HINT), String("0\n"))
        except:
            pass
        m.metadata_file_location = path
        return Table(
            m^,
            path^,
            self.io.copy(),
            name if namespace == "" else namespace + "." + name,
        )

    def drop_table(self, namespace: String, name: String) raises:
        """Remove every file of a table. Nothing else references them."""
        var loc = self.table_location(namespace, name)
        var files = self.io.list(loc)
        for k in range(len(files)):
            self.io.delete(files[k])

    def list_namespaces(self) raises -> List[String]:
        var out = List[String]()
        var names = self.io.list_names(self.warehouse)
        for k in range(len(names)):
            var n = names[k].copy()
            # A namespace is a directory that is not itself a table.
            if _has_metadata(
                self.io, join_path(join_path(self.warehouse, n), METADATA_DIR)
            ):
                continue
            out.append(n^)
        return out^


def new_table_metadata(
    location: String,
    schema: Schema,
    spec: PartitionSpec,
    var properties: Dict[String, String],
    format_version: Int,
) raises -> TableMetadata:
    """The metadata of a brand new table: schema, spec, sort order, no data."""
    if format_version < 1 or format_version > SUPPORTED_FORMAT_VERSION:
        raise Error(
            "iceberg: format version "
            + String(format_version)
            + " cannot be written by this build"
        )
    var m = TableMetadata()
    m.format_version = format_version
    m.table_uuid = uuid4()
    m.location = location
    m.has_location = True
    m.last_sequence_number = 0
    m.last_updated_ms = now_ms()
    m.last_column_id = schema.highest_field_id()
    var s = schema.copy()
    s.schema_id = 0
    m.schemas.append(s^)
    m.current_schema_id = 0
    var sp = spec.copy()
    sp.spec_id = INITIAL_SPEC_ID
    var last_partition_id = 999
    for k in range(len(sp.fields)):
        if sp.fields[k].field_id > last_partition_id:
            last_partition_id = sp.fields[k].field_id
    m.partition_specs.append(sp^)
    m.default_spec_id = INITIAL_SPEC_ID
    m.last_partition_id = last_partition_id
    m.sort_orders.append(SortOrder(INITIAL_SORT_ORDER_ID, []))
    m.default_sort_order_id = INITIAL_SORT_ORDER_ID
    m.properties = properties^
    m.current_snapshot_id = -1
    m.has_current_snapshot = False
    if format_version >= 3:
        m.next_row_id = 0
        m.has_next_row_id = True
    return m^
