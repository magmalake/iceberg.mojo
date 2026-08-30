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
header, a raw deflate stream and an eight-byte trailer, and avro.mojo already
carries a deflate decoder, so no extra dependency is needed.
"""

from std.collections import Dict

from avro.deflate import inflate
from parquet import RecordBatch

from ..append import (
    AppendResult,
    metadata_file_name,
    next_metadata_version,
    prepare_append,
    write_and_prepare_append,
)
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
from ..util import now_ms, uuid4
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
    if not io.exists(path):
        return -1
    var text = io.read_text(path)
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
        var candidates = [
            join_path(metadata_dir, "v" + String(hint) + METADATA_SUFFIX),
            join_path(metadata_dir, String(hint) + METADATA_SUFFIX),
            join_path(metadata_dir, "v" + String(hint) + GZ_METADATA_SUFFIX),
        ]
        for k in range(len(candidates)):
            if io.exists(candidates[k]):
                return candidates[k]

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
        var path = find_latest_metadata(
            self.io, dirname(self.metadata_location)
        )
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

    def commit(mut self, result: AppendResult) raises:
        """Persist a prepared commit as a new `<V>-<uuid>.metadata.json`.

        The new file is written with *create* semantics — it must not already
        exist — which is what makes two writers racing on the same version
        detectable. A local filesystem really provides that; an object store
        does not, and the spec says so, so this is unsafe over `s3://` in
        exactly the way every other filesystem-table writer is.
        """
        var dir = dirname(self.metadata_location)
        var version = next_metadata_version(result.metadata)
        var path = join_path(dir, metadata_file_name(version))
        var doc = result.metadata.to_json()
        self.io.write_new(path, doc.as_bytes())
        # `version-hint.text` is advisory: a reader that cannot find the file
        # it names falls back to listing, which is why it is written after.
        try:
            self.io.write_text(
                join_path(dir, VERSION_HINT), String(version) + "\n"
            )
        except:
            pass
        self.metadata = result.metadata.copy()
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
                # data files are already written and stay valid.
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
