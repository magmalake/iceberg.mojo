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

from std.os import listdir
from std.pathlib import Path

from avro.deflate import inflate

from ..io import FileIO, basename, dirname, join_path, strip_scheme
from ..json import substr
from ..metadata import TableMetadata
from ..scan import TableScan


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
    var names = listdir(Path(io.resolve(metadata_dir)))
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
        raise Error(
            "iceberg: no *.metadata.json under '" + metadata_dir + "'"
        )
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
        var names = listdir(Path(io.resolve(dir)))
        for k in range(len(names)):
            if String(names[k]).endswith(METADATA_SUFFIX):
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
        var names = listdir(Path(self.io.resolve(base)))
        for k in range(len(names)):
            var n = String(names[k])
            if _has_metadata(self.io, join_path(join_path(base, n), METADATA_DIR)):
                out.append(n^)
        return out^

    def list_namespaces(self) raises -> List[String]:
        var out = List[String]()
        var names = listdir(Path(self.io.resolve(self.warehouse)))
        for k in range(len(names)):
            var n = String(names[k])
            # A namespace is a directory that is not itself a table.
            if _has_metadata(self.io, join_path(join_path(self.warehouse, n), METADATA_DIR)):
                continue
            out.append(n^)
        return out^
