"""File access: locations in, bytes out — over local files, S3, GCS, Azure or
plain HTTP.

Iceberg metadata stores **absolute** locations — a manifest list names its
manifests by full URI, a manifest names its data files by full URI. That is
fine in place and awkward everywhere else: a table copied to another path, a
warehouse mounted somewhere new, or a test fixture checked into a repo all need
those locations redirected without touching the metadata. `FileIO` therefore
resolves every location through an ordered list of prefix rewrites before
opening it. `FileIO.local()` does none; `rebase(old, new)` adds one.

The backends come from [objectstore.mojo](https://github.com/magmalake/objectstore.mojo):
`FileIOResolver` picks one by URI scheme, configures it from Iceberg property
names (`s3.endpoint`, `s3.access-key-id`, `gcs.oauth2.token`,
`adls.sas-token.<account>`, …) and from the `storage-credentials` a REST
catalog vends, and falls back to the `AWS_*` environment. Everything above this
line only ever sees a location and a byte range.

`InputFile` is objectstore's trait, a superset of the one this module used to
declare: `location`, `exists`, `read_all` as before, plus `length` and
`read_range`, which is what a Parquet reader needs in order to fetch a footer
without downloading the whole file.
"""

from std.collections import Dict

from objectstore.fileio import (
    AnyInputFile,
    FileIOResolver,
    InputFile,
    OutputFile,
    StorageCredential,
)
from objectstore.local import LocalInputFile, LocalOutputFile
from objectstore.path import parse_uri

from .json import substr


comptime FILE_SCHEME = String("file://")


def strip_scheme(location: String) -> String:
    """`file:///a/b` -> `/a/b`. Other schemes are returned unchanged."""
    if location.startswith(FILE_SCHEME):
        return substr(
            location, FILE_SCHEME.byte_length(), location.byte_length()
        )
    return location


def is_local_location(location: String) -> Bool:
    """True when this location names an ordinary filesystem path."""
    var i = location.find("://")
    if i < 0:
        return True
    return location.startswith(FILE_SCHEME)


struct FileIO(Copyable, Movable):
    """Resolves Iceberg locations to bytes.

    Rewrites are applied in the order they were added; the first prefix that
    matches wins. Properties and vended credentials configure the object-store
    backends, and are ignored for `file://`.
    """

    var resolver: FileIOResolver
    var base_location: String
    """The table's own location, used to resolve *relative* locations.

    Format v4 allows a table to store locations relative to the table root so
    that a warehouse can be moved without rewriting metadata. Absolute
    locations — anything with a scheme or a leading `/` — are untouched.
    """

    def __init__(out self):
        self.resolver = FileIOResolver()
        self.base_location = ""

    def __init__(out self, var resolver: FileIOResolver):
        self.resolver = resolver^
        self.base_location = ""

    @staticmethod
    def local() -> Self:
        return Self()

    @staticmethod
    def with_properties(var properties: Dict[String, String]) -> Self:
        return Self(FileIOResolver(properties^))

    # ── configuration ──────────────────────────────────────────────────────
    def rebase(mut self, var old_prefix: String, var new_prefix: String):
        """Redirect every location under `old_prefix` to `new_prefix`."""
        self.resolver.rebase(old_prefix^, new_prefix^)

    def with_base(mut self, var location: String):
        """Set the table root that relative locations resolve against."""
        var b = location^
        while b.endswith("/"):
            b = substr(b, 0, b.byte_length() - 1)
        self.base_location = b^

    def set(mut self, var key: String, var value: String):
        """Set one storage property, using Iceberg's own property names."""
        self.resolver.set(key^, value^)

    def add_storage_credential(
        mut self, var prefix: String, var config: Dict[String, String]
    ):
        """Absorb one `storage-credentials` entry from a `loadTable` response.
        """
        self.resolver.add_storage_credential(prefix^, config^)

    def properties_for(self, location: String) -> Dict[String, String]:
        return self.resolver.properties_for(self.absolute(location))

    # ── resolution ─────────────────────────────────────────────────────────
    def resolve(self, location: String) -> String:
        """The location this one should actually be read from.

        Local locations come back as bare filesystem paths, which is what the
        callers that hand a path to `open()` or `listdir()` expect; every other
        scheme comes back as a URI.
        """
        var target = self.resolver.resolve(self.absolute(location))
        if is_local_location(target):
            return strip_scheme(target)
        return target^

    def absolute(self, location: String) -> String:
        """A relative location joined to the table root; anything else as is."""
        if self.base_location == "":
            return location
        if location.find("://") >= 0 or location.startswith("/"):
            return location
        return self.base_location + "/" + location

    def local_path(self, location: String) raises -> String:
        """The filesystem path for a location that must be local."""
        var target = self.resolve(location)
        if not is_local_location(target):
            raise Error("iceberg: '" + target + "' is not a local path")
        return target^

    def is_local(self, location: String) -> Bool:
        return is_local_location(self.resolver.resolve(location))

    # ── reading ────────────────────────────────────────────────────────────
    def new_input(self, location: String) raises -> AnyInputFile:
        return self.resolver.new_input(self.absolute(location))

    def read_all(self, location: String) raises -> List[UInt8]:
        return self.resolver.new_input(self.absolute(location)).read_all()

    def read_range(
        self, location: String, offset: Int, length: Int
    ) raises -> List[UInt8]:
        return self.resolver.new_input(self.absolute(location)).read_range(
            offset, length
        )

    def length(self, location: String) raises -> Int:
        return self.resolver.new_input(self.absolute(location)).length()

    def read_text(self, location: String) raises -> String:
        var raw = self.read_all(location)
        return String(StringSlice(unsafe_from_utf8=Span(raw)))

    def exists(self, location: String) -> Bool:
        try:
            return self.resolver.new_input(self.absolute(location)).exists()
        except:
            return False

    # ── listing ────────────────────────────────────────────────────────────
    def list(self, prefix: String) raises -> List[String]:
        """Every location under `prefix`, as full URIs."""
        return self.resolver.list(self.absolute(prefix))

    def list_names(self, directory: String) raises -> List[String]:
        """The immediate children of a directory, by name.

        Object stores have no directories, so the names are derived from the
        keys: everything under the prefix, cut at the first `/`, deduplicated.
        A local directory is listed directly.
        """
        var out = List[String]()
        var prefix = directory
        while prefix.endswith("/"):
            prefix = substr(prefix, 0, prefix.byte_length() - 1)
        var entries = self.resolver.list(self.absolute(prefix) + "/")
        var resolved = self.resolver.resolve(self.absolute(prefix))
        while resolved.endswith("/"):
            resolved = substr(resolved, 0, resolved.byte_length() - 1)
        var cut = resolved.byte_length() + 1
        for k in range(len(entries)):
            var e = entries[k]
            # `list` returns local results as file:// URIs; compare on the
            # same footing as the resolved prefix.
            if is_local_location(e):
                e = strip_scheme(e)
            if not e.startswith(resolved) or e.byte_length() <= cut:
                continue
            var rest = substr(e, cut, e.byte_length())
            var slash = rest.find("/")
            var name = rest if slash < 0 else substr(rest, 0, slash)
            var seen = False
            for j in range(len(out)):
                if out[j] == name:
                    seen = True
                    break
            if not seen:
                out.append(name^)
        return out^


# ── path helpers ────────────────────────────────────────────────────────────
def basename(path: String) -> String:
    var i = path.rfind("/")
    if i < 0:
        return path
    return substr(path, i + 1, path.byte_length())


def dirname(path: String) -> String:
    var i = path.rfind("/")
    if i < 0:
        return String(".")
    return substr(path, 0, i)


def join_path(a: String, b: String) -> String:
    if a == "":
        return b
    if a.endswith("/"):
        return a + b
    return a + "/" + b
