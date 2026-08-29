"""File access: an `InputFile` trait, a local `FileIO`, and location rewriting.

Iceberg metadata stores **absolute** locations — a manifest list names its
manifests by full URI, a manifest names its data files by full URI. That is
fine in place and awkward everywhere else: a table copied to another path, a
warehouse mounted somewhere new, or a test fixture checked into a repo all need
those locations redirected without touching the metadata.

`FileIO` therefore resolves every location through an ordered list of prefix
rewrites before opening it. `FileIO.local()` does none; `rebase(old, new)` adds
one.

`InputFile` is the seam for objectstore.mojo: implement it and an S3 or GCS
backend drops in without any change above this line. Only `file://` and bare
paths are handled here.
"""

from .json import substr


comptime FILE_SCHEME = String("file://")


trait InputFile(Copyable, Movable):
    """A readable byte source addressed by an Iceberg location."""

    def location(self) -> String:
        ...

    def exists(self) raises -> Bool:
        ...

    def read_all(self) raises -> List[UInt8]:
        ...


@fieldwise_init
struct LocalInputFile(InputFile, Copyable, Movable):
    """An `InputFile` over an ordinary filesystem path."""

    var path: String

    def location(self) -> String:
        return self.path

    def exists(self) raises -> Bool:
        try:
            with open(self.path, "r") as f:
                _ = f.read_bytes(1)
            return True
        except:
            return False

    def read_all(self) raises -> List[UInt8]:
        with open(self.path, "r") as f:
            return f.read_bytes()


def strip_scheme(location: String) -> String:
    """`file:///a/b` -> `/a/b`. Other schemes are returned unchanged."""
    if location.startswith(FILE_SCHEME):
        return substr(location, FILE_SCHEME.byte_length(), location.byte_length())
    return location


struct FileIO(Copyable, Movable):
    """Resolves Iceberg locations to readable paths.

    Rewrites are applied in the order they were added; the first prefix that
    matches wins.
    """

    var from_prefixes: List[String]
    var to_prefixes: List[String]

    def __init__(out self):
        self.from_prefixes = []
        self.to_prefixes = []

    @staticmethod
    def local() -> Self:
        return Self()

    def rebase(mut self, var old_prefix: String, var new_prefix: String):
        """Redirect every location under `old_prefix` to `new_prefix`."""
        self.from_prefixes.append(old_prefix^)
        self.to_prefixes.append(new_prefix^)

    def resolve(self, location: String) -> String:
        """The path this location should actually be read from."""
        for k in range(len(self.from_prefixes)):
            if location.startswith(self.from_prefixes[k]):
                var rest = substr(
                    location,
                    self.from_prefixes[k].byte_length(),
                    location.byte_length(),
                )
                return self.to_prefixes[k] + rest
        return strip_scheme(location)

    def new_input(self, location: String) -> LocalInputFile:
        return LocalInputFile(self.resolve(location))

    def read_all(self, location: String) raises -> List[UInt8]:
        with open(self.resolve(location), "r") as f:
            return f.read_bytes()

    def read_text(self, location: String) raises -> String:
        with open(self.resolve(location), "r") as f:
            return f.read()

    def exists(self, location: String) -> Bool:
        try:
            with open(self.resolve(location), "r") as f:
                _ = f.read_bytes(1)
            return True
        except:
            return False


# ── filesystem table discovery ──────────────────────────────────────────────
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
