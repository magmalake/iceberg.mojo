"""Puffin files: the sidecar format Iceberg keeps deletion vectors and
sketches in.

    Magic Blob1 Blob2 ... BlobN Footer
    Footer := Magic FooterPayload FooterPayloadSize Flags Magic

`Magic` is `PFA1` (0x50 0x46 0x41 0x31). `FooterPayloadSize` and `Flags` are
little-endian 4-byte integers; bit 0 of the first flag byte says the payload is
a single LZ4 frame rather than plain UTF-8 JSON. The payload is a `FileMetadata`
object: a list of `BlobMetadata` (type, fields, snapshot-id, sequence-number,
offset, length, compression-codec, properties) plus file-level properties.

Two ways in, and the difference matters:

* `PuffinFile.open` reads the footer — the trailing 12 bytes first, then just
  the payload — which is four small ranged reads instead of downloading the
  file. Use it to *discover* what a Puffin file holds.
* `read_dv_blob` skips the footer entirely. A delete manifest entry already
  records `content_offset` and `content_size_in_bytes` for its deletion
  vector, and the spec requires them to match the footer exactly, so a scan
  reads the blob directly and never parses the footer at all.

Only `deletion-vector-v1` is decoded, by roaring.mojo, which verifies the
blob's magic and CRC-32. `apache-datasketches-theta-v1` blobs are listed with
their metadata (including the `ndv` property) but their sketches are not
decoded: that needs the DataSketches Theta format, which nothing in Mojo
implements and which no reader needs.
"""

from lz4 import decompress_frame
from roaring import Bitmap64, decode_iceberg_dv

from .io import FileIO
from .json import Json, parse_json, substr


comptime PUFFIN_MAGIC_0: UInt8 = 0x50
comptime PUFFIN_MAGIC_1: UInt8 = 0x46
comptime PUFFIN_MAGIC_2: UInt8 = 0x41
comptime PUFFIN_MAGIC_3: UInt8 = 0x31
comptime PUFFIN_MAGIC_LEN = 4
comptime PUFFIN_FOOTER_TAIL = 12
"""`FooterPayloadSize` (4) + `Flags` (4) + trailing `Magic` (4)."""

comptime BLOB_DELETION_VECTOR_V1 = String("deletion-vector-v1")
comptime BLOB_THETA_V1 = String("apache-datasketches-theta-v1")


def _le32(data: Span[UInt8, _], at: Int) -> Int:
    """A little-endian signed 4-byte integer, as the spec's footer uses."""
    var v = (
        UInt32(data[at])
        | (UInt32(data[at + 1]) << 8)
        | (UInt32(data[at + 2]) << 16)
        | (UInt32(data[at + 3]) << 24)
    )
    if v >= 0x80000000:
        return Int(v) - 0x100000000
    return Int(v)


def _check_magic(data: Span[UInt8, _], at: Int) raises:
    if at + PUFFIN_MAGIC_LEN > len(data):
        raise Error("iceberg: truncated Puffin file")
    if (
        data[at] != PUFFIN_MAGIC_0
        or data[at + 1] != PUFFIN_MAGIC_1
        or data[at + 2] != PUFFIN_MAGIC_2
        or data[at + 3] != PUFFIN_MAGIC_3
    ):
        raise Error("iceberg: not a Puffin file (bad PFA1 magic)")


@fieldwise_init
struct BlobMetadata(Copyable, Movable, Writable):
    """One entry of a Puffin footer's `blobs` list."""

    var type: String
    var fields: List[Int]
    var snapshot_id: Int64
    var sequence_number: Int64
    var offset: Int64
    var length: Int64
    var compression_codec: String
    """Empty when the blob is uncompressed, as the spec's absent field means.
    """
    var property_keys: List[String]
    var property_values: List[String]

    def property(self, key: String) -> String:
        for k in range(len(self.property_keys)):
            if self.property_keys[k] == key:
                return self.property_values[k]
        return String("")

    def has_property(self, key: String) -> Bool:
        for k in range(len(self.property_keys)):
            if self.property_keys[k] == key:
                return True
        return False

    def is_deletion_vector(self) -> Bool:
        return self.type == BLOB_DELETION_VECTOR_V1

    def referenced_data_file(self) -> String:
        """A deletion vector's `referenced-data-file` property."""
        return self.property("referenced-data-file")

    def cardinality(self) raises -> Int64:
        """A deletion vector's declared number of deleted rows, or -1."""
        var v = self.property("cardinality")
        if v == "":
            return -1
        return Int64(Int(v))

    def write_to(self, mut writer: Some[Writer]):
        writer.write(
            "BlobMetadata(",
            self.type,
            ", offset=",
            self.offset,
            ", length=",
            self.length,
            ")",
        )


struct PuffinFile(Copyable, Movable):
    """A Puffin file's footer, and the location it came from."""

    var location: String
    var blobs: List[BlobMetadata]
    var property_keys: List[String]
    var property_values: List[String]
    var footer_compressed: Bool

    def __init__(
        out self,
        var location: String,
        var blobs: List[BlobMetadata],
        var property_keys: List[String],
        var property_values: List[String],
        footer_compressed: Bool,
    ):
        self.location = location^
        self.blobs = blobs^
        self.property_keys = property_keys^
        self.property_values = property_values^
        self.footer_compressed = footer_compressed

    def property(self, key: String) -> String:
        for k in range(len(self.property_keys)):
            if self.property_keys[k] == key:
                return self.property_values[k]
        return String("")

    def created_by(self) -> String:
        return self.property("created-by")

    @staticmethod
    def open(io: FileIO, location: String) raises -> Self:
        """Read a Puffin footer without reading the blobs.

        Four ranged reads: the trailing 12 bytes, then the payload, then the
        two magics that bracket it. On a local file that is one `seek` each;
        on S3 it is a `HEAD` plus two `GET`s with `Range`.
        """
        var f = io.new_input(location)
        var size = f.length()
        if size < 2 * PUFFIN_MAGIC_LEN + PUFFIN_FOOTER_TAIL:
            raise Error("iceberg: file too small to be a Puffin file")
        var tail = f.read_range(size - PUFFIN_FOOTER_TAIL, PUFFIN_FOOTER_TAIL)
        _check_magic(Span(tail), 8)
        var payload_size = _le32(Span(tail), 0)
        var flags = tail[4]
        if payload_size < 0:
            raise Error("iceberg: negative Puffin footer payload size")
        var footer_start = (
            size - PUFFIN_FOOTER_TAIL - payload_size - PUFFIN_MAGIC_LEN
        )
        if footer_start < PUFFIN_MAGIC_LEN:
            raise Error("iceberg: Puffin footer payload overruns the file")
        var head = f.read_range(footer_start, PUFFIN_MAGIC_LEN + payload_size)
        _check_magic(Span(head), 0)
        var compressed = (flags & 1) != 0
        var payload = List[UInt8]()
        for k in range(PUFFIN_MAGIC_LEN, len(head)):
            payload.append(head[k])
        if compressed:
            payload = decompress_frame(Span(payload))
        return Self.from_payload(String(location), Span(payload), compressed)

    @staticmethod
    def from_payload(
        var location: String, payload: Span[UInt8, _], compressed: Bool
    ) raises -> Self:
        """Parse a `FileMetadata` JSON payload."""
        var doc = parse_json(String(StringSlice(unsafe_from_utf8=payload)))
        var blobs = List[BlobMetadata]()
        var arr = doc.get(doc.root, "blobs")
        if arr < 0:
            raise Error("iceberg: Puffin footer has no 'blobs'")
        for k in range(doc.size(arr)):
            var b = doc.at(arr, k)
            var fields = List[Int]()
            var fi = doc.get(b, "fields")
            if fi >= 0:
                for j in range(doc.size(fi)):
                    fields.append(Int(doc.as_int(doc.at(fi, j))))
            var keys = List[String]()
            var values = List[String]()
            var pi = doc.get(b, "properties")
            if pi >= 0 and not doc.is_null(pi):
                var m = doc.string_map(b, "properties")
                for entry in m.items():
                    keys.append(entry.key)
                    values.append(entry.value)
            blobs.append(
                BlobMetadata(
                    doc.req_string(b, "type"),
                    fields^,
                    doc.opt_int(b, "snapshot-id", -1),
                    doc.opt_int(b, "sequence-number", -1),
                    doc.opt_int(b, "offset", -1),
                    doc.opt_int(b, "length", -1),
                    doc.opt_string(b, "compression-codec", ""),
                    keys^,
                    values^,
                )
            )
        var fkeys = List[String]()
        var fvalues = List[String]()
        var fp = doc.get(doc.root, "properties")
        if fp >= 0 and not doc.is_null(fp):
            var m = doc.string_map(doc.root, "properties")
            for entry in m.items():
                fkeys.append(entry.key)
                fvalues.append(entry.value)
        return Self(location^, blobs^, fkeys^, fvalues^, compressed)


def read_blob_bytes(
    io: FileIO, location: String, offset: Int64, length: Int64
) raises -> List[UInt8]:
    """The raw bytes of one blob, by the offset and length a manifest records.
    """
    if length < 0:
        raise Error("iceberg: negative Puffin blob length")
    return io.read_range(location, Int(offset), Int(length))


def decompress_blob(var data: List[UInt8], codec: String) raises -> List[UInt8]:
    """Undo a blob's `compression-codec`. Only `lz4` is reachable here: `zstd`
    is legal in the format but no blob type Iceberg defines uses it, and
    deletion vectors must omit the codec entirely."""
    if codec == "" or codec == "none":
        return data^
    if codec == "lz4":
        return decompress_frame(Span(data))
    raise Error("iceberg: unsupported Puffin compression codec '" + codec + "'")


def read_deletion_vector(
    io: FileIO, location: String, offset: Int64, length: Int64
) raises -> Bitmap64:
    """Read and decode one `deletion-vector-v1` blob.

    `decode_iceberg_dv` checks the blob's own framing — the big-endian length,
    the `D1 D3 39 64` magic and the CRC-32 over both — so a truncated or
    corrupted vector is an error here rather than silently missing deletes.
    """
    var raw = read_blob_bytes(io, location, offset, length)
    return decode_iceberg_dv(Span(raw))


def deleted_positions(
    io: FileIO, location: String, offset: Int64, length: Int64
) raises -> List[UInt64]:
    """The deleted row positions of one deletion vector, ascending."""
    var bitmap = read_deletion_vector(io, location, offset, length)
    return bitmap.to_list()
