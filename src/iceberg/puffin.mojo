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

`PuffinWriter` is the other direction: blobs in, a complete file out, with
the footer written as plain JSON or as a single LZ4 frame. It is what writes
the deletion vectors a merge-on-read delete produces.

Only `deletion-vector-v1` is decoded, by roaring.mojo, which verifies the
blob's magic and CRC-32. `apache-datasketches-theta-v1` blobs are listed with
their metadata (including the `ndv` property) but their sketches are not
decoded: that needs the DataSketches Theta format, which nothing in Mojo
implements and which no reader needs.
"""

from lz4 import compress_frame, decompress_frame
from roaring import Bitmap64, decode_iceberg_dv, encode_iceberg_dv

from .io import FileIO
from .json import Json, json_quote, parse_json, substr


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


# ── writing ─────────────────────────────────────────────────────────────────
comptime PUFFIN_CREATED_BY = String("iceberg.mojo")
"""The `created-by` file property, so a Puffin file says who made it."""


def _put_le32(mut out: List[UInt8], v: Int):
    """A little-endian 4-byte integer, the only integer the footer uses."""
    var u = UInt32(UInt64(Int64(v)) & 0xFFFFFFFF)
    out.append(UInt8(u & 0xFF))
    out.append(UInt8((u >> 8) & 0xFF))
    out.append(UInt8((u >> 16) & 0xFF))
    out.append(UInt8((u >> 24) & 0xFF))


def _put_magic(mut out: List[UInt8]):
    out.append(PUFFIN_MAGIC_0)
    out.append(PUFFIN_MAGIC_1)
    out.append(PUFFIN_MAGIC_2)
    out.append(PUFFIN_MAGIC_3)


struct PuffinWriter(Movable):
    """Builds a Puffin file blob by blob, footer last.

    ```mojo
    var w = PuffinWriter()
    _ = w.add_deletion_vector(data_file_path, bitmap)
    var bytes = w^.finish()
    ```

    The blobs go into the body as they arrive, each recording the `offset` and
    `length` its `BlobMetadata` will claim; `finish` appends the footer, whose
    payload is the `FileMetadata` JSON — optionally as a single LZ4 frame,
    which is the only compression the format allows there.

    A `deletion-vector-v1` blob is written the way the spec requires and no
    other way: never compressed, `snapshot-id` and `sequence-number` both -1
    (a DV is not owned by a snapshot; the manifest entry that references it
    is), `fields` empty, and the two properties a reader needs —
    `referenced-data-file` and `cardinality`.
    """

    var body: List[UInt8]
    """Magic, then every blob written so far."""
    var blobs: List[BlobMetadata]
    var property_keys: List[String]
    var property_values: List[String]

    def __init__(out self, var created_by: String = PUFFIN_CREATED_BY):
        self.body = List[UInt8]()
        _put_magic(self.body)
        self.blobs = List[BlobMetadata]()
        self.property_keys = List[String]()
        self.property_values = List[String]()
        if created_by != "":
            self.property_keys.append(String("created-by"))
            self.property_values.append(created_by^)

    def __init__(out self, *, deinit move: Self):
        self.body = move.body^
        self.blobs = move.blobs^
        self.property_keys = move.property_keys^
        self.property_values = move.property_values^

    def set_property(mut self, var key: String, var value: String):
        """Set a file-level property, replacing any previous value."""
        for k in range(len(self.property_keys)):
            if self.property_keys[k] == key:
                self.property_values[k] = value^
                return
        self.property_keys.append(key^)
        self.property_values.append(value^)

    def add_blob(
        mut self,
        var type: String,
        var fields: List[Int],
        snapshot_id: Int64,
        sequence_number: Int64,
        data: Span[UInt8, _],
        var compression_codec: String = String(""),
        var property_keys: List[String] = List[String](),
        var property_values: List[String] = List[String](),
    ) raises -> Int:
        """Append one blob; returns its index in `blobs`.

        `compression_codec` is applied here: `lz4` means a single LZ4 frame,
        which is the codec Puffin defines for blob payloads. Anything else is
        refused rather than written under a name a reader would trust.
        """
        var payload: List[UInt8]
        if compression_codec == "" or compression_codec == "none":
            payload = List[UInt8]()
            payload.extend(data)
            compression_codec = String("")
        elif compression_codec == "lz4":
            payload = compress_frame(data)
        else:
            raise Error(
                "iceberg: Puffin blob compression codec '"
                + compression_codec
                + "' is not one this build writes (lz4, or none)"
            )
        var offset = len(self.body)
        self.body.extend(payload^)
        var length = len(self.body) - offset
        self.blobs.append(
            BlobMetadata(
                type^,
                fields^,
                snapshot_id,
                sequence_number,
                Int64(offset),
                Int64(length),
                compression_codec^,
                property_keys^,
                property_values^,
            )
        )
        return len(self.blobs) - 1

    def add_deletion_vector(
        mut self, referenced_data_file: String, bitmap: Bitmap64
    ) raises -> Int:
        """One `deletion-vector-v1` blob for one data file.

        The bitmap is framed by roaring.mojo — big-endian length, the
        `D1 D3 39 64` magic, the portable serialisation and a CRC-32 over both
        — which is byte-for-byte what a reader verifies.
        """
        var blob = encode_iceberg_dv(bitmap)
        var keys = List[String]()
        var values = List[String]()
        keys.append(String("referenced-data-file"))
        values.append(referenced_data_file)
        keys.append(String("cardinality"))
        values.append(String(bitmap.cardinality()))
        return self.add_blob(
            String(BLOB_DELETION_VECTOR_V1),
            List[Int](),
            -1,
            -1,
            Span(blob),
            String(""),
            keys^,
            values^,
        )

    def blob(self, i: Int) -> BlobMetadata:
        return self.blobs[i].copy()

    def footer_json(self) -> String:
        """The `FileMetadata` payload: every blob, then the file properties."""
        var out = String('{"blobs":[')
        for k in range(len(self.blobs)):
            if k > 0:
                out += ","
            ref b = self.blobs[k]
            out += '{"type":' + json_quote(b.type)
            out += ',"fields":['
            for j in range(len(b.fields)):
                if j > 0:
                    out += ","
                out += String(b.fields[j])
            out += "]"
            out += ',"snapshot-id":' + String(b.snapshot_id)
            out += ',"sequence-number":' + String(b.sequence_number)
            out += ',"offset":' + String(b.offset)
            out += ',"length":' + String(b.length)
            if b.compression_codec != "":
                out += ',"compression-codec":' + json_quote(b.compression_codec)
            if len(b.property_keys) > 0:
                out += ',"properties":{'
                for j in range(len(b.property_keys)):
                    if j > 0:
                        out += ","
                    out += json_quote(b.property_keys[j]) + ":"
                    out += json_quote(b.property_values[j])
                out += "}"
            out += "}"
        out += "]"
        if len(self.property_keys) > 0:
            out += ',"properties":{'
            for k in range(len(self.property_keys)):
                if k > 0:
                    out += ","
                out += json_quote(self.property_keys[k]) + ":"
                out += json_quote(self.property_values[k])
            out += "}"
        out += "}"
        return out^

    def finish(self, compress_footer: Bool = False) raises -> List[UInt8]:
        """The complete file: body, then `Magic payload size flags Magic`.

        `compress_footer` writes the payload as one LZ4 frame and sets bit 0
        of the first flag byte, which is the only flag the format defines. LZ4
        is also the only footer codec Puffin allows — there is no ZSTD footer
        — so this is a `Bool` rather than a codec name.
        """
        var text = self.footer_json()
        var payload: List[UInt8]
        if compress_footer:
            payload = compress_frame(text.as_bytes())
        else:
            payload = List[UInt8]()
            payload.extend(text.as_bytes())
        var size = len(payload)
        var out = self.body.copy()
        _put_magic(out)
        out.extend(payload^)
        _put_le32(out, size)
        _put_le32(out, 1 if compress_footer else 0)
        _put_magic(out)
        return out^
