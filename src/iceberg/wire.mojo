"""The byte primitives of iceberg's own wire form for data file entries.

`DataFile.encode` / `DataFile.decode` (and `Datum`'s and `ColumnMetrics`'
in them) send a data file entry from one process to another without writing
a manifest: a worker writes Parquet files and returns their entries, and
another process commits them. That form is built out of these:

* an integer is 8 bytes, little-endian two's complement (`Int`, `Int64`);
* a `Float64` is its 8 IEEE 754 bytes, little-endian, NaN payloads kept;
* a `UInt8` is one byte, and a `Bool` one byte, 0 or 1;
* a byte string is its length as an integer, then the bytes; a `String` is
  a byte string of its UTF-8;
* a list is its length as an integer, then each element.

The layout of a `DataFile` is versioned: its first byte is
`iceberg.manifest.DATA_FILE_WIRE_VERSION`, and a reader refuses a version it
doesn't know rather than misreading it, so two builds that disagree say so. Every read is
bounds-checked, and a length that doesn't fit what's left raises, so a
truncated or corrupt buffer is an error, never a read past the end.

The signatures follow one shape, `encode(self, mut out: List[UInt8])` and
`@staticmethod decode(buf: Span[UInt8, _], mut pos: Int) raises -> Self`,
which is the shape of dista's `Codec` trait. iceberg doesn't depend on
dista, so its types don't declare `Codec`; a wrapper that does is two
one-line methods (orca's `orca.tables.DataFiles` is one).
"""

from std.memory import bitcast


def need(buf: Span[UInt8, _], pos: Int, n: Int) raises:
    """Raises unless `n` bytes are left at `pos`."""
    if n < 0 or pos < 0 or n > len(buf) - pos:
        raise Error(
            "iceberg wire: needed ",
            n,
            " bytes at ",
            pos,
            ", have ",
            len(buf) - pos,
        )


def put_u8(mut out: List[UInt8], v: UInt8):
    out.append(v)


def get_u8(buf: Span[UInt8, _], mut pos: Int) raises -> UInt8:
    need(buf, pos, 1)
    pos += 1
    return buf[pos - 1]


def put_bool(mut out: List[UInt8], v: Bool):
    out.append(UInt8(1) if v else UInt8(0))


def get_bool(buf: Span[UInt8, _], mut pos: Int) raises -> Bool:
    var b = get_u8(buf, pos)
    if b > 1:
        raise Error("iceberg wire: a Bool is 0 or 1, not ", Int(b))
    return b == 1


def put_i64(mut out: List[UInt8], v: Int64):
    var u = UInt64(v)
    for k in range(8):
        out.append(UInt8((u >> UInt64(8 * k)) & 0xFF))


def get_i64(buf: Span[UInt8, _], mut pos: Int) raises -> Int64:
    need(buf, pos, 8)
    var u: UInt64 = 0
    for k in range(8):
        u |= UInt64(buf[pos + k]) << UInt64(8 * k)
    pos += 8
    return Int64(u)


def put_int(mut out: List[UInt8], v: Int):
    put_i64(out, Int64(v))


def get_int(buf: Span[UInt8, _], mut pos: Int) raises -> Int:
    return Int(get_i64(buf, pos))


def put_f64(mut out: List[UInt8], v: Float64):
    put_i64(out, bitcast[DType.int64](v))


def get_f64(buf: Span[UInt8, _], mut pos: Int) raises -> Float64:
    return bitcast[DType.float64](get_i64(buf, pos))


def get_count(buf: Span[UInt8, _], mut pos: Int, min_size: Int) raises -> Int:
    """A list length, refused if its elements, of at least `min_size` bytes
    each, can't fit in what's left."""
    var n = get_int(buf, pos)
    if n < 0 or (min_size > 0 and n > (len(buf) - pos) // min_size):
        raise Error(
            "iceberg wire: a list of ",
            n,
            " doesn't fit in ",
            len(buf) - pos,
            " bytes",
        )
    return n


def put_bytes(mut out: List[UInt8], b: List[UInt8]):
    put_int(out, len(b))
    out.extend(Span(b))


def get_bytes(buf: Span[UInt8, _], mut pos: Int) raises -> List[UInt8]:
    var n = get_int(buf, pos)
    need(buf, pos, n)
    var out = List[UInt8](buf[pos : pos + n])
    pos += n
    return out^


def put_string(mut out: List[UInt8], s: String):
    put_int(out, s.byte_length())
    out.extend(s.as_bytes())


def get_string(buf: Span[UInt8, _], mut pos: Int) raises -> String:
    var n = get_int(buf, pos)
    need(buf, pos, n)
    var s = String(from_utf8=buf[pos : pos + n])
    pos += n
    return s^
