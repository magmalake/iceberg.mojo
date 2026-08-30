"""Columnar kernels: the Arrow arrays a scan produces, without a `Datum` in
sight.

Everything here operates on parquet.mojo's `ArrayData` — one Arrow array, its
validity bitmap and its buffers — and produces another one. A scan uses them in
this order:

1. **cast** — the file's physical column is retagged or widened to the type the
   table's *current* schema says the column has (`int` -> `long`,
   `float` -> `double`, a decimal whose precision grew, a timestamp whose unit
   differs, `large_utf8` -> `utf8`);
2. **constant** — a column the file does not have becomes an array of one
   repeated value: an identity partition value, an `initial-default`, a
   metadata column like `_file`, or all-null;
3. **select** — deletes and the residual predicate produce one `List[Bool]` per
   batch, and `filter_array` applies it to every output column in one pass;
4. **concat** — batches are stitched together when a caller wants one table
   instead of a stream.

None of this materialises a per-cell tagged value, which is the whole point:
the previous implementation built a `Datum` for every cell of every column it
read, and that cost about 70x.
"""

from std.memory import bitcast

from parquet.arrow import (
    AT_BINARY,
    AT_BOOL,
    AT_DATE32,
    AT_DECIMAL128,
    AT_FIXED_SIZE_BINARY,
    AT_FLOAT16,
    AT_FLOAT32,
    AT_FLOAT64,
    AT_INT16,
    AT_INT32,
    AT_INT64,
    AT_INT8,
    AT_LARGE_BINARY,
    AT_LARGE_UTF8,
    AT_NULL,
    AT_TIME32,
    AT_TIME64,
    AT_TIMESTAMP,
    AT_UINT16,
    AT_UINT32,
    AT_UINT64,
    AT_UINT8,
    AT_UTF8,
    TU_MICRO,
    TU_MILLI,
    TU_NANO,
    TU_SECOND,
    ArrayData,
    ArrowType,
    bit_get,
    bit_set,
    load_f32,
    load_f64,
    load_i32,
    load_i64,
)

from .values import Datum
from .types import (
    P_BINARY,
    P_BOOLEAN,
    P_DATE,
    P_DECIMAL,
    P_DOUBLE,
    P_FIXED,
    P_FLOAT,
    P_INT,
    P_LONG,
    P_STRING,
    P_TIME,
    P_TIMESTAMP,
    P_TIMESTAMPTZ,
    P_TIMESTAMPTZ_NS,
    P_TIMESTAMP_NS,
    P_UNKNOWN,
    P_UUID,
)


# ── shapes ──────────────────────────────────────────────────────────────────
def is_binary_type(t: ArrowType) -> Bool:
    """Variable-width with 32-bit offsets."""
    return t.id == AT_UTF8 or t.id == AT_BINARY


def is_large_binary_type(t: ArrowType) -> Bool:
    """Variable-width with 64-bit offsets."""
    return t.id == AT_LARGE_UTF8 or t.id == AT_LARGE_BINARY


def is_var_width(t: ArrowType) -> Bool:
    return is_binary_type(t) or is_large_binary_type(t)


comptime CMP_NONE = 0
comptime CMP_INT = 1
comptime CMP_FLOAT = 2
comptime CMP_BYTES = 3


def compare_class(t: ArrowType) -> Int:
    """Which comparison kernel, if any, serves this Arrow type."""
    var i = t.id
    if (
        i == AT_BOOL
        or i == AT_INT8
        or i == AT_INT16
        or i == AT_INT32
        or i == AT_INT64
        or i == AT_UINT8
        or i == AT_UINT16
        or i == AT_UINT32
        or i == AT_UINT64
        or i == AT_DATE32
        or i == AT_TIME32
        or i == AT_TIME64
        or i == AT_TIMESTAMP
    ):
        return CMP_INT
    if i == AT_FLOAT32 or i == AT_FLOAT64:
        return CMP_FLOAT
    if (
        i == AT_UTF8
        or i == AT_BINARY
        or i == AT_LARGE_UTF8
        or i == AT_LARGE_BINARY
        or i == AT_FIXED_SIZE_BINARY
    ):
        return CMP_BYTES
    # decimal128 and anything else: the caller falls back to `Datum`.
    return CMP_NONE


# ── the Arrow type an Iceberg primitive is produced at ──────────────────────
def arrow_type_for(
    kind: UInt8, precision: Int, scale: Int, length: Int
) raises -> ArrowType:
    var t: ArrowType
    if kind == P_BOOLEAN:
        t = ArrowType(AT_BOOL)
    elif kind == P_INT:
        t = ArrowType(AT_INT32)
    elif kind == P_FLOAT:
        t = ArrowType(AT_FLOAT32)
    elif kind == P_DOUBLE:
        t = ArrowType(AT_FLOAT64)
    elif kind == P_DATE:
        t = ArrowType(AT_DATE32)
    elif kind == P_TIME:
        t = ArrowType(AT_TIME64)
        t.unit = TU_MICRO
    elif kind == P_TIMESTAMP or kind == P_TIMESTAMPTZ:
        t = ArrowType(AT_TIMESTAMP)
        t.unit = TU_MICRO
        if kind == P_TIMESTAMPTZ:
            t.tz = String("UTC")
    elif kind == P_TIMESTAMP_NS or kind == P_TIMESTAMPTZ_NS:
        t = ArrowType(AT_TIMESTAMP)
        t.unit = TU_NANO
        if kind == P_TIMESTAMPTZ_NS:
            t.tz = String("UTC")
    elif kind == P_STRING:
        t = ArrowType(AT_UTF8)
    elif kind == P_UUID:
        t = ArrowType(AT_FIXED_SIZE_BINARY)
        t.byte_width = 16
    elif kind == P_FIXED:
        t = ArrowType(AT_FIXED_SIZE_BINARY)
        t.byte_width = length
    elif kind == P_BINARY:
        t = ArrowType(AT_BINARY)
    elif kind == P_DECIMAL:
        t = ArrowType(AT_DECIMAL128)
        t.precision = precision
        t.scale = scale
        t.byte_width = 16
    elif kind == P_LONG:
        t = ArrowType(AT_INT64)
    else:
        # `unknown`, `variant`, geometry/geography: carried as binary, which is
        # what they are on the wire.
        t = ArrowType(AT_BINARY)
    return t^


# ── empty and constant arrays ───────────────────────────────────────────────
def empty_array(
    name: String, field_id: Int, var t: ArrowType
) raises -> ArrayData:
    var a = ArrayData(t^, name)
    a.nullable = True
    a.field_id = Int32(field_id)
    a.length = 0
    if is_binary_type(a.type):
        a.offsets.append(0)
    elif is_large_binary_type(a.type):
        a.large_offsets.append(0)
    return a^


def _unit_scale(unit: Int) -> Int64:
    if unit == TU_SECOND:
        return 1
    if unit == TU_MILLI:
        return 1000
    if unit == TU_MICRO:
        return 1000000
    return 1000000000


def constant_array(
    name: String,
    field_id: Int,
    kind: UInt8,
    precision: Int,
    scale: Int,
    length: Int,
    d: Datum,
    n: Int,
) raises -> ArrayData:
    """`n` copies of one value — a partition constant, a default, or all-null.
    """
    var a = empty_array(
        name, field_id, arrow_type_for(kind, precision, scale, length)
    )
    a.length = n
    if n == 0:
        return a^
    if not d.valid:
        a.null_count = n
        for _ in range((n + 7) // 8):
            a.validity.append(0)
        if is_binary_type(a.type):
            for _ in range(n):
                a.offsets.append(0)
        elif is_large_binary_type(a.type):
            for _ in range(n):
                a.large_offsets.append(0)
        else:
            var w = a.type.fixed_width()
            if a.type.id == AT_BOOL:
                for _ in range((n + 7) // 8):
                    a.values.append(0)
            else:
                for _ in range(n * w):
                    a.values.append(0)
        return a^

    # Every entry is valid.
    var whole = n // 8
    for _ in range(whole):
        a.validity.append(0xFF)
    if n % 8:
        a.validity.append((UInt8(1) << UInt8(n % 8)) - 1)

    var cell = encode_cell(d, a.type, kind)
    if is_var_width(a.type):
        var at = 0
        for _ in range(n):
            for k in range(len(cell)):
                a.values.append(cell[k])
            at += len(cell)
            if is_binary_type(a.type):
                a.offsets.append(Int32(at))
            else:
                a.large_offsets.append(Int64(at))
    elif a.type.id == AT_BOOL:
        var on = d.i != 0
        for _ in range((n + 7) // 8):
            a.values.append(UInt8(0xFF) if on else UInt8(0))
    else:
        for _ in range(n):
            for k in range(len(cell)):
                a.values.append(cell[k])
    return a^


def encode_cell(d: Datum, t: ArrowType, kind: UInt8) raises -> List[UInt8]:
    """One value as the bytes Arrow stores for it."""
    var out = List[UInt8]()
    if is_var_width(t):
        if kind == P_STRING:
            out.extend(d.s.as_bytes())
        else:
            out.extend(Span(d.b))
        return out^
    if t.id == AT_DECIMAL128:
        return decimal_le16(d)
    if t.id == AT_FIXED_SIZE_BINARY:
        var w = t.byte_width
        for k in range(w):
            out.append(d.b[k] if k < len(d.b) else 0)
        return out^
    if t.id == AT_FLOAT32:
        _put_le(out, UInt64(UInt32(bitcast[DType.uint32](Float32(d.f)))), 4)
        return out^
    if t.id == AT_FLOAT64:
        _put_le(out, bitcast[DType.uint64](d.f), 8)
        return out^
    if t.id == AT_BOOL:
        out.append(UInt8(1) if d.i != 0 else UInt8(0))
        return out^
    _put_le(out, UInt64(d.i), t.fixed_width())
    return out^


def decimal_le16(d: Datum) -> List[UInt8]:
    """A decimal's unscaled value as Arrow's little-endian 16-byte two's
    complement."""
    var out = List[UInt8]()
    if not d.valid or len(d.b) == 0:
        for _ in range(16):
            out.append(0)
        return out^
    var sign = UInt8(0xFF) if (d.b[0] & 0x80) != 0 else UInt8(0)
    for k in range(len(d.b)):
        out.append(d.b[len(d.b) - 1 - k])
    while len(out) < 16:
        out.append(sign)
    while len(out) > 16:
        _ = out.pop()
    return out^


def _put_le(mut out: List[UInt8], v: UInt64, width: Int):
    for k in range(width):
        out.append(UInt8((v >> UInt64(8 * k)) & 0xFF))


def int64_array(name: String, values: List[Int64]) raises -> ArrayData:
    """A non-null `int64` column — `_pos`, `_row_id` and friends."""
    var a = ArrayData(ArrowType(AT_INT64), name)
    a.nullable = True
    a.field_id = -1
    a.length = len(values)
    var whole = a.length // 8
    for _ in range(whole):
        a.validity.append(0xFF)
    if a.length % 8:
        a.validity.append((UInt8(1) << UInt8(a.length % 8)) - 1)
    a.values = List[UInt8](capacity=8 * a.length)
    for k in range(len(values)):
        _put_le(a.values, bitcast[DType.uint64](values[k]), 8)
    return a^


# ── casts ───────────────────────────────────────────────────────────────────
def cast_array(
    var a: ArrayData, target: ArrowType, name: String, field_id: Int
) raises -> ArrayData:
    """The file's column, produced at the table's current type.

    Iceberg only ever widens: `int` -> `long`, `float` -> `double`, and a
    decimal whose precision grew (which Arrow stores at a fixed 16 bytes either
    way, so only the tag changes). Everything else here is a retag — the same
    bytes under a different Arrow type — which is what makes a full scan of an
    unevolved table copy nothing at all.
    """
    a.name = name
    a.field_id = Int32(field_id)
    var src = a.type.copy()
    if src.id == target.id:
        if src.id == AT_TIMESTAMP or src.id == AT_TIME64 or src.id == AT_TIME32:
            if src.unit != target.unit:
                return _rescale_time(a^, src, target)
        a.type = target.copy()
        return a^
    if src.id == AT_NULL:
        # Nothing was decoded; produce nulls of the right shape.
        var n = a.length
        var out = empty_array(name, field_id, target.copy())
        out.length = n
        out.null_count = n
        for _ in range((n + 7) // 8):
            out.validity.append(0)
        if is_binary_type(out.type):
            for _ in range(n):
                out.offsets.append(0)
        elif is_large_binary_type(out.type):
            for _ in range(n):
                out.large_offsets.append(0)
        else:
            var w = out.type.fixed_width()
            if out.type.id == AT_BOOL:
                for _ in range((n + 7) // 8):
                    out.values.append(0)
            else:
                for _ in range(n * w):
                    out.values.append(0)
        return out^
    if is_binary_type(src) and is_binary_type(target):
        a.type = target.copy()
        return a^
    if is_large_binary_type(src) and is_large_binary_type(target):
        a.type = target.copy()
        return a^
    if is_large_binary_type(src) and is_binary_type(target):
        var out = _reoffset(a, target, name, field_id, True)
        return out^
    if is_binary_type(src) and is_large_binary_type(target):
        var out = _reoffset(a, target, name, field_id, False)
        return out^
    var sw = src.fixed_width()
    var tw = target.fixed_width()
    if sw > 0 and tw > 0 and src.id != AT_BOOL and target.id != AT_BOOL:
        var src_float = src.id == AT_FLOAT32 or src.id == AT_FLOAT64
        var dst_float = target.id == AT_FLOAT32 or target.id == AT_FLOAT64
        if src.id == AT_DECIMAL128 or target.id == AT_DECIMAL128:
            if src.id == AT_DECIMAL128 and target.id == AT_DECIMAL128:
                a.type = target.copy()
                return a^
            raise Error(
                "iceberg: cannot cast " + String(src) + " to " + String(target)
            )
        if src.id == AT_FIXED_SIZE_BINARY or target.id == AT_FIXED_SIZE_BINARY:
            if sw == tw:
                a.type = target.copy()
                return a^
            raise Error(
                "iceberg: cannot cast " + String(src) + " to " + String(target)
            )
        return _rewiden(a, src, target, name, field_id, src_float, dst_float)
    raise Error("iceberg: cannot cast " + String(src) + " to " + String(target))


def _rewiden(
    a: ArrayData,
    src: ArrowType,
    target: ArrowType,
    name: String,
    field_id: Int,
    src_float: Bool,
    dst_float: Bool,
) raises -> ArrayData:
    var out = ArrayData(target.copy(), name)
    out.nullable = a.nullable
    out.field_id = Int32(field_id)
    out.length = a.length
    out.null_count = a.null_count
    out.validity = a.validity.copy()
    var tw = target.fixed_width()
    out.values = List[UInt8](capacity=tw * a.length)
    for i in range(a.length):
        if dst_float:
            var f: Float64
            if src_float:
                f = Float64(
                    load_f32(Span(a.values), i)
                ) if src.id == AT_FLOAT32 else load_f64(Span(a.values), i)
            else:
                f = Float64(int_at(a, i))
            if target.id == AT_FLOAT32:
                _put_le(
                    out.values,
                    UInt64(UInt32(bitcast[DType.uint32](Float32(f)))),
                    4,
                )
            else:
                _put_le(out.values, bitcast[DType.uint64](f), 8)
        else:
            var v = Int64(load_f64(Span(a.values), i)) if src_float else int_at(
                a, i
            )
            _put_le(out.values, bitcast[DType.uint64](v), tw)
    return out^


def _rescale_time(
    var a: ArrayData, src: ArrowType, target: ArrowType
) raises -> ArrayData:
    var from_s = _unit_scale(src.unit)
    var to_s = _unit_scale(target.unit)
    var out = ArrayData(target.copy(), a.name.copy())
    out.nullable = a.nullable
    out.field_id = a.field_id
    out.length = a.length
    out.null_count = a.null_count
    out.validity = a.validity.copy()
    var tw = target.fixed_width()
    out.values = List[UInt8](capacity=tw * a.length)
    for i in range(a.length):
        var v = int_at(a, i)
        if to_s >= from_s:
            v = v * (to_s // from_s)
        else:
            v = v // (from_s // to_s)
        _put_le(out.values, bitcast[DType.uint64](v), tw)
    return out^


def _reoffset(
    a: ArrayData, target: ArrowType, name: String, field_id: Int, to_small: Bool
) raises -> ArrayData:
    var out = ArrayData(target.copy(), name)
    out.nullable = a.nullable
    out.field_id = Int32(field_id)
    out.length = a.length
    out.null_count = a.null_count
    out.validity = a.validity.copy()
    out.values = a.values.copy()
    if to_small:
        for k in range(len(a.large_offsets)):
            out.offsets.append(Int32(a.large_offsets[k]))
    else:
        for k in range(len(a.offsets)):
            out.large_offsets.append(Int64(a.offsets[k]))
    return out^


# ── typed reads ─────────────────────────────────────────────────────────────
def int_at(a: ArrayData, i: Int) raises -> Int64:
    """The integral value at `i`, whatever fixed width it was stored at."""
    var id = a.type.id
    if id == AT_BOOL:
        return 1 if bit_get(Span(a.values), i) else 0
    if id == AT_INT8:
        return Int64(bitcast[DType.int8](a.values[i]))
    if id == AT_UINT8:
        return Int64(a.values[i])
    if id == AT_INT16 or id == AT_UINT16:
        var v = UInt16(a.values[2 * i]) | (UInt16(a.values[2 * i + 1]) << 8)
        if id == AT_INT16:
            return Int64(bitcast[DType.int16](v))
        return Int64(v)
    if id == AT_INT32 or id == AT_DATE32 or id == AT_TIME32:
        return Int64(load_i32(Span(a.values), i))
    if id == AT_UINT32:
        return Int64(UInt32(bitcast[DType.uint32](load_i32(Span(a.values), i))))
    if id == AT_INT64 or id == AT_TIME64 or id == AT_TIMESTAMP:
        return load_i64(Span(a.values), i)
    if id == AT_UINT64:
        return load_i64(Span(a.values), i)
    if id == AT_FLOAT32:
        return Int64(load_f32(Span(a.values), i))
    if id == AT_FLOAT64:
        return Int64(load_f64(Span(a.values), i))
    raise Error(
        "iceberg: cannot read an integer from Arrow type " + String(a.type)
    )


def float_at(a: ArrayData, i: Int) raises -> Float64:
    var id = a.type.id
    if id == AT_FLOAT32:
        return Float64(load_f32(Span(a.values), i))
    if id == AT_FLOAT64:
        return load_f64(Span(a.values), i)
    return Float64(int_at(a, i))


def value_extent(a: ArrayData, i: Int) raises -> Tuple[Int, Int]:
    """`(start, end)` of the raw bytes of element `i` in `a.values`."""
    var id = a.type.id
    if id == AT_UTF8 or id == AT_BINARY:
        return (Int(a.offsets[i]), Int(a.offsets[i + 1]))
    if id == AT_LARGE_UTF8 or id == AT_LARGE_BINARY:
        return (Int(a.large_offsets[i]), Int(a.large_offsets[i + 1]))
    if id == AT_FIXED_SIZE_BINARY or id == AT_DECIMAL128:
        var w = a.type.fixed_width()
        return (w * i, w * i + w)
    raise Error("iceberg: cannot read bytes from Arrow type " + String(a.type))


def bytes_at(a: ArrayData, i: Int) raises -> List[UInt8]:
    var extent = value_extent(a, i)
    var out = List[UInt8](capacity=extent[1] - extent[0])
    for k in range(extent[0], extent[1]):
        out.append(a.values[k])
    return out^


# ── one value, as a tagged `Datum` ──────────────────────────────────────────
def extract_datum(
    a: ArrayData, i: Int, kind: UInt8, precision: Int, scale: Int, length: Int
) raises -> Datum:
    """Element `i`, typed as the table's current schema says it is.

    This is the only place a scan builds a per-cell tagged value, and it is
    built on demand: `ScanResult.value`, CSV and JSON output, the partition
    transforms a write applies, and the handful of predicate shapes no
    vectorised kernel covers.
    """
    if not a.is_valid(i):
        return Datum.none()
    if kind == P_BOOLEAN:
        return Datum.bool_(int_at(a, i) != 0)
    if kind == P_FLOAT:
        return Datum.float_(Float64(Float32(float_at(a, i))))
    if kind == P_DOUBLE:
        return Datum.double_(float_at(a, i))
    if kind == P_STRING:
        var b = bytes_at(a, i)
        return Datum.string_(String(StringSlice(unsafe_from_utf8=Span(b))))
    if kind == P_UUID:
        var b = bytes_at(a, i)
        return Datum.uuid_(b^)
    if kind == P_FIXED:
        var b = bytes_at(a, i)
        return Datum.fixed_(b^)
    if kind == P_BINARY or kind == P_UNKNOWN:
        var b = bytes_at(a, i)
        return Datum.binary_(b^)
    if kind == P_DECIMAL:
        # Arrow decimal128 is little-endian; Iceberg's is big-endian minimal.
        var le = bytes_at(a, i)
        var be = List[UInt8]()
        for k in range(len(le)):
            be.append(le[len(le) - 1 - k])
        return Datum.decimal_(be^, precision, scale)
    # int, long, date, time, timestamp and their nanosecond forms.
    return Datum.integral(kind, int_at(a, i))


# ── filter ──────────────────────────────────────────────────────────────────
def filter_array(
    a: ArrayData, keep: List[Bool], n_keep: Int
) raises -> ArrayData:
    """The rows of `a` for which `keep` is true, in order.

    Kept rows are copied in **runs**, not one at a time: a filter over a
    million rows is usually either a scattering of long runs (a range
    predicate over a sorted column) or a dense sample (an equality predicate
    over a low-cardinality one), and both come out of the same loop — find how
    far the run of kept rows extends, then move that whole span with one
    `extend`. A byte-at-a-time copy of a fixed-width column cost eight bounds
    checks and eight capacity checks per value.
    """
    var out = ArrayData(a.type.copy(), a.name.copy())
    out.nullable = a.nullable
    out.field_id = a.field_id
    out.length = n_keep
    var no_nulls = len(a.validity) == 0
    if is_binary_type(a.type):
        out.offsets = List[Int32](capacity=n_keep + 1)
        out.offsets.append(0)
    elif is_large_binary_type(a.type):
        out.large_offsets = List[Int64](capacity=n_keep + 1)
        out.large_offsets.append(0)
    var width = a.type.fixed_width()
    if width > 0 and a.type.id != AT_BOOL:
        out.values = List[UInt8](capacity=width * n_keep)
    if not no_nulls:
        out.validity = List[UInt8](length=(n_keep + 7) // 8, fill=0)
    var n = len(keep)
    var j = 0
    var i = 0
    while i < n:
        if not keep[i]:
            i += 1
            continue
        # `[i, end)` is a maximal run of kept rows.
        var end = i + 1
        while end < n and keep[end]:
            end += 1
        var count = end - i
        if not no_nulls:
            for r in range(i, end):
                var valid = bit_get(Span(a.validity), r)
                if valid:
                    bit_set(out.validity, j + (r - i), True)
                else:
                    out.null_count += 1
        if is_var_width(a.type):
            var lo = value_extent(a, i)[0]
            var hi = value_extent(a, end - 1)[1]
            var shift = len(out.values) - lo
            out.values.extend(Span(a.values)[lo:hi])
            if is_binary_type(a.type):
                for r in range(i + 1, end + 1):
                    out.offsets.append(Int32(Int(a.offsets[r]) + shift))
            else:
                for r in range(i + 1, end + 1):
                    out.large_offsets.append(a.large_offsets[r] + Int64(shift))
        elif a.type.id == AT_BOOL:
            while len(out.values) < (j + count + 7) // 8:
                out.values.append(0)
            for r in range(i, end):
                if bit_get(Span(a.values), r):
                    var at = j + (r - i)
                    out.values[at // 8] |= UInt8(1) << UInt8(at % 8)
        elif width > 0:
            out.values.extend(Span(a.values)[width * i : width * end])
        j += count
        i = end
    if out.null_count == 0:
        # No kept row was null, so Arrow's "buffer absent" encoding says it.
        out.validity.clear()
    if a.type.id == AT_BOOL:
        while len(out.values) < (n_keep + 7) // 8:
            out.values.append(0)
    return out^


# ── concat ──────────────────────────────────────────────────────────────────
def concat_into(mut dst: ArrayData, src: ArrayData) raises:
    """Append every row of `src` to `dst`; both must have the same type.

    The common case — neither side has a null — never touches a validity
    bitmap: Arrow's "buffer absent" encoding already says every row is valid,
    and materialising a million all-ones bits only to clear them again was
    most of what concatenating a scan's batches cost.
    """
    if src.length == 0:
        return
    if (
        dst.length == 0
        and len(dst.values) == 0
        and len(dst.offsets) <= 1
        and len(dst.large_offsets) <= 1
    ):
        # An empty destination adopts the source outright.
        var t = dst.type.copy()
        var nm = dst.name.copy()
        var fid = dst.field_id
        dst = src.copy()
        dst.type = t^
        dst.name = nm^
        dst.field_id = fid
        return
    var base = dst.length
    var src_no_nulls = len(src.validity) == 0
    var dst_no_nulls = len(dst.validity) == 0
    if not (src_no_nulls and dst_no_nulls):
        if dst_no_nulls and base > 0:
            # `dst` had no nulls at all; materialise its bitmap before
            # appending.
            var whole = base // 8
            for _ in range(whole):
                dst.validity.append(0xFF)
            if base % 8:
                dst.validity.append((UInt8(1) << UInt8(base % 8)) - 1)
        dst.validity.resize((base + src.length + 7) // 8, 0)
        if src_no_nulls:
            for i in range(src.length):
                bit_set(dst.validity, base + i, True)
        else:
            for i in range(src.length):
                var valid = bit_get(Span(src.validity), i)
                bit_set(dst.validity, base + i, valid)
                if not valid:
                    dst.null_count += 1
    if is_var_width(src.type):
        var shift = len(dst.values)
        dst.values.extend(Span(src.values))
        if is_binary_type(src.type):
            dst.offsets.reserve(len(dst.offsets) + len(src.offsets))
            for k in range(1, len(src.offsets)):
                dst.offsets.append(Int32(Int(src.offsets[k]) + shift))
        else:
            dst.large_offsets.reserve(
                len(dst.large_offsets) + len(src.large_offsets)
            )
            for k in range(1, len(src.large_offsets)):
                dst.large_offsets.append(src.large_offsets[k] + Int64(shift))
    elif src.type.id == AT_BOOL:
        dst.values.resize((base + src.length + 7) // 8, 0)
        if base % 8 == 0:
            var whole = src.length // 8
            for b in range(whole):
                dst.values[base // 8 + b] = src.values[b]
            for i in range(whole * 8, src.length):
                var j = base + i
                if bit_get(Span(src.values), i):
                    dst.values[j // 8] |= UInt8(1) << UInt8(j % 8)
        else:
            for i in range(src.length):
                var j = base + i
                if bit_get(Span(src.values), i):
                    dst.values[j // 8] |= UInt8(1) << UInt8(j % 8)
                else:
                    dst.values[j // 8] &= ~(UInt8(1) << UInt8(j % 8))
    else:
        dst.values.extend(Span(src.values))
    dst.length = base + src.length
    if len(dst.validity) > 0:
        dst.validity.resize((dst.length + 7) // 8, 0)
        if dst.null_count == 0:
            dst.validity.clear()


# ── key encoding, for equality deletes ──────────────────────────────────────
def append_key(mut key: List[UInt8], a: ArrayData, i: Int) raises:
    """Append element `i` of `a` to a canonical key buffer.

    A null contributes a single `0` tag, a value a `1` tag and its Arrow bytes.
    Two rows produce the same key exactly when their values are equal — which
    is what an equality delete asks — and `null` matches `null`, as the spec
    requires.
    """
    var valid = len(a.validity) == 0 or bit_get(Span(a.validity), i)
    if not valid:
        key.append(0)
        return
    key.append(1)
    if (
        is_var_width(a.type)
        or a.type.id == AT_FIXED_SIZE_BINARY
        or a.type.id == AT_DECIMAL128
    ):
        var extent = value_extent(a, i)
        var n = extent[1] - extent[0]
        _put_le(key, UInt64(n), 4)
        for k in range(extent[0], extent[1]):
            key.append(a.values[k])
        return
    if a.type.id == AT_BOOL:
        key.append(UInt8(1) if bit_get(Span(a.values), i) else UInt8(0))
        return
    var w = a.type.fixed_width()
    for k in range(w * i, w * i + w):
        key.append(a.values[k])


def hash_key(key: List[UInt8]) -> UInt64:
    """FNV-1a, which is enough to bucket a delete set."""
    var h: UInt64 = 0xCBF29CE484222325
    for k in range(len(key)):
        h ^= UInt64(key[k])
        h *= 0x100000001B3
    return h


def keys_equal(a: List[UInt8], b: List[UInt8]) -> Bool:
    if len(a) != len(b):
        return False
    for k in range(len(a)):
        if a[k] != b[k]:
            return False
    return True
