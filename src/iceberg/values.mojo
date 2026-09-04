"""Typed single values (`Datum`) and Iceberg's two single-value encodings.

A `Datum` is one primitive value tagged with its Iceberg type. It is what
predicates compare against, what a partition transform consumes and produces,
and what a manifest's `lower_bound` / `upper_bound` bytes decode into.

Two serializations matter for reading:

* **Appendix D, binary** — `lower_bounds`, `upper_bounds` and partition field
  summaries. `datum_from_bytes` / `datum_to_bytes`.
* **Appendix D, JSON** — `initial-default`, `write-default` and the JSON forms
  of partition values. `datum_from_json`.

Integers, dates, times and timestamps all ride in `Datum.i` (`Int64`); floats
and doubles in `Datum.f`; strings in `Datum.s`; everything byte-shaped
(`binary`, `fixed`, `uuid`, and a decimal's unscaled two's-complement) in
`Datum.b`.

Decimals carry both forms: `b` holds the minimal big-endian two's-complement
unscaled value (what bucket hashing and the binary encoding need) and `i` holds
the same value as an `Int64` when precision ≤ 18 (what truncation needs).
Comparison always goes through the bytes, so decimals wider than 64 bits still
compare correctly even though this build cannot do arithmetic on them.
"""

from std.memory import bitcast

from .json import (
    Json,
    parse_json,
    parse_int64,
    json_quote,
    substr,
    format_double,
)
from .types import (
    P_BOOLEAN,
    P_INT,
    P_LONG,
    P_FLOAT,
    P_DOUBLE,
    P_DATE,
    P_TIME,
    P_TIMESTAMP,
    P_TIMESTAMPTZ,
    P_TIMESTAMP_NS,
    P_TIMESTAMPTZ_NS,
    P_STRING,
    P_UUID,
    P_FIXED,
    P_BINARY,
    P_DECIMAL,
    P_UNKNOWN,
    P_VARIANT,
    P_GEOMETRY,
    P_GEOGRAPHY,
    P_UNRECOGNIZED,
    primitive_name,
    is_integer_like,
    TypeStore,
    TK_PRIMITIVE,
)


@fieldwise_init
struct Datum(Copyable, Movable, Writable):
    """One primitive Iceberg value, tagged with its type."""

    var kind: UInt8
    var i: Int64
    var f: Float64
    var s: String
    var b: List[UInt8]
    var precision: Int
    var scale: Int
    var length: Int
    var valid: Bool
    """False for "no value" — an absent bound, a null partition value."""

    @staticmethod
    def none() -> Self:
        return Self(P_UNKNOWN, 0, 0.0, "", [], 0, 0, 0, False)

    @staticmethod
    def bool_(v: Bool) -> Self:
        return Self(
            P_BOOLEAN, Int64(1) if v else Int64(0), 0.0, "", [], 0, 0, 0, True
        )

    @staticmethod
    def int_(v: Int64) -> Self:
        return Self(P_INT, v, 0.0, "", [], 0, 0, 0, True)

    @staticmethod
    def long_(v: Int64) -> Self:
        return Self(P_LONG, v, 0.0, "", [], 0, 0, 0, True)

    @staticmethod
    def integral(kind: UInt8, v: Int64) -> Self:
        """`int`/`long`/`date`/`time`/`timestamp[_ns][tz]` — all carried as Int64."""
        return Self(kind, v, 0.0, "", [], 0, 0, 0, True)

    @staticmethod
    def float_(v: Float64) -> Self:
        return Self(P_FLOAT, 0, v, "", [], 0, 0, 0, True)

    @staticmethod
    def double_(v: Float64) -> Self:
        return Self(P_DOUBLE, 0, v, "", [], 0, 0, 0, True)

    @staticmethod
    def string_(var v: String) -> Self:
        return Self(P_STRING, 0, 0.0, v^, [], 0, 0, 0, True)

    @staticmethod
    def binary_(var v: List[UInt8]) -> Self:
        return Self(P_BINARY, 0, 0.0, "", v^, 0, 0, 0, True)

    @staticmethod
    def fixed_(var v: List[UInt8]) -> Self:
        var n = len(v)
        return Self(P_FIXED, 0, 0.0, "", v^, 0, 0, n, True)

    @staticmethod
    def uuid_(var v: List[UInt8]) -> Self:
        return Self(P_UUID, 0, 0.0, "", v^, 0, 0, 16, True)

    @staticmethod
    def decimal_(
        var unscaled_be: List[UInt8], precision: Int, scale: Int
    ) -> Self:
        var iv = be_twos_to_int64(unscaled_be)
        return Self(
            P_DECIMAL, iv, 0.0, "", unscaled_be^, precision, scale, 0, True
        )

    @staticmethod
    def decimal_int(unscaled: Int64, precision: Int, scale: Int) -> Self:
        var b = int64_to_be_twos(unscaled)
        return Self(P_DECIMAL, unscaled, 0.0, "", b^, precision, scale, 0, True)

    def is_nan(self) -> Bool:
        return (
            self.kind == P_FLOAT or self.kind == P_DOUBLE
        ) and self.f != self.f

    def write_to(self, mut writer: Some[Writer]):
        if not self.valid:
            writer.write("None")
            return
        writer.write(primitive_name(self.kind), "(", self.repr_(), ")")

    def repr_(self) -> String:
        if not self.valid:
            return "null"
        if self.kind == P_BOOLEAN:
            return "true" if self.i != 0 else "false"
        if self.kind == P_FLOAT or self.kind == P_DOUBLE:
            return format_double(self.f)
        if self.kind == P_STRING:
            return self.s
        if self.kind == P_DECIMAL:
            return decimal_text(self.b, self.scale)
        if self.kind == P_UUID:
            return uuid_text(self.b)
        if self.kind == P_BINARY or self.kind == P_FIXED:
            return hex_text(self.b)
        return String(self.i)

    def to_json(self) -> String:
        """Appendix D, JSON half."""
        if not self.valid:
            return "null"
        if self.kind == P_BOOLEAN:
            return "true" if self.i != 0 else "false"
        if self.kind == P_FLOAT or self.kind == P_DOUBLE:
            return format_double(self.f)
        if is_integer_like(self.kind):
            if self.kind == P_INT or self.kind == P_LONG:
                return String(self.i)
            # date/time/timestamp are ISO strings in JSON.
            return json_quote(iso_text(self.kind, self.i))
        if self.kind == P_STRING:
            return json_quote(self.s)
        if self.kind == P_DECIMAL:
            return json_quote(decimal_text(self.b, self.scale))
        if self.kind == P_UUID:
            return json_quote(uuid_text(self.b))
        return json_quote(hex_text(self.b))


# ── ordering ────────────────────────────────────────────────────────────────
def compare(a: Datum, b: Datum) raises -> Int:
    """Iceberg's natural order: -1 / 0 / 1. Both values must be comparable."""
    if a.kind == P_STRING or b.kind == P_STRING:
        if a.s < b.s:
            return -1
        return 0 if a.s == b.s else 1
    if a.kind == P_DECIMAL and b.kind == P_DECIMAL:
        return cmp_be_twos(a.b, b.b)
    if (
        a.kind == P_FLOAT
        or a.kind == P_DOUBLE
        or b.kind == P_FLOAT
        or b.kind == P_DOUBLE
    ):
        var x = a.f if (a.kind == P_FLOAT or a.kind == P_DOUBLE) else Float64(
            a.i
        )
        var y = b.f if (b.kind == P_FLOAT or b.kind == P_DOUBLE) else Float64(
            b.i
        )
        # Iceberg sorts NaN last and treats -0.0 == 0.0.
        var xn = x != x
        var yn = y != y
        if xn and yn:
            return 0
        if xn:
            return 1
        if yn:
            return -1
        if x < y:
            return -1
        return 0 if x == y else 1
    if a.kind == P_UUID or a.kind == P_BINARY or a.kind == P_FIXED:
        return cmp_unsigned(a.b, b.b)
    if a.i < b.i:
        return -1
    return 0 if a.i == b.i else 1


def cmp_unsigned(a: List[UInt8], b: List[UInt8]) -> Int:
    """Lexicographic unsigned-byte order (uuid, fixed, binary)."""
    var n = len(a) if len(a) < len(b) else len(b)
    for k in range(n):
        if a[k] != b[k]:
            return -1 if a[k] < b[k] else 1
    if len(a) == len(b):
        return 0
    return -1 if len(a) < len(b) else 1


def cmp_be_twos(a: List[UInt8], b: List[UInt8]) -> Int:
    """Signed compare of two minimal big-endian two's-complement integers."""
    var an = len(a) > 0 and (a[0] & 0x80) != 0
    var bn = len(b) > 0 and (b[0] & 0x80) != 0
    if an != bn:
        return -1 if an else 1
    # Same sign: sign-extend to a common width, then compare byte by byte.
    var w = len(a) if len(a) > len(b) else len(b)
    var pad_a: UInt8 = 0xFF if an else 0
    var pad_b: UInt8 = 0xFF if bn else 0
    for k in range(w):
        var ia = k - (w - len(a))
        var ib = k - (w - len(b))
        var x = a[ia] if ia >= 0 else pad_a
        var y = b[ib] if ib >= 0 else pad_b
        if x != y:
            return -1 if x < y else 1
    return 0


# ── two's-complement helpers ────────────────────────────────────────────────
def be_twos_to_int64(b: List[UInt8]) -> Int64:
    """Big-endian two's complement → Int64. Wider-than-64-bit input saturates.
    """
    if len(b) == 0:
        return 0
    var neg = (b[0] & 0x80) != 0
    var v: UInt64 = 0xFFFFFFFFFFFFFFFF if neg else 0
    var start = 0
    if len(b) > 8:
        start = len(b) - 8
    for k in range(start, len(b)):
        v = (v << 8) | UInt64(b[k])
    return Int64(v)


def int64_to_be_twos(v: Int64) -> List[UInt8]:
    """Int64 → the *minimal* big-endian two's-complement byte string."""
    var full = List[UInt8]()
    var u = UInt64(v)
    for k in range(8):
        full.append(UInt8((u >> UInt64(56 - 8 * k)) & 0xFF))
    # Trim leading 0x00 (positive) / 0xFF (negative) while the sign survives.
    var start = 0
    while start < 7:
        var lead = full[start]
        var nxt = full[start + 1]
        if lead == 0 and (nxt & 0x80) == 0:
            start += 1
        elif lead == 0xFF and (nxt & 0x80) != 0:
            start += 1
        else:
            break
    var out = List[UInt8]()
    for k in range(start, 8):
        out.append(full[k])
    return out^


# ── text renderings ─────────────────────────────────────────────────────────
comptime _HEXDIG = String("0123456789abcdef")


def hex_text(b: List[UInt8]) -> String:
    var out = String("")
    for k in range(len(b)):
        out += String(_HEXDIG[byte=Int(b[k] >> 4)])
        out += String(_HEXDIG[byte=Int(b[k] & 0xF)])
    return out^


def hex_bytes(s: String) raises -> List[UInt8]:
    var out = List[UInt8]()
    var t = s
    if t.startswith("0x") or t.startswith("0X"):
        t = substr(t, 2, t.byte_length())
    var b = t.as_bytes()
    if len(b) % 2 != 0:
        raise Error("iceberg: hex string of odd length")
    for k in range(0, len(b), 2):
        out.append(UInt8(_hexval(b[k]) * 16 + _hexval(b[k + 1])))
    return out^


def _hexval(c: UInt8) raises -> Int:
    var v = Int(c)
    if v >= 48 and v <= 57:
        return v - 48
    if v >= 97 and v <= 102:
        return v - 87
    if v >= 65 and v <= 70:
        return v - 55
    raise Error("iceberg: bad hex digit")


def uuid_text(b: List[UInt8]) -> String:
    if len(b) != 16:
        return hex_text(b)
    var h = hex_text(b)
    return (
        substr(h, 0, 8)
        + "-"
        + substr(h, 8, 12)
        + "-"
        + substr(h, 12, 16)
        + "-"
        + substr(h, 16, 20)
        + "-"
        + substr(h, 20, 32)
    )


def uuid_bytes(s: String) raises -> List[UInt8]:
    var clean = String("")
    var b = s.as_bytes()
    for k in range(len(b)):
        if b[k] != UInt8(45):  # '-'
            clean += String(StringSlice(unsafe_from_utf8=Span(b)[k : k + 1]))
    if clean.byte_length() != 32:
        raise Error("iceberg: '" + s + "' is not a uuid")
    return hex_bytes(clean)


# ── arbitrary-precision decimal helpers ─────────────────────────────────────
# Iceberg allows decimal(38, S), whose unscaled value needs 128 bits. Datum
# keeps the authoritative form as minimal big-endian two's complement bytes and
# only mirrors it into `Datum.i` when it fits, so parsing and rendering work on
# the byte string directly rather than through Int64.


def _mag_mul10_add(mut mag: List[UInt8], d: Int):
    """Big-endian unsigned magnitude: mag = mag * 10 + d."""
    var carry = d
    for k in range(len(mag) - 1, -1, -1):
        var v = Int(mag[k]) * 10 + carry
        mag[k] = UInt8(v & 0xFF)
        carry = v >> 8
    while carry != 0:
        mag.insert(0, UInt8(carry & 0xFF))
        carry = carry >> 8


def _mag_divmod10(mut mag: List[UInt8]) -> Int:
    """Divide a big-endian magnitude by 10 in place; return the remainder."""
    var rem = 0
    for k in range(len(mag)):
        var v = rem * 256 + Int(mag[k])
        mag[k] = UInt8(v // 10)
        rem = v % 10
    while len(mag) > 1 and mag[0] == 0:
        _ = mag.pop(0)
    return rem


def _mag_is_zero(mag: List[UInt8]) -> Bool:
    for k in range(len(mag)):
        if mag[k] != 0:
            return False
    return True


def _mag_to_twos(var mag: List[UInt8], neg: Bool) -> List[UInt8]:
    """Unsigned magnitude → minimal big-endian two's complement."""
    while len(mag) > 1 and mag[0] == 0:
        _ = mag.pop(0)
    if len(mag) == 0:
        mag.append(0)
    if not neg:
        # A leading byte with its top bit set would read as negative.
        if (mag[0] & 0x80) != 0:
            mag.insert(0, 0)
        return mag^
    # Two's complement: invert then add one, widening first if needed.
    if (mag[0] & 0x80) != 0:
        mag.insert(0, 0)
    var out = List[UInt8]()
    for k in range(len(mag)):
        out.append(~mag[k])
    var carry = 1
    for k in range(len(out) - 1, -1, -1):
        var v = Int(out[k]) + carry
        out[k] = UInt8(v & 0xFF)
        carry = v >> 8
        if carry == 0:
            break
    # Trim redundant sign bytes.
    while len(out) > 1 and out[0] == 0xFF and (out[1] & 0x80) != 0:
        _ = out.pop(0)
    return out^


def _twos_to_mag(b: List[UInt8]) -> List[UInt8]:
    """Minimal two's complement → the magnitude of its absolute value."""
    var neg = len(b) > 0 and (b[0] & 0x80) != 0
    var out = List[UInt8]()
    if not neg:
        for k in range(len(b)):
            out.append(b[k])
        while len(out) > 1 and out[0] == 0:
            _ = out.pop(0)
        return out^
    for k in range(len(b)):
        out.append(~b[k])
    var carry = 1
    for k in range(len(out) - 1, -1, -1):
        var v = Int(out[k]) + carry
        out[k] = UInt8(v & 0xFF)
        carry = v >> 8
        if carry == 0:
            break
    while len(out) > 1 and out[0] == 0:
        _ = out.pop(0)
    return out^


def decimal_text(unscaled_be: List[UInt8], scale: Int) -> String:
    """Render a decimal from its unscaled two's-complement value and scale."""
    var neg = len(unscaled_be) > 0 and (unscaled_be[0] & 0x80) != 0
    var mag = _twos_to_mag(unscaled_be)
    var digits = String("")
    if _mag_is_zero(mag):
        digits = "0"
    else:
        while not _mag_is_zero(mag):
            digits = String(_mag_divmod10(mag)) + digits
    if scale <= 0:
        return ("-" if neg else "") + digits
    while digits.byte_length() <= scale:
        digits = "0" + digits
    var cut = digits.byte_length() - scale
    return (
        ("-" if neg else "")
        + substr(digits, 0, cut)
        + "."
        + substr(digits, cut, digits.byte_length())
    )


def decimal_from_text(s: String, precision: Int, scale: Int) raises -> Datum:
    """Parse decimal text at the column's scale, to full 38-digit width."""
    var neg = s.startswith("-")
    var t = substr(s, 1, s.byte_length()) if (neg or s.startswith("+")) else s
    var dot = t.find(".")
    var digits: String
    var frac = 0
    if dot < 0:
        digits = t
    else:
        digits = substr(t, 0, dot) + substr(t, dot + 1, t.byte_length())
        frac = t.byte_length() - dot - 1
    while frac < scale:
        digits += "0"
        frac += 1
    while frac > scale:
        digits = substr(digits, 0, digits.byte_length() - 1)
        frac -= 1
    var mag = List[UInt8]()
    mag.append(0)
    var db = digits.as_bytes()
    for k in range(len(db)):
        var c = Int(db[k])
        if c < 48 or c > 57:
            raise Error("iceberg: '" + s + "' is not a decimal")
        _mag_mul10_add(mag, c - 48)
    var be = _mag_to_twos(mag^, neg)
    return Datum.decimal_(be^, precision, scale)


# ── date / time arithmetic (proleptic Gregorian, floor semantics) ───────────
def floor_div(a: Int64, b: Int64) -> Int64:
    """Division that rounds toward negative infinity.

    Mojo's `//` on integers already floors (Python semantics, verified on both
    stable 1.0.0 and the nightly: `-1 // 86400000000 == -1`, `-7 // 2 == -4`),
    so this is a rename that documents the intent. Every date and timestamp
    calculation in Iceberg needs floor, not truncation, or pre-epoch values
    land a day early.
    """
    return a // b


def floor_mod(a: Int64, b: Int64) -> Int64:
    """Remainder with the sign of the divisor — `%` already does this in Mojo.
    """
    return a % b


def civil_from_days(z_in: Int64) -> List[Int64]:
    """Days since 1970-01-01 → [year, month, day]. Howard Hinnant's algorithm.
    """
    var z = z_in + 719468
    var era = floor_div(z, 146097)
    var doe = z - era * 146097
    var yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    var y = yoe + era * 400
    var doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    var mp = (5 * doy + 2) // 153
    var d = doy - (153 * mp + 2) // 5 + 1
    var m = mp + 3 if mp < 10 else mp - 9
    return [y + (Int64(1) if m <= 2 else Int64(0)), m, d]


def days_from_civil(y_in: Int64, m: Int64, d: Int64) -> Int64:
    var y = y_in - (Int64(1) if m <= 2 else Int64(0))
    var era = floor_div(y, 400)
    var yoe = y - era * 400
    var mp = m + 9 if m <= 2 else m - 3
    var doy = (153 * mp + 2) // 5 + d - 1
    var doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


comptime MICROS_PER_DAY: Int64 = 86400000000
comptime MICROS_PER_HOUR: Int64 = 3600000000
comptime NANOS_PER_MICRO: Int64 = 1000


def iso_text(kind: UInt8, v: Int64) -> String:
    """ISO-8601 rendering used by the JSON single-value form."""
    if kind == P_DATE:
        var c = civil_from_days(v)
        return _pad4(c[0]) + "-" + _pad2(c[1]) + "-" + _pad2(c[2])
    if kind == P_TIME:
        return _time_text(v, 6)
    var micros = v
    var frac_digits = 6
    if kind == P_TIMESTAMP_NS or kind == P_TIMESTAMPTZ_NS:
        micros = v
        frac_digits = 9
    var unit: Int64 = MICROS_PER_DAY
    if frac_digits == 9:
        unit = MICROS_PER_DAY * 1000
    var days = floor_div(micros, unit)
    var rem = micros - days * unit
    var c = civil_from_days(days)
    var out = _pad4(c[0]) + "-" + _pad2(c[1]) + "-" + _pad2(c[2]) + "T"
    out += _time_text(rem, frac_digits)
    if kind == P_TIMESTAMPTZ or kind == P_TIMESTAMPTZ_NS:
        out += "+00:00"
    return out^


def _time_text(v: Int64, frac_digits: Int) -> String:
    var per_sec: Int64 = 1000000 if frac_digits == 6 else 1000000000
    var secs = floor_div(v, per_sec)
    var frac = v - secs * per_sec
    var h = secs // 3600
    var mi = (secs % 3600) // 60
    var s = secs % 60
    var out = _pad2(h) + ":" + _pad2(mi) + ":" + _pad2(s)
    if frac != 0:
        var fs = String(frac)
        while fs.byte_length() < frac_digits:
            fs = "0" + fs
        while fs.byte_length() > 1 and fs.as_bytes()[
            fs.byte_length() - 1
        ] == UInt8(48):
            fs = substr(fs, 0, fs.byte_length() - 1)
        out += "." + fs
    return out^


def _pad2(v: Int64) -> String:
    var s = String(v)
    return s if s.byte_length() >= 2 else "0" + s


def _pad4(v: Int64) -> String:
    if v < 0:
        var s = String(-v)
        while s.byte_length() < 4:
            s = "0" + s
        return "-" + s
    var s = String(v)
    while s.byte_length() < 4:
        s = "0" + s
    return s^


def parse_iso(kind: UInt8, text: String) raises -> Int64:
    """ISO-8601 → the integer Iceberg stores for `kind`."""
    var s = text
    if kind == P_DATE:
        var p = _split_date(s)
        return days_from_civil(p[0], p[1], p[2])
    if kind == P_TIME:
        return _parse_time(s, 1000000)
    # timestamp: <date>T<time>[Z|+hh:mm]
    var tpos = s.find("T")
    if tpos < 0:
        tpos = s.find(" ")
    if tpos < 0:
        raise Error("iceberg: '" + text + "' is not a timestamp")
    var date_part = substr(s, 0, tpos)
    var time_part = substr(s, tpos + 1, s.byte_length())
    var offset_secs: Int64 = 0
    if time_part.endswith("Z") or time_part.endswith("z"):
        time_part = substr(time_part, 0, time_part.byte_length() - 1)
    else:
        var pp = _find_offset(time_part)
        if pp >= 0:
            var sign: Int64 = -1 if time_part.as_bytes()[pp] == UInt8(43) else 1
            var off = substr(time_part, pp + 1, time_part.byte_length())
            var colon = off.find(":")
            var oh = parse_int64(
                String(substr(off, 0, colon))
            ) if colon > 0 else parse_int64(off)
            var om = parse_int64(
                String(substr(off, colon + 1, off.byte_length()))
            ) if colon > 0 else Int64(0)
            offset_secs = sign * (oh * 3600 + om * 60)
            time_part = substr(time_part, 0, pp)
    var per_sec: Int64 = 1000000
    if kind == P_TIMESTAMP_NS or kind == P_TIMESTAMPTZ_NS:
        per_sec = 1000000000
    var d = _split_date(date_part)
    var days = days_from_civil(d[0], d[1], d[2])
    var tod = _parse_time(time_part, per_sec)
    return days * (86400 * per_sec) + tod + offset_secs * per_sec


def _find_offset(t: String) -> Int:
    var b = t.as_bytes()
    for k in range(len(b) - 1, 0, -1):
        if b[k] == UInt8(43) or b[k] == UInt8(45):  # '+' '-'
            return k
    return -1


def _split_date(s: String) raises -> List[Int64]:
    var neg = s.startswith("-")
    var t = substr(s, 1, s.byte_length()) if neg else s
    var a = t.find("-")
    var b = t.find("-", a + 1)
    if a < 0 or b < 0:
        raise Error("iceberg: '" + s + "' is not a date")
    var y = parse_int64(String(substr(t, 0, a)))
    return [
        -y if neg else y,
        parse_int64(String(substr(t, a + 1, b))),
        parse_int64(String(substr(t, b + 1, t.byte_length()))),
    ]


def _parse_time(s: String, per_sec: Int64) raises -> Int64:
    var a = s.find(":")
    if a < 0:
        raise Error("iceberg: '" + s + "' is not a time")
    var b = s.find(":", a + 1)
    var h = parse_int64(String(substr(s, 0, a)))
    var mi: Int64
    var sec: Int64 = 0
    var frac: Int64 = 0
    if b < 0:
        mi = parse_int64(String(substr(s, a + 1, s.byte_length())))
    else:
        mi = parse_int64(String(substr(s, a + 1, b)))
        var rest = substr(s, b + 1, s.byte_length())
        var dot = rest.find(".")
        if dot < 0:
            sec = parse_int64(String(rest))
        else:
            sec = parse_int64(String(substr(rest, 0, dot)))
            var fs = substr(rest, dot + 1, rest.byte_length())
            var want = 6 if per_sec == 1000000 else 9
            while fs.byte_length() < want:
                fs += "0"
            frac = parse_int64(String(substr(fs, 0, want)))
    return (h * 3600 + mi * 60 + sec) * per_sec + frac


# ── Appendix D: binary single-value serialization ───────────────────────────
def _le_int(b: Span[UInt8, _], n: Int) -> Int64:
    """Little-endian signed integer of `n` bytes."""
    var v: UInt64 = 0
    for k in range(n):
        v |= UInt64(b[k]) << UInt64(8 * k)
    if n < 8 and (b[n - 1] & 0x80) != 0:
        # Sign-extend.
        for k in range(n, 8):
            v |= UInt64(0xFF) << UInt64(8 * k)
    return Int64(v)


def _le_bytes(v: Int64, n: Int) -> List[UInt8]:
    var out = List[UInt8]()
    var u = UInt64(v)
    for k in range(n):
        out.append(UInt8((u >> UInt64(8 * k)) & 0xFF))
    return out^


def datum_from_bytes(
    store: TypeStore, type_idx: Int, b: List[UInt8]
) raises -> Datum:
    """Decode Appendix-D binary bytes as the type at `type_idx`.

    Byte length disambiguates *promoted* columns: a `long` column whose bounds
    were written while it was still an `int` carries 4 bytes, a `double`
    promoted from `float` carries 4, and a `timestamp` promoted from `date`
    carries 4. The spec requires readers to infer the original type from the
    length rather than mis-decode.
    """
    ref n = store.nodes[type_idx]
    if n.kind != TK_PRIMITIVE:
        raise Error("iceberg: cannot decode a bound for a nested type")
    return datum_from_bytes_prim(n.prim, n.precision, n.scale, n.length, b)


def datum_from_bytes_prim(
    prim: UInt8, precision: Int, scale: Int, length: Int, b: List[UInt8]
) raises -> Datum:
    var sp = Span(b)
    if prim == P_BOOLEAN:
        return Datum.bool_(len(b) > 0 and b[0] != 0)
    if prim == P_INT:
        return Datum.int_(_le_int(sp, 4))
    if prim == P_LONG:
        if len(b) == 4:
            return Datum.long_(_le_int(sp, 4))  # promoted int→long
        return Datum.long_(_le_int(sp, 8))
    if prim == P_FLOAT:
        return Datum.float_(Float64(_bits_to_f32(_le_int(sp, 4))))
    if prim == P_DOUBLE:
        if len(b) == 4:
            return Datum.double_(Float64(_bits_to_f32(_le_int(sp, 4))))
        return Datum.double_(_bits_to_f64(_le_int(sp, 8)))
    if prim == P_DATE:
        return Datum.integral(P_DATE, _le_int(sp, 4))
    if prim == P_TIME:
        return Datum.integral(P_TIME, _le_int(sp, 8))
    if prim == P_TIMESTAMP or prim == P_TIMESTAMPTZ:
        if len(b) == 4:
            # Promoted date→timestamp: days become microseconds.
            return Datum.integral(prim, _le_int(sp, 4) * MICROS_PER_DAY)
        return Datum.integral(prim, _le_int(sp, 8))
    if prim == P_TIMESTAMP_NS or prim == P_TIMESTAMPTZ_NS:
        if len(b) == 4:
            return Datum.integral(prim, _le_int(sp, 4) * MICROS_PER_DAY * 1000)
        return Datum.integral(prim, _le_int(sp, 8))
    if prim == P_STRING:
        return Datum.string_(String(StringSlice(unsafe_from_utf8=sp)))
    if prim == P_UUID:
        return Datum.uuid_(b.copy())
    if prim == P_FIXED:
        var d = Datum.fixed_(b.copy())
        d.length = length if length > 0 else len(b)
        return d^
    if prim == P_DECIMAL:
        return Datum.decimal_(b.copy(), precision, scale)
    if prim == P_UNKNOWN or prim == P_VARIANT:
        return Datum.none()
    # geometry / geography / unrecognised: keep the raw bytes.
    return Datum.binary_(b.copy())


def datum_to_bytes(d: Datum) raises -> List[UInt8]:
    """Encode a `Datum` in Appendix-D binary form."""
    if d.kind == P_BOOLEAN:
        return [UInt8(1) if d.i != 0 else UInt8(0)]
    if d.kind == P_INT or d.kind == P_DATE:
        return _le_bytes(d.i, 4)
    if (
        d.kind == P_LONG
        or d.kind == P_TIME
        or d.kind == P_TIMESTAMP
        or d.kind == P_TIMESTAMPTZ
        or d.kind == P_TIMESTAMP_NS
        or d.kind == P_TIMESTAMPTZ_NS
    ):
        return _le_bytes(d.i, 8)
    if d.kind == P_FLOAT:
        return _le_bytes(Int64(_f32_to_bits(Float32(d.f))), 4)
    if d.kind == P_DOUBLE:
        return _le_bytes(_f64_to_bits(d.f), 8)
    if d.kind == P_STRING:
        var out = List[UInt8]()
        var sb = d.s.as_bytes()
        for k in range(len(sb)):
            out.append(sb[k])
        return out^
    return d.b.copy()


def _bits_to_f32(bits: Int64) -> Float32:
    return bitcast[DType.float32](UInt32(UInt64(bits) & 0xFFFFFFFF))


def _bits_to_f64(bits: Int64) -> Float64:
    return bitcast[DType.float64](UInt64(bits))


def _f32_to_bits(v: Float32) -> UInt32:
    return bitcast[DType.uint32](v)


def _f64_to_bits(v: Float64) -> Int64:
    return Int64(bitcast[DType.uint64](v))


# ── Appendix D: JSON single-value serialization ─────────────────────────────
def datum_from_json(
    store: TypeStore, type_idx: Int, doc: Json, i: Int
) raises -> Datum:
    ref n = store.nodes[type_idx]
    if n.kind != TK_PRIMITIVE:
        return Datum.none()
    return datum_from_json_prim(n.prim, n.precision, n.scale, n.length, doc, i)


def datum_from_json_prim(
    prim: UInt8, precision: Int, scale: Int, length: Int, doc: Json, i: Int
) raises -> Datum:
    if i < 0 or doc.is_null(i):
        return Datum.none()
    if prim == P_BOOLEAN:
        if doc.kind(i) == 1:
            return Datum.bool_(doc.as_bool(i))
        return Datum.bool_(doc.as_int(i) != 0)
    if prim == P_INT:
        return Datum.int_(doc.as_int(i))
    if prim == P_LONG:
        return Datum.long_(doc.as_int(i))
    if prim == P_FLOAT:
        return Datum.float_(doc.as_float(i))
    if prim == P_DOUBLE:
        return Datum.double_(doc.as_float(i))
    if prim == P_STRING:
        return Datum.string_(doc.as_string(i))
    if prim == P_UUID:
        return Datum.uuid_(uuid_bytes(doc.as_string(i)))
    if prim == P_FIXED or prim == P_BINARY:
        var bb = hex_bytes(doc.as_string(i))
        return Datum.fixed_(bb^) if prim == P_FIXED else Datum.binary_(bb^)
    if prim == P_DECIMAL:
        return decimal_from_text(doc.as_string(i), precision, scale)
    if (
        prim == P_DATE
        or prim == P_TIME
        or prim == P_TIMESTAMP
        or prim == P_TIMESTAMPTZ
        or prim == P_TIMESTAMP_NS
        or prim == P_TIMESTAMPTZ_NS
    ):
        # Iceberg's JSON form is ISO text, but partition summaries and
        # hand-written filters often carry the raw integer; accept both.
        if doc.kind(i) == 2 or doc.kind(i) == 3:
            return Datum.integral(prim, doc.as_int(i))
        return Datum.integral(prim, parse_iso(prim, doc.as_string(i)))
    return Datum.none()
