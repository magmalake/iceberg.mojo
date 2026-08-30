"""Partition transforms and partition specs.

Every transform in the spec's transform table is here: `identity`, `bucket[N]`,
`truncate[W]`, `year`, `month`, `day`, `hour` and `void`. A transform spelling
this build does not recognise becomes `T_UNKNOWN`, which keeps its original
text, produces an `unknown` result type and reports `can_project() == False` —
the spec requires readers to ignore such a partition field when filtering
rather than fail, and requires that behaviour outright in v3.

`day` is the only time transform whose result is a `date`; `year`, `month` and
`hour` produce plain `int` counts from the epoch. Nanosecond timestamps are
truncated to microseconds before hashing (Appendix B note 3) so that a value
buckets the same however its column is typed.
"""

from std.memory import bitcast

from hashes import (
    iceberg_bucket,
    iceberg_hash_bytes,
    iceberg_hash_long,
    iceberg_hash_string,
    murmur3_x86_32,
)

from .json import Json, json_quote, substr, parse_json
from .schema import Schema
from .types import (
    TypeStore,
    NestedField,
    TK_PRIMITIVE,
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
)
from .values import (
    Datum,
    MICROS_PER_DAY,
    MICROS_PER_HOUR,
    civil_from_days,
    floor_div,
    floor_mod,
    int64_to_be_twos,
    be_twos_to_int64,
)


# ── transform kinds ─────────────────────────────────────────────────────────
comptime T_IDENTITY: UInt8 = 0
comptime T_BUCKET: UInt8 = 1
comptime T_TRUNCATE: UInt8 = 2
comptime T_YEAR: UInt8 = 3
comptime T_MONTH: UInt8 = 4
comptime T_DAY: UInt8 = 5
comptime T_HOUR: UInt8 = 6
comptime T_VOID: UInt8 = 7
comptime T_UNKNOWN: UInt8 = 255


@fieldwise_init
struct Transform(Copyable, Movable, Writable):
    """A partition transform: a kind plus the `[N]`/`[W]` parameter."""

    var kind: UInt8
    var param: Int
    var raw: String
    """Original spelling — the only thing kept for `T_UNKNOWN`."""

    @staticmethod
    def identity() -> Self:
        return Self(T_IDENTITY, 0, "identity")

    @staticmethod
    def bucket(n: Int) -> Self:
        return Self(T_BUCKET, n, "bucket[" + String(n) + "]")

    @staticmethod
    def truncate(w: Int) -> Self:
        return Self(T_TRUNCATE, w, "truncate[" + String(w) + "]")

    @staticmethod
    def void() -> Self:
        return Self(T_VOID, 0, "void")

    def name(self) -> String:
        return self.raw

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.raw)

    def can_project(self) -> Bool:
        """False when a predicate cannot be pushed through this transform."""
        return self.kind != T_UNKNOWN

    def is_time(self) -> Bool:
        return (
            self.kind == T_YEAR
            or self.kind == T_MONTH
            or self.kind == T_DAY
            or self.kind == T_HOUR
        )

    def preserves_order(self) -> Bool:
        """True when `a <= b` implies `T(a) <= T(b)` — the projection rules
        for `<`/`>` only hold for these."""
        return (
            self.kind == T_IDENTITY or self.kind == T_TRUNCATE or self.is_time()
        )

    def result_type(self, mut store: TypeStore, source: Int) raises -> Int:
        """The type a partition value of this transform has."""
        if self.kind == T_BUCKET:
            return store.primitive(P_INT)
        if self.kind == T_YEAR or self.kind == T_MONTH or self.kind == T_HOUR:
            return store.primitive(P_INT)
        if self.kind == T_DAY:
            return store.primitive(P_DATE)
        if self.kind == T_UNKNOWN:
            return store.primitive(P_UNKNOWN)
        # identity, truncate and void keep the source type.
        return _clone_prim(store, source)

    def apply(self, v: Datum) raises -> Datum:
        """Transform one value. All transforms map null to null."""
        if not v.valid:
            return Datum.none()
        if self.kind == T_VOID:
            return Datum.none()
        if self.kind == T_UNKNOWN:
            return Datum.none()
        if self.kind == T_IDENTITY:
            return v.copy()
        if self.kind == T_BUCKET:
            return Datum.int_(Int64(bucket_of(v, self.param)))
        if self.kind == T_TRUNCATE:
            return truncate_of(v, self.param)
        return time_of(v, self.kind)


def _clone_prim(mut store: TypeStore, source: Int) raises -> Int:
    """Copy a primitive type node so the result lives in `store`."""
    var n = store.nodes[source].copy()
    return store.add(n^)


# ── the hash side of bucket[N] ──────────────────────────────────────────────
def iceberg_hash(v: Datum) raises -> Int32:
    """Appendix B's 32-bit hash of a typed value."""
    var k = v.kind
    if k == P_INT or k == P_LONG or k == P_DATE or k == P_TIME:
        # `date` is hashInt(days) and note 1 makes int and long hashes equal,
        # so every one of these is hashLong of the stored integer.
        return iceberg_hash_long(v.i)
    if k == P_TIMESTAMP or k == P_TIMESTAMPTZ:
        return iceberg_hash_long(v.i)
    if k == P_TIMESTAMP_NS or k == P_TIMESTAMPTZ_NS:
        # Note 3: nanosecond timestamps hash at microsecond precision.
        return iceberg_hash_long(floor_div(v.i, 1000))
    if k == P_DECIMAL:
        return iceberg_hash_bytes(Span(v.b))
    if k == P_STRING:
        return iceberg_hash_string(StringSpan(v.s))
    if k == P_UUID or k == P_FIXED or k == P_BINARY:
        return iceberg_hash_bytes(Span(v.b))
    if k == P_BOOLEAN:
        return iceberg_hash_long(v.i)
    if k == P_FLOAT or k == P_DOUBLE:
        # Note 5: canonicalize NaN and -0.0, then hash the double's bits.
        var d = v.f
        if d != d:
            return iceberg_hash_long(Int64(0x7FF8000000000000))
        if d == 0.0:
            d = 0.0
        return iceberg_hash_long(Int64(bitcast[DType.uint64](d)))
    raise Error("iceberg: no 32-bit hash is defined for " + primitive_name(k))


def bucket_of(v: Datum, n: Int) raises -> Int:
    var h = iceberg_hash(v)
    return iceberg_bucket(bitcast[DType.uint32](h), n)


# ── truncate[W] ─────────────────────────────────────────────────────────────
def truncate_of(v: Datum, w: Int) raises -> Datum:
    var k = v.kind
    if k == P_INT or k == P_LONG:
        return Datum.integral(k, v.i - floor_mod(v.i, Int64(w)))
    if k == P_DECIMAL:
        # W is applied at the column's scale, so it truncates the unscaled
        # value directly: v - (((v % W) + W) % W).
        var u = v.i - floor_mod(v.i, Int64(w))
        return Datum.decimal_int(u, v.precision, v.scale)
    if k == P_STRING:
        return Datum.string_(truncate_codepoints(v.s, w))
    if k == P_BINARY or k == P_FIXED:
        var out = List[UInt8]()
        var lim = w if w < len(v.b) else len(v.b)
        for j in range(lim):
            out.append(v.b[j])
        return Datum.binary_(out^)
    raise Error("iceberg: truncate is not defined for " + primitive_name(k))


def truncate_codepoints(s: String, w: Int) -> String:
    """First `w` Unicode code points — the spec truncates strings by code
    point, never mid-character."""
    if w <= 0:
        return String("")
    var b = s.as_bytes()
    var count = 0
    var i = 0
    while i < len(b) and count < w:
        var c = b[i]
        var n = 1
        if c >= 0xF0:
            n = 4
        elif c >= 0xE0:
            n = 3
        elif c >= 0xC0:
            n = 2
        i += n
        count += 1
    if i > len(b):
        i = len(b)
    return String(StringSlice(unsafe_from_utf8=Span(b)[0:i]))


# ── year / month / day / hour ───────────────────────────────────────────────
def _to_micros(v: Datum) raises -> Int64:
    """Microseconds since the epoch for any timestamp-shaped value."""
    if v.kind == P_TIMESTAMP_NS or v.kind == P_TIMESTAMPTZ_NS:
        return floor_div(v.i, 1000)
    return v.i


def time_of(v: Datum, kind: UInt8) raises -> Datum:
    var days: Int64
    if v.kind == P_DATE:
        days = v.i
        if kind == T_HOUR:
            raise Error("iceberg: hour is not defined for a date column")
        if kind == T_DAY:
            return Datum.integral(P_DATE, days)
    else:
        var micros = _to_micros(v)
        if kind == T_HOUR:
            return Datum.int_(floor_div(micros, MICROS_PER_HOUR))
        days = floor_div(micros, MICROS_PER_DAY)
        if kind == T_DAY:
            return Datum.integral(P_DATE, days)
    var c = civil_from_days(days)
    if kind == T_YEAR:
        return Datum.int_(c[0] - 1970)
    return Datum.int_((c[0] - 1970) * 12 + (c[1] - 1))


# ── parsing ─────────────────────────────────────────────────────────────────
def parse_transform(s: String) raises -> Transform:
    if s == "identity":
        return Transform.identity()
    if s == "void":
        return Transform.void()
    if s == "year":
        return Transform(T_YEAR, 0, "year")
    if s == "month":
        return Transform(T_MONTH, 0, "month")
    if s == "day":
        return Transform(T_DAY, 0, "day")
    if s == "hour":
        return Transform(T_HOUR, 0, "hour")
    if s.startswith("bucket["):
        return Transform(T_BUCKET, _bracket_int(s), s)
    if s.startswith("truncate["):
        return Transform(T_TRUNCATE, _bracket_int(s), s)
    # Historic spellings some writers emitted before the bracket form.
    if s.startswith("bucket("):
        return Transform(T_BUCKET, _paren_int(s), s)
    if s.startswith("truncate("):
        return Transform(T_TRUNCATE, _paren_int(s), s)
    if s == "dateint" or s == "":
        return Transform(T_UNKNOWN, 0, s)
    return Transform(T_UNKNOWN, 0, s)


def _bracket_int(s: String) raises -> Int:
    var a = s.find("[")
    var b = s.find("]")
    if a < 0 or b < 0:
        raise Error("iceberg: malformed transform '" + s + "'")
    return Int(String(substr(s, a + 1, b).strip()))


def _paren_int(s: String) raises -> Int:
    var a = s.find("(")
    var b = s.find(")")
    if a < 0 or b < 0:
        raise Error("iceberg: malformed transform '" + s + "'")
    return Int(String(substr(s, a + 1, b).strip()))


# ── partition specs ─────────────────────────────────────────────────────────
comptime PARTITION_DATA_ID_START: Int = 1000
"""v1 assigned partition field ids sequentially from 1000 with no record."""


@fieldwise_init
struct PartitionField(Copyable, Movable, Writable):
    """One field of a partition spec."""

    var source_id: Int
    var source_ids: List[Int]
    """v3 multi-argument transforms. Single-source specs mirror `source_id`."""
    var field_id: Int
    var name: String
    var transform: Transform

    @staticmethod
    def single(
        source_id: Int, field_id: Int, var name: String, var t: Transform
    ) -> Self:
        return Self(source_id, [source_id], field_id, name^, t^)

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.name, "=", self.transform, "(", self.source_id, ")")


struct PartitionSpec(Copyable, Movable):
    """A partition spec: an id and an ordered list of partition fields."""

    var spec_id: Int
    var fields: List[PartitionField]

    def __init__(out self, spec_id: Int, var fields: List[PartitionField]):
        self.spec_id = spec_id
        self.fields = fields^

    @staticmethod
    def unpartitioned(spec_id: Int = 0) -> Self:
        return Self(spec_id, [])

    def is_unpartitioned(self) -> Bool:
        if len(self.fields) == 0:
            return True
        for k in range(len(self.fields)):
            if self.fields[k].transform.kind != T_VOID:
                return False
        return True

    def field_index(self, field_id: Int) -> Int:
        for k in range(len(self.fields)):
            if self.fields[k].field_id == field_id:
                return k
        return -1

    def fields_for_source(self, source_id: Int) -> List[Int]:
        """Positions of every partition field derived from `source_id`."""
        var out = List[Int]()
        for k in range(len(self.fields)):
            if self.fields[k].source_id == source_id:
                out.append(k)
        return out^

    def partition_type(self, schema: Schema) raises -> Schema:
        """The struct type of a partition tuple under this spec.

        Field ids are the *partition* field ids (1000+), matching how manifest
        files type their `partition` column. Every field is optional: a
        transform may legitimately produce null.
        """
        var store = TypeStore()
        var fields = List[NestedField]()
        for k in range(len(self.fields)):
            ref pf = self.fields[k]
            var src: Int
            if schema.has_field(pf.source_id):
                var af = schema.find_field(pf.source_id)
                src = _copy_prim(schema.store, af.type, store)
            else:
                src = store.primitive(P_UNKNOWN)
            var rt = pf.transform.result_type(store, src)
            fields.append(NestedField.simple(pf.field_id, pf.name, False, rt))
        var root = store.struct_(fields^)
        return Schema(store^, root, 0)

    # ── JSON (Appendix C) ──────────────────────────────────────────────────
    @staticmethod
    def from_json(doc: Json, i: Int, default_spec_id: Int = 0) raises -> Self:
        var sid = Int(doc.opt_int(i, "spec-id", Int64(default_spec_id)))
        var fs = doc.get(i, "fields")
        var fields = List[PartitionField]()
        var next_v1_id = PARTITION_DATA_ID_START
        for k in range(doc.size(fs)):
            var fi = doc.at(fs, k)
            var t = parse_transform(doc.req_string(fi, "transform"))
            var sids = doc.int_list(fi, "source-ids")
            var src = 0
            if doc.has(fi, "source-id"):
                src = Int(doc.req_int(fi, "source-id"))
            elif len(sids) > 0:
                src = sids[0]
            if len(sids) == 0:
                sids.append(src)
            # v1 specs carry no `field-id`; ids were assigned from 1000 up.
            var fid = next_v1_id
            if doc.has(fi, "field-id"):
                fid = Int(doc.req_int(fi, "field-id"))
            else:
                next_v1_id += 1
            fields.append(
                PartitionField(src, sids^, fid, doc.req_string(fi, "name"), t^)
            )
        return Self(sid, fields^)

    @staticmethod
    def from_fields_json(doc: Json, i: Int, spec_id: Int) raises -> Self:
        """v1's bare `partition-spec` array (no wrapping object)."""
        var fields = List[PartitionField]()
        var next_v1_id = PARTITION_DATA_ID_START
        for k in range(doc.size(i)):
            var fi = doc.at(i, k)
            var t = parse_transform(doc.req_string(fi, "transform"))
            var src = Int(doc.req_int(fi, "source-id"))
            var fid = next_v1_id
            if doc.has(fi, "field-id"):
                fid = Int(doc.req_int(fi, "field-id"))
            else:
                next_v1_id += 1
            fields.append(
                PartitionField.single(src, fid, doc.req_string(fi, "name"), t^)
            )
        return Self(spec_id, fields^)

    def to_json(self) -> String:
        var out = String('{"spec-id":') + String(self.spec_id) + ',"fields":['
        for k in range(len(self.fields)):
            if k > 0:
                out += ","
            ref f = self.fields[k]
            out += '{"source-id":' + String(f.source_id)
            out += ',"field-id":' + String(f.field_id)
            out += ',"transform":' + json_quote(f.transform.raw)
            out += ',"name":' + json_quote(f.name)
            out += "}"
        out += "]}"
        return out^


def _copy_prim(src: TypeStore, i: Int, mut dst: TypeStore) raises -> Int:
    var n = src.nodes[i].copy()
    return dst.add(n^)


# ── sort orders ─────────────────────────────────────────────────────────────
comptime SORT_ASC: UInt8 = 0
comptime SORT_DESC: UInt8 = 1
comptime NULLS_FIRST: UInt8 = 0
comptime NULLS_LAST: UInt8 = 1


@fieldwise_init
struct SortField(Copyable, Movable):
    var source_id: Int
    var source_ids: List[Int]
    var transform: Transform
    var direction: UInt8
    var null_order: UInt8


struct SortOrder(Copyable, Movable):
    var order_id: Int
    var fields: List[SortField]

    def __init__(out self, order_id: Int, var fields: List[SortField]):
        self.order_id = order_id
        self.fields = fields^

    @staticmethod
    def from_json(doc: Json, i: Int) raises -> Self:
        var oid = Int(doc.opt_int(i, "order-id", 0))
        var fs = doc.get(i, "fields")
        var fields = List[SortField]()
        for k in range(doc.size(fs)):
            var fi = doc.at(fs, k)
            var t = parse_transform(doc.req_string(fi, "transform"))
            var sids = doc.int_list(fi, "source-ids")
            var src = 0
            if doc.has(fi, "source-id"):
                src = Int(doc.req_int(fi, "source-id"))
            elif len(sids) > 0:
                src = sids[0]
            if len(sids) == 0:
                sids.append(src)
            var dir = (
                SORT_DESC if doc.opt_string(fi, "direction", "asc")
                == "desc" else SORT_ASC
            )
            var no = (
                NULLS_LAST if doc.opt_string(fi, "null-order", "nulls-first")
                == "nulls-last" else NULLS_FIRST
            )
            fields.append(SortField(src, sids^, t^, dir, no))
        return Self(oid, fields^)

    def to_json(self) -> String:
        var out = String('{"order-id":') + String(self.order_id) + ',"fields":['
        for k in range(len(self.fields)):
            if k > 0:
                out += ","
            ref f = self.fields[k]
            out += '{"transform":' + json_quote(f.transform.raw)
            out += ',"source-id":' + String(f.source_id)
            out += ',"direction":' + (
                '"desc"' if f.direction == SORT_DESC else '"asc"'
            )
            out += ',"null-order":' + (
                '"nulls-last"' if f.null_order
                == NULLS_LAST else '"nulls-first"'
            )
            out += "}"
        out += "]}"
        return out^
