"""Iceberg data types — primitives, nested types, and their JSON forms.

Iceberg types are recursive (a struct holds fields whose types may be structs).
Mojo has no easy recursive value type, so a whole type tree lives in a
`TypeStore` arena: every node is an entry in `TypeStore.nodes` and children are
referred to by `Int` index. A "type" in this module is therefore always a
`(store, index)` pair; `TypeStore` owns the memory.

All primitives from format versions 1, 2 and 3 are covered, including the v3
additions `unknown`, `timestamp_ns`, `timestamptz_ns`, `variant`,
`geometry(C)` and `geography(C, A)`. A primitive the reader does not recognise
is kept as `UNKNOWN_PRIM` with its original spelling in `TypeNode.raw`, so
forward-compatible metadata (the spec requires readers to tolerate it) still
parses instead of blowing up.
"""

from .json import Json, JsonNode, json_quote, substr


# ── type kinds ──────────────────────────────────────────────────────────────
comptime TK_PRIMITIVE: UInt8 = 0
comptime TK_STRUCT: UInt8 = 1
comptime TK_LIST: UInt8 = 2
comptime TK_MAP: UInt8 = 3

# ── primitive kinds ─────────────────────────────────────────────────────────
comptime P_BOOLEAN: UInt8 = 0
comptime P_INT: UInt8 = 1
comptime P_LONG: UInt8 = 2
comptime P_FLOAT: UInt8 = 3
comptime P_DOUBLE: UInt8 = 4
comptime P_DATE: UInt8 = 5
comptime P_TIME: UInt8 = 6
comptime P_TIMESTAMP: UInt8 = 7
comptime P_TIMESTAMPTZ: UInt8 = 8
comptime P_TIMESTAMP_NS: UInt8 = 9
comptime P_TIMESTAMPTZ_NS: UInt8 = 10
comptime P_STRING: UInt8 = 11
comptime P_UUID: UInt8 = 12
comptime P_FIXED: UInt8 = 13
comptime P_BINARY: UInt8 = 14
comptime P_DECIMAL: UInt8 = 15
comptime P_UNKNOWN: UInt8 = 16
comptime P_VARIANT: UInt8 = 17
comptime P_GEOMETRY: UInt8 = 18
comptime P_GEOGRAPHY: UInt8 = 19
comptime P_UNRECOGNIZED: UInt8 = 255
"""A primitive spelling this build does not know — kept verbatim, never fatal."""

comptime DEFAULT_CRS = String("OGC:CRS84")
comptime DEFAULT_EDGE_ALGORITHM = String("spherical")


def primitive_name(kind: UInt8) -> String:
    if kind == P_BOOLEAN:
        return "boolean"
    if kind == P_INT:
        return "int"
    if kind == P_LONG:
        return "long"
    if kind == P_FLOAT:
        return "float"
    if kind == P_DOUBLE:
        return "double"
    if kind == P_DATE:
        return "date"
    if kind == P_TIME:
        return "time"
    if kind == P_TIMESTAMP:
        return "timestamp"
    if kind == P_TIMESTAMPTZ:
        return "timestamptz"
    if kind == P_TIMESTAMP_NS:
        return "timestamp_ns"
    if kind == P_TIMESTAMPTZ_NS:
        return "timestamptz_ns"
    if kind == P_STRING:
        return "string"
    if kind == P_UUID:
        return "uuid"
    if kind == P_FIXED:
        return "fixed"
    if kind == P_BINARY:
        return "binary"
    if kind == P_DECIMAL:
        return "decimal"
    if kind == P_UNKNOWN:
        return "unknown"
    if kind == P_VARIANT:
        return "variant"
    if kind == P_GEOMETRY:
        return "geometry"
    if kind == P_GEOGRAPHY:
        return "geography"
    return "unrecognized"


def is_integer_like(kind: UInt8) -> Bool:
    """True for primitives whose value is carried as a signed 64-bit integer."""
    return (
        kind == P_INT
        or kind == P_LONG
        or kind == P_DATE
        or kind == P_TIME
        or kind == P_TIMESTAMP
        or kind == P_TIMESTAMPTZ
        or kind == P_TIMESTAMP_NS
        or kind == P_TIMESTAMPTZ_NS
    )


def is_time_like(kind: UInt8) -> Bool:
    return (
        kind == P_DATE
        or kind == P_TIME
        or kind == P_TIMESTAMP
        or kind == P_TIMESTAMPTZ
        or kind == P_TIMESTAMP_NS
        or kind == P_TIMESTAMPTZ_NS
    )


@fieldwise_init
struct NestedField(Copyable, Movable, Writable):
    """A field of a struct type: id, name, requiredness and a type index."""

    var id: Int
    var name: String
    var required: Bool
    var type: Int
    """Index into the owning `TypeStore`."""
    var doc: String
    var has_doc: Bool
    var initial_default: String
    """Raw JSON text of `initial-default`, or "" when absent."""
    var has_initial_default: Bool
    var write_default: String
    var has_write_default: Bool

    @staticmethod
    def simple(id: Int, var name: String, required: Bool, type: Int) -> Self:
        return Self(id, name^, required, type, "", False, "", False, "", False)

    def write_to(self, mut writer: Some[Writer]):
        writer.write(
            "NestedField(", self.id, ", ", self.name, ", req=", self.required, ")"
        )


@fieldwise_init
struct TypeNode(Copyable, Movable):
    """One node of a type tree. Which fields matter depends on `kind`/`prim`."""

    var kind: UInt8
    var prim: UInt8
    var precision: Int
    """decimal(P, S)"""
    var scale: Int
    var length: Int
    """fixed[L]"""
    var crs: String
    """geometry(C) / geography(C, A)"""
    var algorithm: String
    var raw: String
    """Original spelling, for `P_UNRECOGNIZED` and for exact re-serialization."""
    var fields: List[NestedField]
    """struct"""
    var element: Int
    """list: element type index"""
    var element_id: Int
    var element_required: Bool
    var key: Int
    """map: key type index"""
    var key_id: Int
    var value: Int
    var value_id: Int
    var value_required: Bool

    @staticmethod
    def prim_(kind: UInt8) -> Self:
        return Self(
            TK_PRIMITIVE, kind, 0, 0, 0, "", "", "", [], -1, -1, False, -1, -1,
            -1, -1, False,
        )


struct TypeStore(Copyable, Movable):
    """Arena of type nodes. Indices returned by the builders point in here."""

    var nodes: List[TypeNode]

    def __init__(out self):
        self.nodes = []

    def add(mut self, var n: TypeNode) -> Int:
        self.nodes.append(n^)
        return len(self.nodes) - 1

    def primitive(mut self, kind: UInt8) -> Int:
        return self.add(TypeNode.prim_(kind))

    def decimal(mut self, precision: Int, scale: Int) -> Int:
        var n = TypeNode.prim_(P_DECIMAL)
        n.precision = precision
        n.scale = scale
        return self.add(n^)

    def fixed(mut self, length: Int) -> Int:
        var n = TypeNode.prim_(P_FIXED)
        n.length = length
        return self.add(n^)

    def struct_(mut self, var fields: List[NestedField]) -> Int:
        var n = TypeNode.prim_(P_UNKNOWN)
        n.kind = TK_STRUCT
        n.fields = fields^
        return self.add(n^)

    def list_(mut self, element_id: Int, element: Int, required: Bool) -> Int:
        var n = TypeNode.prim_(P_UNKNOWN)
        n.kind = TK_LIST
        n.element_id = element_id
        n.element = element
        n.element_required = required
        return self.add(n^)

    def map_(
        mut self,
        key_id: Int,
        key: Int,
        value_id: Int,
        value: Int,
        value_required: Bool,
    ) -> Int:
        var n = TypeNode.prim_(P_UNKNOWN)
        n.kind = TK_MAP
        n.key_id = key_id
        n.key = key
        n.value_id = value_id
        n.value = value
        n.value_required = value_required
        return self.add(n^)

    # ── inspection ─────────────────────────────────────────────────────────
    def kind(self, i: Int) -> UInt8:
        return self.nodes[i].kind

    def prim(self, i: Int) -> UInt8:
        return self.nodes[i].prim

    def is_primitive(self, i: Int) -> Bool:
        return self.nodes[i].kind == TK_PRIMITIVE

    def is_nested(self, i: Int) -> Bool:
        return self.nodes[i].kind != TK_PRIMITIVE

    def type_name(self, i: Int) -> String:
        """The Iceberg JSON spelling of a primitive; the kind for nested types."""
        ref n = self.nodes[i]
        if n.kind == TK_STRUCT:
            return "struct"
        if n.kind == TK_LIST:
            return "list"
        if n.kind == TK_MAP:
            return "map"
        if n.prim == P_DECIMAL:
            return (
                "decimal(" + String(n.precision) + ", " + String(n.scale) + ")"
            )
        if n.prim == P_FIXED:
            return "fixed[" + String(n.length) + "]"
        if n.prim == P_GEOMETRY:
            if n.crs == DEFAULT_CRS:
                return "geometry"
            return "geometry(" + n.crs + ")"
        if n.prim == P_GEOGRAPHY:
            if n.crs == DEFAULT_CRS and n.algorithm == DEFAULT_EDGE_ALGORITHM:
                return "geography"
            if n.algorithm == DEFAULT_EDGE_ALGORITHM:
                return "geography(" + n.crs + ")"
            return "geography(" + n.crs + ", " + n.algorithm + ")"
        if n.prim == P_UNRECOGNIZED:
            return n.raw
        return primitive_name(n.prim)

    def equal(self, a: Int, b: Int) -> Bool:
        """Structural equality of two types in this store."""
        ref x = self.nodes[a]
        ref y = self.nodes[b]
        if x.kind != y.kind:
            return False
        if x.kind == TK_PRIMITIVE:
            if x.prim != y.prim:
                return False
            if x.prim == P_DECIMAL:
                return x.precision == y.precision and x.scale == y.scale
            if x.prim == P_FIXED:
                return x.length == y.length
            if x.prim == P_GEOMETRY:
                return x.crs == y.crs
            if x.prim == P_GEOGRAPHY:
                return x.crs == y.crs and x.algorithm == y.algorithm
            if x.prim == P_UNRECOGNIZED:
                return x.raw == y.raw
            return True
        if x.kind == TK_LIST:
            return (
                x.element_id == y.element_id
                and x.element_required == y.element_required
                and self.equal(x.element, y.element)
            )
        if x.kind == TK_MAP:
            return (
                x.key_id == y.key_id
                and x.value_id == y.value_id
                and x.value_required == y.value_required
                and self.equal(x.key, y.key)
                and self.equal(x.value, y.value)
            )
        if len(x.fields) != len(y.fields):
            return False
        for k in range(len(x.fields)):
            if (
                x.fields[k].id != y.fields[k].id
                or x.fields[k].name != y.fields[k].name
                or x.fields[k].required != y.fields[k].required
                or not self.equal(x.fields[k].type, y.fields[k].type)
            ):
                return False
        return True

    # ── JSON ───────────────────────────────────────────────────────────────
    def parse_type(mut self, doc: Json, i: Int) raises -> Int:
        """Parse an Iceberg type from JSON — a string, or a nested-type object.
        """
        if doc.kind(i) == 4:  # JSON_STRING
            return self.parse_primitive(doc.as_string(i))
        if doc.kind(i) != 6:  # JSON_OBJECT
            raise Error("iceberg: type must be a string or an object")
        var t = doc.req_string(i, "type")
        if t == "struct":
            var fs = doc.get(i, "fields")
            var fields = List[NestedField]()
            for k in range(doc.size(fs)):
                fields.append(self.parse_field(doc, doc.at(fs, k)))
            return self.struct_(fields^)
        if t == "list":
            var el = self.parse_type(doc, doc.get(i, "element"))
            return self.list_(
                Int(doc.req_int(i, "element-id")),
                el,
                doc.opt_bool(i, "element-required", False),
            )
        if t == "map":
            var kt = self.parse_type(doc, doc.get(i, "key"))
            var vt = self.parse_type(doc, doc.get(i, "value"))
            return self.map_(
                Int(doc.req_int(i, "key-id")),
                kt,
                Int(doc.req_int(i, "value-id")),
                vt,
                doc.opt_bool(i, "value-required", False),
            )
        # An unrecognised *nested* type: keep the object's text so it can be
        # round-tripped, and treat it as an opaque primitive for reading.
        var n = TypeNode.prim_(P_UNRECOGNIZED)
        n.raw = doc.dump(i)
        return self.add(n^)

    def parse_field(mut self, doc: Json, i: Int) raises -> NestedField:
        var ty = self.parse_type(doc, doc.get(i, "type"))
        var f = NestedField.simple(
            Int(doc.req_int(i, "id")),
            doc.req_string(i, "name"),
            doc.opt_bool(i, "required", False),
            ty,
        )
        var d = doc.get(i, "doc")
        if d >= 0 and not doc.is_null(d):
            f.doc = doc.as_string(d)
            f.has_doc = True
        var iv = doc.get(i, "initial-default")
        if iv >= 0:
            f.initial_default = doc.dump(iv)
            f.has_initial_default = True
        var wv = doc.get(i, "write-default")
        if wv >= 0:
            f.write_default = doc.dump(wv)
            f.has_write_default = True
        return f^

    def parse_primitive(mut self, s: String) raises -> Int:
        """Parse a primitive spelling. Unknown spellings are kept, not rejected.
        """
        if s == "boolean":
            return self.primitive(P_BOOLEAN)
        if s == "int":
            return self.primitive(P_INT)
        if s == "long":
            return self.primitive(P_LONG)
        if s == "float":
            return self.primitive(P_FLOAT)
        if s == "double":
            return self.primitive(P_DOUBLE)
        if s == "date":
            return self.primitive(P_DATE)
        if s == "time":
            return self.primitive(P_TIME)
        if s == "timestamp":
            return self.primitive(P_TIMESTAMP)
        if s == "timestamptz":
            return self.primitive(P_TIMESTAMPTZ)
        if s == "timestamp_ns":
            return self.primitive(P_TIMESTAMP_NS)
        if s == "timestamptz_ns":
            return self.primitive(P_TIMESTAMPTZ_NS)
        if s == "string":
            return self.primitive(P_STRING)
        if s == "uuid":
            return self.primitive(P_UUID)
        if s == "binary":
            return self.primitive(P_BINARY)
        if s == "unknown":
            return self.primitive(P_UNKNOWN)
        if s == "variant":
            return self.primitive(P_VARIANT)
        if s.startswith("decimal"):
            var p = _parse_two_ints(s, "decimal")
            return self.decimal(p[0], p[1])
        if s.startswith("fixed"):
            return self.fixed(_parse_one_int(s))
        if s == "geometry":
            var n = TypeNode.prim_(P_GEOMETRY)
            n.crs = DEFAULT_CRS
            return self.add(n^)
        if s.startswith("geometry("):
            var n = TypeNode.prim_(P_GEOMETRY)
            n.crs = String(substr(s, 9, s.byte_length() - 1).strip())
            return self.add(n^)
        if s == "geography":
            var n = TypeNode.prim_(P_GEOGRAPHY)
            n.crs = DEFAULT_CRS
            n.algorithm = DEFAULT_EDGE_ALGORITHM
            return self.add(n^)
        if s.startswith("geography("):
            var inner = substr(s, 10, s.byte_length() - 1)
            var n = TypeNode.prim_(P_GEOGRAPHY)
            var comma = inner.find(",")
            if comma < 0:
                n.crs = String(inner.strip())
                n.algorithm = DEFAULT_EDGE_ALGORITHM
            else:
                n.crs = String(substr(inner, 0, comma).strip())
                n.algorithm = String(
                    substr(inner, comma + 1, inner.byte_length()).strip()
                )
            return self.add(n^)
        var un = TypeNode.prim_(P_UNRECOGNIZED)
        un.raw = s
        return self.add(un^)

    def type_json(self, i: Int) -> String:
        """Serialize a type back to its Iceberg JSON form."""
        ref n = self.nodes[i]
        if n.kind == TK_PRIMITIVE:
            if n.prim == P_UNRECOGNIZED and n.raw.startswith("{"):
                return n.raw
            return json_quote(self.type_name(i))
        if n.kind == TK_LIST:
            return (
                '{"type":"list","element-id":'
                + String(n.element_id)
                + ',"element":'
                + self.type_json(n.element)
                + ',"element-required":'
                + ("true" if n.element_required else "false")
                + "}"
            )
        if n.kind == TK_MAP:
            return (
                '{"type":"map","key-id":'
                + String(n.key_id)
                + ',"key":'
                + self.type_json(n.key)
                + ',"value-id":'
                + String(n.value_id)
                + ',"value":'
                + self.type_json(n.value)
                + ',"value-required":'
                + ("true" if n.value_required else "false")
                + "}"
            )
        var out = String('{"type":"struct","fields":[')
        for k in range(len(n.fields)):
            if k > 0:
                out += ","
            out += self.field_json(n.fields[k])
        out += "]}"
        return out^

    def field_json(self, f: NestedField) -> String:
        var out = String('{"id":') + String(f.id)
        out += ',"name":' + json_quote(f.name)
        out += ',"required":' + ("true" if f.required else "false")
        out += ',"type":' + self.type_json(f.type)
        if f.has_doc:
            out += ',"doc":' + json_quote(f.doc)
        if f.has_initial_default:
            out += ',"initial-default":' + f.initial_default
        if f.has_write_default:
            out += ',"write-default":' + f.write_default
        out += "}"
        return out^


def _parse_one_int(s: String) raises -> Int:
    """`fixed[16]` → 16."""
    var a = s.find("[")
    var b = s.find("]")
    if a < 0 or b < 0:
        raise Error("iceberg: malformed type '" + s + "'")
    return Int(String(substr(s, a + 1, b).strip()))


def _parse_two_ints(s: String, what: String) raises -> List[Int]:
    """`decimal(9, 2)` → [9, 2]."""
    var a = s.find("(")
    var b = s.find(")")
    var comma = s.find(",")
    if a < 0 or b < 0 or comma < 0:
        raise Error("iceberg: malformed " + what + " type '" + s + "'")
    return [
        Int(String(substr(s, a + 1, comma).strip())),
        Int(String(substr(s, comma + 1, b).strip())),
    ]
