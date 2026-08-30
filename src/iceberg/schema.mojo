"""`Schema` — an Iceberg struct type with a schema id and identifier fields."""

from .json import Json, json_quote, parse_json
from .types import (
    TypeStore,
    TypeNode,
    NestedField,
    TK_PRIMITIVE,
    TK_STRUCT,
    TK_LIST,
    TK_MAP,
    P_UNKNOWN,
)


@fieldwise_init
struct AccessorField(Copyable, Movable):
    """A leaf reachable by name: its id, full dotted name and type index."""

    var id: Int
    var name: String
    var type: Int
    var required: Bool


struct Schema(Copyable, Movable):
    """A named struct type. Owns its `TypeStore`.

    `root` is the index of the top-level struct node; `by_id` / `by_name` are
    flattened indexes over *all* nested fields, built once at construction.
    Nested names are dotted (`addr.city`); list elements use `.element`, map
    keys/values `.key` / `.value`, matching the reference implementations.
    """

    var store: TypeStore
    var root: Int
    var schema_id: Int
    var identifier_field_ids: List[Int]
    var flat: List[AccessorField]

    def __init__(out self, var store: TypeStore, root: Int, schema_id: Int):
        self.store = store^
        self.root = root
        self.schema_id = schema_id
        self.identifier_field_ids = []
        self.flat = []
        self._index()

    def _index(mut self):
        var flat = List[AccessorField]()
        # `root < 0` is the empty schema a manifest without a `schema` key gets.
        if self.root >= 0:
            self._walk_struct(self.root, "", flat)
        self.flat = flat^

    def _walk_struct(
        self, node: Int, prefix: String, mut out: List[AccessorField]
    ):
        ref n = self.store.nodes[node]
        for k in range(len(n.fields)):
            ref f = n.fields[k]
            var full = f.name if prefix == "" else prefix + "." + f.name
            out.append(AccessorField(f.id, full, f.type, f.required))
            self._walk_type(f.type, full, out)

    def _walk_type(self, t: Int, prefix: String, mut out: List[AccessorField]):
        ref n = self.store.nodes[t]
        if n.kind == TK_STRUCT:
            self._walk_struct(t, prefix, out)
        elif n.kind == TK_LIST:
            var nm = prefix + ".element"
            out.append(
                AccessorField(n.element_id, nm, n.element, n.element_required)
            )
            self._walk_type(n.element, nm, out)
        elif n.kind == TK_MAP:
            var kn = prefix + ".key"
            var vn = prefix + ".value"
            out.append(AccessorField(n.key_id, kn, n.key, True))
            self._walk_type(n.key, kn, out)
            out.append(AccessorField(n.value_id, vn, n.value, n.value_required))
            self._walk_type(n.value, vn, out)

    # ── lookup ─────────────────────────────────────────────────────────────
    def find_index(self, id: Int) -> Int:
        """Position in `flat` of field `id`, or -1."""
        for k in range(len(self.flat)):
            if self.flat[k].id == id:
                return k
        return -1

    def find_field(self, id: Int) raises -> AccessorField:
        var k = self.find_index(id)
        if k < 0:
            raise Error("iceberg: no field with id " + String(id))
        return self.flat[k].copy()

    def has_field(self, id: Int) -> Bool:
        return self.find_index(id) >= 0

    def find_by_name(self, name: String) raises -> AccessorField:
        var k = self.find_name_index(name)
        if k < 0:
            raise Error("iceberg: no field named '" + name + "'")
        return self.flat[k].copy()

    def find_name_index(self, name: String) -> Int:
        """Case-sensitive first, then case-insensitive (Iceberg allows both)."""
        for k in range(len(self.flat)):
            if self.flat[k].name == name:
                return k
        var lowered = name.lower()
        for k in range(len(self.flat)):
            if self.flat[k].name.lower() == lowered:
                return k
        return -1

    def has_name(self, name: String) -> Bool:
        return self.find_name_index(name) >= 0

    def name_of(self, id: Int) -> String:
        var k = self.find_index(id)
        if k < 0:
            return ""
        return self.flat[k].name

    def columns(self) -> List[NestedField]:
        """The top-level fields only."""
        return self.store.nodes[self.root].fields.copy()

    def select(self, ids: List[Int]) raises -> Schema:
        """Project to the named ids, keeping ancestors of any selected field."""
        var keep = List[Int]()
        for k in range(len(ids)):
            keep.append(ids[k])
        var store = TypeStore()
        var fields = List[NestedField]()
        ref top = self.store.nodes[self.root]
        for k in range(len(top.fields)):
            ref f = top.fields[k]
            if _contains(keep, f.id):
                var ty = _copy_type(self.store, f.type, store)
                var nf = f.copy()
                nf.type = ty
                fields.append(nf^)
        var root = store.struct_(fields^)
        var s = Schema(store^, root, self.schema_id)
        var ids2 = List[Int]()
        for k in range(len(self.identifier_field_ids)):
            if _contains(keep, self.identifier_field_ids[k]):
                ids2.append(self.identifier_field_ids[k])
        s.identifier_field_ids = ids2^
        return s^

    def highest_field_id(self) -> Int:
        var m = 0
        for k in range(len(self.flat)):
            if self.flat[k].id > m:
                m = self.flat[k].id
        return m

    # ── JSON ───────────────────────────────────────────────────────────────
    @staticmethod
    def from_json(doc: Json, i: Int) raises -> Schema:
        var store = TypeStore()
        var root = store.parse_type(doc, i)
        var sid = Int(doc.opt_int(i, "schema-id", 0))
        var s = Schema(store^, root, sid)
        s.identifier_field_ids = doc.int_list(i, "identifier-field-ids")
        return s^

    @staticmethod
    def parse(text: String) raises -> Schema:
        var doc = parse_json(text)
        return Schema.from_json(doc, doc.root)

    def to_json(self) -> String:
        var out = String('{"type":"struct","schema-id":') + String(
            self.schema_id
        )
        if len(self.identifier_field_ids) > 0:
            out += ',"identifier-field-ids":['
            for k in range(len(self.identifier_field_ids)):
                if k > 0:
                    out += ","
                out += String(self.identifier_field_ids[k])
            out += "]"
        out += ',"fields":['
        ref top = self.store.nodes[self.root]
        for k in range(len(top.fields)):
            if k > 0:
                out += ","
            out += self.store.field_json(top.fields[k])
        out += "]}"
        return out^


def _contains(l: List[Int], v: Int) -> Bool:
    for k in range(len(l)):
        if l[k] == v:
            return True
    return False


def _copy_type(src: TypeStore, i: Int, mut dst: TypeStore) -> Int:
    """Deep-copy a type subtree from one arena into another."""
    var n = src.nodes[i].copy()
    if n.kind == TK_STRUCT:
        var fields = List[NestedField]()
        for k in range(len(n.fields)):
            var f = n.fields[k].copy()
            f.type = _copy_type(src, f.type, dst)
            fields.append(f^)
        n.fields = fields^
    elif n.kind == TK_LIST:
        n.element = _copy_type(src, n.element, dst)
    elif n.kind == TK_MAP:
        n.key = _copy_type(src, n.key, dst)
        n.value = _copy_type(src, n.value, dst)
    return dst.add(n^)
