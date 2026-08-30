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
    """A field reachable by name: its id, full dotted name and type index."""

    var id: Int
    var name: String
    var type: Int
    var required: Bool
    var parent: Int
    """Position in `Schema.flat` of the field this one is nested in, or -1."""
    var child_pos: Int
    """Position among the parent struct's fields, or -1 for a list element
    and a map key or value — the places a dotted path cannot walk through."""

    @staticmethod
    def simple(id: Int, var name: String, type: Int, required: Bool) -> Self:
        return Self(id, name^, type, required, -1, -1)


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
            self._walk_struct(self.root, "", -1, flat)
        self.flat = flat^

    def _walk_struct(
        self,
        node: Int,
        prefix: String,
        parent: Int,
        mut out: List[AccessorField],
    ):
        ref n = self.store.nodes[node]
        for k in range(len(n.fields)):
            ref f = n.fields[k]
            var full = f.name if prefix == "" else prefix + "." + f.name
            out.append(AccessorField(f.id, full, f.type, f.required, parent, k))
            var me = len(out) - 1
            self._walk_type(f.type, full, me, out)

    def _walk_type(
        self, t: Int, prefix: String, parent: Int, mut out: List[AccessorField]
    ):
        ref n = self.store.nodes[t]
        if n.kind == TK_STRUCT:
            self._walk_struct(t, prefix, parent, out)
        elif n.kind == TK_LIST:
            var nm = prefix + ".element"
            out.append(
                AccessorField(
                    n.element_id,
                    nm,
                    n.element,
                    n.element_required,
                    parent,
                    -1,
                )
            )
            var me = len(out) - 1
            self._walk_type(n.element, nm, me, out)
        elif n.kind == TK_MAP:
            var kn = prefix + ".key"
            out.append(AccessorField(n.key_id, kn, n.key, True, parent, -1))
            var ki = len(out) - 1
            self._walk_type(n.key, kn, ki, out)
            var vn = prefix + ".value"
            out.append(
                AccessorField(
                    n.value_id, vn, n.value, n.value_required, parent, -1
                )
            )
            var vi = len(out) - 1
            self._walk_type(n.value, vn, vi, out)

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

    # ── nested navigation ──────────────────────────────────────────────────
    def top_ancestor_id(self, id: Int) -> Int:
        """The id of the top-level column `id` lives in, or `id` itself."""
        var k = self.find_index(id)
        if k < 0:
            return id
        while self.flat[k].parent >= 0:
            k = self.flat[k].parent
        return self.flat[k].id

    def struct_path(self, id: Int) raises -> List[Int]:
        """Child positions from the top-level column down to field `id`.

        Empty for a top-level field. Raises when the path crosses a list
        element or a map key/value, which a dotted name cannot address and a
        row predicate cannot evaluate.
        """
        var k = self.find_index(id)
        if k < 0:
            raise Error("iceberg: no field with id " + String(id))
        var rev = List[Int]()
        while self.flat[k].parent >= 0:
            if self.flat[k].child_pos < 0:
                raise Error(
                    "iceberg: '"
                    + self.flat[k].name
                    + "' is inside a list or a map, which cannot be addressed"
                    " one value at a time"
                )
            rev.append(self.flat[k].child_pos)
            k = self.flat[k].parent
        var out = List[Int]()
        for j in range(len(rev)):
            out.append(rev[len(rev) - 1 - j])
        return out^

    def in_struct_only(self, id: Int) -> Bool:
        """True when `id` is reachable from the top through structs alone."""
        var k = self.find_index(id)
        if k < 0:
            return False
        while self.flat[k].parent >= 0:
            if self.flat[k].child_pos < 0:
                return False
            k = self.flat[k].parent
        return True

    def is_top_level(self, id: Int) -> Bool:
        var k = self.find_index(id)
        return k >= 0 and self.flat[k].parent < 0

    def column_type(self, id: Int) raises -> Schema:
        """A one-column schema holding just the type of field `id`."""
        return self.select([id])

    def select(self, ids: List[Int]) raises -> Schema:
        """Project to the named ids, keeping ancestors of any selected field.

        Selecting a nested field keeps the structs above it and *prunes* the
        ones beside it: `select(["a.b"])` gives back a struct `a` whose only
        field is `b`. Selecting a whole nested column keeps all of it. A list
        or a map is never partially selected — its element type is pruned when
        the selection reaches inside it, but the container itself stays.
        """
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
            elif _subtree_selected(self.store, f.type, keep):
                var nf = f.copy()
                nf.type = _prune_type(self.store, f.type, keep, store)
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


def _subtree_selected(src: TypeStore, i: Int, ids: List[Int]) -> Bool:
    """Whether any field id under type `i` was selected."""
    ref n = src.nodes[i]
    if n.kind == TK_STRUCT:
        for k in range(len(n.fields)):
            if _contains(ids, n.fields[k].id):
                return True
            if _subtree_selected(src, n.fields[k].type, ids):
                return True
        return False
    if n.kind == TK_LIST:
        if _contains(ids, n.element_id):
            return True
        return _subtree_selected(src, n.element, ids)
    if n.kind == TK_MAP:
        if _contains(ids, n.key_id) or _contains(ids, n.value_id):
            return True
        if _subtree_selected(src, n.key, ids):
            return True
        return _subtree_selected(src, n.value, ids)
    return False


def _prune_type(
    src: TypeStore, i: Int, ids: List[Int], mut dst: TypeStore
) raises -> Int:
    """Copy type `i`, keeping only the fields on a path to a selected id."""
    ref n = src.nodes[i]
    if n.kind == TK_STRUCT:
        var fields = List[NestedField]()
        for k in range(len(n.fields)):
            ref f = n.fields[k]
            var nf = f.copy()
            if _contains(ids, f.id):
                nf.type = _copy_type(src, f.type, dst)
                fields.append(nf^)
            elif _subtree_selected(src, f.type, ids):
                nf.type = _prune_type(src, f.type, ids, dst)
                fields.append(nf^)
        return dst.struct_(fields^)
    if n.kind == TK_LIST:
        var el = _keep_or_prune(src, n.element, n.element_id, ids, dst)
        return dst.list_(n.element_id, el, n.element_required)
    if n.kind == TK_MAP:
        var kt = _copy_type(src, n.key, dst)
        var vt = _keep_or_prune(src, n.value, n.value_id, ids, dst)
        return dst.map_(n.key_id, kt, n.value_id, vt, n.value_required)
    return _copy_type(src, i, dst)


def _keep_or_prune(
    src: TypeStore, i: Int, own_id: Int, ids: List[Int], mut dst: TypeStore
) raises -> Int:
    if _contains(ids, own_id) or not _subtree_selected(src, i, ids):
        return _copy_type(src, i, dst)
    return _prune_type(src, i, ids, dst)
