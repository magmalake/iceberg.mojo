"""Nested columns — struct, list and map — as Arrow trees.

parquet.mojo hands a nested column back as an `ArrayData` whose children are
*indices into an `ArrayArena`*, because Mojo cannot put a `List` of a struct
inside that same struct. A scan therefore has to carry a whole arena per
column, not one array, and every kernel `iceberg.kernels` provides for a flat
column needs a counterpart that walks children and offsets.

That is what this module is. A column is a `ColumnTree`: an arena plus the
index of its root. Each kernel reads one `(arena, node)` pair and writes its
result into another arena, appending children before their parent — which
keeps a subtree contiguous and ending at its root, the invariant
`read.mojo` relies on when it moves decoded columns out of a batch.

| kernel | flat counterpart |
|---|---|
| `cast_column` / `cast_tree` | `kernels.cast_array` |
| `filter_tree` | `kernels.filter_array` |
| `concat_tree` | `kernels.concat_into` |
| `null_tree` / `empty_tree` | `kernels.constant_array` / `empty_array` |
| `flatten_leaf` | — (a struct leaf, seen as a flat column) |
| `cell_json` | `Datum.to_json` |

**Buffer layouts** are Arrow's, exactly as parquet.mojo writes them: a struct
is a validity bitmap and N children of the struct's own length; a list is a
validity bitmap, `length + 1` 32-bit offsets and one child; a map is a list
whose child is a non-nullable two-field struct (`key`, `value`).
"""

from parquet.arrow import (
    AT_LARGE_LIST,
    AT_LIST,
    AT_MAP,
    AT_NULL,
    AT_STRUCT,
    ArrayArena,
    ArrayData,
    ArrowType,
    bit_get,
    bit_set,
)

from .json import Json, json_quote, parse_json
from .kernels import (
    arrow_type_for,
    cast_array,
    extract_datum,
    concat_into,
    constant_array,
    empty_array,
    filter_array,
    is_binary_type,
    is_large_binary_type,
)
from .types import (
    NestedField,
    TK_LIST,
    TK_MAP,
    TK_PRIMITIVE,
    TK_STRUCT,
    TypeNode,
    TypeStore,
)
from .values import Datum, datum_from_json_prim


comptime ELEMENT_NAME = String("element")
comptime ENTRIES_NAME = String("entries")
comptime KEY_NAME = String("key")
comptime VALUE_NAME = String("value")


def is_nested_arrow(t: ArrowType) -> Bool:
    return (
        t.id == AT_STRUCT
        or t.id == AT_LIST
        or t.id == AT_LARGE_LIST
        or t.id == AT_MAP
    )


@fieldwise_init
struct ColumnType(Copyable, Movable):
    """One column's Iceberg type: an arena and the index of its root node."""

    var store: TypeStore
    var type: Int

    @staticmethod
    def none() raises -> Self:
        return Self(TypeStore(), -1)

    def is_nested(self) -> Bool:
        if self.type < 0:
            return False
        return self.store.nodes[self.type].kind != TK_PRIMITIVE


struct ColumnTree(Copyable, Defaultable, Movable):
    """One column's Arrow array, with the arena its children live in."""

    var arena: ArrayArena
    var root: Int

    def __init__(out self):
        self.arena = ArrayArena()
        self.root = self.arena.add(ArrayData())

    def __init__(out self, var a: ArrayData):
        self.arena = ArrayArena()
        self.root = self.arena.add(a^)

    def __init__(out self, var arena: ArrayArena, root: Int):
        self.arena = arena^
        self.root = root

    def __init__(out self, *, copy: Self):
        self.arena = copy.arena.copy()
        self.root = copy.root

    def __init__(out self, *, deinit move: Self):
        self.arena = move.arena^
        self.root = move.root

    def node(ref self) -> ref[self.arena.nodes[0]] ArrayData:
        return self.arena.nodes[self.root]

    def length(self) -> Int:
        return self.arena.nodes[self.root].length

    def is_flat(self) -> Bool:
        return len(self.arena.nodes) == 1

    def take_arena(deinit self) -> ArrayArena:
        """The arena, moved out — read `root` before calling this."""
        return self.arena^


# ── moving and copying subtrees ─────────────────────────────────────────────
def subtree_copy(src: ArrayArena, node: Int, mut dst: ArrayArena) raises -> Int:
    """Deep-copy `node` and everything under it into another arena."""
    var a = src.nodes[node].copy()
    if len(a.children) == 0:
        return dst.add(a^)
    var kids = List[Int]()
    for k in range(len(a.children)):
        kids.append(subtree_copy(src, a.children[k], dst))
    a.children = kids^
    return dst.add(a^)


def subtree_lowest(arena: ArrayArena, node: Int) -> Int:
    """The smallest arena index in `node`'s subtree."""
    var lo = node
    ref kids = arena.nodes[node].children
    if len(kids) == 0:
        return lo
    for k in range(len(kids)):
        var c = subtree_lowest(arena, kids[k])
        if c < lo:
            lo = c
    return lo


def take_subtree(mut src: ArrayArena, node: Int) raises -> ColumnTree:
    """Move `node`'s subtree out of `src`, or copy it when it is not on top.

    parquet.mojo appends every child before its parent, so a root is the last
    node of its own subtree and the subtree occupies one contiguous range. A
    column can therefore be lifted out of a decoded batch without copying a
    single buffer, which is what keeps a flat scan free of per-batch copies.
    """
    var lo = subtree_lowest(src, node)
    if node != len(src.nodes) - 1 or node - lo + 1 > len(src.nodes):
        var out = ArrayArena()
        var r = subtree_copy(src, node, out)
        return ColumnTree(out^, r)
    var n = node - lo + 1
    var rev = List[ArrayData]()
    for _ in range(n):
        rev.append(src.nodes.pop())
    var out = ArrayArena()
    for _ in range(n):
        var a = rev.pop()
        for k in range(len(a.children)):
            a.children[k] -= lo
        _ = out.add(a^)
    return ColumnTree(out^, node - lo)


def move_tree_into(
    mut src: ArrayArena, root: Int, mut dst: ArrayArena
) raises -> Int:
    """Move every node of `src` into `dst`; returns the root's new index.

    `src` must hold exactly one column's tree, which is what a `ScanColumn`
    owns — nothing is copied, so handing a finished result to Arrow costs
    nothing but the renumbering.
    """
    var base = len(dst.nodes)
    var n = len(src.nodes)
    var rev = List[ArrayData]()
    for _ in range(n):
        rev.append(src.nodes.pop())
    for _ in range(n):
        var a = rev.pop()
        for k in range(len(a.children)):
            a.children[k] += base
        _ = dst.add(a^)
    return base + root


# ── the Arrow type of an Iceberg type ───────────────────────────────────────
def arrow_type_of(store: TypeStore, i: Int) raises -> ArrowType:
    ref n = store.nodes[i]
    if n.kind == TK_STRUCT:
        return ArrowType(AT_STRUCT)
    if n.kind == TK_LIST:
        return ArrowType(AT_LIST)
    if n.kind == TK_MAP:
        return ArrowType(AT_MAP)
    return arrow_type_for(n.prim, n.precision, n.scale, n.length)


# ── empty and all-null trees ────────────────────────────────────────────────
def empty_tree(
    mut dst: ArrayArena,
    store: TypeStore,
    ti: Int,
    name: String,
    field_id: Int,
    nullable: Bool,
) raises -> Int:
    """A zero-row array of the Iceberg type at `ti`."""
    ref t = store.nodes[ti]
    if t.kind == TK_PRIMITIVE:
        var a = empty_array(name, field_id, arrow_type_of(store, ti))
        a.nullable = nullable
        return dst.add(a^)
    var kids = List[Int]()
    if t.kind == TK_STRUCT:
        for k in range(len(t.fields)):
            ref f = t.fields[k]
            kids.append(
                empty_tree(dst, store, f.type, f.name, f.id, not f.required)
            )
    elif t.kind == TK_LIST:
        kids.append(
            empty_tree(
                dst,
                store,
                t.element,
                ELEMENT_NAME,
                t.element_id,
                not t.element_required,
            )
        )
    else:
        kids.append(_empty_entries(dst, store, t))
    var a = ArrayData(arrow_type_of(store, ti), name)
    a.nullable = nullable
    a.field_id = Int32(field_id)
    a.length = 0
    if t.kind != TK_STRUCT:
        a.offsets.append(0)
    a.children = kids^
    return dst.add(a^)


def _empty_entries(
    mut dst: ArrayArena, store: TypeStore, t: TypeNode
) raises -> Int:
    var kids = List[Int]()
    kids.append(empty_tree(dst, store, t.key, KEY_NAME, t.key_id, False))
    kids.append(
        empty_tree(
            dst, store, t.value, VALUE_NAME, t.value_id, not t.value_required
        )
    )
    var entries = ArrayData(ArrowType(AT_STRUCT), ENTRIES_NAME)
    entries.nullable = False
    entries.length = 0
    entries.children = kids^
    return dst.add(entries^)


def null_tree(
    mut dst: ArrayArena,
    store: TypeStore,
    ti: Int,
    name: String,
    field_id: Int,
    nullable: Bool,
    n: Int,
) raises -> Int:
    """`n` null values of the Iceberg type at `ti`.

    A null container still has to be shaped like one: a null struct carries
    `n` (null) children so its columns line up, a null list carries `n + 1`
    equal offsets over an empty element array.
    """
    ref t = store.nodes[ti]
    if t.kind == TK_PRIMITIVE:
        var a = constant_array(
            name,
            field_id,
            t.prim,
            t.precision,
            t.scale,
            t.length,
            Datum.none(),
            n,
        )
        a.nullable = nullable
        return dst.add(a^)
    var kids = List[Int]()
    if t.kind == TK_STRUCT:
        for k in range(len(t.fields)):
            ref f = t.fields[k]
            kids.append(
                null_tree(dst, store, f.type, f.name, f.id, not f.required, n)
            )
    elif t.kind == TK_LIST:
        kids.append(
            empty_tree(
                dst,
                store,
                t.element,
                ELEMENT_NAME,
                t.element_id,
                not t.element_required,
            )
        )
    else:
        kids.append(_empty_entries(dst, store, t))
    var a = ArrayData(arrow_type_of(store, ti), name)
    a.nullable = nullable
    a.field_id = Int32(field_id)
    a.length = n
    a.null_count = n
    for _ in range((n + 7) // 8):
        a.validity.append(0)
    if t.kind != TK_STRUCT:
        for _ in range(n + 1):
            a.offsets.append(0)
    a.children = kids^
    return dst.add(a^)


def default_tree(
    mut dst: ArrayArena, store: TypeStore, f: NestedField, n: Int
) raises -> Int:
    """A column a data file does not have: its `initial-default`, else null.

    The spec's column-projection rules end at "return the default value if it
    has a defined `initial-default`, otherwise null". A nested field with a
    nested default is the one shape not built here — it falls through to
    null, which is what a reader that does not understand the default must
    do anyway.
    """
    ref t = store.nodes[f.type]
    if f.has_initial_default and t.kind == TK_PRIMITIVE:
        var doc = parse_json(f.initial_default)
        var d = datum_from_json_prim(
            t.prim, t.precision, t.scale, t.length, doc, doc.root
        )
        var a = constant_array(
            f.name,
            f.id,
            t.prim,
            t.precision,
            t.scale,
            t.length,
            d,
            n,
        )
        a.nullable = not f.required
        return dst.add(a^)
    return null_tree(dst, store, f.type, f.name, f.id, not f.required, n)


# ── casting a decoded column to the table's current type ────────────────────
def cast_column(
    var src: ColumnTree,
    store: TypeStore,
    ti: Int,
    name: String,
    field_id: Int,
    nullable: Bool,
) raises -> ColumnTree:
    """The file's column, produced at the table's current Iceberg type.

    A flat column is *moved* through `kernels.cast_array`, which retags rather
    than copies whenever the file's physical type already matches. A nested
    one is rebuilt child by child, matching the file's children to the table's
    fields **by field id** — which is what makes a field added, renamed,
    reordered or promoted inside a struct come out right.
    """
    if store.nodes[ti].kind == TK_PRIMITIVE and src.is_flat():
        var a = src.arena.nodes.pop()
        return ColumnTree(
            cast_array(a^, arrow_type_of(store, ti), name, field_id)
        )
    var out = ArrayArena()
    var r = cast_tree(
        src.arena, src.root, store, ti, name, field_id, nullable, out
    )
    return ColumnTree(out^, r)


def cast_tree(
    src: ArrayArena,
    node: Int,
    store: TypeStore,
    ti: Int,
    name: String,
    field_id: Int,
    nullable: Bool,
    mut dst: ArrayArena,
) raises -> Int:
    ref t = store.nodes[ti]
    ref s = src.nodes[node]
    if t.kind == TK_PRIMITIVE:
        if is_nested_arrow(s.type):
            raise Error(
                "iceberg: column '"
                + name
                + "' is "
                + String(s.type)
                + " in the file but a primitive in the table schema"
            )
        var a = s.copy()
        return dst.add(cast_array(a^, arrow_type_of(store, ti), name, field_id))

    if t.kind == TK_STRUCT:
        if s.type.id != AT_STRUCT:
            raise Error(
                "iceberg: column '" + name + "' is not a struct in the file"
            )
        var kids = List[Int]()
        for k in range(len(t.fields)):
            ref f = t.fields[k]
            var at = _child_by_id(src, node, f.id)
            if at < 0:
                kids.append(default_tree(dst, store, f, s.length))
            else:
                kids.append(
                    cast_tree(
                        src,
                        at,
                        store,
                        f.type,
                        f.name,
                        f.id,
                        not f.required,
                        dst,
                    )
                )
        var out = ArrayData(ArrowType(AT_STRUCT), name)
        out.nullable = nullable
        out.field_id = Int32(field_id)
        out.length = s.length
        out.null_count = s.null_count
        out.validity = s.validity.copy()
        out.children = kids^
        return dst.add(out^)

    if t.kind == TK_LIST:
        if s.type.id != AT_LIST and s.type.id != AT_LARGE_LIST:
            raise Error(
                "iceberg: column '" + name + "' is not a list in the file"
            )
        if len(s.children) != 1:
            raise Error("iceberg: list column '" + name + "' has no element")
        var kid = cast_tree(
            src,
            s.children[0],
            store,
            t.element,
            ELEMENT_NAME,
            t.element_id,
            not t.element_required,
            dst,
        )
        var out = _container_of(s, ArrowType(AT_LIST), name, field_id, nullable)
        out.children.append(kid)
        return dst.add(out^)

    # A map: a list of a two-field struct.
    if s.type.id != AT_MAP and s.type.id != AT_LIST:
        raise Error("iceberg: column '" + name + "' is not a map in the file")
    if len(s.children) != 1:
        raise Error("iceberg: map column '" + name + "' has no entries")
    var entries = s.children[0]
    if len(src.nodes[entries].children) != 2:
        raise Error(
            "iceberg: map column '" + name + "' does not have key and value"
        )
    var key_at = _child_by_id(src, entries, t.key_id)
    if key_at < 0:
        key_at = src.nodes[entries].children[0]
    var val_at = _child_by_id(src, entries, t.value_id)
    if val_at < 0:
        val_at = src.nodes[entries].children[1]
    var pair = List[Int]()
    pair.append(
        cast_tree(src, key_at, store, t.key, KEY_NAME, t.key_id, False, dst)
    )
    pair.append(
        cast_tree(
            src,
            val_at,
            store,
            t.value,
            VALUE_NAME,
            t.value_id,
            not t.value_required,
            dst,
        )
    )
    var kv = ArrayData(ArrowType(AT_STRUCT), ENTRIES_NAME)
    kv.nullable = False
    kv.length = src.nodes[entries].length
    kv.children = pair^
    var kv_at = dst.add(kv^)
    var out = _container_of(s, ArrowType(AT_MAP), name, field_id, nullable)
    out.children.append(kv_at)
    return dst.add(out^)


def _child_by_id(src: ArrayArena, node: Int, id: Int) -> Int:
    ref kids = src.nodes[node].children
    for k in range(len(kids)):
        if src.nodes[kids[k]].field_id == Int32(id):
            return kids[k]
    return -1


def _container_of(
    s: ArrayData, var t: ArrowType, name: String, field_id: Int, nullable: Bool
) raises -> ArrayData:
    """A list or map header carrying `s`'s validity and offsets."""
    var out = ArrayData(t^, name)
    out.nullable = nullable
    out.field_id = Int32(field_id)
    out.length = s.length
    out.null_count = s.null_count
    out.validity = s.validity.copy()
    if len(s.offsets) > 0:
        out.offsets = s.offsets.copy()
    else:
        for k in range(len(s.large_offsets)):
            out.offsets.append(Int32(s.large_offsets[k]))
    if len(out.offsets) == 0:
        out.offsets.append(0)
    return out^


# ── filter ──────────────────────────────────────────────────────────────────
def filter_tree(
    src: ArrayArena,
    node: Int,
    keep: List[Bool],
    n_keep: Int,
    mut dst: ArrayArena,
) raises -> Int:
    """The rows of a nested column for which `keep` is true, in order."""
    ref s = src.nodes[node]
    if not is_nested_arrow(s.type):
        return dst.add(filter_array(s, keep, n_keep))

    if s.type.id == AT_STRUCT:
        var kids = List[Int]()
        for k in range(len(s.children)):
            kids.append(filter_tree(src, s.children[k], keep, n_keep, dst))
        var out = ArrayData(s.type.copy(), s.name.copy())
        out.nullable = s.nullable
        out.field_id = s.field_id
        out.length = n_keep
        _filter_validity(s, keep, n_keep, out)
        out.children = kids^
        return dst.add(out^)

    # A list or a map: keep whole containers, and with them their elements.
    var child = s.children[0]
    var child_len = src.nodes[child].length
    var child_keep = List[Bool](length=child_len, fill=False)
    var kept_children = 0
    var offsets = List[Int32]()
    offsets.append(0)
    var at = 0
    for i in range(len(keep)):
        if not keep[i]:
            continue
        var lo = Int(s.offsets[i])
        var hi = Int(s.offsets[i + 1])
        for e in range(lo, hi):
            if e < child_len:
                child_keep[e] = True
                kept_children += 1
        at += hi - lo
        offsets.append(Int32(at))
    _ = kept_children
    var kid = filter_tree(src, child, child_keep, at, dst)
    var out = ArrayData(s.type.copy(), s.name.copy())
    out.nullable = s.nullable
    out.field_id = s.field_id
    out.length = n_keep
    _filter_validity(s, keep, n_keep, out)
    out.offsets = offsets^
    out.children.append(kid)
    return dst.add(out^)


def _filter_validity(
    s: ArrayData, keep: List[Bool], n_keep: Int, mut out: ArrayData
) raises:
    var no_nulls = len(s.validity) == 0
    if no_nulls:
        return
    var j = 0
    for i in range(len(keep)):
        if not keep[i]:
            continue
        var valid = bit_get(Span(s.validity), i)
        bit_set(out.validity, j, valid)
        if not valid:
            out.null_count += 1
        j += 1
    while len(out.validity) < (n_keep + 7) // 8:
        out.validity.append(0)
    if out.null_count == 0:
        out.validity.clear()


# ── concat ──────────────────────────────────────────────────────────────────
def concat_tree(
    mut dst: ArrayArena, dnode: Int, src: ArrayArena, snode: Int
) raises:
    """Append every row of `src`'s subtree to `dst`'s; the shapes must match."""
    if not is_nested_arrow(dst.nodes[dnode].type):
        var s = src.nodes[snode].copy()
        concat_into(dst.nodes[dnode], s)
        return
    if src.nodes[snode].length == 0:
        return
    var kids = dst.nodes[dnode].children.copy()
    var skids = src.nodes[snode].children.copy()
    if len(kids) != len(skids):
        raise Error("iceberg: cannot concatenate mismatched nested columns")
    var is_struct = dst.nodes[dnode].type.id == AT_STRUCT
    var base = dst.nodes[dnode].length
    _concat_header(dst.nodes[dnode], src.nodes[snode], is_struct)
    for k in range(len(kids)):
        concat_tree(dst, kids[k], src, skids[k])
    _ = base


def _concat_header(mut d: ArrayData, s: ArrayData, is_struct: Bool) raises:
    var base = d.length
    var src_no_nulls = len(s.validity) == 0
    if len(d.validity) == 0 and base > 0:
        var whole = base // 8
        for _ in range(whole):
            d.validity.append(0xFF)
        if base % 8:
            d.validity.append((UInt8(1) << UInt8(base % 8)) - 1)
    for i in range(s.length):
        var valid = True if src_no_nulls else bit_get(Span(s.validity), i)
        bit_set(d.validity, base + i, valid)
        if not valid:
            d.null_count += 1
    if not is_struct:
        if len(d.offsets) == 0:
            d.offsets.append(0)
        var shift = Int(d.offsets[len(d.offsets) - 1])
        for k in range(1, len(s.offsets)):
            d.offsets.append(Int32(Int(s.offsets[k]) + shift))
    d.length = base + s.length
    while len(d.validity) < (d.length + 7) // 8:
        d.validity.append(0)
    if d.null_count == 0:
        d.validity.clear()


# ── a struct leaf, seen as a flat column ────────────────────────────────────
def flatten_leaf(
    arena: ArrayArena, node: Int, path: List[Int]
) raises -> ArrayData:
    """The leaf `path` names inside a struct, as a top-level array.

    A struct's children are dense — one slot per row, whatever the struct's
    own validity says — so the leaf's buffers already line up with the rows.
    Only the validity has to be combined: a leaf under a null struct is null,
    which is exactly what a predicate on `a.b` must see.
    """
    var n = arena.nodes[node].length
    var valid = List[Bool](length=n, fill=True)
    var cur = node
    _and_validity(arena.nodes[cur], valid)
    for k in range(len(path)):
        ref kids = arena.nodes[cur].children
        if path[k] < 0 or path[k] >= len(kids):
            raise Error("iceberg: no such sub-field on this struct")
        cur = kids[path[k]]
        _and_validity(arena.nodes[cur], valid)
    var out = arena.nodes[cur].copy()
    out.validity.clear()
    out.null_count = 0
    var any_null = False
    for i in range(n):
        if not valid[i]:
            any_null = True
            break
    if any_null:
        for i in range(n):
            bit_set(out.validity, i, valid[i])
            if not valid[i]:
                out.null_count += 1
        while len(out.validity) < (n + 7) // 8:
            out.validity.append(0)
    return out^


def _and_validity(a: ArrayData, mut valid: List[Bool]):
    if len(a.validity) == 0:
        return
    for i in range(len(valid)):
        if valid[i] and not bit_get(Span(a.validity), i):
            valid[i] = False


# ── finding a struct leaf inside a column's type ─────────────────────────────
def find_struct_path(
    store: TypeStore, ti: Int, id: Int, mut out: List[Int]
) -> Bool:
    """Child positions from type `ti` down to field `id`, through structs.

    The path is relative to the *projected* type, not the table schema: a
    scan that reads only some of a struct's fields renumbers the rest, and
    the array it built follows the projection.
    """
    ref n = store.nodes[ti]
    if n.kind != TK_STRUCT:
        return False
    for k in range(len(n.fields)):
        out.append(k)
        if n.fields[k].id == id:
            return True
        if find_struct_path(store, n.fields[k].type, id, out):
            return True
        _ = out.pop()
    return False


# ── one nested cell, as JSON ────────────────────────────────────────────────
def cell_json(
    arena: ArrayArena, node: Int, store: TypeStore, ti: Int, row: Int
) raises -> String:
    """One value of a nested column, in the spec's JSON shape.

    Appendix D's single-value serialization, with one deliberate change: a
    struct is keyed by **field name** rather than field id, because these are
    rows a caller reads, not a default stored in metadata, and the row object
    around them is keyed by name too. Lists are JSON arrays and a map is
    `{"keys": [...], "values": [...]}`, both exactly as the spec says.
    """
    ref a = arena.nodes[node]
    ref t = store.nodes[ti]
    if not a.is_valid(row):
        return String("null")
    if t.kind == TK_PRIMITIVE:
        return extract_datum(
            a, row, t.prim, t.precision, t.scale, t.length
        ).to_json()
    if t.kind == TK_STRUCT:
        var out = String("{")
        for k in range(len(t.fields)):
            if k > 0:
                out += ","
            ref f = t.fields[k]
            out += json_quote(f.name) + ":"
            out += cell_json(arena, a.children[k], store, f.type, row)
        out += "}"
        return out^
    var lo = Int(a.offsets[row])
    var hi = Int(a.offsets[row + 1])
    if t.kind == TK_LIST:
        var out = String("[")
        for e in range(lo, hi):
            if e > lo:
                out += ","
            out += cell_json(arena, a.children[0], store, t.element, e)
        out += "]"
        return out^
    var entries = a.children[0]
    var kn = arena.nodes[entries].children[0]
    var vn = arena.nodes[entries].children[1]
    var keys = String('{"keys":[')
    var values = String('"values":[')
    for e in range(lo, hi):
        if e > lo:
            keys += ","
            values += ","
        keys += cell_json(arena, kn, store, t.key, e)
        values += cell_json(arena, vn, store, t.value, e)
    keys += "],"
    values += "]}"
    return keys + values


def cell_datum(
    arena: ArrayArena, node: Int, store: TypeStore, ti: Int, row: Int
) raises -> Datum:
    """One cell as a `Datum`: the value itself when the column is primitive,
    and the JSON text of `cell_json` when it is nested — `Datum` is a *tagged
    scalar*, so a struct, a list or a map arrives as its canonical JSON."""
    ref t = store.nodes[ti]
    if t.kind == TK_PRIMITIVE:
        return extract_datum(
            arena.nodes[node], row, t.prim, t.precision, t.scale, t.length
        )
    if not arena.nodes[node].is_valid(row):
        return Datum.none()
    return Datum.string_(cell_json(arena, node, store, ti, row))
