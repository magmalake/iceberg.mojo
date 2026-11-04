"""Schema evolution: add columns, rename them, make them optional, widen them.

    var u = UpdateSchema()
    u.add_column("note", "string")
    u.add_nested_column("addr", "zip", "int")
    u.rename_column("amount", "total")
    u.make_column_optional("region")
    u.update_column_type("qty", "long")
    _ = table.update_schema(u)                 # a filesystem table
    _ = rest.update_schema("db", "orders", u)  # or through a catalog

The shape is Java's and PyIceberg's `UpdateSchema`: a list of changes that
is applied to the table's current schema when the commit is prepared, so a
commit that loses a race re-applies the same changes to whatever schema won.
What it produces is what the spec asks for:

* every new field — the column itself, then the fields, elements, keys and
  values inside its type — gets a fresh id above the table's
  `last-column-id`, which moves up to the highest id handed out;
* the result is a **new** schema with the next free `schema-id`, appended to
  `schemas`, and `current-schema-id` points at it. A result equal to a
  schema the table already has reuses that schema's id instead, as Java does;
  a result equal to the current schema commits nothing at all;
* through a REST catalog the request carries `assert-table-uuid`,
  `assert-current-schema-id` and `assert-last-assigned-field-id`, and the
  updates are `add-schema` then `set-current-schema` with `-1` ("the schema
  just added").

Nothing here rewrites a data file. A file written before a column existed has
no column with that id, and the reader's projection by field id already
produces null for it; a widened column is read at the file's width and cast.

**Only compatible changes.** An added column is optional — rows already in the
table have no value for it, and there is no `initial-default` support on the
write side. Type changes are the spec's promotions only: `int`→`long`,
`float`→`double`, and `decimal(P, S)`→`decimal(P', S)` with `P' > P`. Columns
are not deleted or moved. `union_by_name` refuses any type difference rather
than promoting, so a caller handing it a schema always learns about a type
change instead of having it applied.
"""

from .json import json_quote, parse_json
from .schema import Schema, _copy_type
from .metadata import MetadataLogEntry, TableMetadata
from .types import (
    NestedField,
    TypeStore,
    TK_LIST,
    TK_MAP,
    TK_PRIMITIVE,
    TK_STRUCT,
    P_DECIMAL,
    P_DOUBLE,
    P_FLOAT,
    P_INT,
    P_LONG,
    P_UNRECOGNIZED,
)
from .util import now_ms


comptime _ADD = 0
comptime _RENAME = 1
comptime _OPTIONAL = 2
comptime _UNION = 3
comptime _PROMOTE = 4


@fieldwise_init
struct _SchemaOp(Copyable, Movable):
    var kind: Int
    var path: String
    """The parent for an add ("" for a top-level column); the field for the
    rest. Dotted, as `Schema.flat` names fields."""
    var name: String
    """The new column's name, or a renamed field's new name."""
    var text: String
    """An add's or a promotion's type as Iceberg JSON, or a union's schema."""
    var doc: String
    var has_doc: Bool


struct UpdateSchema(Copyable, Defaultable, Movable):
    """Schema changes, recorded now and applied at commit time."""

    var ops: List[_SchemaOp]

    def __init__(out self):
        self.ops = []

    def is_empty(self) -> Bool:
        return len(self.ops) == 0

    def add_column(
        mut self, name: String, type: String, doc: String = String("")
    ) raises:
        """Add an optional top-level column.

        `type` is the Iceberg JSON type: a primitive's spelling (`long`,
        `decimal(10, 2)`, quoted or not) or a `struct`/`list`/`map` object.
        Field ids inside a nested type are placeholders; they are replaced by
        fresh ones.
        """
        _check_name(name)
        self.ops.append(
            _SchemaOp(_ADD, String(""), name, _type_text(type), doc, doc != "")
        )

    def add_nested_column(
        mut self,
        parent: String,
        name: String,
        type: String,
        doc: String = String(""),
    ) raises:
        """Add an optional field to the struct at `parent` (`addr`, `a.b`).

        A `parent` that is a list adds to its element struct, and a map to its
        value struct, the way Java resolves a parent.
        """
        _check_name(name)
        if parent == "":
            raise Error("iceberg: add_nested_column needs a parent")
        self.ops.append(
            _SchemaOp(_ADD, parent, name, _type_text(type), doc, doc != "")
        )

    def rename_column(mut self, path: String, new_name: String) raises:
        """Rename a field. Its id, and so every data file, is unaffected."""
        _check_name(new_name)
        self.ops.append(
            _SchemaOp(_RENAME, path, new_name, String(""), String(""), False)
        )

    def make_column_optional(mut self, path: String):
        """Let a required field (or a list element, or a map value) be null."""
        self.ops.append(
            _SchemaOp(
                _OPTIONAL, path, String(""), String(""), String(""), False
            )
        )

    def update_column_type(mut self, path: String, new_type: String) raises:
        """Widen a primitive the way the spec allows: `int`→`long`,
        `float`→`double`, `decimal(P, S)`→`decimal(P', S)` with `P' > P`."""
        self.ops.append(
            _SchemaOp(
                _PROMOTE,
                path,
                String(""),
                _type_text(new_type),
                String(""),
                False,
            )
        )

    def union_by_name(mut self, schema: Schema):
        """Make the table's schema hold every field of `schema`, matched by
        name: a missing field is added (it must be optional), a field
        `schema` marks optional becomes optional, a changed `doc` is taken,
        and fields `schema` does not mention are kept. Structs are merged at
        any depth, including a list's element and a map's value. A field
        whose type differs raises: no promotion, no replacement.

        `schema`'s own field ids are ignored — only names count.
        """
        self.ops.append(
            _SchemaOp(
                _UNION, String(""), String(""), schema.to_json(), "", False
            )
        )

    def apply(
        self, base: Schema, last_column_id: Int
    ) raises -> Tuple[Schema, Int]:
        """The changes applied to `base`: the new schema (still carrying
        `base`'s id) and the new `last-column-id`."""
        var s = base.copy()
        var next_id = last_column_id
        for k in range(len(self.ops)):
            ref op = self.ops[k]
            if op.kind == _ADD:
                _apply_add(s, op, next_id)
            elif op.kind == _RENAME:
                _apply_rename(s, op)
            elif op.kind == _OPTIONAL:
                _apply_optional(s, op.path)
            elif op.kind == _PROMOTE:
                _apply_promote(s, op)
            else:
                var other = Schema.parse(op.text)
                _union_struct(s, s.root, "", other, other.root, next_id)
            s._index()
        return (s^, next_id)


@fieldwise_init
struct SchemaCommit(Copyable, Movable):
    """What committing an `UpdateSchema` would do to one table metadata."""

    var metadata: TableMetadata
    """The table metadata after the change. Not persisted."""
    var schema: Schema
    """The new current schema."""
    var added: Bool
    """A new schema was appended to `schemas` (else an existing one is
    current again)."""
    var changed: Bool
    """False when the changes leave the current schema as it was; there is
    then nothing to commit."""


def prepare_schema_update(
    metadata: TableMetadata, update: UpdateSchema
) raises -> SchemaCommit:
    """Apply `update` to `metadata`'s current schema and build the metadata a
    commit would write. Nothing is persisted: the caller commits it."""
    var current = metadata.schema()
    var applied = update.apply(current, metadata.last_column_id)
    var schema = applied[0].copy()
    var last_column_id = max(applied[1], metadata.last_column_id)
    if _same_schema(schema, current):
        return SchemaCommit(metadata.copy(), current^, False, False)

    var out = metadata.copy()
    var added = True
    var id = -1
    var highest = -1
    for k in range(len(out.schemas)):
        if out.schemas[k].schema_id > highest:
            highest = out.schemas[k].schema_id
        if id < 0 and _same_schema(schema, out.schemas[k]):
            id = out.schemas[k].schema_id
    if id >= 0:
        added = False
    else:
        id = highest + 1
    schema.schema_id = id
    if added:
        out.schemas.append(schema.copy())
    out.current_schema_id = id
    out.last_column_id = last_column_id
    out.last_updated_ms = now_ms()
    if metadata.metadata_file_location != "":
        out.metadata_log.append(
            MetadataLogEntry(
                metadata.last_updated_ms, metadata.metadata_file_location
            )
        )
    return SchemaCommit(out^, schema^, added, True)


def schema_update_body(
    namespace: String,
    table: String,
    current: TableMetadata,
    commit: SchemaCommit,
) raises -> String:
    """The REST `CommitTableRequest` for a schema change.

    The requirements are Java's: the table is the one we read, its current
    schema is the one the changes were applied to, and no column id has been
    handed out since — so a concurrent schema change is a 409, not two columns
    with the same id.
    """
    var out = String('{"identifier":{"namespace":[')
    var parts = namespace.split(".")
    for k in range(len(parts)):
        if k > 0:
            out += ","
        out += _quote(String(parts[k]))
    out += '],"name":' + _quote(table) + "}"
    out += ',"requirements":['
    out += (
        '{"type":"assert-table-uuid","uuid":' + _quote(current.table_uuid) + "}"
    )
    out += (
        ',{"type":"assert-current-schema-id","current-schema-id":'
        + String(current.current_schema_id)
        + "}"
    )
    out += (
        ',{"type":"assert-last-assigned-field-id","last-assigned-field-id":'
        + String(current.last_column_id)
        + "}"
    )
    out += '],"updates":['
    if commit.added:
        out += (
            '{"action":"add-schema","schema":'
            + commit.schema.to_json()
            + ',"last-column-id":'
            + String(commit.metadata.last_column_id)
            + "},"
        )
        # -1 is "the schema this request just added": a server may number
        # schemas itself, and this is how the spec lets a client not care.
        out += '{"action":"set-current-schema","schema-id":-1}'
    else:
        out += (
            '{"action":"set-current-schema","schema-id":'
            + String(commit.schema.schema_id)
            + "}"
        )
    out += "]}"
    return out^


# ── applying one change ────────────────────────────────────────────────────


def _quote(s: String) -> String:
    return json_quote(s)


def _check_name(name: String) raises:
    if name == "":
        raise Error("iceberg: a column name cannot be empty")
    if "." in name:
        raise Error(
            "iceberg: '"
            + name
            + "' has a dot in it, which a dotted path could not tell apart"
            " from nesting; add it to its parent with add_nested_column"
        )


def _type_text(type: String) raises -> String:
    """`long` and `"long"` both mean the primitive; an object stays as is."""
    var t = String(type.strip())
    if t.startswith("{") or t.startswith('"'):
        return t^
    return '"' + t + '"'


def _exact_index(s: Schema, path: String) -> Int:
    """Position in `s.flat` of the field named exactly `path`, or -1."""
    for k in range(len(s.flat)):
        if s.flat[k].name == path:
            return k
    return -1


def _field_at(s: Schema, path: String) raises -> Int:
    var k = _exact_index(s, path)
    if k < 0:
        raise Error("iceberg: the schema has no field '" + path + "'")
    return k


def _owner_struct(s: Schema, k: Int) -> Int:
    """The struct node that holds `s.flat[k]` as one of its fields."""
    var p = s.flat[k].parent
    if p < 0:
        return s.root
    return s.flat[p].type


def _is_identifier(s: Schema, id: Int) -> Bool:
    for k in range(len(s.identifier_field_ids)):
        if s.identifier_field_ids[k] == id:
            return True
    return False


def _struct_target(s: Schema, parent: String) raises -> Int:
    """The struct node a field added under `parent` goes into."""
    if parent == "":
        return s.root
    var k = _field_at(s, parent)
    var t = s.flat[k].type
    ref n = s.store.nodes[t]
    if n.kind == TK_LIST:
        t = n.element
    elif n.kind == TK_MAP:
        t = n.value
    if s.store.nodes[t].kind != TK_STRUCT:
        raise Error(
            "iceberg: cannot add a field to '"
            + parent
            + "', which is a "
            + s.store.type_name(t)
            + ", not a struct"
        )
    return t


def _field_named(store: TypeStore, node: Int, name: String) -> Int:
    ref fields = store.nodes[node].fields
    for k in range(len(fields)):
        if fields[k].name == name:
            return k
    return -1


def _assign_fresh_ids(mut store: TypeStore, t: Int, mut next_id: Int):
    """Give every field, element, key and value under type `t` a new id.

    A struct's own fields are numbered before anything inside them, which is
    the order Java's `AssignFreshIds` and PyIceberg's use.
    """
    var kind = store.nodes[t].kind
    if kind == TK_STRUCT:
        var n = len(store.nodes[t].fields)
        for k in range(n):
            next_id += 1
            store.nodes[t].fields[k].id = next_id
        for k in range(n):
            _assign_fresh_ids(store, store.nodes[t].fields[k].type, next_id)
    elif kind == TK_LIST:
        next_id += 1
        store.nodes[t].element_id = next_id
        _assign_fresh_ids(store, store.nodes[t].element, next_id)
    elif kind == TK_MAP:
        next_id += 1
        store.nodes[t].key_id = next_id
        next_id += 1
        store.nodes[t].value_id = next_id
        _assign_fresh_ids(store, store.nodes[t].key, next_id)
        _assign_fresh_ids(store, store.nodes[t].value, next_id)


def _check_known(store: TypeStore, t: Int) raises:
    """Refuse a type spelling this build does not recognise, at any depth —
    a typo would otherwise be committed as an opaque type."""
    ref n = store.nodes[t]
    if n.kind == TK_PRIMITIVE:
        if n.prim == P_UNRECOGNIZED:
            raise Error("iceberg: unknown type '" + n.raw + "'")
        return
    if n.kind == TK_STRUCT:
        for k in range(len(n.fields)):
            _check_known(store, n.fields[k].type)
    elif n.kind == TK_LIST:
        _check_known(store, n.element)
    else:
        _check_known(store, n.key)
        _check_known(store, n.value)


def _append_field(
    mut s: Schema,
    struct_node: Int,
    name: String,
    type: Int,
    doc: String,
    has_doc: Bool,
    label: String,
    mut next_id: Int,
) raises:
    if _field_named(s.store, struct_node, name) >= 0:
        raise Error(
            "iceberg: cannot add '" + label + "': a field with that name exists"
        )
    next_id += 1
    var f = NestedField.simple(next_id, name, False, type)
    if has_doc:
        f.doc = doc
        f.has_doc = True
    _assign_fresh_ids(s.store, type, next_id)
    s.store.nodes[struct_node].fields.append(f^)


def _apply_add(mut s: Schema, op: _SchemaOp, mut next_id: Int) raises:
    var target = _struct_target(s, op.path)
    var doc = parse_json(op.text)
    var t = s.store.parse_type(doc, doc.root)
    _check_known(s.store, t)
    var label = op.name if op.path == "" else op.path + "." + op.name
    _append_field(s, target, op.name, t, op.doc, op.has_doc, label, next_id)


def _apply_rename(mut s: Schema, op: _SchemaOp) raises:
    var k = _field_at(s, op.path)
    var pos = s.flat[k].child_pos
    if pos < 0:
        raise Error(
            "iceberg: '"
            + op.path
            + "' is a list element or a map key or value, which has no name"
            " of its own to change"
        )
    var owner = _owner_struct(s, k)
    var clash = _field_named(s.store, owner, op.name)
    if clash >= 0 and clash != pos:
        raise Error(
            "iceberg: cannot rename '"
            + op.path
            + "' to '"
            + op.name
            + "': a field with that name exists"
        )
    s.store.nodes[owner].fields[pos].name = op.name


def _apply_optional(mut s: Schema, path: String) raises:
    var k = _field_at(s, path)
    if _is_identifier(s, s.flat[k].id):
        raise Error(
            "iceberg: '"
            + path
            + "' is an identifier field, and identifier fields are required"
        )
    var pos = s.flat[k].child_pos
    if pos >= 0:
        s.store.nodes[_owner_struct(s, k)].fields[pos].required = False
        return
    var container = s.flat[s.flat[k].parent].type
    if s.store.nodes[container].kind == TK_LIST:
        s.store.nodes[container].element_required = False
    elif path.endswith(".value"):
        s.store.nodes[container].value_required = False
    else:
        raise Error("iceberg: a map key is always required: '" + path + "'")


def _can_promote(
    store: TypeStore, from_t: Int, to: TypeStore, to_t: Int
) -> Bool:
    ref a = store.nodes[from_t]
    ref b = to.nodes[to_t]
    if a.kind != TK_PRIMITIVE or b.kind != TK_PRIMITIVE:
        return False
    if a.prim == P_INT and b.prim == P_LONG:
        return True
    if a.prim == P_FLOAT and b.prim == P_DOUBLE:
        return True
    if a.prim == P_DECIMAL and b.prim == P_DECIMAL:
        return b.scale == a.scale and b.precision > a.precision
    return False


def _apply_promote(mut s: Schema, op: _SchemaOp) raises:
    var k = _field_at(s, op.path)
    var t = s.flat[k].type
    var store = TypeStore()
    var doc = parse_json(op.text)
    var want = store.parse_type(doc, doc.root)
    if _same_type(s.store, t, store, want):
        return
    if not _can_promote(s.store, t, store, want):
        raise Error(
            "iceberg: cannot change '"
            + op.path
            + "' from "
            + s.store.type_name(t)
            + " to "
            + store.type_name(want)
            + "; the spec allows int to long, float to double and a wider"
            " decimal precision"
        )
    ref w = store.nodes[want]
    s.store.nodes[t].prim = w.prim
    s.store.nodes[t].precision = w.precision
    s.store.nodes[t].scale = w.scale


# ── union by name ──────────────────────────────────────────────────────────


def _same_type(a: TypeStore, x: Int, b: TypeStore, y: Int) -> Bool:
    """Structural equality across two stores, by name, ignoring field ids."""
    ref p = a.nodes[x]
    ref q = b.nodes[y]
    if p.kind != q.kind:
        return False
    if p.kind == TK_PRIMITIVE:
        return a.type_name(x) == b.type_name(y)
    if p.kind == TK_LIST:
        return p.element_required == q.element_required and _same_type(
            a, p.element, b, q.element
        )
    if p.kind == TK_MAP:
        return (
            p.value_required == q.value_required
            and _same_type(a, p.key, b, q.key)
            and _same_type(a, p.value, b, q.value)
        )
    if len(p.fields) != len(q.fields):
        return False
    for k in range(len(p.fields)):
        if (
            p.fields[k].name != q.fields[k].name
            or p.fields[k].required != q.fields[k].required
            or not _same_type(a, p.fields[k].type, b, q.fields[k].type)
        ):
            return False
    return True


def _join(prefix: String, name: String) -> String:
    return name if prefix == "" else prefix + "." + name


def _type_clash(path: String, have: String, want: String) -> Error:
    return Error(
        "iceberg: cannot change the type of '"
        + path
        + "' from "
        + have
        + " to "
        + want
        + " by name; promote it explicitly with update_column_type, or"
        " write it as a new column"
    )


def _union_struct(
    mut s: Schema,
    node: Int,
    prefix: String,
    other: Schema,
    other_node: Int,
    mut next_id: Int,
) raises:
    var n_other = len(other.store.nodes[other_node].fields)
    for j in range(n_other):
        var f = other.store.nodes[other_node].fields[j].copy()
        var path = _join(prefix, f.name)
        var k = _field_named(s.store, node, f.name)
        if k < 0:
            if f.required:
                raise Error(
                    "iceberg: cannot add '"
                    + path
                    + "' as a required column: the rows already in the table"
                    " have no value for it. Make it optional"
                )
            var t = _copy_type(other.store, f.type, s.store)
            _append_field(s, node, f.name, t, f.doc, f.has_doc, path, next_id)
            continue
        var mine = s.store.nodes[node].fields[k].copy()
        if mine.required and not f.required:
            if _is_identifier(s, mine.id):
                raise Error(
                    "iceberg: '"
                    + path
                    + "' is an identifier field, and identifier fields are"
                    " required"
                )
            s.store.nodes[node].fields[k].required = False
        if f.has_doc and (not mine.has_doc or mine.doc != f.doc):
            s.store.nodes[node].fields[k].doc = f.doc
            s.store.nodes[node].fields[k].has_doc = True
        _union_type(s, mine.type, path, other, f.type, next_id)


def _union_type(
    mut s: Schema,
    t: Int,
    path: String,
    other: Schema,
    ot: Int,
    mut next_id: Int,
) raises:
    var kind = s.store.nodes[t].kind
    var okind = other.store.nodes[ot].kind
    if kind != okind:
        raise _type_clash(path, s.store.type_name(t), other.store.type_name(ot))
    if kind == TK_STRUCT:
        _union_struct(s, t, path, other, ot, next_id)
    elif kind == TK_LIST:
        if (
            s.store.nodes[t].element_required
            and not other.store.nodes[ot].element_required
        ):
            s.store.nodes[t].element_required = False
        _union_type(
            s,
            s.store.nodes[t].element,
            path + ".element",
            other,
            other.store.nodes[ot].element,
            next_id,
        )
    elif kind == TK_MAP:
        if not _same_type(
            s.store,
            s.store.nodes[t].key,
            other.store,
            other.store.nodes[ot].key,
        ):
            raise _type_clash(
                path + ".key",
                s.store.type_name(s.store.nodes[t].key),
                other.store.type_name(other.store.nodes[ot].key),
            )
        if (
            s.store.nodes[t].value_required
            and not other.store.nodes[ot].value_required
        ):
            s.store.nodes[t].value_required = False
        _union_type(
            s,
            s.store.nodes[t].value,
            path + ".value",
            other,
            other.store.nodes[ot].value,
            next_id,
        )
    elif not _same_type(s.store, t, other.store, ot):
        raise _type_clash(path, s.store.type_name(t), other.store.type_name(ot))


def _same_schema(a: Schema, b: Schema) -> Bool:
    """The same fields, ids, types and identifier fields; ids of the schemas
    themselves aside."""
    if len(a.identifier_field_ids) != len(b.identifier_field_ids):
        return False
    for k in range(len(a.identifier_field_ids)):
        if a.identifier_field_ids[k] != b.identifier_field_ids[k]:
            return False
    var x = a.copy()
    var y = b.copy()
    x.schema_id = 0
    y.schema_id = 0
    return x.to_json() == y.to_json()
