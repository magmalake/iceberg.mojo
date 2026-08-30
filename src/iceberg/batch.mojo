"""Building an Arrow `RecordBatch` from Mojo values.

The write path takes Arrow, which is the right input when the data already
*is* Arrow — a scan of another table, a Parquet file, anything speaking the C
Data Interface. When it is not, this is the bridge: a `ColumnBuilder` per
column, a `Datum` per cell, and `batch_of` to tie them together.

```mojo
var ids = ColumnBuilder(String("id"), 1, P_LONG)
var region = ColumnBuilder(String("region"), 2, P_STRING)
for k in range(n):
    ids.add(Datum.long_(Int64(k)))
    region.add(Datum.string_(names[k]))
var batch = batch_of([ids^, region^])
```

This is deliberately the *slow* side of the library — one tagged value per
cell, which is exactly what `iceberg.kernels` exists to avoid on the read
path. It is here because the alternative is asking a caller to lay out
validity bitmaps and offset buffers by hand, and a batch built once and
written once is not where the time goes.
"""

from std.memory import bitcast

from parquet import RecordBatch
from parquet.arrow import (
    AT_BINARY,
    AT_STRUCT,
    AT_BOOL,
    AT_DECIMAL128,
    AT_FIXED_SIZE_BINARY,
    AT_FLOAT32,
    AT_FLOAT64,
    AT_LARGE_BINARY,
    AT_LARGE_UTF8,
    AT_UTF8,
    ArrayArena,
    ArrayData,
    ArrowType,
    bit_set,
)

from .kernels import arrow_type_for, decimal_le16
from .json import Json, parse_json
from .nested import (
    ColumnTree,
    ColumnType,
    ELEMENT_NAME,
    ENTRIES_NAME,
    KEY_NAME,
    VALUE_NAME,
    arrow_type_of,
    empty_tree,
    move_tree_into,
)
from .schema import Schema
from .types import (
    P_LONG,
    P_STRING,
    TK_LIST,
    TK_MAP,
    TK_PRIMITIVE,
    TK_STRUCT,
    TypeStore,
)
from .values import Datum, datum_from_json_prim


struct ColumnBuilder(Copyable, Movable, Sized):
    """One column's values, before they become an Arrow array."""

    var name: String
    var field_id: Int
    var kind: UInt8
    var precision: Int
    var scale: Int
    var length: Int
    var nullable: Bool
    var values: List[Datum]

    def __init__(
        out self,
        var name: String,
        field_id: Int,
        kind: UInt8,
        precision: Int = 0,
        scale: Int = 0,
        length: Int = 0,
        nullable: Bool = True,
    ):
        self.name = name^
        self.field_id = field_id
        self.kind = kind
        self.precision = precision
        self.scale = scale
        self.length = length
        self.nullable = nullable
        self.values = []

    def __init__(out self, *, copy: Self):
        self.name = copy.name.copy()
        self.field_id = copy.field_id
        self.kind = copy.kind
        self.precision = copy.precision
        self.scale = copy.scale
        self.length = copy.length
        self.nullable = copy.nullable
        self.values = copy.values.copy()

    def __init__(out self, *, deinit move: Self):
        self.name = move.name^
        self.field_id = move.field_id
        self.kind = move.kind
        self.precision = move.precision
        self.scale = move.scale
        self.length = move.length
        self.nullable = move.nullable
        self.values = move.values^

    @staticmethod
    def of(schema: Schema, field_id: Int) raises -> Self:
        """A builder typed by one of the table's columns."""
        var f = schema.find_field(field_id)
        ref t = schema.store.nodes[f.type]
        if t.kind != TK_PRIMITIVE:
            raise Error(
                "iceberg: cannot build a nested column ('" + f.name + "')"
            )
        return Self(
            f.name,
            field_id,
            t.prim,
            t.precision,
            t.scale,
            t.length,
            not f.required,
        )

    def add(mut self, var value: Datum):
        self.values.append(value^)

    def add_null(mut self):
        self.values.append(Datum.none())

    def __len__(self) -> Int:
        return len(self.values)

    def build_tree(self) raises -> ColumnTree:
        """The same array, ready to sit beside a nested one."""
        return ColumnTree(self.build())

    def build(self) raises -> ArrayData:
        return array_from_datums(
            self.name,
            self.field_id,
            self.kind,
            self.precision,
            self.scale,
            self.length,
            self.nullable,
            self.values,
        )


def array_from_datums(
    name: String,
    field_id: Int,
    kind: UInt8,
    precision: Int,
    scale: Int,
    length: Int,
    nullable: Bool,
    values: List[Datum],
) raises -> ArrayData:
    """One Arrow array from a column of tagged values."""
    var t = arrow_type_for(kind, precision, scale, length)
    var a = ArrayData(t^, name)
    a.nullable = nullable
    a.field_id = Int32(field_id)
    if is_var_width_array(a):
        a.offsets.append(0)
    for r in range(len(values)):
        append_datum(a, values[r], kind)
    finish_array(a)
    return a^


def is_var_width_array(a: ArrayData) -> Bool:
    return (
        a.type.id == AT_UTF8
        or a.type.id == AT_BINARY
        or a.type.id == AT_LARGE_UTF8
        or a.type.id == AT_LARGE_BINARY
    )


def append_datum(mut a: ArrayData, d: Datum, kind: UInt8) raises:
    """Append one value to a primitive array, growing every buffer it has."""
    var r = a.length
    var width = a.type.fixed_width()
    bit_set(a.validity, r, d.valid)
    if not d.valid:
        a.null_count += 1
    if a.type.id == AT_BOOL:
        while len(a.values) <= r // 8:
            a.values.append(0)
        if d.valid and d.i != 0:
            a.values[r // 8] |= UInt8(1) << UInt8(r % 8)
    elif is_var_width_array(a):
        if d.valid:
            if kind == P_STRING:
                a.values.extend(d.s.as_bytes())
            else:
                a.values.extend(Span(d.b))
        a.offsets.append(Int32(len(a.values)))
    elif a.type.id == AT_DECIMAL128:
        var le = decimal_le16(d)
        for k in range(16):
            a.values.append(le[k])
    elif a.type.id == AT_FIXED_SIZE_BINARY:
        for k in range(width):
            a.values.append(d.b[k] if d.valid and k < len(d.b) else 0)
    elif a.type.id == AT_FLOAT32:
        _put_le(
            a.values,
            UInt64(
                UInt32(bitcast[DType.uint32](Float32(d.f if d.valid else 0.0)))
            ),
            4,
        )
    elif a.type.id == AT_FLOAT64:
        _put_le(a.values, bitcast[DType.uint64](d.f if d.valid else 0.0), 8)
    else:
        _put_le(a.values, UInt64(Int64(d.i if d.valid else 0)), width)
    a.length = r + 1


def finish_array(mut a: ArrayData):
    """Pad the buffers a partly filled last byte leaves short."""
    var n = a.length
    while len(a.validity) < (n + 7) // 8:
        a.validity.append(0)
    if a.type.id == AT_BOOL:
        while len(a.values) < (n + 7) // 8:
            a.values.append(0)


def _put_le(mut out: List[UInt8], v: UInt64, width: Int):
    for k in range(width):
        out.append(UInt8((v >> UInt64(8 * k)) & 0xFF))


def batch_of(columns: List[ColumnBuilder]) raises -> RecordBatch:
    """One `RecordBatch` from a set of equally long column builders."""
    var batch = RecordBatch()
    if len(columns) == 0:
        return batch^
    var n = len(columns[0])
    for k in range(len(columns)):
        if len(columns[k]) != n:
            raise Error(
                "iceberg: column '"
                + columns[k].name
                + "' has "
                + String(len(columns[k]))
                + " values, expected "
                + String(n)
            )
    batch.num_rows = n
    for k in range(len(columns)):
        batch.roots.append(batch.arena.add(columns[k].build()))
    return batch^


# ── nested columns ──────────────────────────────────────────────────────────
struct NestedBuilder(Copyable, Movable, Sized):
    """A struct, list or map column, one JSON value per row.

    A nested cell has no scalar to carry it, so the input here is text: the
    same JSON shape `ScanResult.cell` prints, which makes a round trip through
    this library one string per cell.

    ```mojo
    var addr = NestedBuilder.of(schema, 4)
    addr.add('{"city":"eu","zip":10}')
    addr.add_null()
    var batch = batch_of_columns([ids^.build_tree(), addr^.build()])
    ```

    Structs are objects keyed by field **name**, lists are arrays, and a map is
    either `{"keys": [...], "values": [...]}` — the spec's own shape — or a
    plain object when its keys are strings.
    """

    var name: String
    var field_id: Int
    var ctype: ColumnType
    var nullable: Bool
    var rows: List[String]

    def __init__(
        out self,
        var name: String,
        field_id: Int,
        var ctype: ColumnType,
        nullable: Bool = True,
    ):
        self.name = name^
        self.field_id = field_id
        self.ctype = ctype^
        self.nullable = nullable
        self.rows = []

    def __init__(out self, *, copy: Self):
        self.name = copy.name.copy()
        self.field_id = copy.field_id
        self.ctype = copy.ctype.copy()
        self.nullable = copy.nullable
        self.rows = copy.rows.copy()

    def __init__(out self, *, deinit move: Self):
        self.name = move.name^
        self.field_id = move.field_id
        self.ctype = move.ctype^
        self.nullable = move.nullable
        self.rows = move.rows^

    @staticmethod
    def of(schema: Schema, field_id: Int) raises -> Self:
        """A builder typed by one of the table's columns."""
        var f = schema.find_field(field_id)
        var sel = schema.select([field_id])
        ref top = sel.store.nodes[sel.root]
        return Self(
            f.name,
            field_id,
            ColumnType(sel.store.copy(), top.fields[0].type),
            not f.required,
        )

    def add(mut self, var json: String):
        self.rows.append(json^)

    def add_null(mut self):
        self.rows.append(String("null"))

    def __len__(self) -> Int:
        return len(self.rows)

    def build(self) raises -> ColumnTree:
        var arena = ArrayArena()
        var root = empty_tree(
            arena,
            self.ctype.store,
            self.ctype.type,
            self.name,
            self.field_id,
            self.nullable,
        )
        for r in range(len(self.rows)):
            var doc = parse_json(self.rows[r])
            push_json(
                arena, root, self.ctype.store, self.ctype.type, doc, doc.root
            )
        finish_tree(arena, root)
        return ColumnTree(arena^, root)


def push_json(
    mut arena: ArrayArena,
    node: Int,
    store: TypeStore,
    ti: Int,
    doc: Json,
    at: Int,
) raises:
    """Append one JSON value to a (possibly nested) Arrow array."""
    ref t = store.nodes[ti]
    var present = at >= 0 and not doc.is_null(at)
    if t.kind == TK_PRIMITIVE:
        var d = Datum.none()
        if present:
            d = datum_from_json_prim(
                t.prim, t.precision, t.scale, t.length, doc, at
            )
        append_datum(arena.nodes[node], d, t.prim)
        return

    var r = arena.nodes[node].length
    bit_set(arena.nodes[node].validity, r, present)
    if not present:
        arena.nodes[node].null_count += 1
    arena.nodes[node].length = r + 1
    var kids = arena.nodes[node].children.copy()

    if t.kind == TK_STRUCT:
        for k in range(len(t.fields)):
            ref f = t.fields[k]
            var child_at = -1
            if present:
                child_at = doc.get(at, f.name)
            push_json(arena, kids[k], store, f.type, doc, child_at)
        return

    if t.kind == TK_LIST:
        var n = doc.size(at) if present else 0
        for k in range(n):
            push_json(arena, kids[0], store, t.element, doc, doc.at(at, k))
        arena.nodes[node].offsets.append(Int32(arena.nodes[kids[0]].length))
        return

    # A map: `{"keys": [...], "values": [...]}`, or an object of string keys.
    var entries = kids[0]
    var pair = arena.nodes[entries].children.copy()
    var n = 0
    if present:
        var keys = doc.get(at, "keys")
        var values = doc.get(at, "values")
        if keys >= 0 and values >= 0:
            n = doc.size(keys)
            for k in range(n):
                push_json(arena, pair[0], store, t.key, doc, doc.at(keys, k))
                push_json(
                    arena, pair[1], store, t.value, doc, doc.at(values, k)
                )
        else:
            if store.nodes[t.key].prim != P_STRING:
                raise Error(
                    "iceberg: a map with non-string keys needs the"
                    ' {"keys": [...], "values": [...]} form'
                )
            n = doc.size(at)
            for k in range(n):
                append_datum(
                    arena.nodes[pair[0]],
                    Datum.string_(doc.key_at(at, k)),
                    P_STRING,
                )
                push_json(arena, pair[1], store, t.value, doc, doc.at(at, k))
    arena.nodes[entries].length += n
    arena.nodes[node].offsets.append(Int32(arena.nodes[entries].length))


def finish_tree(mut arena: ArrayArena, node: Int) raises:
    """Pad every buffer in a freshly built tree."""
    var kids = arena.nodes[node].children.copy()
    if len(kids) > 0:
        for k in range(len(kids)):
            finish_tree(arena, kids[k])
    var n = arena.nodes[node].length
    while len(arena.nodes[node].validity) < (n + 7) // 8:
        arena.nodes[node].validity.append(0)
    if arena.nodes[node].type.id == AT_BOOL:
        while len(arena.nodes[node].values) < (n + 7) // 8:
            arena.nodes[node].values.append(0)


def batch_of_columns(var columns: List[ColumnTree]) raises -> RecordBatch:
    """One `RecordBatch` from column trees, nested or not."""
    var batch = RecordBatch()
    if len(columns) == 0:
        return batch^
    batch.num_rows = columns[0].length()
    for k in range(len(columns)):
        if columns[k].length() != batch.num_rows:
            raise Error(
                "iceberg: column '"
                + columns[k].node().name
                + "' has "
                + String(columns[k].length())
                + " values, expected "
                + String(batch.num_rows)
            )
        batch.roots.append(
            move_tree_into(columns[k].arena, columns[k].root, batch.arena)
        )
    return batch^
