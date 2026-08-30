"""Reading rows: `TableScan.to_table()` and `to_batches()`.

This is the half of a scan that `plan_files()` stops short of. For each
`FileScanTask` it opens the data file through the `FileIO`, decodes it with
[parquet.mojo](https://github.com/magmalake/parquet.mojo), resolves every
projected column **by field id**, applies the deletes the planner associated,
evaluates whatever is left of the filter, and hands back Arrow arrays.

Column projection follows the spec's order exactly. For each projected field
id, in turn:

1. the column with that id in the data file;
2. otherwise, the value from the manifest's partition tuple, when an identity
   partition field is defined on that source column — the "metadata-only Hive
   migration" case;
3. otherwise, the column the table property `schema.name-mapping.default` maps
   that id onto, for files written without field ids at all;
4. otherwise, the field's `initial-default`;
5. otherwise, null.

Schema evolution falls out of that: a renamed column is found by id under its
new name, a column added after the file was written resolves to its default or
null, and a promoted column (`int`->`long`, `float`->`double`, a decimal whose
precision grew) is read at the file's physical width and *cast* to the table's
current type by a columnar kernel.

**Nothing is materialised per cell.** The rows a scan returns are Arrow arrays
throughout: `iceberg.kernels` casts them, builds constant arrays for the
columns a file does not have, and applies deletes and the residual predicate as
one `List[Bool]` per batch followed by a single filter pass per column. A
`Datum` is built only when a caller asks for one — `ScanResult.value`, CSV and
JSON output — or for the handful of predicate shapes no kernel covers, which
`_vector_leaf` names.

Deletes are applied in the order the spec implies. A deletion vector, when one
applies, replaces every position delete file for that data file; otherwise
position delete files are read and their `pos` values collected for this file
path. Equality deletes are matched last, on the columns named by
`equality_ids`, through a hashed lookup over canonically encoded keys, with
`null` equal to `null` as the spec requires. The planner has already scoped
every delete by sequence number and partition.
"""

from std.collections import Dict
from std.memory import bitcast

from parquet import ParquetReader, Predicate, RecordBatch
from parquet.reader import (
    OP_EQ as OP_EQ_PQ,
    OP_GE as OP_GE_PQ,
    OP_GT as OP_GT_PQ,
    OP_LE as OP_LE_PQ,
    OP_LT as OP_LT_PQ,
)
from parquet.stats import SV_NONE, ScalarValue
from parquet.arrow import (
    AT_BINARY,
    AT_BOOL,
    AT_DATE32,
    AT_DECIMAL128,
    AT_FIXED_SIZE_BINARY,
    AT_FLOAT32,
    AT_FLOAT64,
    AT_INT16,
    AT_INT32,
    AT_INT64,
    AT_INT8,
    AT_LARGE_BINARY,
    AT_LARGE_UTF8,
    AT_TIME32,
    AT_TIME64,
    AT_TIMESTAMP,
    AT_UINT16,
    AT_UINT32,
    AT_UINT64,
    AT_UINT8,
    AT_UTF8,
    TU_MICRO,
    TU_NANO,
    ArrayArena,
    ArrayData,
    ArrowType,
    bit_get,
    bit_set,
    load_f32,
    load_f64,
    load_i32,
    load_i64,
)
from parquet.ext_full import AllCodecs

from .expressions import (
    Expr,
    OP_AND,
    OP_EQ,
    OP_FALSE,
    OP_GT,
    OP_GT_EQ,
    OP_IN,
    OP_IS_NAN,
    OP_IS_NULL,
    OP_LT,
    OP_LT_EQ,
    OP_NOT,
    OP_NOT_EQ,
    OP_NOT_IN,
    OP_NOT_NAN,
    OP_NOT_NULL,
    OP_NOT_STARTS_WITH,
    OP_OR,
    OP_STARTS_WITH,
    OP_TRUE,
    bind,
    parse_filter,
)
from .io import FileIO
from .json import Json, json_quote, parse_json, substr
from .nested import (
    ColumnTree,
    ColumnType,
    cast_column,
    cell_datum,
    cell_json,
    default_tree,
    empty_tree,
    find_struct_path,
    filter_tree,
    concat_tree,
    move_tree_into,
    flatten_leaf,
    null_tree,
    subtree_copy,
    take_subtree,
)
from .kernels import (
    CMP_BYTES,
    CMP_FLOAT,
    CMP_INT,
    CMP_NONE,
    append_key,
    arrow_type_for,
    bytes_at,
    cast_array,
    compare_class,
    concat_into,
    constant_array,
    decimal_le16,
    empty_array,
    extract_datum,
    filter_array,
    float_at,
    hash_key,
    int64_array,
    int_at,
    is_binary_type,
    is_large_binary_type,
    is_var_width,
    keys_equal,
    value_extent,
)
from .manifest import DataFile
from .metadata import TableMetadata
from .puffin import deleted_positions
from .schema import Schema
from .transforms import PartitionSpec, T_IDENTITY
from .types import (
    NestedField,
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
    TK_PRIMITIVE,
    TK_STRUCT,
    TypeStore,
)
from .values import (
    Datum,
    compare,
    datum_from_json_prim,
    int64_to_be_twos,
)


# ── reserved ids ────────────────────────────────────────────────────────────
comptime POS_DELETE_FILE_PATH_ID = 2147483546
comptime POS_DELETE_POS_ID = 2147483545

comptime META_FILE = String("_file")
comptime META_POS = String("_pos")
comptime META_SPEC_ID = String("_spec_id")
comptime META_PARTITION = String("_partition")
comptime META_ROW_ID = String("_row_id")
comptime META_LAST_UPDATED = String("_last_updated_sequence_number")

comptime NAME_MAPPING_PROPERTY = String("schema.name-mapping.default")


def is_metadata_column(name: String) -> Bool:
    return (
        name == META_FILE
        or name == META_POS
        or name == META_SPEC_ID
        or name == META_PARTITION
        or name == META_ROW_ID
        or name == META_LAST_UPDATED
    )


def arrow_type_of(
    kind: UInt8, precision: Int, scale: Int, length: Int
) raises -> ArrowType:
    """The Arrow type an Iceberg primitive is produced at."""
    return arrow_type_for(kind, precision, scale, length)


# ── a scan's rows ───────────────────────────────────────────────────────────
struct ScanColumn(Copyable, Movable):
    """One output column: its name, its Iceberg type, and its Arrow array.

    A primitive column is a single array. A struct, list or map column is a
    whole tree, so the array lives in the column's own `arena` with `root`
    naming it and every child referring to a sibling index — the layout
    parquet.mojo decodes into and `iceberg.nested` operates on. `ctype` is
    the column's Iceberg type, which is what tells a `Datum`, a JSON cell or
    a CSV cell how to read the bytes; it is empty for a metadata column,
    which is always one of a handful of primitives.
    """

    var name: String
    var field_id: Int
    var kind: UInt8
    var precision: Int
    var scale: Int
    var length: Int
    var ctype: ColumnType
    var arena: ArrayArena
    var root: Int

    def __init__(
        out self,
        var name: String,
        field_id: Int,
        kind: UInt8,
        precision: Int,
        scale: Int,
        length: Int,
        var array: ArrayData,
    ):
        self.name = name^
        self.field_id = field_id
        self.kind = kind
        self.precision = precision
        self.scale = scale
        self.length = length
        self.ctype = ColumnType(TypeStore(), -1)
        self.arena = ArrayArena()
        self.root = self.arena.add(array^)

    def __init__(
        out self,
        var name: String,
        field_id: Int,
        kind: UInt8,
        precision: Int,
        scale: Int,
        length: Int,
        var ctype: ColumnType,
        var tree: ColumnTree,
    ):
        self.name = name^
        self.field_id = field_id
        self.kind = kind
        self.precision = precision
        self.scale = scale
        self.length = length
        self.ctype = ctype^
        self.root = tree.root
        self.arena = tree^.take_arena()

    @staticmethod
    def empty(
        var name: String,
        field_id: Int,
        kind: UInt8,
        precision: Int,
        scale: Int,
        length: Int,
    ) raises -> Self:
        var a = empty_array(
            name, field_id, arrow_type_for(kind, precision, scale, length)
        )
        return Self(name^, field_id, kind, precision, scale, length, a^)

    def __init__(out self, *, copy: Self):
        self.name = copy.name.copy()
        self.field_id = copy.field_id
        self.kind = copy.kind
        self.precision = copy.precision
        self.scale = copy.scale
        self.length = copy.length
        self.ctype = copy.ctype.copy()
        self.arena = copy.arena.copy()
        self.root = copy.root

    def __init__(out self, *, deinit move: Self):
        self.name = move.name^
        self.field_id = move.field_id
        self.kind = move.kind
        self.precision = move.precision
        self.scale = move.scale
        self.length = move.length
        self.ctype = move.ctype^
        self.arena = move.arena^
        self.root = move.root

    def array(ref self) -> ref[self.arena.nodes[0]] ArrayData:
        """The column's Arrow array; its children index into `self.arena`."""
        return self.arena.nodes[self.root]

    def is_nested(self) -> Bool:
        return self.ctype.is_nested()

    def num_rows(self) -> Int:
        return self.arena.nodes[self.root].length

    def take_tree(deinit self) -> ColumnTree:
        return ColumnTree(self.arena^, self.root)


struct ScanResult(Copyable, Defaultable, Movable):
    """The rows a scan returned, column-major, as Arrow arrays."""

    var columns: List[ScanColumn]

    def __init__(out self):
        self.columns = []

    def __init__(out self, var columns: List[ScanColumn]):
        self.columns = columns^

    def __init__(out self, *, copy: Self):
        self.columns = copy.columns.copy()

    def __init__(out self, *, deinit move: Self):
        self.columns = move.columns^

    def num_columns(self) -> Int:
        return len(self.columns)

    def num_rows(self) -> Int:
        if len(self.columns) == 0:
            return 0
        return self.columns[0].num_rows()

    def name(self, i: Int) -> String:
        return self.columns[i].name

    def value(self, row: Int, col: Int) raises -> Datum:
        """One cell, typed as the table's current schema says it is.

        This is the only place a `Datum` is built on the read path, and it is
        built on demand. A `Datum` is a tagged *scalar*, so a struct, list or
        map cell arrives as its canonical JSON text — `cell` gives the same
        thing without the `Datum` around it.
        """
        ref c = self.columns[col]
        if c.ctype.type >= 0:
            return cell_datum(c.arena, c.root, c.ctype.store, c.ctype.type, row)
        return _extract(
            c.arena.nodes[c.root], row, c.kind, c.precision, c.scale, c.length
        )

    def cell(self, row: Int, col: Int) raises -> String:
        """One cell as JSON — nested for a struct, list or map."""
        ref c = self.columns[col]
        if c.ctype.type >= 0:
            return cell_json(c.arena, c.root, c.ctype.store, c.ctype.type, row)
        return _extract(
            c.arena.nodes[c.root], row, c.kind, c.precision, c.scale, c.length
        ).to_json()

    def column(
        ref self, i: Int
    ) -> ref[self.columns[0].arena.nodes[0]] ArrayData:
        return self.columns[i].arena.nodes[self.columns[i].root]

    def append(mut self, other: ScanResult) raises:
        """Concatenate another result with the same columns."""
        if len(self.columns) == 0:
            self.columns = other.columns.copy()
            return
        if other.num_rows() == 0:
            return
        if len(self.columns) != len(other.columns):
            raise Error("iceberg: cannot append results with different shapes")
        for k in range(len(self.columns)):
            concat_tree(
                self.columns[k].arena,
                self.columns[k].root,
                other.columns[k].arena,
                other.columns[k].root,
            )

    # ── output ─────────────────────────────────────────────────────────────
    def to_csv(self, header: Bool = True) raises -> String:
        var out = String("")
        if header:
            for c in range(len(self.columns)):
                if c > 0:
                    out += ","
                out += _csv_cell(self.columns[c].name)
            out += "\n"
        for r in range(self.num_rows()):
            for c in range(len(self.columns)):
                if c > 0:
                    out += ","
                var d = self.value(r, c)
                out += "" if not d.valid else _csv_cell(d.repr_())
            out += "\n"
        return out^

    def to_json(self) raises -> String:
        """One JSON object per row, in an array — the Appendix-D encoding."""
        var out = String("[")
        for r in range(self.num_rows()):
            if r > 0:
                out += ","
            out += "{"
            for c in range(len(self.columns)):
                if c > 0:
                    out += ","
                out += json_quote(self.columns[c].name) + ":"
                out += self.cell(r, c)
            out += "}"
        out += "]"
        return out^

    def to_batch(self) raises -> RecordBatch:
        """The same rows as an Arrow `RecordBatch`, ready for `export_c`.

        A nested column brings its whole subtree across, renumbered into the
        batch's arena, so the C Data Interface export of a nested scan is the
        same call as the export of a flat one.
        """
        var batch = RecordBatch()
        batch.num_rows = self.num_rows()
        for c in range(len(self.columns)):
            batch.roots.append(
                subtree_copy(
                    self.columns[c].arena, self.columns[c].root, batch.arena
                )
            )
        return batch^

    def take_batch(deinit self) raises -> RecordBatch:
        """`to_batch`, consuming the result so no buffer is copied."""
        var batch = RecordBatch()
        batch.num_rows = self.num_rows()
        var cols = self.columns^
        var n = len(cols)
        var rev = List[ColumnTree]()
        for _ in range(n):
            rev.append(cols.pop().take_tree())
        for _ in range(n):
            var t = rev.pop()
            batch.roots.append(move_tree_into(t.arena, t.root, batch.arena))
        return batch^


def _csv_cell(s: String) -> String:
    var needs = (
        s.find(",") >= 0
        or s.find('"') >= 0
        or s.find("\n") >= 0
        or s.find("\r") >= 0
    )
    if not needs:
        return s
    var out = String('"')
    var b = s.as_bytes()
    for k in range(len(b)):
        if b[k] == 34:
            out += '""'
        else:
            out += String(StringSlice(unsafe_from_utf8=Span(b)[k : k + 1]))
    out += '"'
    return out^


# ── reading one value out of an Arrow array ─────────────────────────────────
def _int_at(a: ArrayData, i: Int) raises -> Int64:
    return int_at(a, i)


def _float_at(a: ArrayData, i: Int) raises -> Float64:
    return float_at(a, i)


def _bytes_at(a: ArrayData, i: Int) raises -> List[UInt8]:
    return bytes_at(a, i)


def _extract(
    a: ArrayData, i: Int, kind: UInt8, precision: Int, scale: Int, length: Int
) raises -> Datum:
    """One value, typed as the table's current schema says it is."""
    return extract_datum(a, i, kind, precision, scale, length)


def _decimal_le16(d: Datum) -> List[UInt8]:
    return decimal_le16(d)


# ── column plans ────────────────────────────────────────────────────────────
comptime SRC_FILE = 0
comptime SRC_CONSTANT = 1
comptime SRC_POS = 2
comptime SRC_ROW_ID = 3


@fieldwise_init
struct _ColumnPlan(Copyable, Movable):
    var name: String
    var field_id: Int
    var kind: UInt8
    var precision: Int
    var scale: Int
    var length: Int
    var source: Int
    var batch_index: Int
    """Which column of the Parquet batch, for `SRC_FILE`."""
    var constant: Datum
    var field: NestedField
    """The schema field this column projects, whose `type` indexes `read`."""
    var read: ColumnType
    """The type the file's column is cast to: everything this scan must see,
    which is the projection plus whatever the residual still needs."""
    var out: ColumnType
    """The type the caller asked for. Narrower than `read` only when the
    residual reaches a nested field beside the ones being projected."""
    var reproject: Bool
    var nullable: Bool

    def is_nested(self) -> Bool:
        return self.read.is_nested()


@fieldwise_init
struct _LeafPlan(Copyable, Movable):
    """A primitive nested inside a struct, as a filter sees it.

    `a.b > 5` binds to the id of `b`, which is not a column of the batch: it
    is a child of the column `a`. The path is the chain of child positions
    from that column's root, which `nested.flatten_leaf` walks to produce a
    flat array with the parent structs' nulls folded in.
    """

    var field_id: Int
    var slot: Int
    var path: List[Int]
    var kind: UInt8
    var precision: Int
    var scale: Int
    var length: Int


@fieldwise_init
struct NameMapping(Copyable, Defaultable, Movable):
    """`schema.name-mapping.default`: fallback ids for files without them."""

    var names: List[String]
    var ids: List[Int]

    def __init__(out self):
        self.names = []
        self.ids = []

    @staticmethod
    def parse(text: String) raises -> Self:
        var doc = parse_json(text)
        var out = Self()
        out._collect(doc, doc.root, "")
        return out^

    def _collect(mut self, doc: Json, node: Int, prefix: String) raises:
        for k in range(doc.size(node)):
            var e = doc.at(node, k)
            var id = -1
            var fi = doc.get(e, "field-id")
            if fi >= 0 and not doc.is_null(fi):
                id = Int(doc.as_int(fi))
            var names = doc.get(e, "names")
            var first = String("")
            if names >= 0:
                for j in range(doc.size(names)):
                    var n = doc.as_string(doc.at(names, j))
                    var full = n if prefix == "" else prefix + "." + n
                    if id >= 0:
                        self.names.append(full)
                        self.ids.append(id)
                    if j == 0:
                        first = full^
            var kids = doc.get(e, "fields")
            if kids >= 0 and not doc.is_null(kids):
                self._collect(doc, kids, first)

    def id_for(self, name: String) -> Int:
        for k in range(len(self.names)):
            if self.names[k] == name:
                return self.ids[k]
        return -1

    def name_for(self, id: Int) -> String:
        for k in range(len(self.ids)):
            if self.ids[k] == id:
                return self.names[k]
        return String("")


# ── row-level predicate evaluation ──────────────────────────────────────────
def collect_field_ids(e: Expr, i: Int, mut out: List[Int]):
    """Every column the expression touches, by id."""
    if i < 0:
        return
    ref n = e.nodes[i]
    if n.op == OP_AND or n.op == OP_OR:
        collect_field_ids(e, n.left, out)
        collect_field_ids(e, n.right, out)
        return
    if n.op == OP_NOT:
        collect_field_ids(e, n.left, out)
        return
    if n.op == OP_TRUE or n.op == OP_FALSE:
        return
    if n.field_id < 0:
        return
    for k in range(len(out)):
        if out[k] == n.field_id:
            return
    out.append(n.field_id)


def eval_leaf(e: Expr, i: Int, v: Datum) raises -> Bool:
    """One bound leaf predicate against one value.

    A predicate on a null value is false for everything except `is-null` and
    the negations that the planner's `rewrite_not` has already pushed down,
    which is Iceberg's three-valued logic collapsed the way a filter needs it.
    """
    ref n = e.nodes[i]
    if n.op == OP_IS_NULL:
        return not v.valid
    if n.op == OP_NOT_NULL:
        return v.valid
    if n.op == OP_IS_NAN:
        return v.valid and v.is_nan()
    if n.op == OP_NOT_NAN:
        return v.valid and not v.is_nan()
    if not v.valid:
        return False

    if n.op == OP_STARTS_WITH:
        return v.s.startswith(n.lits[0].s)
    if n.op == OP_NOT_STARTS_WITH:
        return not v.s.startswith(n.lits[0].s)
    if n.op == OP_IN or n.op == OP_NOT_IN:
        var found = False
        for k in range(len(n.lits)):
            if compare(v, n.lits[k]) == 0:
                found = True
                break
        return found if n.op == OP_IN else not found

    var c = compare(v, n.lits[0])
    if n.op == OP_EQ:
        return c == 0
    if n.op == OP_NOT_EQ:
        return c != 0
    if n.op == OP_LT:
        return c < 0
    if n.op == OP_LT_EQ:
        return c <= 0
    if n.op == OP_GT:
        return c > 0
    if n.op == OP_GT_EQ:
        return c >= 0
    raise Error("iceberg: cannot evaluate operator " + String(n.op))


def eval_row(e: Expr, i: Int, row: List[Datum], ids: List[Int]) raises -> Bool:
    """Evaluate a *bound* expression against one row.

    `row[k]` is the value of field `ids[k]`.
    """
    if i < 0:
        return True
    ref n = e.nodes[i]
    if n.op == OP_TRUE:
        return True
    if n.op == OP_FALSE:
        return False
    if n.op == OP_AND:
        return eval_row(e, n.left, row, ids) and eval_row(e, n.right, row, ids)
    if n.op == OP_OR:
        return eval_row(e, n.left, row, ids) or eval_row(e, n.right, row, ids)
    if n.op == OP_NOT:
        return not eval_row(e, n.left, row, ids)

    var slot = -1
    for k in range(len(ids)):
        if ids[k] == n.field_id:
            slot = k
            break
    if slot < 0:
        # A predicate on a column that is not being read: it was already
        # applied by the planner, or it cannot be evaluated here.
        return True
    return eval_leaf(e, i, row[slot])


# ── vectorised predicate evaluation ─────────────────────────────────────────
def _cmp_float(x: Float64, y: Float64) -> Int:
    """Iceberg's float order: NaN last, -0.0 == 0.0."""
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


def _cmp_bytes_at(a: ArrayData, i: Int, lit: List[UInt8]) raises -> Int:
    """Unsigned lexicographic order of element `i` against a literal."""
    var extent = value_extent(a, i)
    var n = extent[1] - extent[0]
    var m = n if n < len(lit) else len(lit)
    for k in range(m):
        var x = a.values[extent[0] + k]
        var y = lit[k]
        if x != y:
            return -1 if x < y else 1
    if n == len(lit):
        return 0
    return -1 if n < len(lit) else 1


def _starts_with_at(a: ArrayData, i: Int, lit: List[UInt8]) raises -> Bool:
    var extent = value_extent(a, i)
    if extent[1] - extent[0] < len(lit):
        return False
    for k in range(len(lit)):
        if a.values[extent[0] + k] != lit[k]:
            return False
    return True


def _literal_bytes(d: Datum) -> List[UInt8]:
    var out = List[UInt8]()
    if d.kind == P_STRING:
        out.extend(d.s.as_bytes())
    else:
        out.extend(Span(d.b))
    return out^


def _accept(op: UInt8, c: Int) raises -> Bool:
    if op == OP_EQ:
        return c == 0
    if op == OP_NOT_EQ:
        return c != 0
    if op == OP_LT:
        return c < 0
    if op == OP_LT_EQ:
        return c <= 0
    if op == OP_GT:
        return c > 0
    if op == OP_GT_EQ:
        return c >= 0
    raise Error("iceberg: cannot evaluate operator " + String(op))


def _is_comparison(op: UInt8) -> Bool:
    return (
        op == OP_EQ
        or op == OP_NOT_EQ
        or op == OP_LT
        or op == OP_LT_EQ
        or op == OP_GT
        or op == OP_GT_EQ
    )


def _vector_leaf(
    e: Expr, i: Int, a: ArrayData, mut out: List[Bool]
) raises -> Bool:
    """Evaluate one leaf predicate over a whole column.

    Returns `False` when no kernel covers this predicate/type pair, which is
    the caller's cue to fall back to `Datum` for that leaf alone. The shapes
    that fall back are: any comparison, `in` or `not-in` on a **decimal**
    column (Arrow stores it as a 16-byte little-endian two's complement, which
    is not orderable by a byte compare), and `starts-with` on anything that is
    not a byte-shaped column.
    """
    ref nd = e.nodes[i]
    var n = a.length
    var no_nulls = len(a.validity) == 0

    if nd.op == OP_IS_NULL:
        for r in range(n):
            out[r] = False if no_nulls else not bit_get(Span(a.validity), r)
        return True
    if nd.op == OP_NOT_NULL:
        for r in range(n):
            out[r] = True if no_nulls else bit_get(Span(a.validity), r)
        return True

    var cls = compare_class(a.type)

    if nd.op == OP_IS_NAN or nd.op == OP_NOT_NAN:
        var want_nan = nd.op == OP_IS_NAN
        for r in range(n):
            var valid = True if no_nulls else bit_get(Span(a.validity), r)
            if not valid:
                out[r] = False
                continue
            if cls != CMP_FLOAT:
                out[r] = not want_nan
                continue
            var v = float_at(a, r)
            out[r] = (v != v) if want_nan else (v == v)
        return True

    if cls == CMP_NONE:
        return False

    if nd.op == OP_STARTS_WITH or nd.op == OP_NOT_STARTS_WITH:
        if cls != CMP_BYTES:
            return False
        var lit = _literal_bytes(nd.lits[0])
        var want = nd.op == OP_STARTS_WITH
        for r in range(n):
            var valid = True if no_nulls else bit_get(Span(a.validity), r)
            if not valid:
                out[r] = False
                continue
            out[r] = _starts_with_at(a, r, lit) == want
        return True

    if nd.op == OP_IN or nd.op == OP_NOT_IN:
        var want = nd.op == OP_IN
        if cls == CMP_INT:
            var vals = List[Int64]()
            for k in range(len(nd.lits)):
                vals.append(nd.lits[k].i)
            for r in range(n):
                var valid = True if no_nulls else bit_get(Span(a.validity), r)
                if not valid:
                    out[r] = False
                    continue
                var v = int_at(a, r)
                var found = False
                for k in range(len(vals)):
                    if vals[k] == v:
                        found = True
                        break
                out[r] = found == want
            return True
        if cls == CMP_FLOAT:
            var fvals = List[Float64]()
            for k in range(len(nd.lits)):
                ref d = nd.lits[k]
                fvals.append(
                    d.f if (
                        d.kind == P_FLOAT or d.kind == P_DOUBLE
                    ) else Float64(d.i)
                )
            for r in range(n):
                var valid = True if no_nulls else bit_get(Span(a.validity), r)
                if not valid:
                    out[r] = False
                    continue
                var v = float_at(a, r)
                var found = False
                for k in range(len(fvals)):
                    if _cmp_float(v, fvals[k]) == 0:
                        found = True
                        break
                out[r] = found == want
            return True
        var bvals = List[List[UInt8]]()
        for k in range(len(nd.lits)):
            bvals.append(_literal_bytes(nd.lits[k]))
        for r in range(n):
            var valid = True if no_nulls else bit_get(Span(a.validity), r)
            if not valid:
                out[r] = False
                continue
            var found = False
            for k in range(len(bvals)):
                if _cmp_bytes_at(a, r, bvals[k]) == 0:
                    found = True
                    break
            out[r] = found == want
        return True

    if not _is_comparison(nd.op):
        return False
    if len(nd.lits) == 0:
        return False

    if cls == CMP_INT:
        var lit = nd.lits[0].i
        for r in range(n):
            var valid = True if no_nulls else bit_get(Span(a.validity), r)
            if not valid:
                out[r] = False
                continue
            var v = int_at(a, r)
            var c = 0 if v == lit else (-1 if v < lit else 1)
            out[r] = _accept(nd.op, c)
        return True
    if cls == CMP_FLOAT:
        ref d = nd.lits[0]
        var flit = d.f if (
            d.kind == P_FLOAT or d.kind == P_DOUBLE
        ) else Float64(d.i)
        for r in range(n):
            var valid = True if no_nulls else bit_get(Span(a.validity), r)
            if not valid:
                out[r] = False
                continue
            out[r] = _accept(nd.op, _cmp_float(float_at(a, r), flit))
        return True
    var blit = _literal_bytes(nd.lits[0])
    for r in range(n):
        var valid = True if no_nulls else bit_get(Span(a.validity), r)
        if not valid:
            out[r] = False
            continue
        out[r] = _accept(nd.op, _cmp_bytes_at(a, r, blit))
    return True


def _selection(
    e: Expr,
    i: Int,
    arrays: List[ColumnTree],
    plans: List[_ColumnPlan],
    read_ids: List[Int],
    leaves: List[_LeafPlan],
    leaf_arrays: List[ArrayData],
    n: Int,
) raises -> List[Bool]:
    """A residual predicate as a selection bitmap over one batch."""
    if i < 0:
        return List[Bool](length=n, fill=True)
    ref nd = e.nodes[i]
    if nd.op == OP_TRUE:
        return List[Bool](length=n, fill=True)
    if nd.op == OP_FALSE:
        return List[Bool](length=n, fill=False)
    if nd.op == OP_AND:
        var left = _selection(
            e, nd.left, arrays, plans, read_ids, leaves, leaf_arrays, n
        )
        var any = False
        for r in range(n):
            if left[r]:
                any = True
                break
        if not any:
            return left^
        var right = _selection(
            e, nd.right, arrays, plans, read_ids, leaves, leaf_arrays, n
        )
        for r in range(n):
            left[r] = left[r] and right[r]
        return left^
    if nd.op == OP_OR:
        var left = _selection(
            e, nd.left, arrays, plans, read_ids, leaves, leaf_arrays, n
        )
        var right = _selection(
            e, nd.right, arrays, plans, read_ids, leaves, leaf_arrays, n
        )
        for r in range(n):
            left[r] = left[r] or right[r]
        return left^
    if nd.op == OP_NOT:
        var inner = _selection(
            e, nd.left, arrays, plans, read_ids, leaves, leaf_arrays, n
        )
        for r in range(n):
            inner[r] = not inner[r]
        return inner^

    var slot = _index_of(read_ids, nd.field_id)
    if slot < 0:
        # A struct leaf: it came out of a column, not as one.
        for k in range(len(leaves)):
            if leaves[k].field_id != nd.field_id:
                continue
            ref lp = leaves[k]
            var out = List[Bool](length=n, fill=False)
            if _vector_leaf(e, i, leaf_arrays[k], out):
                return out^
            for r in range(n):
                out[r] = eval_leaf(
                    e,
                    i,
                    _extract(
                        leaf_arrays[k],
                        r,
                        lp.kind,
                        lp.precision,
                        lp.scale,
                        lp.length,
                    ),
                )
            return out^
        return List[Bool](length=n, fill=True)

    ref p = plans[slot]
    if p.source != SRC_FILE:
        # A constant column: one evaluation decides the whole batch.
        if p.is_nested():
            var v = eval_leaf(e, i, Datum.none())
            return List[Bool](length=n, fill=v)
        var v = eval_leaf(e, i, p.constant)
        return List[Bool](length=n, fill=v)

    var out = List[Bool](length=n, fill=False)
    ref a = arrays[slot].arena.nodes[arrays[slot].root]
    if _vector_leaf(e, i, a, out):
        return out^
    # No kernel for this shape: fall back to a `Datum` per row, for this
    # column only.
    for r in range(n):
        out[r] = eval_leaf(
            e, i, _extract(a, r, p.kind, p.precision, p.scale, p.length)
        )
    return out^


# ── scan options ────────────────────────────────────────────────────────────
@fieldwise_init
struct ScanOptions(Copyable, Defaultable, Movable):
    """How to read, as opposed to what."""

    var batch_size: Int
    """Rows per output batch. The default is one batch per Parquet row group.

    Chopping a row group into smaller batches costs and saves nothing: the
    decoder materialises the whole row group either way (`ParquetReader._load`
    decodes a column chunk in one go), so a small `batch_size` only multiplies
    the per-batch Arrow assembly, the per-batch selection vector and the
    per-batch concatenation. On the million-row bench table the 8192 this used
    to default to cost 24 ms of pure re-assembly. Set it smaller only when a
    consumer genuinely wants to see rows in smaller pieces.
    """
    var limit: Int
    """Stop after this many rows; -1 for all of them."""
    var verify_crc: Bool
    var prune: Bool
    """Use the residual to drop row groups and pages before decoding."""
    var lazy: Bool
    """Fetch the footer and only the surviving row groups, instead of the
    whole file. Worth it over the network, pointless on a local disk."""

    def __init__(out self):
        self.batch_size = 1 << 20
        self.limit = -1
        self.verify_crc = False
        self.prune = True
        self.lazy = False


# ── deletes ─────────────────────────────────────────────────────────────────
def _position_delete_bitmap(
    io: FileIO,
    data_path: String,
    record_count: Int64,
    deletes: List[DataFile],
    options: ScanOptions,
) raises -> List[Bool]:
    """Which positions of this data file are deleted.

    A deletion vector, when one applies, already contains every delete for the
    file — the spec requires a writer adding one to fold in the position
    delete files it replaces — so the older files are skipped, which is also
    what the planner's association guarantees.
    """
    var n = Int(record_count)
    var out = List[Bool](length=n if n > 0 else 0, fill=False)
    var has_vector = False
    for k in range(len(deletes)):
        if deletes[k].is_deletion_vector():
            has_vector = True
            break

    for k in range(len(deletes)):
        ref d = deletes[k]
        if d.is_deletion_vector():
            var positions = deleted_positions(
                io, d.file_path, d.content_offset, d.content_size_in_bytes
            )
            for j in range(len(positions)):
                var p = Int(positions[j])
                if p >= 0 and p < len(out):
                    out[p] = True
            continue
        if has_vector or not d.is_position_delete():
            continue
        _apply_position_delete_file(io, d, data_path, out, options)
    return out^


def _apply_position_delete_file(
    io: FileIO,
    delete_file: DataFile,
    data_path: String,
    mut out: List[Bool],
    options: ScanOptions,
) raises:
    """Read a position delete file and mark the rows it removes.

    The two columns are addressed by the reserved ids the spec assigns them,
    2147483546 (`file_path`) and 2147483545 (`pos`), never by name.
    """
    var data = io.read_all(delete_file.file_path)
    var reader = ParquetReader[AllCodecs](data^)
    reader.batch_size = options.batch_size
    reader.verify_crc = options.verify_crc
    var names = List[String]()
    var path_idx = reader.schema.field_by_id(Int32(POS_DELETE_FILE_PATH_ID))
    var pos_idx = reader.schema.field_by_id(Int32(POS_DELETE_POS_ID))
    if path_idx < 0 or pos_idx < 0:
        # Some writers omit the field ids; the spec fixes the names too.
        path_idx = reader.schema.field_by_name("file_path")
        pos_idx = reader.schema.field_by_name("pos")
    if path_idx < 0 or pos_idx < 0:
        raise Error(
            "iceberg: position delete file '"
            + delete_file.file_path
            + "' has no file_path/pos columns"
        )
    names.append(reader.schema.fields[path_idx].name)
    names.append(reader.schema.fields[pos_idx].name)
    reader.select_columns(names)
    var want = List[UInt8]()
    want.extend(data_path.as_bytes())
    while reader.has_next():
        var batch = reader.read_batch()
        var paths = batch.column(0).copy()
        var positions = batch.column(1).copy()
        for r in range(batch.num_rows):
            if not paths.is_valid(r):
                continue
            if _cmp_bytes_at(paths, r, want) != 0:
                continue
            var at = Int(int_at(positions, r))
            if at >= 0 and at < len(out):
                out[at] = True


struct _EqualityDeletes(Copyable, Defaultable, Movable):
    """One equality delete file's rows, as canonically encoded keys.

    A delete row matches when every delete column is equal, and `null` equals
    `null` — the spec's "a null value in a delete column matches a row if the
    row's value is null". `kernels.append_key` encodes exactly that, so a
    match is a byte comparison behind a hash bucket.
    """

    var ids: List[Int]
    var keys: List[List[UInt8]]
    var buckets: Dict[UInt64, List[Int]]

    def __init__(out self):
        self.ids = []
        self.keys = []
        self.buckets = Dict[UInt64, List[Int]]()

    def __init__(out self, *, copy: Self):
        self.ids = copy.ids.copy()
        self.keys = copy.keys.copy()
        self.buckets = copy.buckets.copy()

    def __init__(out self, *, deinit move: Self):
        self.ids = move.ids^
        self.keys = move.keys^
        self.buckets = move.buckets^

    def add(mut self, var key: List[UInt8]) raises:
        var h = hash_key(key)
        var at = len(self.keys)
        self.keys.append(key^)
        if h in self.buckets:
            self.buckets[h].append(at)
        else:
            self.buckets[h] = [at]

    def contains(self, key: List[UInt8]) raises -> Bool:
        var h = hash_key(key)
        if h not in self.buckets:
            return False
        ref bucket = self.buckets[h]
        for k in range(len(bucket)):
            if keys_equal(self.keys[bucket[k]], key):
                return True
        return False


def _read_equality_deletes(
    io: FileIO,
    deletes: List[DataFile],
    schema: Schema,
    options: ScanOptions,
) raises -> List[_EqualityDeletes]:
    """One `_EqualityDeletes` per delete file, since each names its own
    `equality_ids`."""
    var out = List[_EqualityDeletes]()
    for k in range(len(deletes)):
        ref d = deletes[k]
        if not d.is_equality_delete():
            continue
        if len(d.equality_ids) == 0:
            raise Error(
                "iceberg: equality delete file '"
                + d.file_path
                + "' declares no equality_ids"
            )
        var eq = _EqualityDeletes()
        eq.ids = d.equality_ids.copy()
        var data = io.read_all(d.file_path)
        var reader = ParquetReader[AllCodecs](data^)
        reader.batch_size = options.batch_size
        reader.verify_crc = options.verify_crc
        var names = List[String]()
        var targets = List[ArrowType]()
        for j in range(len(eq.ids)):
            var idx = reader.schema.field_by_id(Int32(eq.ids[j]))
            if idx < 0:
                raise Error(
                    "iceberg: equality delete file is missing column "
                    + String(eq.ids[j])
                )
            names.append(reader.schema.fields[idx].name)
            var f = schema.find_field(eq.ids[j])
            ref t = schema.store.nodes[f.type]
            if t.kind != TK_PRIMITIVE:
                raise Error(
                    "iceberg: an equality delete cannot match on the nested"
                    " field '"
                    + f.name
                    + "'"
                )
            targets.append(
                arrow_type_for(t.prim, t.precision, t.scale, t.length)
            )
        reader.select_columns(names)
        while reader.has_next():
            var batch = reader.read_batch()
            var cols = List[ArrayData]()
            for c in range(len(eq.ids)):
                cols.append(
                    cast_array(
                        batch.column(c).copy(),
                        targets[c],
                        names[c],
                        eq.ids[c],
                    )
                )
            for r in range(batch.num_rows):
                var key = List[UInt8]()
                for c in range(len(cols)):
                    append_key(key, cols[c], r)
                eq.add(key^)
        out.append(eq^)
    return out^


# ── the data-file reader ────────────────────────────────────────────────────
def _identity_partition_value(
    spec: PartitionSpec, partition: List[Datum], source_id: Int
) -> Datum:
    """The partition value that stands in for a column missing from a file.

    Only an identity transform can supply one: every other transform loses
    information, so it cannot reconstruct the column.
    """
    for k in range(len(spec.fields)):
        if spec.fields[k].transform.kind != T_IDENTITY:
            continue
        if spec.fields[k].source_id != source_id:
            continue
        if k < len(partition):
            return partition[k].copy()
    return Datum.none()


def _partition_json(spec: PartitionSpec, partition: List[Datum]) -> String:
    var out = String("{")
    for k in range(len(spec.fields)):
        if k > 0:
            out += ","
        out += json_quote(spec.fields[k].name) + ":"
        if k < len(partition):
            out += partition[k].to_json()
        else:
            out += "null"
    out += "}"
    return out^


def _column_type(schema: Schema, ids: List[Int]) raises -> ColumnType:
    """The type of the one top-level column `ids` all live under, pruned to
    exactly the fields `ids` selects."""
    var sel = schema.select(ids)
    ref top = sel.store.nodes[sel.root]
    if len(top.fields) != 1:
        raise Error("iceberg: a projection must resolve to one column")
    return ColumnType(sel.store.copy(), top.fields[0].type)


def _column_field(schema: Schema, ids: List[Int]) raises -> NestedField:
    var sel = schema.select(ids)
    ref top = sel.store.nodes[sel.root]
    return top.fields[0].copy()


def _same_ids(a: List[Int], b: List[Int]) -> Bool:
    if len(a) != len(b):
        return False
    for k in range(len(a)):
        if not _contains(b, a[k]):
            return False
    return True


def _take_column_trees(var batch: RecordBatch) raises -> List[ColumnTree]:
    """The batch's columns, moved out of its arena where that is safe.

    parquet.mojo builds a field's children before the field itself, so each
    root closes a contiguous run of arena nodes and the runs come out in
    order — which means every column, nested or not, can be lifted straight
    out instead of copied.
    """
    var n = batch.num_columns()
    var rev = List[ColumnTree]()
    for k in range(n - 1, -1, -1):
        rev.append(take_subtree(batch.arena, batch.roots[k]))
    var out = List[ColumnTree]()
    for _ in range(n):
        out.append(rev.pop())
    return out^


def _filter_column(
    tree: ColumnTree, keep: List[Bool], n_keep: Int
) raises -> ColumnTree:
    var out = ArrayArena()
    var r = filter_tree(tree.arena, tree.root, keep, n_keep, out)
    return ColumnTree(out^, r)


def read_data_file(
    io: FileIO,
    data_file: DataFile,
    delete_files: List[DataFile],
    data_sequence_number: Int64,
    spec: PartitionSpec,
    schema: Schema,
    projected: List[Int],
    meta_columns: List[String],
    mapping: NameMapping,
    residual_dsl: String,
    case_sensitive: Bool,
    options: ScanOptions,
) raises -> List[ScanResult]:
    """One data file's surviving rows, as Arrow batches.

    `projected` is a list of field ids, which may name whole columns or
    fields nested inside them. Ids that share a top-level column come back as
    one column whose type has been pruned to exactly what was asked for, and
    the Parquet read is pruned with it: `["a.b", "c"]` decodes the leaves
    under `a.b` and `c`, and nothing else.
    """
    if data_file.file_format.lower() != "parquet":
        raise Error(
            "iceberg: only Parquet data files can be read; '"
            + data_file.file_path
            + "' is "
            + data_file.file_format
        )

    var residual = bind(parse_filter(residual_dsl), schema, case_sensitive)
    var filter_ids = List[Int]()
    collect_field_ids(residual, residual.root, filter_ids)
    var trivial = (
        residual.root < 0 or residual.nodes[residual.root].op == OP_TRUE
    )

    var equality = _read_equality_deletes(io, delete_files, schema, options)

    # ── which columns, and how much of each ────────────────────────────────
    # Every projected id is resolved to the top-level column it lives in;
    # `out_subs` is what the caller asked for, `read_subs` adds whatever the
    # residual and the equality deletes still need to see.
    var read_ids = List[Int]()
    var out_subs = List[List[Int]]()
    var read_subs = List[List[Int]]()
    for k in range(len(projected)):
        var top = schema.top_ancestor_id(projected[k])
        var at = _index_of(read_ids, top)
        if at < 0:
            read_ids.append(top)
            out_subs.append([projected[k]])
            read_subs.append([projected[k]])
        else:
            if not _contains(out_subs[at], projected[k]):
                out_subs[at].append(projected[k])
                read_subs[at].append(projected[k])
    var n_projected = len(read_ids)

    for k in range(len(filter_ids)):
        var top = schema.top_ancestor_id(filter_ids[k])
        var at = _index_of(read_ids, top)
        if at < 0:
            read_ids.append(top)
            out_subs.append(List[Int]())
            read_subs.append([filter_ids[k]])
        elif not _contains(read_subs[at], filter_ids[k]):
            read_subs[at].append(filter_ids[k])
    for k in range(len(equality)):
        for j in range(len(equality[k].ids)):
            var id = equality[k].ids[j]
            if not schema.is_top_level(id):
                raise Error(
                    "iceberg: an equality delete cannot match on the nested"
                    " field '"
                    + schema.name_of(id)
                    + "'"
                )
            if _index_of(read_ids, id) < 0:
                read_ids.append(id)
                out_subs.append(List[Int]())
                read_subs.append([id])

    var data = _load_bytes(io, data_file, options)
    var reader = ParquetReader[AllCodecs](data^)
    reader.batch_size = options.batch_size
    reader.verify_crc = options.verify_crc

    # ── column projection, in the spec's order ─────────────────────────────
    var plans = List[_ColumnPlan]()
    var file_fields = List[Int]()
    var n_file_columns = 0
    for k in range(len(read_ids)):
        var id = read_ids[k]
        var field = _column_field(schema, read_subs[k])
        var read_ct = _column_type(schema, read_subs[k])
        var out_ct = read_ct.copy()
        var reproject = False
        if k < n_projected and not _same_ids(read_subs[k], out_subs[k]):
            out_ct = _column_type(schema, out_subs[k])
            reproject = True
        ref t = read_ct.store.nodes[read_ct.type]
        var prim = t.prim if t.kind == TK_PRIMITIVE else P_UNKNOWN
        var plan = _ColumnPlan(
            field.name,
            id,
            prim,
            t.precision,
            t.scale,
            t.length,
            SRC_CONSTANT,
            -1,
            Datum.none(),
            field.copy(),
            read_ct^,
            out_ct^,
            reproject,
            not field.required,
        )
        # 1. the column with this id in the file.
        var idx = reader.schema.field_by_id(Int32(id))
        # 2. an identity partition value.
        if idx < 0:
            var pv = _identity_partition_value(spec, data_file.partition, id)
            if pv.valid:
                plan.constant = pv^
                plans.append(plan^)
                continue
        # 3. `schema.name-mapping.default`.
        if idx < 0:
            var mapped = mapping.name_for(id)
            if mapped != "":
                idx = reader.schema.field_by_name(mapped)
        if idx >= 0:
            plan.source = SRC_FILE
            plan.batch_index = n_file_columns
            n_file_columns += 1
            # Only the sub-trees this scan needs are decoded.
            var sel = List[Int]()
            if not _contains(read_subs[k], id):
                for j in range(len(read_subs[k])):
                    var fi = reader.schema.field_by_id(Int32(read_subs[k][j]))
                    if fi >= 0 and not _contains(sel, fi):
                        sel.append(fi)
            if len(sel) == 0:
                sel.append(idx)
            for j in range(len(sel)):
                file_fields.append(sel[j])
            plans.append(plan^)
            continue
        # 4. `initial-default`, then 5. null — both in `nested.default_tree`.
        if plan.field.has_initial_default and not plan.is_nested():
            plan.constant = datum_from_json_prim(
                plan.kind,
                plan.precision,
                plan.scale,
                plan.length,
                parse_json(plan.field.initial_default),
                parse_json(plan.field.initial_default).root,
            )
        plans.append(plan^)

    # Struct leaves the residual reaches: they are children of a column, not
    # columns, so each one records where to find it once a batch is decoded.
    var leaves = List[_LeafPlan]()
    for k in range(len(filter_ids)):
        var id = filter_ids[k]
        if _index_of(read_ids, id) >= 0:
            continue
        var slot = _index_of(read_ids, schema.top_ancestor_id(id))
        if slot < 0:
            continue
        var path = List[Int]()
        if not find_struct_path(
            plans[slot].read.store, plans[slot].read.type, id, path
        ):
            continue
        var af = schema.find_field(id)
        ref tn = schema.store.nodes[af.type]
        leaves.append(
            _LeafPlan(
                id,
                slot,
                path^,
                tn.prim if tn.kind == TK_PRIMITIVE else P_UNKNOWN,
                tn.precision,
                tn.scale,
                tn.length,
            )
        )

    # Metadata columns are appended after the real ones, in the order asked.
    for k in range(len(meta_columns)):
        var name = meta_columns[k]
        var plan = _ColumnPlan(
            name,
            -1,
            P_LONG,
            0,
            0,
            0,
            SRC_CONSTANT,
            -1,
            Datum.none(),
            NestedField.simple(-1, name, False, 0),
            ColumnType(TypeStore(), -1),
            ColumnType(TypeStore(), -1),
            False,
            True,
        )
        if name == META_FILE:
            plan.kind = P_STRING
            plan.constant = Datum.string_(data_file.file_path)
        elif name == META_POS:
            plan.source = SRC_POS
        elif name == META_SPEC_ID:
            plan.kind = P_INT
            plan.constant = Datum.int_(Int64(spec.spec_id))
        elif name == META_PARTITION:
            plan.kind = P_STRING
            plan.constant = Datum.string_(
                _partition_json(spec, data_file.partition)
            )
        elif name == META_ROW_ID:
            # v3 row lineage: `_row_id` is the file's `first_row_id` plus the
            # row's position, and null when the file has no `first_row_id`.
            if data_file.has_first_row_id:
                plan.source = SRC_ROW_ID
                plan.constant = Datum.long_(data_file.first_row_id)
        elif name == META_LAST_UPDATED:
            plan.constant = Datum.long_(data_sequence_number)
        else:
            raise Error("iceberg: unknown metadata column '" + name + "'")
        plans.append(plan^)

    reader.select_fields(file_fields)

    # Which columns need a materialised array: everything projected, plus the
    # key columns of any equality delete and the owner of any struct leaf.
    var need_array = List[Bool](length=len(read_ids), fill=False)
    for k in range(n_projected):
        need_array[k] = True
    for k in range(len(equality)):
        for j in range(len(equality[k].ids)):
            var at = _index_of(read_ids, equality[k].ids[j])
            if at >= 0:
                need_array[at] = True
    for k in range(len(leaves)):
        need_array[leaves[k].slot] = True

    # ── deletes and pruning ────────────────────────────────────────────────
    var deleted = _position_delete_bitmap(
        io, data_file.file_path, data_file.record_count, delete_files, options
    )
    var has_position_deletes = False
    for k in range(len(deleted)):
        if deleted[k]:
            has_position_deletes = True
            break
    var need_positions = has_position_deletes or _wants_positions(meta_columns)

    var groups = List[Int]()
    for g in range(reader.num_row_groups()):
        groups.append(g)
    if options.prune and len(file_fields) > 0:
        var preds = _predicates_for(residual, residual.root, reader, schema)
        if len(preds) > 0:
            _ = reader.prune_row_groups(preds)
            groups = reader._row_groups.copy()

    var starts = List[Int64]()
    var at_row: Int64 = 0
    for g in range(reader.num_row_groups()):
        starts.append(at_row)
        at_row += reader.meta.row_groups[g].num_rows

    # ── read ───────────────────────────────────────────────────────────────
    var out = List[ScanResult]()
    var kept_total = 0
    for gi in range(len(groups)):
        var g = groups[gi]
        reader.select_row_groups([g])
        if options.prune and not need_positions and len(file_fields) > 0:
            var preds = _predicates_for(residual, residual.root, reader, schema)
            if len(preds) > 0:
                _ = reader.prune_pages(preds)
        var pos = starts[g]
        while reader.has_next():
            var batch = reader.read_batch()
            var n = batch.num_rows
            var base = pos
            pos += Int64(n)
            var decoded = _take_column_trees(batch^)
            # Consumed in `batch_index` order, which is the order the
            # `SRC_FILE` plans were built in, so each decoded buffer is moved
            # into its cast exactly once and never copied.
            var pending = List[ColumnTree]()
            for _ in range(len(decoded)):
                pending.append(decoded.pop())

            # Cast every column that was read to the table's current type.
            var arrays = List[ColumnTree]()
            for c in range(len(read_ids)):
                ref p = plans[c]
                if p.source == SRC_FILE:
                    arrays.append(
                        cast_column(
                            pending.pop(),
                            p.read.store,
                            p.read.type,
                            p.name,
                            p.field_id,
                            p.nullable,
                        )
                    )
                elif need_array[c]:
                    if p.is_nested():
                        var arena = ArrayArena()
                        var r = default_tree(arena, p.read.store, p.field, n)
                        arrays.append(ColumnTree(arena^, r))
                    else:
                        arrays.append(
                            ColumnTree(
                                constant_array(
                                    p.name,
                                    p.field_id,
                                    p.kind,
                                    p.precision,
                                    p.scale,
                                    p.length,
                                    p.constant,
                                    n,
                                )
                            )
                        )
                else:
                    var arena = ArrayArena()
                    var r = empty_tree(
                        arena,
                        p.read.store,
                        p.read.type,
                        p.name,
                        p.field_id,
                        p.nullable,
                    )
                    arrays.append(ColumnTree(arena^, r))

            # ── selection ──────────────────────────────────────────────────
            var leaf_arrays = List[ArrayData]()
            if not trivial:
                for k in range(len(leaves)):
                    ref lp = leaves[k]
                    leaf_arrays.append(
                        flatten_leaf(
                            arrays[lp.slot].arena,
                            arrays[lp.slot].root,
                            lp.path,
                        )
                    )
            var keep = List[Bool](length=n, fill=True)
            var all_kept = True
            if has_position_deletes:
                for r in range(n):
                    var p_at = Int(base) + r
                    if p_at < len(deleted) and deleted[p_at]:
                        keep[r] = False
                        all_kept = False
            if not trivial:
                var sel = _selection(
                    residual,
                    residual.root,
                    arrays,
                    plans,
                    read_ids,
                    leaves,
                    leaf_arrays,
                    n,
                )
                for r in range(n):
                    if keep[r] and not sel[r]:
                        keep[r] = False
                        all_kept = False
            for e in range(len(equality)):
                ref eq = equality[e]
                var slots = List[Int]()
                for j in range(len(eq.ids)):
                    slots.append(_index_of(read_ids, eq.ids[j]))
                for r in range(n):
                    if not keep[r]:
                        continue
                    var key = List[UInt8]()
                    for j in range(len(slots)):
                        ref a = arrays[slots[j]].arena.nodes[
                            arrays[slots[j]].root
                        ]
                        append_key(key, a, r)
                    if eq.contains(key):
                        keep[r] = False
                        all_kept = False

            var n_keep = 0
            for r in range(n):
                if keep[r]:
                    n_keep += 1
            if options.limit >= 0 and kept_total + n_keep > options.limit:
                var room = options.limit - kept_total
                var seen = 0
                for r in range(n):
                    if not keep[r]:
                        continue
                    if seen >= room:
                        keep[r] = False
                        all_kept = False
                    seen += 1
                n_keep = room
            if n_keep == 0:
                if options.limit >= 0 and kept_total >= options.limit:
                    return out^
                continue

            # ── the surviving rows ─────────────────────────────────────────
            var rest = List[ColumnTree]()
            for _ in range(len(arrays)):
                rest.append(arrays.pop())
            var cols = List[ScanColumn]()
            for c in range(n_projected):
                ref p = plans[c]
                var raw = rest.pop()
                var t = raw^ if all_kept else _filter_column(raw, keep, n_keep)
                if p.reproject:
                    t = cast_column(
                        t^,
                        p.out.store,
                        p.out.type,
                        p.name,
                        p.field_id,
                        p.nullable,
                    )
                cols.append(
                    ScanColumn(
                        p.name,
                        p.field_id,
                        p.kind,
                        p.precision,
                        p.scale,
                        p.length,
                        p.out.copy(),
                        t^,
                    )
                )
            if len(meta_columns) > 0:
                var positions = List[Int64]()
                if need_positions:
                    for r in range(n):
                        if keep[r]:
                            positions.append(base + Int64(r))
                for c in range(len(read_ids), len(plans)):
                    ref p = plans[c]
                    var a: ArrayData
                    if p.source == SRC_POS:
                        a = int64_array(p.name, positions)
                    elif p.source == SRC_ROW_ID:
                        var ids = List[Int64](capacity=len(positions))
                        for j in range(len(positions)):
                            ids.append(p.constant.i + positions[j])
                        a = int64_array(p.name, ids)
                    else:
                        a = constant_array(
                            p.name,
                            p.field_id,
                            p.kind,
                            p.precision,
                            p.scale,
                            p.length,
                            p.constant,
                            n_keep,
                        )
                    cols.append(
                        ScanColumn(
                            p.name,
                            p.field_id,
                            p.kind,
                            p.precision,
                            p.scale,
                            p.length,
                            a^,
                        )
                    )
            out.append(ScanResult(cols^))
            kept_total += n_keep
            if options.limit >= 0 and kept_total >= options.limit:
                return out^
    return out^


def read_data_file_table(
    io: FileIO,
    data_file: DataFile,
    delete_files: List[DataFile],
    data_sequence_number: Int64,
    spec: PartitionSpec,
    schema: Schema,
    projected: List[Int],
    meta_columns: List[String],
    mapping: NameMapping,
    residual_dsl: String,
    case_sensitive: Bool,
    options: ScanOptions,
) raises -> ScanResult:
    """`read_data_file`, with the batches concatenated into one result."""
    var parts = read_data_file(
        io,
        data_file,
        delete_files,
        data_sequence_number,
        spec,
        schema,
        projected,
        meta_columns,
        mapping,
        residual_dsl,
        case_sensitive,
        options,
    )
    var out = ScanResult()
    for k in range(len(parts)):
        out.append(parts[k])
    if len(out.columns) == 0:
        out = empty_scan_result(schema, projected, meta_columns)
    return out^


def empty_scan_result(
    schema: Schema, ids: List[Int], meta_columns: List[String]
) raises -> ScanResult:
    """A result with the right columns and no rows."""
    var cols = List[ScanColumn]()
    var tops = List[Int]()
    var subs = List[List[Int]]()
    for k in range(len(ids)):
        var top = schema.top_ancestor_id(ids[k])
        var at = _index_of(tops, top)
        if at < 0:
            tops.append(top)
            subs.append([ids[k]])
        elif not _contains(subs[at], ids[k]):
            subs[at].append(ids[k])
    for k in range(len(tops)):
        var ct = _column_type(schema, subs[k])
        var field = _column_field(schema, subs[k])
        ref t = ct.store.nodes[ct.type]
        var arena = ArrayArena()
        var r = empty_tree(
            arena, ct.store, ct.type, field.name, tops[k], not field.required
        )
        cols.append(
            ScanColumn(
                field.name,
                tops[k],
                t.prim if t.kind == TK_PRIMITIVE else P_UNKNOWN,
                t.precision,
                t.scale,
                t.length,
                ct^,
                ColumnTree(arena^, r),
            )
        )
    for k in range(len(meta_columns)):
        var name = meta_columns[k]
        var kind = P_LONG
        if name == META_FILE or name == META_PARTITION:
            kind = P_STRING
        elif name == META_SPEC_ID:
            kind = P_INT
        cols.append(ScanColumn.empty(name, -1, kind, 0, 0, 0))
    return ScanResult(cols^)


def _wants_positions(meta_columns: List[String]) -> Bool:
    for k in range(len(meta_columns)):
        if meta_columns[k] == META_POS or meta_columns[k] == META_ROW_ID:
            return True
    return False


def _contains(l: List[Int], v: Int) -> Bool:
    for k in range(len(l)):
        if l[k] == v:
            return True
    return False


def _index_of(l: List[Int], v: Int) -> Int:
    for k in range(len(l)):
        if l[k] == v:
            return k
    return -1


def _nested_field(schema: Schema, id: Int) raises -> NestedField:
    var cols = schema.columns()
    for k in range(len(cols)):
        if cols[k].id == id:
            return cols[k].copy()
    return NestedField.simple(id, String(""), False, 0)


# ── loading the bytes ───────────────────────────────────────────────────────
def _load_bytes(
    io: FileIO, data_file: DataFile, options: ScanOptions
) raises -> List[UInt8]:
    """The file's bytes, or just the parts of them a scan will touch.

    `lazy` fetches the footer and then only the byte ranges of the row groups
    that survive statistics pruning, into a buffer the size of the file with
    everything else left zero. Parquet addresses everything by absolute file
    offset, so a sparse buffer decodes exactly like the whole file — and over
    a network this is the difference between one range request per row group
    and downloading the object.
    """
    if not options.lazy:
        return io.read_all(data_file.file_path)

    var size = Int(data_file.file_size_in_bytes)
    if size <= 0:
        size = io.length(data_file.file_path)
    if size < 12:
        return io.read_all(data_file.file_path)
    var tail = io.read_range(data_file.file_path, size - 8, 8)
    if len(tail) != 8 or tail[4] != 0x50 or tail[7] != 0x31:
        # Not a footer we recognise; fall back rather than guess.
        return io.read_all(data_file.file_path)
    var footer_len = (
        Int(tail[0])
        | (Int(tail[1]) << 8)
        | (Int(tail[2]) << 16)
        | (Int(tail[3]) << 24)
    )
    var footer_start = size - 8 - footer_len
    if footer_start < 4 or footer_len <= 0:
        return io.read_all(data_file.file_path)

    var buf = List[UInt8](length=size, fill=0)
    buf[0] = 0x50
    buf[1] = 0x41
    buf[2] = 0x52
    buf[3] = 0x31
    var footer = io.read_range(
        data_file.file_path, footer_start, footer_len + 8
    )
    for k in range(len(footer)):
        buf[footer_start + k] = footer[k]

    # A footer-only reader is enough to say where each row group lives.
    var probe = ParquetReader[AllCodecs](buf.copy())
    for g in range(probe.num_row_groups()):
        var extent = _row_group_extent(probe, g)
        var start = extent[0]
        var length = extent[1]
        if length <= 0 or start < 0 or start + length > size:
            continue
        var chunk = io.read_range(data_file.file_path, start, length)
        for k in range(len(chunk)):
            buf[start + k] = chunk[k]
    return buf^


def _row_group_extent(
    reader: ParquetReader[AllCodecs], g: Int
) raises -> Tuple[Int, Int]:
    """`(start, length)` of one row group's column data.

    A row group's first byte is the first page of its first column chunk —
    the dictionary page when there is one — and `total_compressed_size` on the
    column chunks covers the rest.
    """
    ref rg = reader.meta.row_groups[g]
    var start = -1
    var end = -1
    for c in range(len(rg.columns)):
        ref chunk = rg.columns[c]
        if not chunk.meta_data:
            continue
        ref md = chunk.meta_data.value()
        var at = Int(md.data_page_offset)
        if md.dictionary_page_offset:
            var dp = Int(md.dictionary_page_offset.value())
            if dp > 0 and dp < at:
                at = dp
        var stop = at + Int(md.total_compressed_size)
        if start < 0 or at < start:
            start = at
        if stop > end:
            end = stop
    if start < 0:
        return (0, 0)
    return (start, end - start)


# ── statistics predicates ───────────────────────────────────────────────────
def _predicates_for(
    e: Expr, i: Int, reader: ParquetReader[AllCodecs], schema: Schema
) raises -> List[Predicate]:
    """The conjunction of simple comparisons the Parquet reader can prune on.

    Only the top-level `AND` chain contributes: a disjunction proves nothing
    about a page. `!=` and `not-in` are excluded for the same reason the
    metrics evaluator excludes them — a bound is not a value that occurs.
    """
    var out = List[Predicate]()
    _collect_predicates(e, i, reader, schema, out)
    return out^


def _collect_predicates(
    e: Expr,
    i: Int,
    reader: ParquetReader[AllCodecs],
    schema: Schema,
    mut out: List[Predicate],
) raises:
    if i < 0:
        return
    ref n = e.nodes[i]
    if n.op == OP_AND:
        _collect_predicates(e, n.left, reader, schema, out)
        _collect_predicates(e, n.right, reader, schema, out)
        return
    var op: Int
    if n.op == OP_EQ:
        op = OP_EQ_PQ
    elif n.op == OP_LT:
        op = OP_LT_PQ
    elif n.op == OP_LT_EQ:
        op = OP_LE_PQ
    elif n.op == OP_GT:
        op = OP_GT_PQ
    elif n.op == OP_GT_EQ:
        op = OP_GE_PQ
    else:
        return
    if n.field_id < 0 or len(n.lits) == 0:
        return
    var idx = reader.schema.field_by_id(Int32(n.field_id))
    if idx < 0:
        return
    # By the leaf's *dotted path*, so a struct field prunes on its own
    # statistics and never on a top-level column that happens to share its
    # name.
    var lf = reader.schema.fields[idx].leaf
    if lf < 0:
        return
    var v = _scalar_of(n.lits[0])
    if v.kind == SV_NONE:
        return
    out.append(Predicate(reader.schema.leaves[lf].dotted(), op, v^))


def _scalar_of(d: Datum) raises -> ScalarValue:
    if not d.valid:
        return ScalarValue()
    if d.kind == P_BOOLEAN:
        return ScalarValue.of_bool(d.i != 0)
    if d.kind == P_FLOAT or d.kind == P_DOUBLE:
        return ScalarValue.of_float(d.f)
    if d.kind == P_STRING:
        return ScalarValue.of_bytes(d.s.as_bytes())
    if d.kind == P_BINARY or d.kind == P_FIXED or d.kind == P_UUID:
        return ScalarValue.of_bytes(Span(d.b))
    if d.kind == P_DECIMAL:
        return ScalarValue()
    if (
        d.kind == P_INT
        or d.kind == P_LONG
        or d.kind == P_DATE
        or d.kind == P_TIME
        or d.kind == P_TIMESTAMP
        or d.kind == P_TIMESTAMPTZ
        or d.kind == P_TIMESTAMP_NS
        or d.kind == P_TIMESTAMPTZ_NS
    ):
        return ScalarValue.of_int(d.i)
    return ScalarValue()
