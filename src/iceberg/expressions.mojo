"""Row filters: unbound and bound predicates, binding, projection, evaluators.

An expression tree lives in an arena (`Expr.nodes`) for the same reason types
do — Mojo values are not recursive. `Expr.root` is the top node.

The pieces, in the order a scan uses them:

1. `parse_filter` reads the JSON S-expression DSL (the same one
   iceberg-rs.mojo's Rust bridge speaks, so oracle plans are directly
   comparable) into an *unbound* tree of column names and untyped literals.
2. `bind` resolves names to field ids and converts each literal to the
   column's type, so `["=", "id", 3]` on a `long` column produces a long datum
   rather than an int one — a mistyped datum silently matches nothing.
3. `rewrite_not` pushes negation down to the leaves.
4. `project_inclusive` / `project_strict` push a bound predicate through a
   partition transform. Inclusive projection is the classic one: it may keep a
   file that has no matching row, but never drops one that has.
5. `ManifestEvaluator` applies the projected predicate to a manifest's
   partition field summaries; `InclusiveMetricsEvaluator` applies the original
   predicate to a data file's per-column bounds and counts.
"""

from std.collections import Dict

from .json import Json, parse_json, json_quote, substr
from .schema import Schema, AccessorField
from .transforms import (
    PartitionSpec,
    PartitionField,
    Transform,
    T_IDENTITY,
    T_BUCKET,
    T_TRUNCATE,
    T_YEAR,
    T_MONTH,
    T_DAY,
    T_HOUR,
    T_VOID,
    T_UNKNOWN,
    truncate_codepoints,
)
from .types import (
    TypeStore,
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
    primitive_name,
    is_integer_like,
)
from .values import (
    Datum,
    compare,
    decimal_from_text,
    hex_bytes,
    uuid_bytes,
    parse_iso,
    datum_from_bytes_prim,
    datum_from_json_prim,
)


# ── operations ──────────────────────────────────────────────────────────────
comptime OP_TRUE: UInt8 = 0
comptime OP_FALSE: UInt8 = 1
comptime OP_AND: UInt8 = 2
comptime OP_OR: UInt8 = 3
comptime OP_NOT: UInt8 = 4
comptime OP_IS_NULL: UInt8 = 5
comptime OP_NOT_NULL: UInt8 = 6
comptime OP_IS_NAN: UInt8 = 7
comptime OP_NOT_NAN: UInt8 = 8
comptime OP_LT: UInt8 = 9
comptime OP_LT_EQ: UInt8 = 10
comptime OP_GT: UInt8 = 11
comptime OP_GT_EQ: UInt8 = 12
comptime OP_EQ: UInt8 = 13
comptime OP_NOT_EQ: UInt8 = 14
comptime OP_IN: UInt8 = 15
comptime OP_NOT_IN: UInt8 = 16
comptime OP_STARTS_WITH: UInt8 = 17
comptime OP_NOT_STARTS_WITH: UInt8 = 18


def op_name(op: UInt8) -> String:
    if op == OP_TRUE:
        return "true"
    if op == OP_FALSE:
        return "false"
    if op == OP_AND:
        return "and"
    if op == OP_OR:
        return "or"
    if op == OP_NOT:
        return "not"
    if op == OP_IS_NULL:
        return "is-null"
    if op == OP_NOT_NULL:
        return "not-null"
    if op == OP_IS_NAN:
        return "is-nan"
    if op == OP_NOT_NAN:
        return "not-nan"
    if op == OP_LT:
        return "<"
    if op == OP_LT_EQ:
        return "<="
    if op == OP_GT:
        return ">"
    if op == OP_GT_EQ:
        return ">="
    if op == OP_EQ:
        return "="
    if op == OP_NOT_EQ:
        return "!="
    if op == OP_IN:
        return "in"
    if op == OP_NOT_IN:
        return "not-in"
    if op == OP_STARTS_WITH:
        return "starts-with"
    return "not-starts-with"


def negate_op(op: UInt8) raises -> UInt8:
    if op == OP_TRUE:
        return OP_FALSE
    if op == OP_FALSE:
        return OP_TRUE
    if op == OP_IS_NULL:
        return OP_NOT_NULL
    if op == OP_NOT_NULL:
        return OP_IS_NULL
    if op == OP_IS_NAN:
        return OP_NOT_NAN
    if op == OP_NOT_NAN:
        return OP_IS_NAN
    if op == OP_LT:
        return OP_GT_EQ
    if op == OP_LT_EQ:
        return OP_GT
    if op == OP_GT:
        return OP_LT_EQ
    if op == OP_GT_EQ:
        return OP_LT
    if op == OP_EQ:
        return OP_NOT_EQ
    if op == OP_NOT_EQ:
        return OP_EQ
    if op == OP_IN:
        return OP_NOT_IN
    if op == OP_NOT_IN:
        return OP_IN
    if op == OP_STARTS_WITH:
        return OP_NOT_STARTS_WITH
    if op == OP_NOT_STARTS_WITH:
        return OP_STARTS_WITH
    raise Error("iceberg: cannot negate " + op_name(op))


def is_predicate(op: UInt8) -> Bool:
    return op >= OP_IS_NULL


@fieldwise_init
struct ExprNode(Copyable, Movable):
    """One node: a boolean connective, a constant, or a predicate on a term."""

    var op: UInt8
    var left: Int
    var right: Int
    var name: String
    """Unbound term: the column name."""
    var field_id: Int
    """Bound term: the field id. -1 while unbound."""
    var prim: UInt8
    """Bound term: the column's primitive kind."""
    var lits: List[Datum]
    """Literals; `in`/`not-in` may hold many, comparisons exactly one."""
    var raw_lits: List[String]
    """Unbound literals as raw JSON text, typed at bind time."""


struct Expr(Copyable, Movable):
    """An expression tree in an arena."""

    var nodes: List[ExprNode]
    var root: Int

    def __init__(out self):
        self.nodes = []
        self.root = -1

    def add(mut self, var n: ExprNode) -> Int:
        self.nodes.append(n^)
        return len(self.nodes) - 1

    def constant(mut self, v: Bool) -> Int:
        return self.add(
            ExprNode(OP_TRUE if v else OP_FALSE, -1, -1, "", -1, 0, [], [])
        )

    def connective(mut self, op: UInt8, a: Int, b: Int) -> Int:
        return self.add(ExprNode(op, a, b, "", -1, 0, [], []))

    def unbound(
        mut self, op: UInt8, var name: String, var raw: List[String]
    ) -> Int:
        return self.add(ExprNode(op, -1, -1, name^, -1, 0, [], raw^))

    def bound(
        mut self, op: UInt8, field_id: Int, prim: UInt8, var lits: List[Datum]
    ) -> Int:
        return self.add(ExprNode(op, -1, -1, "", field_id, prim, lits^, []))

    def is_true(self, i: Int) -> Bool:
        return i >= 0 and self.nodes[i].op == OP_TRUE

    def is_false(self, i: Int) -> Bool:
        return i >= 0 and self.nodes[i].op == OP_FALSE

    def text(self, i: Int) -> String:
        """Round-trippable DSL text — what a scan reports as its residual."""
        if i < 0:
            return '["true"]'
        ref n = self.nodes[i]
        if n.op == OP_TRUE:
            return '["true"]'
        if n.op == OP_FALSE:
            return '["false"]'
        if n.op == OP_NOT:
            return '["not",' + self.text(n.left) + "]"
        if n.op == OP_AND or n.op == OP_OR:
            return (
                "["
                + json_quote(op_name(n.op))
                + ","
                + self.text(n.left)
                + ","
                + self.text(n.right)
                + "]"
            )
        var term = json_quote(n.name) if n.field_id < 0 else String(n.field_id)
        var out = "[" + json_quote(op_name(n.op)) + "," + term
        if n.op == OP_IN or n.op == OP_NOT_IN:
            out += ",["
            for k in range(len(n.lits)):
                if k > 0:
                    out += ","
                out += n.lits[k].to_json()
            out += "]"
        elif len(n.lits) > 0:
            out += "," + n.lits[0].to_json()
        elif len(n.raw_lits) > 0:
            out += "," + n.raw_lits[0]
        out += "]"
        return out^


# ── the JSON S-expression DSL ───────────────────────────────────────────────
def parse_filter(text: String) raises -> Expr:
    """Parse the filter DSL into an unbound expression.

    The grammar is iceberg-rs.mojo's, so the same filter string can be handed
    to the Rust bridge and to this reader and the plans compared directly.
    """
    var doc = parse_json(text)
    var e = Expr()
    e.root = _parse_node(doc, doc.root, e)
    return e^


def _parse_node(doc: Json, i: Int, mut e: Expr) raises -> Int:
    if doc.kind(i) != 5:  # JSON_ARRAY
        raise Error("iceberg: a filter must be a JSON array")
    var n = doc.size(i)
    if n == 0:
        raise Error("iceberg: empty filter expression")
    var op = doc.as_string(doc.at(i, 0))
    if op == "true":
        return e.constant(True)
    if op == "false":
        return e.constant(False)
    if op == "not":
        var inner = _parse_node(doc, doc.at(i, 1), e)
        return e.connective(OP_NOT, inner, -1)
    if op == "and" or op == "or":
        var kind = OP_AND if op == "and" else OP_OR
        if n < 2:
            raise Error("iceberg: '" + op + "' needs at least one operand")
        var acc = _parse_node(doc, doc.at(i, 1), e)
        for k in range(2, n):
            var rhs = _parse_node(doc, doc.at(i, k), e)
            acc = e.connective(kind, acc, rhs)
        return acc
    var pop = _predicate_op(op)
    var name = doc.as_string(doc.at(i, 1))
    var raw = List[String]()
    if pop == OP_IN or pop == OP_NOT_IN:
        var arr = doc.at(i, 2)
        if doc.kind(arr) != 5:
            raise Error("iceberg: '" + op + "' needs a list of literals")
        for k in range(doc.size(arr)):
            raw.append(doc.dump(doc.at(arr, k)))
    elif n > 2:
        raw.append(doc.dump(doc.at(i, 2)))
    return e.unbound(pop, name^, raw^)


def _predicate_op(s: String) raises -> UInt8:
    if s == "=" or s == "==" or s == "eq":
        return OP_EQ
    if s == "!=" or s == "<>" or s == "ne":
        return OP_NOT_EQ
    if s == "<" or s == "lt":
        return OP_LT
    if s == "<=" or s == "lteq":
        return OP_LT_EQ
    if s == ">" or s == "gt":
        return OP_GT
    if s == ">=" or s == "gteq":
        return OP_GT_EQ
    if s == "is-null" or s == "isnull":
        return OP_IS_NULL
    if s == "not-null" or s == "notnull":
        return OP_NOT_NULL
    if s == "is-nan" or s == "isnan":
        return OP_IS_NAN
    if s == "not-nan" or s == "notnan":
        return OP_NOT_NAN
    if s == "in":
        return OP_IN
    if s == "not-in" or s == "notin":
        return OP_NOT_IN
    if s == "starts-with" or s == "startswith":
        return OP_STARTS_WITH
    if s == "not-starts-with" or s == "notstartswith":
        return OP_NOT_STARTS_WITH
    raise Error("iceberg: unknown filter operator '" + s + "'")


# ── literal typing ──────────────────────────────────────────────────────────
def literal_for(
    store: TypeStore, type_idx: Int, raw_json: String
) raises -> Datum:
    """Convert one raw JSON literal to the column's type.

    Widening conversions the spec allows are done here: an int literal on a
    `long` column becomes a long, a string on a `date`/`timestamp` column is
    parsed as ISO-8601, and a number on those columns is taken as the stored
    integer.
    """
    ref n = store.nodes[type_idx]
    if n.kind != TK_PRIMITIVE:
        raise Error("iceberg: cannot compare against a nested column")
    var doc = parse_json(raw_json)
    return datum_from_json_prim(
        n.prim, n.precision, n.scale, n.length, doc, doc.root
    )


def bind(
    var e: Expr, schema: Schema, case_sensitive: Bool = True
) raises -> Expr:
    """Resolve names to field ids and type every literal against the schema.

    A predicate on a column the schema does not have becomes `false` (a row can
    never match a column that is not there) — except `is-null`, which becomes
    `true`, since a missing column reads as all nulls.
    """
    var out = Expr()
    out.root = _bind_node(e, e.root, schema, out, case_sensitive)
    return out^


def _bind_node(
    e: Expr, i: Int, schema: Schema, mut out: Expr, cs: Bool
) raises -> Int:
    ref n = e.nodes[i]
    if n.op == OP_TRUE or n.op == OP_FALSE:
        return out.constant(n.op == OP_TRUE)
    if n.op == OP_NOT:
        var inner = _bind_node(e, n.left, schema, out, cs)
        return out.connective(OP_NOT, inner, -1)
    if n.op == OP_AND or n.op == OP_OR:
        var a = _bind_node(e, n.left, schema, out, cs)
        var b = _bind_node(e, n.right, schema, out, cs)
        # Fold the constants that binding a missing column produces.
        if n.op == OP_AND:
            if out.is_false(a) or out.is_false(b):
                return out.constant(False)
            if out.is_true(a):
                return b
            if out.is_true(b):
                return a
        else:
            if out.is_true(a) or out.is_true(b):
                return out.constant(True)
            if out.is_false(a):
                return b
            if out.is_false(b):
                return a
        return out.connective(n.op, a, b)
    var k = schema.find_name_index(n.name)
    if k < 0:
        return out.constant(n.op == OP_IS_NULL or n.op == OP_NOT_IN)
    var af = schema.flat[k].copy()
    ref tn = schema.store.nodes[af.type]
    if tn.kind != TK_PRIMITIVE:
        raise Error("iceberg: cannot filter on nested column '" + n.name + "'")
    # A required column can never be null, and a non-floating column never NaN.
    if n.op == OP_IS_NULL and af.required:
        return out.constant(False)
    if n.op == OP_NOT_NULL and af.required:
        return out.constant(True)
    var floating = tn.prim == P_FLOAT or tn.prim == P_DOUBLE
    if n.op == OP_IS_NAN and not floating:
        return out.constant(False)
    if n.op == OP_NOT_NAN and not floating:
        return out.constant(True)
    if (n.op == OP_STARTS_WITH or n.op == OP_NOT_STARTS_WITH) and (
        tn.prim != P_STRING and tn.prim != P_BINARY and tn.prim != P_FIXED
    ):
        raise Error(
            "iceberg: starts-with needs a string column, got "
            + primitive_name(tn.prim)
        )
    var lits = List[Datum]()
    for j in range(len(n.raw_lits)):
        lits.append(literal_for(schema.store, af.type, n.raw_lits[j]))
    if n.op == OP_IN and len(lits) == 0:
        return out.constant(False)
    if n.op == OP_NOT_IN and len(lits) == 0:
        return out.constant(True)
    if n.op == OP_IN and len(lits) == 1:
        return out.bound(OP_EQ, af.id, tn.prim, lits^)
    if n.op == OP_NOT_IN and len(lits) == 1:
        return out.bound(OP_NOT_EQ, af.id, tn.prim, lits^)
    return out.bound(n.op, af.id, tn.prim, lits^)


def rewrite_not(e: Expr) raises -> Expr:
    """Push every `not` down to the leaves (De Morgan plus op negation)."""
    var out = Expr()
    out.root = _rewrite(e, e.root, False, out)
    return out^


def _rewrite(e: Expr, i: Int, negated: Bool, mut out: Expr) raises -> Int:
    ref n = e.nodes[i]
    if n.op == OP_NOT:
        return _rewrite(e, n.left, not negated, out)
    if n.op == OP_TRUE or n.op == OP_FALSE:
        var v = n.op == OP_TRUE
        return out.constant(not v if negated else v)
    if n.op == OP_AND or n.op == OP_OR:
        var a = _rewrite(e, n.left, negated, out)
        var b = _rewrite(e, n.right, negated, out)
        var op = n.op
        if negated:
            op = OP_OR if n.op == OP_AND else OP_AND
        return out.connective(op, a, b)
    var op2 = negate_op(n.op) if negated else n.op
    if n.field_id >= 0:
        return out.bound(op2, n.field_id, n.prim, n.lits.copy())
    return out.unbound(op2, n.name, n.raw_lits.copy())


# ── projection through partition transforms ─────────────────────────────────
def project_inclusive(
    e: Expr, spec: PartitionSpec, schema: Schema
) raises -> Expr:
    """Project a bound row filter onto the partition tuple, inclusively.

    The result is true for every partition that *could* hold a matching row. It
    is deliberately loose: dropping a partition that might match would lose
    data, keeping one that cannot only costs a wasted read.
    """
    var out = Expr()
    out.root = _project(e, e.root, spec, schema, out, True)
    return out^


def project_strict(e: Expr, spec: PartitionSpec, schema: Schema) raises -> Expr:
    """Project strictly: true only for partitions where *every* row matches.

    A scan uses this to drop the filter from a partition's residual — if the
    whole partition matches, there is nothing left to check per row.
    """
    var out = Expr()
    out.root = _project(e, e.root, spec, schema, out, False)
    return out^


def _project(
    e: Expr,
    i: Int,
    spec: PartitionSpec,
    schema: Schema,
    mut out: Expr,
    inclusive: Bool,
) raises -> Int:
    ref n = e.nodes[i]
    if n.op == OP_TRUE or n.op == OP_FALSE:
        return out.constant(n.op == OP_TRUE)
    if n.op == OP_NOT:
        raise Error("iceberg: rewrite_not before projecting")
    if n.op == OP_AND or n.op == OP_OR:
        var a = _project(e, n.left, spec, schema, out, inclusive)
        var b = _project(e, n.right, spec, schema, out, inclusive)
        if n.op == OP_AND:
            if out.is_false(a) or out.is_false(b):
                return out.constant(False)
            if out.is_true(a):
                return b
            if out.is_true(b):
                return a
        else:
            if out.is_true(a) or out.is_true(b):
                return out.constant(True)
            if out.is_false(a):
                return b
            if out.is_false(b):
                return a
        return out.connective(n.op, a, b)
    # A predicate projects through every partition field on its source column;
    # each one that yields something narrows the result.
    var idxs = spec.fields_for_source(n.field_id)
    var acc = -1
    for k in range(len(idxs)):
        ref pf = spec.fields[idxs[k]]
        var projected = _project_one(e, i, pf, out, inclusive)
        if projected < 0:
            continue
        if acc < 0:
            acc = projected
        else:
            acc = out.connective(OP_AND if inclusive else OP_OR, acc, projected)
    if acc < 0:
        # Nothing could be said about the partition tuple: inclusive filtering
        # must keep everything, strict filtering must claim nothing.
        return out.constant(inclusive)
    return acc


def _project_one(
    e: Expr, i: Int, pf: PartitionField, mut out: Expr, inclusive: Bool
) raises -> Int:
    """Project one predicate through one partition field. -1 = "no information".
    """
    ref n = e.nodes[i]
    ref t = pf.transform
    if not t.can_project() or t.kind == T_VOID:
        return -1
    # Nullness passes straight through: every transform maps null to null.
    if n.op == OP_IS_NULL:
        return out.bound(OP_IS_NULL, pf.field_id, 0, [])
    if n.op == OP_NOT_NULL:
        return out.bound(OP_NOT_NULL, pf.field_id, 0, [])
    if n.op == OP_IS_NAN or n.op == OP_NOT_NAN:
        # NaN-ness is not preserved by any transform we can reason about.
        return -1
    if t.kind == T_IDENTITY:
        var lits = List[Datum]()
        for k in range(len(n.lits)):
            lits.append(n.lits[k].copy())
        return out.bound(n.op, pf.field_id, n.prim, lits^)

    if t.kind == T_BUCKET:
        # Bucketing destroys order, so only equality survives — and only
        # inclusively: two values can share a bucket.
        if not inclusive:
            return -1
        if n.op == OP_EQ:
            return out.bound(OP_EQ, pf.field_id, P_INT, [t.apply(n.lits[0])])
        if n.op == OP_IN:
            var buckets = List[Datum]()
            for k in range(len(n.lits)):
                var b = t.apply(n.lits[k])
                if not _contains_datum(buckets, b):
                    buckets.append(b^)
            return out.bound(OP_IN, pf.field_id, P_INT, buckets^)
        return -1

    if t.kind == T_TRUNCATE:
        if n.op == OP_STARTS_WITH:
            if not inclusive:
                return -1
            ref p = n.lits[0]
            if p.kind != P_STRING:
                return -1
            var trunc = truncate_codepoints(p.s, t.param)
            if trunc == p.s:
                # The prefix is no longer than the truncation width, so the
                # partition value still starts with it.
                return out.bound(
                    OP_STARTS_WITH, pf.field_id, P_STRING, [p.copy()]
                )
            return out.bound(
                OP_EQ, pf.field_id, P_STRING, [Datum.string_(trunc^)]
            )
        return _project_ordered(e, i, pf, out, inclusive)

    if t.is_time():
        return _project_ordered(e, i, pf, out, inclusive)
    return -1


def _project_ordered(
    e: Expr, i: Int, pf: PartitionField, mut out: Expr, inclusive: Bool
) raises -> Int:
    """Projection for order-preserving transforms (truncate and the time ones).

    Because `a <= b` implies `T(a) <= T(b)`, a bound on the value gives a bound
    on the partition value. `<` and `>` become `<=` and `>=` on the transform of
    the adjacent value: many values share one partition value, so the strict
    end of the range cannot survive.
    """
    ref n = e.nodes[i]
    ref t = pf.transform
    if n.op == OP_IN:
        # Every literal maps to a partition value; the set of them bounds it.
        if not inclusive:
            return -1
        var vs = List[Datum]()
        for k in range(len(n.lits)):
            var m = t.apply(n.lits[k])
            if not _contains_datum(vs, m):
                vs.append(m^)
        if len(vs) == 0:
            return -1
        var kind = vs[0].kind
        return out.bound(OP_IN, pf.field_id, kind, vs^)
    if n.op == OP_NOT_EQ and inclusive:
        # Many values share a partition value, so "not this one" says nothing.
        return -1
    if n.op == OP_EQ and not inclusive:
        # Strictly, a partition matches "= v" only if it holds v alone, which
        # the transform alone cannot establish.
        return -1
    if len(n.lits) == 0:
        return -1
    # `<` and `>` are evaluated at the adjacent value so the projected bound
    # can be inclusive; `<=`, `>=`, `=` and `!=` project the literal itself.
    var literal = n.lits[0].copy()
    if n.op == OP_LT:
        literal = _step(n.lits[0], -1)
    elif n.op == OP_GT:
        literal = _step(n.lits[0], 1)
    var value = t.apply(literal)
    var kind = value.kind
    var op = n.op
    if n.op == OP_LT or n.op == OP_LT_EQ:
        op = OP_LT_EQ if inclusive else OP_LT
    elif n.op == OP_GT or n.op == OP_GT_EQ:
        op = OP_GT_EQ if inclusive else OP_GT
    elif n.op != OP_EQ and n.op != OP_NOT_EQ:
        return -1
    var lits = List[Datum]()
    lits.append(value^)
    return out.bound(op, pf.field_id, kind, lits^)


def _step(d: Datum, by: Int64) raises -> Datum:
    """The adjacent value, for turning `<` into `<=` on the transform."""
    if is_integer_like(d.kind):
        return Datum.integral(d.kind, d.i + by)
    return d.copy()


def _contains_datum(l: List[Datum], v: Datum) raises -> Bool:
    for k in range(len(l)):
        if l[k].valid == v.valid and (not v.valid or compare(l[k], v) == 0):
            return True
    return False


# ── evaluation against summaries and metrics ────────────────────────────────
comptime ROWS_MIGHT_MATCH = True
comptime ROWS_CANNOT_MATCH = False


@fieldwise_init
struct FieldSummary(Copyable, Movable):
    """A manifest list's `partitions` entry: what one partition field holds
    across every file in that manifest."""

    var contains_null: Bool
    var contains_nan: Bool
    var has_contains_nan: Bool
    var lower_bound: List[UInt8]
    var has_lower: Bool
    var upper_bound: List[UInt8]
    var has_upper: Bool

    @staticmethod
    def empty() -> Self:
        return Self(True, False, False, [], False, [], False)


struct ManifestEvaluator(Copyable, Movable):
    """Applies a partition predicate to a manifest's field summaries.

    Constructed from a *row* filter: the filter is projected inclusively
    through the manifest's own partition spec first, so the evaluator only ever
    sees predicates on partition field ids.
    """

    var expr: Expr
    var spec: PartitionSpec
    var part_type: Schema

    def __init__(
        out self, row_filter: Expr, spec: PartitionSpec, schema: Schema
    ) raises:
        var rewritten = rewrite_not(row_filter)
        self.expr = project_inclusive(rewritten, spec, schema)
        self.spec = spec.copy()
        self.part_type = spec.partition_type(schema)

    def eval(self, summaries: List[FieldSummary]) raises -> Bool:
        """True when this manifest might contain a matching row."""
        return self._eval(self.expr.root, summaries)

    def _eval(self, i: Int, s: List[FieldSummary]) raises -> Bool:
        ref n = self.expr.nodes[i]
        if n.op == OP_TRUE:
            return ROWS_MIGHT_MATCH
        if n.op == OP_FALSE:
            return ROWS_CANNOT_MATCH
        if n.op == OP_AND:
            return self._eval(n.left, s) and self._eval(n.right, s)
        if n.op == OP_OR:
            return self._eval(n.left, s) or self._eval(n.right, s)
        if n.op == OP_NOT:
            return not self._eval(n.left, s)
        var pos = self.spec.field_index(n.field_id)
        if pos < 0 or pos >= len(s):
            return ROWS_MIGHT_MATCH
        ref sum = s[pos]
        if n.op == OP_IS_NULL:
            return ROWS_MIGHT_MATCH if sum.contains_null else ROWS_CANNOT_MATCH
        if n.op == OP_NOT_NULL:
            # All-null when there is no lower bound at all.
            return ROWS_CANNOT_MATCH if not sum.has_lower else ROWS_MIGHT_MATCH
        if n.op == OP_IS_NAN:
            if sum.has_contains_nan and not sum.contains_nan:
                return ROWS_CANNOT_MATCH
            return ROWS_MIGHT_MATCH
        if n.op == OP_NOT_NAN:
            return ROWS_MIGHT_MATCH
        if not sum.has_lower or not sum.has_upper:
            # Only nulls in this field: no non-null predicate can match.
            return ROWS_CANNOT_MATCH
        var ptype = self.part_type.find_field(n.field_id).type
        ref tn = self.part_type.store.nodes[ptype]
        var lo = datum_from_bytes_prim(
            tn.prim, tn.precision, tn.scale, tn.length, sum.lower_bound
        )
        var hi = datum_from_bytes_prim(
            tn.prim, tn.precision, tn.scale, tn.length, sum.upper_bound
        )
        return _range_matches(n.op, n.lits, lo, hi)


def _range_matches(
    op: UInt8, lits: List[Datum], lo: Datum, hi: Datum
) raises -> Bool:
    """Shared bound logic for manifest summaries and data-file metrics."""
    if op == OP_LT:
        return (
            ROWS_CANNOT_MATCH if compare(lo, lits[0]) >= 0 else ROWS_MIGHT_MATCH
        )
    if op == OP_LT_EQ:
        return (
            ROWS_CANNOT_MATCH if compare(lo, lits[0]) > 0 else ROWS_MIGHT_MATCH
        )
    if op == OP_GT:
        return (
            ROWS_CANNOT_MATCH if compare(hi, lits[0]) <= 0 else ROWS_MIGHT_MATCH
        )
    if op == OP_GT_EQ:
        return (
            ROWS_CANNOT_MATCH if compare(hi, lits[0]) < 0 else ROWS_MIGHT_MATCH
        )
    if op == OP_EQ:
        if compare(lo, lits[0]) > 0 or compare(hi, lits[0]) < 0:
            return ROWS_CANNOT_MATCH
        return ROWS_MIGHT_MATCH
    if op == OP_NOT_EQ:
        # Never prunes. A bound is not necessarily a value that occurs in the
        # file — string upper bounds in particular are rounded *up* — so
        # `lower == upper == X` does not prove every value is X. Both reference
        # implementations return "might match" here for the same reason.
        return ROWS_MIGHT_MATCH
    if op == OP_IN:
        for k in range(len(lits)):
            if compare(lo, lits[k]) <= 0 and compare(hi, lits[k]) >= 0:
                return ROWS_MIGHT_MATCH
        return ROWS_CANNOT_MATCH
    if op == OP_NOT_IN:
        # Same reasoning as `!=`: bounds cannot rule a value *in*.
        return ROWS_MIGHT_MATCH
    if op == OP_STARTS_WITH:
        if lo.kind != P_STRING or lits[0].kind != P_STRING:
            return ROWS_MIGHT_MATCH
        var p = lits[0].s
        # Bounds may themselves be truncated, so compare only the prefix.
        if _prefix_cmp(lo.s, p) > 0 or _prefix_cmp(hi.s, p) < 0:
            return ROWS_CANNOT_MATCH
        return ROWS_MIGHT_MATCH
    if op == OP_NOT_STARTS_WITH:
        return ROWS_MIGHT_MATCH
    return ROWS_MIGHT_MATCH


def _prefix_cmp(bound: String, prefix: String) -> Int:
    """Compare `bound` truncated to `prefix`'s length against `prefix`."""
    var t = truncate_codepoints(bound, prefix.count_codepoints())
    if t < prefix:
        return -1
    return 0 if t == prefix else 1


@fieldwise_init
struct ColumnMetrics(Copyable, Movable):
    """Per-column statistics carried by a `data_file` entry."""

    var field_id: Int
    var value_count: Int64
    var has_value_count: Bool
    var null_value_count: Int64
    var has_null_value_count: Bool
    var nan_value_count: Int64
    var has_nan_value_count: Bool
    var lower_bound: List[UInt8]
    var has_lower: Bool
    var upper_bound: List[UInt8]
    var has_upper: Bool

    @staticmethod
    def blank(field_id: Int) -> Self:
        return Self(
            field_id, 0, False, 0, False, 0, False, [], False, [], False
        )


struct InclusiveMetricsEvaluator(Copyable, Movable):
    """Applies a bound row filter to one data file's column metrics."""

    var expr: Expr
    var schema: Schema

    def __init__(out self, row_filter: Expr, schema: Schema) raises:
        self.expr = rewrite_not(row_filter)
        self.schema = schema.copy()

    def eval(
        self, record_count: Int64, metrics: List[ColumnMetrics]
    ) raises -> Bool:
        if record_count <= 0:
            return ROWS_CANNOT_MATCH
        return self._eval(self.expr.root, metrics)

    def _find(self, metrics: List[ColumnMetrics], field_id: Int) -> Int:
        for k in range(len(metrics)):
            if metrics[k].field_id == field_id:
                return k
        return -1

    def _eval(self, i: Int, m: List[ColumnMetrics]) raises -> Bool:
        ref n = self.expr.nodes[i]
        if n.op == OP_TRUE:
            return ROWS_MIGHT_MATCH
        if n.op == OP_FALSE:
            return ROWS_CANNOT_MATCH
        if n.op == OP_AND:
            return self._eval(n.left, m) and self._eval(n.right, m)
        if n.op == OP_OR:
            return self._eval(n.left, m) or self._eval(n.right, m)
        if n.op == OP_NOT:
            return not self._eval(n.left, m)
        var k = self._find(m, n.field_id)
        if k < 0:
            return ROWS_MIGHT_MATCH
        ref c = m[k]
        if n.op == OP_IS_NULL:
            if c.has_null_value_count and c.null_value_count == 0:
                return ROWS_CANNOT_MATCH
            return ROWS_MIGHT_MATCH
        if n.op == OP_NOT_NULL:
            if (
                c.has_value_count
                and c.has_null_value_count
                and c.null_value_count == c.value_count
            ):
                return ROWS_CANNOT_MATCH
            return ROWS_MIGHT_MATCH
        if n.op == OP_IS_NAN:
            if c.has_nan_value_count and c.nan_value_count == 0:
                return ROWS_CANNOT_MATCH
            return ROWS_MIGHT_MATCH
        if n.op == OP_NOT_NAN:
            if (
                c.has_value_count
                and c.has_nan_value_count
                and c.nan_value_count == c.value_count
            ):
                return ROWS_CANNOT_MATCH
            return ROWS_MIGHT_MATCH
        # An all-null column cannot satisfy any value predicate.
        if (
            c.has_value_count
            and c.has_null_value_count
            and c.null_value_count == c.value_count
        ):
            return ROWS_CANNOT_MATCH
        if not c.has_lower or not c.has_upper:
            return ROWS_MIGHT_MATCH
        if not self.schema.has_field(n.field_id):
            return ROWS_MIGHT_MATCH
        var af = self.schema.find_field(n.field_id)
        ref tn = self.schema.store.nodes[af.type]
        var lo = datum_from_bytes_prim(
            tn.prim, tn.precision, tn.scale, tn.length, c.lower_bound
        )
        var hi = datum_from_bytes_prim(
            tn.prim, tn.precision, tn.scale, tn.length, c.upper_bound
        )
        return _range_matches(n.op, n.lits, lo, hi)


# ── residuals ───────────────────────────────────────────────────────────────
struct ResidualEvaluator(Copyable, Movable):
    """What is left of a filter once a partition is known to be selected.

    If the strict projection says the whole partition matches, the residual is
    `true` and no per-row check is needed. Otherwise the original filter stands.
    """

    var expr: Expr
    var strict: Expr
    var inclusive: Expr
    var spec: PartitionSpec

    def __init__(
        out self, row_filter: Expr, spec: PartitionSpec, schema: Schema
    ) raises:
        var rewritten = rewrite_not(row_filter)
        self.strict = project_strict(rewritten, spec, schema)
        self.inclusive = project_inclusive(rewritten, spec, schema)
        self.expr = rewritten^
        self.spec = spec.copy()

    def residual_for(self, partition: List[Datum]) raises -> Expr:
        """The residual filter for one partition tuple."""
        if _eval_partition(self.strict, self.strict.root, self.spec, partition):
            var t = Expr()
            t.root = t.constant(True)
            return t^
        return self.expr.copy()

    def selects(self, partition: List[Datum]) raises -> Bool:
        """Whether the inclusive projection keeps this partition tuple."""
        return _eval_partition(
            self.inclusive, self.inclusive.root, self.spec, partition
        )


def _eval_partition(
    e: Expr, i: Int, spec: PartitionSpec, partition: List[Datum]
) raises -> Bool:
    """Evaluate a projected predicate against one concrete partition tuple."""
    ref n = e.nodes[i]
    if n.op == OP_TRUE:
        return True
    if n.op == OP_FALSE:
        return False
    if n.op == OP_AND:
        return _eval_partition(e, n.left, spec, partition) and _eval_partition(
            e, n.right, spec, partition
        )
    if n.op == OP_OR:
        return _eval_partition(e, n.left, spec, partition) or _eval_partition(
            e, n.right, spec, partition
        )
    if n.op == OP_NOT:
        return not _eval_partition(e, n.left, spec, partition)
    var pos = spec.field_index(n.field_id)
    if pos < 0 or pos >= len(partition):
        return True
    ref v = partition[pos]
    if n.op == OP_IS_NULL:
        return not v.valid
    if n.op == OP_NOT_NULL:
        return v.valid
    if n.op == OP_IS_NAN:
        return v.is_nan()
    if n.op == OP_NOT_NAN:
        return not v.is_nan()
    if not v.valid:
        return False
    if n.op == OP_IN or n.op == OP_NOT_IN:
        var found = _contains_datum(n.lits, v)
        return found if n.op == OP_IN else not found
    if n.op == OP_STARTS_WITH or n.op == OP_NOT_STARTS_WITH:
        if v.kind != P_STRING or n.lits[0].kind != P_STRING:
            return True
        var yes = v.s.startswith(n.lits[0].s)
        return yes if n.op == OP_STARTS_WITH else not yes
    var c = compare(v, n.lits[0])
    if n.op == OP_LT:
        return c < 0
    if n.op == OP_LT_EQ:
        return c <= 0
    if n.op == OP_GT:
        return c > 0
    if n.op == OP_GT_EQ:
        return c >= 0
    if n.op == OP_EQ:
        return c == 0
    return c != 0
