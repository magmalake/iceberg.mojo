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
    AT_BOOL,
    AT_DECIMAL128,
    AT_FIXED_SIZE_BINARY,
    AT_FLOAT32,
    AT_FLOAT64,
    AT_LARGE_BINARY,
    AT_LARGE_UTF8,
    AT_UTF8,
    ArrayData,
    ArrowType,
    bit_set,
)

from .kernels import arrow_type_for, decimal_le16
from .schema import Schema
from .types import P_LONG, P_STRING, TK_PRIMITIVE
from .values import Datum


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
    var n = len(values)
    a.length = n
    a.nullable = nullable
    a.field_id = Int32(field_id)
    var width = a.type.fixed_width()
    var is_binary = (
        a.type.id == AT_UTF8
        or a.type.id == AT_BINARY
        or a.type.id == AT_LARGE_UTF8
        or a.type.id == AT_LARGE_BINARY
    )
    if is_binary:
        a.offsets.append(0)
    for r in range(n):
        ref d = values[r]
        bit_set(a.validity, r, d.valid)
        if not d.valid:
            a.null_count += 1
        if a.type.id == AT_BOOL:
            while len(a.values) <= r // 8:
                a.values.append(0)
            if d.valid and d.i != 0:
                a.values[r // 8] |= UInt8(1) << UInt8(r % 8)
        elif is_binary:
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
                    UInt32(
                        bitcast[DType.uint32](Float32(d.f if d.valid else 0.0))
                    )
                ),
                4,
            )
        elif a.type.id == AT_FLOAT64:
            _put_le(a.values, bitcast[DType.uint64](d.f if d.valid else 0.0), 8)
        else:
            _put_le(a.values, UInt64(Int64(d.i if d.valid else 0)), width)
    # A bitmap has to cover every row even when the last byte is partial.
    while len(a.validity) < (n + 7) // 8:
        a.validity.append(0)
    if a.type.id == AT_BOOL:
        while len(a.values) < (n + 7) // 8:
            a.values.append(0)
    return a^


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
