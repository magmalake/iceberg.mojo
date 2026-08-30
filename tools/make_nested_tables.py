#!/usr/bin/env python3
"""Write the three nested-column fixture tables with PyIceberg.

The other fixtures come from iceberg-rust or from PyIceberg's plain-column
path; these are the ones with structs, lists and maps in them, and they go
into the *same* warehouse and the *same* sqlite catalog so the fixture set
stays one coherent Iceberg warehouse.

| table | what it is for |
|---|---|
| `nested_v2` | every nested shape at once: struct, `list<string>`, `map<string,long>`, `list<struct>`, `list<list<int>>`, `map<int,struct>`, and a struct holding a list of structs |
| `nested_evo_v2` | schema evolution *inside* a struct: a field added, a field renamed, and an `int` promoted to `long`, with rows written on each side of the change |
| `nested_part_v2` | partitioned by a nested struct leaf, `identity(addr.city)` — which the spec allows and PyIceberg 0.11.1 can both write and read |

Every table gets two snapshots, so the manifest carry-over paths are exercised
too. Values are deliberately awkward: null containers next to *empty* ones,
null elements inside a list, a null value inside a map, and a struct that is
present with every field null.

Field ids are whatever PyIceberg assigns — it renumbers a schema on
`create_table` — so nothing here depends on the ids chosen below.

    FIXTURE_ROOT=... FIXTURE_OUT=... make_nested_tables.py
"""

from __future__ import annotations

import os
import shutil
import sys

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

NAMESPACE = "db"


def catalog_for(root: str) -> SqlCatalog:
    os.makedirs(os.path.join(root, "warehouse"), exist_ok=True)
    cat = SqlCatalog(
        "sql",
        **{
            "uri": "sqlite:///" + os.path.join(root, "catalog.db"),
            "warehouse": "file://" + os.path.join(root, "warehouse"),
        },
    )
    try:
        cat.create_namespace(NAMESPACE)
    except Exception:
        pass
    return cat


def fresh(cat: SqlCatalog, root: str, name: str, schema: Schema, **kwargs):
    """A table with nothing left over from a previous run.

    Dropping alone leaves the old `metadata/` and `data/` behind, and the
    fixtures are checked in — so the directory goes too, and the checked-in
    copy is exactly one table lifetime.
    """
    ident = f"{NAMESPACE}.{name}"
    try:
        cat.drop_table(ident)
    except Exception:
        pass
    shutil.rmtree(os.path.join(root, "warehouse", NAMESPACE, name), ignore_errors=True)
    return cat.create_table(ident, schema=schema, **kwargs)


# ── nested_v2 ───────────────────────────────────────────────────────────────
RICH = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(
        2,
        "addr",
        StructType(
            NestedField(10, "city", StringType(), required=False),
            NestedField(11, "zip", IntegerType(), required=False),
        ),
        required=False,
    ),
    NestedField(
        3,
        "tags",
        ListType(element_id=20, element_type=StringType(), element_required=False),
        required=False,
    ),
    NestedField(
        4,
        "props",
        MapType(
            key_id=30,
            key_type=StringType(),
            value_id=31,
            value_type=LongType(),
            value_required=False,
        ),
        required=False,
    ),
    NestedField(
        5,
        "items",
        ListType(
            element_id=40,
            element_type=StructType(
                NestedField(41, "sku", StringType(), required=False),
                NestedField(42, "qty", IntegerType(), required=False),
            ),
            element_required=False,
        ),
        required=False,
    ),
    NestedField(
        6,
        "matrix",
        ListType(
            element_id=50,
            element_type=ListType(
                element_id=51, element_type=IntegerType(), element_required=False
            ),
            element_required=False,
        ),
        required=False,
    ),
    NestedField(
        7,
        "who",
        MapType(
            key_id=60,
            key_type=IntegerType(),
            value_id=61,
            value_type=StructType(
                NestedField(62, "name", StringType(), required=False),
                NestedField(63, "age", IntegerType(), required=False),
            ),
            value_required=False,
        ),
        required=False,
    ),
    NestedField(
        8,
        "deep",
        StructType(
            NestedField(
                70,
                "inner",
                ListType(
                    element_id=71,
                    element_type=StructType(
                        NestedField(72, "tag", StringType(), required=False),
                    ),
                    element_required=False,
                ),
                required=False,
            ),
        ),
        required=False,
    ),
)

RICH_ROWS = [
    # id, addr, tags, props, items, matrix, who, deep
    (1, {"city": "eu", "zip": 10}, ["a", "b"], [("x", 1), ("y", 2)],
     [{"sku": "s1", "qty": 3}], [[1, 2], [3]], [(1, {"name": "ann", "age": 30})],
     {"inner": [{"tag": "t1"}, {"tag": None}]}),
    (2, None, [], [], [], [], [], {"inner": []}),
    (3, {"city": None, "zip": 30}, None, None, None, None, None, None),
    (4, {"city": "us", "zip": None}, ["c", None], [("z", None)],
     [{"sku": None, "qty": 7}, None], [[], None], [(2, None)],
     {"inner": None}),
    (5, {"city": "eu", "zip": 50}, ["d"], [("k", 9)],
     [{"sku": "s2", "qty": 1}], [[4, 5, 6]], [(3, {"name": None, "age": 41})],
     {"inner": [{"tag": "t2"}]}),
    (6, {"city": "apac", "zip": 60}, ["e", "f", "g"],
     [("a", 1), ("b", 2), ("c", 3)], [], [[7]], [], {"inner": []}),
    (7, None, ["h"], [("q", 42)], [{"sku": "s3", "qty": None}], None,
     [(4, {"name": "bo", "age": None})], {"inner": [{"tag": "t3"}]}),
    (8, {"city": "us", "zip": 80}, None, [], None, [[8], [9, 10]], None,
     {"inner": [{"tag": "t4"}, {"tag": "t5"}]}),
]

RICH_COLUMNS = ["id", "addr", "tags", "props", "items", "matrix", "who", "deep"]


def arrow_rows(schema_arrow, columns, rows):
    data = {}
    for i, name in enumerate(columns):
        data[name] = pa.array(
            [r[i] for r in rows], type=schema_arrow.field(name).type
        )
    return pa.table(data, schema=schema_arrow)


def make_rich(cat: SqlCatalog, root: str) -> str:
    t = fresh(cat, root, "nested_v2", RICH, properties={"format-version": "2"})
    sa = t.schema().as_arrow()
    t.append(arrow_rows(sa, RICH_COLUMNS, RICH_ROWS[:4]))
    t.append(arrow_rows(sa, RICH_COLUMNS, RICH_ROWS[4:]))
    return "nested_v2"


# ── nested_evo_v2 ───────────────────────────────────────────────────────────
EVO = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(
        2,
        "addr",
        StructType(
            NestedField(10, "city", StringType(), required=False),
            NestedField(11, "zip", IntegerType(), required=False),
        ),
        required=False,
    ),
    NestedField(
        3,
        "tags",
        ListType(element_id=20, element_type=StringType(), element_required=False),
        required=False,
    ),
)

EVO_BEFORE = [
    (1, {"city": "eu", "zip": 10}, ["a"]),
    (2, {"city": "us", "zip": 20}, []),
    (3, None, None),
    (4, {"city": None, "zip": 40}, ["b", "c"]),
]

EVO_AFTER = [
    (5, {"town": "eu", "zip": 50, "country": "de"}, ["d"]),
    (6, {"town": None, "zip": 60, "country": None}, None),
    (7, {"town": "apac", "zip": 7_000_000_000, "country": "sg"}, []),
    (8, None, ["e"]),
]


def make_evolved(cat: SqlCatalog, root: str) -> str:
    """Rows written under one struct shape, then read under another.

    Inside `addr`: a field is **added** (`country`, null in the older file), a
    field is **renamed** (`city` -> `town`, still the same id, so the older
    file's values still resolve) and a field is **promoted** (`zip`, `int` ->
    `long`, so the older file's 32-bit column has to be widened on read).
    """
    t = fresh(cat, root, "nested_evo_v2", EVO, properties={"format-version": "2"})
    t.append(arrow_rows(t.schema().as_arrow(), ["id", "addr", "tags"], EVO_BEFORE))

    with t.update_schema() as us:
        us.add_column(("addr", "country"), StringType())
        us.rename_column(("addr", "city"), "town")
        us.update_column(("addr", "zip"), field_type=LongType())

    t.append(arrow_rows(t.schema().as_arrow(), ["id", "addr", "tags"], EVO_AFTER))
    return "nested_evo_v2"


# ── nested_part_v2 ──────────────────────────────────────────────────────────
PART_ROWS = [
    (1, {"city": "eu", "zip": 10}, ["a"]),
    (2, {"city": "us", "zip": 20}, []),
    (3, {"city": "eu", "zip": 30}, None),
    (4, {"city": None, "zip": 40}, ["b"]),
    (5, {"city": "apac", "zip": 50}, ["c", "d"]),
    (6, {"city": "us", "zip": 60}, None),
    (7, {"city": "eu", "zip": 70}, []),
    (8, None, ["e"]),
]


def make_partitioned(cat: SqlCatalog, root: str) -> str:
    """Partitioned by `identity(addr.city)` — a nested struct leaf.

    The spec lets a partition field's `source-id` name a nested field, and a
    scan of this table has to read the partition tuple, prune on it, and put
    the value back into the struct for the files that do not carry it.
    """
    t = fresh(cat, root, "nested_part_v2", EVO, properties={"format-version": "2"})
    with t.update_spec() as us:
        us.add_field("addr.city", IdentityTransform(), "city")
    t.append(arrow_rows(t.schema().as_arrow(), ["id", "addr", "tags"], PART_ROWS[:4]))
    t.append(arrow_rows(t.schema().as_arrow(), ["id", "addr", "tags"], PART_ROWS[4:]))
    return "nested_part_v2"


def main(argv):
    root = os.environ.get("FIXTURE_ROOT")
    if not root:
        print("FIXTURE_ROOT is required", file=sys.stderr)
        return 2
    cat = catalog_for(root)
    made = []
    for fn in (make_rich, make_evolved, make_partitioned):
        made.append(fn(cat, root))
        t = cat.load_table(f"{NAMESPACE}.{made[-1]}")
        print(
            "wrote %s: %d rows, %d snapshots, %d files"
            % (
                made[-1],
                t.scan().to_arrow().num_rows,
                len(t.metadata.snapshots),
                len(t.inspect.files()),
            )
        )
    print(",".join(made))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
