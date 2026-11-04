#!/usr/bin/env python3
"""Cross-implementation parity for `SqlCatalog` — the acceptance gate.

Two directions, both against the same sqlite file:

1. **PyIceberg reads what we write.** `tools/sql_catalog_write.mojo` creates
   a namespace and a table with this repo's `SqlCatalog`, appends two
   batches, renames a table, drops a scratch one. PyIceberg's own
   `SqlCatalog`, pointed at the same `catalog.db` and warehouse, must list
   the namespace, list the (renamed) table, load it, and read the rows back
   cell-exact — plus confirm the drop and the namespace property landed,
   and read `db.evo`, whose schema we evolved (a column added, one renamed)
   between two appends.

2. **We read what PyIceberg writes.** PyIceberg creates a namespace and
   table in a second sqlite file with a pyarrow append, and this repo's own
   `iceberg-mojo cat --sql` reads it back — checked here by shelling out to
   the CLI binary and comparing its CSV output to what PyIceberg says the
   table holds — then again after PyIceberg's `update_schema` adds a column
   and a struct and renames one, with a file on each side of the change.

Also checked directly against sqlite (no PyIceberg needed for this part,
since it is a property of the schema and the guard, not of either client):
the guarded `UPDATE ... WHERE metadata_location = <stale>` a concurrent
commit already moved past must affect zero rows rather than clobbering.

Usage: verify_sql_catalog.py <catalog1.db> <warehouse1> <catalog2.db> \\
           <warehouse2> <iceberg-mojo-binary>
"""
import csv
import io
import os
import sqlite3
import subprocess
import sys


def region_of(i: int) -> str:
    return ["eu", "us", "apac", "latam", "emea"][i % 5]


def expected_rows(n: int):
    return [(i, region_of(i), float(i) * 1.5) for i in range(n)]


def check(cond: bool, msg: str):
    if not cond:
        raise SystemExit(f"FAIL: {msg}")
    print(f"  ok: {msg}")


def direction_one(catalog1_db: str, warehouse1: str):
    print("== direction 1: PyIceberg reads what we write")
    from pyiceberg.catalog.sql import SqlCatalog

    cat = SqlCatalog(
        "default",
        **{"uri": f"sqlite:///{catalog1_db}", "warehouse": f"file://{warehouse1}"},
    )
    namespaces = [".".join(ns) for ns in cat.list_namespaces()]
    check("db" in namespaces, "PyIceberg lists the 'db' namespace we created")

    tables = [".".join(t) for t in cat.list_tables("db")]
    check("db.rt_renamed" in tables, "PyIceberg lists the renamed table")
    check("db.rt" not in tables, "the pre-rename name is gone")
    check("db.scratch" not in tables, "the dropped scratch table is gone")

    props = cat.load_namespace_properties("db")
    check(props.get("owner") == "marius", "namespace property round-trips")

    table = cat.load_table("db.rt_renamed")
    rows = table.scan().to_arrow().to_pylist()
    rows_sorted = sorted((r["id"], r["region"], r["amount"]) for r in rows)
    check(len(rows_sorted) == 12, f"12 rows, got {len(rows_sorted)}")
    check(
        rows_sorted == expected_rows(12),
        "every row matches cell-exact what the Mojo writer wrote",
    )
    check(len(table.metadata.snapshots) == 2, "two append snapshots")

    # The guarded swap, checked with plain sqlite3 — a property of the schema
    # and the UPDATE predicate, not of either client, so this needs no
    # PyIceberg call. Simulates a writer that read the table before the two
    # real appends landed, and would clobber the pointer if the guard did not
    # hold.
    con = sqlite3.connect(catalog1_db)
    stale_location = table.metadata_location.replace(
        "00002-", "00000-"
    )  # never the real value; just needs to not match the current row
    cur = con.execute(
        "UPDATE iceberg_tables SET metadata_location = ? WHERE catalog_name = ?"
        " AND table_namespace = ? AND table_name = ? AND metadata_location = ?",
        ("bogus-should-not-land", "default", "db", "rt_renamed", stale_location),
    )
    check(cur.rowcount == 0, "a guarded swap against a stale pointer affects 0 rows")
    con.rollback()
    cur2 = con.execute(
        "SELECT metadata_location FROM iceberg_tables WHERE catalog_name = ? AND"
        " table_namespace = ? AND table_name = ?",
        ("default", "db", "rt_renamed"),
    )
    (still,) = cur2.fetchone()
    check(still == table.metadata_location, "the real pointer is untouched")
    con.close()

    # Schema evolution, committed by us: `note` (id 4) added after four rows,
    # `amount` renamed `total`, three rows with `note` after that.
    evo = cat.load_table("db.evo")
    fields = {f.name: f for f in evo.schema().fields}
    check(evo.schema().schema_id == 1, "the evolved table's current schema is 1")
    check(len(evo.metadata.schemas) == 2, "both schemas are in the metadata")
    check(evo.metadata.last_column_id == 4, "last-column-id moved to 4")
    check(
        "note" in fields and fields["note"].field_id == 4
        and not fields["note"].required
        and fields["note"].doc == "added later",
        "PyIceberg sees `note` as optional field 4 with its doc",
    )
    check(
        "total" in fields and fields["total"].field_id == 3
        and "amount" not in fields,
        "PyIceberg sees `amount` renamed to `total`, same id",
    )
    got = sorted(
        (r["id"], r["region"], r["total"], r["note"])
        for r in evo.scan().to_arrow().to_pylist()
    )
    want = [
        (i, region_of(i), float(i) * 1.5, None if i < 4 else f"n{i}")
        for i in range(7)
    ]
    check(got == want, "old files read `note` as null; new ones carry it")
    old = evo.scan(snapshot_id=evo.metadata.snapshots[0].snapshot_id)
    check(
        [f.name for f in old.projection().fields] == ["id", "region", "amount"],
        "the first snapshot keeps the schema it was written with",
    )
    cat.close()


def direction_two(catalog2_db: str, warehouse2: str, iceberg_mojo_bin: str):
    print("== direction 2: we read what PyIceberg writes")
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    cat = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{catalog2_db}",
            "warehouse": f"file://{warehouse2}",
            "init_catalog_tables": "true",
        },
    )
    cat.create_namespace("db2")
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "region", StringType(), required=True),
        NestedField(3, "amount", DoubleType(), required=False),
    )
    table = cat.create_table("db2.pt", schema)
    n = 9
    arrow_table = pa.table(
        {
            "id": pa.array([i for i in range(n)], type=pa.int64()),
            "region": pa.array([region_of(i) for i in range(n)], type=pa.string()),
            "amount": pa.array([float(i) * 1.5 for i in range(n)], type=pa.float64()),
        },
        schema=table.schema().as_arrow(),
    )
    table.append(arrow_table)
    expected = expected_rows(n)

    result = subprocess.run(
        [
            iceberg_mojo_bin,
            "cat",
            "--sql",
            f"sqlite:///{catalog2_db}",
            "--table",
            "db2.pt",
            "--warehouse",
            warehouse2,
            "--select",
            "id,region,amount",
            "--format",
            "csv",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    reader = csv.reader(io.StringIO(result.stdout))
    next(reader)  # header
    got = sorted((int(r[0]), r[1], float(r[2])) for r in reader)
    check(len(got) == n, f"{n} rows read back by iceberg-mojo, got {len(got)}")
    check(
        got == expected,
        "every row iceberg-mojo reads matches what PyIceberg wrote, cell-exact",
    )

    # Schema evolution, committed by PyIceberg: we must read its new schema,
    # and null for `note` in the file written before it existed.
    from pyiceberg.types import StructType

    with table.update_schema() as update:
        update.add_column("note", StringType())
        update.rename_column("amount", "total")
        update.add_column(
            "geo",
            StructType(
                NestedField(100, "lat", DoubleType(), required=False),
            ),
        )
    table = cat.load_table("db2.pt")
    check(table.schema().schema_id == 1, "PyIceberg committed schema 1")
    more = pa.table(
        {
            "id": pa.array([9, 10], type=pa.int64()),
            "region": pa.array([region_of(9), region_of(10)], type=pa.string()),
            "total": pa.array([13.5, 15.0], type=pa.float64()),
            "note": pa.array(["n9", "n10"], type=pa.string()),
            "geo": pa.array(
                [{"lat": 1.0}, None], type=pa.struct([("lat", pa.float64())])
            ),
        },
        schema=table.schema().as_arrow(),
    )
    table.append(more)
    result = subprocess.run(
        [
            iceberg_mojo_bin,
            "cat",
            "--sql",
            f"sqlite:///{catalog2_db}",
            "--table",
            "db2.pt",
            "--warehouse",
            warehouse2,
            "--select",
            "id,total,note,geo.lat",
            "--format",
            "csv",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    reader = csv.reader(io.StringIO(result.stdout))
    header = next(reader)
    check(header == ["id", "total", "note", "geo"], f"header {header}")
    got = sorted((int(r[0]), float(r[1]), r[2], r[3]) for r in reader)
    want = [(i, float(i) * 1.5, "", "") for i in range(n)] + [
        (9, 13.5, "n9", '{"lat":1.0}'),
        (10, 15.0, "n10", ""),
    ]
    check(
        got == want,
        "iceberg-mojo reads PyIceberg's evolved table: null `note` and `geo`"
        f" in the old file, the renamed `total` by id (got {got[-3:]})",
    )
    cat.close()


def main():
    if len(sys.argv) != 6:
        print(__doc__)
        return 1
    catalog1_db, warehouse1, catalog2_db, warehouse2, iceberg_mojo_bin = sys.argv[1:6]
    direction_one(catalog1_db, warehouse1)
    direction_two(catalog2_db, warehouse2, iceberg_mojo_bin)
    print("== ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
