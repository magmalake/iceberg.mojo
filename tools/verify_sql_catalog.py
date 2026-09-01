#!/usr/bin/env python3
"""Cross-implementation parity for `SqlCatalog` — the acceptance gate.

Two directions, both against the same sqlite file:

1. **PyIceberg reads what we write.** `tools/sql_catalog_write.mojo` creates
   a namespace and a table with this repo's `SqlCatalog`, appends two
   batches, renames a table, drops a scratch one. PyIceberg's own
   `SqlCatalog`, pointed at the same `catalog.db` and warehouse, must list
   the namespace, list the (renamed) table, load it, and read the rows back
   cell-exact — plus confirm the drop and the namespace property landed.

2. **We read what PyIceberg writes.** PyIceberg creates a namespace and
   table in a second sqlite file with a pyarrow append, and this repo's own
   `iceberg-mojo cat --sql` reads it back — checked here by shelling out to
   the CLI binary and comparing its CSV output to what PyIceberg says the
   table holds.

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
