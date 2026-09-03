#!/usr/bin/env python3
"""Cross-implementation parity for `SqlCatalog` over PostgreSQL.

`tools/verify_sql_catalog.py`'s two directions, against a server instead of a
file — and against `postgresql+psycopg://`, the SQLAlchemy URL PyIceberg's
`SqlCatalog` takes, so both sides are talking to the same `iceberg_tables` /
`iceberg_namespace_properties` rows over the same wire protocol:

1. **PyIceberg reads what we write.** `tools/sql_catalog_write.mojo` creates a
   namespace and a table through this repo's `SqlCatalog`, appends two
   batches, renames a table and drops a scratch one, all in schema
   ``mojo_side``. PyIceberg's own `SqlCatalog`, pointed at the same schema,
   must list the namespace, list the renamed table, load it and read the rows
   back cell-exact — plus confirm the drop and the namespace property landed.

2. **We read what PyIceberg writes.** PyIceberg creates the catalog tables
   from scratch in schema ``pyiceberg_side``, creates a namespace and a table
   there and appends a pyarrow table; `iceberg-mojo cat --sql
   postgresql://…` reads it back, and its CSV is compared to what PyIceberg
   says the table holds. This is the check that a catalog database **created
   by PyIceberg over Postgres** loads unchanged here: nothing in this
   direction was written by Mojo code at all.

Also checked directly with psycopg, because it is a property of the schema and
the `UPDATE` predicate rather than of either client: the guarded ``UPDATE ...
WHERE metadata_location = <stale>`` a concurrent commit already moved past
must affect zero rows rather than clobbering.

The two schemas are what keeps the directions from colliding on one server;
`search_path` in the DSN is how each side is pointed at its own.

Usage: verify_pg_catalog.py <dsn> <warehouse1> <warehouse2> \\
           <iceberg-mojo-binary>
"""
import csv
import io
import subprocess
import sys

import psycopg

MOJO_SCHEMA = "mojo_side"
PYICEBERG_SCHEMA = "pyiceberg_side"


def region_of(i: int) -> str:
    return ["eu", "us", "apac", "latam", "emea"][i % 5]


def expected_rows(n: int):
    return [(i, region_of(i), float(i) * 1.5) for i in range(n)]


def check(cond: bool, msg: str):
    if not cond:
        raise SystemExit(f"FAIL: {msg}")
    print(f"  ok: {msg}")


def scoped(dsn: str, schema: str) -> str:
    """`dsn` with `search_path` fixed to `schema`, for either client."""
    sep = "&" if "?" in dsn else "?"
    return f"{dsn}{sep}options=-csearch_path%3D{schema}"


def sqlalchemy_url(dsn: str, schema: str) -> str:
    """The same, as the `postgresql+psycopg://` URL PyIceberg's SqlCatalog
    hands to SQLAlchemy."""
    return scoped(dsn, schema).replace("postgresql://", "postgresql+psycopg://", 1)


def direction_one(dsn: str, warehouse1: str):
    print("== direction 1: PyIceberg reads what we write")
    from pyiceberg.catalog.sql import SqlCatalog

    cat = SqlCatalog(
        "default",
        **{
            "uri": sqlalchemy_url(dsn, MOJO_SCHEMA),
            "warehouse": f"file://{warehouse1}",
        },
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

    # The guarded swap, checked with plain psycopg — a property of the schema
    # and the UPDATE predicate, not of either client. Simulates a writer that
    # read the table before the two real appends landed, and would clobber the
    # pointer if the guard did not hold.
    with psycopg.connect(scoped(dsn, MOJO_SCHEMA)) as con:
        stale_location = table.metadata_location.replace("00002-", "00000-")
        with con.cursor() as cur:
            cur.execute(
                "UPDATE iceberg_tables SET metadata_location = %s WHERE"
                " catalog_name = %s AND table_namespace = %s AND"
                " table_name = %s AND metadata_location = %s",
                (
                    "bogus-should-not-land",
                    "default",
                    "db",
                    "rt_renamed",
                    stale_location,
                ),
            )
            check(
                cur.rowcount == 0,
                "a guarded swap against a stale pointer affects 0 rows",
            )
        con.rollback()
        with con.cursor() as cur:
            cur.execute(
                "SELECT metadata_location FROM iceberg_tables WHERE"
                " catalog_name = %s AND table_namespace = %s AND"
                " table_name = %s",
                ("default", "db", "rt_renamed"),
            )
            (still,) = cur.fetchone()
        check(still == table.metadata_location, "the real pointer is untouched")
    cat.close()


def direction_two(dsn: str, warehouse2: str, iceberg_mojo_bin: str):
    print("== direction 2: we read what PyIceberg writes")
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    cat = SqlCatalog(
        "default",
        **{
            "uri": sqlalchemy_url(dsn, PYICEBERG_SCHEMA),
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
            scoped(dsn, PYICEBERG_SCHEMA),
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
    # The tables PyIceberg created are the ones this repo expects, unchanged:
    # opening the same catalog with `create_tables=False` is what the CLI just
    # did, and it found them.
    with psycopg.connect(scoped(dsn, PYICEBERG_SCHEMA)) as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE"
                " table_schema = %s AND table_name = 'iceberg_tables'"
                " ORDER BY ordinal_position",
                (PYICEBERG_SCHEMA,),
            )
            columns = [r[0] for r in cur.fetchall()]
    check(
        columns
        == [
            "catalog_name",
            "table_namespace",
            "table_name",
            "metadata_location",
            "previous_metadata_location",
        ],
        "PyIceberg's own iceberg_tables has exactly the columns we write",
    )
    cat.close()


def main():
    if len(sys.argv) != 5:
        print(__doc__)
        return 1
    dsn, warehouse1, warehouse2, iceberg_mojo_bin = sys.argv[1:5]
    direction_one(dsn, warehouse1)
    direction_two(dsn, warehouse2, iceberg_mojo_bin)
    print("== ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
