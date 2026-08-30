#!/usr/bin/env python3
"""Row-level oracles: what a scan must actually *return*, not just plan.

Two independent readers, written to two separate files per table so the Mojo
side can check against whichever exist:

    oracle/rows_pyiceberg_<k>.json   PyIceberg 0.11.1, all six filters
    oracle/rows_duckdb.json          DuckDB's iceberg extension, unfiltered

Neither covers everything, and the gaps are the interesting part:

* **PyIceberg 0.11.1 refuses equality deletes outright** — `plan_files` raises
  "PyIceberg does not yet support equality deletes" — so it produces nothing
  for `eq_deletes_v2`, and DuckDB is the only oracle for that table.
* **DuckDB 1.5.5 cannot read our v3 deletion-vector table**: it fails with
  `INTERNAL Error: Calling GetValue on a value that is NULL`. The same
  metadata with the pre-DV snapshot as current reads fine, and the same DV
  snapshot declared as v2 gives "DeletionVector not supported in Iceberg V2",
  so it is the v3 DV path specifically. PyIceberg is the oracle there.
* DuckDB takes no filter, so it is run unfiltered only.

**Cell encoding.** Every cell is a string or null, canonical and exact, so
that "the same rows" is a byte comparison and never a float-formatting
argument:

    boolean            "true" / "false"
    int, long          decimal
    float, double      big-endian IEEE-754 bits, lowercase hex (8 / 16 chars)
    date               days since 1970-01-01, decimal
    time               microseconds since midnight, decimal
    timestamp(tz)      microseconds since epoch, decimal
    timestamp_ns(tz)   nanoseconds since epoch, decimal
    string             the text itself
    uuid               canonical 8-4-4-4-12 lowercase
    binary, fixed      lowercase hex
    decimal            the exact decimal text, as written

Rows are sorted (as tuples, nulls first) so ordering never matters.

Usage:  oracle_rows.py <fixtures-dir> [--catalog <catalog.db>]
"""

from __future__ import annotations

import binascii
import datetime as dt
import decimal
import json
import os
import sqlite3
import struct
import sys
import uuid as _uuid

from pyiceberg.table import StaticTable
from pyiceberg.types import (
    BinaryType,
    ListType,
    MapType,
    StructType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimeType,
    TimestampType,
    TimestamptzType,
    UUIDType,
)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from oracle_pyiceberg import (  # noqa: E402
    NESTED_TABLE_PROJECTIONS,
    NESTED_TABLE_SQL,
    PYICEBERG_TABLE_FILTERS,
    catalog_metadata_basenames,
    find_metadata,
    parse_filter,
)

EPOCH = dt.datetime(1970, 1, 1)
EPOCH_TZ = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
EPOCH_DATE = dt.date(1970, 1, 1)


def encode(value, iceberg_type):
    if value is None:
        return None
    t = iceberg_type
    if isinstance(t, BooleanType):
        return "true" if value else "false"
    if isinstance(t, (IntegerType, LongType)):
        return str(int(value))
    if isinstance(t, FloatType):
        return binascii.hexlify(struct.pack(">f", float(value))).decode()
    if isinstance(t, DoubleType):
        return binascii.hexlify(struct.pack(">d", float(value))).decode()
    if isinstance(t, DateType):
        if isinstance(value, dt.datetime):
            value = value.date()
        return str((value - EPOCH_DATE).days)
    if isinstance(t, TimeType):
        micros = (
            value.hour * 3600 + value.minute * 60 + value.second
        ) * 1_000_000 + value.microsecond
        return str(micros)
    if isinstance(t, (TimestampType, TimestamptzType)):
        base = EPOCH_TZ if value.tzinfo is not None else EPOCH
        delta = value - base
        return str(
            delta.days * 86_400_000_000
            + delta.seconds * 1_000_000
            + delta.microseconds
        )
    if isinstance(t, StringType):
        return str(value)
    if isinstance(t, UUIDType):
        if isinstance(value, bytes):
            return str(_uuid.UUID(bytes=value))
        return str(value).lower()
    if isinstance(t, (BinaryType, FixedType)):
        return binascii.hexlify(bytes(value)).decode()
    if isinstance(t, DecimalType):
        return str(decimal.Decimal(value))
    if isinstance(t, (StructType, ListType, MapType)):
        return json.dumps(
            canonical(value, t), separators=(",", ":"), ensure_ascii=False
        )
    return json.dumps(value, sort_keys=True, default=str)


def canonical(value, t):
    """A nested value in the shape `ScanResult.cell` prints.

    A struct is an object keyed by **field name**, a list is an array, and a
    map is `{"keys": [...], "values": [...]}` — Appendix D's own shape, except
    that a struct is keyed by name rather than by field id, because these are
    rows a caller reads rather than a default stored in metadata. Leaves are
    plain JSON: a number for an integer, a quoted string for text, a quoted
    ISO-8601 string for a date or a timestamp. Map entries keep the order the
    file stores them in; the fixtures write their keys in ascending order, so
    the two oracles and this library all agree without anyone sorting.
    """
    if value is None:
        return None
    if isinstance(t, StructType):
        if not isinstance(value, dict):
            value = dict(value)
        return {f.name: canonical(value.get(f.name), f.field_type) for f in t.fields}
    if isinstance(t, ListType):
        return [canonical(v, t.element_type) for v in value]
    if isinstance(t, MapType):
        pairs = list(value.items()) if isinstance(value, dict) else list(value)
        return {
            "keys": [canonical(k, t.key_type) for k, _ in pairs],
            "values": [canonical(v, t.value_type) for _, v in pairs],
        }
    return _leaf_json(value, t)


def _leaf_json(value, t):
    """One primitive, as `Datum.to_json` would print it inside a container."""
    if value is None:
        return None
    if isinstance(t, BooleanType):
        return bool(value)
    if isinstance(t, (IntegerType, LongType)):
        return int(value)
    if isinstance(t, (FloatType, DoubleType)):
        return float(value)
    if isinstance(t, StringType):
        return str(value)
    if isinstance(t, UUIDType):
        return str(value).lower()
    if isinstance(t, (BinaryType, FixedType)):
        return binascii.hexlify(bytes(value)).decode()
    if isinstance(t, DecimalType):
        return str(decimal.Decimal(value))
    # date / time / timestamp: the ISO-8601 spelling Appendix D uses.
    return str(value)


def sort_key(row):
    return tuple((c is not None, c or "") for c in row)


def encode_rows(names, types, records):
    rows = []
    for rec in records:
        rows.append([encode(rec.get(n), t) for n, t in zip(names, types)])
    rows.sort(key=sort_key)
    return rows


def pyiceberg_rows(table, dsl):
    schema = table.schema()
    names = [f.name for f in schema.fields]
    types = [f.field_type for f in schema.fields]
    arrow = table.scan(row_filter=parse_filter(json.loads(dsl))).to_arrow()
    return names, encode_rows(names, types, arrow.to_pylist())


def duckdb_rows(table, metadata_path, where=None):
    import duckdb

    schema = table.schema()
    names = [f.name for f in schema.fields]
    types = [f.field_type for f in schema.fields]
    sql = "SELECT * FROM iceberg_scan(?) AS t"
    if where:
        sql += " WHERE " + where
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    try:
        cur = con.execute(sql, [metadata_path])
        cols = [d[0] for d in cur.description]
        records = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()
    return names, encode_rows(names, types, records)


def pyiceberg_projection(table, fields):
    """`select(["a.b", "c"])` — a projection that reaches inside a struct.

    PyIceberg returns the columns in *schema* order whatever order they were
    asked in, so the comparison is by name, and the struct comes back holding
    only the fields that were selected.
    """
    arrow = table.scan(selected_fields=tuple(fields)).to_arrow()
    projected = table.schema().select(*fields)
    names = [f.name for f in projected.fields]
    types = [f.field_type for f in projected.fields]
    return names, encode_rows(names, types, arrow.to_pylist())


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    fixtures = argv[1]
    catalog_db = None
    if "--catalog" in argv:
        catalog_db = argv[argv.index("--catalog") + 1]
    hints = catalog_metadata_basenames(catalog_db) if catalog_db else {}

    only = ()
    if "--only" in argv:
        only = tuple(argv[argv.index("--only") + 1].split(","))
    names = sorted(
        d for d in os.listdir(fixtures)
        if os.path.isdir(os.path.join(fixtures, d, "metadata"))
        and (not only or d in only)
    )
    summary = {}
    for name in names:
        tdir = os.path.join(fixtures, name)
        oracle_dir = os.path.join(tdir, "oracle")
        os.makedirs(oracle_dir, exist_ok=True)
        meta = find_metadata(tdir, hints.get(name))
        table = StaticTable.from_metadata(meta)
        info = {}

        filters = PYICEBERG_TABLE_FILTERS.get(name)
        if filters is None:
            filters = []
            for k in range(6):
                fpath = os.path.join(oracle_dir, "plan_%d.filter.txt" % k)
                if os.path.exists(fpath):
                    with open(fpath) as fh:
                        filters.append(fh.read().strip())

        ok = 0
        refused = []
        for k, dsl in enumerate(filters):
            try:
                cols, rows = pyiceberg_rows(table, dsl)
            except Exception as e:
                # One filter PyIceberg cannot answer does not stop the rest:
                # `IS NULL` on a list or a map raises in 0.11.1, and DuckDB
                # covers it below.
                info.setdefault("pyiceberg_error", str(e).split("\n")[0])
                refused.append(k)
                continue
            with open(
                os.path.join(oracle_dir, "rows_pyiceberg_%d.json" % k), "w"
            ) as fh:
                json.dump(
                    {"filter": dsl, "columns": cols, "rows": rows},
                    fh, indent=1,
                )
                fh.write("\n")
            ok += 1
        info["pyiceberg_filters"] = ok
        if refused:
            info["pyiceberg_refused"] = refused

        try:
            cols, rows = duckdb_rows(table, meta)
            with open(os.path.join(oracle_dir, "rows_duckdb.json"), "w") as fh:
                json.dump(
                    {"filter": '["true"]', "columns": cols, "rows": rows},
                    fh, indent=1,
                )
                fh.write("\n")
            info["duckdb_rows"] = len(rows)
        except Exception as e:
            info["duckdb_error"] = str(e).split("\n")[0]

        # DuckDB, filter by filter, for the nested tables: a second and wholly
        # independent answer, and the only one where PyIceberg refuses.
        sql = NESTED_TABLE_SQL.get(name)
        if sql:
            n_sql = 0
            for k, where in enumerate(sql):
                try:
                    cols, rows = duckdb_rows(table, meta, where)
                except Exception as e:
                    info.setdefault(
                        "duckdb_filter_error", str(e).split("\n")[0]
                    )
                    continue
                with open(
                    os.path.join(oracle_dir, "rows_duckdb_%d.json" % k), "w"
                ) as fh:
                    json.dump(
                        {"filter": filters[k], "sql": where,
                         "columns": cols, "rows": rows},
                        fh, indent=1,
                    )
                    fh.write("\n")
                n_sql += 1
            info["duckdb_filters"] = n_sql

        projections = NESTED_TABLE_PROJECTIONS.get(name)
        if projections:
            n_proj = 0
            for k, fields in enumerate(projections):
                try:
                    cols, rows = pyiceberg_projection(table, fields)
                except Exception as e:
                    info.setdefault(
                        "projection_error", str(e).split("\n")[0]
                    )
                    continue
                with open(
                    os.path.join(oracle_dir, "rows_project_%d.json" % k), "w"
                ) as fh:
                    json.dump(
                        {"select": fields, "columns": cols, "rows": rows},
                        fh, indent=1,
                    )
                    fh.write("\n")
                n_proj += 1
            info["projections"] = n_proj
        summary[name] = info
        print("%-16s %s" % (name, json.dumps(info)))

    with open(os.path.join(fixtures, "rows_oracle_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
