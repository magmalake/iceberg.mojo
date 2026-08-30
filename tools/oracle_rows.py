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
    # Anything else (struct/list/map) is JSON, which is enough to compare.
    return json.dumps(value, sort_keys=True, default=str)


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


def duckdb_rows(table, metadata_path):
    import duckdb

    schema = table.schema()
    names = [f.name for f in schema.fields]
    types = [f.field_type for f in schema.fields]
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    try:
        cur = con.execute("SELECT * FROM iceberg_scan(?)", [metadata_path])
        cols = [d[0] for d in cur.description]
        records = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()
    return names, encode_rows(names, types, records)


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    fixtures = argv[1]
    catalog_db = None
    if "--catalog" in argv:
        catalog_db = argv[argv.index("--catalog") + 1]
    hints = catalog_metadata_basenames(catalog_db) if catalog_db else {}

    names = sorted(
        d for d in os.listdir(fixtures)
        if os.path.isdir(os.path.join(fixtures, d, "metadata"))
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
        for k, dsl in enumerate(filters):
            try:
                cols, rows = pyiceberg_rows(table, dsl)
            except Exception as e:
                info.setdefault("pyiceberg_error", str(e).split("\n")[0])
                break
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
        summary[name] = info
        print("%-16s %s" % (name, json.dumps(info)))

    with open(os.path.join(fixtures, "rows_oracle_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
