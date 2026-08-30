#!/usr/bin/env python3
"""Import a nested scan's Arrow C Data Interface export into pyarrow.

`tools/carrow_scan.mojo` builds to a shared library with one C entry point:

    int32_t ib_export_scan_column(const char* metadata_json,
                                  const char* fixtures_dir,
                                  int32_t col,
                                  void* array_out  /* 80 bytes */,
                                  void* schema_out /* 72 bytes */);

It copies the root `ArrowArray` and `ArrowSchema` into caller-owned storage,
which is exactly the "move" a C Data Interface consumer performs. This script
allocates that storage with ctypes, calls in once per column, hands the two
addresses to `pyarrow.Array._import_from_c`, and compares the reassembled
table — cell for cell, in `tools/oracle_rows.py`'s canonical encoding — with
the **checked-in row oracle** PyIceberg produced for the same table. pyarrow
then calls our `release` callback when the arrays are collected.

The oracle rather than a live PyIceberg read, because the fixture metadata
carries absolute paths into the warehouse it was generated in (see
tests/fixtures/PROVENANCE.md): the Mojo side rebases them onto the fixtures
directory, and PyIceberg cannot. Reading the metadata file for its *schema* is
fine — that touches no manifest.

The interesting columns are the nested ones: a struct is `+s` with children, a
list is `+l` with one child and an offsets buffer, and a map is `+m` whose
child is the two-field `entries` struct. None of that survives a wrong buffer
count or a wrong child pointer, so an import that produces equal values is a
strong statement about the export.

    python tools/consume_c_data.py build/libibcarrow.dylib [tests/fixtures]
"""

import ctypes
import gc
import json
import os
import sys

import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from oracle_rows import encode_rows  # noqa: E402

LIB = sys.argv[1] if len(sys.argv) > 1 else "build/libibcarrow.dylib"
FIXTURES = os.path.abspath(sys.argv[2] if len(sys.argv) > 2 else "tests/fixtures")

TABLES = ["nested_v2", "nested_evo_v2", "nested_part_v2", "unpartitioned"]

lib = ctypes.CDLL(LIB)
lib.ib_export_scan_column.restype = ctypes.c_int32
lib.ib_export_scan_column.argtypes = [
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_int32,
    ctypes.c_void_p,
    ctypes.c_void_p,
]


def current_metadata(table):
    with open(os.path.join(FIXTURES, "index.json")) as fh:
        index = json.load(fh)
    return os.path.join(
        FIXTURES, table, "metadata", index[table]["current_metadata"]
    )


def oracle(table):
    """The checked-in expected rows: PyIceberg's if it has any, else DuckDB's."""
    for name in ("rows_pyiceberg_0.json", "rows_duckdb.json"):
        path = os.path.join(FIXTURES, table, "oracle", name)
        if os.path.exists(path):
            with open(path) as fh:
                doc = json.load(fh)
            return doc["columns"], doc["rows"], name
    return None, None, None


def export_column(meta, col):
    arr_buf = (ctypes.c_char * 80)()
    sch_buf = (ctypes.c_char * 72)()
    rc = lib.ib_export_scan_column(
        meta.encode(),
        FIXTURES.encode(),
        col,
        ctypes.byref(arr_buf),
        ctypes.byref(sch_buf),
    )
    if rc < 0:
        return None, rc
    got = pa.Array._import_from_c(
        ctypes.addressof(arr_buf), ctypes.addressof(sch_buf)
    )
    return got, rc


def main():
    from pyiceberg.table import StaticTable

    checked = 0
    failures = []
    for table in TABLES:
        meta = current_metadata(table)
        # The metadata file alone: its schema, without touching a manifest.
        schema = StaticTable.from_metadata(meta).schema()
        names, want_rows, source = oracle(table)
        if names is None:
            failures.append("%s: no row oracle" % table)
            continue
        types = {f.name: f.field_type for f in schema.fields}

        columns = []
        for col in range(len(names)):
            got, rc = export_column(meta, col)
            if got is None:
                failures.append("%s[%d]: exporter returned %d" % (table, col, rc))
                columns = None
                break
            if rc != len(names):
                failures.append(
                    "%s: scan produced %d columns, the oracle has %d"
                    % (table, rc, len(names))
                )
            columns.append(got)
        if columns is None:
            continue

        got_table = pa.table({names[i]: columns[i] for i in range(len(names))})
        got_rows = encode_rows(
            names, [types[n] for n in names], got_table.to_pylist()
        )
        if got_rows != want_rows:
            n = min(len(got_rows), len(want_rows))
            first = next(
                (i for i in range(n) if got_rows[i] != want_rows[i]), n
            )
            failures.append(
                "%s differs from %s at row %d\n  got : %s\n  want: %s"
                % (
                    table,
                    source,
                    first,
                    got_rows[first] if first < len(got_rows) else "<missing>",
                    want_rows[first] if first < len(want_rows) else "<missing>",
                )
            )
        else:
            checked += len(names)
        del columns, got_table
        gc.collect()

    print(
        "%d column(s) imported from Mojo into pyarrow and compared over %d "
        "tables" % (checked, len(TABLES))
    )
    for f in failures:
        print("FAIL", f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
