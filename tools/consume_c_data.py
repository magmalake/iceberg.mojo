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
addresses to `pyarrow.Array._import_from_c`, reassembles a table, and compares
it — cell for cell, sorted by `id` — with what PyIceberg reads from the same
Iceberg table. pyarrow then calls our `release` callback when the arrays are
collected.

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
import pyarrow.compute as pc

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


def canon(array):
    """Values only, with the Arrow spelling normalised away.

    PyIceberg produces `large_string` and `large_list`; this reader produces
    the 32-bit forms, which hold the same values. Comparing `to_pylist()`
    sidesteps the spelling entirely — and a map arrives as a list of pairs
    from both sides.
    """
    return array.to_pylist()


def main():
    from pyiceberg.table import StaticTable

    checked = 0
    failures = []
    for table in TABLES:
        meta = current_metadata(table)
        want_table = StaticTable.from_metadata(meta).scan().to_arrow()
        n_cols = want_table.num_columns

        columns = []
        for col in range(n_cols):
            got, rc = export_column(meta, col)
            if got is None:
                failures.append("%s[%d]: exporter returned %d" % (table, col, rc))
                columns = None
                break
            if rc != n_cols:
                failures.append(
                    "%s: scan produced %d columns, PyIceberg %d"
                    % (table, rc, n_cols)
                )
            columns.append(got)
        if columns is None:
            continue

        got_table = pa.table(
            {want_table.column_names[i]: columns[i] for i in range(n_cols)}
        )
        # Both readers see the same files, but nothing promises the same order.
        got_sorted = got_table.sort_by([("id", "ascending")])
        want_sorted = want_table.sort_by([("id", "ascending")])
        if got_sorted.num_rows != want_sorted.num_rows:
            failures.append(
                "%s: %d rows imported, PyIceberg has %d"
                % (table, got_sorted.num_rows, want_sorted.num_rows)
            )
            continue
        for i, name in enumerate(want_table.column_names):
            g = canon(got_sorted.column(i).combine_chunks())
            w = canon(want_sorted.column(i).combine_chunks())
            if g != w:
                failures.append(
                    "%s.%s differs\n  got : %s\n  want: %s" % (table, name, g, w)
                )
                continue
            checked += 1
        del columns, got_table, got_sorted
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
