#!/usr/bin/env python3
"""PyIceberg oracle for the pure-Mojo Iceberg reader, driven by the
iceberg-rs.mojo filter DSL.

Two output shapes are produced:

``plan`` (PyIceberg-shaped, location independent, easy to diff)
    {"filter": "<dsl>",
     "tasks": [{"data_file": "<basename>", "record_count": N,
                "delete_files": ["<basename>", ...]}, ...]}
    ``tasks`` sorted by ``data_file``, ``delete_files`` sorted.

``bridge-plan`` / ``snapshots`` (mirrors what the iceberg-rs.mojo Rust bridge
emits, so the two oracles can be diffed field for field)
    plan:      [{"data-file-path", "deletes", "file-format",
                 "file-size-in-bytes", "length", "project-field-ids",
                 "record-count", "start"}, ...]
    snapshots: [{"manifest-list", "operation", "parent-snapshot-id",
                 "schema-id", "sequence-number", "snapshot-id", "summary",
                 "timestamp-ms"}, ...]  oldest first

The table is opened with ``StaticTable.from_metadata`` (PyIceberg 0.11.1 has no
``from_metadata_file``; ``from_metadata`` is the equivalent and takes the
metadata file location).  No catalog is required to *read* a table, but the
sqlite catalog is used, when available, to work out WHICH ``*.metadata.json``
is the current one -- some fixture ``metadata/`` dirs carry leftovers from
earlier runs, so "highest filename" is not reliable.

The fixture ``metadata.json`` files carry ABSOLUTE ``file://`` paths into the
original warehouse (see tests/fixtures/PROVENANCE.md), so the warehouse must
still be present for manifests and data files to resolve.

Usage:
    oracle_pyiceberg.py <metadata.json|table-dir> '<filter-json>'
    oracle_pyiceberg.py plan        <metadata.json|table-dir> '<filter-json>'
    oracle_pyiceberg.py bridge-plan <metadata.json|table-dir> '<filter-json>'
    oracle_pyiceberg.py snapshots   <metadata.json|table-dir>
    oracle_pyiceberg.py all <fixtures-dir> [--catalog <catalog.db>]

Supported operators (identical to the bridge's DSL):
    = != < <= > >=  is-null not-null  is-nan not-nan
    starts-with  in  not-in  and  or  not  true  false
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys

from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    NotStartsWith,
    Or,
    StartsWith,
)
from pyiceberg.table import StaticTable

# --------------------------------------------------------------- filter DSL --

_BINARY = {
    "=": EqualTo,
    "==": EqualTo,
    "!=": NotEqualTo,
    "<": LessThan,
    "<=": LessThanOrEqual,
    ">": GreaterThan,
    ">=": GreaterThanOrEqual,
    "starts-with": StartsWith,
    "not-starts-with": NotStartsWith,
}

_UNARY = {
    "is-null": IsNull,
    "not-null": NotNull,
    "is-nan": IsNaN,
    "not-nan": NotNaN,
}


def parse_filter(node):
    """Translate one JSON S-expression node into a BooleanExpression."""
    if node is None:
        return AlwaysTrue()
    if isinstance(node, str):
        node = json.loads(node)
    if node == []:
        return AlwaysTrue()
    if not isinstance(node, list):
        raise ValueError("filter node must be a JSON array, got %r" % (node,))

    op = node[0]
    if not isinstance(op, str):
        raise ValueError("filter operator must be a string, got %r" % (op,))
    op = op.lower()

    if op == "true":
        return AlwaysTrue()
    if op == "false":
        return AlwaysFalse()
    if op in _UNARY:
        return _UNARY[op](node[1])
    if op == "in":
        return In(node[1], node[2])
    if op == "not-in":
        return NotIn(node[1], node[2])
    if op in _BINARY:
        return _BINARY[op](node[1], node[2])
    if op == "not":
        return Not(parse_filter(node[1]))
    if op in ("and", "or"):
        parts = [parse_filter(child) for child in node[1:]]
        if not parts:
            raise ValueError("'%s' needs at least one operand" % op)
        combine = And if op == "and" else Or
        expr = parts[0]
        for part in parts[1:]:
            expr = combine(expr, part)
        return expr
    raise ValueError("unknown filter operator: %r" % (op,))


# ------------------------------------------------------- metadata discovery --


def _strip_scheme(loc: str) -> str:
    return loc[len("file://") :] if loc.startswith("file://") else loc


def catalog_metadata_basenames(catalog_db: str) -> dict:
    """{table_name: basename of its current *.metadata.json} from the catalog."""
    out = {}
    if not catalog_db or not os.path.exists(catalog_db):
        return out
    con = sqlite3.connect(catalog_db)
    try:
        rows = con.execute(
            "select table_name, metadata_location from iceberg_tables"
        ).fetchall()
    finally:
        con.close()
    for name, loc in rows:
        if loc:
            out[name] = os.path.basename(_strip_scheme(loc))
    return out


def find_metadata(path: str, basename_hint: str | None = None) -> str:
    """Accept a *.metadata.json, a table dir, or a table dir's metadata/ dir."""
    if path.endswith(".metadata.json"):
        return path
    for candidate in (os.path.join(path, "metadata"), path):
        if not os.path.isdir(candidate):
            continue
        if basename_hint:
            hinted = os.path.join(candidate, basename_hint)
            if os.path.exists(hinted):
                return hinted
        jsons = sorted(
            f for f in os.listdir(candidate) if f.endswith(".metadata.json")
        )
        if jsons:
            # Highest version prefix wins; ties broken by mtime.  Only a
            # heuristic -- pass a basename_hint (from the catalog) when the
            # directory may hold leftovers from earlier runs.
            best = max(
                jsons,
                key=lambda f: (
                    f.split("-", 1)[0],
                    os.path.getmtime(os.path.join(candidate, f)),
                ),
            )
            return os.path.join(candidate, best)
    raise SystemExit("no *.metadata.json found under %s" % path)


def open_table(metadata_location: str):
    return StaticTable.from_metadata(metadata_location)


# ------------------------------------------------------------------- plans --


def plan(table, filter_json):
    """PyIceberg-shaped, basename-only plan."""
    dsl = (
        filter_json
        if isinstance(filter_json, str)
        else json.dumps(filter_json, separators=(",", ":"))
    )
    scan = table.scan(row_filter=parse_filter(filter_json))
    tasks = []
    for task in scan.plan_files():
        tasks.append(
            {
                "data_file": os.path.basename(task.file.file_path),
                "record_count": task.file.record_count,
                "delete_files": sorted(
                    os.path.basename(d.file_path) for d in task.delete_files
                ),
            }
        )
    tasks.sort(key=lambda r: r["data_file"])
    return {"filter": dsl, "tasks": tasks}


def bridge_plan(table, filter_json):
    """Same shape the Rust bridge's scan() emits (absolute paths, sorted keys).

    ``start``/``length`` and ``project-field-ids`` are not carried on
    PyIceberg's FileScanTask; PyIceberg always plans whole files, so start=0
    and length=file_size_in_bytes, and the projected field ids are taken from
    the table's current schema (the scan selects ``*``).
    """
    field_ids = [f.field_id for f in table.schema().fields]
    scan = table.scan(row_filter=parse_filter(filter_json))
    out = []
    for task in scan.plan_files():
        f = task.file
        fmt = getattr(f.file_format, "name", str(f.file_format)).lower()
        out.append(
            {
                "data-file-path": f.file_path,
                "deletes": sorted(d.file_path for d in task.delete_files),
                "file-format": fmt,
                "file-size-in-bytes": f.file_size_in_bytes,
                "length": f.file_size_in_bytes,
                "project-field-ids": field_ids,
                "record-count": f.record_count,
                "start": 0,
            }
        )
    out.sort(key=lambda r: r["data-file-path"])
    return out


def snapshots(table):
    """Same shape the Rust bridge's snapshots() emits, oldest first."""
    out = []
    for snap in sorted(
        table.metadata.snapshots, key=lambda s: (s.sequence_number or 0)
    ):
        summary = {
            k: v
            for k, v in snap.summary.model_dump().items()
            if k != "operation"
        }
        out.append(
            {
                "manifest-list": snap.manifest_list,
                "operation": str(snap.summary.operation.value),
                "parent-snapshot-id": snap.parent_snapshot_id,
                "schema-id": snap.schema_id,
                "sequence-number": snap.sequence_number,
                "snapshot-id": snap.snapshot_id,
                "summary": summary,
                "timestamp-ms": snap.timestamp_ms,
            }
        )
    return out


# --------------------------------------------------------------- batch mode --

BRIDGE_TABLES = (
    "unpartitioned",
    "ident_part",
    "bucket_part",
    "day_part",
    "trunc_part",
)

# Filters for the two PyIceberg-built tables.  The five bridge tables reuse the
# filters the bridge already wrote to oracle/plan_<k>.filter.txt.
PYICEBERG_TABLE_FILTERS = {
    "evolved": [
        '["true"]',
        '["=","label","alpha"]',
        '[">","id",3]',
        '["and",[">=","id",2],["is-null","amount"]]',
        '["in","label",["alpha","delta","zeta"]]',
        '["not-null","extra"]',
    ],
    "deletes_v2": [
        '["true"]',
        '["=","region","eu"]',
        '[">","id",3]',
        '["and",["=","region","us"],["not-null","amount"]]',
        '["in","region",["eu","apac"]]',
        '["is-null","amount"]',
    ],
}

ALL_TABLES = BRIDGE_TABLES + tuple(PYICEBERG_TABLE_FILTERS)


def _compact(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def run_all(fixtures_dir: str, catalog_db: str | None) -> int:
    hints = catalog_metadata_basenames(catalog_db) if catalog_db else {}
    for name in ALL_TABLES:
        tdir = os.path.join(fixtures_dir, name)
        if not os.path.isdir(tdir):
            print("skip %s: no such fixture dir" % name, file=sys.stderr)
            continue
        oracle_dir = os.path.join(tdir, "oracle")
        os.makedirs(oracle_dir, exist_ok=True)
        meta = find_metadata(tdir, hints.get(name))
        table = open_table(meta)

        if name in PYICEBERG_TABLE_FILTERS:
            # The bridge never saw these tables, so PyIceberg supplies the
            # bridge-shaped oracle too.
            shutil.copyfile(meta, os.path.join(oracle_dir, "metadata.json"))
            with open(os.path.join(oracle_dir, "snapshots.json"), "w") as fh:
                fh.write(_compact(snapshots(table)))
            filters = PYICEBERG_TABLE_FILTERS[name]
            for k, dsl in enumerate(filters):
                with open(
                    os.path.join(oracle_dir, "plan_%d.filter.txt" % k), "w"
                ) as fh:
                    fh.write(dsl + "\n")
                with open(
                    os.path.join(oracle_dir, "plan_%d.json" % k), "w"
                ) as fh:
                    fh.write(_compact(bridge_plan(table, dsl)))
        else:
            filters = []
            for k in range(6):
                fpath = os.path.join(oracle_dir, "plan_%d.filter.txt" % k)
                with open(fpath) as fh:
                    filters.append(fh.read().strip())

        for k, dsl in enumerate(filters):
            with open(
                os.path.join(oracle_dir, "pyiceberg_plan_%d.json" % k), "w"
            ) as fh:
                json.dump(plan(table, dsl), fh, indent=2, sort_keys=False)
                fh.write("\n")
        print(
            "%-14s %s  (%d filters)"
            % (name, os.path.basename(meta), len(filters))
        )
    return 0


# -------------------------------------------------------------------- main --


def main(argv):
    if len(argv) >= 2 and argv[1] == "all":
        if len(argv) < 3:
            print(__doc__, file=sys.stderr)
            return 2
        catalog_db = None
        if "--catalog" in argv:
            catalog_db = argv[argv.index("--catalog") + 1]
        return run_all(argv[2], catalog_db)

    if len(argv) >= 2 and argv[1] == "snapshots":
        table = open_table(find_metadata(argv[2]))
        print(json.dumps(snapshots(table), indent=2))
        return 0

    mode = "plan"
    args = argv[1:]
    if args and args[0] in ("plan", "bridge-plan"):
        mode = args[0]
        args = args[1:]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2

    table = open_table(find_metadata(args[0]))
    if mode == "bridge-plan":
        print(json.dumps(bridge_plan(table, args[1]), indent=2))
    else:
        print(json.dumps(plan(table, args[1]), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
