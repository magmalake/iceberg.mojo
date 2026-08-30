#!/usr/bin/env python3
"""Regenerate tests/fixtures/index.json.

A fixture table's `metadata/` directory can hold files from more than one table
lifetime (drop-and-recreate leaves the old `00000-<uuid>.metadata.json` behind),
so "which file is current" is a question only the catalog can answer. This
writes that answer down, once, so the tests do not have to re-derive it.

Usage: make_index.py <fixtures-dir> [--catalog <catalog.db>]
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from oracle_pyiceberg import catalog_metadata_basenames, find_metadata  # noqa


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    fixtures = argv[1]
    catalog_db = None
    if "--catalog" in argv:
        catalog_db = argv[argv.index("--catalog") + 1]
    hints = catalog_metadata_basenames(catalog_db) if catalog_db else {}

    index = {}
    for name in sorted(os.listdir(fixtures)):
        mdir = os.path.join(fixtures, name, "metadata")
        if not os.path.isdir(mdir):
            continue
        current = find_metadata(
            os.path.join(fixtures, name), hints.get(name)
        )
        with open(current) as fh:
            doc = json.load(fh)
        index[name] = {
            "current_metadata": os.path.basename(current),
            "current_snapshot_id": doc.get("current-snapshot-id"),
            "format_version": doc.get("format-version"),
            "metadata_files": sorted(
                f for f in os.listdir(mdir) if f.endswith(".metadata.json")
            ),
            "snapshots": len(doc.get("snapshots", [])),
            "table_uuid": doc.get("table-uuid"),
        }
    with open(os.path.join(fixtures, "index.json"), "w") as fh:
        json.dump(index, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print("index.json: %d tables" % len(index))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
