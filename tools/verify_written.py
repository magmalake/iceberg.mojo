#!/usr/bin/env python3
"""Verify the tables `tools/write_tables.mojo` wrote — with somebody else's reader.

Every table this library writes has to be readable by PyIceberg 0.11.1 and by
DuckDB 1.5.5, and has to say the same thing about itself either way. For each
of the ten tables (five partitioning shapes x format versions 2 and 3) this
checks:

  * **rows** — PyIceberg's `to_arrow()` and DuckDB's `iceberg_scan()` against
    the rows the generator is known to have written, cell for cell;
  * **snapshots** — three of them, chained by parent, with `operation=append`
    and the standard `added-*` / `total-*` summary keys adding up;
  * **files** — `inspect.files()`: record counts, partition values, column
    sizes, value counts, null counts and the lower/upper bounds we wrote;
  * **manifests** — `inspect.manifests()`: one added manifest per snapshot,
    with the partition summaries;
  * **row lineage (v3)** — `first_row_id` on every data file, assigned in
    manifest order with no overlap, and `next-row-id` == the total. PyIceberg
    0.11.1 cannot check this one: it reads manifest lists and manifests with
    `MANIFEST_LIST_FILE_SCHEMAS[DEFAULT_READ_VERSION]`, and
    `DEFAULT_READ_VERSION` is 2, so field 520 (`first_row_id`) and the v3
    `data_file` fields 142-145 are dropped on the way in — the read-side twin
    of the write-side bug this repo's fixtures already documented. **fastavro**
    reads them without a schema of its own, so it is the oracle for the Avro
    layer: file metadata keys, entry statuses, inherited sequence numbers and
    the row-id ranges.

Finally it does the interop in the other direction: PyIceberg *appends* to a
table we created, which the Mojo reader then has to read.

Usage:  verify_written.py <warehouse-dir> [--append-with-pyiceberg]
"""
import json
import math
import os
import sys

REGIONS = ["eu", "us", "apac", "latam", "emea"]
DAY_2024_01_01 = 19723
MICROS_PER_DAY = 86_400_000_000


def expected_rows(n=18):
    out = {}
    for i in range(n):
        out[i] = (
            REGIONS[i % 5],
            None if i % 4 == 0 else i * 1.5,
            (DAY_2024_01_01 + i % 3) * MICROS_PER_DAY + i * 1000,
            None if i % 5 == 0 else (i % 2 == 0),
        )
    return out


def _micros(v):
    if v is None:
        return None
    if isinstance(v, int):
        return v
    # a datetime, from either reader
    import datetime as dt

    epoch = dt.datetime(1970, 1, 1, tzinfo=v.tzinfo)
    return int((v - epoch).total_seconds() * 1_000_000) + 0


def _close(a, b):
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-9)


class Failure(Exception):
    pass


def check(cond, message):
    if not cond:
        raise Failure(message)


def latest_metadata(table_dir):
    md = os.path.join(table_dir, "metadata")
    best = None
    for name in os.listdir(md):
        if not name.endswith(".metadata.json"):
            continue
        v = int(name.split("-")[0])
        if best is None or v > best[0]:
            best = (v, name)
    check(best is not None, "no metadata.json under %s" % md)
    return os.path.join(md, best[1])


def pyiceberg_rows(table):
    tbl = table.scan().to_arrow()
    rows = {}
    for batch in tbl.to_pylist():
        rows[batch["id"]] = (
            batch["region"],
            batch["amount"],
            _micros(batch["ts"]),
            batch["ok"],
        )
    return rows


def duckdb_rows(con, metadata_path):
    """DuckDB is pointed at the metadata file itself.

    Its directory mode guesses `v<N>.metadata.json` from `version-hint.text`,
    which is Java's `HadoopTableOperations` spelling; every catalog-backed
    writer — this one included — uses `<V>-<uuid>.metadata.json` instead, and
    the two conventions do not mix. Naming the file is unambiguous and is what
    `tools/oracle_rows.py` does for the read fixtures too.
    """
    cur = con.execute(
        "SELECT id, region, amount, epoch_us(ts), ok FROM iceberg_scan(?)",
        [metadata_path],
    )
    rows = {}
    for r in cur.fetchall():
        rows[r[0]] = (r[1], r[2], r[3], r[4])
    return rows


def _as_dict(v):
    """A pyarrow map column comes back as a list of pairs, a struct as a dict.
    """
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    return dict(v)


def compare(label, got, want):
    check(
        set(got) == set(want),
        "%s: ids %s, expected %s" % (label, sorted(got), sorted(want)),
    )
    for i in sorted(want):
        g, w = got[i], want[i]
        check(g[0] == w[0], "%s row %d region %r != %r" % (label, i, g[0], w[0]))
        check(_close(g[1], w[1]), "%s row %d amount %r != %r" % (label, i, g[1], w[1]))
        check(g[2] == w[2], "%s row %d ts %r != %r" % (label, i, g[2], w[2]))
        check(g[3] == w[3], "%s row %d ok %r != %r" % (label, i, g[3], w[3]))


def verify_table(name, table_dir, con, want):
    from pyiceberg.table import StaticTable

    meta = latest_metadata(table_dir)
    t = StaticTable.from_metadata("file://" + meta)
    facts = {"name": name, "format-version": t.metadata.format_version}

    # ── rows ───────────────────────────────────────────────────────────────
    compare("%s/pyiceberg" % name, pyiceberg_rows(t), want)
    facts["pyiceberg_rows"] = len(want)
    if con is not None:
        try:
            compare("%s/duckdb" % name, duckdb_rows(con, meta), want)
            facts["duckdb_rows"] = len(want)
        except Failure:
            raise
        except Exception as e:
            facts["duckdb_error"] = str(e).split("\n")[0]

    # ── snapshots ──────────────────────────────────────────────────────────
    snaps = t.inspect.snapshots().to_pylist()
    check(len(snaps) == 3, "%s: %d snapshots, expected 3" % (name, len(snaps)))
    total_records = 0
    parent = None
    for s in sorted(snaps, key=lambda s: s["committed_at"]):
        check(
            s["operation"] == "append",
            "%s: operation %r" % (name, s["operation"]),
        )
        summary = _as_dict(s["summary"])
        total_records += int(summary["added-records"])
        check(
            int(summary["total-records"]) == total_records,
            "%s: total-records %s != %d"
            % (name, summary["total-records"], total_records),
        )
        check(
            int(summary["added-data-files"]) >= 1,
            "%s: added-data-files %s" % (name, summary["added-data-files"]),
        )
        check(
            int(summary["added-files-size"]) > 0,
            "%s: added-files-size %s" % (name, summary["added-files-size"]),
        )
        check(
            s["parent_id"] == parent,
            "%s: parent_id %r != %r" % (name, s["parent_id"], parent),
        )
        parent = s["snapshot_id"]
    facts["snapshots"] = len(snaps)
    facts["total_records"] = total_records

    # ── files ──────────────────────────────────────────────────────────────
    files = t.inspect.files().to_pylist()
    check(
        sum(f["record_count"] for f in files) == len(want),
        "%s: file record counts sum to %d, expected %d"
        % (name, sum(f["record_count"] for f in files), len(want)),
    )
    for f in files:
        sizes = _as_dict(f["column_sizes"])
        counts = _as_dict(f["value_counts"])
        nulls = _as_dict(f["null_value_counts"])
        lowers = _as_dict(f["lower_bounds"])
        uppers = _as_dict(f["upper_bounds"])
        check(f["file_format"] == "PARQUET", "%s: file_format" % name)
        check(f["file_size_in_bytes"] > 0, "%s: file_size_in_bytes" % name)
        check(bool(sizes), "%s: no column_sizes" % name)
        check(bool(counts), "%s: no value_counts" % name)
        check(bool(nulls), "%s: no null_value_counts" % name)
        check(bool(lowers), "%s: no lower_bounds" % name)
        check(bool(uppers), "%s: no upper_bounds" % name)
        check(bool(f["split_offsets"]), "%s: no split_offsets" % name)
        # `id` is column 1 and never null; its bounds must bracket the rows.
        lo = int.from_bytes(lowers[1], "little", signed=True)
        hi = int.from_bytes(uppers[1], "little", signed=True)
        check(lo <= hi, "%s: id bounds inverted %d..%d" % (name, lo, hi))
        # `region` is column 2: its bounds are the raw UTF-8 of the extremes.
        rlo = lowers[2].decode()
        rhi = uppers[2].decode()
        check(rlo <= rhi, "%s: region bounds inverted %r..%r" % (name, rlo, rhi))
        check(
            counts[1] == f["record_count"],
            "%s: value_counts[id] != record_count" % name,
        )
        check(nulls[1] == 0, "%s: null_value_counts[id] != 0" % name)
        check(
            3 in nulls and nulls[3] <= f["record_count"],
            "%s: null_value_counts[amount] missing or impossible" % name,
        )
    facts["data_files"] = len(files)
    facts["partitions"] = sorted(
        {json.dumps(f["partition"], default=str) for f in files}
    )

    # ── manifests ──────────────────────────────────────────────────────────
    manifests = t.inspect.manifests().to_pylist()
    check(
        len(manifests) == 3,
        "%s: %d manifests in the current snapshot, expected 3"
        % (name, len(manifests)),
    )
    check(
        sum(m["added_data_files_count"] for m in manifests) == len(files),
        "%s: manifest added counts do not sum to the file count" % name,
    )
    facts["manifests"] = len(manifests)

    # ── row lineage ────────────────────────────────────────────────────────
    doc = json.load(open(meta))
    if t.metadata.format_version >= 3:
        check("next-row-id" in doc, "%s: v3 metadata has no next-row-id" % name)
        check(
            doc["next-row-id"] == len(want),
            "%s: next-row-id %s != %d"
            % (name, doc["next-row-id"], len(want)),
        )
        for s in doc["snapshots"]:
            check("first-row-id" in s, "%s: snapshot has no first-row-id" % name)
            check("added-rows" in s, "%s: snapshot has no added-rows" % name)
        firsts = sorted(s["first-row-id"] for s in doc["snapshots"])
        adds = {s["first-row-id"]: s["added-rows"] for s in doc["snapshots"]}
        at = 0
        for f in firsts:
            check(
                f == at,
                "%s: first-row-id %d does not follow the previous range (%d)"
                % (name, f, at),
            )
            at += adds[f]
        check(at == doc["next-row-id"], "%s: row ranges do not reach next-row-id" % name)
        facts["next_row_id"] = doc["next-row-id"]
        facts["row_lineage_oracle"] = "fastavro (PyIceberg drops field 520)"
    else:
        check(
            "next-row-id" not in doc,
            "%s: a v2 table must not carry next-row-id" % name,
        )
    facts.update(avro_check(name, table_dir, meta, doc))
    return facts


def avro_check(name, table_dir, meta, doc):
    """Read the manifest list and the manifests with fastavro, not PyIceberg.

    This is the only reader in the loop that decodes the Avro with the schema
    the *file* carries rather than one of its own, which is what makes it able
    to see v3's `first_row_id`.
    """
    import fastavro

    version = doc["format-version"]
    snap = [s for s in doc["snapshots"]
            if s["snapshot-id"] == doc["current-snapshot-id"]][0]
    path = snap["manifest-list"].replace("file://", "")
    with open(path, "rb") as fh:
        reader = fastavro.reader(fh)
        md = {k: v for k, v in reader.metadata.items() if k != "avro.schema"}
        entries = list(reader)
    check(
        md.get("snapshot-id") == str(snap["snapshot-id"]),
        "%s: manifest list snapshot-id %r" % (name, md.get("snapshot-id")),
    )
    check(
        md.get("format-version") == str(version),
        "%s: manifest list format-version %r" % (name, md.get("format-version")),
    )
    check(
        md.get("sequence-number") == str(snap["sequence-number"]),
        "%s: manifest list sequence-number %r" % (name, md.get("sequence-number")),
    )
    want_parent = str(snap.get("parent-snapshot-id", "null"))
    check(
        md.get("parent-snapshot-id") == want_parent,
        "%s: manifest list parent-snapshot-id %r != %r"
        % (name, md.get("parent-snapshot-id"), want_parent),
    )
    check(len(entries) == 3, "%s: %d manifest_file entries" % (name, len(entries)))

    total_rows = 0
    ranges = []
    for e in entries:
        check(e["content"] == 0, "%s: manifest content %r" % (name, e["content"]))
        check(
            e["added_files_count"] >= 1 and e["existing_files_count"] == 0
            and e["deleted_files_count"] == 0,
            "%s: manifest counts %r" % (name, e),
        )
        check(
            e["min_sequence_number"] == e["sequence_number"],
            "%s: min_sequence_number != sequence_number for an all-added manifest"
            % name,
        )
        total_rows += e["added_rows_count"]
        if version >= 3:
            check(
                e.get("first_row_id") is not None,
                "%s: a v3 manifest_file must carry first_row_id" % name,
            )
            ranges.append((e["first_row_id"], e["added_rows_count"]))
        else:
            check(
                e.get("first_row_id") is None,
                "%s: a v2 manifest_file must not carry first_row_id" % name,
            )
        # And the manifest it names.
        mpath = e["manifest_path"].replace("file://", "")
        with open(mpath, "rb") as fh:
            mr = fastavro.reader(fh)
            mmd = {k: v for k, v in mr.metadata.items() if k != "avro.schema"}
            mentries = list(mr)
        for key in ("schema", "schema-id", "partition-spec",
                    "partition-spec-id", "format-version", "content"):
            check(key in mmd, "%s: manifest metadata has no %r" % (name, key))
        check(
            mmd["content"] == "data" and mmd["format-version"] == str(version),
            "%s: manifest metadata %r" % (name, mmd),
        )
        json.loads(mmd["schema"])
        json.loads(mmd["partition-spec"])
        check(
            len(mentries) == e["added_files_count"],
            "%s: %d entries for added_files_count %d"
            % (name, len(mentries), e["added_files_count"]),
        )
        for me in mentries:
            check(me["status"] == 1, "%s: entry status %r" % (name, me["status"]))
            check(
                me["snapshot_id"] is not None,
                "%s: an ADDED entry names its snapshot" % name,
            )
            check(
                me["sequence_number"] is None
                and me["file_sequence_number"] is None,
                "%s: an ADDED entry inherits its sequence numbers (they must"
                " be null)" % name,
            )
            df = me["data_file"]
            check(df["content"] == 0, "%s: data_file content" % name)
            check(df["file_format"] == "PARQUET", "%s: file_format" % name)
            check(df["equality_ids"] is None, "%s: equality_ids" % name)
            check(df["sort_order_id"] == 0, "%s: sort_order_id" % name)
            if version >= 3:
                check(
                    df["first_row_id"] is None,
                    "%s: a data file's first_row_id is inherited, so it must"
                    " be null" % name,
                )
                check(df["referenced_data_file"] is None, "%s: referenced" % name)
            else:
                check(
                    "first_row_id" not in df,
                    "%s: a v2 data_file has no first_row_id field" % name,
                )

    check(
        total_rows == 18,
        "%s: manifest list rows %d != 18" % (name, total_rows),
    )
    if version >= 3:
        at = 0
        for first, n in sorted(ranges):
            check(
                first == at,
                "%s: manifest first_row_id %d does not follow %d"
                % (name, first, at),
            )
            at += n
        check(
            at == doc["next-row-id"],
            "%s: manifest row-id ranges reach %d, next-row-id is %s"
            % (name, at, doc["next-row-id"]),
        )
    return {"manifest_list_entries": len(entries), "avro_rows": total_rows}


def pyiceberg_append(table_dir, start, n):
    """Append `n` rows through PyIceberg to a table we created."""
    import datetime as dt
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = os.path.dirname(os.path.dirname(table_dir))
    catalog = SqlCatalog(
        "interop",
        **{
            "uri": "sqlite:///%s/interop.db" % warehouse,
            "warehouse": "file://%s" % warehouse,
        },
    )
    try:
        catalog.create_namespace("db")
    except Exception:
        pass
    name = os.path.basename(table_dir)
    ident = "db.%s" % name
    try:
        catalog.drop_table(ident)
    except Exception:
        pass
    t = catalog.register_table(ident, "file://" + latest_metadata(table_dir))
    rows = expected_rows(start + n)
    ids = list(range(start, start + n))
    t.append(
        pa.Table.from_pydict(
            {
                "id": ids,
                "region": [rows[i][0] for i in ids],
                "amount": [rows[i][1] for i in ids],
                "ts": [
                    dt.datetime(1970, 1, 1)
                    + dt.timedelta(microseconds=rows[i][2])
                    for i in ids
                ],
                "ok": [rows[i][3] for i in ids],
            },
            schema=t.schema().as_arrow(),
        )
    )
    t = catalog.load_table(ident)
    return t.metadata_location.replace("file://", "")


def main():
    warehouse = os.path.abspath(sys.argv[1])
    do_append = "--append-with-pyiceberg" in sys.argv
    db = os.path.join(warehouse, "db")
    try:
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")
    except Exception as e:
        print("duckdb unavailable (%s); PyIceberg only" % str(e).split("\n")[0])
        con = None

    want = expected_rows()
    report = []
    failures = []
    for name in sorted(os.listdir(db)):
        table_dir = os.path.join(db, name)
        if not os.path.isdir(os.path.join(table_dir, "metadata")):
            continue
        try:
            report.append(verify_table(name, table_dir, con, want))
            print("ok   %s" % name)
        except Failure as e:
            failures.append(str(e))
            print("FAIL %s: %s" % (name, e))
        except Exception as e:
            failures.append("%s: %s" % (name, e))
            print("FAIL %s: %s: %s" % (name, type(e).__name__, e))

    if do_append and not failures:
        for name in ("unpartitioned_v2", "ident_v2"):
            table_dir = os.path.join(db, name)
            try:
                meta = pyiceberg_append(table_dir, 18, 6)
                print("ok   pyiceberg appended to %s -> %s" % (name, meta))
                report.append({"name": name, "pyiceberg_append": meta})
            except Exception as e:
                failures.append("pyiceberg append to %s: %s" % (name, e))
                print("FAIL pyiceberg append to %s: %s" % (name, e))

    with open(os.path.join(warehouse, "verify_report.json"), "w") as fh:
        json.dump({"tables": report, "failures": failures}, fh, indent=1)
    print(
        "\n%d tables verified, %d failures" % (len(report), len(failures))
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
