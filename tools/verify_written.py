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


def row_for(i):
    return (
        REGIONS[i % 5],
        None if i % 4 == 0 else i * 1.5,
        (DAY_2024_01_01 + i % 3) * MICROS_PER_DAY + i * 1000,
        None if i % 5 == 0 else (i % 2 == 0),
    )


def expected_rows(n=18):
    return {i: row_for(i) for i in range(n)}


def rows_of(ids):
    return {i: row_for(i) for i in ids}


# The `del` namespace: what each table's one delete or overwrite leaves.
# `region = 'eu'` is ids 0, 5, 10, 15 — one row in each of three data files,
# and one whole partition of an identity-partitioned table.
EU = [0, 5, 10, 15]
KEPT = [i for i in range(18) if i not in EU]


# What `DELETE WHERE region = 'eu'` costs each partitioning shape. The four
# rows land differently in each: in one file of an unpartitioned table they
# are two rows of the first file and one each of the other two; under
# `identity(region)` they are *whole files*, so the delete never reads a data
# file and never writes a delete file, whatever the mode says; under
# `bucket[4]` one of the affected files holds nothing else and goes too.
MOR_SHAPES = {
    "unpartitioned": {
        "total_records": 18, "position_deletes": 4,
        "delete_files": {2: 1, 3: 3}, "dvs": 3,
    },
    "ident": {
        "total_records": 14, "position_deletes": 0,
        "delete_files": {2: 0, 3: 0}, "dvs": 0,
    },
    "bucket": {
        "total_records": 17, "position_deletes": 3,
        "delete_files": {2: 2, 3: 3}, "dvs": 3,
    },
    "day": {
        "total_records": 18, "position_deletes": 4,
        "delete_files": {2: 3, 3: 4}, "dvs": 4,
    },
}

# Copy-on-write removes every affected file and rewrites what survived, so the
# table always ends up holding exactly the fourteen rows that are left. Under
# `identity(region)` there is nothing to rewrite — the whole partition goes —
# so the snapshot adds no file and is a `delete`, not an `overwrite`.
COW_OPERATIONS = {
    "unpartitioned": "overwrite",
    "ident": "delete",
    "bucket": "overwrite",
    "day": "overwrite",
}


def delete_cases():
    cases = {}
    for version in (2, 3):
        for shape, mor in MOR_SHAPES.items():
            cases["mor_%s_v%d" % (shape, version)] = {
                "ids": KEPT,
                "mode": "merge-on-read",
                "operation": "delete",
                "version": version,
                "total_records": mor["total_records"],
                "position_deletes": mor["position_deletes"],
                "dvs": mor["dvs"] if version >= 3 else 0,
                "delete_files": mor["delete_files"][version],
            }
            cases["cow_%s_v%d" % (shape, version)] = {
                "ids": KEPT,
                "mode": "copy-on-write",
                "operation": COW_OPERATIONS[shape],
                "version": version,
                "total_records": 14,
                "position_deletes": 0,
                "dvs": 0,
                "delete_files": 0,
            }
    cases["mor_twice_v3"] = {
        "ids": [i for i in range(18) if i not in (0, 3)],
        "mode": "merge-on-read",
        "operation": "delete",
        "version": 3,
        "total_records": 18,
        "position_deletes": 2,
        "dvs": 1,
        "delete_files": 1,
        "snapshots": 5,
    }
    cases["ovw_all_v2"] = {
        "ids": [100, 101, 102],
        "mode": "copy-on-write",
        "operation": "overwrite",
        "version": 2,
        "total_records": 3,
        "position_deletes": 0,
        "dvs": 0,
        "delete_files": 0,
    }
    cases["ovw_filter_v3"] = {
        "ids": list(range(12)) + [200, 201],
        "mode": "copy-on-write",
        "operation": "overwrite",
        "version": 3,
        "total_records": 14,
        "position_deletes": 0,
        "dvs": 0,
        "delete_files": 0,
    }
    for version in (2, 3):
        cases["dyn_ident_v%d" % version] = {
            "ids": KEPT + [100],
            "mode": "copy-on-write",
            "operation": "overwrite",
            "version": version,
            "total_records": 15,
            "position_deletes": 0,
            "dvs": 0,
            "delete_files": 0,
        }
    return cases


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


# ── the `del` namespace: delete and overwrite ───────────────────────────────
def _read_manifests(doc):
    """Every (manifest_file, [entries]) of the current snapshot, via fastavro.

    fastavro reads with the schema each file carries, which is the only way to
    see v3's `first_row_id`, `referenced_data_file`, `content_offset` and
    `content_size_in_bytes` — PyIceberg 0.11.1 reads manifests with its v2
    schemas and drops all four.
    """
    import fastavro

    snap = [s for s in doc["snapshots"]
            if s["snapshot-id"] == doc["current-snapshot-id"]][0]
    with open(snap["manifest-list"].replace("file://", ""), "rb") as fh:
        manifests = list(fastavro.reader(fh))
    out = []
    for mf in manifests:
        with open(mf["manifest_path"].replace("file://", ""), "rb") as fh:
            reader = fastavro.reader(fh)
            meta = {k: v for k, v in reader.metadata.items()
                    if k != "avro.schema"}
            out.append((mf, meta, list(reader)))
    return snap, out


DV_MAGIC = bytes([0xD1, 0xD3, 0x39, 0x64])


def _dv_positions(name, path, referenced, offset, length):
    """Decode one of our `deletion-vector-v1` blobs with somebody else's code.

    Two independent checks on the same bytes. First the framing this library
    wrote is verified here, by hand: the big-endian length, the `D1 D3 39 64`
    magic, and a CRC-32 over both computed with `zlib`, not with hashes.mojo.
    Then `pyiceberg.table.puffin.PuffinFile` — which shares no line with
    roaring.mojo — parses the footer and deserialises the bitmap, and *its*
    positions are what the caller compares against the rows that vanished.
    """
    import zlib

    from pyiceberg.table.puffin import PuffinFile

    with open(path, "rb") as fh:
        raw = fh.read()
    blob = raw[offset:offset + length]
    check(
        len(blob) == length,
        "%s: the Puffin file is shorter than the manifest claims" % name,
    )
    declared = int.from_bytes(blob[:4], "big")
    check(
        declared == length - 8,
        "%s: deletion vector length field %d, blob is %d bytes"
        % (name, declared, length),
    )
    check(
        blob[4:8] == DV_MAGIC,
        "%s: deletion vector magic %r" % (name, blob[4:8]),
    )
    crc = int.from_bytes(blob[-4:], "big")
    check(
        crc == zlib.crc32(blob[4:-4]) & 0xFFFFFFFF,
        "%s: deletion vector CRC-32 mismatch" % name,
    )
    vectors = PuffinFile(raw)._deletion_vectors
    check(
        referenced in vectors,
        "%s: the footer has no vector for %s" % (name, referenced),
    )
    out = set()
    for bitmap in vectors[referenced]:
        out |= set(bitmap)
    return sorted(out)


def _parquet_rows(path, columns):
    import pyarrow.parquet as pq

    return pq.read_table(path.replace("file://", ""), columns=columns).to_pydict()


def verify_delete_table(name, table_dir, con, case):
    from pyiceberg.table import StaticTable

    meta = latest_metadata(table_dir)
    doc = json.load(open(meta))
    t = StaticTable.from_metadata("file://" + meta)
    want = rows_of(case["ids"])
    facts = {"name": "del." + name, "format-version": doc["format-version"],
             "mode": case["mode"], "rows": len(want)}
    check(
        doc["format-version"] == case["version"],
        "%s: format-version %s" % (name, doc["format-version"]),
    )

    # ── rows, cell for cell, by both readers ───────────────────────────────
    compare("del.%s/pyiceberg" % name, pyiceberg_rows(t), want)
    if con is not None:
        compare("del.%s/duckdb" % name, duckdb_rows(con, meta), want)
        facts["duckdb_rows"] = len(want)

    # ── the snapshot the delete or overwrite wrote ─────────────────────────
    snaps = t.inspect.snapshots().to_pylist()
    check(
        len(snaps) == case.get("snapshots", 4),
        "%s: %d snapshots" % (name, len(snaps)),
    )
    last = sorted(snaps, key=lambda s: s["committed_at"])[-1]
    summary = _as_dict(last["summary"])
    check(
        last["operation"] == case["operation"],
        "%s: operation %r != %r" % (name, last["operation"], case["operation"]),
    )
    for key in ("total-records", "total-data-files", "total-delete-files",
                "total-position-deletes", "total-equality-deletes"):
        check(key in summary, "%s: summary has no %s" % (name, key))
    check(
        int(summary["total-records"]) == case["total_records"],
        "%s: total-records %s != %d"
        % (name, summary["total-records"], case["total_records"]),
    )
    check(
        int(summary["total-position-deletes"]) == case["position_deletes"],
        "%s: total-position-deletes %s != %d"
        % (name, summary["total-position-deletes"], case["position_deletes"]),
    )
    check(
        int(summary["total-delete-files"]) == case["delete_files"],
        "%s: total-delete-files %s != %d"
        % (name, summary["total-delete-files"], case["delete_files"]),
    )
    if case["dvs"]:
        check(
            int(summary.get("added-dvs", 0)) > 0,
            "%s: a v3 merge-on-read delete must report added-dvs" % name,
        )
    facts["summary"] = {k: summary[k] for k in sorted(summary)}

    # ── the manifests, and what the delete files actually say ──────────────
    snap, manifests = _read_manifests(doc)
    data_files = 0
    data_records = 0
    delete_entries = []
    for mf, mmeta, entries in manifests:
        live = [e for e in entries if e["status"] != 2]
        check(live, "%s: a manifest with nothing live was kept" % name)
        for e in entries:
            df = e["data_file"]
            if e["status"] == 1:
                check(
                    e["sequence_number"] is None
                    and e["file_sequence_number"] is None,
                    "%s: an ADDED entry must inherit its sequence numbers"
                    % name,
                )
            else:
                check(
                    e["sequence_number"] is not None
                    and e["file_sequence_number"] is not None,
                    "%s: a %s entry must carry its sequence numbers; "
                    "inheritance is ADDED-only"
                    % (name, "DELETED" if e["status"] == 2 else "EXISTING"),
                )
                if doc["format-version"] >= 3 and df["content"] == 0:
                    check(
                        df["first_row_id"] is not None,
                        "%s: an EXISTING data file must carry first_row_id"
                        % name,
                    )
            if e["status"] == 2:
                continue
            if df["content"] == 0:
                data_files += 1
                data_records += df["record_count"]
            else:
                delete_entries.append(df)
    check(
        data_records == case["total_records"],
        "%s: live manifest rows %d != total-records %d"
        % (name, data_records, case["total_records"]),
    )
    check(
        data_files == int(summary["total-data-files"]),
        "%s: live data files %d != total-data-files %s"
        % (name, data_files, summary["total-data-files"]),
    )
    check(
        len(delete_entries) == case["delete_files"],
        "%s: %d live delete entries, expected %d"
        % (name, len(delete_entries), case["delete_files"]),
    )

    # ── the delete files themselves ────────────────────────────────────────
    deleted_ids = sorted(set(range(18)) - set(case["ids"]))
    positions_seen = 0
    for df in delete_entries:
        check(df["content"] == 1, "%s: delete file content != 1" % name)
        if case["dvs"]:
            check(
                df["file_format"] == "PUFFIN",
                "%s: v3 delete file format %r" % (name, df["file_format"]),
            )
            check(
                df["referenced_data_file"] is not None,
                "%s: a deletion vector needs referenced_data_file" % name,
            )
            got = _dv_positions(
                name,
                df["file_path"].replace("file://", ""),
                df["referenced_data_file"],
                df["content_offset"],
                df["content_size_in_bytes"],
            )
            check(
                len(got) == df["record_count"],
                "%s: vector holds %d positions, record_count says %d"
                % (name, len(got), df["record_count"]),
            )
            # Every position the vector names must be a row the filter hit.
            data = _parquet_rows(df["referenced_data_file"], ["id"])["id"]
            for p in got:
                check(
                    data[p] in deleted_ids or data[p] not in case["ids"],
                    "%s: vector deletes position %d (id %s), which survives"
                    % (name, p, data[p]),
                )
            positions_seen += len(got)
        else:
            check(
                df["file_format"] == "PARQUET",
                "%s: v2 delete file format %r" % (name, df["file_format"]),
            )
            rows = _parquet_rows(df["file_path"], ["file_path", "pos"])
            pairs = list(zip(rows["file_path"], rows["pos"]))
            check(
                pairs == sorted(pairs),
                "%s: a position delete file must be sorted by path then pos"
                % name,
            )
            check(
                len(pairs) == df["record_count"],
                "%s: position delete rows %d != record_count %d"
                % (name, len(pairs), df["record_count"]),
            )
            for path, pos in pairs:
                ids = _parquet_rows(path, ["id"])["id"]
                check(
                    ids[pos] not in case["ids"],
                    "%s: position delete removes id %s, which survives"
                    % (name, ids[pos]),
                )
            positions_seen += len(pairs)
    check(
        positions_seen == case["position_deletes"],
        "%s: %d delete positions, total-position-deletes says %d"
        % (name, positions_seen, case["position_deletes"]),
    )
    facts["delete_positions"] = positions_seen
    facts["delete_entries"] = len(delete_entries)

    # ── v3 row lineage survives a merge-on-read delete ─────────────────────
    if doc["format-version"] >= 3 and case["mode"] == "merge-on-read":
        check(
            doc["next-row-id"] == 18,
            "%s: a delete adds no rows, so next-row-id stays 18, not %s"
            % (name, doc["next-row-id"]),
        )
        facts["next_row_id"] = doc["next-row-id"]
    return facts


def pyiceberg_append(table_dir, start, n, namespace="db"):
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
        catalog.create_namespace(namespace)
    except Exception:
        pass
    name = os.path.basename(table_dir)
    ident = "%s.%s" % (namespace, name)
    try:
        catalog.drop_table(ident)
    except Exception:
        pass
    t = catalog.register_table(ident, "file://" + latest_metadata(table_dir))
    ids = list(range(start, start + n))
    rows = rows_of(ids)
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

    cases = delete_cases()
    delns = os.path.join(warehouse, "del")
    if os.path.isdir(delns):
        for name in sorted(os.listdir(delns)):
            table_dir = os.path.join(delns, name)
            if not os.path.isdir(os.path.join(table_dir, "metadata")):
                continue
            check_case = cases.get(name)
            if check_case is None:
                failures.append("del.%s: no expectation for this table" % name)
                print("FAIL del.%s: unexpected table" % name)
                continue
            try:
                report.append(
                    verify_delete_table(name, table_dir, con, check_case)
                )
                print("ok   del.%s" % name)
            except Failure as e:
                failures.append(str(e))
                print("FAIL del.%s: %s" % (name, e))
            except Exception as e:
                failures.append("del.%s: %s" % (name, e))
                print("FAIL del.%s: %s: %s" % (name, type(e).__name__, e))
        for name in sorted(cases):
            if not os.path.isdir(os.path.join(delns, name, "metadata")):
                failures.append("del.%s was never written" % name)
                print("FAIL del.%s: missing" % name)

    if do_append and not failures:
        # PyIceberg must still be able to *write* to a table we deleted from.
        for name in ("mor_unpartitioned_v2", "cow_unpartitioned_v2"):
            table_dir = os.path.join(delns, name)
            try:
                meta = pyiceberg_append(table_dir, 300, 3, namespace="del")
                print("ok   pyiceberg appended to del.%s -> %s" % (name, meta))
                report.append({"name": "del." + name, "pyiceberg_append": meta})
            except Exception as e:
                failures.append("pyiceberg append to del.%s: %s" % (name, e))
                print("FAIL pyiceberg append to del.%s: %s" % (name, e))
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
