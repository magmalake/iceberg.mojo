# Fixture provenance

Everything under `tests/fixtures/` is generated. `tools/make_fixtures.sh`
rebuilds the whole set from scratch; the fixtures are checked in, so that
script is only needed when you deliberately want to regenerate them (a new
iceberg-rust or PyIceberg release, a new table, a new filter). Regenerating
changes every UUID, snapshot id and absolute path in here.

## Tool versions

| Component | Version |
|---|---|
| iceberg-rust (the "bridge") | 0.10.1 — `iceberg`, `iceberg-catalog-sql`, `iceberg-storage-opendal`, pinned in `../iceberg-rs.mojo/ffi/Cargo.toml` |
| bridge front end | `iceberg-rs.mojo` (Mojo FFI over the Rust cdylib) |
| PyIceberg | **0.11.1** (`pyiceberg.__version__`) |
| PyArrow | 25.0.1 |
| Python | 3.12.13 (venv at `../iceberg-rs.mojo/build/pyiceberg-venv`) |
| Catalog | PyIceberg `SqlCatalog` over sqlite, shared by both writers |

## Original warehouse

```
warehouse root : /Users/mseritan/dev/magmalake/iceberg.mojo/build/warehouse-root/warehouse/db
sqlite catalog : /Users/mseritan/dev/magmalake/iceberg.mojo/build/warehouse-root/catalog.db
namespace      : db
```

All seven tables live in that one warehouse and one catalog, so the fixture set
is a single coherent Iceberg warehouse rather than seven unrelated ones.

### Absolute paths — Mojo tests must rewrite them

`tests/fixtures/<table>/metadata/*.metadata.json` are **verbatim copies** of the
warehouse metadata. They therefore contain absolute `file://` URLs pointing at
the warehouse root above — in `location`, in `metadata-log`, and in every
snapshot's `manifest-list`. The manifest Avro files and the Parquet data files
they in turn reference are likewise absolute. A Mojo test that reads a fixture
must path-rewrite that prefix to wherever the warehouse actually is (or copy the
data alongside). `tests/fixtures/WAREHOUSE_ROOT.txt` records the prefix that was
baked in.

The same is true of the bridge-shaped oracle files (`oracle/metadata.json`,
`oracle/snapshots.json`, `oracle/plan_<k>.json`) — they carry absolute paths
because the bridge emitted them that way. The PyIceberg-shaped oracle files
(`oracle/pyiceberg_plan_<k>.json`) use **basenames only** and are therefore
location independent.

Note also that a few `metadata/` directories carry leftover `*.metadata.json`
from earlier drop/recreate cycles (`evolved` and `deletes_v2` most of all).
"Highest filename wins" is *not* a safe way to find the current metadata; the
catalog is authoritative, and `oracle/metadata.json` is a copy of the current
one for every table. The current file per table is named below.

## Layout

```
tests/fixtures/
  transform_vectors.json          252 partition-transform vectors from PyIceberg
  WAREHOUSE_ROOT.txt              the absolute prefix baked into the metadata
  <table>/metadata/               verbatim copy of the warehouse metadata dir
  <table>/oracle/
      metadata.json               current table metadata
      snapshots.json              snapshot list, oldest first
      plan_<k>.filter.txt         filter k, in the bridge's JSON S-expression DSL
      plan_<k>.json               bridge-shaped scan plan for filter k
      pyiceberg_plan_<k>.json     PyIceberg scan plan for filter k (basenames)
  deletes_v2/oracle_delete_report.json
```

`pyiceberg_plan_<k>.json` shape:

```json
{"filter": "<the dsl string>",
 "tasks": [{"data_file": "<basename>", "record_count": 3, "delete_files": []}]}
```

sorted by `data_file`, with `delete_files` sorted — diffable as text.

## Oracles and how they were produced

* **Tables 1–5** (`unpartitioned`, `ident_part`, `bucket_part`, `day_part`,
  `trunc_part`) were written by `tools/make_fixtures.mojo` through the
  iceberg-rs.mojo bridge (iceberg-rust 0.10.1), three appends each. The bridge
  emitted `oracle/metadata.json`, `oracle/snapshots.json`, `oracle/plan_<k>.json`
  and `oracle/plan_<k>.filter.txt` directly from
  `Table.metadata_json()` / `snapshots_json()` / `scan(...).plan_files()`.
* **Tables 6–7** (`evolved`, `deletes_v2`) were written by
  `tools/make_pyiceberg_tables.py` with PyIceberg 0.11.1, because
  iceberg-rust 0.10.1 cannot do schema evolution or write position deletes. The
  Rust bridge never saw these two tables, so their bridge-shaped oracle files
  were synthesised from PyIceberg by `tools/oracle_pyiceberg.py` — see the
  caveats below.
* **All 7 tables** get `oracle/pyiceberg_plan_<k>.json` from
  `tools/oracle_pyiceberg.py`, which opens
  `StaticTable.from_metadata(<the fixture copy of the current metadata>)` and
  runs `table.scan(row_filter=...).plan_files()`.
* **`transform_vectors.json`** comes from `tools/gen_transform_vectors.py`,
  driving PyIceberg's `pyiceberg.transforms` directly (252 vectors).

### Caveats on the synthesised bridge-shaped oracle (evolved, deletes_v2)

`oracle/metadata.json` is a byte copy of the current `*.metadata.json`, and
`oracle/snapshots.json` is built from `table.metadata.snapshots` with the exact
key names the Rust bridge used (`manifest-list`, `operation`,
`parent-snapshot-id`, `schema-id`, `sequence-number`, `snapshot-id`, `summary`,
`timestamp-ms`; `operation` hoisted out of `summary`, as the bridge does).

`oracle/plan_<k>.json` uses the bridge's key set (`data-file-path`, `deletes`,
`file-format`, `file-size-in-bytes`, `length`, `project-field-ids`,
`record-count`, `start`), but two of those fields are **not** reachable from
PyIceberg's `FileScanTask` (its fields are only `file`, `delete_files`,
`residual`):

* `start` / `length` — PyIceberg always plans whole files, so these are filled
  in as `0` and `file_size_in_bytes`. That is what the bridge emits for these
  fixtures too (every bridge `plan_<k>.json` entry has `start: 0` and
  `length == file-size-in-bytes`), but it is derived, not observed.
* `project-field-ids` — taken from the current schema's field ids, since the
  scan selects `*`. PyIceberg carries no per-task projection.

One further difference: the synthesised `plan_<k>.json` entries are **sorted by
`data-file-path`**, while the bridge emits them in manifest order. Compare file
sets, not list order.

Nothing else is omitted.

## The `deletes_v2` position delete file — NOT stock PyIceberg output

Read `tests/fixtures/deletes_v2/oracle_delete_report.json` alongside this.

PyIceberg 0.11.1 cannot produce merge-on-read deletes. Even with
`write.delete.mode=merge-on-read` set on the table, `Table.delete(...)` warns

> `UserWarning: Merge on read is not yet supported, falling back to copy-on-write`

and rewrites the data files instead of leaving a delete file behind. Since the
whole point of this fixture is a v2 table with a *position delete*, the delete
file was **hand-written** by `tools/make_pyiceberg_tables.py`: a
`ManifestWriterV2` subclass that reports `ManifestContent.DELETES`, plus a
`_FastAppendFiles` subclass that routes the added file into that delete manifest
and keeps the parent snapshot's data manifests. The position-delete Parquet
itself is written with PyArrow using the spec's reserved field ids
(`file_path` = 2147483546, `pos` = 2147483545).

So: the data files, manifest lists, snapshot summaries and metadata commits are
stock PyIceberg; the delete manifest and the delete Parquet are ours. Do not
treat this table as evidence of what PyIceberg emits. It *is* valid Iceberg v2 —
PyIceberg reads it back correctly, planning both data files with the delete file
attached, and `table.scan().to_arrow()` returns ids `[1, 2, 5]` (ids 3 and 4
shadowed).

## The seven tables

Every table is **format version 2**. `snapshots` counts entries in
`metadata.snapshots`.

---

### 1. `unpartitioned` — bridge (iceberg-rust 0.10.1)

current metadata `00003-a21b4d75-51ce-4d39-b78e-fc2d3e91e2c8.metadata.json` ·
schemas 1 · snapshots 3 · partition spec `[]` (unpartitioned)

| id | name | type | required |
|---|---|---|---|
| 1 | id | long | required |
| 2 | region | string | required |
| 3 | amount | double | optional |
| 4 | ts | timestamp | optional |
| 5 | ok | boolean | optional |
| 6 | cnt | int | optional |

```
0  ["true"]
1  ["=","region","eu"]
2  [">","id",2]
3  ["and",[">","id",1],["is-null","amount"]]
4  ["in","region",["eu","us"]]
5  ["starts-with","region","e"]
```

---

### 2. `ident_part` — bridge (iceberg-rust 0.10.1)

current metadata `00003-370c8eea-e4fc-488c-8b84-2ecc36d5eb1e.metadata.json` ·
schemas 1 · snapshots 3

partition spec: `source-id 2 (region) → field-id 1000 "region" identity`

Schema identical to `unpartitioned` (fields 1–6: id long req, region string req,
amount double, ts timestamp, ok boolean, cnt int).

```
0  ["true"]
1  ["=","region","eu"]
2  [">","id",2]
3  ["and",[">","id",1],["is-null","amount"]]
4  ["in","region",["eu","us"]]
5  ["starts-with","region","e"]
```

---

### 3. `bucket_part` — bridge (iceberg-rust 0.10.1)

current metadata `00003-216a0fca-d810-4438-ac65-541e4bf30d0b.metadata.json` ·
schemas 1 · snapshots 3

partition spec: `source-id 1 (id) → field-id 1000 "id_bucket" bucket[4]`

Schema identical to `unpartitioned`.

```
0  ["true"]
1  ["=","id",3]
2  [">","id",2]
3  ["and",["=","id",1],["=","region","eu"]]
4  ["in","id",[1,4,7]]
5  ["not-null","amount"]
```

Data layout (6 files): `id_bucket=0 → {1,2}`, `id_bucket=1 → {6}`,
`id_bucket=2 → {4}`, `id_bucket=3 → {3}, {7}, {5}`.

**Known oracle disagreement, filter 4.** See the section below.

---

### 4. `day_part` — bridge (iceberg-rust 0.10.1)

current metadata `00003-a2d2b7ac-37a9-48dc-9309-78d9598e8c68.metadata.json` ·
schemas 1 · snapshots 3

partition spec: `source-id 4 (ts) → field-id 1000 "ts_day" day`

Schema identical to `unpartitioned`.

```
0  ["true"]
1  [">=","ts","2023-11-16T00:00:00"]
2  ["<","ts","2023-11-16T00:00:00"]
3  ["and",[">=","ts","2023-11-15T00:00:00"],["<","ts","2023-11-18T00:00:00"]]
4  ["is-null","ts"]
5  ["=","region","eu"]
```

---

### 5. `trunc_part` — bridge (iceberg-rust 0.10.1)

current metadata `00003-f9008911-7f3e-4185-aad0-cb2580132eb3.metadata.json` ·
schemas 1 · snapshots 3

partition spec: `source-id 2 (region) → field-id 1000 "region_trunc" truncate[3]`

Schema identical to `unpartitioned`, but the region values are long
(`europe`, `usa`, `apac`, …) so the truncation is observable.

```
0  ["true"]
1  ["=","region","europe"]
2  ["starts-with","region","eur"]
3  ["in","region",["europe","usa"]]
4  ["and",["starts-with","region","apac"],[">","id",4]]
5  ["not-in","region",["europe"]]
```

---

### 6. `evolved` — PyIceberg 0.11.1

current metadata `00006-4cdfbfe2-4744-467a-ac28-fbeccf64f2ba.metadata.json` ·
**schemas 4** · snapshots 3 · partition spec `[]` (unpartitioned)

Schema chain, one change per commit so the metadata carries the whole history:

* schema 0 — `id long req`, `name string`, `cnt int`, `amount double`
* schema 1 — adds `extra string` (field id 5)
* schema 2 — renames `name` → `label`
* schema 3 — promotes `cnt` `int` → `long`  ← **current**

Current schema (schema-id 3):

| id | name | type | required |
|---|---|---|---|
| 1 | id | long | required |
| 2 | label | string | optional |
| 3 | cnt | long | optional |
| 4 | amount | double | optional |
| 5 | extra | string | optional |

Snapshot 1 was appended under schema 0 (rows id 1–3, no `extra`, `cnt` still an
int on disk); snapshots 2 and 3 under schema 3 (ids 4–5 and id 6). Reading it
back exercises name mapping across a rename and int→long promotion.

```
0  ["true"]
1  ["=","label","alpha"]
2  [">","id",3]
3  ["and",[">=","id",2],["is-null","amount"]]
4  ["in","label",["alpha","delta","zeta"]]
5  ["not-null","extra"]
```

---

### 7. `deletes_v2` — PyIceberg 0.11.1 + a hand-written position delete

current metadata `00003-4778400f-cf1a-4a70-9adb-1e8cad720dcf.metadata.json` ·
schemas 1 · snapshots 3 · partition spec `[]` (unpartitioned)

| id | name | type | required |
|---|---|---|---|
| 1 | id | long | required |
| 2 | region | string | required |
| 3 | amount | double | optional |
| 4 | ts | timestamp | optional |
| 5 | ok | boolean | optional |

Table properties set `write.delete.mode` / `write.update.mode` /
`write.merge.mode` to `merge-on-read`. Two appends (ids 1–3, then 4–5), then a
`delete` snapshot carrying one position-delete file with two rows — see the
section above on how that file was actually produced.

```
0  ["true"]
1  ["=","region","eu"]
2  [">","id",3]
3  ["and",["=","region","us"],["not-null","amount"]]
4  ["in","region",["eu","apac"]]
5  ["is-null","amount"]
```

Both data files carry the same delete file in every plan, so any filter that
keeps a data file is also a test of delete-file attachment.

## Where the two oracles disagree

The five bridge tables have both a bridge plan (`plan_<k>.json`) and a PyIceberg
plan (`pyiceberg_plan_<k>.json`). Across 5 tables × 6 filters the planned data
file sets agree on 29 of 30. The one disagreement:

**`bucket_part`, filter 4 — `["in","id",[1,4,7]]`**

* iceberg-rust 0.10.1 plans **5** files.
* PyIceberg 0.11.1 plans **3** files.

ids 1, 4, 7 land in buckets 0, 2, 3. Both implementations correctly keep the
bucket-0 file (`{1,2}`), the bucket-2 file (`{4}`) and the bucket-3 file `{7}`.
Bucket 3 also holds a `{3}` file and a `{5}` file, which the *partition* filter
cannot exclude. PyIceberg additionally evaluates the `In` predicate against each
data file's column bounds and prunes both; iceberg-rust 0.10.1 does not apply
`In` in its metrics evaluator and keeps them.

Both answers are correct — a scan plan is allowed to be a superset, and row-level
filtering removes the extra rows — but PyIceberg's is strictly tighter. A native
Mojo reader may match either; the fixture keeps both so the difference is a
recorded fact rather than a surprise. The extra files the bridge keeps are
`iceberg-rs-mojo-00000-07b7b9a8-…` (id 3) and
`iceberg-rs-mojo-00001-be6d1933-…` (id 5).

## Size

`du -sh tests/fixtures` → **1.2 MB** (budget: ~5 MB).
