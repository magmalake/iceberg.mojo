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
| DuckDB | **1.5.5**, with its `iceberg` extension — the only oracle that reads equality deletes |
| Python | 3.12.13 (venv at `../iceberg-rs.mojo/build/pyiceberg-venv`) |
| Catalog | PyIceberg `SqlCatalog` over sqlite, shared by both writers |

## Original warehouse

```
warehouse root : /Users/mseritan/dev/magmalake/iceberg.mojo/build/warehouse-root/warehouse/db
sqlite catalog : /Users/mseritan/dev/magmalake/iceberg.mojo/build/warehouse-root/catalog.db
namespace      : db
```

All **nine** tables live in that one warehouse and one catalog, so the fixture
set is a single coherent Iceberg warehouse rather than nine unrelated ones.
Seven were made by the original run (see below); `eq_deletes_v2` and `dv_v3`
were added later by `tools/make_delete_tables.py` into the same warehouse,
which is why their UUIDs and paths differ in generation but not in kind.

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

## Data files

Since the reader reads data, `tests/fixtures/<table>/data/` now holds each
table's Parquet too — verbatim copies, absolute paths and all, rewritten by the
same prefix rebase the metadata needs. Their compression is what the writers
chose: of 271 column chunks across 52 files, 168 are uncompressed, **97 are
ZSTD** and 6 are Snappy — which is why the reader is built against
`parquet.ext_full.AllCodecs` rather than the default codec set.

## The two delete tables

`tools/make_delete_tables.py` builds these, and neither is reachable from
PyIceberg's public API.

### `eq_deletes_v2` — equality deletes

Six rows in two data files, then one commit adding two **equality delete
files**:

| delete file | `equality_ids` | rows | removes |
|---|---|---|---|
| `…-equality-deletes-id-…` | `[1]` (`id`) | `2`, `6` | ids 2 and 6 |
| `…-equality-deletes-amount-…` | `[3]` (`amount`) | a single `NULL` | every row whose `amount` is null — ids 2 and 5 |

Row 2 is matched by both, which is legal and must not be double-counted. Three
rows survive: **1, 3, 4**.

The NULL delete row is the spec's *"a null value in a delete column matches a
row if the row's value is null"*, and it is the reason this table exists.

**PyIceberg is not an oracle here at any level.** It has no equality-delete
writer, and `TableScan.plan_files` raises outright:

```
ValueError: PyIceberg does not yet support equality deletes:
https://github.com/apache/iceberg/issues/6568
```

**DuckDB 1.5.5 is**, and it returns exactly ids 1, 3, 4. That is what
`oracle/rows_duckdb.json` records.

### `dv_v3` — a format-version 3 table with deletion vectors

Six rows in two data files, then a v3 snapshot adding one Puffin file holding
two `deletion-vector-v1` blobs, one per data file, each removing one row.
Four rows survive: **1, 2, 5, 6**.

PyIceberg cannot write any of it — `TableMetadataV3.model_dump_json` raises
`NotImplementedError: Writing V3 is not yet supported`
([iceberg-python#1551](https://github.com/apache/iceberg-python/issues/1551)),
and `write_manifest` / `write_manifest_list` refuse version 3 — but every v3
Avro struct is present in `pyiceberg.manifest`, so the snapshot is assembled
from them:

* the Puffin file is written directly to the spec's framing: `PFA1`, a blob of
  `[4-byte big-endian length][D1 D3 39 64][portable 64-bit Roaring][CRC-32]`,
  then an uncompressed JSON footer;
* a **v3 delete manifest** carries `referenced_data_file`, `content_offset` and
  `content_size_in_bytes`, which the spec requires to match the footer exactly;
* the two pre-upgrade **data manifests are rewritten as v3** with an assigned
  `first_row_id` per file, because the spec requires the first snapshot after
  an upgrade to assign row ids to existing files too;
* the manifest list is v3 and assigns `first_row_id` to both data manifests
  (0 and 3) and `null` to the delete manifest, and the metadata file is
  hand-written with `format-version: 3` and `next-row-id: 6`.

**Both PyIceberg and DuckDB read it back and agree on ids 1, 2, 5, 6.**
`oracle/rows_pyiceberg_0.json` and `oracle/rows_duckdb.json` are those two
readings, and `delete_tables_report.json` records what the generator did.

#### A PyIceberg bug this uncovered

`ManifestWriter.new_writer` builds the in-memory record schema from
`DEFAULT_READ_VERSION` (2) while writing the file with the writer's own
version:

```python
return AvroOutputFile[ManifestEntry](
    file_schema=self._with_partition(self.version),
    record_schema=self._with_partition(DEFAULT_READ_VERSION),   # <- 2
    ...
)
```

For a v3 manifest that silently writes `null` for every v3-only column —
`first_row_id`, `referenced_data_file`, `content_offset`,
`content_size_in_bytes`. The symptoms were a deletion vector that appeared to
apply to every data file (ours) and `INTERNAL Error: Calling GetValue on a
value that is NULL` (DuckDB's). `make_delete_tables.py` overrides `new_writer`.

## Row-level oracles

`tools/oracle_rows.py` writes, per table:

* `oracle/rows_pyiceberg_<k>.json` — PyIceberg's `scan(row_filter).to_arrow()`
  for each of the six filters. Eight tables; `eq_deletes_v2` has none, for the
  reason above.
* `oracle/rows_duckdb.json` — DuckDB's `iceberg_scan`, unfiltered (it takes no
  filter DSL). All nine tables.

Cells are encoded exactly and canonically so that "the same rows" is a byte
comparison rather than a float-formatting argument:

| type | encoding |
|---|---|
| `boolean` | `"true"` / `"false"` |
| `int`, `long` | decimal |
| `float`, `double` | big-endian IEEE-754 bits, lowercase hex |
| `date` | days since 1970-01-01 |
| `time` | microseconds since midnight |
| `timestamp`, `timestamptz` | microseconds since epoch |
| `timestamp_ns`, `timestamptz_ns` | nanoseconds since epoch |
| `string` | the text |
| `uuid` | canonical 8-4-4-4-12 lowercase |
| `binary`, `fixed` | lowercase hex |
| `decimal` | the exact decimal text |

Rows are sorted, so ordering never matters.

## Size

`du -sh tests/fixtures` → **1.9 MB** (budget: ~5 MB), of which 212 KB is
Parquet and Puffin.
