# iceberg.mojo

[![mojoshelf](https://mojoshelf.org/badge/iceberg-mojo.svg)](https://mojoshelf.org/tins/iceberg-mojo) [![mojo nightly](https://mojoshelf.org/badge/iceberg-mojo/nightly.svg)](https://mojoshelf.org/tins/iceberg-mojo)

> Part of **magmalake** — data lake building blocks in Mojo.

Native, pure-Mojo **Apache Iceberg**: read a table's `metadata.json`, pick a
snapshot, decode its manifests, plan a scan, and **read the rows** — deletes
applied, filters evaluated, projection resolved by field id — then **create a
table and append to it**, manifests, snapshot and optimistic commit included.
Over local files, S3, GCS, Azure or plain HTTP, from a filesystem layout or a
live REST catalog. No JVM, no Python, no Rust.

```mojo
from iceberg.catalog.rest import RestCatalog, RestCatalogConfig

var config = RestCatalogConfig("https://catalog.example.com")
config.with_token(token)
config.vend_credentials = True

var catalog = RestCatalog(config^, FileIO.local())
catalog.connect()

var rows = (
    catalog.load_table("db", "orders")
    .scan()
    .filter('["and",[">","id",2],["=","region","eu"]]')
    .select(["id", "region", "amount"])
    .to_table()
)
print(rows.to_csv())
```

And the write side:

```mojo
var table = catalog.create_table("db", "orders", schema, spec)

var tx = table.new_append()
tx.add(batch)                       # an Arrow RecordBatch
_ = tx.commit()                     # manifest, manifest list, snapshot, commit
```

```console
$ iceberg-mojo cat s3://warehouse/db/orders --filter '["=","region","eu"]' --limit 5
$ iceberg-mojo cat --rest https://catalog --table db.orders --token $TOKEN
```

## The stack

**Native read and native fast-append.** Scans decode Parquet through
parquet.mojo and come out as Arrow; appends write Parquet and Avro back
through the same tins and commit against a filesystem layout or a REST
catalog. `overwrite`, `delete` and compaction are not here — they merge
manifests rather than appending to them — and go through
[iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo)'s bridge over
iceberg-rust.

Everything under this line is another magmalake tin, consumed by source path.
iceberg.mojo is the part that knows what Iceberg *means*; the tins below know
what the bytes mean.

```
                         ┌──────────────────────────────┐
                         │        iceberg.mojo          │
                         │  metadata · snapshots        │
                         │  transforms · expressions    │
                         │  manifests · scan planning   │
                         │  puffin · reads · kernels    │
                         │  writes · commits · catalogs │
                         └───┬─────────┬─────────┬──────┘
        manifests (Avro, r/w)│         │         │data files (Parquet, r/w)
                             │         │         │
        ┌────────────────────▼──┐   ┌──▼─────────▼──────────┐
        │      avro.mojo        │   │     parquet.mojo      │
        │  OCF, field ids       │   │  pages, encodings,    │
        │  deflate/snappy/zstd  │   │  Arrow C Data, writer │
        └───────────────────────┘   └──┬─────────┬────────┬─┘
                                       │         │        │
                              ┌────────▼───┐ ┌───▼────┐ ┌─▼─────────┐
                              │thrift.mojo │ │ snappy │ │  hashes   │
                              │  footers   │ │ .mojo  │ │  .mojo    │
                              └────────────┘ └────────┘ └───────────┘
    deletion vectors                              codecs (FFI)
        ┌───────────────┐                    ┌──────────┬──────────┐
        │ roaring.mojo  │                    │zstd.mojo │ lz4.mojo │
        │ Bitmap64, DV  │                    └──────────┴──────────┘
        └───────────────┘
    storage and transport
        ┌──────────────────────────────────────────────────────────┐
        │                    objectstore.mojo                      │
        │  FileIOResolver · local · http · s3 (SigV4) · gcs · azure │
        │  HttpClient over libcurl — the REST catalog's socket      │
        └──────────────────────────────────────────────────────────┘
```

`hashes.mojo` supplies the murmur3 that `bucket[N]` is *defined* in terms of;
`roaring.mojo` decodes the deletion-vector bitmap and checks its CRC;
`objectstore.mojo` (0.2.0: pooled connections, retries with backoff, S3
multipart, range coalescing) turns a location into bytes and supplies the HTTP
client an Iceberg REST catalog needs, because no Mojo HTTP package resolves
from conda.
The ZSTD and LZ4 codecs are needed because real Iceberg writers use them: of
the 271 column chunks in this repo's own fixtures, **97 are ZSTD**.

## Status

Every gate below was **measured**, by `pixi run test`, against fixtures
produced by three independent reference implementations: iceberg-rust 0.10.1
(through [iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo)),
PyIceberg 0.11.1, and DuckDB 1.5.5's `iceberg` extension. Nothing here is
self-checked — the expected values come from them, never from this code. See
[`tests/fixtures/PROVENANCE.md`](tests/fixtures/PROVENANCE.md).

| Gate | Oracle | Result |
|---|---|---|
| **(a)** Rows returned by `to_table()`, all columns, nulls included | PyIceberg | ✅ **48 / 48 cases** (8 tables × 6 filters), **152 rows**, compared cell-exactly |
| **(a′)** The same rows, unfiltered | DuckDB `iceberg_scan` | ✅ **9 / 9 tables**, **51 rows** |
| **(b)** Position deletes: deleted rows absent, counts match | PyIceberg | ✅ ids 1, 2, 5 survive of 5 written |
| **(c)** Equality deletes, incl. `null` matching `null` | **DuckDB** (PyIceberg refuses the table) | ✅ ids 1, 3, 4 survive of 6 written |
| **(d)** Deletion vectors, format v3, Puffin | **PyIceberg and DuckDB** | ✅ ids 1, 2, 5, 6 survive of 6 written; both agree |
| **(e)** Schema evolution: renamed, promoted, added columns | PyIceberg | ✅ `label` (renamed), `cnt` = 5 000 000 000 (`int`→`long`), `extra` null before it existed |
| **(f)** `s3://` end to end, eager and lazy IO | itself, vs the local read | ✅ **4 tables, 23 rows**, identical; MinIO verifies the SigV4 signatures |
| **(f′)** REST catalog, live over HTTP | itself, vs the local read | ✅ **7 tables**: 42 plans and every row identical |
| **(g)** `plan_files()` — planned data files and their deletes | PyIceberg + iceberg-rust | ✅ **48 / 48** vs PyIceberg, **47 / 48** vs iceberg-rust (see below) |
| **(h)** Table metadata parses and round-trips, field by field | both | ✅ **8 / 8 tables** |
| **(i)** Snapshot selection — current, by id, by ref, as-of | both | ✅ **24 / 24 snapshots** |
| **(j)** Partition transforms | PyIceberg + Appendix B | ✅ **252 / 252 vectors**, 87 also checking the raw 32-bit hash |
| **(k)** Manifests decode with inherited sequence numbers | the manifest lists themselves | ✅ **38 entries**, 36 inherited and 2 explicit |
| **(l)** Puffin footer vs the manifest's `content_offset` | the spec's "must exactly match" | ✅ 2 / 2 blobs |
| **(m)** Column projection rules 2–5 — partition value, name mapping, `initial-default`, null | the spec | ✅ each reached by giving the reader a schema whose ids the file does not have |
| **(n)** Tables **we write** — rows, snapshots, partition values, statistics | PyIceberg **and** DuckDB | ✅ **10 / 10 tables** (5 partition shapes × v2/v3), 18 rows each, cell-exact both ways |
| **(o)** Row lineage on tables we write | **fastavro** + the v3 spec's rules | ✅ manifest-list `first_row_id`s tile `0..next-row-id` on all 5 shapes; data files inherit (null), as the spec requires. PyIceberg cannot check this — see below |
| **(p)** PyIceberg **appends to a table we created**, and we read the result | PyIceberg | ✅ 2 tables, 18 + 6 = 24 rows, `_row_id` still intact |
| **(q)** REST commit — requirements, 409 retry, `Idempotency-Key` replay, 5xx → `CommitStateUnknown` | the REST spec, against a mock that checks | ✅ v2 and v3 create + append; a rigged 409 retried; a rigged applied-then-500 recovered by the key, landing **one** snapshot; a server that will not deduplicate reported as unknown state |
| **(r)** `s3://` write end to end | itself, read back | ✅ create, 2 appends, 12 rows, partition pruning; MinIO verifies every signature |
| Tests | | **137 passing**, 0 skipped, identical on `stable` (Mojo 1.0.0) and `default` (nightly) |
| CI | | 5 jobs: {stable, nightly} × {ubuntu, macOS} each running the REST mock and MinIO, plus a write-interop job running PyIceberg and DuckDB against tables we wrote |

### The one plan disagreement, and why it is not a bug

For `["in","id",[1,4,7]]` over the `bucket[4]`-partitioned table, iceberg-rust
0.10.1 plans **5** files and this reader plans **3**. PyIceberg also plans 3,
and they are the same 3.

ids 1, 4 and 7 hash into buckets 0, 2 and 3. All three implementations keep the
bucket-0, bucket-2 and bucket-3 files. Bucket 3 also holds a file of `{3}` and a
file of `{5}`, which the *partition* filter cannot exclude — both are in bucket
3. PyIceberg and this reader then apply the predicate to each file's column
bounds and drop them; iceberg-rust does not apply `In` at that level.

A scan plan is allowed to be a superset — reading a file with no matching rows
costs time, not correctness. So this is looseness in the bridge, not an error in
either. A separate test asserts the direction that *would* be a bug: across all
48 cases this reader never plans a file the bridge does not.

## What is implemented

**Format versions 1, 2 and 3** for reading, tolerating v4 where the spec
already says to (optional `location`, relative locations, unknown fields,
unknown transforms).

### Reading rows

- **Column projection by field id**, in the spec's order: the file's column
  with that id, else an identity partition value from the manifest, else
  `schema.name-mapping.default`, else `initial-default`, else null.
- **Schema evolution** falls out of that: renamed columns are found by id,
  added columns read as null in older files, and `int`→`long`,
  `float`→`double` and decimal-precision widening are read at the file's
  physical width and produced at the table's current type.
- **Deletes**, in the order the spec implies: a deletion vector replaces every
  position delete file for its data file; position delete files are matched by
  the reserved ids 2147483546 / 2147483545 and filtered to this file's path;
  equality delete files match on `equality_ids`, with `null` equal to `null`.
  Scope by sequence number and partition is the planner's job and was already
  done.
- **Pruning** — row groups and pages by the residual, using Parquet statistics
  — with one rule: positions must be exact, so page pruning is skipped whenever
  a row position matters (a delete, `_pos`, or `_row_id`).
- **Metadata columns** on request: `_file`, `_pos`, `_spec_id`, `_partition`,
  `_last_updated_sequence_number`, and v3's `_row_id` through `first_row_id`
  inheritance.
- **Output** as Arrow, throughout. A scan's columns are parquet.mojo
  `ArrayData` from the moment they are decoded: `iceberg.kernels` casts them to
  the table's current type, builds constant arrays for the columns a file does
  not have, turns deletes and the residual into one selection bitmap and
  applies it with a single filter pass per column. `to_batches()` hands back
  `RecordBatch`es (`export_c` gives them to anything speaking the Arrow C Data
  Interface); `to_table()` concatenates them; CSV and Appendix-D JSON are
  formatted on demand. No tagged value is materialised per cell.
- **Lazy IO** as an option: fetch the footer, then only the byte ranges of the
  row groups that survive statistics pruning, into a buffer the size of the
  file with everything else left zero. Parquet addresses everything by absolute
  offset, so a sparse buffer decodes exactly like the whole file — and over a
  network that is one range request per row group instead of the whole object.

### Writing rows

**Fast append** — the one write operation that never rewrites anything.

```mojo
var catalog = FilesystemCatalog.local("/warehouse")
var table = catalog.create_table("db", "orders", schema, spec)

var tx = table.new_append()
tx.add(batch)
_ = tx.commit()
```

- **Data files.** Arrow batches in, Parquet out through parquet.mojo's writer:
  columns aligned to the schema by name or field id, cast to the table's
  types, partitioned by the spec's transforms (`identity`, `bucket[N]`,
  `truncate[W]`, `year`, `month`, `day`, `hour`, `void`), one file per
  partition per batch at `data/<partition path>/<uuid>-<n>.parquet`, with
  field ids and the table's `write.parquet.*` properties (zstd by default).
  `write.parquet.page-size-bytes` is honoured; `write.parquet.row-group-size-bytes`
  is **not**, because parquet.mojo's writer splits a batch by a row count and
  converting would mean guessing a compression ratio before compressing —
  `write.parquet.row-group-size-rows` says what it means instead.
- **Statistics.** Read back out of the footer the writer just produced —
  column sizes, value counts, null counts, and Appendix-D lower and upper
  bounds truncated the way `write.metadata.metrics.default`'s `truncate(16)`
  says, with an upper bound incremented so that truncating it does not stop
  it bounding. Whatever the writer decided the min and max were is what the
  manifest reports, which is the only way the two can agree.
- **Manifests and manifest lists.** The Avro schema is generated *per format
  version* — v1's required `snapshot_id` and `block_size_in_bytes`, v2's
  `content` / `sequence_number` / `equality_ids`, v3's `first_row_id` /
  `referenced_data_file` / `content_offset` / `content_size_in_bytes` — and
  written verbatim through `set_schema_json`, with the file metadata
  (`schema`, `schema-id`, `partition-spec`, `partition-spec-id`,
  `format-version`, `content`) a reader needs to type the `partition` column.
  ADDED entries carry null sequence numbers so that inheritance happens.
- **Snapshots.** `operation=append` with `added-data-files`, `added-records`,
  `added-files-size`, `changed-partition-count` and the `total-*` keys carried
  forward; `sequence-number = last + 1`; the parent's manifests carried into
  the new manifest list *unchanged*, keeping their sequence numbers and their
  `first_row_id`.
- **Row lineage (v3).** The snapshot's `first-row-id` is the table's
  `next-row-id`, the new manifest is assigned it, every data file in that
  manifest inherits from it in order, and `next-row-id` advances by
  `added-rows`. Ranges never overlap, which is what makes `_row_id` stable.
- **Commit.** A filesystem table writes `<V>-<uuid>.metadata.json` and
  `version-hint.text`; a losing writer sees that the file it based its version
  on is no longer the newest, reloads and retries — the data files it already
  wrote stay valid. A REST catalog gets a `CommitTableRequest` with
  `assert-table-uuid` and `assert-ref-snapshot-id`, an `Idempotency-Key`, a
  409 that reloads and retries. The key is what makes a 5xx survivable: it is
  stable across repeats of one commit, so objectstore.mojo's client may retry
  the `POST` and a server that honours the header (the REST spec has asked for
  it since 1.11.0) replays its original answer rather than committing twice —
  the mock proves this by applying a commit, answering 500, and then replaying
  the success. Against a server that does *not* deduplicate, every attempt
  fails alike and the commit is reported as `CommitStateUnknown`: it may or
  may not have landed, and only a reload can say.
- **`iceberg.batch`** turns Mojo values into an Arrow `RecordBatch`, for
  callers whose data is not already Arrow.

### Puffin and deletion vectors

`iceberg.puffin` reads a footer (`PFA1`, the little-endian size and flags, the
LZ4-or-plain JSON `FileMetadata`) and decodes `deletion-vector-v1` blobs
through roaring.mojo, which verifies the blob's big-endian length, its
`D1 D3 39 64` magic and its CRC-32. A scan never reads the footer: the delete
manifest entry already carries `content_offset` and `content_size_in_bytes`,
and the spec requires them to match it exactly, so the blob is read directly.
`apache-datasketches-theta-v1` blobs are listed with their metadata (`ndv` and
all) but their sketches are not decoded.

### Everything under it

- **Types and schemas** — every primitive including the v3 additions
  (`unknown`, `timestamp_ns`, `timestamptz_ns`, `variant`, `geometry(C)`,
  `geography(C, A)`), `decimal(P, S)`, `fixed[L]`, nested struct/list/map with
  field ids, `initial-default` / `write-default`, identifier field ids, and
  dotted-name lookup that reaches list elements and map keys/values.
- **Table metadata** — every field in the spec's table-metadata table, the v1
  singular forms, `refs`, `snapshot-log`, `metadata-log`, `statistics` with
  blob metadata, `partition-statistics`, and the v3 `next-row-id`,
  `encryption-keys` and snapshot `first-row-id` / `added-rows` / `key-id`.
  Unknown top-level keys survive a round trip.
- **Partition transforms** — `identity`, `bucket[N]`, `truncate[W]`, `year`,
  `month`, `day`, `hour`, `void`, with correct result types, code-point string
  truncation, nanosecond timestamps truncated to microseconds before hashing,
  and unknown transforms kept verbatim and excluded from filtering as v3
  requires.
- **Expressions** — the full predicate set, binding against a schema with
  per-column literal typing, `rewrite_not`, inclusive and strict projection
  through transforms, `ManifestEvaluator` over partition summaries,
  `InclusiveMetricsEvaluator` over data-file bounds, and a row-level evaluator
  for what is left after all of that.
- **Manifests** — manifest lists (fields 500–520) and manifests
  (`manifest_entry` plus `data_file` 100–145), with sequence-number,
  snapshot-id and `first_row_id` inheritance, and partition tuples typed by the
  spec in the manifest's own Avro file metadata.
- **Scan planning** — manifest pruning, data-file pruning by partition and by
  metrics, residuals, and delete-file association by the spec's scope rules.
- **Catalogs** — a filesystem catalog (`version-hint.text` or highest-versioned
  `*.metadata.json`, ties broken by `last-updated-ms`), gzipped metadata, and a
  **REST catalog** over HTTPS.

## Storage and catalogs

Every location the reader touches goes through `FileIO`, which is
objectstore.mojo's `FileIOResolver`: it picks a backend by URI scheme,
configures it from Iceberg's own property names, and falls back to the `AWS_*`
environment.

```mojo
var io = FileIO.local()
io.set("s3.endpoint", "https://minio.internal:9000")
io.set("s3.region", "us-east-1")
var t = Table.load("s3://warehouse/db/orders", io^)
```

| Scheme | Backend | Authentication |
|---|---|---|
| `file://`, bare paths | local | — |
| `s3://`, `s3a://`, `s3n://` | S3 | SigV4 from `s3.access-key-id` / `s3.secret-access-key` / `s3.session-token`, or `AWS_*`, or vended per-prefix credentials, or anonymous |
| `gs://`, `gcs://` | GCS over its S3-compatible XML endpoint | `gcs.oauth2.token` |
| `abfs(s)://`, `wasb(s)://`, `az://` | Azure Blob | `adls.sas-token.<account>` |
| `http(s)://` | ranged GETs | — |

`FileIO.rebase(old, new)` rewrites location prefixes before opening anything,
which is how a warehouse copied elsewhere — or the fixtures in this repo — is
readable at all. `FileIO.with_base(location)` resolves *relative* locations
against the table root, which is what a v4 table that stores them that way
needs.

### REST catalog

`RestCatalog` is `RestCatalogConfig` plus objectstore's libcurl-backed
`HttpClient`:

```mojo
var catalog = RestCatalog("https://catalog.example.com")
catalog.config.with_token(token)
catalog.config.with_warehouse("s3://warehouse")
catalog.config.vend_credentials = True
catalog.connect()                       # GET /v1/config, absorb `prefix`

for name in catalog.list_tables("db"):
    print(name)

var t = catalog.load_table("db", "orders")   # FileIO already configured
```

`GET /v1/config` (absorbing the `prefix` override), `listNamespaces`,
`listTables`, `loadTable` and a `HEAD` for existence, with 400/401/403/404/409/
419/5xx mapped onto errors that quote the server's `ErrorModel`. A loaded
table's `FileIO` is configured from the response's `config` and from every
`storage-credentials` entry, longest matching prefix first, so a vended
per-prefix STS credential is used for exactly the objects it was issued for.

`tests/rest_server.py` is a mock catalog the test runner starts; it demands
`Authorization: Bearer` and echoes the delegation header back, so the client's
headers are proved rather than assumed.

## Deliberately out of scope

| Not here | Why |
|---|---|
| `overwrite`, `delete`, `replace`, compaction | Every one of them merges manifests rather than appending to them, which is a different machine. [iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo)'s bridge covers them. |
| Writing delete files or deletion vectors | Same reason. They are *read* completely. |
| Schema and spec evolution as a write | `create_table` fixes both; changing them afterwards is a commit this build does not construct. |
| Writing **format version 1** | The v1 manifest schemas are generated and would be written, but nothing verifies them — v1 has no writers left to check against. v2 and v3 are gated end to end. |
| Nested columns in a scan or a write | `to_table()` and `write_data_files` handle primitive columns; a struct/list/map raises. The *metadata* for them is complete. |
| Non-Parquet data files | ORC and Avro data files are rejected by name. Parquet is what every writer in reach produces. |
| Brotli-compressed Parquet | No Brotli in Mojo. Everything else — uncompressed, Snappy, GZIP, ZSTD, LZ4 — works. |
| Encryption | Neither Parquet modular encryption nor Iceberg's `encryption-keys` are applied. |
| A *safe* filesystem commit over an object store | There is no atomic create-if-absent on S3, so two writers can both believe they won. The spec says the same; use a REST catalog. |
| `remote-signing` delegation | `vended-credentials` is implemented; remote signing is not. |
| Theta sketches | Listed from a Puffin footer, not decoded. |

## Performance

`pixi run bench` builds a one-million-row table and times whole scans —
metadata, plan, Parquet decode, casts, deletes, filter — then runs the same
scans through PyIceberg. M4, one core, six columns (two longs, a string, a
double, a timestamp, a boolean):

| Scan | before (`Datum` per cell) | **now (Arrow fast path)** | PyIceberg 0.11.1 |
|---|---|---|---|
| full scan, 1 M rows × 6 columns | 911 ms — 1.10 M rows/s | **305 ms — 3.28 M rows/s** | 24 ms — 41 M rows/s |
| projection to 2 of 6 columns | 381 ms — 2.62 M rows/s | **87 ms — 11.5 M rows/s** | 6 ms — 165 M rows/s |
| `region = 'eu'` → 200 k rows | 725 ms — 0.28 M rows/s | **293 ms — 0.68 M rows/s** | 13 ms — 15 M rows/s |
| `id > 900000` → 100 k rows | 185 ms — 0.54 M rows/s | **70 ms — 1.41 M rows/s** | 7 ms — 15 M rows/s |
| full scan, lazy IO | 838 ms — 1.19 M rows/s | **284 ms — 3.52 M rows/s** | — |

3.0× to 4.4× faster, and the reason the full scan stops at 3.28 M rows/s is no
longer anything this repository does. Decoding the *same four Parquet files*
with parquet.mojo alone — `ParquetReader.read_batch()` in a loop, no Iceberg at
all — takes **293 ms**. The Iceberg layer on top of it now costs **4 %**. Every
cast on this table is a retag (the buffers are already the right width), the
selection is a single `List[Bool]`, and the decoded buffers are *moved* into the
output batch rather than copied.

So the remaining ~10× to PyIceberg is parquet.mojo's decoder against Arrow C++,
which is a different repository's problem. The two-column projection, where
decoding is cheap enough to stop dominating, reaches **11.5 M rows/s**.

`to_batches()` is the fast path and is what the numbers above measure;
`to_table()` concatenates those batches into one `ScanResult` and costs one
extra copy of each buffer, which on this table is under 5 %.

### Writing

The same million rows, appended to a fresh table in four commits of 250 000 —
building the Arrow batches, writing the Parquet, reading the footers back for
the statistics, writing the manifest, the manifest list and the
`metadata.json`, all inside the measurement:

| | iceberg.mojo | PyIceberg 0.11.1 |
|---|---|---|
| append 1 M rows × 6 columns, 4 commits | 917 ms — **1.09 M rows/s** | 163 ms — 6.13 M rows/s |
| Parquet written (zstd) | 3 MB | 9 MB |

5.6× slower, and the whole of that is the Parquet *encoder*: the manifest, the
manifest list and the metadata are four small Avro files and one JSON document
per commit. The 3 MB against 9 MB is a difference in dictionary encoding, not
in content — PyIceberg reads all 1 000 000 rows back out of those four files
and agrees on every column, including both null counts.

### What still falls back to `Datum`

The kernels cover every comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`), `in`,
`not-in`, `is-null`, `not-null`, `is-nan`, `not-nan`, `starts-with` and
`not-starts-with` over integral, floating-point and byte-shaped columns, and
`and`/`or`/`not` over the resulting bitmaps. Three shapes still build a `Datum`,
and only for the leaf that needs one:

1. **any predicate on a `decimal` column.** Arrow stores a decimal as a 16-byte
   little-endian two's complement, which a byte compare does not order.
2. **`starts-with` on a column that is not byte-shaped** — which the binder
   should already have rejected, so this is belt and braces.
3. **`ScanResult.value`, `to_csv` and `to_json`**, which are asking for a tagged
   value by definition.

A predicate on a *constant* column — an identity partition value, an
`initial-default` — is evaluated once for the whole batch rather than per row.

## Install

```sh
pixi run test              # 137 tests; starts the REST mock and MinIO
pixi run -e stable test
pixi run cli               # builds build/iceberg-mojo
pixi run bench             # scans and appends, against PyIceberg
pixi run verify-writes     # writes 10 tables; PyIceberg and DuckDB read them
```

Consume it with:

```
-I ../iceberg.mojo/src -I ../hashes.mojo/src -I ../avro.mojo/src \
-I ../thrift.mojo/src -I ../snappy.mojo/src -I ../parquet.mojo/src \
-I ../roaring.mojo/src -I ../objectstore.mojo/src \
-I ../zstd.mojo/src -I ../lz4.mojo/src
```

Sibling tins are consumed by **source path**, not as pixi packages:
pixi-build-mojo emits a precompiled artifact built with `mojo-compiler` 1.0.0,
and the nightly compiler refuses to load it. Source paths satisfy both
environments. The three FFI tins (objectstore, zstd, lz4) are *also* pixi git
source dependencies, which is what installs their C shims into the
environment; a consumer needs the same. The same precompiled-package problem
rules out EmberJson, which is why `iceberg.json` is a small in-repo parser.

## API

| Module | What it gives you |
|---|---|
| `iceberg.json` | `parse_json`, `Json` — an arena DOM with exact `Int64` handling |
| `iceberg.types` | `TypeStore`, `NestedField`, the primitive kinds |
| `iceberg.schema` | `Schema` — `find_field(id)`, `find_by_name`, `select(ids)` |
| `iceberg.values` | `Datum`, `compare`, Appendix-D binary and JSON single values |
| `iceberg.transforms` | `Transform`, `PartitionSpec`, `SortOrder`, `bucket_of` |
| `iceberg.expressions` | `parse_filter`, `bind`, projections, the two evaluators |
| `iceberg.metadata` | `TableMetadata`, `Snapshot`, `SnapshotRef`, snapshot selection |
| `iceberg.manifest` | `read_manifest_list_io`, `read_manifest_io`, `DataFile` |
| `iceberg.puffin` | `PuffinFile`, `BlobMetadata`, `read_deletion_vector` |
| `iceberg.kernels` | the columnar kernels: `cast_array`, `constant_array`, `filter_array`, `concat_into` |
| `iceberg.read` | `ScanResult`, `ScanOptions`, `NameMapping`, the metadata columns |
| `iceberg.batch` | `ColumnBuilder`, `batch_of` — Mojo values to an Arrow batch |
| `iceberg.write` | `write_data_files`, `WriteOptions`, bound truncation, partition paths |
| `iceberg.manifest_write` | `write_manifest`, `write_manifest_list`, the per-version Avro schemas |
| `iceberg.append` | `prepare_append`, `AppendResult`, metadata file naming |
| `iceberg.scan` | `TableScan`, `FileScanTask` — `plan_files`, `to_table`, `to_batches` |
| `iceberg.io` | `FileIO` over local, S3, GCS, Azure and HTTP |
| `iceberg.catalog.filesystem` | `Table`, `AppendFiles`, `FilesystemCatalog` |
| `iceberg.catalog.rest` | `RestCatalog`, `RestCatalogConfig`, `LoadTableResult` |

```mojo
from iceberg.catalog.filesystem import Table
from iceberg.read import ScanOptions

def main() raises:
    var t = Table.load_local("/warehouse/db/orders")

    var options = ScanOptions()
    options.limit = 100
    options.lazy = True

    var rows = (
        t.scan()
        .filter('["and",[">","id",2],["=","region","eu"]]')
        .select(["id", "region", "amount", "_file", "_pos"])
        .to_table(options)
    )
    print(rows.num_rows(), "rows")
    print(rows.to_json())

    for batch in t.scan().to_batches():      # Arrow, straight off the kernels
        print(batch.num_rows, batch.num_columns())
```

```mojo
from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import FilesystemCatalog
from iceberg.schema import Schema
from iceberg.transforms import PartitionField, PartitionSpec, parse_transform
from iceberg.values import Datum

def main() raises:
    var catalog = FilesystemCatalog.local("/warehouse")
    var schema = Schema.parse(
        '{"schema-id":0,"type":"struct","fields":['
        '{"id":1,"name":"id","required":true,"type":"long"},'
        '{"id":2,"name":"region","required":true,"type":"string"}]}'
    )
    var spec = PartitionSpec(
        0,
        [
            PartitionField.single(
                2, 1000, String("region"), parse_transform("identity")
            )
        ],
    )
    var table = catalog.create_table("db", "orders", schema, spec)

    var ids = ColumnBuilder.of(schema, 1)
    var region = ColumnBuilder.of(schema, 2)
    for k in range(1000):
        ids.add(Datum.long_(Int64(k)))
        region.add(Datum.string_("eu" if k % 2 == 0 else "us"))

    var tx = table.new_append()
    tx.add(batch_of([ids^, region^]))
    print(tx.commit(), "rows committed")
```

### Filter DSL

Filters are a JSON S-expression — deliberately the same grammar
[iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo) speaks, so one
filter string can be handed to both and the two plans diffed directly.

```jsonc
["=",  "col", <literal>]     ["!=", "col", <literal>]
["<",  "col", <literal>]     ["<=", "col", <literal>]
[">",  "col", <literal>]     [">=", "col", <literal>]
["is-null", "col"]           ["not-null", "col"]
["is-nan",  "col"]           ["not-nan",  "col"]
["starts-with", "col", "prefix"]        ["not-starts-with", "col", "prefix"]
["in", "col", [<literal>, …]]           ["not-in", "col", [<literal>, …]]
["and", <filter>, <filter>, …]          ["or", <filter>, <filter>, …]
["not", <filter>]            ["true"]   ["false"]
```

Literals are typed **against the column**, not by their JSON shape: `["=","id",3]`
on a `long` column produces a long, dates and timestamps accept either an ISO-8601
string or the raw integer, and decimals accept a string.

## CLI

```sh
iceberg-mojo describe  <table>
iceberg-mojo schema    <table>
iceberg-mojo snapshots <table>
iceberg-mojo files     <table> [options]
iceberg-mojo cat       <table> [options]
```

`<table>` is a `metadata.json`, a table directory, a `file://` or `s3://` URI,
or — with `--rest URL --table NS.NAME` — a table in a REST catalog.

| Option | |
|---|---|
| `--snapshot ID`, `--ref NAME`, `--as-of MS` | which snapshot |
| `--filter DSL` | a row filter in the DSL above |
| `--select a,b,c` | project; `cat` also takes `_file`, `_pos`, `_spec_id`, `_partition`, `_row_id`, `_last_updated_sequence_number` |
| `--limit N`, `--format csv\|json` | `cat` output |
| `--lazy` | fetch only the footer and the row groups needed |
| `--rebase FROM=TO` | rewrite location prefixes |
| `--property K=V` | a storage property, e.g. `s3.endpoint` |
| `--rest URL`, `--table NS.NAME`, `--token T`, `--warehouse W`, `--no-vend` | REST catalog |

```sh
iceberg-mojo cat tests/fixtures/dv_v3 \
  --rebase 'file:///…/warehouse/db=tests/fixtures'
```

## Fixtures

Nine tables, one coherent warehouse, ~1.9 MB with their Parquet. Full detail in
[`tests/fixtures/PROVENANCE.md`](tests/fixtures/PROVENANCE.md);
`tools/make_fixtures.sh` regenerates everything.

| Table | What it is for | Built by |
|---|---|---|
| `unpartitioned` | the base case | iceberg-rust 0.10.1 |
| `ident_part` | `identity(region)` | iceberg-rust 0.10.1 |
| `bucket_part` | `bucket[4](id)` | iceberg-rust 0.10.1 |
| `day_part` | `day(ts)` | iceberg-rust 0.10.1 |
| `trunc_part` | `truncate[3](region)` | iceberg-rust 0.10.1 |
| `evolved` | add + rename a column, promote `int`→`long` | PyIceberg 0.11.1 |
| `deletes_v2` | v2 with a **position delete file** | PyIceberg + a delete-manifest writer |
| `eq_deletes_v2` | v2 with **equality deletes**, one of them on a NULL | `tools/make_delete_tables.py` |
| `dv_v3` | **v3 with two deletion vectors** in a Puffin file | `tools/make_delete_tables.py` |

Three of these were not writable by anything to hand, and the workarounds are
recorded rather than hidden:

- **`deletes_v2`.** PyIceberg's `Table.delete()` falls back to copy-on-write
  (*"Merge on read is not yet supported"*), so the position delete file was
  written with a `ManifestWriterV2` subclass reporting `ManifestContent.DELETES`.
- **`eq_deletes_v2`.** PyIceberg has no equality-delete writer and *refuses to
  plan* a scan of such a table at all, so DuckDB is its only oracle.
- **`dv_v3`.** PyIceberg cannot write v3 metadata
  (`TableMetadataV3.model_dump_json` raises `NotImplementedError`) but every v3
  Avro struct is present in it, so the snapshot was assembled from those. Doing
  so surfaced a real PyIceberg bug: `ManifestWriter.new_writer` builds the
  in-memory record schema from `DEFAULT_READ_VERSION` (2) while writing the
  file at the writer's own version, so a v3 manifest silently writes null for
  `first_row_id`, `referenced_data_file`, `content_offset` and
  `content_size_in_bytes`.

Verifying the *write* path found the same bug facing the other way.
`ManifestList` and `ManifestFile` are read with
`MANIFEST_LIST_FILE_SCHEMAS[DEFAULT_READ_VERSION]` and
`MANIFEST_ENTRY_SCHEMAS[DEFAULT_READ_VERSION]`, and `DEFAULT_READ_VERSION` is
2, so field **520 (`first_row_id`)** and the v3 `data_file` fields 142–145 are
dropped on the way *in* as well. PyIceberg therefore reports `first_row_id =
None` for manifests that plainly carry it, and cannot be the oracle for v3 row
lineage. `tools/verify_written.py` uses **fastavro** for that layer instead: it
decodes with the schema the file itself carries, and confirms the manifest
list's `first_row_id` values tile `0..next-row-id` exactly.

## Notes for the next reader

Five things this implementation got wrong first, and the gates caught:

1. **`!=` and `not-in` must never prune on bounds.** A bound is not necessarily
   a value that occurs — string upper bounds are rounded *up* — so
   `lower == upper == X` does not prove every value is `X`.
2. **Mojo's `Int64` `//` and `%` already floor** (Python semantics, on both
   toolchains). A hand-rolled `floor_div` that "corrects" them puts every
   pre-epoch timestamp one day early.
3. **The `snapshots` array is not in chronological order.** iceberg-rust writes
   it in hash order. Find the oldest snapshot by sequence number, not position.
4. **parquet.mojo's `load_i32`/`load_i64`/`load_f32`/`load_f64` take an
   *element* index, not a byte offset.** Passing `8 * i` reads eight elements
   past where you meant, and only row 0 looks right.
5. **Row positions must survive pruning.** Delete positions are absolute
   positions in the data file, so row groups are read one at a time with their
   absolute offset, and page pruning is off whenever a position matters.
6. **A `<V>-<uuid>.metadata.json` name cannot detect a race by itself.** Two
   writers at the same version pick different uuids, so both `create` calls
   succeed and one commit is lost. Java gets atomicity here only from
   `v<N>.metadata.json`, whose name is deterministic; everything else gets it
   from a catalog. This commits by checking that the file it based its version
   on is still the newest, writing, and then checking that nobody else claimed
   the same version — which closes the window but does not eliminate it, and
   the spec says as much.
7. **Truncating an upper bound stops it bounding.** `truncate(16)` on a lower
   bound is free; on an upper bound the result has to be incremented — the
   last code point of a string, the last byte below `0xFF` of a binary — and
   when every unit is already at its maximum the bound has to be *omitted*
   rather than written short.
8. **A relative `file://` location is not a URI.** `file://build/x` parses as
   host `build`, path `/x`, so handing one to an object store's URI parser
   silently addresses the wrong thing. Local locations lose their scheme
   before they get there.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
