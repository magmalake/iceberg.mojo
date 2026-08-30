# iceberg.mojo

> Part of **magmalake** — data lake building blocks in Mojo.

Native, pure-Mojo **Apache Iceberg**: read a table's `metadata.json`, pick a
snapshot, decode its manifests, plan a scan, and **read the rows** — deletes
applied, filters evaluated, projection resolved by field id. Over local files,
S3, GCS, Azure or plain HTTP, from a filesystem layout or a live REST catalog.
No JVM, no Python, no Rust.

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

```console
$ iceberg-mojo cat s3://warehouse/db/orders --filter '["=","region","eu"]' --limit 5
$ iceberg-mojo cat --rest https://catalog --table db.orders --token $TOKEN
```

## The stack

Everything under this line is another magmalake tin, consumed by source path.
iceberg.mojo is the part that knows what Iceberg *means*; the tins below know
what the bytes mean.

```
                         ┌──────────────────────────────┐
                         │        iceberg.mojo          │
                         │  metadata · snapshots        │
                         │  transforms · expressions    │
                         │  manifests · scan planning   │
                         │  puffin · reads · catalogs   │
                         └───┬─────────┬─────────┬──────┘
             manifests (Avro)│         │         │data files (Parquet)
                             │         │         │
        ┌────────────────────▼──┐   ┌──▼─────────▼──────────┐
        │      avro.mojo        │   │     parquet.mojo      │
        │  OCF, field ids       │   │  pages, encodings,    │
        │  deflate/snappy/zstd  │   │  Arrow C Data out     │
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
`objectstore.mojo` turns a location into bytes and supplies the HTTP client an
Iceberg REST catalog needs, because no Mojo HTTP package resolves from conda.
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
| Tests | | **92 passing**, identical on `stable` (Mojo 1.0.0) and `default` (nightly) |
| CI | | 4 jobs: {stable, nightly} × {ubuntu, macOS}, each running the REST mock and MinIO |

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
- **Output** as an Arrow `RecordBatch` (`export_c` hands it to anything
  speaking the Arrow C Data Interface), as CSV, or as Appendix-D JSON.
- **Lazy IO** as an option: fetch the footer, then only the byte ranges of the
  row groups that survive statistics pruning, into a buffer the size of the
  file with everything else left zero. Parquet addresses everything by absolute
  offset, so a sparse buffer decodes exactly like the whole file — and over a
  network that is one range request per row group instead of the whole object.

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
| Writes — appends, commits, compaction | Read-only. The write path is [iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo)'s bridge for now. |
| Nested columns in a scan | `to_table()` reads primitive columns; a struct/list/map projection raises. The *metadata* for them is complete. |
| Non-Parquet data files | ORC and Avro data files are rejected by name. Parquet is what every writer in reach produces. |
| Brotli-compressed Parquet | No Brotli in Mojo. Everything else — uncompressed, Snappy, GZIP, ZSTD, LZ4 — works. |
| Encryption | Neither Parquet modular encryption nor Iceberg's `encryption-keys` are applied. |
| Multipart upload, retries, connection reuse | objectstore.mojo's gaps; each request is a fresh TLS handshake. |
| `remote-signing` delegation | `vended-credentials` is implemented; remote signing is not. |
| Theta sketches | Listed from a Puffin footer, not decoded. |

## Performance

`pixi run bench` builds a one-million-row table and times whole `to_table()`
calls — metadata, plan, Parquet decode, value materialisation — then runs the
same scans through PyIceberg. M4, one core:

| Scan | iceberg.mojo | PyIceberg 0.11.1 |
|---|---|---|
| full scan, 1 M rows × 6 columns | 911 ms — **1.10 M rows/s** | 13 ms — 75 M rows/s |
| projection to 2 of 6 columns | 381 ms — **2.62 M rows/s** | 6 ms — 162 M rows/s |
| `region = 'eu'` → 200 k rows | 725 ms — 0.28 M rows/s | 12 ms — 17 M rows/s |
| `id > 900000` → 100 k rows | 185 ms — 0.54 M rows/s | 6 ms — 16 M rows/s |
| full scan, lazy IO | 838 ms — 1.19 M rows/s | — |

PyIceberg is roughly 70× faster, and the reason is structural rather than
subtle: `to_arrow()` hands Arrow buffers straight out of Arrow C++, while this
reader materialises a tagged `Datum` per cell on the way through. That is what
makes one code path serve every Iceberg type, every promotion and every delete
rule — and it is the obvious thing for a later pass to remove.

## Install

```sh
pixi run test              # 92 tests; starts the REST mock and MinIO
pixi run -e stable test
pixi run cli               # builds build/iceberg-mojo
pixi run bench
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
| `iceberg.read` | `ScanResult`, `ScanOptions`, `NameMapping`, the metadata columns |
| `iceberg.scan` | `TableScan`, `FileScanTask` — `plan_files`, `to_table`, `to_batches` |
| `iceberg.io` | `FileIO` over local, S3, GCS, Azure and HTTP |
| `iceberg.catalog.filesystem` | `Table`, `FilesystemCatalog` |
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

    for batch in t.scan().to_batches():      # Arrow, one per data file
        print(batch.num_rows, batch.num_columns())
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

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
