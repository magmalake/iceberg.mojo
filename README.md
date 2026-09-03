# iceberg.mojo

[![mojoshelf](https://mojoshelf.org/badge/iceberg-mojo.svg)](https://mojoshelf.org/tins/iceberg-mojo) [![mojo nightly](https://mojoshelf.org/badge/iceberg-mojo/nightly.svg)](https://mojoshelf.org/tins/iceberg-mojo)

> Part of [**magmalake**](https://magmalake.org) — data lake building blocks in Mojo.

Native, pure-Mojo **Apache Iceberg**: read a table's `metadata.json`, pick a
snapshot, decode its manifests, plan a scan, and **read the rows** — deletes
applied, filters evaluated, projection resolved by field id, **structs, lists
and maps included** — then **write**:
create a table, append to it, delete rows from it (copy-on-write, or
merge-on-read with real deletion vectors), overwrite it, and expire what is no
longer reachable. Manifests, snapshots and optimistic commits included. Over
local files, S3, GCS, Azure or plain HTTP, from a filesystem layout or a live
REST catalog. No JVM, no Python, no Rust.

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

_ = table.delete_where('["<","id",100]')          # copy-on-write by default
_ = table.overwrite([batch], '["=","region","eu"]')
_ = table.dynamic_partition_overwrite([batch])    # only the partitions it hits
_ = table.expire_snapshots(keep_last=5)           # and the files nobody needs
```

```console
$ iceberg-mojo cat s3://warehouse/db/orders --filter '["=","region","eu"]' --limit 5
$ iceberg-mojo cat --rest https://catalog --table db.orders --token $TOKEN
```

## The stack

**Native reads and native writes.** Scans decode Parquet through parquet.mojo
and come out as Arrow; writes push Parquet, Avro and Puffin back through the
same tins and commit against a filesystem layout or a REST catalog. Nothing
here calls out to another implementation — the
[iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo) bridge over
iceberg-rust is still a useful second opinion in the test fixtures, but **no
operation needs it any more**.

| Operation | How | Checked by |
|---|---|---|
| `create_table` | filesystem or REST `createTable` | PyIceberg, DuckDB |
| `new_append()` / `append` | new manifest, parent's carried by reference | PyIceberg, DuckDB, fastavro |
| `delete_where` — copy-on-write | affected files rewritten, originals `DELETED` | PyIceberg, DuckDB |
| `delete_where` — merge-on-read, v3 | one Puffin **deletion vector** per data file | PyIceberg, DuckDB, pyiceberg's Puffin reader + zlib CRC |
| `delete_where` — merge-on-read, v2 | **position delete files**, sorted `(file_path, pos)` | PyIceberg, DuckDB |
| metadata delete | files whose every row matches are removed unread | PyIceberg, DuckDB |
| `delete_by_equality` (v2) | an **equality delete file** and its `equality_ids` | **DuckDB** (PyIceberg refuses to plan them) |
| `overwrite(batches, filter?)` | copy-on-write delete + append, one snapshot | PyIceberg, DuckDB |
| `dynamic_partition_overwrite` | replaces exactly the partitions the rows land in | PyIceberg, DuckDB |
| `expire_snapshots` | prunes snapshots, then unreachable files | itself, file by file |
| nested columns, read and written | struct / list / map, sub-field projection, nested predicates, `identity(addr.city)` | PyIceberg, DuckDB, pyarrow over the C Data Interface |

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
    cores
        ┌───────────────┐
        │ threads.mojo  │
        │ parallel_for  │
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
`threads.mojo` is what `ScanOptions.num_workers` spends: file scan tasks are
shared-nothing, so a scan over many files is the obvious place for a second
core.

## Status

Every gate below was **measured**, by `pixi run test`, against fixtures
produced by three independent reference implementations: iceberg-rust 0.10.1
(through [iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo)),
PyIceberg 0.11.1, and DuckDB 1.5.5's `iceberg` extension. Nothing here is
self-checked — the expected values come from them, never from this code. See
[`tests/fixtures/PROVENANCE.md`](tests/fixtures/PROVENANCE.md).

| Gate | Oracle | Result |
|---|---|---|
| **(a)** Rows returned by `to_table()`, all columns, nulls included | PyIceberg | ✅ **65 / 65 cases** (11 tables × 6 filters, less the one PyIceberg refuses), **224 rows**, compared cell-exactly |
| **(a′)** The same rows, unfiltered | DuckDB `iceberg_scan` | ✅ **12 / 12 tables**, **75 rows** |
| **(a″)** Nested rows — struct, list and map — filter by filter | **DuckDB** (PyIceberg cannot project a list or a map) | ✅ **18 / 18 cases** (3 tables × 6 filters), **74 rows** |
| **(a‴)** Sub-field projection: `select(["addr.city", "id"])` | PyIceberg | ✅ **7 / 7 projections**, the struct holding exactly the fields asked for |
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
| **(m′)** Nested schema evolution — a field added, renamed and promoted **inside a struct** | PyIceberg | ✅ `addr.country` null in the older file, `addr.city`→`addr.town` found by id, `addr.zip` `int`→`long` reading 7 000 000 000 |
| **(m″)** A partition field whose `source-id` is a nested leaf, `identity(addr.city)` | PyIceberg | ✅ written *and* read; the partition filter prunes files on the nested leaf |
| **(m)** Column projection rules 2–5 — partition value, name mapping, `initial-default`, null | the spec | ✅ each reached by giving the reader a schema whose ids the file does not have |
| **(n)** Tables **we write** — rows, snapshots, partition values, statistics | PyIceberg **and** DuckDB | ✅ **10 / 10 tables** (5 partition shapes × v2/v3), 18 rows each, cell-exact both ways |
| **(o)** Row lineage on tables we write | **fastavro** + the v3 spec's rules | ✅ manifest-list `first_row_id`s tile `0..next-row-id` on all 5 shapes; data files inherit (null), as the spec requires. PyIceberg cannot check this — see below |
| **(p)** PyIceberg **appends to a table we created**, and we read the result | PyIceberg | ✅ 2 tables, 18 + 6 = 24 rows, `_row_id` still intact |
| **(q)** REST commit — requirements, 409 retry, `Idempotency-Key` replay, 5xx → `CommitStateUnknown` | the REST spec, against a mock that checks | ✅ v2 and v3 create + append; a rigged 409 retried; a rigged applied-then-500 recovered by the key, landing **one** snapshot; a server that will not deduplicate reported as unknown state |
| **(r)** `s3://` write end to end | itself, read back | ✅ create, 2 appends, 12 rows, partition pruning; MinIO verifies every signature |
| **(s)** Tables **we delete from and overwrite** — rows, snapshots, summaries | PyIceberg **and** DuckDB | ✅ **22 / 22 tables**: merge-on-read and copy-on-write deletes across unpartitioned / identity / bucket[4] / day at v2 and v3, a second delete that merges a vector, a filtered and an unfiltered overwrite, two dynamic partition overwrites, one equality delete — cell-exact both ways |
| **(t)** The **deletion vectors we write** | pyiceberg's own Puffin reader, and zlib for the CRC | ✅ every blob's length, `D1 D3 39 64` magic and CRC-32 checked here; the bitmap decoded by PyIceberg, and every position it names is a row that vanished |
| **(u)** The **position delete files we write** (v2) | pyarrow + the spec | ✅ sorted by `(file_path, pos)` under ids 2147483546 / 2147483545; every pair points at a row that vanished |
| **(v)** Manifest maintenance on a delete | fastavro | ✅ every `EXISTING`/`DELETED` entry carries its own `sequence_number`, `file_sequence_number` and (v3) `first_row_id`; every `ADDED` entry leaves them null; no manifest with nothing live survives |
| **(w)** PyIceberg **appends to a table we deleted from**, and we read the result | PyIceberg | ✅ 2 tables, 14 + 3 = 17 rows, our deletes still applied |
| **(x′)** Nested columns **we write** — rows, and per-leaf metrics | PyIceberg **and** DuckDB, against the JSON the writer was fed | ✅ **4 / 4 tables** (v2, v3, `identity(addr.city)`, `bucket[4](addr.zip)`), 12 rows each, cell-exact both ways; `value_counts` and `null_value_counts` for all **8 nested leaves** equal to the level records the rows imply |
| **(x″)** Delete and overwrite on a nested table | itself, cell by cell, before and after | ✅ a v3 **deletion vector** and a **copy-on-write rewrite** over struct + list + map columns leave every surviving row byte-identical; `overwrite`, `identity(addr.city)` and `bucket[4](addr.zip)` all round-trip |
| **(y)** A nested scan over the **Arrow C Data Interface** | `pyarrow.Array._import_from_c` | ✅ **20 columns** across 4 tables imported into pyarrow and equal to PyIceberg's own read — structs, lists and maps with their children |
| **(x)** `expire_snapshots` | itself, file by file | ✅ a dry run that touches nothing and never names a live file; an expiry that removes exactly what a copy-on-write delete orphaned; `keep_last` and an age cut; a superseded Puffin file removed while the live one stays |
| Tests | | **166 passing**, 0 skipped, identical on `stable` (Mojo 1.0.0) and `default` (nightly); the SQL-catalog tests run twice, on sqlite and on PostgreSQL |
| CI | | 5 jobs: {stable, nightly} × {ubuntu, macOS} each running the REST mock and MinIO, plus a write-interop job running PyIceberg and DuckDB against **36 tables** we wrote and importing a nested scan into pyarrow |

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
  physical width and produced at the table's current type. All of it applies
  **inside a struct** too — the file's children are matched to the table's
  fields by field id, so a field added, renamed, reordered or promoted within a
  struct resolves exactly as a top-level one does.
- **Nested columns** — `struct`, `list` and `map`, to any depth: `list<struct>`,
  `list<list<int>>`, `map<int, struct>`, a struct holding a list of structs.
  A column arrives as an Arrow tree (`iceberg.nested`), and every kernel a flat
  column gets has a counterpart that walks children and offsets, so deletes,
  the residual filter and `to_batches()` all work on them unchanged. Null
  containers stay distinct from empty ones.
- **Sub-field projection.** `select(["addr.city", "id"])` gives back the column
  `addr` holding only `city`, and **prunes the Parquet read to the leaves under
  it** — `addr.zip` is never decoded. On the benchmark table that is 3.5×
  faster than reading the whole struct.
- **Nested predicates.** `["=", "addr.city", "eu"]` binds to the leaf's field
  id, and the leaf is flattened out of its column with the parent structs'
  nulls folded in, so every vectorised kernel applies to it. `is-null` and
  `not-null` also bind against a struct, a list or a map itself. Row-group and
  page pruning use the nested leaf's own Parquet statistics.
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
- **Nested columns.** A struct, list or map column is aligned child by child
  **by field id**, so a batch whose struct has its fields in another order,
  under other names, or one field short still lines up with the table. The
  Iceberg `element-id`, `key-id` and `value-id` end up on the `element` and
  `key`/`value` nodes of the Parquet schema, which is where a reader looks for
  them. A partition field's `source-id` may name a **nested struct leaf** —
  `identity(addr.city)`, `bucket[4](addr.zip)` — which the spec allows and this
  writer does. `iceberg.batch.NestedBuilder` takes one JSON value per row, so a
  nested batch can be built without laying out offsets by hand.
- **Statistics.** Read back out of the footer the writer just produced —
  column sizes, value counts, null counts, and Appendix-D lower and upper
  bounds truncated the way `write.metadata.metrics.default`'s `truncate(16)`
  says, with an upper bound incremented so that truncating it does not stop
  it bounding. Whatever the writer decided the min and max were is what the
  manifest reports, which is the only way the two can agree. Nested leaves get
  the same treatment, keyed by their own field ids — the `element-id` of a
  list, the `key-id` and `value-id` of a map, every field of a struct. **This
  is where PyIceberg deliberately differs**: `pyiceberg.io.pyarrow` downgrades
  any column whose name contains a dot to `COUNTS`, so it writes no bounds for
  a nested leaf at all. Bounds are kept here, Java-style, because they are what
  lets `addr.zip > 5` prune a whole file.
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

### Delete and overwrite

An append never rewrites anything: its manifest list is the new manifest plus
the parent's, carried by reference. Everything else has to rewrite the
manifests that mention a file it removes, and `iceberg.commit` is where that
happens. The parent's manifest list is walked once:

- a manifest that mentions nothing removed is **carried by reference** — same
  path, same counts, same `sequence_number`, `min_sequence_number` and
  `first_row_id`. A delete of one row does not touch the rest of the table;
- a manifest that does is **rewritten**: the entries it keeps become
  `EXISTING` with their `sequence_number`, `file_sequence_number`,
  `snapshot_id` and (v3) `first_row_id` spelled out, because inheritance is
  **ADDED-only** and a null there would silently re-date the rows — which
  would change which deletes apply to them. The entries it drops become
  `DELETED` with the same numbers, so an expiry can still find the files;
- a rewritten manifest with nothing live left is **dropped from the list**
  rather than written empty.

`write.delete.mode` picks the strategy, and the default is **copy-on-write**,
as in Java (PyIceberg has nothing else):

- **copy-on-write** rewrites every affected data file without the matching
  rows and marks the originals `DELETED`. The result carries no delete files,
  so any reader can read it.
- **merge-on-read** leaves the rows where they are. On **v3** that is a
  deletion vector: one Puffin `deletion-vector-v1` blob per data file, and a
  manifest entry with `content=1`, `file_format=puffin`,
  `referenced_data_file`, `record_count` = cardinality, and the
  `content_offset` / `content_size_in_bytes` the footer must match exactly. On
  **v2** it is a position delete file per partition: Parquet `(file_path,
  pos)` under the reserved ids 2147483546 and 2147483545, sorted by path then
  position.

A **second** merge-on-read delete against a file that already has a vector
reads the old one, unions the new positions in, and marks the old entry
`DELETED` — the spec makes a new vector absorb everything it replaces, because
a reader that finds a vector ignores every position delete file for that data
file.

Either way, a data file whose rows *all* match is removed rather than marked
up. When the scan's residual for it is already `true` — the partition
predicate proved it — the file is never opened: `DELETE WHERE region = 'eu'`
on an `identity(region)` table costs one manifest rewrite and reads no
Parquet at all.

`overwrite(batches, filter?)` is a copy-on-write delete and an append in one
`overwrite` snapshot; with no filter it replaces the table.
`dynamic_partition_overwrite(batches)` derives the filter from where the new
rows land and replaces exactly those partitions, examining no row.

`expire_snapshots(older_than_ms, keep_last, dry_run)` drops old snapshots and
then the files nothing points at. The keep set is built from the *retained*
snapshots first — a data file added five snapshots ago is in every snapshot
since, and expiring the oldest of them must not touch it — and only then is
the expired snapshots' inventory subtracted from it. The pruned metadata is
committed **before** any file is deleted, so an interruption leaves orphans
rather than a snapshot with holes in it.

### Puffin and deletion vectors

`iceberg.puffin` reads a footer (`PFA1`, the little-endian size and flags, the
LZ4-or-plain JSON `FileMetadata`) and decodes `deletion-vector-v1` blobs
through roaring.mojo, which verifies the blob's big-endian length, its
`D1 D3 39 64` magic and its CRC-32. A scan never reads the footer: the delete
manifest entry already carries `content_offset` and `content_size_in_bytes`,
and the spec requires them to match it exactly, so the blob is read directly.
`apache-datasketches-theta-v1` blobs are listed with their metadata (`ndv` and
all) but their sketches are not decoded.

`PuffinWriter` is the other direction. Blobs go into the body as they arrive,
each recording the offset and length its `BlobMetadata` will claim, and
`finish` appends `Magic payload size flags Magic` — the payload plain JSON, or
one LZ4 frame with bit 0 of the flags set, which is the only footer
compression the format defines. `add_deletion_vector` writes a
`deletion-vector-v1` blob the one way the spec allows: never compressed,
`snapshot-id` and `sequence-number` both -1, empty `fields`, and the
`referenced-data-file` and `cardinality` properties a reader needs.

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
  spec in the manifest's own Avro file metadata. Both are read through
  avro.mojo's `RecordCursor`: the file's schema is compiled into a decode plan
  once, and every field this library reads — including the partition columns
  the manifest's own spec names — is a slot number resolved once per file. A
  field a given format version does not have resolves to -1, which is how the
  version differences stay expressible.
- **Scan planning** — manifest pruning, data-file pruning by partition and by
  metrics, residuals, and delete-file association by the spec's scope rules. A
  scan carries a `ManifestCache`, so the schema and spec its manifests share —
  byte-identical across all of them, in the usual case — are parsed once
  rather than once per file.
- **Catalogs** — a filesystem catalog (`version-hint.text` or highest-versioned
  `*.metadata.json`, ties broken by `last-updated-ms`), gzipped metadata, a
  **REST catalog** over HTTPS, and a **SQL catalog** over sqlite.mojo (local
  development and PyIceberg test parity) or postgres.mojo (the same catalog,
  shared by several writers).

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

### SQL catalog

`SqlCatalog` runs on **sqlite.mojo or postgres.mojo**, chosen by the URI and
nothing else — every method below is the same either way. What that buys is
two different things.

Over **sqlite** a SQL catalog is **not a production catalog**: it is local
development and test parity with PyIceberg, whose quickstart default is
exactly this — a SQLite-backed `SqlCatalog` — and which is what
iceberg-rs.mojo's own fixtures already use as an oracle.

Over **PostgreSQL** the same catalog is deployable. `iceberg_tables` and
`iceberg_namespace_properties` are the tables PyIceberg's `SqlCatalog` and the
Java `JdbcCatalog` both use, the guarded `UPDATE` is a real transaction
against a real server, and several writers may share it — which is the one
thing the sqlite file cannot do. It is still not `RestCatalog`: there is no
credential vending and no server-side validation beyond the primary key.

Either way, point it at the database PyIceberg would use and each side reads
what the other wrote: same two tables, same column names, same guarded-update
concurrency scheme.

```mojo
from iceberg.catalog.sql import SqlCatalog

var catalog = SqlCatalog.local("default", "sqlite:///catalog.db", "warehouse")
catalog.create_namespace("db", {"owner": "marius"})

var t = catalog.create_table("db", "orders", schema, spec)
_ = catalog.append("db", "orders", [batch])
_ = catalog.delete_where("db", "orders", '["<","id",100]')
_ = catalog.rename_table("db", "orders", "db", "orders_v2")

# the same catalog, shared:
var shared = SqlCatalog.local(
    "default", "postgresql://iceberg@db.internal/catalog?connect_timeout=5",
    "s3://warehouse",
)
```

The URI is libpq's, so every conninfo option works — `sslmode=require`,
`connect_timeout`, `options=-csearch_path%3Dmyschema` — and it is the same
string SQLAlchemy takes, which is how PyIceberg is pointed at the same rows
(`postgresql+psycopg://…`).

Namespaces are dot-joined strings (`"db.sub"`), matching PyIceberg's own
`SqlCatalog` convention; nested namespaces are a `LIKE`-prefix match over that
string, escaped the same way PyIceberg escapes `%`, `_` and the escape
character itself. The commit path (`append`, `delete_where`, `overwrite`,
`dynamic_partition_overwrite`) writes a fresh metadata JSON file through the
same `prepare_append` / `prepare_delete` / `prepare_overwrite` machinery the
REST catalog uses, then swaps the catalog row's `metadata_location` with a
guarded `UPDATE ... WHERE metadata_location = <the value this attempt read>`
— PyIceberg's own optimistic-concurrency guard. Zero rows affected means a
concurrent commit already moved the pointer; this reloads and retries, the
same shape as the REST catalog's 409 handling, and raises once retries run
out rather than clobbering the row a successful commit just wrote.

`iceberg-mojo cat --sql sqlite:///catalog.db --table db.orders --warehouse W`
reads a table through it from the CLI, and `--sql postgresql://user@host/db`
is the same command against a server. `tools/verify_sql_catalog.py` and
`tools/verify_pg_catalog.py` are the parity checks, one per backend: writing
through `SqlCatalog` and reading the same database back with PyIceberg's, and
the other way round — including a catalog PyIceberg created from nothing over
Postgres, which this reader opens unchanged — rows compared cell-exact. The
suite's own SQL-catalog tests run twice, once per backend, whenever
`$POSTGRES_TEST_DSN` names a server (`tests/run_tests.sh` starts a throwaway
one from the conda `postgresql` package; no Docker).

## Deliberately out of scope

| Not here | Why |
|---|---|
| Compaction / `rewrite_manifests` / `rewrite_data_files` | The machinery is here — `iceberg.commit` writes a snapshot that adds and removes files — but no operation drives it, and nothing measures whether the result is any faster. |
| Row lineage across a **copy-on-write** rewrite | A v3 rewrite should carry each row's `_row_id` into the new file; this one assigns fresh ids to rewritten rows. Merge-on-read, which is where v3 wants a delete to go, moves nothing and preserves them exactly. |
| Removing a position delete *file* when a vector absorbs it | One position delete file can serve several data files, so dropping it would resurrect rows in the others. The superseded *vectors* are removed; a leftover position delete file is inert, because a reader that finds a vector ignores them. |
| Equality deletes on a partitioned table, or on v3 | One unpartitioned equality delete file applies everywhere; pairing one with a partition tuple needs a caller who knows the data. v3 replaced them with deletion vectors and v4 deprecates writing them. |
| Schema and spec evolution as a write | `create_table` fixes both; changing them afterwards is a commit this build does not construct. |
| Writing **format version 1** | The v1 manifest schemas are generated and would be written, but nothing verifies them — v1 has no writers left to check against. v2 and v3 are gated end to end. |
| A predicate *inside* a list or a map | `a.b > 5` on a struct leaf works; `tags.element = 'x'` is refused, because "the row matches" is not a question a per-row filter can answer about a repeated field. `is null` on the container itself does work. |
| An `initial-default` that is itself a struct, list or map | A primitive default resolves at any depth, which is the case schema evolution produces. A nested default falls back to null — which is what the spec tells a reader that does not understand a default to do anyway. |
| Non-Parquet data files | ORC and Avro data files are rejected by name. Parquet is what every writer in reach produces. |
| Brotli-compressed Parquet | No Brotli in Mojo. Everything else — uncompressed, Snappy, GZIP, ZSTD, LZ4 — works. |
| Encryption | Neither Parquet modular encryption nor Iceberg's `encryption-keys` are applied. |
| A *safe* filesystem commit over an object store | There is no atomic create-if-absent on S3, so two writers can both believe they won. The spec says the same; use a REST catalog. |
| `remote-signing` delegation | `vended-credentials` is implemented; remote signing is not. |
| Theta sketches | Listed from a Puffin footer, not decoded. |

## Performance

`pixi run bench` builds a one-million-row table over four data files and times
whole scans — metadata, plan, Parquet decode, casts, deletes, filter — then
runs the same scans through PyIceberg. Both sides take the **best of three,
warm**: one cold run measures the page cache and the allocator (or the
interpreter) warming up as much as it measures the scan, and it measures them
differently for the two implementations. M4, six columns (two longs, a string,
a double, a timestamp, a boolean), ZSTD — which is what PyIceberg writes.

| Scan | 0.3.1 | **0.4.0, one core** | **0.4.0, 4 workers** | PyIceberg 0.11.1 |
|---|---|---|---|---|
| full scan, 1 M rows × 6 columns | 163 ms | **35.6 ms** — 28.1 M rows/s | **12.0 ms** — 82.8 M rows/s | 7.9 ms — 126 M rows/s |
| the same, `to_table` | 159 ms | **43.3 ms** — 23.1 M rows/s | **15.5 ms** — 64.5 M rows/s | — |
| projection to 2 of 6 columns | 53 ms | **13.7 ms** — 72.7 M rows/s | **5.3 ms** — 187 M rows/s | 5.5 ms — 183 M rows/s |
| `region = 'eu'` → 200 k rows | 165 ms | **48.6 ms** — 4.1 M rows/s | **16.1 ms** — 12.4 M rows/s | 9.7 ms — 20.6 M rows/s |
| `id > 900000` → 100 k rows | 39 ms | **11.7 ms** — 8.5 M rows/s | 12.1 ms | 5.6 ms — 17.7 M rows/s |
| full scan, lazy IO | 169 ms | **35.5 ms** — 28.2 M rows/s | — | — |

`id > 900000` does not move with workers, and should not: statistics pruning
leaves one data file, so there is one task to hand out.

PyIceberg's column is **one process, not one core** — `to_arrow()` hands each
file to pyarrow, which uses its own thread pool. Against a single pyarrow
thread the same four files decode in 26.8 ms, so the comparable single-core
gap on the full scan is about 1.3×, and at four workers about 1.5×.

### Where the 163 ms was

`pixi run profile` splits a scan into stages, so a fix can be aimed rather than
guessed at. Almost all of it was in one place, and that place was not this
repository.

| stage | what it covers | 0.4.0 |
|---|---|---|
| `load` | find the metadata file, parse the JSON | 0.2 ms |
| `plan` | manifest list, manifests, entries, residuals | 0.9 ms |
| `io` | read the four data files | 1.7 ms |
| `open` | the Thrift footers | 0.1 ms |
| `decode` | parquet.mojo, nothing of Iceberg's | 32.4 ms |
| `read` | decode plus cast, deletes, residual, assembly | 35.3 ms |
| — the Iceberg layer alone | `read` minus `decode` | **2.9 ms** |
| `concat` | stitching the batches into one `ScanResult` | 7.9 ms |

**1. zstd.mojo opened libzstd once per Parquet page — 153 ms of the 163.** The
bench table is ZSTD (PyIceberg's default) and its four files hold 332 pages;
`zstd.decompress` did a `dlopen`/`dlclose` per call, which on macOS costs about
450 µs whether or not the library is already resident. Decompressing all 332
pages took **152.9 ms**, of which about 25 ms was libzstd. Fixed in zstd.mojo
0.1.1, along with the identical bug in lz4.mojo 0.1.1: the handle now lives in
a process-wide `_Global`, opened on first use and never closed. The same 332
pages now decompress in **33.5 ms**, and the fixed cost of a small
`zstd.decompress` went from ~415 µs to ~1.4 µs.

**2. parquet.mojo assembled each Arrow value a byte at a time.** `load_i64` and
friends did eight bounds-checked loads and eight shifts per `int64`; Arrow's
values buffer is the target's own layout, so it is one unaligned load.
Comparing a million `int64`s went from 10.2 ms to 1.7 ms (parquet.mojo 0.3.2).

**3. `batch_size` defaulted to 8192**, which chopped each row group into 31
batches. It does not save memory — `ParquetReader._load` materialises the whole
column chunk either way — and it multiplies the per-batch work: `concat` for
the million-row scan was 15.6 ms at 8192 and is 7.9 ms with one batch per row
group. (It costs nothing measurable in `decode`; an earlier reading that said
otherwise was measuring a cold page cache.)

**4. The kernels stopped working one row at a time where they did not have to.**
`filter_array` copies each maximal *run* of kept rows with one `extend` instead
of moving a fixed-width value byte by byte; `concat_into` never materialises an
all-ones validity bitmap when neither side has a null; a comparison operator
resolves to three booleans once instead of being re-dispatched per row; and a
UTF-8 column's predicate reads its own 32-bit offsets rather than calling
`value_extent` per row. Together: `to_table` 51 → 43 ms, `filter id>900000`
19 → 13 ms.

**5. The residual's own bool-per-row vector is the selection vector.** A batch
used to allocate a second million-entry `List[Bool]`, fill it with `True`, and
walk it to AND in what `_selection` had just computed. When there are no
position deletes — the common case — the residual's answer *is* the answer.

**6. `lazy` IO filled its sparse buffer one byte at a time**, twice, and made a
whole extra copy of it for the probe reader. It now reads the row-group extents
in offset order and fills the buffer front to back with `extend`: 47.1 → 35.5
ms, which matters because `lazy` is what the `s3://` path uses.

What is left is Parquet decoding, and about 25 ms of that 32 is libzstd.

### More than one core

`ScanOptions.num_workers` — 1 (the default, and what every caller had before
0.4.0), 0 for one per core, or a count — reads data files on
[threads.mojo](https://github.com/magmalake/threads.mojo) workers. File scan
tasks are shared-nothing: each opens its own file, decodes into its own arena,
applies its own deletes and evaluates its own residual. Each writes its result
into its own slot, so the merge afterwards is by task index and the rows and
their order do not depend on which worker finished first — there is a test for
exactly that, over every fixture table, at 1, 2, 4 and 8 workers. Concatenating
for `to_table` parallelises along the other axis: one task per column, each the
only writer of its own arena.

`pixi run bench` also builds a two-million-row table over **eight** data files:

| workers | `to_table` | speedup | `to_batches` | speedup |
|---|---|---|---|---|
| 1 | 88.7 ms | 1.00× | 71.4 ms | 1.00× |
| 2 | 48.2 ms | 1.84× | 38.8 ms | 1.84× |
| 4 | 31.1 ms | 2.85× | 22.9 ms | 3.11× |
| 8 | **25.4 ms** | 3.49× | **17.6 ms** | 4.04× |

PyIceberg reads the same table in 15.1 ms in one process.

**~4× is the ceiling, and it is not a scheduling problem.** Decoding Parquet is
mostly moving bytes, and threads.mojo's own parallel-memcpy benchmark stops
scaling at four workers on this machine for the same reason. `to_table` scales
slightly worse than `to_batches` because the concatenation it adds is a second
pass over every byte.

A scan with a `limit` ignores `num_workers` and stays sequential: stopping
early is only meaningful in order.

### Nested columns

The same benchmark over 200 000 rows of `id long`, `addr struct<city string,
zip int>` and `tags list<string>` (two elements a row on average), which
`pixi run bench` also builds:

| Scan | 0.3.1 | **0.4.0** | PyIceberg 0.11.1 |
|---|---|---|---|
| full scan, struct + list | 63 ms | **13.5 ms** — 14.8 M rows/s | 4.4 ms — 45.3 M rows/s |
| projection `addr.city, id` | 14 ms | **4.3 ms** — 46.3 M rows/s | 3.5 ms — 56.8 M rows/s |
| `addr.city = 'eu'` → 36 923 rows | 36 ms | **15.9 ms** — 2.3 M rows/s | 5.0 ms — 7.4 M rows/s |
| `addr.zip > 90000` → 18 460 rows | 18 ms | **8.0 ms** — 2.3 M rows/s | 4.6 ms — 4.0 M rows/s |

The row worth reading is the second — **3.1× the full scan** — which is the
sub-field projection actually pruning the Parquet read rather than throwing
decoded columns away.

### Writing

The same million rows, appended to a fresh table in four commits of 250 000 —
building the Arrow batches, writing the Parquet, reading the footers back for
the statistics, writing the manifest, the manifest list and the
`metadata.json`, all inside the measurement:

| | iceberg.mojo | PyIceberg 0.11.1 |
|---|---|---|
| append 1 M rows × 6 columns, 4 commits | 233 ms — **4.28 M rows/s** | 165 ms — 6.03 M rows/s |
| Parquet written (zstd) | 3 MB | 9 MB |

1.4× slower, and the whole of that is the Parquet *encoder*: the manifest, the
manifest list and the metadata are four small Avro files and one JSON document
per commit. The 3 MB against 9 MB is a difference in dictionary encoding, not
in content — PyIceberg reads all 1 000 000 rows back out of those four files
and agrees on every column, including both null counts. (This was 917 ms
before zstd.mojo 0.1.1: the writer paid the same per-page `dlopen`.)

### Planning over many manifests

Planning touches no Parquet at all: it reads the snapshot's manifest list and
then every manifest the list does not let it skip, one `manifest_entry` per
data file. That is pure Avro. 0.4.2 moved both readers onto avro.mojo's
schema-compiled `RecordCursor`, which cut the *per-entry* cost 4.3× and left
a fixed ~114 µs per manifest that decoding entries was never part of. 0.4.3
goes after that fixed cost.

`pixi run bench` builds two tables of **500 manifests** each (one commit per
manifest, by this library's own writers), differing only in how many entries
a manifest holds, and times `plan_files()` warm, best of five. Two entry
counts is what separates the two costs — `t = manifests × (fixed + entries ×
per_entry)`, so the slope is the per-entry cost and the intercept is what
opening a manifest costs on its own:

| | 0.4.2 | **0.4.3** | |
|---|---|---|---|
| 4 entries each — 2 000 file tasks | 62.3 ms | **21.4 ms** | 2.9× |
| 20 entries each — 10 000 file tasks | 84.0 ms | **43.4 ms** | 1.9× |
| per entry | 2.71 µs | 2.74 µs | — |
| **fixed, per manifest** | **113.7 µs** | **31.9 µs** | **3.6×** |

(Measured back to back in one session on an M4; the 0.4.2 column is the same
binary the 55.4 / 74.2 ms above were taken from, on a slightly busier
machine.)

Nothing about decoding an entry changed — the per-entry column is flat, as it
should be. What changed is that a scan now carries a `ManifestCache`. Five
hundred manifests written by one writer carry five hundred byte-identical
copies of the same 5 KB `avro.schema`, the same Iceberg `schema` and the same
`partition-spec`, and 0.4.2 parsed every copy of all three. The cache is keyed
on those bytes *before* they are parsed — byte equality is the cheap
sufficient condition — and holds everything they determine: avro.mojo's
compiled decode plan, the parsed schema and spec, the partition type, and the
`_EntrySlots` slot table. The hash over the key is only a filter; a hit is
confirmed by comparing the bytes, so a collision is slow and never wrong.

**What is left.** Of the 31.9 µs a manifest still costs before its first
entry:

| | µs/manifest |
|---|---|
| `open` + `read` + `close` of the file | 11.5 |
| the manifest list's own entry for it | ~3 |
| OCF header (the metadata map) | ~2 |
| the cursor's slot buffers, one set per file | ~3 |
| shape lookup, `DataFile` assembly, evaluators | the rest |

The file read is now **36% of the fixed cost and 27% of the whole plan**, and
it is a floor: 500 files of about 4 KB, one `open`/`read`/`close` each, at
~11 µs apiece on this machine — a raw `open()` + `read_bytes()` with no
`FileIO` around it measures the same. There is no range-coalescing to be had
across separate files and caching their *contents* would be a different
promise, so this library does not. The next measurable item is the cursor's
slot buffers: one `List` per slot is allocated per file, and a cursor
recycled across files with the same plan would save about 3 µs of the 32.

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
   value by definition. A nested cell has no scalar to be: `value` hands back
   its canonical JSON as a string `Datum`, and `cell` gives the same text
   without the `Datum` around it.

A predicate on a *constant* column — an identity partition value, an
`initial-default` — is evaluated once for the whole batch rather than per row.

## Install

```sh
pixi shelf add iceberg-mojo
```

Working with a coding agent? `npx skills add mojoshelf/mojoshelf --skill mojoshelf-consume --yes` teaches it to find and install tins itself — it installs the `shelf` CLI too.

That resolves the tin from [mojoshelf](https://mojoshelf.org) and adds it — along with the tins it depends on — as **pixi git source dependencies**. magmalake tins are not published to a conda channel, so `pixi add iceberg-mojo` will not find them.

`iceberg-mojo` sits at the top of the stack, so this pulls every tin beneath it — that is expected, not a misconfiguration.

Consume it with:

```
-I ../iceberg.mojo/src -I ../hashes.mojo/src -I ../avro.mojo/src \
-I ../thrift.mojo/src -I ../snappy.mojo/src -I ../parquet.mojo/src \
-I ../roaring.mojo/src -I ../objectstore.mojo/src \
-I ../zstd.mojo/src -I ../lz4.mojo/src -I ../brotli.mojo/src \
-I ../threads.mojo/src -I ../sqlite.mojo -I ../postgres.mojo/src
```

Sibling tins are consumed by **source path**, not as pixi packages:
pixi-build-mojo emits a precompiled artifact built with `mojo-compiler` 1.0.0,
and the nightly compiler refuses to load it. Source paths satisfy both
environments. The four FFI tins (objectstore, zstd, lz4, brotli) are *also*
pixi git source dependencies, which is what installs their C shims into the
environment; a consumer needs the same. sqlite.mojo and postgres.mojo (the SQL
catalog's two backing stores) are the same shape — a git dependency purely so
pixi installs `libsqlite` / `libpq`, with the Mojo source itself still coming
from the path checkout — except sqlite.mojo's package directory is `sqlite/`
at its repo root, not `src/`, so the include path is `-I ../sqlite.mojo`, not
`-I ../sqlite.mojo/src`.
threads.mojo has no shim at all — it calls the pthread symbols libc already
exports — so a source checkout is all it needs. The same precompiled-package
problem rules out EmberJson, which is why `iceberg.json` is a small in-repo
parser.

### Working on this repo instead

```sh
pixi run test              # 166 tests; starts the REST mock, MinIO and
                           # a throwaway PostgreSQL server
pixi run -e stable test
pixi run cli               # builds build/iceberg-mojo
pixi run bench             # scans and appends, against PyIceberg
pixi run profile           # per-stage profile of a scan: where the time goes
pixi run verify-writes     # writes 10 tables; PyIceberg and DuckDB read them
pixi run verify-sql-catalog # SqlCatalog vs PyIceberg's, both directions
pixi run verify-pg-catalog  # the same, over PostgreSQL
```

## API

| Module | What it gives you |
|---|---|
| `iceberg.json` | `parse_json`, `Json` — an arena DOM with exact `Int64` handling |
| `iceberg.types` | `TypeStore`, `NestedField`, the primitive kinds |
| `iceberg.schema` | `Schema` — `find_field(id)`, `find_by_name` (dotted), `select(ids)`, `struct_path` |
| `iceberg.values` | `Datum`, `compare`, Appendix-D binary and JSON single values |
| `iceberg.transforms` | `Transform`, `PartitionSpec`, `SortOrder`, `bucket_of` |
| `iceberg.expressions` | `parse_filter`, `bind`, projections, the two evaluators |
| `iceberg.metadata` | `TableMetadata`, `Snapshot`, `SnapshotRef`, snapshot selection |
| `iceberg.manifest` | `read_manifest_list_io`, `read_manifest_io`, `DataFile`, `ManifestCache` |
| `iceberg.puffin` | `PuffinFile`, `PuffinWriter`, `BlobMetadata`, `read_deletion_vector` |
| `iceberg.kernels` | the columnar kernels: `cast_array`, `constant_array`, `filter_array`, `concat_into` |
| `iceberg.nested` | the same kernels for a struct/list/map tree: `cast_column`, `filter_tree`, `concat_tree`, `cell_json`, `flatten_leaf` |
| `iceberg.read` | `ScanResult`, `ScanOptions` (`limit`, `lazy`, `prune`, `batch_size`, `num_workers`), `NameMapping`, the metadata columns |
| `iceberg.batch` | `ColumnBuilder`, `NestedBuilder`, `batch_of`, `batch_of_columns` — Mojo values to an Arrow batch |
| `iceberg.write` | `write_data_files`, `WriteOptions`, bound truncation, partition paths |
| `iceberg.manifest_write` | `write_manifest`, `write_manifest_list`, the per-version Avro schemas |
| `iceberg.append` | `prepare_append`, `AppendResult`, metadata file naming |
| `iceberg.commit` | `prepare_commit`, `FileChanges` — a snapshot that adds *and* removes |
| `iceberg.delete` | `prepare_delete`, `prepare_overwrite`, `write_deletion_vectors`, `write_position_deletes`, `write_equality_deletes` |
| `iceberg.maintain` | `expire_snapshots`, `delete_expired_files`, `ExpireResult` |
| `iceberg.scan` | `TableScan`, `FileScanTask` — `plan_files`, `plan_files_with`, `to_table`, `to_batches` |
| `iceberg.io` | `FileIO` over local, S3, GCS, Azure and HTTP |
| `iceberg.catalog.filesystem` | `Table`, `AppendFiles`, `FilesystemCatalog` |
| `iceberg.catalog.rest` | `RestCatalog`, `RestCatalogConfig`, `LoadTableResult` |
| `iceberg.catalog.sql` | `SqlCatalog`, `NamespacePropertiesUpdateSummary` — local dev / PyIceberg parity, not production connectivity |

```mojo
from iceberg.catalog.filesystem import Table
from iceberg.read import ScanOptions

def main() raises:
    var t = Table.load_local("/warehouse/db/orders")

    var options = ScanOptions()
    options.limit = 100
    options.lazy = True

    var wide = ScanOptions()
    wide.num_workers = 0        # one file-scan worker per core; 1 is default

    var rows = (
        t.scan()
        .filter('["and",[">","id",2],["=","region","eu"]]')
        .select(["id", "region", "amount", "_file", "_pos"])
        .to_table(options)
    )
    print(rows.num_rows(), "rows")
    print(rows.to_json())

    for batch in t.scan().to_batches(wide):  # Arrow, straight off the kernels
        print(batch.num_rows, batch.num_columns())
```

Nested columns need no separate API — a dotted name is a column name, and a
nested cell prints as JSON:

```mojo
    # `addr` comes back holding only `city`; `addr.zip` is never decoded.
    var some = t.scan().select(["addr.city", "id"]).to_table()

    # A predicate on a struct leaf, and one on the container itself.
    var eu = t.scan().filter('["=","addr.city","eu"]').to_table()
    var no_tags = t.scan().filter('["is-null","tags"]').to_table()

    print(eu.cell(0, 1))     # {"city":"eu","zip":10}
    print(eu.to_json())      # nested, all the way down
    print(eu.to_csv())       # the same JSON, as one CSV field
```

Building a nested batch to write takes one JSON value per row:

```mojo
from iceberg.batch import ColumnBuilder, NestedBuilder, batch_of_columns

    var ids = ColumnBuilder.of(schema, 1)
    var addr = NestedBuilder.of(schema, 2)      # struct<city, zip>
    var tags = NestedBuilder.of(schema, 3)      # list<string>
    var props = NestedBuilder.of(schema, 4)     # map<string, long>
    ids.add(Datum.long_(1))
    addr.add('{"city":"eu","zip":10}')
    tags.add('["a","b"]')
    props.add('{"keys":["x","y"],"values":[1,2]}')
    var batch = batch_of_columns(
        [ids^.build_tree(), addr^.build(), tags^.build(), props^.build()]
    )
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
    table.refresh()

    # Copy-on-write, because that is the default. `merge-on-read` writes
    # deletion vectors on v3 and position delete files on v2 — either through
    # the `write.delete.mode` table property or as an argument here.
    print(table.delete_where('["=","region","us"]'), "rows deleted")
    table.refresh()

    # And the files nothing points at any more, once the old snapshots go.
    var expired = table.expire_snapshots(keep_last=1)
    print(len(expired.expired), "snapshots,", expired.total_deleted(), "files")
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

A column name may be **dotted** to reach inside a struct — `addr.city`,
`shipping.address.zip` — and such a predicate prunes on that leaf's own
statistics. `is-null` and `not-null` also apply to a struct, a list or a map
itself (`["is-null","tags"]`). Anything else on a container, and any predicate
that would have to look *inside* a list or a map (`tags.element`,
`props.value`), is refused with a message saying so.

## CLI

```sh
iceberg-mojo describe  <table>
iceberg-mojo schema    <table>
iceberg-mojo snapshots <table>
iceberg-mojo files     <table> [options]
iceberg-mojo cat       <table> [options]
```

`<table>` is a `metadata.json`, a table directory, a `file://` or `s3://` URI,
or — with `--rest URL --table NS.NAME` — a table in a REST catalog, or —
with `--sql URI --table NS.NAME` — a table in a SQL catalog (local dev / test
parity with PyIceberg, not a production catalog; see [SQL
catalog](#sql-catalog)).

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
| `--sql URI`, `--table NS.NAME`, `--warehouse W` | SQL catalog, e.g. `--sql sqlite:///catalog.db` |

```sh
iceberg-mojo cat tests/fixtures/dv_v3 \
  --rebase 'file:///…/warehouse/db=tests/fixtures'
```

## Fixtures

Twelve tables, one coherent warehouse, ~2.1 MB with their Parquet. Full detail in
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
| `nested_v2` | every nested shape at once: struct, `list<string>`, `map<string,long>`, `list<struct>`, `list<list<int>>`, `map<int,struct>`, struct-in-list-in-struct | `tools/make_nested_tables.py` |
| `nested_evo_v2` | schema evolution **inside** a struct: a field added, one renamed, `int`→`long` | `tools/make_nested_tables.py` |
| `nested_part_v2` | partitioned by `identity(addr.city)` — a nested struct leaf | `tools/make_nested_tables.py` |

Three of these were not writable by anything to hand, and the workarounds are
recorded rather than hidden:

- **`deletes_v2`.** PyIceberg's `Table.delete()` falls back to copy-on-write
  (*"Merge on read is not yet supported"*), so the position delete file was
  written with a `ManifestWriterV2` subclass reporting `ManifestContent.DELETES`.
- **`eq_deletes_v2`.** PyIceberg has no equality-delete writer and *refuses to
  plan* a scan of such a table at all, so DuckDB is its only oracle.
- **`nested_v2`.** PyIceberg 0.11.1 cannot answer a filter on a list or a map
  at all — `tags IS NULL` raises *"Cannot explicitly project List or Map
  types"* — so DuckDB is the only oracle for that one filter, and
  `oracle/rows_duckdb_<k>.json` exists for all six filters of all three nested
  tables. `tools/make_nested_fixtures.sh` rebuilds just these three in place.
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

Eleven things this implementation got wrong first, and the gates caught:

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
9. **A projection that reaches inside a struct renumbers it.** `select(["a.c"])`
   gives back a struct whose only field is `c`, so the child position of `c` in
   the *projected* type is 0 whatever it was in the table schema. A filter leaf
   inside that column has to record its path against the projected type, not
   against the schema, or `a.c > 5` silently reads `a.b`.
10. **A Parquet statistics predicate must address a leaf by its dotted path.**
   `addr.city` and a top-level column called `city` are two different leaves
   with the same short name; pruning on the wrong one drops rows.
11. **A null container is not an empty one.** A null list has `n + 1` equal
   offsets over an empty child, an empty list has the same offsets and a
   *valid* bit, and the difference survives Parquet only if the definition
   levels do. Half the nested fixtures exist to keep the two apart.

Two upstream bugs the nested gates turned up, both reproducible without any
Iceberg in the picture:

- **pyarrow 25.0.1 miscounts a fixed-width leaf under a `list<struct>`.**
  Write a `list<struct<s: string, i32: int32>>` in which some rows' lists are
  null or empty, and the Parquet `Statistics` for `i32` claim more non-null
  values than the file's own data contains — the empty-container slots are
  counted as present. The `string` leaf beside it is correct, and reading the
  values back gives the right answer, so it is the statistics alone.
  `tools/verify_written.py` therefore gates our `null_value_counts` against the
  level records the rows imply and names the leaves where pyarrow disagrees
  with itself.
- **PyIceberg 0.11.1 loses the null on a `list<struct>` and a
  `map<K, struct>`.** A null list of structs comes back as `[]` — on the way
  *out* as well as in: a fixture PyIceberg writes from `None` has `[]` in the
  Parquet file. pyarrow reading the same file directly, DuckDB 1.5.5 and this
  library all say null. It also cannot *filter* on a list or a map at all
  (`Cannot explicitly project List or Map types`), which is why DuckDB is the
  oracle for `["is-null","tags"]`.


## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
