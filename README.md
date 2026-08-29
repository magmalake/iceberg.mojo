# iceberg.mojo

> Part of **magmalake** — data lake building blocks in Mojo.

Native, pure-Mojo **Apache Iceberg** metadata: read a table's `metadata.json`,
pick a snapshot, decode its manifests, and plan a scan — with real partition
transforms, real predicate pushdown, and real delete-file association. No JVM,
no Python, no Rust: the only dependencies are two sibling magmalake tins,
[hashes.mojo](https://github.com/magmalake/hashes.mojo) (for the murmur3 the
`bucket[N]` transform is defined in terms of) and
[avro.mojo](https://github.com/magmalake/avro.mojo) (manifests are Avro).

It stops where the data starts. `plan_files()` tells you exactly which Parquet
files a query must read and which delete files apply to each — reading them is
[parquet.mojo](https://github.com/magmalake)'s job, and it does not exist yet.

## Status

Every gate below was **measured**, by `pixi run test`, against fixtures produced
by two independent reference implementations: iceberg-rust 0.10.1 (through
[iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo)) and PyIceberg
0.11.1. Nothing here is self-checked — the expected values come from them, never
from this code. See [`tests/fixtures/PROVENANCE.md`](tests/fixtures/PROVENANCE.md).

| Gate | Result |
|---|---|
| **(a)** Table metadata parses, re-serializes losslessly, and matches the oracle field by field | ✅ **7 / 7 tables** — round trip is stable and byte-identical on a second pass; every scalar, schema, spec, sort order, snapshot, log entry, ref and property compared individually |
| **(b)** Snapshot selection — current, by id, by ref, as-of | ✅ **21 / 21 snapshots** across 7 tables agree with the oracle on sequence number, timestamp, manifest list, parent and operation |
| **(c)** Partition transforms | ✅ **252 / 252 vectors** from PyIceberg, **87** of them also checking the raw 32-bit hash, plus Appendix B's own published test values |
| **(d)** `plan_files()` — the planned data files and their delete files | ✅ **42 / 42 cases** (7 tables × 6 filters) match **PyIceberg**; **41 / 42** match iceberg-rust — see below |
| **(e)** Manifests decode with correct inherited sequence numbers | ✅ **42 entries** across **21 manifest lists**, each inherited value checked against the manifest list it came from |
| Tests | **65 passing**, identical on `stable` (Mojo 1.0.0) and `default` (nightly) |
| CI | 4 jobs: {stable, nightly} × {ubuntu-latest, macos-latest} |

### The one disagreement, and why it is not a bug

For `["in","id",[1,4,7]]` over the `bucket[4]`-partitioned table, iceberg-rust
0.10.1 plans **5** files and this reader plans **3**. PyIceberg also plans 3, and
they are the same 3.

ids 1, 4 and 7 hash into buckets 0, 2 and 3. All three implementations keep the
bucket-0, bucket-2 and bucket-3 files. Bucket 3 also holds a file of `{3}` and a
file of `{5}`, which the *partition* filter cannot exclude — both are in bucket
3. PyIceberg and this reader then apply the predicate to each file's column
bounds and drop them; iceberg-rust does not apply `In` at that level.

A scan plan is allowed to be a superset — reading a file with no matching rows
costs time, not correctness. So this is looseness in the bridge, not an error in
either. A separate test (`test_plan_files_never_drops_a_file_the_bridge_keeps`)
asserts the direction that *would* be a bug: across all 42 cases this reader
never plans a file the bridge does not, so it is never over-pruning.

## What is implemented

**Format versions 1, 2 and 3** for reading, tolerating v4 where the spec already
says to (optional `location`, unknown fields, unknown transforms).

- **Types and schemas** — every primitive including the v3 additions (`unknown`,
  `timestamp_ns`, `timestamptz_ns`, `variant`, `geometry(C)`, `geography(C, A)`),
  `decimal(P, S)`, `fixed[L]`, nested struct/list/map with field ids,
  `initial-default` / `write-default`, identifier field ids, and dotted-name
  lookup that reaches list elements and map keys/values.
- **Table metadata** — every field in the spec's table-metadata table, the v1
  singular `schema` and bare `partition-spec` forms, `refs`, `snapshot-log`,
  `metadata-log`, `statistics` with blob metadata, `partition-statistics`, and
  the v3 `next-row-id`, `encryption-keys` and snapshot `first-row-id` /
  `added-rows` / `key-id`. Unknown top-level keys survive a round trip.
- **Partition transforms** — `identity`, `bucket[N]`, `truncate[W]`, `year`,
  `month`, `day`, `hour`, `void`, with correct result types (`day` is the only
  one returning `date`), code-point string truncation, nanosecond timestamps
  truncated to microseconds before hashing, and unknown transforms kept verbatim
  and excluded from filtering as v3 requires.
- **Expressions** — the full predicate set, binding against a schema with
  per-column literal typing, `rewrite_not`, inclusive and strict projection
  through transforms, `ManifestEvaluator` over partition summaries and
  `InclusiveMetricsEvaluator` over data-file bounds and counts.
- **Manifests** — manifest lists (fields 500–520) and manifests (`manifest_entry`
  plus `data_file` 100–145), with sequence-number, snapshot-id and `first_row_id`
  inheritance, and partition tuples typed by the spec in the manifest's own Avro
  file metadata.
- **Scan planning** — manifest pruning, data-file pruning by partition and by
  metrics, residuals, and delete-file association by the spec's scope rules
  (position deletes at `data_seq <= delete_seq`, equality deletes at
  `data_seq < delete_seq` or globally when written unpartitioned, deletion
  vectors superseding position delete files for the same data file).
- **Catalogs** — a filesystem catalog (`version-hint.text` or highest-versioned
  `*.metadata.json`, ties broken by `last-updated-ms`), gzipped
  `*.gz.metadata.json`, and an `InputFile` trait plus location rewriting.

## Deliberately out of scope

Until the rest of the stack lands:

| Not here | Why | Where it will live |
|---|---|---|
| Reading data | No Parquet decoder in Mojo yet. `plan_files()` stops at the task list. | parquet.mojo |
| Reading deletion vectors | Puffin blobs; the *metadata* is decoded and associated, the bitmap is not read. | Puffin + [roaring.mojo](https://github.com/magmalake/roaring.mojo) |
| S3 / GCS / Azure | Only `file://` and bare paths. `InputFile` is the seam. | objectstore.mojo |
| Writes | Read-only: no appends, no commits, no compaction. | later |
| REST catalog transport | No Mojo HTTP client resolves from conda on both environments (see below). URL/header shaping and response parsing **are** implemented and tested. | when one exists |

## Install

```sh
pixi run test    # 65 tests on the nightly environment
pixi run -e stable test
pixi run cli     # builds build/iceberg-mojo
```

Consume it with `-I ../iceberg.mojo/src -I ../hashes.mojo/src -I ../avro.mojo/src`.

Sibling tins are consumed by **source path**, not as pixi packages: pixi-build-mojo
emits a precompiled `.mojopkg` built with `mojo-compiler` 1.0.0, and the nightly
compiler refuses to load it (`Precompiled file ... version 1.0.0 is older than
compiler version 1.1.0.dev...`). Source paths satisfy both environments. The same
issue rules out EmberJson, which is why `iceberg.json` is a small in-repo parser.

## API

```mojo
from iceberg.catalog.filesystem import Table
from iceberg.io import FileIO

def main() raises:
    var t = Table.load_local("/warehouse/db/orders")

    print(t.metadata.format_version, t.metadata.table_uuid)
    print(t.metadata.schema().to_json())

    var snap = t.metadata.current_snapshot()
    print(snap.snapshot_id, snap.sequence_number, snap.operation())

    var tasks = (
        t.scan()
        .filter('["and",[">","id",2],["=","region","eu"]]')
        .select(["id", "region", "amount"])
        .plan_files()
    )
    for k in range(len(tasks)):
        print(tasks[k].data_file.file_path, tasks[k].data_file.record_count)
        for j in range(len(tasks[k].delete_files)):
            print("   delete:", tasks[k].delete_files[j].file_path)
```

| Module | What it gives you |
|---|---|
| `iceberg.json` | `parse_json`, `Json` — an arena DOM with exact `Int64` handling |
| `iceberg.types` | `TypeStore`, `NestedField`, the primitive kinds |
| `iceberg.schema` | `Schema` — `find_field(id)`, `find_by_name`, `select(ids)` |
| `iceberg.values` | `Datum`, `compare`, Appendix-D binary and JSON single values |
| `iceberg.transforms` | `Transform`, `PartitionSpec`, `SortOrder`, `bucket_of` |
| `iceberg.expressions` | `parse_filter`, `bind`, projections, the two evaluators |
| `iceberg.metadata` | `TableMetadata`, `Snapshot`, `SnapshotRef`, snapshot selection |
| `iceberg.manifest` | `read_manifest_list`, `read_manifest`, `DataFile` |
| `iceberg.scan` | `TableScan`, `FileScanTask` |
| `iceberg.io` | `FileIO`, `InputFile`, location rewriting |
| `iceberg.catalog.filesystem` | `Table`, `FilesystemCatalog` |
| `iceberg.catalog.rest` | `RestCatalogConfig`, `LoadTableResult` (no transport) |

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
iceberg-mojo describe  <metadata.json | table-dir>
iceberg-mojo schema    <metadata.json | table-dir>
iceberg-mojo snapshots <metadata.json | table-dir>
iceberg-mojo files     <metadata.json | table-dir> [--snapshot ID] [--ref NAME]
                                                   [--as-of MS] [--filter DSL]
                                                   [--select a,b,c]
                                                   [--rebase FROM=TO]
```

`files` prints the same JSON shape as the bridge's `ib_scan_plan_files_json`.
`--rebase` redirects the absolute locations Iceberg metadata stores, which is how
the checked-in fixtures are readable from anywhere:

```sh
iceberg-mojo files tests/fixtures/ident_part \
  --filter '["=","region","eu"]' \
  --rebase 'file:///…/warehouse/db=tests/fixtures'
```

## Fixtures

Seven tables, one coherent warehouse, ~1.2 MB of metadata (no Parquet — none is
read). Full detail in [`tests/fixtures/PROVENANCE.md`](tests/fixtures/PROVENANCE.md);
`tools/make_fixtures.sh` regenerates everything.

| Table | Partitioning | Built by |
|---|---|---|
| `unpartitioned` | none | iceberg-rust 0.10.1 |
| `ident_part` | `identity(region)` | iceberg-rust 0.10.1 |
| `bucket_part` | `bucket[4](id)` | iceberg-rust 0.10.1 |
| `day_part` | `day(ts)` | iceberg-rust 0.10.1 |
| `trunc_part` | `truncate[3](region)` | iceberg-rust 0.10.1 |
| `evolved` | none; add + rename a column, promote `int`→`long` | PyIceberg 0.11.1 |
| `deletes_v2` | none; v2 with a **position delete file** | PyIceberg 0.11.1 |

Each has three snapshots. Alongside each table's metadata sit the oracle outputs:
the bridge's metadata and snapshots JSON, and both oracles' plan-files JSON for
six filters. `transform_vectors.json` holds the 252 transform vectors.

Two things worth knowing about these fixtures, because they are what the tests
exercise:

- **`deletes_v2`'s delete file is not stock PyIceberg output.** PyIceberg 0.11.1's
  `Table.delete()` falls back to copy-on-write (*"Merge on read is not yet
  supported"*), so the position delete file was written with a `ManifestWriterV2`
  subclass reporting `ManifestContent.DELETES`. It is a valid v2 position delete;
  it just was not produced by the high-level API.
- **`evolved`'s metadata directory holds three table lifetimes.** The generator
  dropped and recreated the table twice, so three different `table-uuid`s have
  colliding version numbers in one directory. That turned out to be a useful
  accident: it is exactly the case that forces filesystem discovery to break
  version ties by `last-updated-ms` rather than by file name.

## Notes for the next reader

Three things this implementation got wrong first and the parity gates caught:

1. **`!=` and `not-in` must never prune on bounds.** A bound is not necessarily a
   value that occurs in the file — string upper bounds are rounded *up* — so
   `lower == upper == X` does not prove every value is `X`. Both reference
   implementations return "might match" here.
2. **Mojo's `Int64` `//` and `%` already floor** (Python semantics, on both
   toolchains). A hand-rolled `floor_div` that "corrects" them puts every
   pre-epoch timestamp one day early.
3. **The `snapshots` array is not in chronological order.** iceberg-rust writes it
   in hash order. Find the oldest snapshot by sequence number, not by position.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
