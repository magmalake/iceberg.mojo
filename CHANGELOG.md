# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases before 0.6.0 predate this file; their contents are in the commit log
(each release is one commit whose subject begins with its version).

## [Unreleased]

## [0.7.0] - 2026-09-07

### Added
- **`TableScan.count()`** — the row count off the manifests when they already
  hold it. Every `DataFile` entry carries `record_count`, so an unfiltered
  `COUNT(*)` is a walk of the manifest tree and a sum, with no data file
  opened: 1.65 ms on the 79.5M-row NYC-taxi table against PyIceberg 0.11.1's
  12.7 ms for the same call (7.7x), and against 3.1 s for the cheapest count
  available before this — a one-column `to_batches()` summing `num_rows`. The
  metadata is only used where it is exactly the answer: a task carrying any
  delete file (v2 position deletes, equality deletes or a v3 deletion vector),
  a task whose residual is not `true`, a task that is not a whole file, or one
  whose `record_count` is not positive is read instead and its surviving rows
  counted, per task, so a scan that is only partly countable pays only for the
  part that is not. `iceberg-mojo count <table>` exposes it on the CLI.
- **`TableScan.to_batch_reader()`, a streaming path for a scan.** `to_batches()`
  returns a `List[RecordBatch]`, so a scan's peak memory was the size of its
  whole result; a `BatchReader` hands the batches out as it reads them, so a
  caller that folds holds one *wave* of data files instead. On the 79.5M-row
  NYC-taxi table, one `double` column over 24 files single-threaded, peak RSS
  went from 978 MB to **319 MB** (p50 of seven processes under
  `/usr/bin/time -l`) and the scan from 549 ms to 463 ms. The rows come back in
  exactly the order `to_batches()` returns them at every `num_workers`: a wave
  is `min(num_workers, files)` files read through the existing `_read_files`
  merge-by-task-index and handed out in task order, rather than each file being
  yielded as its worker finished, which would have made a scan's row order
  depend on which core won a race. The barrier at the end of a wave costs 6–8%
  on the threaded legs, and the memory saved narrows as the wave widens
  (1229 MB → 949 MB at four workers). `to_table()` and `to_batches()` are
  unchanged. `next_batch()` raises what a read raises; `batches()` gives the
  `for`-loop shape, and because `Iterator.__next__` may raise only
  `StopIteration` a failure inside the loop ends it and is reported by
  `raise_if_failed()`.

### Changed
- **`ScanOptions.num_workers` is now a thread budget for the whole scan, not a
  file count.** It used to fan out over planned data files and nothing else, so
  a query that touched one file ran on one core however many workers were
  asked for. The budget is now filled on the file axis first, and whatever the
  plan is too narrow to use is handed to `ParquetReader.num_workers`, which
  spends it on the *(row group, leaf)* pairs inside each file. A plan with at
  least as many files as workers is unchanged, down to reading each file on
  one thread, and no scan reorders rows at any worker count. On the 79.5M-row
  NYC-taxi benchmark, an all-columns scan of a single 3.5M-row file went from
  183 ms to 64 ms (2.8×) at ten workers — from 0.46× to 1.36× the speed of
  PyIceberg 0.11.1 on that query — while the 24-file queries did not move. The
  ladder bends at four workers, the machine's performance-core count, against
  a serial fraction of 25–28%: per-batch casting, filtering and Arrow assembly
  all still happen on the calling thread.

### Fixed
- **A predicate the partition already guarantees no longer costs anything.**
  Strict projection stepped its literal exactly as inclusive projection does,
  but the two need opposite bounds: inclusive needs a *closed* bound and so
  steps `<` and `>`, while strict needs an *open* one and must step `<=` and
  `>=`. Using the inclusive stepping strictly left every strict bound one
  partition too conservative, at all four operators — so `ResidualEvaluator`
  never returned `true` and a range query on a partitioned table read and
  evaluated its filter column for every row of every file, even where the
  partition value alone settled the predicate. A month-aligned range over a
  `month(ts)` table now reduces to a literal `["true"]` residual and the
  timestamp column leaves the read set: on the 79.5M-row NYC-taxi table one
  file and one projected column went from 55.8 ms to 20.9 ms, and the suite's
  three partition-filtered queries went from 0.86×, 1.92× and 1.36× the speed
  of PyIceberg 0.11.1 to 1.84×, 2.12× and 1.47×. A mid-month range still does
  not reduce, which is correct — that partition holds rows on both sides of
  the bound. Covers `year`/`month`/`day`/`hour` on timestamps and dates and
  `truncate[W]` on integers; `truncate` on strings, binaries, decimals and
  floats stays conservative, as it does in Iceberg's Java implementation,
  because those have no adjacent value to step to. Java's
  `fixStrictTimeProjection` comes with it: Iceberg 0.10.0 and earlier wrote
  pre-epoch partition values one unit high, and this change is what newly
  enables reduction in that range, so a value that could have been written
  high is tightened by one rather than trusted.

## [0.6.1] – [0.6.7] - 2026-09-03 … 2026-09-06

Dependency re-pins and repository maintenance only; no change a consumer's
code would see. 0.6.1 re-pinned threads-mojo 0.4.0, 0.6.2/0.6.3 the
deprecation-sweep releases of avro, hashes, objectstore, roaring, sqlite and
thrift, and 0.6.4 through 0.6.7 successive parquet-mojo releases up to 0.7.0.
Alongside them: CI moved to pixi 0.78.0 and setup-pixi v0.10.2, source-
dependency environments were pinned to the stable toolchain, mojolint was
added to CI, and this repository's own sources were made to compile without
deprecation warnings.

## [0.6.0] - 2026-09-02

### Added
- **`SqlCatalog` runs on PostgreSQL**, through
  [postgres.mojo](https://github.com/magmalake/postgres.mojo) 0.2.0, alongside
  sqlite. The URI is the only difference:
  `SqlCatalog.local("default", "postgresql://user@host/db", warehouse)`, and
  every method — namespaces, properties, table CRUD, the guarded commit swap
  and its retry — behaves identically. Over Postgres the catalog is
  deployable rather than development-only: the same `iceberg_tables` /
  `iceberg_namespace_properties` schema PyIceberg's `SqlCatalog` and the Java
  `JdbcCatalog` use, with several writers able to share it.
- `iceberg.catalog._sqldriver.SqlDriver` — the two clients behind one
  interface: `execute(sql, params) -> Int` (rows affected),
  `query(sql, params) -> List[List[Optional[String]]]` (every cell as text,
  SQL NULL preserved), `begin`/`commit`/`rollback`, plus `_placeholders`
  (`?` → `$n`, skipping single-quoted literals) and `is_unique_violation`
  (SQLSTATE `23505`, or SQLite's `UNIQUE constraint failed`).
- `iceberg-mojo cat --sql postgresql://user@host/db --table db.orders` on the
  CLI.
- `pixi run verify-pg-catalog` — the PostgreSQL parity gate, mirroring
  `verify-sql-catalog`: rows written through this `SqlCatalog` read back
  cell-exact by PyIceberg's `SqlCatalog("postgresql+psycopg://…")`, and a
  catalog PyIceberg created from nothing over Postgres read back unchanged
  here.
- `scripts/pg-server.sh` and `scripts/with-pg-server.sh`, copied from
  postgres.mojo: a throwaway PostgreSQL cluster from the conda `postgresql`
  package, with no Docker and no service container. `tests/run_tests.sh`
  starts one and exports `$POSTGRES_TEST_DSN`, which is what makes the six
  SQL-catalog tests run a second time against Postgres; without it they print
  a skip line and run on sqlite alone.

### Changed
- `SqlCatalog.db` (a `sqlite.Database`) is now `SqlCatalog.driver` (a
  `SqlDriver`). The public API and constructor signature are unchanged.
- `SELECT changes()` is gone: rows affected now come from
  `sqlite.Database.changes()` and from libpq's command tag, which is why
  `sqlite-mojo` is pinned to 0.3.1 (`7af88b4`) rather than 0.3.0.
- A failed `create_table` or `rename_table` now says "table already exists"
  only for an actual unique-constraint violation, and re-raises anything else
  instead of relabelling it.

### Fixed
- `parquet.mojo` 0.4 reads Brotli pages, so `brotli.mojo` joins the source
  includes and `brotli-mojo` the dependencies; without them nothing in this
  repo compiled against the current sibling checkouts.
