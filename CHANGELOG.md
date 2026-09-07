# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases before 0.6.0 predate this file; their contents are in the commit log
(each release is one commit whose subject begins with its version).

## [Unreleased]

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
