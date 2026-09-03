# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases before 0.6.0 predate this file; their contents are in the commit log
(each release is one commit whose subject begins with its version).

## [Unreleased]

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
