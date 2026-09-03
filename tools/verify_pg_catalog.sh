#!/usr/bin/env bash
#
# The SQL catalog's parity gate over PostgreSQL: writes through our
# `SqlCatalog`, reads back with PyIceberg's `SqlCatalog("postgresql+psycopg://…")`;
# has PyIceberg create the catalog tables and a table from scratch, reads that
# back with ours (`iceberg-mojo cat --sql postgresql://…`); checks the
# rename/drop/namespace-properties and the guarded-swap commit conflict, all
# against the literal `iceberg_tables` rows, in both directions. See
# `tools/verify_pg_catalog.py` for what each check asserts.
#
# `$POSTGRES_TEST_DSN` names the server; `pixi run verify-pg-catalog` runs this
# under scripts/with-pg-server.sh, which starts a throwaway one. The two
# directions get a schema each (mojo_side, pyiceberg_side) so they cannot
# collide on a shared server.
#
# The venv is the same one `pixi run verify-writes`, `verify-sql-catalog` and
# `bench` use, plus psycopg for the `postgresql+psycopg` dialect; `uv` creates
# it if it is not there. iceberg.mojo has no separate `verify` pixi
# environment — the PyIceberg side has always lived in this venv.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ -z "${POSTGRES_TEST_DSN:-}" ]; then
    echo "== \$POSTGRES_TEST_DSN is unset; run this through" \
        "\`pixi run verify-pg-catalog\`, which starts a server" >&2
    exit 1
fi

WORK="${ICEBERG_PG_VERIFY_ROOT:-$ROOT/build/pg-verify}"
VENV="$ROOT/build/pyiceberg-venv"

if [ ! -x "$VENV/bin/python" ]; then
    command -v uv >/dev/null 2>&1 || {
        echo "error: uv is needed to build the verification venv" >&2; exit 1; }
    uv venv --python 3.12 "$VENV" >/dev/null
fi
# Additive: `verify-sql-catalog` may have built this venv already, in which
# case only psycopg is missing from it.
if ! "$VENV/bin/python" -c "import pyiceberg, pyarrow, sqlalchemy, psycopg" \
        2>/dev/null; then
    command -v uv >/dev/null 2>&1 || {
        echo "error: uv is needed to build the verification venv" >&2; exit 1; }
    VIRTUAL_ENV="$VENV" uv pip install --quiet \
        "pyiceberg[sql-sqlite,pyarrow]==0.11.1" "psycopg[binary]>=3.2" >/dev/null
fi

rm -rf "$WORK"
mkdir -p "$WORK/wh1" "$WORK/wh2"

echo "== preparing the two schemas on $POSTGRES_TEST_DSN"
"$VENV/bin/python" - "$POSTGRES_TEST_DSN" <<'PY'
import sys
import psycopg

with psycopg.connect(sys.argv[1], autocommit=True) as con:
    for schema in ("mojo_side", "pyiceberg_side"):
        con.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        con.execute(f"CREATE SCHEMA {schema}")
PY

# libpq takes `options=-csearch_path=<schema>` in the URI, which is how each
# side is confined to its own schema without either client knowing about it.
MOJO_DSN="$POSTGRES_TEST_DSN?options=-csearch_path%3Dmojo_side"

echo "== building sql-catalog-write and iceberg-mojo"
mojo build tools/sql_catalog_write.mojo $ICEBERG_INCLUDES -o build/sql-catalog-write
mojo build src/main.mojo $ICEBERG_INCLUDES -o build/iceberg-mojo

echo "== writing schema mojo_side through our SqlCatalog"
./build/sql-catalog-write "$MOJO_DSN" "$WORK/wh1"

echo "== verifying both directions with PyIceberg"
"$VENV/bin/python" tools/verify_pg_catalog.py \
    "$POSTGRES_TEST_DSN" "$WORK/wh1" "$WORK/wh2" \
    "$ROOT/build/iceberg-mojo"

echo "== ok"
