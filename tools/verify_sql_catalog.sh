#!/usr/bin/env bash
#
# The SQL catalog's parity gate: writes through our `SqlCatalog`, reads back
# with PyIceberg's; has PyIceberg write, reads back with ours (`iceberg-mojo
# cat --sql`); checks rename/drop/namespace-properties and the guarded-swap
# commit conflict, all against the literal sqlite schema and rows, in both
# directions. See `tools/verify_sql_catalog.py` for what each check asserts.
#
# The venv is the same one `pixi run verify-writes` and `pixi run bench` use;
# `uv` creates it if it is not there.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORK="${ICEBERG_SQL_VERIFY_ROOT:-$ROOT/build/sql-verify}"
VENV="$ROOT/build/pyiceberg-venv"

if [ ! -x "$VENV/bin/python" ] \
    || ! "$VENV/bin/python" -c "import pyiceberg, pyarrow" 2>/dev/null; then
    command -v uv >/dev/null 2>&1 || {
        echo "error: uv is needed to build the verification venv" >&2; exit 1; }
    uv venv --python 3.12 "$VENV" >/dev/null
    VIRTUAL_ENV="$VENV" uv pip install --quiet \
        "pyiceberg[sql-sqlite,pyarrow]==0.11.1" >/dev/null
fi

rm -rf "$WORK"
mkdir -p "$WORK/wh1" "$WORK/wh2"

echo "== building sql-catalog-write and iceberg-mojo"
mojo build tools/sql_catalog_write.mojo $ICEBERG_INCLUDES -o build/sql-catalog-write
mojo build src/main.mojo $ICEBERG_INCLUDES -o build/iceberg-mojo

echo "== writing db1 through our SqlCatalog"
./build/sql-catalog-write "$WORK/catalog1.db" "$WORK/wh1"

echo "== verifying both directions with PyIceberg"
"$VENV/bin/python" tools/verify_sql_catalog.py \
    "$WORK/catalog1.db" "$WORK/wh1" \
    "$WORK/catalog2.db" "$WORK/wh2" \
    "$ROOT/build/iceberg-mojo"

echo "== ok"
