#!/usr/bin/env bash
#
# Rebuild ONLY the three nested-column fixtures, in place.
#
# `tools/make_fixtures.sh` rebuilds the whole set from scratch, which changes
# every UUID, snapshot id and absolute path in `tests/fixtures/` and needs a
# checkout of the bridge at ../iceberg-rs.mojo. This script adds (or replaces)
# `nested_v2`, `nested_evo_v2` and `nested_part_v2` in the SAME warehouse and
# the SAME sqlite catalog, and touches nothing else.
#
# It is also step 3b of make_fixtures.sh, so a full regeneration runs exactly
# these commands.
#
# Requirements: `uv` (only if the PyIceberg venv is not already built).
#
# Usage:  bash tools/make_nested_fixtures.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FIXTURE_ROOT="${FIXTURE_ROOT:-$ROOT/build/warehouse-root}"
FIXTURE_OUT="${FIXTURE_OUT:-$ROOT/tests/fixtures}"
WAREHOUSE="$FIXTURE_ROOT/warehouse/db"
CATALOG_DB="$FIXTURE_ROOT/catalog.db"
VENV="${PYICEBERG_VENV:-$ROOT/build/pyiceberg-venv}"

NESTED_TABLES=(nested_v2 nested_evo_v2 nested_part_v2)

if [ ! -x "$VENV/bin/python" ] \
    || ! "$VENV/bin/python" -c "import pyiceberg, pyarrow, duckdb" 2>/dev/null; then
    command -v uv >/dev/null 2>&1 || {
        echo "error: uv is needed to build the PyIceberg venv" >&2; exit 1; }
    uv venv --python 3.12 "$VENV" >/dev/null
    VIRTUAL_ENV="$VENV" uv pip install --quiet \
        "pyiceberg[sql-sqlite,pyarrow]==0.11.1" "duckdb==1.5.5" >/dev/null
fi
PY="$VENV/bin/python"

echo "== 1/4  writing the nested tables into $WAREHOUSE"
FIXTURE_ROOT="$FIXTURE_ROOT" FIXTURE_OUT="$FIXTURE_OUT" \
    "$PY" "$ROOT/tools/make_nested_tables.py"

echo "== 2/4  copying metadata/ and data/ into tests/fixtures"
for t in "${NESTED_TABLES[@]}"; do
    rm -rf "${FIXTURE_OUT:?}/$t"
    mkdir -p "$FIXTURE_OUT/$t/metadata" "$FIXTURE_OUT/$t/oracle"
    cp -p "$WAREHOUSE/$t/metadata/"* "$FIXTURE_OUT/$t/metadata/"
    [ -d "$WAREHOUSE/$t/data" ] && cp -Rp "$WAREHOUSE/$t/data" "$FIXTURE_OUT/$t/data"
done

ONLY="$(IFS=,; echo "${NESTED_TABLES[*]}")"

echo "== 3/4  index and the PyIceberg plan oracle"
"$PY" "$ROOT/tools/make_index.py" "$FIXTURE_OUT" --catalog "$CATALOG_DB"
"$PY" "$ROOT/tools/oracle_pyiceberg.py" all "$FIXTURE_OUT" \
    --catalog "$CATALOG_DB" --only "$ONLY"

echo "== 4/4  row oracles (PyIceberg and DuckDB, filter by filter)"
"$PY" "$ROOT/tools/oracle_rows.py" "$FIXTURE_OUT" \
    --catalog "$CATALOG_DB" --only "$ONLY"

echo
du -sh "$FIXTURE_OUT"/nested_*
echo "NESTED FIXTURES OK"
