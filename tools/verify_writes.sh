#!/usr/bin/env bash
#
# Write thirty-two tables with this library — ten appended to, twenty-two
# deleted from or overwritten — then read every one of them with PyIceberg
# 0.11.1 and DuckDB 1.5.5 and check they agree, cell for cell. Finally let
# PyIceberg *append* to four of them, two of which this library has deleted
# from, and read the results back here.
#
# The venv is the same one `pixi run bench` builds; `uv` creates it if it is
# not there. Nothing is checked in: the tables live under build/.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WAREHOUSE="${ICEBERG_WRITE_ROOT:-$ROOT/build/write-verify}"
VENV="$ROOT/build/pyiceberg-venv"

if [ ! -x "$VENV/bin/python" ] \
    || ! "$VENV/bin/python" -c "import pyiceberg, pyarrow, duckdb, fastavro" 2>/dev/null; then
    command -v uv >/dev/null 2>&1 || {
        echo "error: uv is needed to build the verification venv" >&2; exit 1; }
    uv venv --python 3.12 "$VENV" >/dev/null
    VIRTUAL_ENV="$VENV" uv pip install --quiet \
        "pyiceberg[sql-sqlite,pyarrow]==0.11.1" "duckdb==1.5.5" fastavro >/dev/null
fi

mkdir -p build
echo "== building the writer"
mojo build tools/write_tables.mojo $ICEBERG_INCLUDES -o build/write-tables

rm -rf "$WAREHOUSE"
echo "== writing tables into $WAREHOUSE"
./build/write-tables "$WAREHOUSE"

echo "== verifying with PyIceberg and DuckDB"
"$VENV/bin/python" tools/verify_written.py "$WAREHOUSE" --append-with-pyiceberg

echo "== reading the PyIceberg-appended tables back with iceberg-mojo"
mojo build src/main.mojo $ICEBERG_INCLUDES -o build/iceberg-mojo
count_rows() {
    ./build/iceberg-mojo cat "$1" --select id --format csv \
        | tail -n +2 | wc -l | tr -d ' '
}
for t in unpartitioned_v2 ident_v2; do
    n=$(count_rows "$WAREHOUSE/db/$t")
    echo "   db.$t: $n rows"
    [ "$n" = "24" ] || { echo "expected 24 rows after the PyIceberg append" >&2; exit 1; }
done
# 18 rows, four deleted here, three appended by PyIceberg — and the position
# deletes this library wrote still apply to the rows it wrote.
for t in mor_unpartitioned_v2 cow_unpartitioned_v2; do
    n=$(count_rows "$WAREHOUSE/del/$t")
    echo "   del.$t: $n rows"
    [ "$n" = "17" ] || { echo "expected 17 rows in del.$t" >&2; exit 1; }
done
echo "== ok"
