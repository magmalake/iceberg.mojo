#!/usr/bin/env bash
#
# Builds the one-million-row bench table if it is not there, runs the Mojo
# benchmark, then runs the same four scans through PyIceberg.
#
# The table lives in build/bench-warehouse and is NOT checked in: it is about
# 15 MB of Parquet and nothing but this script needs it. Set $ICEBERG_BENCH_ROWS
# to change its size.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WAREHOUSE="${ICEBERG_BENCH_ROOT:-$ROOT/build/bench-warehouse}"
ROWS="${ICEBERG_BENCH_ROWS:-1000000}"

VENV="$ROOT/build/pyiceberg-venv"
if [ ! -x "$VENV/bin/python" ] || ! "$VENV/bin/python" -c "import pyiceberg, pyarrow" 2>/dev/null; then
    command -v uv >/dev/null 2>&1 || {
        echo "error: uv is needed to build the bench table" >&2; exit 1; }
    uv venv --python 3.12 "$VENV" >/dev/null
    VIRTUAL_ENV="$VENV" uv pip install --quiet \
        "pyiceberg[sql-sqlite,pyarrow]==0.11.1" >/dev/null
fi
PY="$VENV/bin/python"

if [ ! -f "$WAREHOUSE/metadata_location.txt" ]; then
    echo "== building the bench table ($ROWS rows)"
    "$PY" tools/make_bench_table.py "$WAREHOUSE" "$ROWS"
fi
META="$(cat "$WAREHOUSE/metadata_location.txt")"

mkdir -p build
echo "== iceberg.mojo"
mojo build bench/bench_scan.mojo $ICEBERG_INCLUDES -o build/iceberg-bench
ICEBERG_BENCH_ROOT="$WAREHOUSE" ./build/iceberg-bench

echo
echo "== PyIceberg $("$PY" -c 'import pyiceberg; print(pyiceberg.__version__)')"
"$PY" tools/bench_pyiceberg.py "$META" | grep -v '^{'

# ── the write side ─────────────────────────────────────────────────────────
echo
echo "== appending a million rows"
mojo build bench/bench_append.mojo $ICEBERG_INCLUDES -o build/iceberg-append-bench
ICEBERG_APPEND_BENCH_ROOT="$ROOT/build/append-bench" ./build/iceberg-append-bench
"$PY" tools/bench_pyiceberg_append.py "$ROOT/build/append-bench-py"
