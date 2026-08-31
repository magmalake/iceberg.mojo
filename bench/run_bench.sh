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

# ── nested columns ─────────────────────────────────────────────────────────
NESTED_WAREHOUSE="${ICEBERG_NESTED_BENCH_ROOT:-$ROOT/build/nested-bench}"
NESTED_ROWS="${ICEBERG_NESTED_BENCH_ROWS:-200000}"
if [ ! -f "$NESTED_WAREHOUSE/metadata_location.txt" ]; then
    echo
    echo "== building the nested bench table ($NESTED_ROWS rows)"
    "$PY" tools/make_nested_bench_table.py "$NESTED_WAREHOUSE" "$NESTED_ROWS"
fi
NESTED_META="$(cat "$NESTED_WAREHOUSE/metadata_location.txt")"

echo
echo "== iceberg.mojo, nested columns"
mojo build bench/bench_nested.mojo $ICEBERG_INCLUDES -o build/iceberg-nested-bench
ICEBERG_NESTED_BENCH_ROOT="$NESTED_WAREHOUSE" ./build/iceberg-nested-bench

echo
echo "== PyIceberg, nested columns"
"$PY" tools/bench_pyiceberg_nested.py "$NESTED_META" | grep -v '^{'

# ── many files, many workers ───────────────────────────────────────────────
PAR_WAREHOUSE="${ICEBERG_PARALLEL_BENCH_ROOT:-$ROOT/build/parallel-bench}"
PAR_ROWS="${ICEBERG_PARALLEL_BENCH_ROWS:-2000000}"
if [ ! -f "$PAR_WAREHOUSE/metadata_location.txt" ]; then
    echo
    echo "== building the multi-file bench table ($PAR_ROWS rows, 250k per append)"
    "$PY" tools/make_bench_table.py "$PAR_WAREHOUSE" "$PAR_ROWS"
fi
PAR_META="$(cat "$PAR_WAREHOUSE/metadata_location.txt")"

echo
echo "== iceberg.mojo, 1/2/4/8 workers over eight data files"
mojo build bench/bench_parallel.mojo $ICEBERG_INCLUDES -o build/iceberg-parallel-bench
ICEBERG_PARALLEL_BENCH_ROOT="$PAR_WAREHOUSE" ./build/iceberg-parallel-bench

echo
echo "== PyIceberg, same table, one process"
"$PY" tools/bench_pyiceberg_parallel.py "$PAR_META" | grep -v '^{'

# ── scan planning over many manifests ──────────────────────────────────────
# Pure Avro: the manifest list plus every manifest, and no Parquet at all.
# Two tables, identical but for how many entries a manifest holds (4 and 20) —
# that is what separates the per-entry cost from the fixed cost of opening a
# manifest at all. Both are built by this library's own writers, one commit
# per manifest, under build/planning-bench{,-20}, and neither is checked in.
PLAN_WAREHOUSE="${ICEBERG_PLANNING_BENCH_ROOT:-$ROOT/build/planning-bench}"

echo
echo "== iceberg.mojo, planning a scan over many manifests"
mojo build bench/bench_planning.mojo $ICEBERG_INCLUDES -o build/iceberg-planning-bench
ICEBERG_PLANNING_BENCH_ROOT="$PLAN_WAREHOUSE" ./build/iceberg-planning-bench

# ── the write side ─────────────────────────────────────────────────────────
echo
echo "== appending a million rows"
mojo build bench/bench_append.mojo $ICEBERG_INCLUDES -o build/iceberg-append-bench
ICEBERG_APPEND_BENCH_ROOT="$ROOT/build/append-bench" ./build/iceberg-append-bench
"$PY" tools/bench_pyiceberg_append.py "$ROOT/build/append-bench-py"
