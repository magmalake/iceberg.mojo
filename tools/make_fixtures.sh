#!/usr/bin/env bash
#
# Regenerate the ENTIRE tests/fixtures set from scratch.
#
# The fixtures are CHECKED IN.  You only need this script when you want to
# rebuild them — a new iceberg-rust or PyIceberg release, a new table, a new
# filter.  A normal `pixi run test` never runs it.
#
# Requirements
#   * `pixi`  — the bridge tables are built by tools/make_fixtures.mojo, which
#               runs inside iceberg-rs.mojo's pixi environment (it links the
#               Rust cdylib over iceberg-rust 0.10.1).
#   * `uv`    — used to create the PyIceberg venv if one is not already there.
#   * a checkout of the bridge at ../iceberg-rs.mojo (override with
#     $ICEBERG_RS_DIR).
#
# What it does, in order
#   1. wipes build/warehouse-root and rebuilds the five bridge tables
#      (unpartitioned, ident_part, bucket_part, day_part, trunc_part) plus
#      their bridge-shaped oracle files;
#   2. adds the two PyIceberg-built tables (evolved, deletes_v2) to the SAME
#      sqlite catalog and warehouse;
#   3. adds the two delete tables PyIceberg's public API cannot make
#      (eq_deletes_v2, dv_v3) — see tools/make_delete_tables.py;
#   4. regenerates tests/fixtures/transform_vectors.json;
#   5. copies each table's metadata/ AND data/ dirs into tests/fixtures/;
#   6. runs the PyIceberg plan oracle over all nine tables and six filters,
#      which also writes the bridge-shaped oracle for the PyIceberg tables;
#   7. runs the row-level oracles (PyIceberg for eight tables x six filters,
#      DuckDB unfiltered for eight) — see tools/oracle_rows.py.
#
# NOTE the fixture metadata JSON keeps ABSOLUTE file:// paths into the
# warehouse built in step 1 — see tests/fixtures/PROVENANCE.md.  Rerunning this
# script on a different machine changes those paths (and every UUID and
# snapshot id) throughout the fixtures.
#
# Usage:  bash tools/make_fixtures.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ICEBERG_RS_DIR="${ICEBERG_RS_DIR:-$(cd "$ROOT/../iceberg-rs.mojo" 2>/dev/null && pwd || true)}"
FIXTURE_ROOT="$ROOT/build/warehouse-root"
FIXTURE_OUT="$ROOT/tests/fixtures"
WAREHOUSE="$FIXTURE_ROOT/warehouse/db"
CATALOG_DB="$FIXTURE_ROOT/catalog.db"

BRIDGE_TABLES=(unpartitioned ident_part bucket_part day_part trunc_part)
PY_TABLES=(evolved deletes_v2 eq_deletes_v2 dv_v3)
ALL_TABLES=("${BRIDGE_TABLES[@]}" "${PY_TABLES[@]}")

# ---------------------------------------------------------------- preflight --
for bin in pixi uv; do
  command -v "$bin" >/dev/null 2>&1 || {
    echo "error: $bin not on PATH (see the header of this script)" >&2
    exit 1
  }
done
[ -n "$ICEBERG_RS_DIR" ] && [ -d "$ICEBERG_RS_DIR" ] || {
  echo "error: no iceberg-rs.mojo checkout; set \$ICEBERG_RS_DIR" >&2
  exit 1
}

# ------------------------------------------------- 1. bridge tables 1..5 -----
echo "== 1/7  bridge tables (iceberg-rust, via $ICEBERG_RS_DIR) =="
rm -rf "$FIXTURE_ROOT"
mkdir -p "$FIXTURE_ROOT/warehouse"
for t in "${ALL_TABLES[@]}"; do
  rm -rf "${FIXTURE_OUT:?}/$t/oracle" "${FIXTURE_OUT:?}/$t/metadata"
  mkdir -p "$FIXTURE_OUT/$t/oracle"
done

( cd "$ICEBERG_RS_DIR" && \
  FIXTURE_ROOT="$FIXTURE_ROOT" FIXTURE_OUT="$FIXTURE_OUT" \
  pixi run -e default mojo run -I src -I "$ROOT/tools" "$ROOT/tools/make_fixtures.mojo" )

# ------------------------------------------- 2. PyIceberg tables 6..7 --------
echo "== 2/7  PyIceberg tables (evolved, deletes_v2) =="
VENV=""
for cand in "$ICEBERG_RS_DIR/build/pyiceberg-venv" "$ROOT/build/pyiceberg-venv"; do
  if [ -x "$cand/bin/python" ] && \
     "$cand/bin/python" -c "import pyiceberg, pyarrow" >/dev/null 2>&1; then
    VENV="$cand"
    break
  fi
done
if [ -z "$VENV" ]; then
  VENV="$ROOT/build/pyiceberg-venv"
  uv venv --python 3.12 "$VENV" >/dev/null
  VIRTUAL_ENV="$VENV" uv pip install --quiet \
    "pyiceberg[sql-sqlite,pyarrow]==0.11.1" duckdb >/dev/null
fi
# duckdb is the only oracle for the equality-delete table; add it to a venv
# that predates this script.
"$VENV/bin/python" -c "import duckdb" 2>/dev/null || \
  VIRTUAL_ENV="$VENV" uv pip install --quiet duckdb >/dev/null
PY="$VENV/bin/python"
echo "   venv: $VENV ($("$PY" -c 'import pyiceberg; print("pyiceberg", pyiceberg.__version__)'))"

FIXTURE_ROOT="$FIXTURE_ROOT" FIXTURE_OUT="$FIXTURE_OUT" \
  "$PY" "$ROOT/tools/make_pyiceberg_tables.py"
# make_pyiceberg_tables.py drops its report at the root of $FIXTURE_OUT.
mv -f "$FIXTURE_OUT/deletes_v2_report.json" \
      "$FIXTURE_OUT/deletes_v2/oracle_delete_report.json"

# -------------------------------------- 3. equality deletes + a v3 DV -------
echo "== 3/7  delete tables (eq_deletes_v2, dv_v3) =="
FIXTURE_ROOT="$FIXTURE_ROOT" FIXTURE_OUT="$FIXTURE_OUT" \
  "$PY" "$ROOT/tools/make_delete_tables.py"

# ----------------------------------------------- 4. transform vectors --------
echo "== 4/7  transform vectors =="
"$PY" "$ROOT/tools/gen_transform_vectors.py" "$FIXTURE_OUT/transform_vectors.json"

# --------------------------------------- 5. copy metadata/ and data/ ---------
# The data files are checked in too (about 400 KB): reading them is the whole
# point of the scan gates.
echo "== 5/7  copy metadata/ and data/ into tests/fixtures =="
for t in "${ALL_TABLES[@]}"; do
  rm -rf "${FIXTURE_OUT:?}/$t/metadata" "${FIXTURE_OUT:?}/$t/data"
  mkdir -p "$FIXTURE_OUT/$t/metadata"
  cp -p "$WAREHOUSE/$t/metadata/"* "$FIXTURE_OUT/$t/metadata/"
  [ -d "$WAREHOUSE/$t/data" ] && cp -Rp "$WAREHOUSE/$t/data" "$FIXTURE_OUT/$t/data"
done
printf 'WAREHOUSE_ROOT=%s\n' "$WAREHOUSE" > "$FIXTURE_OUT/WAREHOUSE_ROOT.txt"

# ------------------------------------------------- 6. PyIceberg oracle -------
# Also writes the bridge-shaped metadata.json / snapshots.json / plan_<k>.json
# for the four PyIceberg-built tables, which the Rust bridge never saw.
echo "== 6/7  PyIceberg plan oracle (9 tables x 6 filters) =="
"$PY" "$ROOT/tools/oracle_pyiceberg.py" all "$FIXTURE_OUT" --catalog "$CATALOG_DB"

# ------------------------------------------------- 7. row-level oracles ------
echo "== 7/7  row oracles (PyIceberg x 6 filters, DuckDB unfiltered) =="
"$PY" "$ROOT/tools/make_index.py" "$FIXTURE_OUT" --catalog "$CATALOG_DB"
"$PY" "$ROOT/tools/oracle_rows.py" "$FIXTURE_OUT" --catalog "$CATALOG_DB"

echo
du -sh "$FIXTURE_OUT"
echo "FIXTURES OK"
