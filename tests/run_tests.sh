#!/usr/bin/env bash
#
# Builds the test binary, brings up the servers the tests need, runs them, and
# tears everything down.
#
# Two servers:
#   * tests/rest_server.py — a mock Iceberg REST catalog serving `/v1/config`
#     and `loadTable` out of the checked-in fixtures, and `createTable` /
#     `commitTable` into a scratch warehouse under $WORK. Always available;
#     python is a workspace dependency.
#   * an S3 server for the end-to-end read over `s3://`. MinIO is strongly
#     preferred because it actually *verifies* SigV4 signatures. Found in this
#     order: $MINIO_BINARY, build/minio, `minio` on PATH, then `moto_server`.
#     If none is there the S3 tests skip with a printed reason.
#
# Nothing here is secret: MinIO's well-known test credentials are the only ones
# used, and they only ever reach a server on 127.0.0.1.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/iceberg-test.XXXXXX")"
PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null
    done
    rm -rf "$WORK"
}
trap cleanup EXIT

mkdir -p build

echo "== building tests"
mojo build tests/iceberg_test.mojo $ICEBERG_INCLUDES -o build/iceberg-test || exit 1

# ── S3 server ──────────────────────────────────────────────────────────────
S3_PORT=$(python -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

MINIO="${MINIO_BINARY:-}"
[ -z "$MINIO" ] && [ -x "build/minio" ] && MINIO="$ROOT/build/minio"
[ -z "$MINIO" ] && command -v minio >/dev/null 2>&1 && MINIO="$(command -v minio)"

S3_KIND=none
if [ -n "$MINIO" ]; then
    echo "== starting minio ($MINIO) on :$S3_PORT"
    # MinIO answers 507 Insufficient Storage when its drive is nearly full,
    # which a developer machine can easily be; $ICEBERG_TEST_MINIO_DATA points
    # it somewhere with room (a RAM disk, say). CI runners have plenty.
    MINIO_DATA="${ICEBERG_TEST_MINIO_DATA:-$WORK/minio-data}"
    mkdir -p "$MINIO_DATA"
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
        "$MINIO" server "$MINIO_DATA" --address ":$S3_PORT" \
        > "$WORK/minio.log" 2>&1 &
    PIDS+=($!)
    export AWS_ACCESS_KEY_ID=minioadmin
    export AWS_SECRET_ACCESS_KEY=minioadmin
    S3_KIND=minio
elif python -c "import moto" 2>/dev/null; then
    echo "== starting moto_server on :$S3_PORT (signatures are NOT verified)"
    python -m moto.server -p "$S3_PORT" -H 127.0.0.1 > "$WORK/moto.log" 2>&1 &
    PIDS+=($!)
    export AWS_ACCESS_KEY_ID=testing
    export AWS_SECRET_ACCESS_KEY=testing
    S3_KIND=moto
else
    echo "== no S3 server available (set MINIO_BINARY, put one at build/minio,"
    echo "   or install moto) — the S3 end-to-end tests will skip"
fi

if [ "$S3_KIND" != "none" ]; then
    export AWS_REGION=us-east-1
    export AWS_ENDPOINT_URL_S3="http://127.0.0.1:$S3_PORT"
    BUCKET=iceberg-test
    ready=0
    for _ in $(seq 1 150); do
        if curl -s -o /dev/null "http://127.0.0.1:$S3_PORT/minio/health/live" \
            || curl -s -o /dev/null "http://127.0.0.1:$S3_PORT/"; then
            ready=1
            break
        fi
        sleep 0.2
    done
    if [ "$ready" != "1" ]; then
        echo "== S3 server never became ready; skipping those tests"
    elif python tests/s3_fixtures.py "http://127.0.0.1:$S3_PORT" "$BUCKET" \
            "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" tests/fixtures \
            unpartitioned ident_part deletes_v2 evolved; then
        export ICEBERG_TEST_S3="s3://$BUCKET/db"
        export ICEBERG_TEST_S3_PREFIX="s3://$BUCKET/"
        echo "== s3 warehouse at $ICEBERG_TEST_S3 ($S3_KIND)"
    else
        echo "== fixture upload failed; skipping S3 tests"
    fi
fi

# ── REST catalog mock ──────────────────────────────────────────────────────
export ICEBERG_TEST_REST_TOKEN=test-token
# The `wr` namespace the commit tests write into. Under $WORK so it is torn
# down with everything else.
export ICEBERG_TEST_REST_WAREHOUSE="$WORK/rest-warehouse"
mkdir -p "$ICEBERG_TEST_REST_WAREHOUSE"
python tests/rest_server.py tests/fixtures > "$WORK/rest.url" 2>"$WORK/rest.log" &
PIDS+=($!)
for _ in $(seq 1 100); do
    [ -s "$WORK/rest.url" ] && break
    sleep 0.1
done
if [ -s "$WORK/rest.url" ]; then
    export ICEBERG_TEST_REST="$(cat "$WORK/rest.url")"
    echo "== rest catalog mock at $ICEBERG_TEST_REST"
else
    echo "== rest catalog mock failed to start; those tests will skip"
    cat "$WORK/rest.log" 2>/dev/null | head -20
fi

echo "== running tests"
./build/iceberg-test
exit $?
