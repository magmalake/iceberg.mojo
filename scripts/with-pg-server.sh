#!/bin/sh
#
# Copied verbatim from magmalake/postgres.mojo (scripts/with-pg-server.sh) at rev
# e6dce017ba654de4bb96b9a06215faa2e09dc320. The tins are independent
# repositories, so this is a copy rather than a shared file; keep it in step
# with the original when that one changes.
# Runs a command with a throwaway PostgreSQL server up and POSTGRES_TEST_DSN
# exported, then tears the server down (and its workdir) regardless of how
# the command exits.
#
# Usage: with-pg-server.sh <command...>
#
# Per the spec's "never hang CI on server startup" rule: if the server never
# becomes ready, this prints a note and exits 0 without running <command> —
# the server suite is skipped rather than failing CI on an environment that
# simply can't start PostgreSQL.
set -u

if [ "$#" -eq 0 ]; then
    echo "usage: with-pg-server.sh <command...>" >&2
    exit 1
fi

script_dir="$(cd "$(dirname "$0")" && pwd)"
workdir="$(mktemp -d "${TMPDIR:-/tmp}/pgserver.XXXXXX")"

cleanup() {
    "$script_dir/pg-server.sh" stop "$workdir" >/dev/null 2>&1
    rm -rf "$workdir"
}
trap cleanup EXIT INT TERM

if ! "$script_dir/pg-server.sh" start "$workdir" >"$workdir/start.log" 2>&1; then
    echo "== postgres server unavailable; skipping server tests"
    cat "$workdir/start.log" >&2 || true
    exit 0
fi

POSTGRES_TEST_DSN="$(cat "$workdir/dsn")"
export POSTGRES_TEST_DSN

"$@"
status=$?

exit "$status"
