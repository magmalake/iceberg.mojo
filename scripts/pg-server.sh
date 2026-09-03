#!/bin/sh
#
# Copied verbatim from magmalake/postgres.mojo (scripts/pg-server.sh) at rev
# e6dce017ba654de4bb96b9a06215faa2e09dc320. The tins are independent
# repositories, so this is a copy rather than a shared file; keep it in step
# with the original when that one changes.
# A throwaway single-database PostgreSQL cluster for tests, on macOS and
# Linux, with no Docker. Requires `initdb`, `pg_ctl`, `pg_isready`, `createdb`
# on PATH — the conda `postgresql` package (a pixi dependency) provides all
# four under `pixi run`.
#
# Usage:
#   pg-server.sh start <workdir>   # writes <workdir>/dsn, prints the DSN
#   pg-server.sh stop  <workdir>   # idempotent
#
# The data directory lives under <workdir> (the caller's mktemp -d), but the
# Unix socket does not: macOS caps socket paths at 104 bytes, and a workdir
# under a project checkout can easily bust that once "/pg/.s.PGSQL.NNNNN" is
# appended. So the socket directory is its own mktemp -d under ${TMPDIR:-/tmp}
# — short, and outside the repo — and its path is recorded in
# <workdir>/socketdir so `stop` can find and remove it.
set -eu

action="${1:-}"
workdir="${2:-}"

if [ -z "$action" ] || [ -z "$workdir" ]; then
    echo "usage: pg-server.sh {start|stop} <workdir>" >&2
    exit 1
fi

pgdata="$workdir/pg"
pglog="$workdir/pg.log"

start() {
    mkdir -p "$workdir"

    socketdir="$(mktemp -d "${TMPDIR:-/tmp}/pgsock.XXXXXX")"
    echo "$socketdir" > "$workdir/socketdir"

    if ! initdb -D "$pgdata" --auth=trust --username=postgres -E UTF8 \
            --locale=C >"$workdir/initdb.log" 2>&1
    then
        echo "== initdb failed; tail of $workdir/initdb.log:" >&2
        tail -n 40 "$workdir/initdb.log" >&2 || true
        exit 1
    fi

    port="$(python3 -c '
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
')"
    echo "$port" > "$workdir/port"

    if ! pg_ctl -D "$pgdata" -w -o \
            "-p $port -k $socketdir -c listen_addresses=127.0.0.1 -c fsync=off -c synchronous_commit=off" \
            -l "$pglog" start
    then
        echo "== pg_ctl start failed; tail of $pglog:" >&2
        tail -n 40 "$pglog" >&2 || true
        exit 1
    fi

    ready=0
    i=0
    while [ "$i" -lt 150 ]; do
        if pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
            ready=1
            break
        fi
        i=$((i + 1))
        sleep 0.2
    done
    if [ "$ready" != "1" ]; then
        echo "== postgres never became ready; tail of $pglog:" >&2
        tail -n 40 "$pglog" >&2 || true
        pg_ctl -D "$pgdata" -m immediate stop >/dev/null 2>&1 || true
        exit 1
    fi

    if ! createdb -h 127.0.0.1 -p "$port" -U postgres test \
            >"$workdir/createdb.log" 2>&1
    then
        echo "== createdb failed; tail of $workdir/createdb.log:" >&2
        tail -n 40 "$workdir/createdb.log" >&2 || true
        pg_ctl -D "$pgdata" -m fast stop >/dev/null 2>&1 || true
        exit 1
    fi

    dsn="postgresql://postgres@127.0.0.1:$port/test"
    echo "$dsn" > "$workdir/dsn"
    echo "$dsn"
}

stop() {
    if [ -d "$pgdata" ]; then
        pg_ctl -D "$pgdata" -m fast stop >/dev/null 2>&1 || true
    fi
    if [ -f "$workdir/socketdir" ]; then
        socketdir="$(cat "$workdir/socketdir")"
        rm -rf "$socketdir" 2>/dev/null || true
    fi
}

case "$action" in
    start) start ;;
    stop) stop ;;
    *)
        echo "usage: pg-server.sh {start|stop} <workdir>" >&2
        exit 1
        ;;
esac
