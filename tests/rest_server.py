#!/usr/bin/env python3
"""A minimal Iceberg REST catalog, backed by the checked-in fixture tables.

Enough of the spec to exercise the client end to end and nothing more:

    GET  /v1/config                          -> a `prefix` override
    GET  /v1/<prefix>/namespaces             -> ListNamespacesResponse
    GET  /v1/<prefix>/namespaces/db/tables   -> ListTablesResponse
    GET  /v1/<prefix>/namespaces/db/tables/X -> LoadTableResult
    HEAD (same)                              -> 200 / 404
    POST /v1/<prefix>/namespaces/wr/tables   -> CreateTableRequest
    POST /v1/<prefix>/namespaces/wr/tables/X -> CommitTableRequest

The `db` namespace is the read-only fixture corpus. The `wr` namespace is a
scratch warehouse ($ICEBERG_TEST_REST_WAREHOUSE, or a temp dir) that accepts
`createTable` and `commitTable`: requirements are **checked**, updates are
applied to the stored metadata, and a new `<V>-<uuid>.metadata.json` is
written, so a commit against this server is a real optimistic commit.

`Idempotency-Key` is honoured the way the REST spec has asked since 1.11.0:
the first answer to a key is stored and *replayed* to any repeat of it, so a
retry of a commit the server already applied gets the original success rather
than a second attempt.

Three tables are rigged so the client's paths can be exercised for real:
`conflict_once` answers **409** to the first commit it sees (and accepts the
retry); `unknown_state` applies the commit and *then* answers **500** once —
its key is recorded, so the retry replays the success and the commit lands
exactly once; and `always_5xx` is a server that does **not** support the
header: it applies the first commit, answers **500** to that and to every
repeat, and so leaves the client with the case the spec calls
`CommitStateUnknown`.

The `metadata` in a LoadTableResult is the fixture's real current
`*.metadata.json`, inlined, so the client parses genuine table metadata. Every
request must carry `Authorization: Bearer <token>` or the server answers 401,
which is how the test proves the header is actually sent; the presence of
`X-Iceberg-Access-Delegation` is echoed back in the table `config` so the test
can prove that one too.

Prints its base URL on stdout and then serves forever.
"""
import json
import os
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

FIXTURES = sys.argv[1] if len(sys.argv) > 1 else "tests/fixtures"
TOKEN = os.environ.get("ICEBERG_TEST_REST_TOKEN", "test-token")
PREFIX = "ws"
NAMESPACE = "db"
WRITE_NAMESPACE = "wr"
WAREHOUSE = os.environ.get("ICEBERG_TEST_REST_WAREHOUSE") or tempfile.mkdtemp(
    prefix="iceberg-rest-wh-")

# {table -> (metadata_location, metadata)}, plus the rigged-failure bookkeeping.
WRITTEN = {}
SEEN_409 = set()
SEEN_5XX = set()
# {Idempotency-Key -> (status, response body)}. The whole point of the header:
# the server remembers what it answered so a repeat of a request it already
# acted on replays that answer instead of acting again.
IDEMPOTENT = {}
LOCK = threading.Lock()


def _write_metadata(table, doc, version):
    """Persist a metadata file the way a catalog does, and remember it."""
    d = os.path.join(WAREHOUSE, WRITE_NAMESPACE, table, "metadata")
    os.makedirs(d, exist_ok=True)
    name = "%05d-%s.metadata.json" % (version, uuid.uuid4())
    path = os.path.join(d, name)
    with open(path, "w") as fh:
        json.dump(doc, fh)
    WRITTEN[table] = ("file://" + path, doc, version)
    return "file://" + path


def _new_metadata(req, table):
    """The metadata of a table this server is being asked to create."""
    props = dict(req.get("properties") or {})
    version = int(props.pop("format-version", "2"))
    location = req.get("location") or os.path.join(
        WAREHOUSE, WRITE_NAMESPACE, table)
    schema = dict(req["schema"])
    schema["schema-id"] = 0
    spec = dict(req.get("partition-spec") or {"spec-id": 0, "fields": []})
    spec["spec-id"] = 0
    last_partition_id = max(
        [999] + [f["field-id"] for f in spec.get("fields", [])])
    now = int(time.time() * 1000)
    doc = {
        "format-version": version,
        "table-uuid": str(uuid.uuid4()),
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": now,
        "last-column-id": max(
            [0] + [f["id"] for f in schema.get("fields", [])]),
        "schemas": [schema],
        "current-schema-id": 0,
        "partition-specs": [spec],
        "default-spec-id": 0,
        "last-partition-id": last_partition_id,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "properties": props,
        "current-snapshot-id": -1,
        "refs": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
    }
    if version >= 3:
        doc["next-row-id"] = 0
    return doc


def _check_requirements(doc, requirements):
    """Every requirement the spec defines for an append, actually checked."""
    for r in requirements or []:
        kind = r.get("type")
        if kind == "assert-create":
            if doc is not None:
                return "table already exists"
        elif kind == "assert-table-uuid":
            if doc is None or doc.get("table-uuid") != r.get("uuid"):
                return "assert-table-uuid: %s != %s" % (
                    None if doc is None else doc.get("table-uuid"),
                    r.get("uuid"))
        elif kind == "assert-ref-snapshot-id":
            ref = r.get("ref")
            want = r.get("snapshot-id")
            got = None
            if doc is not None:
                entry = (doc.get("refs") or {}).get(ref)
                got = entry.get("snapshot-id") if entry else None
            if got != want:
                return "assert-ref-snapshot-id %s: %r != %r" % (ref, got, want)
        elif kind == "assert-current-schema-id":
            if doc.get("current-schema-id") != r.get("current-schema-id"):
                return "assert-current-schema-id"
        elif kind == "assert-default-spec-id":
            if doc.get("default-spec-id") != r.get("default-spec-id"):
                return "assert-default-spec-id"
    return None


def _apply_updates(doc, updates):
    """The `TableUpdate` actions a commit from this client can send."""
    for u in updates or []:
        action = u.get("action")
        if action == "add-snapshot":
            s = u["snapshot"]
            doc["snapshots"] = doc.get("snapshots", []) + [s]
            doc["last-sequence-number"] = max(
                doc.get("last-sequence-number", 0), s.get("sequence-number", 0))
            doc["last-updated-ms"] = s.get(
                "timestamp-ms", int(time.time() * 1000))
            doc["snapshot-log"] = doc.get("snapshot-log", []) + [
                {"timestamp-ms": s["timestamp-ms"],
                 "snapshot-id": s["snapshot-id"]}]
            # v3 row lineage: the server derives `next-row-id`; there is no
            # TableUpdate that sets it.
            if doc.get("format-version", 2) >= 3 and "first-row-id" in s:
                doc["next-row-id"] = s["first-row-id"] + s.get("added-rows", 0)
        elif action == "set-snapshot-ref":
            refs = doc.setdefault("refs", {})
            refs[u["ref-name"]] = {
                "snapshot-id": u["snapshot-id"],
                "type": u.get("type", "branch"),
            }
            if u["ref-name"] == "main":
                doc["current-snapshot-id"] = u["snapshot-id"]
        elif action == "add-schema":
            doc["schemas"] = doc.get("schemas", []) + [u["schema"]]
        elif action == "set-current-schema":
            doc["current-schema-id"] = u["schema-id"]
        elif action == "set-properties":
            doc.setdefault("properties", {}).update(u["updates"])
        elif action == "remove-properties":
            for k in u["removals"]:
                doc.get("properties", {}).pop(k, None)
        elif action == "set-location":
            doc["location"] = u["location"]
        elif action == "upgrade-format-version":
            doc["format-version"] = u["format-version"]
        else:
            return "unsupported update action %r" % action
    return None

TABLES = sorted(
    d for d in os.listdir(FIXTURES)
    if os.path.isdir(os.path.join(FIXTURES, d, "metadata"))
)


def current_metadata(table):
    """The fixture's newest metadata file, by version then last-updated-ms."""
    d = os.path.join(FIXTURES, table, "metadata")
    best = None
    for name in os.listdir(d):
        if not name.endswith(".metadata.json"):
            continue
        head = name[1:] if name.startswith("v") else name
        digits = ""
        for ch in head:
            if ch.isdigit():
                digits += ch
            else:
                break
        if not digits:
            continue
        with open(os.path.join(d, name)) as f:
            doc = json.load(f)
        key = (int(digits), doc.get("last-updated-ms", 0))
        if best is None or key > best[0]:
            best = (key, name, doc)
    if best is None:
        raise KeyError(table)
    return best[1], best[2]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _error(self, code, kind, message):
        self._send(code, {"error": {
            "message": message, "type": kind, "code": code}})

    def _authorized(self):
        auth = self.headers.get("Authorization", "")
        return auth == "Bearer " + TOKEN

    def _body(self):
        n = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(n) or b"{}")

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        path = unquote(urlparse(self.path).path)
        base = "/v1/" + PREFIX
        if not path.startswith(base):
            return self._error(404, "NoSuchEndpointException", "bad prefix")
        if not self._authorized():
            return self._error(401, "NotAuthorizedException", "no bearer")
        parts = [p for p in path[len(base):].split("/") if p]
        if len(parts) < 3 or parts[0] != "namespaces" or parts[2] != "tables":
            return self._error(404, "NoSuchEndpointException", "unknown " + path)
        if parts[1] != WRITE_NAMESPACE:
            return self._error(
                403, "ForbiddenException",
                "namespace %r is read-only; write to %r"
                % (parts[1], WRITE_NAMESPACE))
        try:
            req = self._body()
        except Exception as e:
            return self._error(400, "BadRequestException", str(e))

        key = self.headers.get("Idempotency-Key", "")
        with LOCK:
            if key and key in IDEMPOTENT:
                code, obj = IDEMPOTENT[key]
                return self._send(code, obj)
            if len(parts) == 3:
                return self._create_table(req)
            return self._commit_table(parts[3], req, key)

    def _create_table(self, req):
        name = req.get("name")
        if not name:
            return self._error(400, "BadRequestException", "no name")
        if name in WRITTEN:
            return self._error(
                409, "AlreadyExistsException", "table exists: " + name)
        doc = _new_metadata(req, name)
        loc = _write_metadata(name, doc, 0)
        return self._send(200, {"metadata-location": loc, "metadata": doc,
                                "config": {}})

    def _commit_table(self, name, req, key=""):
        entry = WRITTEN.get(name)
        if entry is None:
            return self._error(
                404, "NoSuchTableException", "no table " + name)
        loc, doc, version = entry
        # `always_5xx` is a server without idempotency-key support: it applies
        # the first commit, then answers 500 to that one and to every repeat,
        # never recording the key. The client can only report that it does not
        # know whether the commit landed.
        broken = name.startswith("always_5xx")
        if broken and name in SEEN_5XX:
            return self._error(
                500, "ServerErrorException", "still broken (rigged)")
        # `conflict_once` answers 409 the first time, so the client's reload
        # and retry is exercised against a server that really did refuse.
        if name.startswith("conflict_once") and name not in SEEN_409:
            SEEN_409.add(name)
            return self._error(
                409, "CommitFailedException",
                "the requirements are not met (rigged, once)")
        why = _check_requirements(doc, req.get("requirements"))
        if why is not None:
            return self._error(409, "CommitFailedException", why)
        updated = json.loads(json.dumps(doc))
        why = _apply_updates(updated, req.get("updates"))
        if why is not None:
            return self._error(400, "BadRequestException", why)
        updated["metadata-log"] = (updated.get("metadata-log") or []) + [
            {"timestamp-ms": doc["last-updated-ms"],
             "metadata-file": loc}]
        new_loc = _write_metadata(name, updated, version + 1)
        ok = {"metadata-location": new_loc, "metadata": updated}
        if broken:
            SEEN_5XX.add(name)
            return self._error(
                500, "ServerErrorException", "applied, then died (rigged)")
        # The commit is applied and recorded under its key *before* the
        # answer, so a retry of it is answered from the record. That is what
        # makes `unknown_state` — applied, then 500 — recoverable: the retry
        # replays this success instead of committing a second time.
        if key:
            IDEMPOTENT[key] = (200, ok)
        if name.startswith("unknown_state"):
            return self._error(
                500, "ServerErrorException", "applied, then died (rigged)")
        return self._send(200, ok)

    def do_GET(self):
        path = unquote(urlparse(self.path).path)
        if path == "/v1/config":
            if not self._authorized():
                return self._error(401, "NotAuthorizedException", "no bearer")
            return self._send(200, {
                "defaults": {"clients": "4"},
                "overrides": {"prefix": PREFIX},
                "endpoints": [
                    "GET /v1/{prefix}/namespaces",
                    "GET /v1/{prefix}/namespaces/{namespace}/tables",
                    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                    "POST /v1/{prefix}/namespaces/{namespace}/tables",
                    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                ],
            })

        base = "/v1/" + PREFIX
        if not path.startswith(base):
            return self._error(404, "NoSuchEndpointException", "bad prefix")
        if not self._authorized():
            return self._error(401, "NotAuthorizedException", "no bearer")
        rest = path[len(base):]

        if rest == "/namespaces":
            return self._send(
                200, {"namespaces": [[NAMESPACE], [WRITE_NAMESPACE]]})

        parts = [p for p in rest.split("/") if p]
        if len(parts) == 3 and parts[0] == "namespaces" and parts[2] == "tables":
            if parts[1] != NAMESPACE:
                return self._error(
                    404, "NoSuchNamespaceException", "no namespace " + parts[1])
            return self._send(200, {"identifiers": [
                {"namespace": [NAMESPACE], "name": t} for t in TABLES]})

        if (len(parts) == 4 and parts[0] == "namespaces"
                and parts[2] == "tables" and parts[1] == WRITE_NAMESPACE):
            entry = WRITTEN.get(parts[3])
            if entry is None:
                return self._error(
                    404, "NoSuchTableException", "no table " + parts[3])
            return self._send(200, {"metadata-location": entry[0],
                                    "metadata": entry[1], "config": {}})

        if (len(parts) == 3 and parts[0] == "namespaces"
                and parts[2] == "tables" and parts[1] == WRITE_NAMESPACE):
            return self._send(200, {"identifiers": [
                {"namespace": [WRITE_NAMESPACE], "name": t}
                for t in sorted(WRITTEN)]})

        if len(parts) == 4 and parts[0] == "namespaces" and parts[2] == "tables":
            if parts[1] != NAMESPACE:
                return self._error(
                    404, "NoSuchNamespaceException", "no namespace " + parts[1])
            try:
                name, doc = current_metadata(parts[3])
            except (KeyError, FileNotFoundError, NotADirectoryError):
                return self._error(
                    404, "NoSuchTableException", "no table " + parts[3])
            delegation = self.headers.get("X-Iceberg-Access-Delegation", "")
            location = os.path.abspath(
                os.path.join(FIXTURES, parts[3], "metadata", name))
            body = {
                "metadata-location": "file://" + location,
                "metadata": doc,
                "config": {
                    "echo.delegation": delegation,
                    "s3.region": "us-east-1",
                },
            }
            if delegation == "vended-credentials":
                body["storage-credentials"] = [{
                    "prefix": os.environ.get(
                        "ICEBERG_TEST_S3_PREFIX", "s3://iceberg-test/"),
                    "config": {
                        "s3.access-key-id": os.environ.get(
                            "AWS_ACCESS_KEY_ID", "minioadmin"),
                        "s3.secret-access-key": os.environ.get(
                            "AWS_SECRET_ACCESS_KEY", "minioadmin"),
                        "s3.region": "us-east-1",
                    },
                }]
                endpoint = os.environ.get("AWS_ENDPOINT_URL_S3", "")
                if endpoint:
                    body["storage-credentials"][0]["config"][
                        "s3.endpoint"] = endpoint
            return self._send(200, body)

        return self._error(404, "NoSuchEndpointException", "unknown " + path)


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    print("http://127.0.0.1:%d" % server.server_address[1], flush=True)
    print("warehouse: %s" % WAREHOUSE, file=sys.stderr, flush=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Event().wait()


if __name__ == "__main__":
    main()
