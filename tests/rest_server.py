#!/usr/bin/env python3
"""A minimal Iceberg REST catalog, backed by the checked-in fixture tables.

Enough of the spec to exercise the client end to end and nothing more:

    GET  /v1/config                          -> a `prefix` override
    GET  /v1/<prefix>/namespaces             -> ListNamespacesResponse
    GET  /v1/<prefix>/namespaces/db/tables   -> ListTablesResponse
    GET  /v1/<prefix>/namespaces/db/tables/X -> LoadTableResult
    HEAD (same)                              -> 200 / 404

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
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

FIXTURES = sys.argv[1] if len(sys.argv) > 1 else "tests/fixtures"
TOKEN = os.environ.get("ICEBERG_TEST_REST_TOKEN", "test-token")
PREFIX = "ws"
NAMESPACE = "db"

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

    def do_HEAD(self):
        self.do_GET()

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
                ],
            })

        base = "/v1/" + PREFIX
        if not path.startswith(base):
            return self._error(404, "NoSuchEndpointException", "bad prefix")
        if not self._authorized():
            return self._error(401, "NotAuthorizedException", "no bearer")
        rest = path[len(base):]

        if rest == "/namespaces":
            return self._send(200, {"namespaces": [[NAMESPACE]]})

        parts = [p for p in rest.split("/") if p]
        if len(parts) == 3 and parts[0] == "namespaces" and parts[2] == "tables":
            if parts[1] != NAMESPACE:
                return self._error(
                    404, "NoSuchNamespaceException", "no namespace " + parts[1])
            return self._send(200, {"identifiers": [
                {"namespace": [NAMESPACE], "name": t} for t in TABLES]})

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
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Event().wait()


if __name__ == "__main__":
    main()
