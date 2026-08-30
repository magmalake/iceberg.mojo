"""Iceberg REST catalog — URLs, headers, responses, and the socket.

The socket is objectstore.mojo's `HttpClient`, a libcurl-backed client behind a
fixed-arity C shim. It exists because no Mojo HTTP package resolves from conda
on either of this repo's environments (floki, flare, lightbug-http and
fire-http were all tried; all reported "no candidates were found"), and an
Iceberg REST catalog is HTTPS or nothing.

Three layers, separable and separately tested:

* `RestCatalogConfig` builds the URLs and headers for `GET /v1/config`,
  `loadTable`, `listNamespaces` and `listTables`, including the `prefix` a
  config response can prepend to every subsequent path, bearer authorization,
  and the `X-Iceberg-Access-Delegation: vended-credentials` header.
* `LoadTableResult` parses a `loadTable` response body — the inline
  `metadata`, its `metadata-location`, per-table `config`, and
  `storage-credentials` — into a real `TableMetadata`.
* `RestCatalog` puts a client behind them, maps HTTP status codes onto Iceberg
  error types, and hands back a `Table` whose `FileIO` is already configured
  with whatever the catalog vended.
"""

from std.collections import Dict

from objectstore.http import Header, HttpClient, Response
from parquet import RecordBatch

from ..append import AppendResult, prepare_append
from ..delete import (
    prepare_delete,
    prepare_dynamic_partition_overwrite,
    prepare_overwrite,
)
from ..io import FileIO
from ..json import Json, json_quote, parse_json, substr
from ..manifest import DataFile
from ..metadata import MAIN_BRANCH, REF_BRANCH, TableMetadata
from ..schema import Schema
from ..transforms import PartitionSpec
from ..util import uuid4
from ..write import WriteOptions, write_data_files
from .filesystem import Table


comptime ACCESS_DELEGATION_HEADER = String("X-Iceberg-Access-Delegation")

comptime COMMIT_DELETE = 0
comptime COMMIT_OVERWRITE = 1
comptime COMMIT_DYNAMIC = 2
comptime VENDED_CREDENTIALS = String("vended-credentials")
comptime REMOTE_SIGNING = String("remote-signing")


@fieldwise_init
struct StorageCredential(Copyable, Movable):
    """One entry of a `loadTable` response's `storage-credentials`."""

    var prefix: String
    var config: Dict[String, String]


struct LoadTableResult(Copyable, Movable):
    """A parsed `LoadTableResult` body."""

    var metadata_location: String
    var has_metadata_location: Bool
    var metadata: TableMetadata
    var config: Dict[String, String]
    var storage_credentials: List[StorageCredential]

    def __init__(
        out self,
        var metadata_location: String,
        has_metadata_location: Bool,
        var metadata: TableMetadata,
        var config: Dict[String, String],
        var storage_credentials: List[StorageCredential],
    ):
        self.metadata_location = metadata_location^
        self.has_metadata_location = has_metadata_location
        self.metadata = metadata^
        self.config = config^
        self.storage_credentials = storage_credentials^

    @staticmethod
    def parse(body: String) raises -> Self:
        var doc = parse_json(body)
        var root = doc.root
        var mi = doc.get(root, "metadata")
        if mi < 0:
            raise Error("iceberg: loadTable response has no 'metadata' object")
        var m = TableMetadata.from_json(doc, mi)
        var loc = String("")
        var has_loc = False
        var li = doc.get(root, "metadata-location")
        if li >= 0 and not doc.is_null(li):
            loc = doc.as_string(li)
            has_loc = True
            m.metadata_file_location = loc
        var creds = List[StorageCredential]()
        var sc = doc.get(root, "storage-credentials")
        if sc >= 0 and not doc.is_null(sc):
            for k in range(doc.size(sc)):
                var e = doc.at(sc, k)
                creds.append(
                    StorageCredential(
                        doc.opt_string(e, "prefix", ""),
                        doc.string_map(e, "config"),
                    )
                )
        return Self(loc^, has_loc, m^, doc.string_map(root, "config"), creds^)


struct RestCatalogConfig(Copyable, Movable):
    """URL and header construction for the REST catalog API."""

    var uri: String
    var prefix: String
    """The `prefix` a `GET /v1/config` response asks clients to insert."""
    var token: String
    var has_token: Bool
    var warehouse: String
    var vend_credentials: Bool
    var extra_headers: List[Header]

    def __init__(out self, var uri: String):
        var u = uri^
        while u.endswith("/"):
            u = substr(u, 0, u.byte_length() - 1)
        self.uri = u^
        self.prefix = ""
        self.token = ""
        self.has_token = False
        self.warehouse = ""
        self.vend_credentials = False
        self.extra_headers = []

    def with_token(mut self, var token: String):
        self.token = token^
        self.has_token = True

    def with_warehouse(mut self, var warehouse: String):
        self.warehouse = warehouse^

    def _base(self) -> String:
        if self.prefix == "":
            return self.uri + "/v1"
        return self.uri + "/v1/" + self.prefix

    # ── endpoints ──────────────────────────────────────────────────────────
    def config_url(self) -> String:
        """`GET /v1/config` — always unprefixed; it is what supplies a prefix.
        """
        var u = self.uri + "/v1/config"
        if self.warehouse != "":
            u += "?warehouse=" + url_encode(self.warehouse)
        return u^

    def namespaces_url(self, parent: String = "") -> String:
        var u = self._base() + "/namespaces"
        if parent != "":
            u += "?parent=" + url_encode(encode_namespace(parent))
        return u^

    def tables_url(self, namespace: String) -> String:
        return (
            self._base()
            + "/namespaces/"
            + url_encode(encode_namespace(namespace))
            + "/tables"
        )

    def commit_table_url(self, namespace: String, table: String) -> String:
        return self.load_table_url(namespace, table)

    def load_table_url(self, namespace: String, table: String) -> String:
        return self.tables_url(namespace) + "/" + url_encode(table)

    def headers(self) -> List[Header]:
        var out = List[Header]()
        out.append(Header("Accept", "application/json"))
        out.append(Header("Content-Type", "application/json"))
        if self.has_token:
            out.append(Header("Authorization", "Bearer " + self.token))
        if self.vend_credentials:
            out.append(Header(ACCESS_DELEGATION_HEADER, VENDED_CREDENTIALS))
        for k in range(len(self.extra_headers)):
            out.append(self.extra_headers[k].copy())
        return out^

    def apply_config(mut self, body: String) raises:
        """Absorb a `GET /v1/config` response: `overrides`, `defaults`, `prefix`.
        """
        var doc = parse_json(body)
        var overrides = doc.string_map(doc.root, "overrides")
        var defaults = doc.string_map(doc.root, "defaults")
        if "prefix" in overrides:
            self.prefix = overrides["prefix"]
        elif "prefix" in defaults:
            self.prefix = defaults["prefix"]

    # ── responses that need no transport ───────────────────────────────────
    @staticmethod
    def parse_namespaces(body: String) raises -> List[String]:
        """`ListNamespacesResponse` -> dotted namespace names."""
        var doc = parse_json(body)
        var out = List[String]()
        var arr = doc.get(doc.root, "namespaces")
        if arr < 0:
            return out^
        for k in range(doc.size(arr)):
            var levels = doc.at(arr, k)
            var name = String("")
            for j in range(doc.size(levels)):
                if j > 0:
                    name += "."
                name += doc.as_string(doc.at(levels, j))
            out.append(name^)
        return out^

    @staticmethod
    def parse_tables(body: String) raises -> List[String]:
        """`ListTablesResponse` -> table names."""
        var doc = parse_json(body)
        var out = List[String]()
        var arr = doc.get(doc.root, "identifiers")
        if arr < 0:
            return out^
        for k in range(doc.size(arr)):
            out.append(doc.req_string(doc.at(arr, k), "name"))
        return out^


def encode_namespace(dotted: String) -> String:
    """Multipart namespaces travel as one path segment joined by 0x1F."""
    var out = List[UInt8]()
    var b = dotted.as_bytes()
    for k in range(len(b)):
        if b[k] == 46:  # '.'
            out.append(0x1F)
        else:
            out.append(b[k])
    return String(StringSlice(unsafe_from_utf8=Span(out)))


comptime _HEXUP = String("0123456789ABCDEF")


def url_encode(s: String) -> String:
    """Percent-encode everything outside the unreserved set."""
    var out = String("")
    var b = s.as_bytes()
    for k in range(len(b)):
        var c = b[k]
        var unreserved = (
            (c >= 48 and c <= 57)
            or (c >= 65 and c <= 90)
            or (c >= 97 and c <= 122)
            or c == 45
            or c == 46
            or c == 95
            or c == 126
        )
        if unreserved:
            out += String(StringSlice(unsafe_from_utf8=Span(b)[k : k + 1]))
        else:
            out += "%"
            out += String(_HEXUP[byte=Int(c >> 4)])
            out += String(_HEXUP[byte=Int(c & 0xF)])
    return out^


# ── the socket ──────────────────────────────────────────────────────────────
def rest_error(what: String, resp: Response) raises -> Error:
    """Turn a REST error response into an Error naming what actually happened.

    The spec's `ErrorModel` carries `{"error": {"message", "type", "code"}}`;
    servers that do not send one still leave a status code worth reporting.
    """
    var detail = String("")
    try:
        var doc = parse_json(resp.text())
        var e = doc.get(doc.root, "error")
        if e >= 0:
            var msg = doc.opt_string(e, "message", "")
            var kind = doc.opt_string(e, "type", "")
            if kind != "":
                detail = kind + ": " + msg
            else:
                detail = msg^
    except:
        detail = resp.body_excerpt(200)

    var name = String("error")
    if resp.status == 400:
        name = String("bad request")
    elif resp.status == 401:
        name = String("not authenticated")
    elif resp.status == 403:
        name = String("forbidden")
    elif resp.status == 404:
        name = String("not found")
    elif resp.status == 409:
        name = String("conflict")
    elif resp.status == 419:
        name = String("credentials expired")
    elif resp.status == 503:
        name = String("service unavailable")
    elif resp.status >= 500:
        name = String("server error")
    return Error(
        "iceberg: "
        + what
        + " failed with "
        + String(resp.status)
        + " ("
        + name
        + ")"
        + (": " + detail if detail != "" else "")
    )


struct RestCatalog(Copyable, Movable):
    """An Iceberg REST catalog reachable over HTTP(S).

    ```mojo
    var cat = RestCatalog("https://catalog.example.com")
    cat.config.with_token(token)
    cat.config.vend_credentials = True
    cat.connect()                        # GET /v1/config, absorb the prefix
    var t = cat.load_table("db", "orders")
    ```

    `connect()` is separate from construction because it does I/O, and a caller
    that already knows the prefix (or is talking to a server without one) can
    skip it.
    """

    var config: RestCatalogConfig
    var client: HttpClient
    var io: FileIO
    """The `FileIO` template every loaded table starts from — rebases and
    storage properties set here survive into each table."""

    def __init__(out self, var uri: String):
        self.config = RestCatalogConfig(uri^)
        self.client = HttpClient()
        self.io = FileIO.local()

    def __init__(out self, var config: RestCatalogConfig, var io: FileIO):
        self.config = config^
        self.client = HttpClient()
        self.io = io^

    def _get(self, url: String, what: String) raises -> String:
        var resp = self.client.get(url, self.config.headers())
        if not resp.ok():
            var err = rest_error(what, resp)
            raise err^
        return resp.text()

    def _post(self, url: String, body: String, what: String) raises -> String:
        var resp = self.client.post(url, body.as_bytes(), self.config.headers())
        if not resp.ok():
            var err = rest_error(what, resp)
            raise err^
        return resp.text()

    def connect(mut self) raises:
        """`GET /v1/config` — absorbs `overrides`, `defaults` and `prefix`."""
        var body = self._get(self.config.config_url(), "GET /v1/config")
        self.config.apply_config(body)

    def list_namespaces(self, parent: String = "") raises -> List[String]:
        return RestCatalogConfig.parse_namespaces(
            self._get(
                self.config.namespaces_url(parent), "GET /v1/.../namespaces"
            )
        )

    def list_tables(self, namespace: String) raises -> List[String]:
        return RestCatalogConfig.parse_tables(
            self._get(self.config.tables_url(namespace), "GET /v1/.../tables")
        )

    def load_table_result(
        self, namespace: String, table: String
    ) raises -> LoadTableResult:
        return LoadTableResult.parse(
            self._get(
                self.config.load_table_url(namespace, table),
                "loadTable " + namespace + "." + table,
            )
        )

    def table_exists(self, namespace: String, table: String) raises -> Bool:
        var resp = self.client.head(
            self.config.load_table_url(namespace, table), self.config.headers()
        )
        if resp.status == 404:
            return False
        if not resp.ok():
            var err = rest_error("HEAD " + namespace + "." + table, resp)
            raise err^
        return True

    def load_table(self, namespace: String, table: String) raises -> Table:
        """Load a table, configuring its `FileIO` from what the catalog says.

        The table's own `config` becomes storage properties (`s3.endpoint`,
        `s3.region`, …) and every `storage-credentials` entry is registered
        under its prefix, so a vended, per-prefix STS credential is used for
        exactly the objects it was issued for.
        """
        var res = self.load_table_result(namespace, table)
        var io = self.io.copy()
        for entry in res.config.items():
            io.set(entry.key, entry.value)
        for k in range(len(res.storage_credentials)):
            io.add_storage_credential(
                res.storage_credentials[k].prefix,
                res.storage_credentials[k].config.copy(),
            )
        var loc = res.metadata_location.copy()
        var m = res.metadata.copy()
        if m.location != "":
            io.with_base(m.location)
        return Table(m^, loc^, io^, namespace + "." + table)

    # ── writing ────────────────────────────────────────────────────────────
    def _commit(
        self,
        namespace: String,
        table: String,
        body: String,
        idempotency_key: String,
        what: String,
    ) raises -> Response:
        """One `POST .../tables/{table}`, with the `Idempotency-Key` header.

        The key is the same across retries of the *same* commit, so a server
        that saw the request but could not answer can recognise the repeat
        rather than applying it twice.
        """
        var headers = self.config.headers()
        headers.append(Header("Idempotency-Key", idempotency_key))
        return self.client.post(
            self.config.commit_table_url(namespace, table),
            body.as_bytes(),
            headers,
        )

    def commit_append(
        self,
        namespace: String,
        table: String,
        data_files: List[DataFile],
        retries: Int = 4,
    ) raises -> Table:
        """Commit already-written data files as one append snapshot.

        Optimistic concurrency, the way the REST spec defines it: the request
        carries `assert-table-uuid` and `assert-ref-snapshot-id` for `main`,
        so a server whose `main` has moved answers **409** and this reloads,
        rebuilds the snapshot against the new parent — the data files are
        still valid, only the manifest list has to be rewritten — and tries
        again. A **5xx** is *not* retried: the commit may well have been
        applied, and the caller is told the state is unknown rather than
        being handed a second attempt at the same append.

        Each attempt writes a fresh manifest and manifest list; the ones a
        refused attempt wrote are orphaned, exactly as in every other
        implementation, and are nobody's to read.

        There is no `next-row-id` update in the spec's `TableUpdate` list. A
        v3 server derives it from the snapshot's `first-row-id` plus its
        `added-rows`, both of which `add-snapshot` carries.
        """
        var loaded = self.load_table(namespace, table)
        var key = uuid4()
        var attempt = 0
        while True:
            var result = prepare_append(
                loaded.io,
                loaded.metadata,
                data_files.copy(),
                Dict[String, String](),
            )
            var body = commit_append_body(
                namespace, table, loaded.metadata, result
            )
            var resp = self._commit(
                namespace, table, body, key, "commit " + table
            )
            if resp.ok():
                var out = loaded.copy()
                var doc = parse_json(resp.text())
                var loc = doc.opt_string(doc.root, "metadata-location", "")
                var meta = doc.get(doc.root, "metadata")
                if meta >= 0 and not doc.is_null(meta):
                    out.metadata = TableMetadata.from_json(doc, meta)
                else:
                    out.metadata = result.metadata.copy()
                if loc != "":
                    out.metadata.metadata_file_location = loc
                    out.metadata_location = loc^
                return out^
            if resp.status == 409 and attempt < retries:
                attempt += 1
                loaded = self.load_table(namespace, table)
                continue
            if resp.status >= 500:
                raise Error(
                    "iceberg: CommitStateUnknown — the commit of "
                    + namespace
                    + "."
                    + table
                    + " returned "
                    + String(resp.status)
                    + " and may or may not have been applied; reload the"
                    + " table before retrying: "
                    + resp.body_excerpt(200)
                )
            var err = rest_error("commit " + namespace + "." + table, resp)
            raise err^

    def commit_change(
        self,
        namespace: String,
        table: String,
        kind: Int,
        filter_dsl: String,
        batches: List[RecordBatch],
        mode: String,
        retries: Int,
    ) raises -> Table:
        """A delete or an overwrite, committed the way an append is.

        The `CommitTableRequest` is identical — one `add-snapshot` and one
        `set-snapshot-ref` behind the same two requirements — because the spec
        has no per-operation update action: what the snapshot *is* lives
        entirely in the manifest list it names. So the retry story is the
        same too, and one 409 reload re-plans the whole operation against the
        new parent, because a delete planned against a snapshot that has moved
        may now be missing rows a concurrent writer added.
        """
        var loaded = self.load_table(namespace, table)
        var key = uuid4()
        var attempt = 0
        while True:
            var result: AppendResult
            if kind == COMMIT_DELETE:
                result = prepare_delete(
                    loaded.io, loaded.metadata, filter_dsl, mode
                )
            elif kind == COMMIT_OVERWRITE:
                result = prepare_overwrite(
                    loaded.io, loaded.metadata, batches, filter_dsl
                )
            else:
                result = prepare_dynamic_partition_overwrite(
                    loaded.io, loaded.metadata, batches
                )
            var body = commit_append_body(
                namespace, table, loaded.metadata, result
            )
            var resp = self._commit(
                namespace, table, body, key, "commit " + table
            )
            if resp.ok():
                var out = loaded.copy()
                var doc = parse_json(resp.text())
                var loc = doc.opt_string(doc.root, "metadata-location", "")
                var meta = doc.get(doc.root, "metadata")
                if meta >= 0 and not doc.is_null(meta):
                    out.metadata = TableMetadata.from_json(doc, meta)
                else:
                    out.metadata = result.metadata.copy()
                if loc != "":
                    out.metadata.metadata_file_location = loc
                    out.metadata_location = loc^
                return out^
            if resp.status == 409 and attempt < retries:
                attempt += 1
                # A fresh key: this is a *different* commit now, planned
                # against a different parent, and must not be deduplicated
                # against the one the server refused.
                key = uuid4()
                loaded = self.load_table(namespace, table)
                continue
            if resp.status >= 500:
                raise Error(
                    "iceberg: CommitStateUnknown — the commit of "
                    + namespace
                    + "."
                    + table
                    + " returned "
                    + String(resp.status)
                    + " and may or may not have been applied; reload the"
                    + " table before retrying: "
                    + resp.body_excerpt(200)
                )
            var err = rest_error("commit " + namespace + "." + table, resp)
            raise err^

    def delete_where(
        self,
        namespace: String,
        table: String,
        filter_dsl: String,
        mode: String = String(""),
        retries: Int = 4,
    ) raises -> Table:
        """`DELETE FROM t WHERE <filter>`, committed through the catalog."""
        return self.commit_change(
            namespace,
            table,
            COMMIT_DELETE,
            filter_dsl,
            List[RecordBatch](),
            mode,
            retries,
        )

    def overwrite(
        self,
        namespace: String,
        table: String,
        batches: List[RecordBatch],
        filter_dsl: String = String('["true"]'),
        retries: Int = 4,
    ) raises -> Table:
        """Delete what the filter matches and add `batches`, in one snapshot.
        """
        return self.commit_change(
            namespace,
            table,
            COMMIT_OVERWRITE,
            filter_dsl,
            batches,
            String(""),
            retries,
        )

    def dynamic_partition_overwrite(
        self,
        namespace: String,
        table: String,
        batches: List[RecordBatch],
        retries: Int = 4,
    ) raises -> Table:
        """Replace exactly the partitions the new rows land in."""
        return self.commit_change(
            namespace,
            table,
            COMMIT_DYNAMIC,
            String('["true"]'),
            batches,
            String(""),
            retries,
        )

    def append(
        self,
        namespace: String,
        table: String,
        batches: List[RecordBatch],
        retries: Int = 4,
    ) raises -> Table:
        """Write `batches` into the table's location, then commit them."""
        var loaded = self.load_table(namespace, table)
        var options = WriteOptions.from_properties(loaded.metadata.properties)
        var files = write_data_files(
            loaded.io,
            loaded.metadata.location,
            batches,
            loaded.metadata.schema(),
            loaded.metadata.spec(),
            loaded.metadata.default_sort_order_id,
            options,
        )
        return self.commit_append(namespace, table, files, retries)

    def create_table(
        mut self,
        namespace: String,
        table: String,
        schema: Schema,
        spec: PartitionSpec = PartitionSpec.unpartitioned(),
        var properties: Dict[String, String] = Dict[String, String](),
        format_version: Int = 2,
        location: String = String(""),
    ) raises -> Table:
        """`POST .../tables` — a `CreateTableRequest`.

        The format version travels as the `format-version` property, which is
        how every REST catalog is told; there is no field for it in the
        request body.
        """
        properties["format-version"] = String(format_version)
        var body = String('{"name":') + json_quote(table)
        if location != "":
            body += ',"location":' + json_quote(location)
        body += ',"schema":' + schema.to_json()
        body += ',"partition-spec":' + spec.to_json()
        body += ',"properties":' + _string_map_json(properties)
        body += "}"
        var text = self._post(
            self.config.tables_url(namespace),
            body,
            "createTable " + namespace + "." + table,
        )
        var res = LoadTableResult.parse(text)
        var io = self.io.copy()
        for entry in res.config.items():
            io.set(entry.key, entry.value)
        for k in range(len(res.storage_credentials)):
            io.add_storage_credential(
                res.storage_credentials[k].prefix,
                res.storage_credentials[k].config.copy(),
            )
        var m = res.metadata.copy()
        if m.location != "":
            io.with_base(m.location)
        var loc = res.metadata_location.copy()
        return Table(m^, loc^, io^, namespace + "." + table)


def _string_map_json(m: Dict[String, String]) -> String:
    var keys = List[String]()
    for entry in m.items():
        keys.append(entry.key)
    for i in range(1, len(keys)):
        var j = i
        while j > 0 and keys[j] < keys[j - 1]:
            keys.swap_elements(j, j - 1)
            j -= 1
    var out = String("{")
    for k in range(len(keys)):
        if k > 0:
            out += ","
        out += json_quote(keys[k])
        out += ":"
        try:
            out += json_quote(m[keys[k]])
        except:
            out += '""'
    out += "}"
    return out^


def commit_append_body(
    namespace: String,
    table: String,
    current: TableMetadata,
    result: AppendResult,
) raises -> String:
    """The `CommitTableRequest` an append produces.

    Two requirements, which together are exactly what an optimistic append
    needs: the table must still be the one we read (`assert-table-uuid`), and
    `main` must still point where it did when the snapshot's parent was
    chosen (`assert-ref-snapshot-id`, with a null id when the table has no
    snapshot yet).
    """
    var snapshot = result.metadata.snapshot_by_id(result.snapshot_id)
    var out = String('{"identifier":{"namespace":[')
    var parts = namespace.split(".")
    for k in range(len(parts)):
        if k > 0:
            out += ","
        out += json_quote(String(parts[k]))
    out += '],"name":' + json_quote(table) + "}"

    out += ',"requirements":['
    out += (
        '{"type":"assert-table-uuid","uuid":'
        + json_quote(current.table_uuid)
        + "}"
    )
    out += (
        ',{"type":"assert-ref-snapshot-id","ref":'
        + json_quote(MAIN_BRANCH)
        + ',"snapshot-id":'
    )
    if current.has_current_snapshot:
        out += String(current.current_snapshot_id)
    else:
        out += "null"
    out += "}]"

    out += ',"updates":['
    out += '{"action":"add-snapshot","snapshot":' + snapshot.to_json() + "}"
    out += (
        ',{"action":"set-snapshot-ref","ref-name":'
        + json_quote(MAIN_BRANCH)
        + ',"type":'
        + json_quote(REF_BRANCH)
        + ',"snapshot-id":'
        + String(result.snapshot_id)
        + "}"
    )
    out += "]}"
    return out^
