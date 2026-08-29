"""Iceberg REST catalog — request shaping and response parsing, no transport.

**Status: no HTTP client.** A REST catalog client needs an HTTPS client with
TLS, and as of this writing there is no Mojo HTTP package that resolves from
conda on both of this repo's environments:

    floki           -> no candidates were found for floki
    flare           -> no candidates were found for flare
    lightbug-http   -> no candidates were found
    fire-http       -> no candidates were found

(`flare` exists as a source checkout at github.com/ehsanmok/flare, but it is
not published to a channel and builds OpenSSL FFI shims on activation, so
depending on it would make this tin unbuildable in CI.)

Rather than ship nothing, this module implements **everything about the REST
catalog that does not need a socket**, so that plugging in a client later is a
small, obvious change:

* `RestCatalogConfig` builds the URLs and headers for `GET /v1/config`,
  `loadTable`, `listNamespaces` and `listTables`, including the `prefix` a
  config response can prepend to every subsequent path, bearer authorization,
  and the `X-Iceberg-Access-Delegation: vended-credentials` header.
* `LoadTableResult` parses a `loadTable` response body — the inline
  `metadata`, its `metadata-location`, per-table `config`, and
  `storage-credentials` — into a real `TableMetadata`.

Both halves are tested. What is missing is only the call itself: give
`RestCatalog` a function that turns (method, url, headers) into a response body
and the rest already works.
"""

from std.collections import Dict

from ..io import FileIO
from ..json import Json, json_quote, parse_json, substr
from ..metadata import TableMetadata


comptime ACCESS_DELEGATION_HEADER = String("X-Iceberg-Access-Delegation")
comptime VENDED_CREDENTIALS = String("vended-credentials")
comptime REMOTE_SIGNING = String("remote-signing")


@fieldwise_init
struct Header(Copyable, Movable):
    var name: String
    var value: String


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
            raise Error(
                "iceberg: loadTable response has no 'metadata' object"
            )
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
        """`GET /v1/config` — always unprefixed; it is what supplies a prefix."""
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
            out += String(_HEXUP[byte = Int(c >> 4)])
            out += String(_HEXUP[byte = Int(c & 0xF)])
    return out^
