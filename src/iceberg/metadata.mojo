"""`TableMetadata` — the whole of an Iceberg `*.metadata.json`, v1 through v3.

Every field in the spec's table-metadata table is parsed and re-serialized,
including the v1-only singular `schema` / `partition-spec` forms, the v2/v3
plural `schemas` / `partition-specs`, `refs`, `statistics`,
`partition-statistics`, the v3 `next-row-id` and `encryption-keys`, and the v3
snapshot fields `first-row-id` / `added-rows` / `key-id`.

Reading is deliberately forgiving in the directions the spec asks for:
unrecognised top-level keys are kept verbatim in `extra` and written back out,
an unknown `format-version` above 3 is rejected (the spec requires it), and a
v4 metadata file's optional `location` is tolerated.

Snapshot selection lives here too — `current_snapshot`, `snapshot_by_id`,
`snapshot_for_ref`, `snapshot_as_of` — along with filesystem-table discovery
(`version-hint.text` plus `v<N>.metadata.json`).
"""

from std.collections import Dict

from .json import (
    Json,
    JsonNode,
    parse_json,
    json_quote,
    substr,
    write_json_string,
)
from .schema import Schema
from .transforms import PartitionSpec, SortOrder, PARTITION_DATA_ID_START
from .types import TypeStore

comptime MAIN_BRANCH = String("main")
comptime SUPPORTED_FORMAT_VERSION: Int = 3
comptime INITIAL_SPEC_ID: Int = 0
comptime INITIAL_SORT_ORDER_ID: Int = 0


@fieldwise_init
struct Snapshot(Copyable, Movable, Writable):
    """One table snapshot."""

    var snapshot_id: Int64
    var parent_snapshot_id: Int64
    var has_parent: Bool
    var sequence_number: Int64
    var timestamp_ms: Int64
    var manifest_list: String
    var manifests: List[String]
    """v1 only: an inline manifest list. Empty when `manifest-list` is set."""
    var summary: Dict[String, String]
    var schema_id: Int
    var has_schema_id: Bool
    var first_row_id: Int64
    var has_first_row_id: Bool
    var added_rows: Int64
    var has_added_rows: Bool
    var key_id: String
    var has_key_id: Bool

    def operation(self) -> String:
        """The required `operation` summary key; "append" when absent."""
        try:
            return self.summary["operation"]
        except:
            return "append"

    def summary_int(self, key: String, dflt: Int64) -> Int64:
        # Two `try`s rather than one: a missing key raises `DictKeyError` and
        # an unparseable value raises `Error`, and one block cannot have both
        # error types.
        var raw: String
        try:
            raw = self.summary[key]
        except:
            return dflt
        try:
            return Int64(Int(raw))
        except:
            return dflt

    def write_to(self, mut writer: Some[Writer]):
        writer.write(
            "Snapshot(",
            self.snapshot_id,
            ", seq=",
            self.sequence_number,
            ", ",
            self.operation(),
            ")",
        )

    @staticmethod
    def from_json(doc: Json, i: Int, format_version: Int) raises -> Self:
        var s = Self(
            doc.req_int(i, "snapshot-id"),
            0,
            False,
            0,
            0,
            "",
            [],
            Dict[String, String](),
            0,
            False,
            0,
            False,
            0,
            False,
            "",
            False,
        )
        var p = doc.get(i, "parent-snapshot-id")
        if p >= 0 and not doc.is_null(p):
            s.parent_snapshot_id = doc.as_int(p)
            s.has_parent = True
        s.sequence_number = doc.opt_int(i, "sequence-number", 0)
        s.timestamp_ms = doc.req_int(i, "timestamp-ms")
        s.manifest_list = doc.opt_string(i, "manifest-list", "")
        s.manifests = doc.string_list(i, "manifests")
        s.summary = doc.string_map(i, "summary")
        var sc = doc.get(i, "schema-id")
        if sc >= 0 and not doc.is_null(sc):
            s.schema_id = Int(doc.as_int(sc))
            s.has_schema_id = True
        var fr = doc.get(i, "first-row-id")
        if fr >= 0 and not doc.is_null(fr):
            s.first_row_id = doc.as_int(fr)
            s.has_first_row_id = True
        var ar = doc.get(i, "added-rows")
        if ar >= 0 and not doc.is_null(ar):
            s.added_rows = doc.as_int(ar)
            s.has_added_rows = True
        var ki = doc.get(i, "key-id")
        if ki >= 0 and not doc.is_null(ki):
            s.key_id = doc.as_string(ki)
            s.has_key_id = True
        return s^

    def to_json(self) -> String:
        var out = String('{"snapshot-id":') + String(self.snapshot_id)
        if self.has_parent:
            out += ',"parent-snapshot-id":' + String(self.parent_snapshot_id)
        out += ',"sequence-number":' + String(self.sequence_number)
        out += ',"timestamp-ms":' + String(self.timestamp_ms)
        out += ',"summary":' + _string_map_json(self.summary)
        if self.manifest_list != "":
            out += ',"manifest-list":' + json_quote(self.manifest_list)
        elif len(self.manifests) > 0:
            out += ',"manifests":' + _string_array_json(self.manifests)
        if self.has_schema_id:
            out += ',"schema-id":' + String(self.schema_id)
        if self.has_first_row_id:
            out += ',"first-row-id":' + String(self.first_row_id)
        if self.has_added_rows:
            out += ',"added-rows":' + String(self.added_rows)
        if self.has_key_id:
            out += ',"key-id":' + json_quote(self.key_id)
        out += "}"
        return out^


comptime REF_BRANCH = String("branch")
comptime REF_TAG = String("tag")


@fieldwise_init
struct SnapshotRef(Copyable, Movable):
    """A named branch or tag plus its retention policy."""

    var name: String
    var snapshot_id: Int64
    var type: String
    var min_snapshots_to_keep: Int64
    var has_min_snapshots_to_keep: Bool
    var max_snapshot_age_ms: Int64
    var has_max_snapshot_age_ms: Bool
    var max_ref_age_ms: Int64
    var has_max_ref_age_ms: Bool

    def is_branch(self) -> Bool:
        return self.type == REF_BRANCH

    @staticmethod
    def from_json(doc: Json, i: Int, var name: String) raises -> Self:
        var r = Self(
            name^,
            doc.req_int(i, "snapshot-id"),
            doc.opt_string(i, "type", REF_BRANCH),
            0,
            False,
            0,
            False,
            0,
            False,
        )
        var a = doc.get(i, "min-snapshots-to-keep")
        if a >= 0 and not doc.is_null(a):
            r.min_snapshots_to_keep = doc.as_int(a)
            r.has_min_snapshots_to_keep = True
        var b = doc.get(i, "max-snapshot-age-ms")
        if b >= 0 and not doc.is_null(b):
            r.max_snapshot_age_ms = doc.as_int(b)
            r.has_max_snapshot_age_ms = True
        var c = doc.get(i, "max-ref-age-ms")
        if c >= 0 and not doc.is_null(c):
            r.max_ref_age_ms = doc.as_int(c)
            r.has_max_ref_age_ms = True
        return r^

    def to_json(self) -> String:
        var out = String('{"snapshot-id":') + String(self.snapshot_id)
        out += ',"type":' + json_quote(self.type)
        if self.has_min_snapshots_to_keep:
            out += ',"min-snapshots-to-keep":' + String(
                self.min_snapshots_to_keep
            )
        if self.has_max_snapshot_age_ms:
            out += ',"max-snapshot-age-ms":' + String(self.max_snapshot_age_ms)
        if self.has_max_ref_age_ms:
            out += ',"max-ref-age-ms":' + String(self.max_ref_age_ms)
        out += "}"
        return out^


@fieldwise_init
struct SnapshotLogEntry(Copyable, Movable):
    var timestamp_ms: Int64
    var snapshot_id: Int64


@fieldwise_init
struct MetadataLogEntry(Copyable, Movable):
    var timestamp_ms: Int64
    var metadata_file: String


@fieldwise_init
struct BlobMetadata(Copyable, Movable):
    var type: String
    var snapshot_id: Int64
    var sequence_number: Int64
    var fields: List[Int]
    var properties: Dict[String, String]


@fieldwise_init
struct StatisticsFile(Copyable, Movable):
    """A Puffin statistics file attached to one snapshot."""

    var snapshot_id: Int64
    var statistics_path: String
    var file_size_in_bytes: Int64
    var file_footer_size_in_bytes: Int64
    var key_metadata: String
    var has_key_metadata: Bool
    var blob_metadata: List[BlobMetadata]

    @staticmethod
    def from_json(doc: Json, i: Int) raises -> Self:
        var blobs = List[BlobMetadata]()
        var bm = doc.get(i, "blob-metadata")
        if bm >= 0:
            for k in range(doc.size(bm)):
                var b = doc.at(bm, k)
                blobs.append(
                    BlobMetadata(
                        doc.opt_string(b, "type", ""),
                        doc.opt_int(b, "snapshot-id", 0),
                        doc.opt_int(b, "sequence-number", 0),
                        doc.int_list(b, "fields"),
                        doc.string_map(b, "properties"),
                    )
                )
        var s = Self(
            doc.req_int(i, "snapshot-id"),
            doc.req_string(i, "statistics-path"),
            doc.opt_int(i, "file-size-in-bytes", 0),
            doc.opt_int(i, "file-footer-size-in-bytes", 0),
            "",
            False,
            blobs^,
        )
        var km = doc.get(i, "key-metadata")
        if km >= 0 and not doc.is_null(km):
            s.key_metadata = doc.as_string(km)
            s.has_key_metadata = True
        return s^

    def to_json(self) -> String:
        var out = String('{"snapshot-id":') + String(self.snapshot_id)
        out += ',"statistics-path":' + json_quote(self.statistics_path)
        out += ',"file-size-in-bytes":' + String(self.file_size_in_bytes)
        out += ',"file-footer-size-in-bytes":' + String(
            self.file_footer_size_in_bytes
        )
        if self.has_key_metadata:
            out += ',"key-metadata":' + json_quote(self.key_metadata)
        out += ',"blob-metadata":['
        for k in range(len(self.blob_metadata)):
            if k > 0:
                out += ","
            ref b = self.blob_metadata[k]
            out += '{"type":' + json_quote(b.type)
            out += ',"snapshot-id":' + String(b.snapshot_id)
            out += ',"sequence-number":' + String(b.sequence_number)
            out += ',"fields":['
            for j in range(len(b.fields)):
                if j > 0:
                    out += ","
                out += String(b.fields[j])
            out += "]"
            if len(b.properties) > 0:
                out += ',"properties":' + _string_map_json(b.properties)
            out += "}"
        out += "]}"
        return out^


@fieldwise_init
struct PartitionStatisticsFile(Copyable, Movable):
    var snapshot_id: Int64
    var statistics_path: String
    var file_size_in_bytes: Int64

    def to_json(self) -> String:
        return (
            String('{"snapshot-id":')
            + String(self.snapshot_id)
            + ',"statistics-path":'
            + json_quote(self.statistics_path)
            + ',"file-size-in-bytes":'
            + String(self.file_size_in_bytes)
            + "}"
        )


@fieldwise_init
struct EncryptionKey(Copyable, Movable):
    """A v3 table-encryption key entry.

    The spec's field table names the payload `encrypted-key-metadata` while its
    own prose calls it `key-metadata`; both spellings are accepted on read and
    the spec's table spelling is written back.
    """

    var key_id: String
    var encrypted_key_metadata: String
    var encrypted_by_id: String
    var has_encrypted_by_id: Bool
    var properties: Dict[String, String]

    @staticmethod
    def from_json(doc: Json, i: Int) raises -> Self:
        var payload = doc.opt_string(i, "encrypted-key-metadata", "")
        if payload == "":
            payload = doc.opt_string(i, "key-metadata", "")
        var e = Self(
            doc.opt_string(i, "key-id", ""),
            payload^,
            "",
            False,
            doc.string_map(i, "properties"),
        )
        var by = doc.get(i, "encrypted-by-id")
        if by >= 0 and not doc.is_null(by):
            e.encrypted_by_id = doc.as_string(by)
            e.has_encrypted_by_id = True
        return e^

    def to_json(self) -> String:
        var out = String('{"key-id":') + json_quote(self.key_id)
        out += ',"encrypted-key-metadata":' + json_quote(
            self.encrypted_key_metadata
        )
        if self.has_encrypted_by_id:
            out += ',"encrypted-by-id":' + json_quote(self.encrypted_by_id)
        if len(self.properties) > 0:
            out += ',"properties":' + _string_map_json(self.properties)
        out += "}"
        return out^


struct TableMetadata(Copyable, Movable):
    """A parsed `*.metadata.json`."""

    var format_version: Int
    var table_uuid: String
    var location: String
    var has_location: Bool
    """v4 makes `location` optional; v1-v3 always carry it."""
    var last_sequence_number: Int64
    var last_updated_ms: Int64
    var last_column_id: Int
    var schemas: List[Schema]
    var current_schema_id: Int
    var partition_specs: List[PartitionSpec]
    var default_spec_id: Int
    var last_partition_id: Int
    var properties: Dict[String, String]
    var current_snapshot_id: Int64
    var has_current_snapshot: Bool
    var snapshots: List[Snapshot]
    var snapshot_log: List[SnapshotLogEntry]
    var metadata_log: List[MetadataLogEntry]
    var sort_orders: List[SortOrder]
    var default_sort_order_id: Int
    var refs: List[SnapshotRef]
    var statistics: List[StatisticsFile]
    var partition_statistics: List[PartitionStatisticsFile]
    var next_row_id: Int64
    var has_next_row_id: Bool
    var encryption_keys: List[EncryptionKey]
    var extra_keys: List[String]
    """Top-level keys this build does not know, preserved for round-tripping."""
    var extra_values: List[String]
    var metadata_file_location: String
    """Where this file was read from, when it was read from a file."""

    def __init__(out self):
        self.format_version = 2
        self.table_uuid = ""
        self.location = ""
        self.has_location = False
        self.last_sequence_number = 0
        self.last_updated_ms = 0
        self.last_column_id = 0
        self.schemas = []
        self.current_schema_id = 0
        self.partition_specs = []
        self.default_spec_id = INITIAL_SPEC_ID
        self.last_partition_id = PARTITION_DATA_ID_START - 1
        self.properties = Dict[String, String]()
        self.current_snapshot_id = -1
        self.has_current_snapshot = False
        self.snapshots = []
        self.snapshot_log = []
        self.metadata_log = []
        self.sort_orders = []
        self.default_sort_order_id = INITIAL_SORT_ORDER_ID
        self.refs = []
        self.statistics = []
        self.partition_statistics = []
        self.next_row_id = 0
        self.has_next_row_id = False
        self.encryption_keys = []
        self.extra_keys = []
        self.extra_values = []
        self.metadata_file_location = ""

    # ── lookup ─────────────────────────────────────────────────────────────
    def schema(self) raises -> Schema:
        """The table's current schema."""
        return self.schema_by_id(self.current_schema_id)

    def schema_by_id(self, id: Int) raises -> Schema:
        for k in range(len(self.schemas)):
            if self.schemas[k].schema_id == id:
                return self.schemas[k].copy()
        raise Error("iceberg: no schema with id " + String(id))

    def has_schema(self, id: Int) -> Bool:
        for k in range(len(self.schemas)):
            if self.schemas[k].schema_id == id:
                return True
        return False

    def spec(self) raises -> PartitionSpec:
        return self.spec_by_id(self.default_spec_id)

    def spec_by_id(self, id: Int) raises -> PartitionSpec:
        for k in range(len(self.partition_specs)):
            if self.partition_specs[k].spec_id == id:
                return self.partition_specs[k].copy()
        raise Error("iceberg: no partition spec with id " + String(id))

    def has_spec(self, id: Int) -> Bool:
        for k in range(len(self.partition_specs)):
            if self.partition_specs[k].spec_id == id:
                return True
        return False

    def sort_order_by_id(self, id: Int) raises -> SortOrder:
        for k in range(len(self.sort_orders)):
            if self.sort_orders[k].order_id == id:
                return self.sort_orders[k].copy()
        raise Error("iceberg: no sort order with id " + String(id))

    def snapshot_index(self, id: Int64) -> Int:
        for k in range(len(self.snapshots)):
            if self.snapshots[k].snapshot_id == id:
                return k
        return -1

    def snapshot_by_id(self, id: Int64) raises -> Snapshot:
        var k = self.snapshot_index(id)
        if k < 0:
            raise Error("iceberg: no snapshot with id " + String(id))
        return self.snapshots[k].copy()

    def current_snapshot(self) raises -> Snapshot:
        if not self.has_current_snapshot or self.current_snapshot_id < 0:
            raise Error("iceberg: the table has no current snapshot")
        return self.snapshot_by_id(self.current_snapshot_id)

    def ref_index(self, name: String) -> Int:
        for k in range(len(self.refs)):
            if self.refs[k].name == name:
                return k
        return -1

    def snapshot_for_ref(self, name: String) raises -> Snapshot:
        """Resolve a branch or tag. `main` falls back to `current-snapshot-id`,
        which the spec says is always the main branch's snapshot even when the
        `refs` map is absent."""
        var k = self.ref_index(name)
        if k >= 0:
            return self.snapshot_by_id(self.refs[k].snapshot_id)
        if name == MAIN_BRANCH:
            return self.current_snapshot()
        raise Error("iceberg: no branch or tag named '" + name + "'")

    def snapshot_as_of(self, timestamp_ms: Int64) raises -> Snapshot:
        """The snapshot that was current at `timestamp_ms`.

        Resolved from `snapshot-log` — the record of which snapshot was current
        when — falling back to the snapshot list when the log is empty.
        """
        var best: Int64 = 0
        var best_ts: Int64 = 0
        var found = False
        for k in range(len(self.snapshot_log)):
            ref e = self.snapshot_log[k]
            if e.timestamp_ms <= timestamp_ms:
                if not found or e.timestamp_ms >= best_ts:
                    best = e.snapshot_id
                    best_ts = e.timestamp_ms
                    found = True
        if found:
            return self.snapshot_by_id(best)
        for k in range(len(self.snapshots)):
            ref s = self.snapshots[k]
            if s.timestamp_ms <= timestamp_ms:
                if not found or s.timestamp_ms >= best_ts:
                    best = s.snapshot_id
                    best_ts = s.timestamp_ms
                    found = True
        if not found:
            raise Error(
                "iceberg: no snapshot at or before " + String(timestamp_ms)
            )
        return self.snapshot_by_id(best)

    def schema_for_snapshot(self, s: Snapshot) raises -> Schema:
        """The schema in effect for a snapshot: its own `schema-id` when it
        records one, else the table's current schema."""
        if s.has_schema_id and self.has_schema(s.schema_id):
            return self.schema_by_id(s.schema_id)
        return self.schema()

    def property_(self, key: String, dflt: String) -> String:
        try:
            return self.properties[key]
        except:
            return dflt

    # ── parsing ────────────────────────────────────────────────────────────
    @staticmethod
    def parse(text: String) raises -> TableMetadata:
        var doc = parse_json(text)
        return TableMetadata.from_json(doc, doc.root)

    @staticmethod
    def from_json(doc: Json, i: Int) raises -> TableMetadata:
        var m = TableMetadata()
        m.format_version = Int(doc.req_int(i, "format-version"))
        if m.format_version > SUPPORTED_FORMAT_VERSION:
            raise Error(
                "iceberg: table format version "
                + String(m.format_version)
                + " is newer than the supported version "
                + String(SUPPORTED_FORMAT_VERSION)
            )
        if m.format_version < 1:
            raise Error(
                "iceberg: invalid format version " + String(m.format_version)
            )
        m.table_uuid = doc.opt_string(i, "table-uuid", "")
        var loc = doc.get(i, "location")
        if loc >= 0 and not doc.is_null(loc):
            m.location = doc.as_string(loc)
            m.has_location = True
        m.last_sequence_number = doc.opt_int(i, "last-sequence-number", 0)
        m.last_updated_ms = doc.opt_int(i, "last-updated-ms", 0)
        m.last_column_id = Int(doc.opt_int(i, "last-column-id", 0))

        # ── schemas: the v2+ list, or v1's singular `schema` ───────────────
        var schemas = doc.get(i, "schemas")
        if schemas >= 0 and not doc.is_null(schemas):
            for k in range(doc.size(schemas)):
                m.schemas.append(Schema.from_json(doc, doc.at(schemas, k)))
            m.current_schema_id = Int(doc.opt_int(i, "current-schema-id", 0))
        else:
            var one = doc.get(i, "schema")
            if one < 0:
                raise Error("iceberg: metadata has neither schemas nor schema")
            var s = Schema.from_json(doc, one)
            m.current_schema_id = s.schema_id
            m.schemas.append(s^)
        if not m.has_schema(m.current_schema_id) and len(m.schemas) > 0:
            # Tolerate a dangling current-schema-id rather than fail the read.
            m.current_schema_id = m.schemas[len(m.schemas) - 1].schema_id

        # ── partition specs: the v2+ list, or v1's bare field array ────────
        var specs = doc.get(i, "partition-specs")
        if specs >= 0 and not doc.is_null(specs):
            for k in range(doc.size(specs)):
                m.partition_specs.append(
                    PartitionSpec.from_json(doc, doc.at(specs, k))
                )
            m.default_spec_id = Int(
                doc.opt_int(i, "default-spec-id", Int64(INITIAL_SPEC_ID))
            )
        else:
            var one = doc.get(i, "partition-spec")
            if one >= 0 and not doc.is_null(one):
                m.partition_specs.append(
                    PartitionSpec.from_fields_json(doc, one, INITIAL_SPEC_ID)
                )
            else:
                m.partition_specs.append(PartitionSpec.unpartitioned())
            m.default_spec_id = INITIAL_SPEC_ID
        if not m.has_spec(m.default_spec_id) and len(m.partition_specs) > 0:
            m.default_spec_id = m.partition_specs[0].spec_id
        var lpid = doc.get(i, "last-partition-id")
        if lpid >= 0 and not doc.is_null(lpid):
            m.last_partition_id = Int(doc.as_int(lpid))
        else:
            m.last_partition_id = _highest_partition_id(m.partition_specs)

        m.properties = doc.string_map(i, "properties")

        var csid = doc.get(i, "current-snapshot-id")
        if csid >= 0 and not doc.is_null(csid):
            m.current_snapshot_id = doc.as_int(csid)
            # -1 is how writers spell "no current snapshot".
            m.has_current_snapshot = m.current_snapshot_id >= 0

        var snaps = doc.get(i, "snapshots")
        if snaps >= 0 and not doc.is_null(snaps):
            for k in range(doc.size(snaps)):
                m.snapshots.append(
                    Snapshot.from_json(doc, doc.at(snaps, k), m.format_version)
                )

        var slog = doc.get(i, "snapshot-log")
        if slog >= 0 and not doc.is_null(slog):
            for k in range(doc.size(slog)):
                var e = doc.at(slog, k)
                m.snapshot_log.append(
                    SnapshotLogEntry(
                        doc.req_int(e, "timestamp-ms"),
                        doc.req_int(e, "snapshot-id"),
                    )
                )

        var mlog = doc.get(i, "metadata-log")
        if mlog >= 0 and not doc.is_null(mlog):
            for k in range(doc.size(mlog)):
                var e = doc.at(mlog, k)
                m.metadata_log.append(
                    MetadataLogEntry(
                        doc.req_int(e, "timestamp-ms"),
                        doc.req_string(e, "metadata-file"),
                    )
                )

        var orders = doc.get(i, "sort-orders")
        if orders >= 0 and not doc.is_null(orders):
            for k in range(doc.size(orders)):
                m.sort_orders.append(
                    SortOrder.from_json(doc, doc.at(orders, k))
                )
        if len(m.sort_orders) == 0:
            m.sort_orders.append(SortOrder(INITIAL_SORT_ORDER_ID, []))
        m.default_sort_order_id = Int(
            doc.opt_int(
                i, "default-sort-order-id", Int64(INITIAL_SORT_ORDER_ID)
            )
        )

        var refs = doc.get(i, "refs")
        if refs >= 0 and not doc.is_null(refs):
            for k in range(doc.size(refs)):
                m.refs.append(
                    SnapshotRef.from_json(
                        doc, doc.at(refs, k), doc.key_at(refs, k)
                    )
                )

        var stats = doc.get(i, "statistics")
        if stats >= 0 and not doc.is_null(stats):
            for k in range(doc.size(stats)):
                m.statistics.append(
                    StatisticsFile.from_json(doc, doc.at(stats, k))
                )

        var pstats = doc.get(i, "partition-statistics")
        if pstats >= 0 and not doc.is_null(pstats):
            for k in range(doc.size(pstats)):
                var e = doc.at(pstats, k)
                m.partition_statistics.append(
                    PartitionStatisticsFile(
                        doc.req_int(e, "snapshot-id"),
                        doc.req_string(e, "statistics-path"),
                        doc.opt_int(e, "file-size-in-bytes", 0),
                    )
                )

        var nri = doc.get(i, "next-row-id")
        if nri >= 0 and not doc.is_null(nri):
            m.next_row_id = doc.as_int(nri)
            m.has_next_row_id = True

        var keys = doc.get(i, "encryption-keys")
        if keys >= 0 and not doc.is_null(keys):
            for k in range(doc.size(keys)):
                m.encryption_keys.append(
                    EncryptionKey.from_json(doc, doc.at(keys, k))
                )

        # Anything else is kept verbatim so a round trip does not lose it.
        for k in range(doc.size(i)):
            var key = doc.key_at(i, k)
            if not _is_known_key(key):
                m.extra_keys.append(key)
                m.extra_values.append(doc.dump(doc.at(i, k)))
        return m^

    # ── serialization ──────────────────────────────────────────────────────
    def to_json(self) -> String:
        var out = String('{"format-version":') + String(self.format_version)
        if self.table_uuid != "":
            out += ',"table-uuid":' + json_quote(self.table_uuid)
        if self.has_location:
            out += ',"location":' + json_quote(self.location)
        if self.format_version > 1:
            out += ',"last-sequence-number":' + String(
                self.last_sequence_number
            )
        out += ',"last-updated-ms":' + String(self.last_updated_ms)
        out += ',"last-column-id":' + String(self.last_column_id)
        out += ',"current-schema-id":' + String(self.current_schema_id)
        out += ',"schemas":['
        for k in range(len(self.schemas)):
            if k > 0:
                out += ","
            out += self.schemas[k].to_json()
        out += "]"
        out += ',"default-spec-id":' + String(self.default_spec_id)
        out += ',"partition-specs":['
        for k in range(len(self.partition_specs)):
            if k > 0:
                out += ","
            out += self.partition_specs[k].to_json()
        out += "]"
        out += ',"last-partition-id":' + String(self.last_partition_id)
        out += ',"default-sort-order-id":' + String(self.default_sort_order_id)
        out += ',"sort-orders":['
        for k in range(len(self.sort_orders)):
            if k > 0:
                out += ","
            out += self.sort_orders[k].to_json()
        out += "]"
        out += ',"properties":' + _string_map_json(self.properties)
        out += ',"current-snapshot-id":' + (
            String(
                self.current_snapshot_id
            ) if self.has_current_snapshot else "-1"
        )
        out += ',"refs":{'
        for k in range(len(self.refs)):
            if k > 0:
                out += ","
            write_json_string(self.refs[k].name, out)
            out += ":" + self.refs[k].to_json()
        out += "}"
        out += ',"snapshots":['
        for k in range(len(self.snapshots)):
            if k > 0:
                out += ","
            out += self.snapshots[k].to_json()
        out += "]"
        if len(self.statistics) > 0:
            out += ',"statistics":['
            for k in range(len(self.statistics)):
                if k > 0:
                    out += ","
                out += self.statistics[k].to_json()
            out += "]"
        if len(self.partition_statistics) > 0:
            out += ',"partition-statistics":['
            for k in range(len(self.partition_statistics)):
                if k > 0:
                    out += ","
                out += self.partition_statistics[k].to_json()
            out += "]"
        out += ',"snapshot-log":['
        for k in range(len(self.snapshot_log)):
            if k > 0:
                out += ","
            out += '{"timestamp-ms":' + String(
                self.snapshot_log[k].timestamp_ms
            )
            out += ',"snapshot-id":' + String(self.snapshot_log[k].snapshot_id)
            out += "}"
        out += "]"
        out += ',"metadata-log":['
        for k in range(len(self.metadata_log)):
            if k > 0:
                out += ","
            out += '{"timestamp-ms":' + String(
                self.metadata_log[k].timestamp_ms
            )
            out += ',"metadata-file":' + json_quote(
                self.metadata_log[k].metadata_file
            )
            out += "}"
        out += "]"
        if self.has_next_row_id:
            out += ',"next-row-id":' + String(self.next_row_id)
        if len(self.encryption_keys) > 0:
            out += ',"encryption-keys":['
            for k in range(len(self.encryption_keys)):
                if k > 0:
                    out += ","
                out += self.encryption_keys[k].to_json()
            out += "]"
        for k in range(len(self.extra_keys)):
            out += ","
            write_json_string(self.extra_keys[k], out)
            out += ":" + self.extra_values[k]
        out += "}"
        return out^


def _highest_partition_id(specs: List[PartitionSpec]) -> Int:
    var m = PARTITION_DATA_ID_START - 1
    for k in range(len(specs)):
        for j in range(len(specs[k].fields)):
            if specs[k].fields[j].field_id > m:
                m = specs[k].fields[j].field_id
    return m


comptime _KNOWN_KEYS = String(
    "|format-version|table-uuid|location|last-sequence-number|last-updated-ms"
    "|last-column-id|schema|schemas|current-schema-id|partition-spec"
    "|partition-specs|default-spec-id|last-partition-id|properties"
    "|current-snapshot-id|snapshots|snapshot-log|metadata-log|sort-orders"
    "|default-sort-order-id|refs|statistics|partition-statistics|next-row-id"
    "|encryption-keys|"
)


def _is_known_key(k: String) -> Bool:
    return _KNOWN_KEYS.find("|" + k + "|") >= 0


def _string_map_json(m: Dict[String, String]) -> String:
    """Serialize a string map with its keys sorted, so output is stable."""
    var keys = List[String]()
    for e in m.items():
        keys.append(e.key)
    _sort_strings(keys)
    var out = String("{")
    for k in range(len(keys)):
        if k > 0:
            out += ","
        write_json_string(keys[k], out)
        out += ":"
        try:
            write_json_string(m[keys[k]], out)
        except:
            out += '""'
    out += "}"
    return out^


def _string_array_json(l: List[String]) -> String:
    var out = String("[")
    for k in range(len(l)):
        if k > 0:
            out += ","
        write_json_string(l[k], out)
    out += "]"
    return out^


def _sort_strings(mut l: List[String]):
    for i in range(1, len(l)):
        var j = i
        while j > 0 and l[j] < l[j - 1]:
            l.swap_elements(j, j - 1)
            j -= 1
