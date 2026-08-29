"""Manifest lists and manifests, read through avro.mojo.

Two Avro files, both `manifest_file`/`manifest_entry` records:

* the **manifest list** (`snap-<id>-<n>-<uuid>.avro`) names every manifest in a
  snapshot, with the counts and per-partition-field summaries a scan uses to
  skip manifests without opening them;
* each **manifest** lists data or delete files with their partition tuple,
  per-column metrics, and the entry status that drives inheritance.

Three inheritance rules are implemented here, because getting them wrong
silently mis-plans a scan:

* **Sequence numbers.** A `null` `sequence_number` / `file_sequence_number` is
  inherited from the manifest's entry in the manifest list — but *only* for
  ADDED entries. EXISTING and DELETED entries must carry both explicitly, and a
  v1 manifest with no sequence column defaults every file to 0.
* **Snapshot id.** A `null` `snapshot_id` is inherited from the manifest's
  `added_snapshot_id`.
* **`first_row_id`** (v3). A `null` value is the manifest's `first_row_id` plus
  the record counts of every preceding data file in that manifest that also had
  a null `first_row_id`.

Fields this build does not recognise are ignored rather than fatal, and the
reserved id 141 and the v4-only `content_stats` (146) are simply not read.
"""

from std.collections import Dict

from avro import DataFileReader, Value

from .expressions import ColumnMetrics, FieldSummary
from .json import Json, parse_json, json_quote
from .schema import Schema
from .transforms import PartitionSpec
from .types import TypeStore, TK_PRIMITIVE, P_UNKNOWN
from .values import Datum, datum_from_bytes_prim


# ── manifest content ────────────────────────────────────────────────────────
comptime MANIFEST_CONTENT_DATA: Int = 0
comptime MANIFEST_CONTENT_DELETES: Int = 1

# ── data file content ───────────────────────────────────────────────────────
comptime CONTENT_DATA: Int = 0
comptime CONTENT_POSITION_DELETES: Int = 1
comptime CONTENT_EQUALITY_DELETES: Int = 2

# ── manifest entry status ───────────────────────────────────────────────────
comptime STATUS_EXISTING: Int = 0
comptime STATUS_ADDED: Int = 1
comptime STATUS_DELETED: Int = 2


@fieldwise_init
struct ManifestFile(Copyable, Movable, Writable):
    """One entry of a manifest list (`manifest_file`, field ids 500-520)."""

    var manifest_path: String
    var manifest_length: Int64
    var partition_spec_id: Int
    var content: Int
    """0 = data, 1 = deletes. Always 0 for a v1 manifest list."""
    var sequence_number: Int64
    var min_sequence_number: Int64
    var added_snapshot_id: Int64
    var added_files_count: Int
    var has_added_files_count: Bool
    var existing_files_count: Int
    var has_existing_files_count: Bool
    var deleted_files_count: Int
    var has_deleted_files_count: Bool
    var added_rows_count: Int64
    var has_added_rows_count: Bool
    var existing_rows_count: Int64
    var has_existing_rows_count: Bool
    var deleted_rows_count: Int64
    var has_deleted_rows_count: Bool
    var partitions: List[FieldSummary]
    var has_partitions: Bool
    var key_metadata: List[UInt8]
    var first_row_id: Int64
    var has_first_row_id: Bool

    def is_delete_manifest(self) -> Bool:
        return self.content == MANIFEST_CONTENT_DELETES

    def live_files(self) -> Int:
        """Entries that are not DELETED, as far as the counts can say."""
        var n = 0
        if self.has_added_files_count:
            n += self.added_files_count
        if self.has_existing_files_count:
            n += self.existing_files_count
        if not self.has_added_files_count and not self.has_existing_files_count:
            # "when null this is assumed to be non-zero"
            return 1
        return n

    def write_to(self, mut writer: Some[Writer]):
        writer.write(
            "ManifestFile(", self.manifest_path, ", spec=",
            self.partition_spec_id, ", seq=", self.sequence_number, ")",
        )


@fieldwise_init
struct DataFile(Copyable, Movable, Writable):
    """A `data_file` struct (field ids 100-145)."""

    var content: Int
    var file_path: String
    var file_format: String
    var partition: List[Datum]
    """Positional, matching the writing spec's field order."""
    var record_count: Int64
    var file_size_in_bytes: Int64
    var column_sizes: List[ColumnMetrics]
    """Reused as the carrier for all the per-column metric maps."""
    var metrics: List[ColumnMetrics]
    var key_metadata: List[UInt8]
    var has_key_metadata: Bool
    var split_offsets: List[Int64]
    var equality_ids: List[Int]
    var sort_order_id: Int
    var has_sort_order_id: Bool
    var first_row_id: Int64
    var has_first_row_id: Bool
    var referenced_data_file: String
    var has_referenced_data_file: Bool
    var content_offset: Int64
    var has_content_offset: Bool
    var content_size_in_bytes: Int64
    var has_content_size_in_bytes: Bool

    def is_data(self) -> Bool:
        return self.content == CONTENT_DATA

    def is_position_delete(self) -> Bool:
        return self.content == CONTENT_POSITION_DELETES

    def is_equality_delete(self) -> Bool:
        return self.content == CONTENT_EQUALITY_DELETES

    def is_deletion_vector(self) -> Bool:
        """A DV is a position-delete entry in a Puffin blob with a referent."""
        return (
            self.content == CONTENT_POSITION_DELETES
            and self.file_format.lower() == "puffin"
            and self.has_referenced_data_file
        )

    def write_to(self, mut writer: Some[Writer]):
        writer.write(
            "DataFile(", self.file_path, ", content=", self.content,
            ", rows=", self.record_count, ")",
        )


@fieldwise_init
struct ManifestEntry(Copyable, Movable):
    """A `manifest_entry`, with every inherited value already filled in."""

    var status: Int
    var snapshot_id: Int64
    var has_snapshot_id: Bool
    var sequence_number: Int64
    """The *data* sequence number, after inheritance."""
    var file_sequence_number: Int64
    var data_file: DataFile

    def is_live(self) -> Bool:
        return self.status != STATUS_DELETED


# ── Avro helpers ────────────────────────────────────────────────────────────
def _opt(v: Value) -> Value:
    """Unwrap a union; Iceberg's optional fields are `["null", T]`."""
    return v.unwrap()


def _has(rec: Value, name: String) -> Bool:
    if not rec.has(name):
        return False
    try:
        return not _opt(rec.field(name)).is_null()
    except:
        return False


def _long(rec: Value, name: String, dflt: Int64) raises -> Int64:
    if not _has(rec, name):
        return dflt
    return _opt(rec.field(name)).as_long()


def _int(rec: Value, name: String, dflt: Int) raises -> Int:
    return Int(_long(rec, name, Int64(dflt)))


def _str(rec: Value, name: String, dflt: String) raises -> String:
    if not _has(rec, name):
        return dflt
    return _opt(rec.field(name)).as_string()


def _bytes(rec: Value, name: String) raises -> List[UInt8]:
    if not _has(rec, name):
        return List[UInt8]()
    return _opt(rec.field(name)).as_bytes()


def _bool(rec: Value, name: String, dflt: Bool) raises -> Bool:
    if not _has(rec, name):
        return dflt
    return _opt(rec.field(name)).as_bool()


def _int_map(rec: Value, name: String) raises -> List[Int64]:
    """Read an Iceberg `map<int, long>`, which Avro encodes as an array of
    {key, value} records. Returns [k0, v0, k1, v1, …]."""
    var out = List[Int64]()
    if not _has(rec, name):
        return out^
    var arr = _opt(rec.field(name))
    for k in range(len(arr)):
        var e = arr.at(k)
        out.append(_opt(e.field("key")).as_long())
        out.append(_opt(e.field("value")).as_long())
    return out^


def _bytes_map(rec: Value, name: String) raises -> Tuple[List[Int], List[List[UInt8]]]:
    """Read an Iceberg `map<int, binary>` as parallel key/value lists."""
    var keys = List[Int]()
    var vals = List[List[UInt8]]()
    if not _has(rec, name):
        return (keys^, vals^)
    var arr = _opt(rec.field(name))
    for k in range(len(arr)):
        var e = arr.at(k)
        keys.append(Int(_opt(e.field("key")).as_long()))
        vals.append(_opt(e.field("value")).as_bytes())
    return (keys^, vals^)


def _metric_index(mut m: List[ColumnMetrics], field_id: Int) -> Int:
    for k in range(len(m)):
        if m[k].field_id == field_id:
            return k
    m.append(ColumnMetrics.blank(field_id))
    return len(m) - 1


# ── manifest lists ──────────────────────────────────────────────────────────
def read_manifest_list(path: String) raises -> List[ManifestFile]:
    """Read a snapshot's manifest list."""
    var r = DataFileReader.open(path)
    var out = List[ManifestFile]()
    while r.has_next():
        var rec = r.next()
        out.append(_manifest_file_from(rec))
    return out^


def _manifest_file_from(rec: Value) raises -> ManifestFile:
    var summaries = List[FieldSummary]()
    var has_parts = _has(rec, "partitions")
    if has_parts:
        var arr = _opt(rec.field("partitions"))
        for k in range(len(arr)):
            var p = arr.at(k)
            var lo = _bytes(p, "lower_bound")
            var hi = _bytes(p, "upper_bound")
            summaries.append(
                FieldSummary(
                    _bool(p, "contains_null", True),
                    _bool(p, "contains_nan", False),
                    _has(p, "contains_nan"),
                    lo^,
                    _has(p, "lower_bound"),
                    hi^,
                    _has(p, "upper_bound"),
                )
            )
    var m = ManifestFile(
        _str(rec, "manifest_path", ""),
        _long(rec, "manifest_length", 0),
        _int(rec, "partition_spec_id", 0),
        _int(rec, "content", MANIFEST_CONTENT_DATA),
        # v1 manifest lists have no sequence columns; the spec says read 0.
        _long(rec, "sequence_number", 0),
        _long(rec, "min_sequence_number", 0),
        _long(rec, "added_snapshot_id", 0),
        _int(rec, "added_files_count", 0),
        _has(rec, "added_files_count"),
        _int(rec, "existing_files_count", 0),
        _has(rec, "existing_files_count"),
        _int(rec, "deleted_files_count", 0),
        _has(rec, "deleted_files_count"),
        _long(rec, "added_rows_count", 0),
        _has(rec, "added_rows_count"),
        _long(rec, "existing_rows_count", 0),
        _has(rec, "existing_rows_count"),
        _long(rec, "deleted_rows_count", 0),
        _has(rec, "deleted_rows_count"),
        summaries^,
        has_parts,
        _bytes(rec, "key_metadata"),
        _long(rec, "first_row_id", 0),
        _has(rec, "first_row_id"),
    )
    return m^


# ── manifests ───────────────────────────────────────────────────────────────
struct Manifest(Copyable, Movable):
    """A decoded manifest file: its metadata plus its entries."""

    var entries: List[ManifestEntry]
    var partition_spec: PartitionSpec
    var partition_spec_id: Int
    var schema: Schema
    var format_version: Int
    var content: Int

    def __init__(
        out self,
        var entries: List[ManifestEntry],
        var spec: PartitionSpec,
        spec_id: Int,
        var schema: Schema,
        format_version: Int,
        content: Int,
    ):
        self.entries = entries^
        self.partition_spec = spec^
        self.partition_spec_id = spec_id
        self.schema = schema^
        self.format_version = format_version
        self.content = content

    def live_entries(self) -> List[ManifestEntry]:
        var out = List[ManifestEntry]()
        for k in range(len(self.entries)):
            if self.entries[k].is_live():
                out.append(self.entries[k].copy())
        return out^


def read_manifest(mf: ManifestFile) raises -> Manifest:
    """Read a manifest, inheriting everything the manifest list supplies."""
    return read_manifest_at(mf.manifest_path, mf)


def read_manifest_at(path: String, mf: ManifestFile) raises -> Manifest:
    var r = DataFileReader.open(path)

    # The manifest's own Avro file metadata carries the schema and spec that
    # its `partition` struct was typed with — not necessarily the table's
    # current ones, which is exactly why they are stored here.
    var format_version = 1
    if "format-version" in r.metadata:
        format_version = Int(
            String(from_utf8_lossy=Span(r.metadata["format-version"])).strip()
        )
    var content = MANIFEST_CONTENT_DATA
    if "content" in r.metadata:
        var c = String(from_utf8_lossy=Span(r.metadata["content"])).strip()
        if c == "deletes" or c == "1":
            content = MANIFEST_CONTENT_DELETES

    var spec_id = mf.partition_spec_id
    if "partition-spec-id" in r.metadata:
        spec_id = Int(
            String(
                from_utf8_lossy=Span(r.metadata["partition-spec-id"])
            ).strip()
        )

    var empty_store = TypeStore()
    var empty_root = empty_store.struct_([])
    var schema = Schema(empty_store^, empty_root, 0)
    if "schema" in r.metadata:
        schema = Schema.parse(
            String(from_utf8_lossy=Span(r.metadata["schema"]))
        )

    var spec = PartitionSpec.unpartitioned(spec_id)
    if "partition-spec" in r.metadata:
        var text = String(from_utf8_lossy=Span(r.metadata["partition-spec"]))
        var doc = parse_json(text)
        if doc.kind(doc.root) == 5:  # a bare array of fields
            spec = PartitionSpec.from_fields_json(doc, doc.root, spec_id)
        else:
            spec = PartitionSpec.from_json(doc, doc.root, spec_id)

    var part_type = spec.partition_type(schema)

    var entries = List[ManifestEntry]()
    # first_row_id inheritance accumulates over the whole manifest.
    var next_row_id = mf.first_row_id
    while r.has_next():
        var rec = r.next()
        var status = _int(rec, "status", STATUS_ADDED)
        var df_val = _opt(rec.field("data_file"))
        var df = _data_file_from(df_val, spec, part_type)

        # ── snapshot id inheritance ────────────────────────────────────────
        var snapshot_id = mf.added_snapshot_id
        var has_snapshot_id = True
        if _has(rec, "snapshot_id"):
            snapshot_id = _long(rec, "snapshot_id", mf.added_snapshot_id)

        # ── sequence number inheritance ────────────────────────────────────
        # Only ADDED entries inherit. EXISTING and DELETED entries are required
        # to carry both numbers explicitly; a v1 manifest has neither column
        # and every file defaults to 0.
        var seq: Int64 = 0
        var file_seq: Int64 = 0
        if format_version > 1:
            if _has(rec, "sequence_number"):
                seq = _long(rec, "sequence_number", 0)
            elif status == STATUS_ADDED:
                seq = mf.sequence_number
            if _has(rec, "file_sequence_number"):
                file_seq = _long(rec, "file_sequence_number", 0)
            elif status == STATUS_ADDED:
                file_seq = mf.sequence_number

        # ── first_row_id inheritance ───────────────────────────────────────
        if df.is_data():
            if not df.has_first_row_id:
                if mf.has_first_row_id:
                    df.first_row_id = next_row_id
                    df.has_first_row_id = True
                    next_row_id += df.record_count
            else:
                # An explicit id does not advance the running counter.
                pass
        entries.append(
            ManifestEntry(
                status, snapshot_id, has_snapshot_id, seq, file_seq, df^
            )
        )
    return Manifest(entries^, spec^, spec_id, schema^, format_version, content)


def _data_file_from(
    rec: Value, spec: PartitionSpec, part_type: Schema
) raises -> DataFile:
    var metrics = List[ColumnMetrics]()

    var vc = _int_map(rec, "value_counts")
    for k in range(0, len(vc), 2):
        var i = _metric_index(metrics, Int(vc[k]))
        metrics[i].value_count = vc[k + 1]
        metrics[i].has_value_count = True
    var nvc = _int_map(rec, "null_value_counts")
    for k in range(0, len(nvc), 2):
        var i = _metric_index(metrics, Int(nvc[k]))
        metrics[i].null_value_count = nvc[k + 1]
        metrics[i].has_null_value_count = True
    var nanc = _int_map(rec, "nan_value_counts")
    for k in range(0, len(nanc), 2):
        var i = _metric_index(metrics, Int(nanc[k]))
        metrics[i].nan_value_count = nanc[k + 1]
        metrics[i].has_nan_value_count = True
    var lb = _bytes_map(rec, "lower_bounds")
    for k in range(len(lb[0])):
        var i = _metric_index(metrics, lb[0][k])
        metrics[i].lower_bound = lb[1][k].copy()
        metrics[i].has_lower = True
    var ub = _bytes_map(rec, "upper_bounds")
    for k in range(len(ub[0])):
        var i = _metric_index(metrics, ub[0][k])
        metrics[i].upper_bound = ub[1][k].copy()
        metrics[i].has_upper = True

    var sizes = List[ColumnMetrics]()
    var cs = _int_map(rec, "column_sizes")
    for k in range(0, len(cs), 2):
        var c = ColumnMetrics.blank(Int(cs[k]))
        c.value_count = cs[k + 1]
        c.has_value_count = True
        sizes.append(c^)

    var offsets = List[Int64]()
    if _has(rec, "split_offsets"):
        var arr = _opt(rec.field("split_offsets"))
        for k in range(len(arr)):
            offsets.append(arr.at(k).as_long())

    var eq_ids = List[Int]()
    if _has(rec, "equality_ids"):
        var arr = _opt(rec.field("equality_ids"))
        for k in range(len(arr)):
            eq_ids.append(Int(arr.at(k).as_long()))

    var partition = _partition_from(rec, spec, part_type)

    var df = DataFile(
        _int(rec, "content", CONTENT_DATA),
        _str(rec, "file_path", ""),
        _str(rec, "file_format", ""),
        partition^,
        _long(rec, "record_count", 0),
        _long(rec, "file_size_in_bytes", 0),
        sizes^,
        metrics^,
        _bytes(rec, "key_metadata"),
        _has(rec, "key_metadata"),
        offsets^,
        eq_ids^,
        _int(rec, "sort_order_id", 0),
        _has(rec, "sort_order_id"),
        _long(rec, "first_row_id", 0),
        _has(rec, "first_row_id"),
        _str(rec, "referenced_data_file", ""),
        _has(rec, "referenced_data_file"),
        _long(rec, "content_offset", 0),
        _has(rec, "content_offset"),
        _long(rec, "content_size_in_bytes", 0),
        _has(rec, "content_size_in_bytes"),
    )
    return df^


def _partition_from(
    rec: Value, spec: PartitionSpec, part_type: Schema
) raises -> List[Datum]:
    """Decode the `partition` struct, typed by the manifest's own spec."""
    var out = List[Datum]()
    if not rec.has("partition"):
        for _k in range(len(spec.fields)):
            out.append(Datum.none())
        return out^
    var p = _opt(rec.field("partition"))
    for k in range(len(spec.fields)):
        ref pf = spec.fields[k]
        if not p.has(pf.name):
            out.append(Datum.none())
            continue
        var v = _opt(p.field(pf.name))
        if v.is_null():
            out.append(Datum.none())
            continue
        var prim = P_UNKNOWN
        var precision = 0
        var scale = 0
        var length = 0
        if part_type.has_field(pf.field_id):
            var af = part_type.find_field(pf.field_id)
            ref tn = part_type.store.nodes[af.type]
            if tn.kind == TK_PRIMITIVE:
                prim = tn.prim
                precision = tn.precision
                scale = tn.scale
                length = tn.length
        out.append(_datum_from_avro(v, prim, precision, scale, length))
    return out^


def _datum_from_avro(
    v: Value, prim: UInt8, precision: Int, scale: Int, length: Int
) raises -> Datum:
    """Convert a decoded Avro value to a typed Iceberg `Datum`.

    Avro carries decimals, uuids and fixed values as bytes in exactly the
    Appendix-D layout, so those route through the binary decoder; the rest map
    onto Avro's own primitives.
    """
    var kind = v.kind()
    if kind == 1:  # boolean
        return Datum.bool_(v.as_bool())
    if kind == 2 or kind == 3:  # int, long
        return Datum.integral(prim, v.as_long())
    if kind == 4:  # float
        return Datum.float_(Float64(v.as_float()))
    if kind == 5:  # double
        return Datum.double_(v.as_double())
    if kind == 7:  # string
        return Datum.string_(v.as_string())
    # bytes / fixed
    return datum_from_bytes_prim(prim, precision, scale, length, v.as_bytes())
