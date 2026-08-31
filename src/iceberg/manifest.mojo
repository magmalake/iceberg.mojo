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

Both files are read with avro.mojo's `RecordCursor` rather than its `Value`
API. Manifests sit on the scan-planning critical path and a `Value` costs
roughly sixty heap allocations per entry, plus a string comparison per field
lookup. The cursor compiles the file's own schema into a decode plan once,
and every field this module wants is then a slot number resolved once per
file (`_ListSlots` / `_EntrySlots`) — the per-entry work is integer indexing
and, for a path or a bound, a span into the block buffer. A field the
manifest's schema does not have resolves to slot -1, which is how the
version differences below stay expressible.

**Once per file is still once too many.** A snapshot's five hundred
manifests were written by one writer and carry five hundred byte-identical
copies of the same `avro.schema`, the same Iceberg `schema` and the same
`partition-spec`. `ManifestCache` keys everything those three determine —
the compiled decode plan, the parsed schema and spec, the partition type and
the resolved slot table — on the raw metadata bytes, before any of it is
parsed. Byte equality is the cheap sufficient condition, and the hash over
those bytes is only a filter: a hit is confirmed by comparing them. A scan
owns one cache and drops it when it is done; there is no global state, and a
plan handed out stays alive on its own because a cursor holds its own
reference to it.
"""

from std.collections import Dict

from avro import PlanCache, RecordCursor
from avro.cursor import DecodePlan, schema_hash
from avro.datafile import DataFileReader
from avro.schema import BOOLEAN, BYTES, DOUBLE, FIXED, FLOAT, INT, LONG, STRING

from .expressions import ColumnMetrics, FieldSummary
from .io import FileIO
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
            "ManifestFile(",
            self.manifest_path,
            ", spec=",
            self.partition_spec_id,
            ", seq=",
            self.sequence_number,
            ")",
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
            "DataFile(",
            self.file_path,
            ", content=",
            self.content,
            ", rows=",
            self.record_count,
            ")",
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


# ── reading through the cursor ──────────────────────────────────────────────
#
# Every accessor here takes a slot number, not a name. -1 means "this
# manifest's schema has no such field", which is the same thing the old
# `rec.has(name)` answered and is resolved once per file rather than per
# entry.


@always_inline
def _present(c: RecordCursor, slot: Int) -> Bool:
    """The field exists in this file and this entry did not leave it null."""
    return slot >= 0 and not c.is_null(slot)


@always_inline
def _long(c: RecordCursor, slot: Int, dflt: Int64) -> Int64:
    if not _present(c, slot):
        return dflt
    return c.get_long(slot)


@always_inline
def _int(c: RecordCursor, slot: Int, dflt: Int) -> Int:
    return Int(_long(c, slot, Int64(dflt)))


def _str(c: RecordCursor, slot: Int, dflt: String) -> String:
    if not _present(c, slot):
        return dflt
    return c.get_string(slot)


def _bytes(c: RecordCursor, slot: Int) -> List[UInt8]:
    if not _present(c, slot):
        return List[UInt8]()
    return c.get_bytes_copy(slot)


@always_inline
def _bool(c: RecordCursor, slot: Int, dflt: Bool) -> Bool:
    if not _present(c, slot):
        return dflt
    return c.get_bool(slot)


def _metric_index(mut m: List[ColumnMetrics], field_id: Int) -> Int:
    for k in range(len(m)):
        if m[k].field_id == field_id:
            return k
    m.append(ColumnMetrics.blank(field_id))
    return len(m) - 1


@fieldwise_init
struct _MapSlots(Copyable, Movable):
    """An Iceberg `map<int, V>`, which Avro writes as an array of {key,
    value} records — the array's count slot and the two element slots."""

    var count: Int
    var key: Int
    var value: Int

    @staticmethod
    def of(plan: DecodePlan, path: String) raises -> _MapSlots:
        return _MapSlots(
            plan.try_slot(path),
            plan.try_slot(String(path, ".element.key")),
            plan.try_slot(String(path, ".element.value")),
        )

    @always_inline
    def n(self, c: RecordCursor) -> Int:
        if self.count < 0:
            return 0
        return c.array_len(self.count)


# ── manifest lists ──────────────────────────────────────────────────────────


struct _ListSlots(Copyable, Movable):
    """Every `manifest_file` field, resolved once for the whole file."""

    var manifest_path: Int
    var manifest_length: Int
    var partition_spec_id: Int
    var content: Int
    var sequence_number: Int
    var min_sequence_number: Int
    var added_snapshot_id: Int
    var added_files_count: Int
    var existing_files_count: Int
    var deleted_files_count: Int
    var added_rows_count: Int
    var existing_rows_count: Int
    var deleted_rows_count: Int
    var partitions: Int
    var contains_null: Int
    var contains_nan: Int
    var lower_bound: Int
    var upper_bound: Int
    var key_metadata: Int
    var first_row_id: Int

    def __init__(out self, plan: DecodePlan) raises:
        self.manifest_path = plan.try_slot("manifest_path")
        self.manifest_length = plan.try_slot("manifest_length")
        self.partition_spec_id = plan.try_slot("partition_spec_id")
        self.content = plan.try_slot("content")
        self.sequence_number = plan.try_slot("sequence_number")
        self.min_sequence_number = plan.try_slot("min_sequence_number")
        self.added_snapshot_id = plan.try_slot("added_snapshot_id")
        self.added_files_count = plan.try_slot("added_files_count")
        self.existing_files_count = plan.try_slot("existing_files_count")
        self.deleted_files_count = plan.try_slot("deleted_files_count")
        self.added_rows_count = plan.try_slot("added_rows_count")
        self.existing_rows_count = plan.try_slot("existing_rows_count")
        self.deleted_rows_count = plan.try_slot("deleted_rows_count")
        self.partitions = plan.try_slot("partitions")
        self.contains_null = plan.try_slot("partitions.element.contains_null")
        self.contains_nan = plan.try_slot("partitions.element.contains_nan")
        self.lower_bound = plan.try_slot("partitions.element.lower_bound")
        self.upper_bound = plan.try_slot("partitions.element.upper_bound")
        self.key_metadata = plan.try_slot("key_metadata")
        self.first_row_id = plan.try_slot("first_row_id")


def read_manifest_list(path: String) raises -> List[ManifestFile]:
    """Read a snapshot's manifest list from a local path."""
    return read_manifest_list_bytes(read_local(path))


def read_manifest_list_io(
    io: FileIO, location: String
) raises -> List[ManifestFile]:
    """Read a snapshot's manifest list through a `FileIO`.

    A manifest list is small and read whole; there is nothing to seek to.
    """
    return read_manifest_list_bytes(io.read_all(location))


def read_local(path: String) raises -> List[UInt8]:
    with open(path, "r") as f:
        return f.read_bytes()


def read_manifest_list_io_cached(
    io: FileIO, location: String, mut cache: ManifestCache
) raises -> List[ManifestFile]:
    """Read a manifest list, reusing whatever `cache` already parsed.

    One scan reads one list, so this pays for itself only across snapshots —
    time travel, `expire_snapshots`, a diff between two refs.
    """
    return read_manifest_list_bytes_cached(io.read_all(location), cache)


def read_manifest_list_bytes(
    var data: List[UInt8],
) raises -> List[ManifestFile]:
    var cache = ManifestCache.disabled()
    return read_manifest_list_bytes_cached(data^, cache)


def read_manifest_list_bytes_cached(
    var data: List[UInt8], mut cache: ManifestCache
) raises -> List[ManifestFile]:
    var c = RecordCursor.of_bytes_cached(data^, cache.plans)
    var si = c.reader.metadata_index("avro.schema")
    var key = List[UInt8]()
    _append_keyed(key, Span(c.reader.metadata_vals[si]))
    var at = cache.find_list(Span(key))
    if at < 0:
        at = cache.add_list(key^, _ListSlots(c.plan))
    ref s = cache.lists[at]
    var out = List[ManifestFile]()
    while c.next():
        out.append(_manifest_file_from(c, s))
    return out^


def _manifest_file_from(c: RecordCursor, s: _ListSlots) raises -> ManifestFile:
    var summaries = List[FieldSummary]()
    var has_parts = _present(c, s.partitions)
    if has_parts:
        for k in range(c.array_len(s.partitions)):
            var lo = List[UInt8]()
            var has_lo = _present_at(c, s.lower_bound, k)
            if has_lo:
                lo = c.get_bytes_copy(s.lower_bound, k)
            var hi = List[UInt8]()
            var has_hi = _present_at(c, s.upper_bound, k)
            if has_hi:
                hi = c.get_bytes_copy(s.upper_bound, k)
            var has_nan = _present_at(c, s.contains_nan, k)
            summaries.append(
                FieldSummary(
                    c.get_bool(s.contains_null, k) if _present_at(
                        c, s.contains_null, k
                    ) else True,
                    c.get_bool(s.contains_nan, k) if has_nan else False,
                    has_nan,
                    lo^,
                    has_lo,
                    hi^,
                    has_hi,
                )
            )
    var m = ManifestFile(
        _str(c, s.manifest_path, ""),
        _long(c, s.manifest_length, 0),
        _int(c, s.partition_spec_id, 0),
        _int(c, s.content, MANIFEST_CONTENT_DATA),
        # v1 manifest lists have no sequence columns; the spec says read 0.
        _long(c, s.sequence_number, 0),
        _long(c, s.min_sequence_number, 0),
        _long(c, s.added_snapshot_id, 0),
        _int(c, s.added_files_count, 0),
        _present(c, s.added_files_count),
        _int(c, s.existing_files_count, 0),
        _present(c, s.existing_files_count),
        _int(c, s.deleted_files_count, 0),
        _present(c, s.deleted_files_count),
        _long(c, s.added_rows_count, 0),
        _present(c, s.added_rows_count),
        _long(c, s.existing_rows_count, 0),
        _present(c, s.existing_rows_count),
        _long(c, s.deleted_rows_count, 0),
        _present(c, s.deleted_rows_count),
        summaries^,
        has_parts,
        _bytes(c, s.key_metadata),
        _long(c, s.first_row_id, 0),
        _present(c, s.first_row_id),
    )
    return m^


@always_inline
def _present_at(c: RecordCursor, slot: Int, k: Int) -> Bool:
    return slot >= 0 and k < c.count(slot) and not c.is_null(slot, k)


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


struct _EntrySlots(Copyable, Movable):
    """Every `manifest_entry` and `data_file` field, plus the partition
    columns the manifest's own spec names, resolved once for the file."""

    var status: Int
    var snapshot_id: Int
    var sequence_number: Int
    var file_sequence_number: Int
    var content: Int
    var file_path: Int
    var file_format: Int
    var record_count: Int
    var file_size_in_bytes: Int
    var column_sizes: _MapSlots
    var value_counts: _MapSlots
    var null_value_counts: _MapSlots
    var nan_value_counts: _MapSlots
    var lower_bounds: _MapSlots
    var upper_bounds: _MapSlots
    var key_metadata: Int
    var split_offsets: Int
    var split_offset: Int
    var equality_ids: Int
    var equality_id: Int
    var sort_order_id: Int
    var first_row_id: Int
    var referenced_data_file: Int
    var content_offset: Int
    var content_size_in_bytes: Int
    var partition: List[Int]
    """One slot per partition field of the manifest's own spec, in order."""

    def __init__(out self, plan: DecodePlan, spec: PartitionSpec) raises:
        self.status = plan.try_slot("status")
        self.snapshot_id = plan.try_slot("snapshot_id")
        self.sequence_number = plan.try_slot("sequence_number")
        self.file_sequence_number = plan.try_slot("file_sequence_number")
        self.content = plan.try_slot("data_file.content")
        self.file_path = plan.try_slot("data_file.file_path")
        self.file_format = plan.try_slot("data_file.file_format")
        self.record_count = plan.try_slot("data_file.record_count")
        self.file_size_in_bytes = plan.try_slot("data_file.file_size_in_bytes")
        self.column_sizes = _MapSlots.of(plan, String("data_file.column_sizes"))
        self.value_counts = _MapSlots.of(plan, String("data_file.value_counts"))
        self.null_value_counts = _MapSlots.of(
            plan, String("data_file.null_value_counts")
        )
        self.nan_value_counts = _MapSlots.of(
            plan, String("data_file.nan_value_counts")
        )
        self.lower_bounds = _MapSlots.of(plan, String("data_file.lower_bounds"))
        self.upper_bounds = _MapSlots.of(plan, String("data_file.upper_bounds"))
        self.key_metadata = plan.try_slot("data_file.key_metadata")
        self.split_offsets = plan.try_slot("data_file.split_offsets")
        self.split_offset = plan.try_slot("data_file.split_offsets.element")
        self.equality_ids = plan.try_slot("data_file.equality_ids")
        self.equality_id = plan.try_slot("data_file.equality_ids.element")
        self.sort_order_id = plan.try_slot("data_file.sort_order_id")
        self.first_row_id = plan.try_slot("data_file.first_row_id")
        self.referenced_data_file = plan.try_slot(
            "data_file.referenced_data_file"
        )
        self.content_offset = plan.try_slot("data_file.content_offset")
        self.content_size_in_bytes = plan.try_slot(
            "data_file.content_size_in_bytes"
        )
        self.partition = List[Int](capacity=len(spec.fields))
        for k in range(len(spec.fields)):
            self.partition.append(
                plan.try_slot(
                    String("data_file.partition.", spec.fields[k].name)
                )
            )


# ── the per-scan cache ──────────────────────────────────────────────────────


struct _ManifestShape(Copyable, Movable):
    """Everything a manifest's file metadata determines, parsed once.

    Not the entries — those differ per file. This is the frame they are read
    in: the manifest's own Iceberg schema and partition spec, the partition
    type those two imply, and the slot numbers the decode plan gave every
    field.
    """

    var schema: Schema
    var spec: PartitionSpec
    var part_type: Schema
    var slots: _EntrySlots
    var spec_id: Int
    var format_version: Int
    var content: Int

    def __init__(
        out self,
        var schema: Schema,
        var spec: PartitionSpec,
        var part_type: Schema,
        var slots: _EntrySlots,
        spec_id: Int,
        format_version: Int,
        content: Int,
    ):
        self.schema = schema^
        self.spec = spec^
        self.part_type = part_type^
        self.slots = slots^
        self.spec_id = spec_id
        self.format_version = format_version
        self.content = content

    def __init__(out self, *, copy: Self):
        self.schema = copy.schema.copy()
        self.spec = copy.spec.copy()
        self.part_type = copy.part_type.copy()
        self.slots = copy.slots.copy()
        self.spec_id = copy.spec_id
        self.format_version = copy.format_version
        self.content = copy.content

    def __init__(out self, *, deinit move: Self):
        self.schema = move.schema^
        self.spec = move.spec^
        self.part_type = move.part_type^
        self.slots = move.slots^
        self.spec_id = move.spec_id
        self.format_version = move.format_version
        self.content = move.content


struct ManifestCache(Movable):
    """One scan's worth of parsed manifest metadata.

    Three caches that share a lifetime, all keyed on raw bytes:

    * `plans` — avro.mojo's own `PlanCache`, keyed on the file's
      `avro.schema`, holding the compiled decode plan;
    * `shapes` — keyed on every metadata entry a `_ManifestShape` is derived
      from, so two manifests written under different specs cannot share one;
    * `lists` — the manifest list's `_ListSlots`, keyed on its `avro.schema`.

    `disabled()` turns all three off without changing the code path, which is
    how the tests check that the cache is transparent.
    """

    var plans: PlanCache
    var shape_hashes: List[UInt64]
    var shape_keys: List[List[UInt8]]
    var shapes: List[_ManifestShape]
    var list_hashes: List[UInt64]
    var list_keys: List[List[UInt8]]
    var lists: List[_ListSlots]
    var enabled: Bool
    var collide: Bool
    """A test seam: force every key to hash to zero, so the byte compare is
    the only thing telling two manifest shapes apart."""
    var hits: Int
    var misses: Int

    def __init__(out self):
        self.plans = PlanCache()
        self.shape_hashes = List[UInt64]()
        self.shape_keys = List[List[UInt8]]()
        self.shapes = List[_ManifestShape]()
        self.list_hashes = List[UInt64]()
        self.list_keys = List[List[UInt8]]()
        self.lists = List[_ListSlots]()
        self.enabled = True
        self.collide = False
        self.hits = 0
        self.misses = 0

    def __init__(out self, *, deinit move: Self):
        self.plans = move.plans^
        self.shape_hashes = move.shape_hashes^
        self.shape_keys = move.shape_keys^
        self.shapes = move.shapes^
        self.list_hashes = move.list_hashes^
        self.list_keys = move.list_keys^
        self.lists = move.lists^
        self.enabled = move.enabled
        self.collide = move.collide
        self.hits = move.hits
        self.misses = move.misses

    @staticmethod
    def disabled() -> Self:
        """Every lookup a miss, nothing stored — the escape hatch."""
        var c = Self()
        c.enabled = False
        c.plans = PlanCache.disabled()
        return c^

    def collide_all_hashes(mut self):
        """Force every key — plans, shapes and lists — to hash to zero. A
        test seam: it leaves the byte compare as the only thing telling two
        different schemas apart."""
        self.plans.collide_hashes = True
        self.collide = True

    def hash_of(self, key: Span[UInt8, _]) -> UInt64:
        return 0 if self.collide else schema_hash(key)

    def find_shape(self, key: Span[UInt8, _]) -> Int:
        if not self.enabled:
            return -1
        var h = self.hash_of(key)
        for k in range(len(self.shape_hashes)):
            if self.shape_hashes[k] != h:
                continue
            if _same_bytes(Span(self.shape_keys[k]), key):
                return k
        return -1

    def add_shape(
        mut self, var key: List[UInt8], var shape: _ManifestShape
    ) -> Int:
        """Store `shape` and return where it landed. A disabled cache keeps
        only the newest one — the caller still reads it back by index, but
        `find_shape` will never hand it out again."""
        if not self.enabled:
            self.shapes.clear()
            self.shapes.append(shape^)
            return 0
        self.shape_hashes.append(self.hash_of(Span(key)))
        self.shape_keys.append(key^)
        self.shapes.append(shape^)
        return len(self.shapes) - 1

    def find_list(self, key: Span[UInt8, _]) -> Int:
        if not self.enabled:
            return -1
        var h = self.hash_of(key)
        for k in range(len(self.list_hashes)):
            if self.list_hashes[k] != h:
                continue
            if _same_bytes(Span(self.list_keys[k]), key):
                return k
        return -1

    def add_list(mut self, var key: List[UInt8], var slots: _ListSlots) -> Int:
        if not self.enabled:
            self.lists.clear()
            self.lists.append(slots^)
            return 0
        self.list_hashes.append(self.hash_of(Span(key)))
        self.list_keys.append(key^)
        self.lists.append(slots^)
        return len(self.lists) - 1


def _same_bytes(a: Span[UInt8, _], b: Span[UInt8, _]) -> Bool:
    if len(a) != len(b):
        return False
    for k in range(len(a)):
        if a[k] != b[k]:
            return False
    return True


comptime _SHAPE_KEYS = String(
    "schema,partition-spec,partition-spec-id,format-version,content"
)


def _append_len(mut key: List[UInt8], n: Int):
    """Length-prefix a component so two of them cannot run together."""
    for k in range(4):
        key.append(UInt8((n >> (8 * k)) & 0xFF))
    key.append(UInt8(1) if n < 0 else UInt8(0))


def _append_keyed(mut key: List[UInt8], part: Span[UInt8, _]):
    _append_len(key, len(part))
    key.extend(part)


def _shape_key(c: RecordCursor, spec_id_fallback: Int) raises -> List[UInt8]:
    """Every metadata entry a `_ManifestShape` is derived from, verbatim.

    The `avro.schema` is in the key only by proxy. `PlanCache` has already
    compared those bytes for us and `plan_id` names the entry it matched, so
    two manifests with the same non-negative `plan_id` provably carry the
    same schema — and the key stays a few hundred bytes rather than the five
    kilobytes an Iceberg manifest schema runs to. A cursor that compiled its
    own plan has `plan_id == -1` and pays the full compare.

    The fallback spec id goes in too: a manifest with no `partition-spec-id`
    entry takes it from the manifest list, so two otherwise identical files
    reached through different list entries are genuinely different shapes.
    """
    ref r = c.reader
    var key = List[UInt8]()
    _append_len(key, c.plan_id)
    if c.plan_id < 0:
        var si = r.metadata_index("avro.schema")
        _append_keyed(key, Span(r.metadata_vals[si]))
    var names = _SHAPE_KEYS.split(",")
    for k in range(len(names)):
        var i = r.metadata_index(names[k])
        if i < 0:
            _append_len(key, 0)
        else:
            _append_keyed(key, Span(r.metadata_vals[i]))
    _append_keyed(key, String(spec_id_fallback).as_bytes())
    return key^


def read_manifest(mf: ManifestFile) raises -> Manifest:
    """Read a manifest, inheriting everything the manifest list supplies."""
    return read_manifest_at(mf.manifest_path, mf)


def read_manifest_at(path: String, mf: ManifestFile) raises -> Manifest:
    """Read a manifest from a local path."""
    return read_manifest_bytes(read_local(path), mf)


def read_manifest_io(
    io: FileIO, location: String, mf: ManifestFile
) raises -> Manifest:
    """Read a manifest through a `FileIO`."""
    return read_manifest_bytes(io.read_all(location), mf)


def read_manifest_io_cached(
    io: FileIO, location: String, mf: ManifestFile, mut cache: ManifestCache
) raises -> Manifest:
    """Read a manifest, reusing whatever `cache` already parsed."""
    return read_manifest_bytes_cached(io.read_all(location), mf, cache)


def read_manifest_bytes(
    var data: List[UInt8], mf: ManifestFile
) raises -> Manifest:
    """Read a manifest with nothing cached — every schema parsed afresh."""
    var cache = ManifestCache.disabled()
    return read_manifest_bytes_cached(data^, mf, cache)


def _manifest_shape(c: RecordCursor, mf: ManifestFile) raises -> _ManifestShape:
    """Parse everything the manifest's file metadata determines.

    The manifest's own Avro file metadata carries the schema and spec that
    its `partition` struct was typed with — not necessarily the table's
    current ones, which is exactly why they are stored there.
    """
    var format_version = 1
    var i = c.reader.metadata_index("format-version")
    if i >= 0:
        format_version = Int(
            String(from_utf8_lossy=Span(c.reader.metadata_vals[i])).strip()
        )
    var content = MANIFEST_CONTENT_DATA
    i = c.reader.metadata_index("content")
    if i >= 0:
        var t = String(from_utf8_lossy=Span(c.reader.metadata_vals[i])).strip()
        if t == "deletes" or t == "1":
            content = MANIFEST_CONTENT_DELETES

    var spec_id = mf.partition_spec_id
    i = c.reader.metadata_index("partition-spec-id")
    if i >= 0:
        spec_id = Int(
            String(from_utf8_lossy=Span(c.reader.metadata_vals[i])).strip()
        )

    var empty_store = TypeStore()
    var empty_root = empty_store.struct_([])
    var schema = Schema(empty_store^, empty_root, 0)
    i = c.reader.metadata_index("schema")
    if i >= 0:
        schema = Schema.parse(
            String(from_utf8_lossy=Span(c.reader.metadata_vals[i]))
        )

    var spec = PartitionSpec.unpartitioned(spec_id)
    i = c.reader.metadata_index("partition-spec")
    if i >= 0:
        var text = String(from_utf8_lossy=Span(c.reader.metadata_vals[i]))
        var doc = parse_json(text)
        if doc.kind(doc.root) == 5:  # a bare array of fields
            spec = PartitionSpec.from_fields_json(doc, doc.root, spec_id)
        else:
            spec = PartitionSpec.from_json(doc, doc.root, spec_id)

    var part_type = spec.partition_type(schema)
    var slots = _EntrySlots(c.plan, spec)
    return _ManifestShape(
        schema^, spec^, part_type^, slots^, spec_id, format_version, content
    )


def read_manifest_bytes_cached(
    var data: List[UInt8], mf: ManifestFile, mut cache: ManifestCache
) raises -> Manifest:
    var c = RecordCursor.of_bytes_cached(data^, cache.plans)

    var key = _shape_key(c, mf.partition_spec_id)
    var at = cache.find_shape(Span(key))
    if at >= 0:
        cache.hits += 1
    else:
        cache.misses += 1
        at = cache.add_shape(key^, _manifest_shape(c, mf))
    ref sh = cache.shapes[at]
    var format_version = sh.format_version
    var spec_id = sh.spec_id
    var content = sh.content
    ref spec = sh.spec
    ref part_type = sh.part_type
    ref s = sh.slots

    var entries = List[ManifestEntry]()
    # first_row_id inheritance accumulates over the whole manifest.
    var next_row_id = mf.first_row_id
    while c.next():
        var status = _int(c, s.status, STATUS_ADDED)
        var df = _data_file_from(c, s, spec, part_type)

        # ── snapshot id inheritance ────────────────────────────────────────
        var snapshot_id = mf.added_snapshot_id
        var has_snapshot_id = True
        if _present(c, s.snapshot_id):
            snapshot_id = _long(c, s.snapshot_id, mf.added_snapshot_id)

        # ── sequence number inheritance ────────────────────────────────────
        # Only ADDED entries inherit. EXISTING and DELETED entries are required
        # to carry both numbers explicitly; a v1 manifest has neither column
        # and every file defaults to 0.
        var seq: Int64 = 0
        var file_seq: Int64 = 0
        if format_version > 1:
            if _present(c, s.sequence_number):
                seq = _long(c, s.sequence_number, 0)
            elif status == STATUS_ADDED:
                seq = mf.sequence_number
            if _present(c, s.file_sequence_number):
                file_seq = _long(c, s.file_sequence_number, 0)
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
    return Manifest(
        entries^,
        spec.copy(),
        spec_id,
        sh.schema.copy(),
        format_version,
        content,
    )


def _data_file_from(
    c: RecordCursor, s: _EntrySlots, spec: PartitionSpec, part_type: Schema
) raises -> DataFile:
    var metrics = List[ColumnMetrics]()

    for k in range(s.value_counts.n(c)):
        var i = _metric_index(metrics, Int(c.get_long(s.value_counts.key, k)))
        metrics[i].value_count = c.get_long(s.value_counts.value, k)
        metrics[i].has_value_count = True
    for k in range(s.null_value_counts.n(c)):
        var i = _metric_index(
            metrics, Int(c.get_long(s.null_value_counts.key, k))
        )
        metrics[i].null_value_count = c.get_long(s.null_value_counts.value, k)
        metrics[i].has_null_value_count = True
    for k in range(s.nan_value_counts.n(c)):
        var i = _metric_index(
            metrics, Int(c.get_long(s.nan_value_counts.key, k))
        )
        metrics[i].nan_value_count = c.get_long(s.nan_value_counts.value, k)
        metrics[i].has_nan_value_count = True
    for k in range(s.lower_bounds.n(c)):
        var i = _metric_index(metrics, Int(c.get_long(s.lower_bounds.key, k)))
        metrics[i].lower_bound = c.get_bytes_copy(s.lower_bounds.value, k)
        metrics[i].has_lower = True
    for k in range(s.upper_bounds.n(c)):
        var i = _metric_index(metrics, Int(c.get_long(s.upper_bounds.key, k)))
        metrics[i].upper_bound = c.get_bytes_copy(s.upper_bounds.value, k)
        metrics[i].has_upper = True

    var sizes = List[ColumnMetrics]()
    for k in range(s.column_sizes.n(c)):
        var cm = ColumnMetrics.blank(Int(c.get_long(s.column_sizes.key, k)))
        cm.value_count = c.get_long(s.column_sizes.value, k)
        cm.has_value_count = True
        sizes.append(cm^)

    var offsets = List[Int64]()
    if _present(c, s.split_offsets):
        for k in range(c.array_len(s.split_offsets)):
            offsets.append(c.get_long(s.split_offset, k))

    var eq_ids = List[Int]()
    if _present(c, s.equality_ids):
        for k in range(c.array_len(s.equality_ids)):
            eq_ids.append(Int(c.get_long(s.equality_id, k)))

    var partition = _partition_from(c, s, spec, part_type)

    var df = DataFile(
        _int(c, s.content, CONTENT_DATA),
        _str(c, s.file_path, ""),
        _str(c, s.file_format, ""),
        partition^,
        _long(c, s.record_count, 0),
        _long(c, s.file_size_in_bytes, 0),
        sizes^,
        metrics^,
        _bytes(c, s.key_metadata),
        _present(c, s.key_metadata),
        offsets^,
        eq_ids^,
        _int(c, s.sort_order_id, 0),
        _present(c, s.sort_order_id),
        _long(c, s.first_row_id, 0),
        _present(c, s.first_row_id),
        _str(c, s.referenced_data_file, ""),
        _present(c, s.referenced_data_file),
        _long(c, s.content_offset, 0),
        _present(c, s.content_offset),
        _long(c, s.content_size_in_bytes, 0),
        _present(c, s.content_size_in_bytes),
    )
    return df^


def _partition_from(
    c: RecordCursor, s: _EntrySlots, spec: PartitionSpec, part_type: Schema
) raises -> List[Datum]:
    """Decode the `partition` struct, typed by the manifest's own spec."""
    var out = List[Datum]()
    for k in range(len(spec.fields)):
        var slot = s.partition[k]
        if not _present(c, slot):
            out.append(Datum.none())
            continue
        ref pf = spec.fields[k]
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
        out.append(_datum_from_avro(c, slot, prim, precision, scale, length))
    return out^


def _datum_from_avro(
    c: RecordCursor,
    slot: Int,
    prim: UInt8,
    precision: Int,
    scale: Int,
    length: Int,
) raises -> Datum:
    """Convert a decoded Avro value to a typed Iceberg `Datum`.

    Avro carries decimals, uuids and fixed values as bytes in exactly the
    Appendix-D layout, so those route through the binary decoder; the rest map
    onto Avro's own primitives.
    """
    var kind = c.plan.slot_kind(slot)
    if kind == BOOLEAN:
        return Datum.bool_(c.get_bool(slot))
    if kind == INT or kind == LONG:
        return Datum.integral(prim, c.get_long(slot))
    if kind == FLOAT:
        return Datum.float_(Float64(c.get_float(slot)))
    if kind == DOUBLE:
        return Datum.double_(c.get_double(slot))
    if kind == STRING:
        return Datum.string_(c.get_string(slot))
    # bytes / fixed
    return datum_from_bytes_prim(
        prim, precision, scale, length, c.get_bytes_copy(slot)
    )
