"""A shared library that reads one scan split into an `ArrowArrayStream`.

`tools/carrow_scan.mojo` exports a single column of a whole scan and exists to
prove nested Arrow survives the boundary. This is the other thing a C Data
Interface producer is for: the **local** answer to the question Flight answers
remotely — hand a consumer in this process the rows of one unit of work, as
Arrow, with no wire format in between.

The pair of functions is deliberately the same shape as the Flight server's two
calls, because they are answering the same two questions:

| | Flight | here |
|---|---|---|
| what are the units of work? | `GetFlightInfo` | `ib_plan_splits` |
| give me one | `DoGet` | `ib_scan_split` |

**The ticket format is `flight.mojo`'s**, `<snapshot>|<start>|<length>|<path>`,
so a client holding a ticket can redeem it either way — over gRPC from another
machine, or in this process through `dlopen`. That makes the two paths
comparable at the level that matters, which is the point of having both. The
duplication of the format between two repos is real; a shared definition is
where this goes if it outlives being an example.

```console
mojo build --emit shared-lib tools/carrow_stream_scan.mojo $ICEBERG_INCLUDES \\
    -o build/libibscan.so
```

Consumers: `pyarrow.RecordBatchReader._import_from_c(addr)` takes the address
`ib_scan_split` returns and owns the stream from there — including freeing it.
"""

from std.os import getenv
from std.time import perf_counter_ns

from arrow_mlake.arrow import AT_STRUCT, ArrayData, ArrowType
from arrow_mlake.batch import RecordBatch
from arrow_mlake.carrow import ExportedArray, export_c
from arrow_mlake.carrow_import import release_c_array
from arrow_mlake.carrow_stream import export_stream_of
from iceberg.catalog.filesystem import find_latest_metadata
from iceberg.io import FileIO
from iceberg.metadata import TableMetadata
from iceberg.read import ScanOptions
from iceberg.scan import TableScan


def _read_file(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


def _split_commas(s: String) -> List[String]:
    var out = List[String]()
    var bytes = s.as_bytes()
    var start = 0
    for i in range(len(bytes)):
        if bytes[i] == UInt8(44):  # ","
            if i > start:
                out.append(String(unsafe_from_utf8=bytes[start:i]))
            start = i + 1
    if len(bytes) > start:
        out.append(String(unsafe_from_utf8=bytes[start:]))
    return out^


def _scan(
    table_dir: String,
    split_size: Int,
    columns: List[String],
    filter_dsl: String,
) raises -> TableScan:
    """An empty `columns` selects every column, as a bare scan does.

    An empty `filter_dsl` is no predicate. A non-empty one is the filter DSL
    `TableScan.filter` takes — a JSON s-expression, `["gt", "trip_distance",
    10]` — and it must be given to the plan and to every read of that plan's
    tickets alike: planning uses it to prune manifests and files, and the read
    needs the same predicate to leave the same rows behind.
    """
    var io = FileIO.local()
    var meta_path = find_latest_metadata(io, table_dir)
    var metadata = TableMetadata.parse(_read_file(meta_path))
    var scan = TableScan(metadata^, io^).with_split_size(split_size)
    if filter_dsl.byte_length() != 0:
        scan = scan.filter(filter_dsl)
    if len(columns) == 0:
        return scan^
    return scan.select(columns.copy())


def _ticket(
    snapshot_id: Int64, start: Int64, length: Int64, path: String
) -> String:
    return String(
        String(snapshot_id),
        "|",
        String(start),
        "|",
        String(length),
        "|",
        path,
    )


def _parse_ticket(ticket: String) raises -> Tuple[Int64, Int64, Int64, String]:
    """Split on the first three bars; the rest is the path.

    Only the first three, because a file path may itself contain one.
    """
    var bytes = ticket.as_bytes()
    var cuts = List[Int]()
    for i in range(len(bytes)):
        if bytes[i] == UInt8(124):  # "|"
            cuts.append(i)
            if len(cuts) == 3:
                break
    if len(cuts) < 3:
        raise Error(
            "iceberg: malformed ticket, expected"
            " '<snapshot>|<start>|<length>|<path>'"
        )
    var snap = Int64(atol(String(unsafe_from_utf8=bytes[: cuts[0]])))
    var start = Int64(
        atol(String(unsafe_from_utf8=bytes[cuts[0] + 1 : cuts[1]]))
    )
    var length = Int64(
        atol(String(unsafe_from_utf8=bytes[cuts[1] + 1 : cuts[2]]))
    )
    var path = String(unsafe_from_utf8=bytes[cuts[2] + 1 :])
    return (snap, start, length, path^)


def _batch_root(mut batch: RecordBatch) raises -> Int:
    """Wrap a batch's columns in the struct array a record batch *is*.

    `RecordBatch.roots` is one array per column, which is how the scan builds
    it. The C Data Interface wants the batch itself — a struct whose children
    are the columns — so this adds that node rather than exporting the columns
    separately, which would hand the consumer several arrays and no batch.
    """
    var row = ArrayData(ArrowType(AT_STRUCT), String("row"))
    row.nullable = False
    row.length = batch.num_rows
    row.null_count = 0
    row.children = batch.roots.copy()
    return batch.arena.add(row^)


@export("ib_plan_splits")
def ib_plan_splits(
    table_dir_ptr: UnsafePointer[UInt8, ImmUntrackedOrigin],
    split_size: Int64,
    filter_ptr: UnsafePointer[UInt8, ImmUntrackedOrigin],
    snapshot_id: Int64,
    dest: UnsafePointer[UInt8, MutUntrackedOrigin],
    dest_len: Int64,
) abi("C") -> Int64:
    """The scan plan as newline-separated tickets; returns the byte length.

    `filter_ptr` is the filter DSL, empty for no predicate. It belongs here
    rather than only on the read because this is where it saves the most: a
    predicate over a partition column drops whole manifests and whole files
    before a ticket is ever minted, so a consumer that pushes one down gets a
    shorter task list rather than the same tasks returning fewer rows.

    Two-call protocol, which is what a C caller with no allocator of ours can
    use: ask with `dest_len` 0 to learn the size, allocate, ask again. Nothing
    is written unless the whole blob fits, so a short buffer is never a
    partial answer. Returns -1 on any error.

    `snapshot_id` is the version to plan against, or `0` for the current one.
    Passing it matters once a consumer plans more than once for the same
    dataset — which is what pushing a predicate down means, since the task list
    depends on the predicate. The snapshot goes into every ticket either way,
    so a plan describes one version of the table even if a commit lands before
    the tickets are redeemed; naming it keeps two plans of the same dataset on
    the same version as well.
    """
    try:
        var table_dir = String(unsafe_from_utf8_ptr=table_dir_ptr)
        var filter_dsl = String(unsafe_from_utf8_ptr=filter_ptr)
        var scan = _scan(table_dir, Int(split_size), List[String](), filter_dsl)
        var snapshot = snapshot_id
        if snapshot == 0:
            snapshot = scan.snapshot().snapshot_id

        var blob = String("")
        for task in scan.use_snapshot(snapshot).plan_files():
            blob += _ticket(
                snapshot, task.start, task.length, task.data_file.file_path
            )
            blob += "\n"

        var bytes = blob.as_bytes()
        var n = len(bytes)
        if Int64(n) <= dest_len:
            for i in range(n):
                dest[unsafe_offset=i] = bytes[i]
        return Int64(n)
    except:
        return -1


@export("ib_snapshot")
def ib_snapshot(
    table_dir_ptr: UnsafePointer[UInt8, ImmUntrackedOrigin],
) abi("C") -> Int64:
    """The table's current snapshot id; 0 if it has none, -1 on error.

    A consumer that plans more than once — one plan per predicate pushed down —
    resolves this at the start and hands it back to every `ib_plan_splits`, so
    the plans it makes over the life of one dataset all describe one version of
    the table. It reads the metadata file and nothing else.
    """
    try:
        var table_dir = String(unsafe_from_utf8_ptr=table_dir_ptr)
        var scan = _scan(table_dir, 0, List[String](), String(""))
        if not scan.has_any_snapshot():
            return 0
        return scan.snapshot().snapshot_id
    except:
        return -1


@export("ib_scan_split")
def ib_scan_split(
    table_dir_ptr: UnsafePointer[UInt8, ImmUntrackedOrigin],
    ticket_ptr: UnsafePointer[UInt8, ImmUntrackedOrigin],
    split_size: Int64,
    columns_ptr: UnsafePointer[UInt8, ImmUntrackedOrigin],
    filter_ptr: UnsafePointer[UInt8, ImmUntrackedOrigin],
) abi("C") -> Int:
    """One ticket's rows as an `ArrowArrayStream`; returns its address, 0 on
    error.

    `columns_ptr` is a comma-separated projection; empty reads every column.
    `filter_ptr` is the filter DSL, empty for no predicate; it must be the one
    the plan was made with, for the same reason `split_size` must be.

    A predicate that survives to here still pays: the residual drops row groups
    and pages on the statistics in the Parquet footer before anything is
    decoded, and what it cannot drop it applies row by row. A consumer that
    filters afterwards instead decodes every page and materialises every row.

    `split_size` must match the one the plan was made with: the read re-plans
    and then selects the task at that offset, so a different division has no
    task there and the split comes back empty. That is the same rule a worker
    holding a stale Flight ticket follows.

    The address belongs to the caller from here. Releasing the stream frees
    everything behind it, and dropping the address instead leaks it.
    """
    try:
        # IB_SCAN_TIMING=1 prints where a split's milliseconds went. A consumer
        # measuring this library from another language has no other way to see
        # inside one call.
        var timing = getenv("IB_SCAN_TIMING", "") != ""
        var t0 = perf_counter_ns()
        var table_dir = String(unsafe_from_utf8_ptr=table_dir_ptr)
        var parsed = _parse_ticket(String(unsafe_from_utf8_ptr=ticket_ptr))
        var paths = List[String]()
        var starts = List[Int64]()
        starts.append(parsed[1])
        paths.append(parsed[3])

        var columns = _split_commas(String(unsafe_from_utf8_ptr=columns_ptr))
        var filter_dsl = String(unsafe_from_utf8_ptr=filter_ptr)
        var scan = _scan(
            table_dir, Int(split_size), columns, filter_dsl
        ).use_snapshot(parsed[0])
        var t1 = perf_counter_ns()
        var batches = scan.to_batches_for_splits(paths^, starts^, ScanOptions())
        var t2 = perf_counter_ns()
        if len(batches) == 0:
            return 0

        var exported = List[ExportedArray]()
        for i in range(len(batches)):
            var root = _batch_root(batches[i])
            exported.append(export_c(batches[i].arena, root))
        var t3 = perf_counter_ns()
        var addr = export_stream_of(exported^)
        if timing:
            print(
                "ib_scan_split: open",
                (t1 - t0) // 1000000,
                "ms, read",
                (t2 - t1) // 1000000,
                "ms, export",
                (t3 - t2) // 1000000,
                "ms",
                flush=True,
            )
        return addr
    except:
        return 0


@export("ib_schema")
def ib_schema(
    table_dir_ptr: UnsafePointer[UInt8, ImmUntrackedOrigin],
    split_size: Int64,
    columns_ptr: UnsafePointer[UInt8, ImmUntrackedOrigin],
) abi("C") -> Int:
    """The projected schema as a `CArrowSchema` address; 0 on error.

    A consumer needs the schema before it decides to read anything — Daft
    plans with it, a client may refuse on it — and asking for it should not
    cost a scan. This reads **one row** of the first task with
    `ScanOptions.limit`, takes the schema off that batch and releases the
    array, so the answer costs one row group rather than a table.

    The address belongs to the caller, who must release it.
    """
    try:
        var table_dir = String(unsafe_from_utf8_ptr=table_dir_ptr)
        var columns = _split_commas(String(unsafe_from_utf8_ptr=columns_ptr))
        var scan = _scan(table_dir, Int(split_size), columns, String(""))
        var snapshot = scan.snapshot().snapshot_id

        var tasks = scan.use_snapshot(snapshot).plan_files()
        if len(tasks) == 0:
            return 0

        var paths = List[String]()
        var starts = List[Int64]()
        paths.append(tasks[0].data_file.file_path)
        starts.append(tasks[0].start)

        var options = ScanOptions()
        options.limit = 1
        var batches = (
            _scan(table_dir, Int(split_size), columns, String(""))
            .use_snapshot(snapshot)
            .to_batches_for_splits(paths^, starts^, options)
        )
        if len(batches) == 0:
            return 0

        var root = _batch_root(batches[0])
        var exported = export_c(batches[0].arena, root)
        var pair = exported.into_raw()
        # The array was only ever a carrier for its schema.
        release_c_array(pair[0])
        return pair[1]
    except:
        return 0
