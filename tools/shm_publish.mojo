"""Publish Iceberg scan splits into shared mappings, one line at a time.

The producer half of a cross-process Arrow handover. It plans a scan, then
reads tickets on stdin and, for each, writes that split's Arrow buffers into a
mapping and prints where they landed. A consumer in another process maps the
file and points at the buffers: nothing is encoded, nothing is sent, and the
reading side copies nothing at all.

Why a long-lived process on a pipe rather than a command per split: the point
is the cost of handing data over, and a process launch per split would measure
Mojo's startup instead. The pipe carries one line each way, which is the
control plane doing what a control plane costs — a few bytes.

```console
mojo build tools/shm_publish.mojo $ICEBERG_INCLUDES -o build/ib-shm-publish
./build/ib-shm-publish <table-dir> <split-size> <columns> <out-dir>
```

Several of these can share an output directory — a consumer that wants the
decode parallelised runs a few — so each names its files after its own pid.

It prints one JSON line on startup naming the tickets, then one line per
ticket it is given:

    {"tickets": ["<snapshot>|<start>|<length>|<path>", ...]}
    {"batches": [{"path": "...", "columns": [...], "bytes": N}, ...]}

`columns` describes each buffer by **offset** into the mapping, because that
is what survives the trip — the mapping lands at a different address in the
consumer. Flat columns only; `export_shared` refuses a nested one rather than
writing something the consumer would misread.
"""

from std.ffi import external_call
from std.sys import argv

from arrow_mlake.carrow_shared import export_shared
from iceberg.catalog.filesystem import find_latest_metadata
from iceberg.io import FileIO
from iceberg.metadata import TableMetadata
from iceberg.read import ScanOptions
from iceberg.scan import TableScan


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


def _read_file(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


def _scan(
    table_dir: String, split_size: Int, columns: List[String]
) raises -> TableScan:
    var io = FileIO.local()
    var metadata = TableMetadata.parse(
        _read_file(find_latest_metadata(io, table_dir))
    )
    var scan = TableScan(metadata^, io^).with_split_size(split_size)
    if len(columns) == 0:
        return scan^
    return scan.select(columns.copy())


def _parse_ticket(t: String) raises -> Tuple[Int64, Int64, String]:
    """`<snapshot>|<start>|<length>|<path>` — length is not needed here."""
    var parts = List[String]()
    var bytes = t.as_bytes()
    var start = 0
    for i in range(len(bytes)):
        if bytes[i] == UInt8(124):  # "|"
            parts.append(String(unsafe_from_utf8=bytes[start:i]))
            start = i + 1
    parts.append(String(unsafe_from_utf8=bytes[start:]))
    if len(parts) != 4:
        raise Error("shm_publish: malformed ticket")
    return (Int64(atol(parts[0])), Int64(atol(parts[1])), parts[3])


def main() raises:
    var args = argv()
    if len(args) < 5:
        print(
            (
                "usage: ib-shm-publish <table-dir> <split-size> <columns>"
                " <out-dir>"
            ),
            flush=True,
        )
        return
    var table_dir = String(args[1])
    var split_size = atol(String(args[2]))
    var columns = _split_commas(String(args[3]))
    var out_dir = String(args[4])

    # The plan is pinned to one snapshot and handed over once, for the same
    # reason the Flight server pins it: a consumer redeeming tickets later
    # still sees the table it was told about.
    var scan = _scan(table_dir, split_size, columns)
    var snapshot = scan.snapshot().snapshot_id
    var tickets = String('{"tickets":[')
    var n = 0
    for task in scan.use_snapshot(snapshot).plan_files():
        if n:
            tickets += ","
        tickets += '"' + String(snapshot) + "|" + String(task.start) + "|"
        tickets += String(task.length) + "|" + task.data_file.file_path + '"'
        n += 1
    tickets += "]}"
    print(tickets, flush=True)

    # The file name carries the pid: a consumer may run several of these to
    # decode in parallel, and they share an output directory. Without it two
    # workers both write `batch-0` and whichever consumer reads second finds
    # a file the first one has already unlinked.
    var pid = Int(external_call["getpid", Int32]())
    var seq = 0
    while True:
        var line: String
        try:
            line = input("")
        except:
            break  # EOF: the consumer is done with us
        if line.byte_length() == 0:
            continue

        var parsed = _parse_ticket(line)
        var paths: List[String] = [parsed[2]]
        var starts: List[Int64] = [parsed[1]]
        # The scan built at startup, reused. Rebuilding it per ticket would
        # re-read the table metadata and re-plan for every split — the same
        # fixed cost per unit of work that made flight.mojo's DoGet slow, and
        # just as invisible until it is measured.
        var batches = scan.use_snapshot(parsed[0]).to_batches_for_splits(
            paths^, starts^, ScanOptions()
        )

        var out = String('{"batches":[')
        for b in range(len(batches)):
            ref batch = batches[b]
            var names = List[String]()
            for c in range(len(batch.roots)):
                names.append(batch.arena.nodes[batch.roots[c]].name)
            var path = (
                out_dir + "/batch-" + String(pid) + "-" + String(seq) + ".arrow"
            )
            seq += 1
            var manifest = export_shared(batch.arena, batch.roots, names, path)
            if b:
                out += ","
            # Splice the path in beside the columns the manifest describes.
            out += '{"path":"' + path + '",' + manifest[byte=1:]
        out += "]}"
        print(out, flush=True)
