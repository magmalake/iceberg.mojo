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

and then, per ticket, one line per batch as it lands followed by an end
marker — so a consumer reads the first batch while the second is still being
written:

    {"batch": {"path": "...", "columns": [...], "bytes": N}}
    {"end": true}

`columns` describes each buffer by **offset** into the mapping, because that
is what survives the trip — the mapping lands at a different address in the
consumer. Flat columns only; `export_shared` refuses a nested one rather than
writing something the consumer would misread.
"""

from std.ffi import external_call
from std.os import getenv
from std.sys import argv
from std.time import perf_counter_ns

from arrow_mlake.carrow_shared import (
    export_shared_into,
    shared_size,
)
from arrow_mlake.memory_shim import open_split_region
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

        # SHM_TIMING=1 splits a ticket into the scan and the publish. A
        # consumer measuring this from another language has no other way to
        # see which half it is waiting for.
        var timing = getenv("SHM_TIMING", "") != ""
        var t0 = perf_counter_ns()
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

        var t1 = perf_counter_ns()
        var export_ns = 0

        # One mapping for the whole split, sized up front, with each batch
        # written into it in turn. A mapping per batch paid create, size, map,
        # unmap and unlink four times over, which measured larger than the
        # copy itself.
        var total = 0
        var all_names = List[List[String]]()
        for b in range(len(batches)):
            ref batch = batches[b]
            total += shared_size(batch.arena, batch.roots)
            var names = List[String]()
            for c in range(len(batch.roots)):
                names.append(batch.arena.nodes[batch.roots[c]].name)
            all_names.append(names^)

        var path = (
            out_dir + "/split-" + String(pid) + "-" + String(seq) + ".arrow"
        )
        seq += 1
        var bump = open_split_region(path, total)

        # Still one line per batch as it lands, so the consumer folds batch 1
        # while this writes batch 2 — they just share a file now, and the
        # offsets in each manifest are from the region's base.
        for b in range(len(batches)):
            ref batch = batches[b]
            var e0 = perf_counter_ns()
            var manifest = export_shared_into(
                bump, batch.arena, batch.roots, all_names[b]
            )
            export_ns += perf_counter_ns() - e0
            print(
                '{"batch":{"path":"' + path + '",' + manifest[byte=1:] + "}",
                flush=True,
            )
        bump^.close()
        print('{"end":true}', flush=True)
        if timing:
            # A message of its own, so a consumer that does not care about
            # timing skips it the way it skips anything it does not know.
            print(
                String(
                    '{"timing":{"scan_ms":',
                    (t1 - t0) // 1000000,
                    ',"publish_ms":',
                    export_ns // 1000000,
                    ',"batches":',
                    len(batches),
                    "}}",
                ),
                flush=True,
            )
