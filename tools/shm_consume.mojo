"""Read published mappings without a copy, and without leaving Mojo.

The consumer half of `tools/shm_publish.mojo`, in the same language as the
producer. It reads manifests on stdin, maps each file once, and folds a
`float64` column straight out of the mapping — no decode, no copy, and no
Python anywhere in the data path.

```console
printf '%s\n' "$tickets" | ib-shm-publish <table> <split> <col> <dir> \\
    | ib-shm-consume
```

Everything in a manifest is an offset from the region's base, so this maps
wherever the kernel puts it and adds its own base. That is the property that
lets Arrow buffers cross a process at all: the C Data Interface hands over
pointers, and a pointer means nothing in another address space.
"""

from std.ffi import external_call
from std.os import getenv

from stdin_lines import StdinLines
from std.time import perf_counter_ns

from iceberg.json import JSON_OBJECT, Json, parse_json
from memory_region import SharedMapping


def main() raises:
    var rows = 0
    var total = Float64(0)
    var mapped = 0
    # The mapping for the file being read. A split's batches arrive one after
    # another naming the same file, so it is mapped once and reused; the next
    # file replaces it, and the replaced one unmaps as it goes.
    var current: Optional[SharedMapping] = None
    # SHM_FOLD=0 maps and counts without touching the values. The difference
    # between the two is what folding costs, which is the consumer's own
    # business — a scalar loop here, eight parallel tasks in an engine — and
    # what is left is the handover.
    var fold = getenv("SHM_FOLD", "1") != "0"
    var stdin = StdinLines()
    var t0 = perf_counter_ns()

    while True:
        var line: String
        try:
            line = stdin.next_line()
        except:
            break
        if line.byte_length() == 0:
            continue

        var doc = parse_json(line)
        var batch = doc.get(doc.root, "batch")
        if batch < 0:
            continue  # {"end":…} and {"timing":…} are not ours to read

        var path = doc.as_string(doc.get(batch, "path"))
        # One map per file: a split's batches share it, and the offsets in
        # each manifest are from the region's base rather than the batch's.
        if not current or current.value().path != path:
            current = SharedMapping(path^)
            mapped += 1
        ref region = current.value()

        var columns = doc.get(batch, "columns")
        for c in range(doc.size(columns)):
            var column = doc.at(columns, c)
            var n = Int(doc.as_int(doc.get(column, "length")))
            var buffers = doc.get(column, "buffers")
            # buffer 0 is validity and may be absent; the values are last.
            var values = doc.at(buffers, doc.size(buffers) - 1)
            # A `null` buffer — an all-valid validity bitmap — has nothing in
            # it to fold; a real one is an object with an offset.
            if doc.kind(values) != JSON_OBJECT:
                continue
            var offset = Int(doc.as_int(doc.get(values, "offset")))
            # Checked against the mapping: an offset or length the manifest
            # got wrong raises here instead of reading past the end.
            var column_values = region.span[DType.float64](offset, n)
            if fold:
                for x in column_values:
                    total += x
            else:
                # Touch one value per 16 KiB page: the mapping is only real
                # once its pages are, and a count that never faults them in
                # would be measuring nothing.
                var stride = 2048
                var i = 0
                while i < n:
                    total += column_values[i]
                    i += stride
            rows += n

    var ms = (perf_counter_ns() - t0) // 1000000
    print(
        String(
            "consumed ",
            rows,
            " rows from ",
            mapped,
            " mappings in ",
            ms,
            " ms, sum ",
            total,
        )
    )
