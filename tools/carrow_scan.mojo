"""A tiny C shared library: scan a table and hand one column to the caller
over the Arrow C Data Interface.

`tools/consume_c_data.py` dlopens this, passes two ctypes buffers, and feeds
the result straight to `pyarrow.Array._import_from_c` — which is what proves
a nested scan produces real Arrow: the struct, list and map trees a scan
builds are exported children and all, with no copy and no conversion on the
way out.

The fixtures carry absolute `file://` paths into the warehouse they were
generated in (see tests/fixtures/PROVENANCE.md), so the caller passes the
fixtures directory too and the `FileIO` is rebased onto it, exactly as the
Mojo tests do.

```console
mojo build --emit shared-lib tools/carrow_scan.mojo $ICEBERG_INCLUDES \\
    -o build/libibcarrow.so
```
"""

from iceberg.io import FileIO
from iceberg.metadata import TableMetadata
from iceberg.scan import TableScan
from parquet import export_c


comptime WAREHOUSE_PREFIX = String(
    "file:///Users/mseritan/dev/magmalake/iceberg.mojo/build/warehouse-root"
    "/warehouse/db"
)
"""The prefix baked into the fixture metadata; `tests/fixtures/
WAREHOUSE_ROOT.txt` records it and `tests/iceberg_test.mojo` uses the same
constant."""


def _read_file(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


@export("ib_export_scan_column")
def ib_export_scan_column(
    meta_path: UnsafePointer[UInt8, ImmUntrackedOrigin],
    fixtures: UnsafePointer[UInt8, ImmUntrackedOrigin],
    col: Int32,
    arr_out: UnsafePointer[UInt8, MutUntrackedOrigin],
    sch_out: UnsafePointer[UInt8, MutUntrackedOrigin],
) abi("C") -> Int32:
    """Export column `col` of a whole scan of the table at `meta_path`.

    The root `ArrowArray` (80 bytes) is copied into `arr_out` and the root
    `ArrowSchema` (72 bytes) into `sch_out`, exactly as a C producer would
    "move" them into caller-owned storage. Returns the number of columns the
    scan produced, or -1 on any error.
    """
    try:
        var meta = String(unsafe_from_utf8_ptr=meta_path)
        var root = String(unsafe_from_utf8_ptr=fixtures)
        var io = FileIO.local()
        io.rebase(WAREHOUSE_PREFIX, root)
        var scan = TableScan(TableMetadata.parse(_read_file(meta)), io^)
        var batch = scan.to_table().to_batch()
        var ci = Int(col)
        if ci < 0 or ci >= batch.num_columns():
            return -1
        var e = export_c(batch.arena, batch.roots[ci])
        var raw = e.into_raw()
        var a = UnsafePointer[UInt8, ImmUntrackedOrigin](
            unsafe_from_address=raw[0]
        )
        var s = UnsafePointer[UInt8, ImmUntrackedOrigin](
            unsafe_from_address=raw[1]
        )
        for i in range(80):
            arr_out[unsafe_offset=i] = a[i]
        for i in range(72):
            sch_out[unsafe_offset=i] = s[i]
        return Int32(batch.num_columns())
    except:
        return -1
