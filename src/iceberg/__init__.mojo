"""iceberg.mojo — native Apache Iceberg metadata, transforms and scan planning.

Part of magmalake: data lake building blocks in Mojo.

```mojo
from iceberg.catalog.filesystem import Table

def main() raises:
    var t = Table.load_local("/warehouse/db/orders")
    var tasks = t.scan().filter('[">","id",2]').plan_files()
    for k in range(len(tasks)):
        print(tasks[k].data_file.file_path)
```

Reads format versions 1, 2 and 3, and tolerates the parts of v4 the spec
already tells readers to tolerate. It plans scans; it does not read data —
that needs a Parquet decoder, which is a separate tin.
"""

from iceberg.expressions import (
    ColumnMetrics,
    Expr,
    FieldSummary,
    InclusiveMetricsEvaluator,
    ManifestEvaluator,
    ResidualEvaluator,
    bind,
    parse_filter,
    project_inclusive,
    project_strict,
    rewrite_not,
)
from iceberg.io import FileIO, InputFile, LocalInputFile, strip_scheme
from iceberg.json import Json, parse_json
from iceberg.manifest import (
    DataFile,
    Manifest,
    ManifestEntry,
    ManifestFile,
    read_manifest,
    read_manifest_list,
)
from iceberg.metadata import (
    Snapshot,
    SnapshotRef,
    TableMetadata,
    SUPPORTED_FORMAT_VERSION,
)
from iceberg.scan import FileScanTask, TableScan
from iceberg.schema import Schema
from iceberg.transforms import (
    PartitionField,
    PartitionSpec,
    SortOrder,
    Transform,
    bucket_of,
    parse_transform,
)
from iceberg.types import NestedField, TypeStore
from iceberg.values import Datum, compare
