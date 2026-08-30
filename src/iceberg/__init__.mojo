"""iceberg.mojo — native Apache Iceberg: metadata, planning, and rows.

Part of magmalake: data lake building blocks in Mojo.

```mojo
from iceberg.catalog.filesystem import Table

def main() raises:
    var t = Table.load_local("/warehouse/db/orders")
    var rows = t.scan().filter('[">","id",2]').to_table()
    print(rows.to_csv())
```

Reads format versions 1, 2 and 3, and tolerates the parts of v4 the spec
already tells readers to tolerate: metadata, snapshots, partition transforms,
expressions, manifests, scan planning, Puffin deletion vectors, and the data
files themselves — over local files, S3, GCS, Azure or HTTP, from a filesystem
layout or a live REST catalog.
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
    read_manifest_io,
    read_manifest_list,
    read_manifest_list_io,
)
from iceberg.metadata import (
    Snapshot,
    SnapshotRef,
    TableMetadata,
    SUPPORTED_FORMAT_VERSION,
)
from iceberg.puffin import (
    BlobMetadata,
    PuffinFile,
    deleted_positions,
    read_deletion_vector,
)
from iceberg.read import (
    NameMapping,
    ScanColumn,
    ScanOptions,
    ScanResult,
    arrow_type_of,
    is_metadata_column,
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
