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

Writes v2 and v3: `create_table`, `append`, `delete_where` (copy-on-write, or
merge-on-read with deletion vectors on v3 and position delete files on v2),
`delete_by_equality`, `overwrite`, `dynamic_partition_overwrite` and
`expire_snapshots`, each committed optimistically against a filesystem layout
or a REST catalog.
"""

from iceberg.append import (
    AppendResult,
    metadata_file_name,
    next_metadata_version,
    prepare_append,
    write_and_prepare_append,
)
from iceberg.batch import (
    ColumnBuilder,
    NestedBuilder,
    array_from_datums,
    batch_of,
    batch_of_columns,
)
from iceberg.commit import FileChanges, OP_DELETE, OP_OVERWRITE, prepare_commit
from iceberg.delete import (
    MODE_COPY_ON_WRITE,
    MODE_MERGE_ON_READ,
    PROP_DELETE_MODE,
    RowDelete,
    delete_mode_of,
    plan_row_deletes,
    prepare_delete,
    prepare_delete_from,
    prepare_dynamic_partition_overwrite,
    prepare_equality_delete,
    prepare_overwrite,
    write_equality_deletes,
    write_deletion_vectors,
    write_position_deletes,
)
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
from iceberg.kernels import (
    arrow_type_for,
    cast_array,
    concat_into,
    constant_array,
    filter_array,
)
from iceberg.json import Json, parse_json
from iceberg.nested import (
    ColumnTree,
    ColumnType,
    cast_column,
    cell_datum,
    cell_json,
    concat_tree,
    filter_tree,
    flatten_leaf,
    null_tree,
    subtree_copy,
)
from iceberg.maintain import (
    ExpireResult,
    choose_expired,
    delete_expired_files,
    expire_snapshots,
)
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
from iceberg.manifest_write import (
    WrittenManifest,
    manifest_entry_schema_json,
    manifest_list_schema_json,
    write_manifest,
    write_manifest_list,
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
    PuffinWriter,
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
    read_data_file,
    read_data_file_table,
)
from iceberg.scan import FileScanTask, TableScan
from iceberg.schema import Schema
from iceberg.write import (
    WriteOptions,
    partition_path,
    truncate_lower,
    truncate_upper,
    write_data_files,
)
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
