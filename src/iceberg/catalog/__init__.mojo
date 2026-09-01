"""Catalogs: how a table name becomes a `TableMetadata`.

`filesystem` needs nothing but a path. `rest` is a documented stub — see its
module docstring for exactly what is and is not implemented and why. `sql` is
a JDBC-style catalog over sqlite.mojo, for local development and PyIceberg
test parity — see its module docstring for why it exists and what it is not
for.
"""

from .filesystem import (
    FilesystemCatalog,
    Table,
    find_latest_metadata,
    read_metadata_file,
    read_version_hint,
)
from .rest import (
    LoadTableResult,
    RestCatalogConfig,
    ACCESS_DELEGATION_HEADER,
    VENDED_CREDENTIALS,
)
from .sql import (
    NamespacePropertiesUpdateSummary,
    SqlCatalog,
    sqlite_path_from_uri,
)
