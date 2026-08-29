"""Catalogs: how a table name becomes a `TableMetadata`.

`filesystem` needs nothing but a path. `rest` is a documented stub — see its
module docstring for exactly what is and is not implemented and why.
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
