"""Catalogs: how a table name becomes a `TableMetadata`.

`filesystem` needs nothing but a path. `rest` is a documented stub — see its
module docstring for exactly what is and is not implemented and why. `sql` is
a JDBC-style catalog over sqlite.mojo or postgres.mojo — sqlite for local
development and PyIceberg test parity, PostgreSQL for a catalog several
writers can share — with `_sqldriver` holding the small part that differs
between the two.
"""

from ._sqldriver import SqlDriver, is_postgres_uri, is_unique_violation
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
