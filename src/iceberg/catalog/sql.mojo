"""A JDBC-style SQL catalog, over sqlite.mojo or postgres.mojo — the schema
and semantics PyIceberg's `SqlCatalog` uses.

Which backend answers is the URI's business and nothing else's:
`sqlite:///catalog.db` opens a file, `postgresql://user@host/db` opens a
server, and every method below is the same either way. See `_sqldriver` for
what that costs (placeholder rewriting, and one predicate over SQLSTATEs).

Over **sqlite** this is a development catalog: local, single-process, and test
parity with PyIceberg, whose quickstart default is exactly this — a
SQLite-backed `SqlCatalog` — and which is what iceberg-rs.mojo's own fixtures
already use as an oracle. Over **PostgreSQL** the same catalog is deployable:
`iceberg_tables` and `iceberg_namespace_properties` are the tables PyIceberg's
`SqlCatalog` and the Java `JdbcCatalog` both use, the guarded `UPDATE` is a
real transaction against a real server, and many writers may share it. A
database this catalog creates opens cleanly in PyIceberg's `SqlCatalog` and
vice versa, on either engine: same two tables, same column names, same
optimistic-concurrency guard.

    iceberg_tables (
        catalog_name, table_namespace, table_name,   -- primary key
        metadata_location, previous_metadata_location
    )
    iceberg_namespace_properties (
        catalog_name, namespace, property_key,        -- primary key
        property_value
    )

Namespaces are dot-joined strings (`'db.sub'`); a nested namespace is a prefix
of a longer one, matched with a `LIKE ... ESCAPE '!'` pattern the same way
PyIceberg escapes `%`, `_` and the escape character itself.

The commit path is the one part of this file that is not shared with the
sibling catalogs: unlike `FilesystemCatalog` (whose optimistic concurrency
compares the newest file a directory listing turns up) and `RestCatalog`
(whose server enforces `assert-ref-snapshot-id`), a SQL catalog's source of
truth is one row, and the swap is a guarded

    UPDATE iceberg_tables SET metadata_location = <new>
    WHERE ... AND metadata_location = <the value this attempt read>

exactly PyIceberg's guard. Zero rows affected means somebody else's commit
already moved the pointer; this reloads and retries like the REST catalog's
409 handling, and raises a commit-failed error when retries run out. Manifest
writing, data-file writing and metadata JSON serialization are the same
`prepare_append` / `prepare_delete` / `prepare_overwrite` /
`prepare_dynamic_partition_overwrite` machinery `RestCatalog` uses — this
file only adds the row that names the current metadata file.
"""

from std.collections import Dict

from parquet import RecordBatch

from ._sqldriver import (
    SqlDriver,
    is_postgres_uri,
    is_unique_violation,
    sqlite_path_from_uri,
)
from ..append import (
    AppendResult,
    metadata_file_name,
    next_metadata_version,
    prepare_append,
)
from ..delete import (
    prepare_delete,
    prepare_dynamic_partition_overwrite,
    prepare_overwrite,
)
from ..io import FileIO, dirname, join_path
from ..manifest import DataFile
from ..schema import Schema
from ..transforms import PartitionSpec
from ..write import WriteOptions, write_data_files
from .filesystem import Table, new_table_metadata, read_metadata_file
from .rest import COMMIT_DELETE, COMMIT_DYNAMIC, COMMIT_OVERWRITE


comptime NAMESPACE_EXISTS_PROPERTY = String("exists")

comptime _CREATE_TABLES_SQL = String(
    "CREATE TABLE IF NOT EXISTS iceberg_tables ("
    "catalog_name VARCHAR(255) NOT NULL,"
    "table_namespace VARCHAR(255) NOT NULL,"
    "table_name VARCHAR(255) NOT NULL,"
    "metadata_location VARCHAR(1000),"
    "previous_metadata_location VARCHAR(1000),"
    "PRIMARY KEY (catalog_name, table_namespace, table_name))"
)
comptime _CREATE_NAMESPACE_PROPERTIES_SQL = String(
    "CREATE TABLE IF NOT EXISTS iceberg_namespace_properties ("
    "catalog_name VARCHAR(255) NOT NULL,"
    "namespace VARCHAR(255) NOT NULL,"
    "property_key VARCHAR(255) NOT NULL,"
    "property_value VARCHAR(1000) NOT NULL,"
    "PRIMARY KEY (catalog_name, namespace, property_key))"
)


def _cell(row: List[Optional[String]], col: Int) -> String:
    """One cell of a `SqlDriver.query` row as text.

    SQL NULL reads as `""`, which is what both engines' text accessors have
    always rendered it as here and what `load_table` already tested for: a
    row whose `metadata_location` is NULL is a table that does not exist.

    Args:
        row: The row.
        col: The 0-based column index.

    Returns:
        The cell's text, or `""` for SQL NULL or a missing column.
    """
    if col < len(row) and row[col]:
        return row[col].value()
    return String("")


def _escape_like(namespace: String) -> String:
    """PyIceberg's `%`/`_`/`!` escaping, so a literal namespace segment never
    matches as a wildcard in a child-namespace `LIKE ... ESCAPE '!'` query."""
    var out = String("")
    for cp in namespace.codepoint_slices():
        var s = String(cp)
        if s == "!":
            out += "!!"
        elif s == "_":
            out += "!_"
        elif s == "%":
            out += "!%"
        else:
            out += s
    return out^


def _namespace_parts(namespace: String) -> List[String]:
    if namespace == "":
        return List[String]()
    var raw = namespace.split(".")
    var out = List[String]()
    for k in range(len(raw)):
        out.append(String(raw[k]))
    return out^


def _sort_strings(mut l: List[String]):
    for i in range(1, len(l)):
        var j = i
        while j > 0 and l[j] < l[j - 1]:
            l.swap_elements(j, j - 1)
            j -= 1


struct NamespacePropertiesUpdateSummary(Copyable, Movable):
    """What `update_namespace_properties` actually did — PyIceberg's
    `PropertiesUpdateSummary` shape."""

    var removed: List[String]
    var updated: List[String]
    var missing: List[String]

    def __init__(
        out self,
        var removed: List[String],
        var updated: List[String],
        var missing: List[String],
    ):
        self.removed = removed^
        self.updated = updated^
        self.missing = missing^


struct SqlCatalog(Movable):
    """A JDBC/SQL catalog: tables and namespaces live as rows in a SQLite
    file or a PostgreSQL database, table data and metadata JSON live under
    `warehouse` — the same split PyIceberg's `SqlCatalog` makes.

    ```mojo
    var cat = SqlCatalog.local("default", "sqlite:///catalog.db", "warehouse")
    cat.create_namespace("db")
    var t = cat.create_table("db", "orders", schema)
    _ = cat.append("db", "orders", batches)
    ```

    The URI is the only thing that changes between the two backends:

    ```mojo
    var shared = SqlCatalog.local(
        "default", "postgresql://iceberg@db.internal/catalog?connect_timeout=5",
        "s3://warehouse",
    )
    ```
    """

    var name: String
    var warehouse: String
    var io: FileIO
    var driver: SqlDriver

    def __init__(
        out self,
        var name: String,
        uri: String,
        var warehouse: String,
        var io: FileIO,
        create_tables: Bool = True,
    ) raises:
        self.name = name^
        self.warehouse = warehouse^
        self.io = io^
        self.driver = SqlDriver(uri)
        if create_tables:
            _ = self.driver.execute(_CREATE_TABLES_SQL)
            _ = self.driver.execute(_CREATE_NAMESPACE_PROPERTIES_SQL)

    @staticmethod
    def local(
        var name: String, uri: String, var warehouse: String
    ) raises -> Self:
        return Self(name^, uri, warehouse^, FileIO.local())

    def create_tables(mut self) raises:
        """`CREATE TABLE IF NOT EXISTS` for both catalog tables. Idempotent;
        opening a database another SQL catalog (ours or PyIceberg's) already
        initialized is a no-op."""
        _ = self.driver.execute(_CREATE_TABLES_SQL)
        _ = self.driver.execute(_CREATE_NAMESPACE_PROPERTIES_SQL)

    def destroy_tables(mut self) raises:
        """Drop both catalog tables. Mirrors PyIceberg's `destroy_tables`."""
        _ = self.driver.execute("DROP TABLE IF EXISTS iceberg_tables")
        _ = self.driver.execute(
            "DROP TABLE IF EXISTS iceberg_namespace_properties"
        )

    def _rollback_quietly(mut self):
        """Undo the open transaction block, swallowing any failure.

        The old `with self.db.transaction():` guard rolled back from its
        destructor when the block raised; this is that, made explicit, and it
        matters more on PostgreSQL than it did on SQLite — a failed statement
        there leaves the block refusing everything but a rollback, so the
        connection stays unusable until one is issued. The original error is
        what the caller gets to see, so a failure here is dropped.
        """
        try:
            self.driver.rollback()
        except:
            pass

    def table_location(self, namespace: String, name: String) -> String:
        if namespace == "":
            return join_path(self.warehouse, name)
        return join_path(join_path(self.warehouse, namespace), name)

    # ── namespaces ───────────────────────────────────────────────────────────

    def namespace_exists(self, namespace: String) raises -> Bool:
        var pattern = _escape_like(namespace) + ".%"
        var args: List[String] = [self.name, namespace, pattern]
        var t = self.driver.query(
            (
                "SELECT 1 FROM iceberg_tables WHERE catalog_name=? AND"
                " (table_namespace=? OR table_namespace LIKE ? ESCAPE '!')"
                " LIMIT 1"
            ),
            args,
        )
        if len(t) > 0:
            return True
        var p = self.driver.query(
            (
                "SELECT 1 FROM iceberg_namespace_properties WHERE"
                " catalog_name=? AND (namespace=? OR namespace LIKE ? ESCAPE"
                " '!') LIMIT 1"
            ),
            args,
        )
        return len(p) > 0

    def create_namespace(
        mut self,
        namespace: String,
        var properties: Dict[String, String] = Dict[String, String](),
    ) raises:
        if self.namespace_exists(namespace):
            raise Error("iceberg: namespace already exists: " + namespace)
        if len(properties) == 0:
            properties[NAMESPACE_EXISTS_PROPERTY] = "true"
        self.driver.begin()
        try:
            for entry in properties.items():
                var args: List[String] = [
                    self.name,
                    namespace,
                    entry.key,
                    entry.value,
                ]
                _ = self.driver.execute(
                    (
                        "INSERT INTO iceberg_namespace_properties"
                        " (catalog_name, namespace, property_key,"
                        " property_value) VALUES (?,?,?,?)"
                    ),
                    args,
                )
        except e:
            self._rollback_quietly()
            raise e
        self.driver.commit()

    def drop_namespace(mut self, namespace: String) raises:
        if not self.namespace_exists(namespace):
            raise Error("iceberg: namespace does not exist: " + namespace)
        var tables = self.list_tables(namespace)
        if len(tables) > 0:
            raise Error(
                "iceberg: namespace "
                + namespace
                + " is not empty: "
                + String(len(tables))
                + " tables exist"
            )
        var args: List[String] = [self.name, namespace]
        _ = self.driver.execute(
            (
                "DELETE FROM iceberg_namespace_properties WHERE catalog_name=?"
                " AND namespace=?"
            ),
            args,
        )

    def list_namespaces(self, parent: String = "") raises -> List[String]:
        if parent != "" and not self.namespace_exists(parent):
            raise Error("iceberg: namespace does not exist: " + parent)
        var parent_parts = _namespace_parts(parent)
        var need_len = len(parent_parts) + 1
        var pattern = parent + ".%" if parent != "" else String("")

        var candidates = List[String]()
        var args: List[String] = [self.name]
        if pattern != "":
            args.append(pattern)

        var sql1 = String(
            "SELECT DISTINCT table_namespace FROM iceberg_tables WHERE"
            " catalog_name=?"
        )
        if pattern != "":
            sql1 += " AND table_namespace LIKE ?"
        var rows1 = self.driver.query(sql1, args)
        for k in range(len(rows1)):
            candidates.append(_cell(rows1[k], 0))

        var sql2 = String(
            "SELECT DISTINCT namespace FROM iceberg_namespace_properties"
            " WHERE catalog_name=?"
        )
        if pattern != "":
            sql2 += " AND namespace LIKE ?"
        var rows2 = self.driver.query(sql2, args)
        for k in range(len(rows2)):
            candidates.append(_cell(rows2[k], 0))

        var seen = Dict[String, Bool]()
        var out = List[String]()
        for k in range(len(candidates)):
            var parts = _namespace_parts(candidates[k])
            if len(parts) < need_len:
                continue
            var matches = True
            for j in range(len(parent_parts)):
                if parts[j] != parent_parts[j]:
                    matches = False
                    break
            if not matches:
                continue
            var truncated = String("")
            for j in range(need_len):
                if j > 0:
                    truncated += "."
                truncated += parts[j]
            if truncated not in seen:
                seen[truncated] = True
                out.append(truncated^)
        _sort_strings(out)
        return out^

    def load_namespace_properties(
        self, namespace: String
    ) raises -> Dict[String, String]:
        if not self.namespace_exists(namespace):
            raise Error("iceberg: namespace does not exist: " + namespace)
        var args: List[String] = [self.name, namespace]
        var rows = self.driver.query(
            (
                "SELECT property_key, property_value FROM"
                " iceberg_namespace_properties WHERE catalog_name=? AND"
                " namespace=?"
            ),
            args,
        )
        var out = Dict[String, String]()
        for k in range(len(rows)):
            out[_cell(rows[k], 0)] = _cell(rows[k], 1)
        return out^

    def update_namespace_properties(
        mut self,
        namespace: String,
        var removals: List[String] = List[String](),
        var updates: Dict[String, String] = Dict[String, String](),
    ) raises -> NamespacePropertiesUpdateSummary:
        if not self.namespace_exists(namespace):
            raise Error("iceberg: namespace does not exist: " + namespace)
        var current = self.load_namespace_properties(namespace)

        var removed = List[String]()
        var missing = List[String]()
        for k in range(len(removals)):
            if removals[k] in current:
                removed.append(removals[k])
            else:
                missing.append(removals[k])
        var updated = List[String]()
        for entry in updates.items():
            updated.append(entry.key)

        self.driver.begin()
        try:
            for k in range(len(removals)):
                var del_args: List[String] = [
                    self.name,
                    namespace,
                    removals[k],
                ]
                _ = self.driver.execute(
                    (
                        "DELETE FROM iceberg_namespace_properties WHERE"
                        " catalog_name=? AND namespace=? AND property_key=?"
                    ),
                    del_args,
                )
            for entry in updates.items():
                var key_args: List[String] = [
                    self.name,
                    namespace,
                    entry.key,
                ]
                _ = self.driver.execute(
                    (
                        "DELETE FROM iceberg_namespace_properties WHERE"
                        " catalog_name=? AND namespace=? AND property_key=?"
                    ),
                    key_args,
                )
                var ins_args: List[String] = [
                    self.name,
                    namespace,
                    entry.key,
                    entry.value,
                ]
                _ = self.driver.execute(
                    (
                        "INSERT INTO iceberg_namespace_properties"
                        " (catalog_name, namespace, property_key,"
                        " property_value) VALUES (?,?,?,?)"
                    ),
                    ins_args,
                )
        except e:
            self._rollback_quietly()
            raise e
        self.driver.commit()

        return NamespacePropertiesUpdateSummary(removed^, updated^, missing^)

    # ── tables ───────────────────────────────────────────────────────────────

    def load_table(self, namespace: String, name: String) raises -> Table:
        var args: List[String] = [self.name, namespace, name]
        var rows = self.driver.query(
            (
                "SELECT metadata_location FROM iceberg_tables WHERE"
                " catalog_name=? AND table_namespace=? AND table_name=?"
            ),
            args,
        )
        if len(rows) == 0:
            raise Error(
                "iceberg: table does not exist: " + namespace + "." + name
            )
        var loc = _cell(rows[0], 0)
        if loc == "":
            raise Error(
                "iceberg: table does not exist: " + namespace + "." + name
            )
        var m = read_metadata_file(self.io, loc)
        return Table(m^, loc, self.io.copy(), namespace + "." + name)

    def table_exists(self, namespace: String, name: String) raises -> Bool:
        try:
            _ = self.load_table(namespace, name)
            return True
        except:
            return False

    def list_tables(self, namespace: String) raises -> List[String]:
        if namespace != "" and not self.namespace_exists(namespace):
            raise Error("iceberg: namespace does not exist: " + namespace)
        var args: List[String] = [self.name, namespace]
        var rows = self.driver.query(
            (
                "SELECT table_name FROM iceberg_tables WHERE catalog_name=? AND"
                " table_namespace=?"
            ),
            args,
        )
        var out = List[String]()
        for k in range(len(rows)):
            out.append(_cell(rows[k], 0))
        return out^

    def create_table(
        mut self,
        namespace: String,
        name: String,
        schema: Schema,
        spec: PartitionSpec = PartitionSpec.unpartitioned(),
        var properties: Dict[String, String] = Dict[String, String](),
        format_version: Int = 2,
        location: String = String(""),
    ) raises -> Table:
        """Create an empty table: one `00000-<uuid>.metadata.json`, no
        snapshot, and one row in `iceberg_tables`."""
        if not self.namespace_exists(namespace):
            raise Error("iceberg: namespace does not exist: " + namespace)
        if self.table_exists(namespace, name):
            raise Error(
                "iceberg: table already exists: " + namespace + "." + name
            )
        var loc = location if location != "" else self.table_location(
            namespace, name
        )
        var m = new_table_metadata(
            loc, schema, spec, properties^, format_version
        )
        var path = join_path(join_path(loc, "metadata"), metadata_file_name(0))
        self.io.write_new(path, m.to_json().as_bytes())
        m.metadata_file_location = path

        var args: List[String] = [self.name, namespace, name, path]
        try:
            _ = self.driver.execute(
                (
                    "INSERT INTO iceberg_tables (catalog_name, table_namespace,"
                    " table_name, metadata_location,"
                    " previous_metadata_location) VALUES (?,?,?,?,NULL)"
                ),
                args,
            )
        except e:
            # The metadata file was written before the row; a refused row
            # leaves it orphaned, so take it back out.
            try:
                self.io.delete(path)
            except:
                pass
            if is_unique_violation(e):
                raise Error(
                    "iceberg: table already exists: " + namespace + "." + name
                )
            raise e

        return Table(m^, path^, self.io.copy(), namespace + "." + name)

    def drop_table(mut self, namespace: String, name: String) raises:
        """Remove the catalog row. Matches PyIceberg's `SqlCatalog`: the data
        and metadata files are left in place — use `purge_table` semantics at
        a higher layer if the files themselves need to go."""
        var args: List[String] = [self.name, namespace, name]
        var affected = self.driver.execute(
            (
                "DELETE FROM iceberg_tables WHERE catalog_name=? AND"
                " table_namespace=? AND table_name=?"
            ),
            args,
        )
        if affected < 1:
            raise Error(
                "iceberg: table does not exist: " + namespace + "." + name
            )

    def rename_table(
        mut self,
        from_namespace: String,
        from_name: String,
        to_namespace: String,
        to_name: String,
    ) raises -> Table:
        if not self.namespace_exists(to_namespace):
            raise Error("iceberg: namespace does not exist: " + to_namespace)
        var args: List[String] = [
            to_namespace,
            to_name,
            self.name,
            from_namespace,
            from_name,
        ]
        var affected: Int
        try:
            affected = self.driver.execute(
                (
                    "UPDATE iceberg_tables SET table_namespace=?, table_name=?"
                    " WHERE catalog_name=? AND table_namespace=? AND"
                    " table_name=?"
                ),
                args,
            )
        except e:
            if is_unique_violation(e):
                raise Error(
                    "iceberg: table already exists: "
                    + to_namespace
                    + "."
                    + to_name
                )
            raise e
        if affected < 1:
            raise Error(
                "iceberg: table does not exist: "
                + from_namespace
                + "."
                + from_name
            )
        return self.load_table(to_namespace, to_name)

    # ── the commit path ──────────────────────────────────────────────────────

    def _guarded_swap(
        mut self,
        namespace: String,
        name: String,
        old_location: String,
        new_location: String,
    ) raises -> Bool:
        """The atomic pointer swap: `UPDATE ... WHERE metadata_location =
        <old_location>`. `old_location` is the value this attempt's read saw,
        so zero rows affected means another commit already moved it — the
        same guard PyIceberg's `SqlCatalog.commit_table` uses."""
        var args: List[String] = [
            new_location,
            old_location,
            self.name,
            namespace,
            name,
            old_location,
        ]
        return (
            self.driver.execute(
                (
                    "UPDATE iceberg_tables SET metadata_location=?,"
                    " previous_metadata_location=? WHERE catalog_name=? AND"
                    " table_namespace=? AND table_name=? AND"
                    " metadata_location=?"
                ),
                args,
            )
            >= 1
        )

    def commit_append(
        mut self,
        namespace: String,
        name: String,
        data_files: List[DataFile],
        retries: Int = 4,
    ) raises -> Table:
        """Commit already-written data files as one append snapshot.

        Each attempt writes a fresh metadata JSON file (and, inside
        `prepare_append`, a fresh manifest and manifest list); a refused
        attempt's files are orphaned, exactly as in every other
        implementation here, and are nobody's to read.
        """
        var loaded = self.load_table(namespace, name)
        var attempt = 0
        while True:
            var result = prepare_append(
                loaded.io,
                loaded.metadata,
                data_files.copy(),
                Dict[String, String](),
            )
            var version = next_metadata_version(result.metadata)
            var new_path = join_path(
                dirname(loaded.metadata_location), metadata_file_name(version)
            )
            self.io.write_new(new_path, result.metadata.to_json().as_bytes())
            if self._guarded_swap(
                namespace, name, loaded.metadata_location, new_path
            ):
                var out = loaded.copy()
                out.metadata = result.metadata.copy()
                out.metadata.metadata_file_location = new_path
                out.metadata_location = new_path
                return out^
            try:
                self.io.delete(new_path)
            except:
                pass
            attempt += 1
            if attempt > retries:
                raise Error(
                    "iceberg: commit failed — table has been updated by"
                    " another process: "
                    + namespace
                    + "."
                    + name
                )
            loaded = self.load_table(namespace, name)

    def append(
        mut self,
        namespace: String,
        name: String,
        batches: List[RecordBatch],
        retries: Int = 4,
    ) raises -> Table:
        """Write `batches` into the table's location, then commit them."""
        var loaded = self.load_table(namespace, name)
        var options = WriteOptions.from_properties(loaded.metadata.properties)
        var files = write_data_files(
            loaded.io,
            loaded.metadata.location,
            batches,
            loaded.metadata.schema(),
            loaded.metadata.spec(),
            loaded.metadata.default_sort_order_id,
            options,
        )
        return self.commit_append(namespace, name, files, retries)

    def commit_change(
        mut self,
        namespace: String,
        name: String,
        kind: Int,
        filter_dsl: String,
        batches: List[RecordBatch],
        mode: String,
        retries: Int = 4,
    ) raises -> Table:
        """A delete or an overwrite, committed through the same guarded swap
        an append uses — the plan is entirely in the manifest list, so there
        is no per-operation update to the catalog row."""
        var loaded = self.load_table(namespace, name)
        var attempt = 0
        while True:
            var result: AppendResult
            if kind == COMMIT_DELETE:
                result = prepare_delete(
                    loaded.io, loaded.metadata, filter_dsl, mode
                )
            elif kind == COMMIT_OVERWRITE:
                result = prepare_overwrite(
                    loaded.io, loaded.metadata, batches, filter_dsl
                )
            else:
                result = prepare_dynamic_partition_overwrite(
                    loaded.io, loaded.metadata, batches
                )
            var version = next_metadata_version(result.metadata)
            var new_path = join_path(
                dirname(loaded.metadata_location), metadata_file_name(version)
            )
            self.io.write_new(new_path, result.metadata.to_json().as_bytes())
            if self._guarded_swap(
                namespace, name, loaded.metadata_location, new_path
            ):
                var out = loaded.copy()
                out.metadata = result.metadata.copy()
                out.metadata.metadata_file_location = new_path
                out.metadata_location = new_path
                return out^
            try:
                self.io.delete(new_path)
            except:
                pass
            attempt += 1
            if attempt > retries:
                raise Error(
                    "iceberg: commit failed — table has been updated by"
                    " another process: "
                    + namespace
                    + "."
                    + name
                )
            loaded = self.load_table(namespace, name)

    def delete_where(
        mut self,
        namespace: String,
        name: String,
        filter_dsl: String,
        mode: String = String(""),
        retries: Int = 4,
    ) raises -> Table:
        """`DELETE FROM t WHERE <filter>`, committed through the catalog."""
        return self.commit_change(
            namespace,
            name,
            COMMIT_DELETE,
            filter_dsl,
            List[RecordBatch](),
            mode,
            retries,
        )

    def overwrite(
        mut self,
        namespace: String,
        name: String,
        batches: List[RecordBatch],
        filter_dsl: String = String('["true"]'),
        retries: Int = 4,
    ) raises -> Table:
        """Delete what the filter matches and add `batches`, in one snapshot."""
        return self.commit_change(
            namespace,
            name,
            COMMIT_OVERWRITE,
            filter_dsl,
            batches,
            String(""),
            retries,
        )

    def dynamic_partition_overwrite(
        mut self,
        namespace: String,
        name: String,
        batches: List[RecordBatch],
        retries: Int = 4,
    ) raises -> Table:
        """Replace exactly the partitions the new rows land in."""
        return self.commit_change(
            namespace,
            name,
            COMMIT_DYNAMIC,
            String('["true"]'),
            batches,
            String(""),
            retries,
        )
