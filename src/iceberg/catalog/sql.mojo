"""A JDBC-style SQL catalog, backed by sqlite.mojo — the schema and semantics
PyIceberg's `SqlCatalog` uses.

This is **not** how production Iceberg deployments connect: real catalogs are
`RestCatalog` or a metastore. What a SQL catalog buys is local development and
test parity with PyIceberg, whose quickstart default is exactly this —
a SQLite-backed `SqlCatalog` — and which is what iceberg-rs.mojo's own
fixtures already use as an oracle. A database this catalog creates opens
cleanly in PyIceberg's `SqlCatalog` and vice versa: same two tables, same
column names, same optimistic-concurrency guard.

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
from sqlite import Database

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
from ..json import substr
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


def sqlite_path_from_uri(uri: String) -> String:
    """`sqlite:///rel.db` -> `rel.db`; `sqlite:////abs.db` -> `/abs.db`.

    Matches the two forms SQLAlchemy's sqlite dialect accepts (three slashes
    for a path relative to the process, four for an absolute one) so the same
    `uri` property string works for both PyIceberg's `SqlCatalog` and this
    one, and the same catalog file is what "PyIceberg reads what we write"
    parity needs. Anything without the `sqlite:///` prefix — including the
    literal `:memory:` — is passed straight through to `sqlite3_open`.
    """
    comptime PREFIX = String("sqlite:///")
    if uri.startswith(PREFIX):
        return substr(uri, PREFIX.byte_length(), uri.byte_length())
    return uri


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
    database, table data and metadata JSON live under `warehouse` — the same
    split PyIceberg's `SqlCatalog` makes.

    ```mojo
    var cat = SqlCatalog.local("default", "sqlite:///catalog.db", "warehouse")
    cat.create_namespace("db")
    var t = cat.create_table("db", "orders", schema)
    _ = cat.append("db", "orders", batches)
    ```
    """

    var name: String
    var warehouse: String
    var io: FileIO
    var db: Database

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
        self.db = Database(sqlite_path_from_uri(uri))
        if create_tables:
            self.db.execute(_CREATE_TABLES_SQL)
            self.db.execute(_CREATE_NAMESPACE_PROPERTIES_SQL)

    @staticmethod
    def local(var name: String, uri: String, var warehouse: String) raises -> Self:
        return Self(name^, uri, warehouse^, FileIO.local())

    def create_tables(mut self) raises:
        """`CREATE TABLE IF NOT EXISTS` for both catalog tables. Idempotent;
        opening a database another SQL catalog (ours or PyIceberg's) already
        initialized is a no-op."""
        self.db.execute(_CREATE_TABLES_SQL)
        self.db.execute(_CREATE_NAMESPACE_PROPERTIES_SQL)

    def destroy_tables(mut self) raises:
        """Drop both catalog tables. Mirrors PyIceberg's `destroy_tables`."""
        self.db.execute("DROP TABLE IF EXISTS iceberg_tables")
        self.db.execute("DROP TABLE IF EXISTS iceberg_namespace_properties")

    def table_location(self, namespace: String, name: String) -> String:
        if namespace == "":
            return join_path(self.warehouse, name)
        return join_path(join_path(self.warehouse, namespace), name)

    def _changes(self) raises -> Int:
        """`SELECT changes()` — rows affected by the last INSERT/UPDATE/DELETE
        on this connection. sqlite.mojo has no `sqlite3_changes` binding, but
        SQLite exposes the same counter as an ordinary SQL scalar function, so
        no FFI addition is needed."""
        var stmt = self.db.prepare("SELECT changes()")
        var row = stmt.step()
        if row:
            return row.value().int_val(0)
        return 0

    # ── namespaces ───────────────────────────────────────────────────────────

    def namespace_exists(self, namespace: String) raises -> Bool:
        var pattern = _escape_like(namespace) + ".%"
        var t = self.db.prepare(
            "SELECT 1 FROM iceberg_tables WHERE catalog_name=? AND"
            " (table_namespace=? OR table_namespace LIKE ? ESCAPE '!') LIMIT 1"
        )
        t.bind_text(1, self.name)
        t.bind_text(2, namespace)
        t.bind_text(3, pattern)
        if t.step():
            return True
        var p = self.db.prepare(
            "SELECT 1 FROM iceberg_namespace_properties WHERE catalog_name=?"
            " AND (namespace=? OR namespace LIKE ? ESCAPE '!') LIMIT 1"
        )
        p.bind_text(1, self.name)
        p.bind_text(2, namespace)
        p.bind_text(3, pattern)
        if p.step():
            return True
        return False

    def create_namespace(
        mut self,
        namespace: String,
        var properties: Dict[String, String] = Dict[String, String](),
    ) raises:
        if self.namespace_exists(namespace):
            raise Error("iceberg: namespace already exists: " + namespace)
        if len(properties) == 0:
            properties[NAMESPACE_EXISTS_PROPERTY] = "true"
        with self.db.transaction():
            for entry in properties.items():
                var stmt = self.db.prepare(
                    "INSERT INTO iceberg_namespace_properties"
                    " (catalog_name, namespace, property_key, property_value)"
                    " VALUES (?,?,?,?)"
                )
                stmt.bind_text(1, self.name)
                stmt.bind_text(2, namespace)
                stmt.bind_text(3, entry.key)
                stmt.bind_text(4, entry.value)
                _ = stmt.step()

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
        var stmt = self.db.prepare(
            "DELETE FROM iceberg_namespace_properties WHERE catalog_name=?"
            " AND namespace=?"
        )
        stmt.bind_text(1, self.name)
        stmt.bind_text(2, namespace)
        _ = stmt.step()

    def list_namespaces(self, parent: String = "") raises -> List[String]:
        if parent != "" and not self.namespace_exists(parent):
            raise Error("iceberg: namespace does not exist: " + parent)
        var parent_parts = _namespace_parts(parent)
        var need_len = len(parent_parts) + 1
        var pattern = parent + ".%" if parent != "" else String("")

        var candidates = List[String]()
        var sql1 = String(
            "SELECT DISTINCT table_namespace FROM iceberg_tables WHERE"
            " catalog_name=?"
        )
        if pattern != "":
            sql1 += " AND table_namespace LIKE ?"
        var stmt1 = self.db.prepare(sql1)
        stmt1.bind_text(1, self.name)
        if pattern != "":
            stmt1.bind_text(2, pattern)
        while True:
            var row = stmt1.step()
            if not row:
                break
            candidates.append(row.value().text_val(0))

        var sql2 = String(
            "SELECT DISTINCT namespace FROM iceberg_namespace_properties"
            " WHERE catalog_name=?"
        )
        if pattern != "":
            sql2 += " AND namespace LIKE ?"
        var stmt2 = self.db.prepare(sql2)
        stmt2.bind_text(1, self.name)
        if pattern != "":
            stmt2.bind_text(2, pattern)
        while True:
            var row = stmt2.step()
            if not row:
                break
            candidates.append(row.value().text_val(0))

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

    def load_namespace_properties(self, namespace: String) raises -> Dict[String, String]:
        if not self.namespace_exists(namespace):
            raise Error("iceberg: namespace does not exist: " + namespace)
        var stmt = self.db.prepare(
            "SELECT property_key, property_value FROM"
            " iceberg_namespace_properties WHERE catalog_name=? AND"
            " namespace=?"
        )
        stmt.bind_text(1, self.name)
        stmt.bind_text(2, namespace)
        var out = Dict[String, String]()
        while True:
            var row = stmt.step()
            if not row:
                break
            out[row.value().text_val(0)] = row.value().text_val(1)
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

        with self.db.transaction():
            for k in range(len(removals)):
                var stmt = self.db.prepare(
                    "DELETE FROM iceberg_namespace_properties WHERE"
                    " catalog_name=? AND namespace=? AND property_key=?"
                )
                stmt.bind_text(1, self.name)
                stmt.bind_text(2, namespace)
                stmt.bind_text(3, removals[k])
                _ = stmt.step()
            for entry in updates.items():
                var del_stmt = self.db.prepare(
                    "DELETE FROM iceberg_namespace_properties WHERE"
                    " catalog_name=? AND namespace=? AND property_key=?"
                )
                del_stmt.bind_text(1, self.name)
                del_stmt.bind_text(2, namespace)
                del_stmt.bind_text(3, entry.key)
                _ = del_stmt.step()
                var ins_stmt = self.db.prepare(
                    "INSERT INTO iceberg_namespace_properties"
                    " (catalog_name, namespace, property_key, property_value)"
                    " VALUES (?,?,?,?)"
                )
                ins_stmt.bind_text(1, self.name)
                ins_stmt.bind_text(2, namespace)
                ins_stmt.bind_text(3, entry.key)
                ins_stmt.bind_text(4, entry.value)
                _ = ins_stmt.step()

        return NamespacePropertiesUpdateSummary(removed^, updated^, missing^)

    # ── tables ───────────────────────────────────────────────────────────────

    def load_table(self, namespace: String, name: String) raises -> Table:
        var stmt = self.db.prepare(
            "SELECT metadata_location FROM iceberg_tables WHERE"
            " catalog_name=? AND table_namespace=? AND table_name=?"
        )
        stmt.bind_text(1, self.name)
        stmt.bind_text(2, namespace)
        stmt.bind_text(3, name)
        var row = stmt.step()
        if not row:
            raise Error("iceberg: table does not exist: " + namespace + "." + name)
        var loc = row.value().text_val(0)
        if loc == "":
            raise Error("iceberg: table does not exist: " + namespace + "." + name)
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
        var stmt = self.db.prepare(
            "SELECT table_name FROM iceberg_tables WHERE catalog_name=? AND"
            " table_namespace=?"
        )
        stmt.bind_text(1, self.name)
        stmt.bind_text(2, namespace)
        var out = List[String]()
        while True:
            var row = stmt.step()
            if not row:
                break
            out.append(row.value().text_val(0))
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
            raise Error("iceberg: table already exists: " + namespace + "." + name)
        var loc = location if location != "" else self.table_location(namespace, name)
        var m = new_table_metadata(loc, schema, spec, properties^, format_version)
        var path = join_path(join_path(loc, "metadata"), metadata_file_name(0))
        self.io.write_new(path, m.to_json().as_bytes())
        m.metadata_file_location = path

        try:
            var stmt = self.db.prepare(
                "INSERT INTO iceberg_tables (catalog_name, table_namespace,"
                " table_name, metadata_location, previous_metadata_location)"
                " VALUES (?,?,?,?,NULL)"
            )
            stmt.bind_text(1, self.name)
            stmt.bind_text(2, namespace)
            stmt.bind_text(3, name)
            stmt.bind_text(4, path)
            _ = stmt.step()
        except:
            try:
                self.io.delete(path)
            except:
                pass
            raise Error("iceberg: table already exists: " + namespace + "." + name)

        return Table(m^, path^, self.io.copy(), namespace + "." + name)

    def drop_table(mut self, namespace: String, name: String) raises:
        """Remove the catalog row. Matches PyIceberg's `SqlCatalog`: the data
        and metadata files are left in place — use `purge_table` semantics at
        a higher layer if the files themselves need to go."""
        var stmt = self.db.prepare(
            "DELETE FROM iceberg_tables WHERE catalog_name=? AND"
            " table_namespace=? AND table_name=?"
        )
        stmt.bind_text(1, self.name)
        stmt.bind_text(2, namespace)
        stmt.bind_text(3, name)
        _ = stmt.step()
        if self._changes() < 1:
            raise Error("iceberg: table does not exist: " + namespace + "." + name)

    def rename_table(
        mut self,
        from_namespace: String,
        from_name: String,
        to_namespace: String,
        to_name: String,
    ) raises -> Table:
        if not self.namespace_exists(to_namespace):
            raise Error("iceberg: namespace does not exist: " + to_namespace)
        try:
            var stmt = self.db.prepare(
                "UPDATE iceberg_tables SET table_namespace=?, table_name=?"
                " WHERE catalog_name=? AND table_namespace=? AND table_name=?"
            )
            stmt.bind_text(1, to_namespace)
            stmt.bind_text(2, to_name)
            stmt.bind_text(3, self.name)
            stmt.bind_text(4, from_namespace)
            stmt.bind_text(5, from_name)
            _ = stmt.step()
        except:
            raise Error(
                "iceberg: table already exists: " + to_namespace + "." + to_name
            )
        if self._changes() < 1:
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
        var stmt = self.db.prepare(
            "UPDATE iceberg_tables SET metadata_location=?,"
            " previous_metadata_location=? WHERE catalog_name=? AND"
            " table_namespace=? AND table_name=? AND metadata_location=?"
        )
        stmt.bind_text(1, new_location)
        stmt.bind_text(2, old_location)
        stmt.bind_text(3, self.name)
        stmt.bind_text(4, namespace)
        stmt.bind_text(5, name)
        stmt.bind_text(6, old_location)
        _ = stmt.step()
        return self._changes() >= 1

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
                result = prepare_delete(loaded.io, loaded.metadata, filter_dsl, mode)
            elif kind == COMMIT_OVERWRITE:
                result = prepare_overwrite(loaded.io, loaded.metadata, batches, filter_dsl)
            else:
                result = prepare_dynamic_partition_overwrite(loaded.io, loaded.metadata, batches)
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
            namespace, name, COMMIT_DELETE, filter_dsl, List[RecordBatch](), mode, retries
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
            namespace, name, COMMIT_OVERWRITE, filter_dsl, batches, String(""), retries
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
