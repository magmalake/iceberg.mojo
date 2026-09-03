"""The two databases `SqlCatalog` speaks to, behind one interface.

The catalog's SQL is ordinary: `SELECT`, `INSERT`, `UPDATE` and `DELETE` over
two tables whose DDL is PyIceberg's, with every value bound as text. What
differs between sqlite.mojo and postgres.mojo is not the SQL but the shape of
the client — sqlite compiles a `Statement` and binds `?` by position while
libpq takes `$1` and a parallel value array; sqlite's "rows affected" is
`sqlite3_changes` while libpq's is the command tag — and that is all this
module exists to flatten.

    var driver = SqlDriver("postgresql://user@host/warehouse")
    var n = driver.execute(
        "DELETE FROM iceberg_tables WHERE catalog_name=?", [name]
    )
    var rows = driver.query(
        "SELECT table_name FROM iceberg_tables WHERE catalog_name=?", [name]
    )

`execute` returns the rows the statement affected, which is what the catalog's
optimistic-concurrency guard reads: an `UPDATE ... WHERE metadata_location =
<what this attempt saw>` that affects zero rows lost the race. `query` returns
every cell as `Optional[String]` — text is what both backends hand back and
what every column in this schema holds, and `None` keeps SQL NULL distinct
from the empty string, which matters because `metadata_location` is nullable
and PyIceberg writes NULL into `previous_metadata_location`.

The URI picks the backend, exactly as it does for PyIceberg's `SqlCatalog`:
`sqlite:///catalog.db` (or a bare path, or `:memory:`) opens sqlite,
`postgresql://…` / `postgres://…` is handed to libpq unchanged, so the same
string works for SQLAlchemy's `create_engine` and for this.

Two things are not abstracted, because they cannot be. Placeholders are
rewritten here — `_placeholders` turns the `?`s the catalog writes into
`$1, $2, …` for libpq, skipping anything inside a single-quoted literal — and
a duplicate primary key is recognised by `is_unique_violation`, which reads
PostgreSQL's SQLSTATE ``23505`` out of the message postgres.mojo formats and
matches SQLite's own ``UNIQUE constraint failed`` text otherwise.
"""

from std.memory import ArcPointer

import postgres
import sqlite

from ..json import substr


comptime DRIVER_SQLITE = 0
"""`SqlDriver.kind` for a sqlite.mojo `Database`."""
comptime DRIVER_POSTGRES = 1
"""`SqlDriver.kind` for a postgres.mojo `Connection`."""

comptime _POSTGRES_PREFIX: StaticString = "postgresql://"
comptime _POSTGRES_SHORT_PREFIX: StaticString = "postgres://"
comptime _SQLITE_PREFIX: StaticString = "sqlite:///"


def sqlite_path_from_uri(uri: String) -> String:
    """`sqlite:///rel.db` -> `rel.db`; `sqlite:////abs.db` -> `/abs.db`.

    Matches the two forms SQLAlchemy's sqlite dialect accepts (three slashes
    for a path relative to the process, four for an absolute one) so the same
    `uri` property string works for both PyIceberg's `SqlCatalog` and this
    one, and the same catalog file is what "PyIceberg reads what we write"
    parity needs. Anything without the `sqlite:///` prefix — including the
    literal `:memory:` — is passed straight through to `sqlite3_open`.

    Args:
        uri: The catalog URI.

    Returns:
        The path `sqlite3_open` wants.
    """
    if uri.startswith(String(_SQLITE_PREFIX)):
        return substr(uri, _SQLITE_PREFIX.byte_length(), uri.byte_length())
    return uri


def is_postgres_uri(uri: String) -> Bool:
    """Whether `uri` names a PostgreSQL server rather than a sqlite file.

    Args:
        uri: The catalog URI.

    Returns:
        True for `postgresql://…` and `postgres://…`, the two schemes libpq
        and SQLAlchemy both accept.
    """
    return uri.startswith(String(_POSTGRES_PREFIX)) or uri.startswith(
        String(_POSTGRES_SHORT_PREFIX)
    )


def _placeholders(sql: String) -> String:
    """Rewrite positional `?` placeholders as libpq's `$1`, `$2`, ….

    Single-quoted literals are skipped, so the `'!'` in
    ``LIKE ? ESCAPE '!'`` — and any future literal that happens to contain a
    question mark — is left alone. Doubled quotes (`''`) inside a literal are
    handled the way SQL defines them: the second quote re-opens nothing, it is
    an escaped quote, and the scan stays inside the literal.

    Args:
        sql: A statement written with `?` placeholders.

    Returns:
        The same statement with `$n` placeholders, numbered from 1 in the
        order the `?`s appeared.
    """
    var out = String("")
    var n = 0
    var in_literal = False
    var bytes = sql.as_bytes()
    var i = 0
    comptime QUOTE = UInt8(ord("'"))
    comptime QUESTION = UInt8(ord("?"))
    while i < len(bytes):
        var b = bytes[i]
        if in_literal:
            out += String(sql[byte=i])
            if b == QUOTE:
                # `''` is an escaped quote, not the end of the literal.
                if i + 1 < len(bytes) and bytes[i + 1] == QUOTE:
                    out += "'"
                    i += 2
                    continue
                in_literal = False
            i += 1
            continue
        if b == QUOTE:
            in_literal = True
            out += "'"
        elif b == QUESTION:
            n += 1
            out += "$" + String(n)
        else:
            out += String(sql[byte=i])
        i += 1
    return out^


def is_unique_violation(err: Error) -> Bool:
    """Whether `err` is a primary-key/unique-index conflict.

    The one server error this catalog acts on rather than propagates: a second
    `INSERT INTO iceberg_tables` for a name that is already there, or a
    `rename_table` onto an occupied one, is "the table already exists", not a
    failure of the database.

    Args:
        err: The error a `SqlDriver` call raised.

    Returns:
        True for PostgreSQL's SQLSTATE ``23505``, which postgres.mojo formats
        into the message and `postgres.sqlstate_of` reads back out, and for
        SQLite's ``UNIQUE constraint failed`` / ``PRIMARY KEY must be
        unique``, which sqlite.mojo passes through from `sqlite3_errmsg`.
    """
    if postgres.sqlstate_of(err) == String(postgres.UNIQUE_VIOLATION):
        return True
    var message = String(err)
    return (
        message.find("UNIQUE constraint failed") >= 0
        or message.find("PRIMARY KEY must be unique") >= 0
    )


struct SqlDriver(Movable):
    """One open database — sqlite or PostgreSQL — under one small interface.

    Built from the catalog URI; `SqlDriver.kind` says which backend answered.
    Every method takes text parameters and hands back text cells, because that
    is the whole of the catalog schema: nine `VARCHAR` columns across the two
    tables, two of them nullable.

    ```mojo
    var driver = SqlDriver("sqlite:///catalog.db")
    _ = driver.execute("INSERT INTO iceberg_tables VALUES (?,?,?,?,NULL)",
                       [name, ns, table, location])
    ```

    The PostgreSQL connection is held through an `std.memory.ArcPointer`, not
    directly: postgres.mojo's `Connection.query` takes `mut self` while the
    catalog's readers (`load_table`, `list_tables`, `namespace_exists`) take an
    immutable `self`, and the `ArcPointer`'s interior mutability is what lets a
    read stay a read. sqlite.mojo's `Database` already takes `self` on every
    call, so it is stored inline.
    """

    var kind: Int
    """`DRIVER_SQLITE` or `DRIVER_POSTGRES`."""
    var _sqlite: Optional[sqlite.Database]
    """The sqlite connection, when `kind` is `DRIVER_SQLITE`."""
    var _pg: Optional[ArcPointer[postgres.Connection]]
    """The libpq connection, when `kind` is `DRIVER_POSTGRES`."""

    def __init__(out self, uri: String) raises:
        """Open the database `uri` names.

        Args:
            uri: `postgresql://…` or `postgres://…` for a server — passed to
                libpq unchanged, so every conninfo option (`sslmode`,
                `connect_timeout`, …) works — and anything else for sqlite,
                through `sqlite_path_from_uri`.

        Raises:
            Error: If the database could not be opened; for PostgreSQL that is
                a `postgres.PostgresError` with SQLSTATE ``08001`` carrying
                libpq's own explanation.
        """
        if is_postgres_uri(uri):
            self.kind = DRIVER_POSTGRES
            self._sqlite = None
            self._pg = ArcPointer(postgres.Connection(uri))
        else:
            self.kind = DRIVER_SQLITE
            self._sqlite = sqlite.Database(sqlite_path_from_uri(uri))
            self._pg = None

    def is_postgres(self) -> Bool:
        """Whether this driver is talking to a PostgreSQL server.

        Returns:
            True for `DRIVER_POSTGRES`.
        """
        return self.kind == DRIVER_POSTGRES

    def execute(
        mut self, sql: String, params: List[String] = List[String]()
    ) raises -> Int:
        """Run one statement and report how many rows it changed.

        Args:
            sql: A single statement, with `?` placeholders — rewritten to
                `$n` on the PostgreSQL path.
            params: One text value per placeholder, in order.

        Returns:
            The rows the statement inserted, updated or deleted:
            `sqlite3_changes` on the sqlite path, libpq's command tag on the
            PostgreSQL one. DDL reports 0 on both.

        Raises:
            Error: If the server or the engine rejected the statement.
        """
        if self.kind == DRIVER_POSTGRES:
            var p = postgres.Params()
            for k in range(len(params)):
                p = p^.text(params[k])
            return self._pg.value()[].execute(_placeholders(sql), p^)
        ref db = self._sqlite.value()
        var stmt = db.prepare(sql)
        for k in range(len(params)):
            stmt.bind_text(k + 1, params[k])
        _ = stmt.step()
        return db.changes()

    def query(
        self, sql: String, params: List[String] = List[String]()
    ) raises -> List[List[Optional[String]]]:
        """Run one statement and return every row it produced.

        Args:
            sql: A single statement, with `?` placeholders — rewritten to
                `$n` on the PostgreSQL path.
            params: One text value per placeholder, in order.

        Returns:
            One `List[Optional[String]]` per row, one entry per selected
            column, in the order the statement named them. `None` is SQL NULL
            — distinct from `""`, which is an empty string that was really
            stored.

        Raises:
            Error: If the server or the engine rejected the statement.
        """
        var out = List[List[Optional[String]]]()
        if self.kind == DRIVER_POSTGRES:
            var p = postgres.Params()
            for k in range(len(params)):
                p = p^.text(params[k])
            var res = self._pg.value()[].query(_placeholders(sql), p^)
            for r in range(res.num_rows()):
                var row = List[Optional[String]](capacity=res.num_cols())
                for c in range(res.num_cols()):
                    if res.is_null(r, c):
                        row.append(None)
                    else:
                        row.append(Optional[String](res.text(r, c)))
                out.append(row^)
            return out^
        ref db = self._sqlite.value()
        var stmt = db.prepare(sql)
        for k in range(len(params)):
            stmt.bind_text(k + 1, params[k])
        while True:
            var stepped = stmt.step()
            if not stepped:
                break
            ref got = stepped.value()
            var row = List[Optional[String]](capacity=got.num_cols())
            for c in range(got.num_cols()):
                if got.is_null(c):
                    row.append(None)
                else:
                    row.append(Optional[String](got.text_val(c)))
            out.append(row^)
        return out^

    def begin(mut self) raises:
        """Open a transaction block.

        Plain `BEGIN` on both backends rather than either client's RAII guard:
        the catalog's blocks end in an explicit `SqlDriver.commit` or, on any
        raise, an explicit `SqlDriver.rollback`, and a guard whose destructor
        also rolls back would have to be threaded through the catalog's own
        `try`/`except` to say which happened.

        Raises:
            Error: If a transaction is already open, or the connection is
                gone.
        """
        _ = self.execute("BEGIN")

    def commit(mut self) raises:
        """Commit the open transaction block.

        Raises:
            Error: If the commit failed.
        """
        _ = self.execute("COMMIT")

    def rollback(mut self) raises:
        """Roll the open transaction block back.

        Accepted in every state on both backends, including PostgreSQL's
        failed-block state where nothing else is.

        Raises:
            Error: If the rollback failed, which in practice means the
                connection has gone.
        """
        _ = self.execute("ROLLBACK")
