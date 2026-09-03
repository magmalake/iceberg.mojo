"""`iceberg-mojo` — inspect an Iceberg table from the command line.

    iceberg-mojo describe  <metadata.json | table-dir>
    iceberg-mojo schema    <metadata.json | table-dir>
    iceberg-mojo snapshots <metadata.json | table-dir>
    iceberg-mojo cat       <metadata.json | table-dir> [--limit N]
                                                       [--format csv|json]
    iceberg-mojo files     <metadata.json | table-dir> [--snapshot ID]
                                                       [--ref NAME]
                                                       [--as-of MS]
                                                       [--filter DSL]
                                                       [--select a,b,c]
                                                       [--rebase FROM=TO]

`files` prints the plan-files JSON, which is the same shape iceberg-rs.mojo's
`ib_scan_plan_files_json` emits, so the two can be diffed directly.

`cat` reads the rows themselves — deletes applied, filter evaluated,
projection resolved by field id — and prints them as CSV or JSON.

A location can be a local path, a `file://` or `s3://` URI (S3 credentials
come from `AWS_*` in the environment, or from `--property`), or a table in a
REST catalog: `--rest https://host --table db.orders`.

`--rebase` redirects locations that were written absolute: the metadata of a
table copied out of its original warehouse still names the old path, and
`--rebase file:///old/root=/new/root` points the reader at where the files
actually are.
"""

from std.sys import argv

from iceberg.catalog.filesystem import Table, find_latest_metadata
from iceberg.catalog.rest import RestCatalog, RestCatalogConfig
from iceberg.catalog.sql import SqlCatalog
from iceberg.io import FileIO
from iceberg.json import substr
from iceberg.metadata import TableMetadata
from iceberg.read import ScanOptions
from iceberg.scan import TableScan


comptime USAGE = String(
    "iceberg-mojo — native Apache Iceberg for Mojo\n"
    "\n"
    "usage:\n"
    "  iceberg-mojo describe  <table>\n"
    "  iceberg-mojo schema    <table>\n"
    "  iceberg-mojo snapshots <table>\n"
    "  iceberg-mojo files     <table> [options]\n"
    "  iceberg-mojo cat       <table> [options]\n"
    "\n"
    "<table> is a metadata.json, a table directory, a file:// or s3:// URI,\n"
    "or — with --rest and --table — a table in a REST catalog.\n"
    "\n"
    "options:\n"
    "  --snapshot ID     use this snapshot instead of the current one\n"
    "  --ref NAME        use the head of a branch or tag\n"
    "  --as-of MS        use the snapshot current at a millisecond timestamp\n"
    "  --filter DSL      a JSON S-expression row filter, e.g.\n"
    '                    \'["and",[">","id",2],["=","region","eu"]]\'\n'
    "  --select a,b,c    project these columns; `cat` also accepts the\n"
    "                    metadata columns _file, _pos, _spec_id, _partition,\n"
    "                    _row_id and _last_updated_sequence_number\n"
    "  --limit N         stop after N rows (`cat`)\n"
    "  --format csv|json output format (`cat`, default csv)\n"
    "  --lazy            fetch only the footer and the row groups needed\n"
    "  --rebase FROM=TO  rewrite location prefixes before opening files\n"
    "  --property K=V    a storage property, e.g. s3.endpoint or s3.region\n"
    "  --rest URL        load the table from a REST catalog at URL\n"
    "  --sql URI         load the table from a SQL catalog, e.g.\n"
    "                    sqlite:///catalog.db (local dev / PyIceberg parity)\n"
    "                    or postgresql://user@host/db (a catalog several\n"
    "                    writers can share) — see catalog/sql.mojo\n"
    "  --table NS.NAME   the table to load from that catalog\n"
    "  --token T         bearer token for the REST catalog\n"
    "  --warehouse W     warehouse root for --sql, or to ask the REST\n"
    "                    catalog for\n"
    "  --no-vend         do not ask the catalog to vend credentials\n"
)


def split_commas(s: String) -> List[String]:
    var out = List[String]()
    if s == "":
        return out^
    var start = 0
    while True:
        var c = s.find(",", start)
        if c < 0:
            out.append(substr(s, start, s.byte_length()))
            break
        out.append(substr(s, start, c))
        start = c + 1
    return out^


def main() raises:
    var args = argv()
    if len(args) < 2:
        print(USAGE)
        return
    var command = String(args[1])
    var location = String("")
    var first_option = 2
    if len(args) > 2 and not String(args[2]).startswith("--"):
        location = String(args[2])
        first_option = 3

    var io = FileIO.local()
    var filter_dsl = String('["true"]')
    var selected = List[String]()
    var snapshot_id: Int64 = 0
    var has_snapshot = False
    var ref_name = String("")
    var as_of: Int64 = 0
    var has_as_of = False
    var limit = -1
    var format = String("csv")
    var lazy = False
    var rest_uri = String("")
    var rest_table = String("")
    var token = String("")
    var warehouse = String("")
    var vend = True
    var sql_uri = String("")

    var k = first_option
    while k < len(args):
        var a = String(args[k])
        if a == "--filter" and k + 1 < len(args):
            filter_dsl = String(args[k + 1])
            k += 2
        elif a == "--select" and k + 1 < len(args):
            selected = split_commas(String(args[k + 1]))
            k += 2
        elif a == "--snapshot" and k + 1 < len(args):
            snapshot_id = Int64(Int(String(args[k + 1])))
            has_snapshot = True
            k += 2
        elif a == "--ref" and k + 1 < len(args):
            ref_name = String(args[k + 1])
            k += 2
        elif a == "--as-of" and k + 1 < len(args):
            as_of = Int64(Int(String(args[k + 1])))
            has_as_of = True
            k += 2
        elif a == "--rebase" and k + 1 < len(args):
            var spec = String(args[k + 1])
            var eq = spec.find("=")
            if eq < 0:
                raise Error("--rebase needs FROM=TO")
            io.rebase(
                substr(spec, 0, eq), substr(spec, eq + 1, spec.byte_length())
            )
            k += 2
        elif a == "--limit" and k + 1 < len(args):
            limit = Int(String(args[k + 1]))
            k += 2
        elif a == "--format" and k + 1 < len(args):
            format = String(args[k + 1])
            k += 2
        elif a == "--lazy":
            lazy = True
            k += 1
        elif a == "--property" and k + 1 < len(args):
            var kv = String(args[k + 1])
            var eq = kv.find("=")
            if eq < 0:
                raise Error("--property needs K=V")
            io.set(substr(kv, 0, eq), substr(kv, eq + 1, kv.byte_length()))
            k += 2
        elif a == "--rest" and k + 1 < len(args):
            rest_uri = String(args[k + 1])
            k += 2
        elif a == "--sql" and k + 1 < len(args):
            sql_uri = String(args[k + 1])
            k += 2
        elif a == "--table" and k + 1 < len(args):
            rest_table = String(args[k + 1])
            k += 2
        elif a == "--token" and k + 1 < len(args):
            token = String(args[k + 1])
            k += 2
        elif a == "--warehouse" and k + 1 < len(args):
            warehouse = String(args[k + 1])
            k += 2
        elif a == "--no-vend":
            vend = False
            k += 1
        else:
            raise Error("unknown option '" + a + "'")

    var table = _open(
        location, rest_uri, rest_table, token, warehouse, vend, sql_uri, io^
    )
    ref m = table.metadata

    if command == "describe":
        print("location:        ", m.location)
        print("metadata:        ", table.metadata_location)
        print("format-version:  ", m.format_version)
        print("table-uuid:      ", m.table_uuid)
        print("last-updated-ms: ", m.last_updated_ms)
        print("last-sequence:   ", m.last_sequence_number)
        print("last-column-id:  ", m.last_column_id)
        print(
            "schemas:         ",
            len(m.schemas),
            "(current",
            String(m.current_schema_id) + ")",
        )
        print(
            "partition-specs: ",
            len(m.partition_specs),
            "(default",
            String(m.default_spec_id) + ")",
        )
        print(
            "sort-orders:     ",
            len(m.sort_orders),
            "(default",
            String(m.default_sort_order_id) + ")",
        )
        print("snapshots:       ", len(m.snapshots))
        if m.has_current_snapshot:
            print("current-snapshot:", m.current_snapshot_id)
        else:
            print("current-snapshot: none")
        if len(m.refs) > 0:
            var names = String("")
            for j in range(len(m.refs)):
                if j > 0:
                    names += ", "
                names += m.refs[j].name + " (" + m.refs[j].type + ")"
            print("refs:            ", names)
        if m.has_next_row_id:
            print("next-row-id:     ", m.next_row_id)
        print("properties:      ", len(m.properties))
        for e in m.properties.items():
            print("   ", e.key, "=", e.value)
        print("default spec:    ", m.spec().to_json())
    elif command == "schema":
        print(m.schema().to_json())
    elif command == "snapshots":
        print("[")
        for j in range(len(m.snapshots)):
            var sep = "," if j + 1 < len(m.snapshots) else ""
            print(" " + m.snapshots[j].to_json() + sep)
        print("]")
    elif command == "cat":
        var scan = table.scan().filter(filter_dsl)
        if len(selected) > 0:
            scan = scan.select(selected^)
        if has_snapshot:
            scan = scan.use_snapshot(snapshot_id)
        elif ref_name != "":
            scan = scan.use_ref(ref_name)
        elif has_as_of:
            scan = scan.as_of(as_of)
        var options = ScanOptions()
        options.limit = limit
        options.lazy = lazy
        var rows = scan.to_table(options)
        if format == "json":
            print(rows.to_json())
        elif format == "csv":
            print(rows.to_csv(), end="")
        else:
            raise Error("unknown --format '" + format + "'; use csv or json")
    elif command == "files":
        var scan = table.scan().filter(filter_dsl)
        if len(selected) > 0:
            scan = scan.select(selected^)
        if has_snapshot:
            scan = scan.use_snapshot(snapshot_id)
        elif ref_name != "":
            scan = scan.use_ref(ref_name)
        elif has_as_of:
            scan = scan.as_of(as_of)
        print(scan.plan_files_json())
    else:
        print(USAGE)
        raise Error("unknown command '" + command + "'")


def _open(
    location: String,
    rest_uri: String,
    rest_table: String,
    token: String,
    warehouse: String,
    vend: Bool,
    sql_uri: String,
    var io: FileIO,
) raises -> Table:
    """A table from a REST or SQL catalog when one was named, from a path
    otherwise."""
    if rest_uri != "":
        if rest_table == "":
            raise Error("--rest needs --table NS.NAME")
        var dot = rest_table.rfind(".")
        if dot < 0:
            raise Error("--table wants a dotted NAMESPACE.NAME")
        var config = RestCatalogConfig(rest_uri)
        if token != "":
            config.with_token(token)
        if warehouse != "":
            config.with_warehouse(warehouse)
        config.vend_credentials = vend
        var catalog = RestCatalog(config^, io^)
        catalog.connect()
        return catalog.load_table(
            substr(rest_table, 0, dot),
            substr(rest_table, dot + 1, rest_table.byte_length()),
        )
    if sql_uri != "":
        if rest_table == "":
            raise Error("--sql needs --table NS.NAME")
        var dot = rest_table.rfind(".")
        if dot < 0:
            raise Error("--table wants a dotted NAMESPACE.NAME")
        var catalog = SqlCatalog(
            "default", sql_uri, warehouse, io^, create_tables=False
        )
        return catalog.load_table(
            substr(rest_table, 0, dot),
            substr(rest_table, dot + 1, rest_table.byte_length()),
        )
    if location == "":
        raise Error(
            "no table given: pass a path, --rest URL --table NS.T, or --sql"
            " URI --table NS.T"
        )
    return Table.load(location, io^)
