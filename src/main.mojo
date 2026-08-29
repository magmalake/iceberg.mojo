"""`iceberg-mojo` — inspect an Iceberg table from the command line.

    iceberg-mojo describe  <metadata.json | table-dir>
    iceberg-mojo schema    <metadata.json | table-dir>
    iceberg-mojo snapshots <metadata.json | table-dir>
    iceberg-mojo files     <metadata.json | table-dir> [--snapshot ID]
                                                       [--ref NAME]
                                                       [--as-of MS]
                                                       [--filter DSL]
                                                       [--select a,b,c]
                                                       [--rebase FROM=TO]

`files` prints the plan-files JSON, which is the same shape iceberg-rs.mojo's
`ib_scan_plan_files_json` emits, so the two can be diffed directly.

`--rebase` redirects locations that were written absolute: the metadata of a
table copied out of its original warehouse still names the old path, and
`--rebase file:///old/root=/new/root` points the reader at where the files
actually are.
"""

from std.sys import argv

from iceberg.catalog.filesystem import Table, find_latest_metadata
from iceberg.io import FileIO
from iceberg.json import substr
from iceberg.metadata import TableMetadata
from iceberg.scan import TableScan


comptime USAGE = String(
    "iceberg-mojo — native Apache Iceberg for Mojo\n"
    "\n"
    "usage:\n"
    "  iceberg-mojo describe  <metadata.json | table-dir>\n"
    "  iceberg-mojo schema    <metadata.json | table-dir>\n"
    "  iceberg-mojo snapshots <metadata.json | table-dir>\n"
    "  iceberg-mojo files     <metadata.json | table-dir> [options]\n"
    "\n"
    "options for `files`:\n"
    "  --snapshot ID     plan this snapshot instead of the current one\n"
    "  --ref NAME        plan the head of a branch or tag\n"
    "  --as-of MS        plan the snapshot current at a millisecond timestamp\n"
    "  --filter DSL      a JSON S-expression row filter, e.g.\n"
    '                    \'["and",[">","id",2],["=","region","eu"]]\'\n'
    "  --select a,b,c    project these columns\n"
    "  --rebase FROM=TO  rewrite location prefixes before opening files\n"
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
    if len(args) < 3:
        print(USAGE)
        return
    var command = String(args[1])
    var location = String(args[2])

    var io = FileIO.local()
    var filter_dsl = String('["true"]')
    var selected = List[String]()
    var snapshot_id: Int64 = 0
    var has_snapshot = False
    var ref_name = String("")
    var as_of: Int64 = 0
    var has_as_of = False

    var k = 3
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
        else:
            raise Error("unknown option '" + a + "'")

    var table = Table.load(location, io^)
    ref m = table.metadata

    if command == "describe":
        print("location:        ", m.location)
        print("metadata:        ", table.metadata_location)
        print("format-version:  ", m.format_version)
        print("table-uuid:      ", m.table_uuid)
        print("last-updated-ms: ", m.last_updated_ms)
        print("last-sequence:   ", m.last_sequence_number)
        print("last-column-id:  ", m.last_column_id)
        print("schemas:         ", len(m.schemas), "(current", String(m.current_schema_id) + ")")
        print("partition-specs: ", len(m.partition_specs), "(default", String(m.default_spec_id) + ")")
        print("sort-orders:     ", len(m.sort_orders), "(default", String(m.default_sort_order_id) + ")")
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
