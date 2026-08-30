"""Build the tables the write path is verified against.

Ten **append** tables — five partitioning shapes x format versions 2 and 3 —
each created empty and then appended to three times, so each has three
snapshots.

Then twenty-one **delete and overwrite** tables, in the `del` namespace, each
built the same way and then modified once:

* `mor_<shape>_v<n>` — a merge-on-read `DELETE WHERE region = 'eu'`, which is
  a deletion vector per affected data file on v3 and a position delete file
  per partition on v2;
* `cow_<shape>_v<n>` — the same delete, copy-on-write, which rewrites the
  affected data files and leaves no delete file at all;
* `mor_twice_v3` — two merge-on-read deletes against the same data file, so
  the second vector has to absorb the first;
* `ovw_all_v2` — an unfiltered overwrite; `ovw_filter_v3` — a filtered one;
* `dyn_ident_v<n>` — a dynamic partition overwrite, which must replace the
  `eu` partition and leave the other four alone.

Everything here goes through the public API: `FilesystemCatalog.create_table`,
`Table.new_append()`, `commit()`, `delete_where()`, `overwrite()`,
`dynamic_partition_overwrite()`.

`tools/verify_written.py` then reads every one of them with PyIceberg 0.11.1
and DuckDB 1.5.5 and compares rows, snapshots, partition values and statistics
against the manifest this writer produced.

    write-tables <warehouse-dir>
"""

from std.memory import bitcast
from std.sys import argv

from parquet import RecordBatch

from iceberg.batch import (
    ColumnBuilder,
    NestedBuilder,
    batch_of,
    batch_of_columns,
)
from iceberg.json import parse_json
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.io import FileIO, join_path
from iceberg.schema import Schema
from iceberg.transforms import PartitionField, PartitionSpec, parse_transform
from iceberg.values import Datum


comptime SCHEMA_JSON = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"region","required":true,"type":"string"},'
    '{"id":3,"name":"amount","required":false,"type":"double"},'
    '{"id":4,"name":"ts","required":false,"type":"timestamp"},'
    '{"id":5,"name":"ok","required":false,"type":"boolean"}]}'
)

comptime DAY_2024_01_01: Int64 = 19723
comptime MICROS_PER_DAY: Int64 = 86400000000
comptime REGIONS = String("eu,us,apac,latam,emea")


def region_of(i: Int) raises -> String:
    var parts = REGIONS.split(",")
    return String(parts[i % 5])


def make_batch(schema: Schema, start: Int, n: Int) raises -> RecordBatch:
    """`n` rows starting at `start`, with nulls in `amount` and in `ok`."""
    var ids = ColumnBuilder.of(schema, 1)
    var region = ColumnBuilder.of(schema, 2)
    var amount = ColumnBuilder.of(schema, 3)
    var ts = ColumnBuilder.of(schema, 4)
    var ok = ColumnBuilder.of(schema, 5)
    for k in range(n):
        var i = start + k
        ids.add(Datum.long_(Int64(i)))
        region.add(Datum.string_(region_of(i)))
        if i % 4 == 0:
            amount.add_null()
        else:
            amount.add(Datum.double_(Float64(i) * 1.5))
        ts.add(
            Datum.integral(
                ts.kind,
                (DAY_2024_01_01 + Int64(i % 3)) * MICROS_PER_DAY
                + Int64(i) * 1000,
            )
        )
        if i % 5 == 0:
            ok.add_null()
        else:
            ok.add(Datum.bool_(i % 2 == 0))
    return batch_of([ids^, region^, amount^, ts^, ok^])


def spec_for(kind: String) raises -> PartitionSpec:
    if kind == "unpartitioned":
        return PartitionSpec.unpartitioned()
    if kind == "ident":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    2, 1000, String("region"), parse_transform("identity")
                )
            ],
        )
    if kind == "bucket":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    1, 1000, String("id_bucket"), parse_transform("bucket[4]")
                )
            ],
        )
    if kind == "day":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    4, 1000, String("ts_day"), parse_transform("day")
                )
            ],
        )
    if kind == "trunc":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    2,
                    1000,
                    String("region_trunc"),
                    parse_transform("truncate[3]"),
                )
            ],
        )
    raise Error("unknown partition shape: " + kind)


def main() raises:
    var args = argv()
    if len(args) < 2:
        print("usage: write-tables <warehouse-dir>")
        return
    var warehouse = String(args[1])
    var catalog = FilesystemCatalog.local(warehouse)
    var schema = Schema.parse(SCHEMA_JSON)
    var shapes: List[String] = [
        String("unpartitioned"),
        String("ident"),
        String("bucket"),
        String("day"),
        String("trunc"),
    ]
    var versions: List[Int] = [2, 3]
    for vi in range(len(versions)):
        var v = versions[vi]
        for si in range(len(shapes)):
            var name = shapes[si] + "_v" + String(v)
            var table = catalog.create_table(
                String("db"),
                name,
                schema,
                spec_for(shapes[si]),
                Dict[String, String](),
                v,
            )
            var total: Int64 = 0
            for b in range(3):
                var batches = List[RecordBatch]()
                batches.append(make_batch(schema, b * 6, 6))
                var tx = table.new_append()
                tx.add_batches(batches)
                total += tx.commit()
                table.refresh()
            print(
                "wrote db." + name,
                "v" + String(v),
                String(total) + " rows",
                String(len(table.metadata.snapshots)) + " snapshots",
                "next-row-id=" + String(table.metadata.next_row_id),
            )
    write_delete_tables(catalog, schema)
    write_nested_tables(catalog, warehouse)


def seeded(
    catalog: FilesystemCatalog,
    schema: Schema,
    name: String,
    shape: String,
    version: Int,
    mode: String,
) raises -> Table:
    """A table with the standard three appends, and a delete mode set."""
    var props = Dict[String, String]()
    if mode != "":
        props["write.delete.mode"] = mode
    var table = catalog.create_table(
        String("del"), name, schema, spec_for(shape), props^, version
    )
    for b in range(3):
        var batches = List[RecordBatch]()
        batches.append(make_batch(schema, b * 6, 6))
        var tx = table.new_append()
        tx.add_batches(batches)
        _ = tx.commit()
        table.refresh()
    return table^


def report(name: String, table: Table, note: String) raises:
    var rows = table.scan().to_table().num_rows()
    print(
        "wrote del." + name,
        "v" + String(table.metadata.format_version),
        String(rows) + " rows",
        String(len(table.metadata.snapshots)) + " snapshots",
        note,
    )


def write_delete_tables(catalog: FilesystemCatalog, schema: Schema) raises:
    var shapes: List[String] = [
        String("unpartitioned"),
        String("ident"),
        String("bucket"),
        String("day"),
    ]
    var versions: List[Int] = [2, 3]
    for vi in range(len(versions)):
        var v = versions[vi]
        for si in range(len(shapes)):
            var mor = "mor_" + shapes[si] + "_v" + String(v)
            var t = seeded(
                catalog, schema, mor, shapes[si], v, String("merge-on-read")
            )
            var n = t.delete_where(String('["=","region","eu"]'))
            report(mor, t, "merge-on-read delete of " + String(n) + " rows")

            var cow = "cow_" + shapes[si] + "_v" + String(v)
            var c = seeded(catalog, schema, cow, shapes[si], v, String(""))
            var m = c.delete_where(String('["=","region","eu"]'))
            report(cow, c, "copy-on-write delete of " + String(m) + " rows")

    # Two merge-on-read deletes against the same file: the second vector has
    # to absorb the first, and the first must be marked DELETED.
    var twice = seeded(
        catalog,
        schema,
        String("mor_twice_v3"),
        String("unpartitioned"),
        3,
        String("merge-on-read"),
    )
    _ = twice.delete_where(String('["=","id",0]'))
    _ = twice.delete_where(String('["=","id",3]'))
    report(String("mor_twice_v3"), twice, "two merged deletion vectors")

    var all_ovw = seeded(
        catalog,
        schema,
        String("ovw_all_v2"),
        String("unpartitioned"),
        2,
        String(""),
    )
    var fresh = List[RecordBatch]()
    fresh.append(make_batch(schema, 100, 3))
    _ = all_ovw.overwrite(fresh)
    report(String("ovw_all_v2"), all_ovw, "unfiltered overwrite")

    var filtered = seeded(
        catalog,
        schema,
        String("ovw_filter_v3"),
        String("unpartitioned"),
        3,
        String(""),
    )
    var more = List[RecordBatch]()
    more.append(make_batch(schema, 200, 2))
    _ = filtered.overwrite(more, String('[">","id",11]'))
    report(String("ovw_filter_v3"), filtered, "overwrite where id > 11")

    # v2 equality deletes: values, not positions. PyIceberg cannot plan
    # these at all, so DuckDB is the only external reader that can check them.
    var eq = seeded(
        catalog,
        schema,
        String("eq_v2"),
        String("unpartitioned"),
        2,
        String(""),
    )
    var victims = ColumnBuilder.of(schema, 1)
    victims.add(Datum.long_(Int64(2)))
    victims.add(Datum.long_(Int64(9)))
    victims.add(Datum.long_(Int64(16)))
    _ = eq.delete_by_equality(batch_of([victims^]), [1])
    report(String("eq_v2"), eq, "equality delete of ids 2, 9 and 16")

    for vi in range(len(versions)):
        var v = versions[vi]
        var name = "dyn_ident_v" + String(v)
        var dyn = seeded(catalog, schema, name, String("ident"), v, String(""))
        var one = List[RecordBatch]()
        one.append(make_batch(schema, 100, 1))
        _ = dyn.dynamic_partition_overwrite(one)
        report(name, dyn, "dynamic overwrite of the eu partition")


# ── nested columns ──────────────────────────────────────────────────────────
comptime NESTED_SCHEMA_JSON = String(
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"addr","required":false,"type":{"type":"struct","fields":['
    '{"id":10,"name":"city","required":false,"type":"string"},'
    '{"id":11,"name":"zip","required":false,"type":"int"}]}},'
    '{"id":3,"name":"tags","required":false,"type":'
    '{"type":"list","element-id":20,"element":"string",'
    '"element-required":false}},'
    '{"id":4,"name":"props","required":false,"type":'
    '{"type":"map","key-id":30,"key":"string","value-id":31,"value":"long",'
    '"value-required":false}},'
    '{"id":5,"name":"items","required":false,"type":'
    '{"type":"list","element-id":40,"element":{"type":"struct","fields":['
    '{"id":41,"name":"sku","required":false,"type":"string"},'
    '{"id":42,"name":"qty","required":false,"type":"int"}]},'
    '"element-required":false}}]}'
)

comptime NESTED_ROWS = String(
    "["
    '{"id":0,"addr":{"city":"eu","zip":10},"tags":["a","b"],'
    '"props":{"keys":["x","y"],"values":[1,2]},'
    '"items":[{"sku":"s0","qty":3}]},'
    '{"id":1,"addr":null,"tags":[],"props":{"keys":[],"values":[]},'
    '"items":[]},'
    '{"id":2,"addr":{"city":null,"zip":30},"tags":null,"props":null,'
    '"items":null},'
    '{"id":3,"addr":{"city":"us","zip":null},"tags":["c",null],'
    '"props":{"keys":["z"],"values":[null]},'
    '"items":[{"sku":null,"qty":7},null]},'
    '{"id":4,"addr":{"city":"eu","zip":50},"tags":["d"],'
    '"props":{"keys":["k"],"values":[9]},"items":[{"sku":"s4","qty":1}]},'
    '{"id":5,"addr":{"city":"apac","zip":60},"tags":["e","f"],'
    '"props":{"keys":["a","b"],"values":[1,2]},"items":[]},'
    '{"id":6,"addr":{"city":"us","zip":70},"tags":["g"],'
    '"props":{"keys":["q"],"values":[42]},'
    '"items":[{"sku":"s6","qty":null}]},'
    '{"id":7,"addr":null,"tags":null,"props":{"keys":[],"values":[]},'
    '"items":[{"sku":"s7","qty":9}]},'
    '{"id":8,"addr":{"city":"eu","zip":80},"tags":["h","i","j"],'
    '"props":{"keys":["m"],"values":[5]},"items":null},'
    '{"id":9,"addr":{"city":"latam","zip":90},"tags":[],'
    '"props":null,"items":[{"sku":"s9","qty":2},{"sku":null,"qty":null}]},'
    '{"id":10,"addr":{"city":null,"zip":100},"tags":["k"],'
    '"props":{"keys":["n","o"],"values":[7,null]},"items":[]},'
    '{"id":11,"addr":{"city":"eu","zip":110},"tags":null,'
    '"props":{"keys":["p"],"values":[8]},"items":[{"sku":"sb","qty":4}]}'
    "]"
)
"""The rows the nested tables are written from, verbatim.

`tools/verify_written.py` reads the same text out of `nest/rows.json` and
compares PyIceberg's and DuckDB's answers against it, so the chain it checks
is JSON in -> our Arrow builder -> our Parquet writer -> somebody else's
reader -> the same JSON out. Map keys are in ascending order in every row, so
no side has to sort to agree."""


def nested_batch(schema: Schema, start: Int, n: Int) raises -> RecordBatch:
    """`n` of `NESTED_ROWS`, starting at `start`, as one `RecordBatch`."""
    var doc = parse_json(NESTED_ROWS)
    var ids = ColumnBuilder.of(schema, 1)
    var addr = NestedBuilder.of(schema, 2)
    var tags = NestedBuilder.of(schema, 3)
    var props = NestedBuilder.of(schema, 4)
    var items = NestedBuilder.of(schema, 5)
    for k in range(n):
        var row = doc.at(doc.root, start + k)
        ids.add(Datum.long_(doc.req_int(row, "id")))
        addr.add(doc.dump(doc.get(row, "addr")))
        tags.add(doc.dump(doc.get(row, "tags")))
        props.add(doc.dump(doc.get(row, "props")))
        items.add(doc.dump(doc.get(row, "items")))
    return batch_of_columns(
        [
            ids^.build_tree(),
            addr^.build(),
            tags^.build(),
            props^.build(),
            items^.build(),
        ]
    )


def nested_spec(kind: String) raises -> PartitionSpec:
    """A partition spec whose source is a field *inside* a struct."""
    if kind == "unpartitioned":
        return PartitionSpec.unpartitioned()
    if kind == "ident":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    10, 1000, String("city"), parse_transform("identity")
                )
            ],
        )
    if kind == "bucket":
        return PartitionSpec(
            0,
            [
                PartitionField.single(
                    11, 1000, String("zip_bucket"), parse_transform("bucket[4]")
                )
            ],
        )
    raise Error("unknown nested partition shape: " + kind)


def write_nested_tables(catalog: FilesystemCatalog, warehouse: String) raises:
    """The `nest` namespace: structs, lists and maps, written by this library.

    Four tables — unpartitioned on v2 and v3, and two partitioned by a leaf
    *inside* a struct, which the spec allows a partition field's `source-id`
    to name. Each is created empty and appended to twice.
    """
    var schema = Schema.parse(NESTED_SCHEMA_JSON)
    var io = FileIO.local()
    io.write_all(
        join_path(join_path(warehouse, "nest"), "rows.json"),
        NESTED_ROWS.as_bytes(),
    )
    var names: List[String] = [
        String("plain_v2"),
        String("plain_v3"),
        String("ident_v2"),
        String("bucket_v2"),
    ]
    var shapes: List[String] = [
        String("unpartitioned"),
        String("unpartitioned"),
        String("ident"),
        String("bucket"),
    ]
    var versions: List[Int] = [2, 3, 2, 2]
    for k in range(len(names)):
        var table = catalog.create_table(
            String("nest"),
            names[k],
            schema,
            nested_spec(shapes[k]),
            Dict[String, String](),
            versions[k],
        )
        var total: Int64 = 0
        for b in range(2):
            var batches = List[RecordBatch]()
            batches.append(nested_batch(schema, b * 6, 6))
            var tx = table.new_append()
            tx.add_batches(batches)
            total += tx.commit()
            table.refresh()
        print(
            "wrote nest." + names[k],
            "v" + String(versions[k]),
            String(total) + " rows",
            String(len(table.metadata.snapshots)) + " snapshots",
        )
