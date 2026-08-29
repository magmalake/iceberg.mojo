from std.testing import TestSuite, assert_equal, assert_true
from iceberg.json import parse_json
from iceberg.schema import Schema

def test_json_smoke() raises:
    var d = parse_json('{"a":9223372036854775807,"b":[1,2.5,true,null,"x"]}')
    assert_equal(d.as_int(d.get(d.root, "a")), 9223372036854775807)
    assert_equal(d.dump_root(), '{"a":9223372036854775807,"b":[1,2.5,true,null,"x"]}')

def test_schema_smoke() raises:
    var s = Schema.parse('{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"},{"id":2,"name":"d","required":false,"type":"decimal(9, 2)"},{"id":3,"name":"n","required":false,"type":{"type":"struct","fields":[{"id":4,"name":"x","required":false,"type":"string"}]}}]}')
    assert_equal(s.find_by_name("n.x").id, 4)
    assert_equal(s.store.type_name(s.find_field(2).type), "decimal(9, 2)")

def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
