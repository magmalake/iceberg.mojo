"""A small, dependency-free JSON reader/writer for Iceberg metadata.

Why not [EmberJson](https://github.com/bgreni/EmberJson)? It resolves fine on
both pixi environments (`emberjson 0.3.4` from `modular-community`), but the
package ships a *precompiled* `emberjson.mojoc` built with Mojo `1.0.0`, and the
nightly compiler hard-errors on it:

    error: Mojo precompiled file is incompatible with the current version of the
    Mojo compiler. Precompiled file '.../emberjson.mojoc' version 1.0.0 is older
    than compiler version 1.1.0.dev2026082905.

Since this tin must build on *both* `default` (nightly) and `stable`, the parser
lives here instead. It is deliberately small: Iceberg metadata JSON is at most a
few hundred KB.

Design: an **arena DOM**. Every value is a `JsonNode` in `Json.nodes`, and
containers refer to their children by index. Recursive Mojo data types are
awkward; integer indices are not.

Numbers are the one place correctness really bites. Snapshot ids, sequence
numbers and `last-updated-ms` are signed 64-bit and must survive a parse →
serialize round trip *exactly*, so integers are accumulated into `Int64` digit
by digit and are never routed through `Float64`. A number is only treated as a
floating-point value when it actually carries a `.`, an `e` or an `E`.
"""

from std.collections import Dict


# ── node kinds ──────────────────────────────────────────────────────────────
comptime JSON_NULL: UInt8 = 0
comptime JSON_BOOL: UInt8 = 1
comptime JSON_INT: UInt8 = 2
comptime JSON_FLOAT: UInt8 = 3
comptime JSON_STRING: UInt8 = 4
comptime JSON_ARRAY: UInt8 = 5
comptime JSON_OBJECT: UInt8 = 6


@always_inline
def _o(c: StringSlice) -> UInt8:
    """Byte value of a one-character ASCII literal (`ord` returns `Int`)."""
    return UInt8(ord(c))


@fieldwise_init
struct JsonNode(Copyable, Movable):
    """One JSON value. Containers hold child node indices, not child values."""

    var kind: UInt8
    var b: Bool
    var i: Int64
    var f: Float64
    var s: String
    var keys: List[String]
    """Object member names, parallel to `children`. Empty for arrays."""
    var children: List[Int]
    """Indices into `Json.nodes` of the array elements / object values."""

    @staticmethod
    def null() -> Self:
        return Self(JSON_NULL, False, 0, 0.0, "", [], [])

    @staticmethod
    def bool_(v: Bool) -> Self:
        return Self(JSON_BOOL, v, 0, 0.0, "", [], [])

    @staticmethod
    def int_(v: Int64) -> Self:
        return Self(JSON_INT, False, v, 0.0, "", [], [])

    @staticmethod
    def float_(v: Float64) -> Self:
        return Self(JSON_FLOAT, False, 0, v, "", [], [])

    @staticmethod
    def string_(var v: String) -> Self:
        return Self(JSON_STRING, False, 0, 0.0, v^, [], [])

    @staticmethod
    def array_(var kids: List[Int]) -> Self:
        return Self(JSON_ARRAY, False, 0, 0.0, "", [], kids^)

    @staticmethod
    def object_(var keys: List[String], var kids: List[Int]) -> Self:
        return Self(JSON_OBJECT, False, 0, 0.0, "", keys^, kids^)


struct Json(Copyable, Movable):
    """A parsed JSON document: an arena of nodes plus the index of the root."""

    var nodes: List[JsonNode]
    var root: Int

    def __init__(out self):
        self.nodes = []
        self.root = -1

    # ── construction (used by the serializers as well as the parser) ────────
    def add(mut self, var node: JsonNode) -> Int:
        self.nodes.append(node^)
        return len(self.nodes) - 1

    # ── inspection ─────────────────────────────────────────────────────────
    def kind(self, i: Int) -> UInt8:
        return self.nodes[i].kind

    def is_null(self, i: Int) -> Bool:
        return i < 0 or self.nodes[i].kind == JSON_NULL

    def size(self, i: Int) -> Int:
        """Number of elements (array) or members (object)."""
        return len(self.nodes[i].children)

    def at(self, i: Int, k: Int) -> Int:
        """Index of array element / object value `k`."""
        return self.nodes[i].children[k]

    def key_at(self, i: Int, k: Int) -> String:
        return self.nodes[i].keys[k]

    def get(self, i: Int, key: String) -> Int:
        """Index of object member `key`, or -1 when absent."""
        if i < 0 or self.nodes[i].kind != JSON_OBJECT:
            return -1
        for k in range(len(self.nodes[i].keys)):
            if self.nodes[i].keys[k] == key:
                return self.nodes[i].children[k]
        return -1

    def has(self, i: Int, key: String) -> Bool:
        return self.get(i, key) >= 0

    # ── typed accessors ────────────────────────────────────────────────────
    def as_int(self, i: Int) raises -> Int64:
        ref n = self.nodes[i]
        if n.kind == JSON_INT:
            return n.i
        if n.kind == JSON_FLOAT:
            return Int64(n.f)
        if n.kind == JSON_STRING:
            return parse_int64(n.s)
        raise Error("json: value at " + String(i) + " is not a number")

    def as_float(self, i: Int) raises -> Float64:
        ref n = self.nodes[i]
        if n.kind == JSON_FLOAT:
            return n.f
        if n.kind == JSON_INT:
            return Float64(n.i)
        if n.kind == JSON_STRING:
            # Iceberg writes non-finite doubles as "NaN"/"Infinity" strings.
            if n.s == "NaN":
                return Float64(0.0) / Float64(0.0)
            if n.s == "Infinity":
                return Float64(1.0) / Float64(0.0)
            if n.s == "-Infinity":
                return Float64(-1.0) / Float64(0.0)
            return Float64(n.s)
        raise Error("json: value at " + String(i) + " is not a number")

    def as_string(self, i: Int) raises -> String:
        ref n = self.nodes[i]
        if n.kind == JSON_STRING:
            return n.s
        raise Error("json: value at " + String(i) + " is not a string")

    def as_bool(self, i: Int) raises -> Bool:
        ref n = self.nodes[i]
        if n.kind == JSON_BOOL:
            return n.b
        raise Error("json: value at " + String(i) + " is not a boolean")

    # ── convenience field readers (with defaults) ──────────────────────────
    def opt_int(self, i: Int, key: String, dflt: Int64) raises -> Int64:
        var j = self.get(i, key)
        if j < 0 or self.nodes[j].kind == JSON_NULL:
            return dflt
        return self.as_int(j)

    def req_int(self, i: Int, key: String) raises -> Int64:
        var j = self.get(i, key)
        if j < 0:
            raise Error("json: missing required field '" + key + "'")
        return self.as_int(j)

    def opt_string(self, i: Int, key: String, dflt: String) raises -> String:
        var j = self.get(i, key)
        if j < 0 or self.nodes[j].kind == JSON_NULL:
            return dflt
        return self.as_string(j)

    def req_string(self, i: Int, key: String) raises -> String:
        var j = self.get(i, key)
        if j < 0:
            raise Error("json: missing required field '" + key + "'")
        return self.as_string(j)

    def opt_bool(self, i: Int, key: String, dflt: Bool) raises -> Bool:
        var j = self.get(i, key)
        if j < 0 or self.nodes[j].kind == JSON_NULL:
            return dflt
        return self.as_bool(j)

    def string_map(self, i: Int, key: String) raises -> Dict[String, String]:
        """Read an object of string→string (Iceberg `properties` and friends).
        """
        var out = Dict[String, String]()
        var j = self.get(i, key)
        if j < 0 or self.nodes[j].kind != JSON_OBJECT:
            return out^
        for k in range(len(self.nodes[j].keys)):
            var v = self.nodes[j].children[k]
            if self.nodes[v].kind == JSON_STRING:
                out[self.nodes[j].keys[k]] = self.nodes[v].s
            else:
                out[self.nodes[j].keys[k]] = self.dump(v)
        return out^

    def int_list(self, i: Int, key: String) raises -> List[Int]:
        var out = List[Int]()
        var j = self.get(i, key)
        if j < 0 or self.nodes[j].kind != JSON_ARRAY:
            return out^
        for k in range(len(self.nodes[j].children)):
            out.append(Int(self.as_int(self.nodes[j].children[k])))
        return out^

    def string_list(self, i: Int, key: String) raises -> List[String]:
        var out = List[String]()
        var j = self.get(i, key)
        if j < 0 or self.nodes[j].kind != JSON_ARRAY:
            return out^
        for k in range(len(self.nodes[j].children)):
            out.append(self.as_string(self.nodes[j].children[k]))
        return out^

    # ── serialization ──────────────────────────────────────────────────────
    def dump(self, i: Int) -> String:
        """Compact JSON text for the subtree rooted at `i`."""
        var out = String("")
        self._dump_into(i, out)
        return out^

    def dump_root(self) -> String:
        return self.dump(self.root)

    def _dump_into(self, i: Int, mut out: String):
        if i < 0:
            out += "null"
            return
        var k = self.nodes[i].kind
        if k == JSON_NULL:
            out += "null"
        elif k == JSON_BOOL:
            out += "true" if self.nodes[i].b else "false"
        elif k == JSON_INT:
            out += String(self.nodes[i].i)
        elif k == JSON_FLOAT:
            out += format_double(self.nodes[i].f)
        elif k == JSON_STRING:
            write_json_string(self.nodes[i].s, out)
        elif k == JSON_ARRAY:
            out += "["
            for c in range(len(self.nodes[i].children)):
                if c > 0:
                    out += ","
                self._dump_into(self.nodes[i].children[c], out)
            out += "]"
        else:
            out += "{"
            for c in range(len(self.nodes[i].children)):
                if c > 0:
                    out += ","
                write_json_string(self.nodes[i].keys[c], out)
                out += ":"
                self._dump_into(self.nodes[i].children[c], out)
            out += "}"


# ── string escaping ─────────────────────────────────────────────────────────
comptime _HEX = String("0123456789abcdef")


def write_json_string(s: String, mut out: String):
    """Append `s` as a quoted, escaped JSON string."""
    out += '"'
    var b = s.as_bytes()
    for k in range(len(b)):
        var c = b[k]
        if c == _o('"'):
            out += '\\"'
        elif c == _o("\\"):
            out += "\\\\"
        elif c == 0x08:
            out += "\\b"
        elif c == 0x0C:
            out += "\\f"
        elif c == 0x0A:
            out += "\\n"
        elif c == 0x0D:
            out += "\\r"
        elif c == 0x09:
            out += "\\t"
        elif c < 0x20:
            out += "\\u00"
            out += String(_HEX[byte=Int(c >> 4)])
            out += String(_HEX[byte=Int(c & 0xF)])
        else:
            out += String(StringSlice(unsafe_from_utf8=Span(b)[k : k + 1]))
    out += '"'


def json_quote(s: String) -> String:
    var out = String("")
    write_json_string(s, out)
    return out^


def format_double(v: Float64) -> String:
    """Render a double the way Iceberg's JSON writers do.

    `-0.0` keeps its sign (it is a distinct bound value), and non-finite values
    become the strings Jackson emits, since JSON has no literal for them.
    """
    if v != v:
        return '"NaN"'
    if v == Float64(1.0) / Float64(0.0):
        return '"Infinity"'
    if v == Float64(-1.0) / Float64(0.0):
        return '"-Infinity"'
    var s = String(v)
    # Mojo prints whole doubles as "3.0"; that is already valid JSON.
    return s^


# ── number parsing ──────────────────────────────────────────────────────────
def parse_int64(s: String) raises -> Int64:
    """Exact decimal → Int64, including `-9223372036854775808`."""
    var b = s.as_bytes()
    var n = len(b)
    var p = 0
    if n == 0:
        raise Error("json: empty integer")
    var neg = False
    if b[0] == _o("-"):
        neg = True
        p = 1
    elif b[0] == _o("+"):
        p = 1
    if p >= n:
        raise Error("json: bare sign is not an integer")
    # Accumulate the magnitude in unsigned space so Int64.MIN is reachable.
    var mag: UInt64 = 0
    while p < n:
        var c = b[p]
        if c < _o("0") or c > _o("9"):
            raise Error("json: bad integer '" + s + "'")
        mag = mag * 10 + UInt64(c - _o("0"))
        p += 1
    if neg:
        return Int64(0) - Int64(mag)
    return Int64(mag)


# ── the parser ──────────────────────────────────────────────────────────────
struct _Parser(Copyable, Movable):
    var b: List[UInt8]
    var p: Int
    var n: Int
    var doc: Json

    def __init__(out self, text: String):
        self.b = List[UInt8]()
        var src = text.as_bytes()
        for k in range(len(src)):
            self.b.append(src[k])
        self.p = 0
        self.n = len(self.b)
        self.doc = Json()

    def _err(self, msg: String) -> Error:
        return Error("json: " + msg + " at offset " + String(self.p))

    def skip_ws(mut self):
        while self.p < self.n:
            var c = self.b[self.p]
            if c == 0x20 or c == 0x09 or c == 0x0A or c == 0x0D:
                self.p += 1
            else:
                break

    def peek(self) -> UInt8:
        if self.p >= self.n:
            return 0
        return self.b[self.p]

    def expect(mut self, c: UInt8) raises:
        if self.p >= self.n or self.b[self.p] != c:
            raise self._err("expected '" + String(chr(Int(c))) + "'")
        self.p += 1

    def lit(mut self, word: String) raises:
        var w = word.as_bytes()
        if self.p + len(w) > self.n:
            raise self._err("truncated literal '" + word + "'")
        for k in range(len(w)):
            if self.b[self.p + k] != w[k]:
                raise self._err("expected literal '" + word + "'")
        self.p += len(w)

    def parse_value(mut self) raises -> Int:
        self.skip_ws()
        if self.p >= self.n:
            raise self._err("unexpected end of input")
        var c = self.b[self.p]
        if c == _o("{"):
            return self.parse_object()
        if c == _o("["):
            return self.parse_array()
        if c == _o('"'):
            var s = self.parse_string()
            return self.doc.add(JsonNode.string_(s^))
        if c == _o("t"):
            self.lit("true")
            return self.doc.add(JsonNode.bool_(True))
        if c == _o("f"):
            self.lit("false")
            return self.doc.add(JsonNode.bool_(False))
        if c == _o("n"):
            self.lit("null")
            return self.doc.add(JsonNode.null())
        return self.parse_number()

    def parse_object(mut self) raises -> Int:
        self.expect(_o("{"))
        var keys = List[String]()
        var kids = List[Int]()
        self.skip_ws()
        if self.peek() == _o("}"):
            self.p += 1
            return self.doc.add(JsonNode.object_(keys^, kids^))
        while True:
            self.skip_ws()
            var k = self.parse_string()
            self.skip_ws()
            self.expect(_o(":"))
            var v = self.parse_value()
            keys.append(k^)
            kids.append(v)
            self.skip_ws()
            var c = self.peek()
            if c == _o(","):
                self.p += 1
                continue
            if c == _o("}"):
                self.p += 1
                break
            raise self._err("expected ',' or '}' in object")
        return self.doc.add(JsonNode.object_(keys^, kids^))

    def parse_array(mut self) raises -> Int:
        self.expect(_o("["))
        var kids = List[Int]()
        self.skip_ws()
        if self.peek() == _o("]"):
            self.p += 1
            return self.doc.add(JsonNode.array_(kids^))
        while True:
            var v = self.parse_value()
            kids.append(v)
            self.skip_ws()
            var c = self.peek()
            if c == _o(","):
                self.p += 1
                continue
            if c == _o("]"):
                self.p += 1
                break
            raise self._err("expected ',' or ']' in array")
        return self.doc.add(JsonNode.array_(kids^))

    def parse_string(mut self) raises -> String:
        self.expect(_o('"'))
        var out = List[UInt8]()
        while True:
            if self.p >= self.n:
                raise self._err("unterminated string")
            var c = self.b[self.p]
            if c == _o('"'):
                self.p += 1
                break
            if c == _o("\\"):
                self.p += 1
                if self.p >= self.n:
                    raise self._err("unterminated escape")
                var e = self.b[self.p]
                self.p += 1
                if e == _o('"'):
                    out.append(_o('"'))
                elif e == _o("\\"):
                    out.append(_o("\\"))
                elif e == _o("/"):
                    out.append(_o("/"))
                elif e == _o("b"):
                    out.append(0x08)
                elif e == _o("f"):
                    out.append(0x0C)
                elif e == _o("n"):
                    out.append(0x0A)
                elif e == _o("r"):
                    out.append(0x0D)
                elif e == _o("t"):
                    out.append(0x09)
                elif e == _o("u"):
                    var cp = self.hex4()
                    # Surrogate pair: \uD800-\uDBFF must be followed by a low
                    # surrogate, otherwise the text is not well-formed UTF-16.
                    if cp >= 0xD800 and cp <= 0xDBFF:
                        if (
                            self.p + 1 < self.n
                            and self.b[self.p] == _o("\\")
                            and self.b[self.p + 1] == _o("u")
                        ):
                            self.p += 2
                            var lo = self.hex4()
                            if lo >= 0xDC00 and lo <= 0xDFFF:
                                cp = (
                                    0x10000
                                    + ((cp - 0xD800) << 10)
                                    + (lo - 0xDC00)
                                )
                            else:
                                encode_utf8(cp, out)
                                cp = lo
                    encode_utf8(cp, out)
                else:
                    raise self._err("unknown escape")
                continue
            out.append(c)
            self.p += 1
        return String(StringSlice(unsafe_from_utf8=Span(out)))

    def hex4(mut self) raises -> Int:
        if self.p + 4 > self.n:
            raise self._err("truncated \\u escape")
        var v = 0
        for _k in range(4):
            var c = self.b[self.p]
            self.p += 1
            if c >= _o("0") and c <= _o("9"):
                v = v * 16 + Int(c - _o("0"))
            elif c >= _o("a") and c <= _o("f"):
                v = v * 16 + Int(c - _o("a")) + 10
            elif c >= _o("A") and c <= _o("F"):
                v = v * 16 + Int(c - _o("A")) + 10
            else:
                raise self._err("bad hex digit in \\u escape")
        return v

    def finish(deinit self, root: Int) -> Json:
        var doc = self.doc^
        doc.root = root
        return doc^

    def parse_number(mut self) raises -> Int:
        var start = self.p
        if self.peek() == _o("-") or self.peek() == _o("+"):
            self.p += 1
        var is_float = False
        while self.p < self.n:
            var c = self.b[self.p]
            if c >= _o("0") and c <= _o("9"):
                self.p += 1
            elif c == _o(".") or c == _o("e") or c == _o("E"):
                is_float = True
                self.p += 1
            elif (c == _o("-") or c == _o("+")) and is_float:
                self.p += 1
            else:
                break
        if self.p == start:
            raise self._err("expected a value")
        var text = String(
            StringSlice(unsafe_from_utf8=Span(self.b)[start : self.p])
        )
        if is_float:
            return self.doc.add(JsonNode.float_(Float64(text)))
        return self.doc.add(JsonNode.int_(parse_int64(text)))


def encode_utf8(cp: Int, mut out: List[UInt8]):
    if cp < 0x80:
        out.append(UInt8(cp))
    elif cp < 0x800:
        out.append(UInt8(0xC0 | (cp >> 6)))
        out.append(UInt8(0x80 | (cp & 0x3F)))
    elif cp < 0x10000:
        out.append(UInt8(0xE0 | (cp >> 12)))
        out.append(UInt8(0x80 | ((cp >> 6) & 0x3F)))
        out.append(UInt8(0x80 | (cp & 0x3F)))
    else:
        out.append(UInt8(0xF0 | (cp >> 18)))
        out.append(UInt8(0x80 | ((cp >> 12) & 0x3F)))
        out.append(UInt8(0x80 | ((cp >> 6) & 0x3F)))
        out.append(UInt8(0x80 | (cp & 0x3F)))


def parse_json(text: String) raises -> Json:
    """Parse a complete JSON document."""
    var p = _Parser(text)
    var root = p.parse_value()
    p.skip_ws()
    if p.p != p.n:
        raise p._err("trailing content after the top-level value")
    return p^.finish(root)


def substr(s: String, start: Int, end: Int) -> String:
    """Byte-range substring. Mojo's `String` has no slice syntax."""
    var b = s.as_bytes()
    var a = start if start > 0 else 0
    var z = end if end < len(b) else len(b)
    if a >= z:
        return String("")
    return String(StringSlice(unsafe_from_utf8=Span(b)[a:z]))
