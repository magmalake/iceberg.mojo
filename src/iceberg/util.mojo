"""The three things a writer needs from outside itself: the wall clock, a
random 64-bit number, and a UUID.

Mojo's `std.time` only exposes monotonic counters, so `last-updated-ms` and a
snapshot's `timestamp-ms` come from `clock_gettime(CLOCK_REALTIME)` directly.
Snapshot ids are "a random long" in the spec's words, and every file this
library writes is named with a version 4 UUID, so both come from
`std.random`.
"""

from std.ffi import external_call
from std.random import random_ui64, seed


comptime _HEX = String("0123456789abcdef")


def now_ms() -> Int64:
    """Milliseconds since the Unix epoch."""
    var ts = List[Int64](length=2, fill=0)
    var p = ts.unsafe_ptr()
    _ = external_call["clock_gettime", Int32](Int32(0), p)
    return ts[0] * 1000 + ts[1] // 1000000


def random_long() -> Int64:
    """A random signed 64-bit value, for a snapshot id.

    The spec says only "a unique long"; every implementation uses a random
    one, and the top bit is cleared so the id prints without a sign — which is
    what Java's `SnapshotIdGeneratorUtil` does too.
    """
    var v = random_ui64(0, UInt64.MAX) & 0x7FFFFFFFFFFFFFFF
    if v == 0:
        return 1
    return Int64(v)


def uuid4() -> String:
    """A version 4 UUID in the canonical 8-4-4-4-12 form."""
    var hi = random_ui64(0, UInt64.MAX)
    var lo = random_ui64(0, UInt64.MAX)
    var b = List[UInt8](capacity=16)
    for k in range(8):
        b.append(UInt8((hi >> UInt64(8 * k)) & 0xFF))
    for k in range(8):
        b.append(UInt8((lo >> UInt64(8 * k)) & 0xFF))
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    var out = String("")
    for k in range(16):
        if k == 4 or k == 6 or k == 8 or k == 10:
            out += "-"
        out += _HEX[byte=Int(b[k] >> 4)]
        out += _HEX[byte=Int(b[k] & 0x0F)]
    return out^


def seed_random():
    """Seed the generator from the OS once per process."""
    seed()


def zero_pad(v: Int, width: Int) -> String:
    """`5, 5` -> `00005`, the way Java names metadata files."""
    var s = String(v)
    var out = String("")
    for _ in range(width - s.byte_length()):
        out += "0"
    return out + s
