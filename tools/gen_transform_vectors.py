#!/usr/bin/env python3
"""Emit tests/fixtures/transform_vectors.json from PyIceberg's transforms.

Every vector is one object:

    {"transform": "bucket[4]", "type": "long",
     "input": 34, "input_int": 34,
     "hash": 2017239379, "output": 3}

  * ``input``     - the value in an unambiguous JSON encoding: an integer for
                    int/long, an ISO-8601 string for date/time/timestamp(tz), a
                    plain decimal string for decimal, lowercase hex for
                    binary/fixed, the canonical 8-4-4-4-12 form for uuid.
  * ``input_int`` - the same value as the integer Iceberg stores it as (days
                    since the epoch for date, microseconds since midnight for
                    time, microseconds since the epoch for timestamp(tz), the
                    unscaled value for decimal), or null where there is none.
                    A Mojo test can consume whichever it prefers.
  * ``hash``      - the raw 32-bit murmur3 hash, signed, for bucket vectors
                    only (``BucketTransform.transform(type, bucket=False)``).
  * ``output``    - the transform result; an integer for
                    bucket/year/month/day/hour, the truncated value (same
                    encoding rules as ``input``) for truncate/identity, and
                    null for void.

Coverage includes the Iceberg spec's published bucket test vectors, negative
and pre-epoch dates and timestamps, and multibyte / emoji string truncation
(truncate counts UTF-8 code points, not bytes).

Run under the PyIceberg venv:
    python tools/gen_transform_vectors.py tests/fixtures/transform_vectors.json
"""

from __future__ import annotations

import datetime as dt
import json
import sys
import uuid as uuidlib
from decimal import Decimal

from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    VoidTransform,
    YearTransform,
)
from pyiceberg.types import (
    BinaryType,
    DateType,
    DecimalType,
    FixedType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
from pyiceberg.utils.datetime import (
    date_to_days,
    datetime_to_micros,
    time_to_micros,
)

EPOCH_DATE = dt.date(1970, 1, 1)


# ---------------------------------------------------------------- encoding --
def type_name(t) -> str:
    if isinstance(t, DecimalType):
        return "decimal(%d,%d)" % (t.precision, t.scale)
    if isinstance(t, FixedType):
        return "fixed[%d]" % len(t)
    return {
        IntegerType: "int",
        LongType: "long",
        StringType: "string",
        UUIDType: "uuid",
        DateType: "date",
        TimeType: "time",
        TimestampType: "timestamp",
        TimestamptzType: "timestamptz",
        BinaryType: "binary",
    }[type(t)]


def encode(value):
    """JSON-safe, unambiguous encoding of an Iceberg value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, uuidlib.UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dt.time):
        return value.isoformat()
    return value


def as_int(value):
    """The integer Iceberg physically stores, or None."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, dt.datetime):
        return datetime_to_micros(value)
    if isinstance(value, dt.date):
        return date_to_days(value)
    if isinstance(value, dt.time):
        return time_to_micros(value)
    if isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        unscaled = int("".join(str(d) for d in digits))
        return -unscaled if sign else unscaled
    if isinstance(value, int):
        return value
    return None


VECTORS: list[dict] = []


def emit(transform, itype, value, *, want_hash=False):
    fn = transform.transform(itype)
    out = fn(value)
    rec = {
        "transform": str(transform),
        "type": type_name(itype),
        "input": encode(value),
        "input_int": as_int(value),
        "output": encode(out),
    }
    if want_hash:
        rec["hash"] = transform.transform(itype, bucket=False)(value)
    VECTORS.append(rec)
    return rec


# ------------------------------------------------------------------ bucket --
# The values marked "spec" are the published vectors from the Iceberg spec's
# "Appendix B: 32-bit Hash Requirements" table.
SPEC_BUCKET = [
    (IntegerType(), 34),  # spec: hash 2017239379
    (LongType(), 34),  # spec: hash 2017239379
    (DecimalType(9, 2), Decimal("14.20")),  # spec: hash -500754589
    (DateType(), dt.date(2017, 11, 16)),  # spec: hash -653330422
    (TimeType(), dt.time(22, 31, 8)),  # spec: hash -662762989
    (TimestampType(), dt.datetime(2017, 11, 16, 22, 31, 8)),  # spec: -2047944441
    (
        TimestamptzType(),
        dt.datetime(
            2017, 11, 16, 14, 31, 8, tzinfo=dt.timezone(dt.timedelta(hours=-8))
        ),
    ),  # spec: hash -2047944441
    (StringType(), "iceberg"),  # spec: hash 1210000089
    (UUIDType(), uuidlib.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")),  # 1488055340
    (FixedType(4), b"\x00\x01\x02\x03"),  # spec: hash -188683207
    (BinaryType(), b"\x00\x01\x02\x03"),  # spec: hash -188683207
]

EXTRA_BUCKET = [
    (IntegerType(), 0),
    (IntegerType(), -1),
    (IntegerType(), 2147483647),
    (IntegerType(), -2147483648),
    (LongType(), 0),
    (LongType(), -1),
    (LongType(), 9223372036854775807),
    (LongType(), -9223372036854775808),
    (StringType(), ""),
    (StringType(), "a"),
    (StringType(), "измерение"),
    (StringType(), "\U0001f600\U0001f603"),
    (DateType(), EPOCH_DATE),
    (DateType(), dt.date(1969, 12, 31)),
    (DateType(), dt.date(1969, 1, 1)),
    (TimeType(), dt.time(0, 0, 0)),
    (TimeType(), dt.time(23, 59, 59, 999999)),
    (TimestampType(), dt.datetime(1970, 1, 1, 0, 0, 0)),
    (TimestampType(), dt.datetime(1969, 12, 31, 23, 59, 59, 999999)),
    (
        TimestamptzType(),
        dt.datetime(1970, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc),
    ),
    (DecimalType(9, 2), Decimal("-14.20")),
    (DecimalType(9, 2), Decimal("0.00")),
    (DecimalType(38, 10), Decimal("12345678901234567890.1234567890")),
    (BinaryType(), b""),
    (BinaryType(), b"\xff\xfe"),
    (FixedType(4), b"\xff\xff\xff\xff"),
    (
        UUIDType(),
        uuidlib.UUID("00000000-0000-0000-0000-000000000000"),
    ),
]


def bucket_vectors():
    for n in (4, 16, 100):
        for itype, value in SPEC_BUCKET:
            emit(BucketTransform(n), itype, value, want_hash=True)
    for itype, value in EXTRA_BUCKET:
        emit(BucketTransform(4), itype, value, want_hash=True)
        emit(BucketTransform(16), itype, value, want_hash=True)


# ---------------------------------------------------------------- truncate --
TRUNCATE = [
    # (width, type, value)   spec vectors first
    (10, IntegerType(), 1),  # spec: 0
    (10, IntegerType(), -1),  # spec: -10
    (10, LongType(), 1),  # spec: 0
    (10, LongType(), -1),  # spec: -10
    (50, DecimalType(9, 2), Decimal("10.65")),  # spec: 10.50
    (3, StringType(), "iceberg"),  # spec: ice
    # integers around the boundaries
    (10, IntegerType(), 0),
    (10, IntegerType(), 9),
    (10, IntegerType(), 10),
    (10, IntegerType(), 11),
    (10, IntegerType(), -10),
    (10, IntegerType(), -11),
    (10, IntegerType(), -2147483648),
    (7, IntegerType(), 2147483647),
    (10, LongType(), -9223372036854775808),
    (10, LongType(), 9223372036854775807),
    (10, LongType(), -100),
    (10, LongType(), -101),
    # strings truncate by code point, not by byte
    (5, StringType(), "iceberg"),
    (10, StringType(), "iceberg"),
    (0 + 1, StringType(), "iceberg"),
    (5, StringType(), "измерение"),
    (3, StringType(), "измерение"),
    (20, StringType(), "измерение"),
    (3, StringType(), "\U0001f600\U0001f603\U0001f604\U0001f601\U0001f606"),
    (1, StringType(), "\U0001f600\U0001f603"),
    (2, StringType(), "aé中\U0001f600"),
    (4, StringType(), "aé中\U0001f600"),
    (3, StringType(), ""),
    # binary truncates by byte
    (2, BinaryType(), b"\x01\x02\x03\x04"),
    (4, BinaryType(), b"\x01\x02"),
    (1, BinaryType(), b"\xff\x00\xff"),
    # decimals truncate on the unscaled value
    (50, DecimalType(9, 2), Decimal("-10.65")),
    (50, DecimalType(9, 2), Decimal("0.00")),
    (100, DecimalType(9, 2), Decimal("14.20")),
    (100, DecimalType(9, 2), Decimal("-14.20")),
    (1000, DecimalType(38, 10), Decimal("12345.6789012345")),
]


def truncate_vectors():
    for width, itype, value in TRUNCATE:
        emit(TruncateTransform(width), itype, value)


# ------------------------------------------------------- year/month/day/hour --
TIME_DATES = [
    dt.date(1970, 1, 1),
    dt.date(1970, 1, 2),
    dt.date(1969, 12, 31),
    dt.date(1969, 12, 1),
    dt.date(1969, 1, 1),
    dt.date(1968, 12, 31),
    dt.date(1971, 1, 1),
    dt.date(2017, 11, 16),
    dt.date(2023, 11, 14),
    dt.date(2024, 2, 29),
]

TIME_TIMESTAMPS = [
    dt.datetime(1970, 1, 1, 0, 0, 0),
    dt.datetime(1970, 1, 1, 0, 0, 0, 1),
    dt.datetime(1970, 1, 1, 1, 0, 0),
    dt.datetime(1969, 12, 31, 23, 59, 59, 999999),
    dt.datetime(1969, 12, 31, 23, 0, 0),
    dt.datetime(1969, 12, 31, 0, 0, 0),
    dt.datetime(1969, 12, 1, 0, 0, 0),
    dt.datetime(1969, 1, 1, 0, 0, 0),
    dt.datetime(1968, 12, 31, 23, 59, 59, 999999),
    dt.datetime(2017, 11, 16, 22, 31, 8),
    dt.datetime(2023, 11, 14, 0, 0, 0),
    dt.datetime(2023, 12, 1, 0, 0, 0),
]


def time_vectors():
    for value in TIME_DATES:
        emit(YearTransform(), DateType(), value)
        emit(MonthTransform(), DateType(), value)
        emit(DayTransform(), DateType(), value)
    for value in TIME_TIMESTAMPS:
        emit(YearTransform(), TimestampType(), value)
        emit(MonthTransform(), TimestampType(), value)
        emit(DayTransform(), TimestampType(), value)
        emit(HourTransform(), TimestampType(), value)
    for value in TIME_TIMESTAMPS[:4] + TIME_TIMESTAMPS[-2:]:
        tz = value.replace(tzinfo=dt.timezone.utc)
        emit(YearTransform(), TimestamptzType(), tz)
        emit(MonthTransform(), TimestamptzType(), tz)
        emit(DayTransform(), TimestamptzType(), tz)
        emit(HourTransform(), TimestamptzType(), tz)


# -------------------------------------------------------- identity and void --
IDENTITY_VOID = [
    (IntegerType(), 34),
    (IntegerType(), -1),
    (LongType(), 9223372036854775807),
    (StringType(), "iceberg"),
    (StringType(), "измерение"),
    (DateType(), dt.date(2017, 11, 16)),
    (DateType(), dt.date(1969, 12, 31)),
    (TimeType(), dt.time(22, 31, 8)),
    (TimestampType(), dt.datetime(2017, 11, 16, 22, 31, 8)),
    (DecimalType(9, 2), Decimal("14.20")),
    (BinaryType(), b"\x00\x01\x02\x03"),
    (FixedType(4), b"\x00\x01\x02\x03"),
    (UUIDType(), uuidlib.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
]


def identity_void_vectors():
    for itype, value in IDENTITY_VOID:
        emit(IdentityTransform(), itype, value)
        emit(VoidTransform(), itype, value)


def main(out_path: str) -> int:
    bucket_vectors()
    truncate_vectors()
    time_vectors()
    identity_void_vectors()
    with open(out_path, "w") as fh:
        json.dump(VECTORS, fh, indent=1)
        fh.write("\n")
    print("wrote %d transform vectors to %s" % (len(VECTORS), out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
