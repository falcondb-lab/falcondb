"""FalconDB native protocol wire format — binary encode/decode helpers.

All multi-byte integers are little-endian.
Frame format: [msg_type:u8][length:u32 LE][payload:length bytes]
"""

from __future__ import annotations

import struct
from typing import Any

# ── Message type tags ────────────────────────────────────────────────────

MSG_CLIENT_HELLO = 0x01
MSG_SERVER_HELLO = 0x02
MSG_AUTH_REQUEST = 0x03
MSG_AUTH_RESPONSE = 0x04
MSG_AUTH_OK = 0x05
MSG_AUTH_FAIL = 0x06
MSG_QUERY_REQUEST = 0x10
MSG_QUERY_RESPONSE = 0x11
MSG_ERROR_RESPONSE = 0x12
MSG_BATCH_REQUEST = 0x13
MSG_BATCH_RESPONSE = 0x14
MSG_PING = 0x20
MSG_PONG = 0x21
MSG_DISCONNECT = 0x30
MSG_DISCONNECT_ACK = 0x31

# ── Type IDs ─────────────────────────────────────────────────────────────

TYPE_NULL = 0x00
TYPE_BOOLEAN = 0x01
TYPE_INT32 = 0x02
TYPE_INT64 = 0x03
TYPE_FLOAT64 = 0x04
TYPE_TEXT = 0x05
TYPE_TIMESTAMP = 0x06
TYPE_DATE = 0x07
TYPE_JSONB = 0x08
TYPE_DECIMAL = 0x09
TYPE_TIME = 0x0A
TYPE_INTERVAL = 0x0B
TYPE_UUID = 0x0C
TYPE_BYTEA = 0x0D
TYPE_ARRAY = 0x0E

# ── Feature flags ────────────────────────────────────────────────────────

FEATURE_COMPRESSION_LZ4 = 1
FEATURE_COMPRESSION_ZSTD = 1 << 1
FEATURE_BATCH_INGEST = 1 << 2
FEATURE_PIPELINE = 1 << 3
FEATURE_EPOCH_FENCING = 1 << 4
FEATURE_TLS = 1 << 5
FEATURE_BINARY_PARAMS = 1 << 6

# ── Auth methods ─────────────────────────────────────────────────────────

AUTH_PASSWORD = 0
AUTH_TOKEN = 1

# ── Session flags ────────────────────────────────────────────────────────

SESSION_AUTOCOMMIT = 1
SESSION_READ_ONLY = 1 << 1

# Frame header: 5 bytes (1 byte msg_type + 4 bytes u32 length)
FRAME_HEADER_SIZE = 5
MAX_FRAME_SIZE = 64 * 1024 * 1024  # 64 MiB

# ── Type name mapping ────────────────────────────────────────────────────

TYPE_NAMES: dict[int, str] = {
    TYPE_NULL: "NULL",
    TYPE_BOOLEAN: "BOOLEAN",
    TYPE_INT32: "INTEGER",
    TYPE_INT64: "BIGINT",
    TYPE_FLOAT64: "DOUBLE",
    TYPE_TEXT: "TEXT",
    TYPE_TIMESTAMP: "TIMESTAMP",
    TYPE_DATE: "DATE",
    TYPE_JSONB: "JSONB",
    TYPE_DECIMAL: "DECIMAL",
    TYPE_TIME: "TIME",
    TYPE_INTERVAL: "INTERVAL",
    TYPE_UUID: "UUID",
    TYPE_BYTEA: "BYTEA",
    TYPE_ARRAY: "ARRAY",
}


# ── Writer helpers ───────────────────────────────────────────────────────


class WireWriter:
    """Accumulates bytes for a native protocol payload."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def u8(self, v: int) -> None:
        self._buf.append(v & 0xFF)

    def u16(self, v: int) -> None:
        self._buf.extend(struct.pack("<H", v))

    def u32(self, v: int) -> None:
        self._buf.extend(struct.pack("<I", v))

    def u64(self, v: int) -> None:
        self._buf.extend(struct.pack("<Q", v))

    def i32(self, v: int) -> None:
        self._buf.extend(struct.pack("<i", v))

    def i64(self, v: int) -> None:
        self._buf.extend(struct.pack("<q", v))

    def f64(self, v: float) -> None:
        self._buf.extend(struct.pack("<d", v))

    def string_u16(self, s: str) -> None:
        encoded = s.encode("utf-8")
        self.u16(len(encoded))
        self._buf.extend(encoded)

    def string_u32(self, s: str) -> None:
        encoded = s.encode("utf-8")
        self.u32(len(encoded))
        self._buf.extend(encoded)

    def raw(self, data: bytes | bytearray) -> None:
        self._buf.extend(data)

    def to_bytes(self) -> bytes:
        return bytes(self._buf)


def build_frame(msg_type: int, payload: bytes) -> bytes:
    """Build a complete frame: [msg_type:u8][length:u32 LE][payload]."""
    return struct.pack("<BI", msg_type, len(payload)) + payload


# ── Reader helpers ───────────────────────────────────────────────────────


class WireReader:
    """Reads values from a bytes buffer at a tracked offset."""

    __slots__ = ("_data", "_pos")

    def __init__(self, data: bytes | bytearray | memoryview) -> None:
        self._data = bytes(data)
        self._pos = 0

    @property
    def remaining(self) -> int:
        return len(self._data) - self._pos

    def u8(self) -> int:
        v = self._data[self._pos]
        self._pos += 1
        return v

    def u16(self) -> int:
        v = struct.unpack_from("<H", self._data, self._pos)[0]
        self._pos += 2
        return v

    def u32(self) -> int:
        v = struct.unpack_from("<I", self._data, self._pos)[0]
        self._pos += 4
        return v

    def u64(self) -> int:
        v = struct.unpack_from("<Q", self._data, self._pos)[0]
        self._pos += 8
        return v

    def i32(self) -> int:
        v = struct.unpack_from("<i", self._data, self._pos)[0]
        self._pos += 4
        return v

    def i64(self) -> int:
        v = struct.unpack_from("<q", self._data, self._pos)[0]
        self._pos += 8
        return v

    def f64(self) -> float:
        v = struct.unpack_from("<d", self._data, self._pos)[0]
        self._pos += 8
        return v

    def raw(self, n: int) -> bytes:
        v = self._data[self._pos : self._pos + n]
        self._pos += n
        return v

    def string_u16(self) -> str:
        length = self.u16()
        s = self._data[self._pos : self._pos + length].decode("utf-8")
        self._pos += length
        return s

    def string_u32(self) -> str:
        length = self.u32()
        s = self._data[self._pos : self._pos + length].decode("utf-8")
        self._pos += length
        return s


def decode_value(reader: WireReader, type_id: int) -> Any:
    """Decode a single typed value from the wire."""
    if type_id == TYPE_BOOLEAN:
        return bool(reader.u8())
    elif type_id == TYPE_INT32:
        return reader.i32()
    elif type_id == TYPE_INT64:
        return reader.i64()
    elif type_id == TYPE_FLOAT64:
        return reader.f64()
    elif type_id == TYPE_TEXT:
        return reader.string_u32()
    elif type_id == TYPE_TIMESTAMP:
        return reader.i64()
    elif type_id == TYPE_DATE:
        return reader.i32()
    elif type_id == TYPE_JSONB:
        return reader.string_u32()
    elif type_id == TYPE_TIME:
        return reader.i64()
    elif type_id == TYPE_BYTEA:
        length = reader.u32()
        return reader.raw(length)
    else:
        return reader.string_u32()


def encode_value(writer: WireWriter, type_id: int, value: Any) -> None:
    """Encode a single typed value onto the wire."""
    if type_id == TYPE_BOOLEAN:
        writer.u8(1 if value else 0)
    elif type_id == TYPE_INT32:
        writer.i32(int(value))
    elif type_id == TYPE_INT64:
        writer.i64(int(value))
    elif type_id == TYPE_FLOAT64:
        writer.f64(float(value))
    elif type_id in (TYPE_TEXT, TYPE_JSONB):
        writer.string_u32(str(value))
    elif type_id in (TYPE_TIMESTAMP, TYPE_TIME):
        writer.i64(int(value))
    elif type_id == TYPE_DATE:
        writer.i32(int(value))
    elif type_id == TYPE_BYTEA:
        data = bytes(value)
        writer.u32(len(data))
        writer.raw(data)
    else:
        writer.string_u32(str(value))


def encode_row(writer: WireWriter, col_types: list[int], values: list[Any]) -> None:
    """Encode a row with null bitmap + typed values."""
    ncols = len(col_types)
    bitmap_bytes = (ncols + 7) // 8
    bitmap = bytearray(bitmap_bytes)
    for i in range(ncols):
        if values[i] is None:
            bitmap[i // 8] |= 1 << (i % 8)
    writer.raw(bitmap)
    for i in range(ncols):
        if values[i] is not None:
            encode_value(writer, col_types[i], values[i])


def decode_row(reader: WireReader, col_types: list[int]) -> list[Any]:
    """Decode a row: null bitmap + typed values."""
    ncols = len(col_types)
    bitmap_bytes = (ncols + 7) // 8
    bitmap = reader.raw(bitmap_bytes)
    values: list[Any] = []
    for i in range(ncols):
        is_null = (bitmap[i // 8] & (1 << (i % 8))) != 0
        if is_null:
            values.append(None)
        else:
            values.append(decode_value(reader, col_types[i]))
    return values
