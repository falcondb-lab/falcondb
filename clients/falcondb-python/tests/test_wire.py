"""Tests for the wire protocol encode/decode helpers."""

import struct
from falcondb.wire import (
    WireWriter,
    WireReader,
    build_frame,
    encode_value,
    decode_value,
    encode_row,
    decode_row,
    TYPE_BOOLEAN,
    TYPE_INT32,
    TYPE_INT64,
    TYPE_FLOAT64,
    TYPE_TEXT,
    TYPE_TIMESTAMP,
    TYPE_DATE,
    TYPE_BYTEA,
    MSG_PING,
)


class TestWireWriter:
    def test_u8(self):
        w = WireWriter()
        w.u8(0x42)
        assert w.to_bytes() == b"\x42"

    def test_u16_le(self):
        w = WireWriter()
        w.u16(0x0102)
        assert w.to_bytes() == b"\x02\x01"

    def test_u32_le(self):
        w = WireWriter()
        w.u32(0x04030201)
        assert w.to_bytes() == b"\x01\x02\x03\x04"

    def test_u64_le(self):
        w = WireWriter()
        w.u64(1)
        assert w.to_bytes() == b"\x01\x00\x00\x00\x00\x00\x00\x00"

    def test_i32_negative(self):
        w = WireWriter()
        w.i32(-1)
        assert w.to_bytes() == b"\xff\xff\xff\xff"

    def test_i64_negative(self):
        w = WireWriter()
        w.i64(-1)
        assert w.to_bytes() == b"\xff" * 8

    def test_f64(self):
        w = WireWriter()
        w.f64(3.14)
        expected = struct.pack("<d", 3.14)
        assert w.to_bytes() == expected

    def test_string_u16(self):
        w = WireWriter()
        w.string_u16("hello")
        data = w.to_bytes()
        assert data[:2] == b"\x05\x00"  # length = 5, LE
        assert data[2:] == b"hello"

    def test_string_u32(self):
        w = WireWriter()
        w.string_u32("hi")
        data = w.to_bytes()
        assert data[:4] == b"\x02\x00\x00\x00"
        assert data[4:] == b"hi"


class TestWireReader:
    def test_u8(self):
        r = WireReader(b"\x42")
        assert r.u8() == 0x42

    def test_u16(self):
        r = WireReader(b"\x02\x01")
        assert r.u16() == 0x0102

    def test_u32(self):
        r = WireReader(b"\x01\x02\x03\x04")
        assert r.u32() == 0x04030201

    def test_i32_negative(self):
        r = WireReader(b"\xff\xff\xff\xff")
        assert r.i32() == -1

    def test_f64(self):
        data = struct.pack("<d", 2.718)
        r = WireReader(data)
        assert abs(r.f64() - 2.718) < 1e-10

    def test_string_u16(self):
        data = b"\x03\x00abc"
        r = WireReader(data)
        assert r.string_u16() == "abc"

    def test_remaining(self):
        r = WireReader(b"\x01\x02\x03")
        r.u8()
        assert r.remaining == 2


class TestBuildFrame:
    def test_ping_frame(self):
        frame = build_frame(MSG_PING, b"")
        assert frame == b"\x20\x00\x00\x00\x00"

    def test_frame_with_payload(self):
        frame = build_frame(0x10, b"\x01\x02")
        assert frame[0] == 0x10
        assert struct.unpack_from("<I", frame, 1)[0] == 2
        assert frame[5:] == b"\x01\x02"


class TestValueCodec:
    def test_bool_roundtrip(self):
        w = WireWriter()
        encode_value(w, TYPE_BOOLEAN, True)
        r = WireReader(w.to_bytes())
        assert decode_value(r, TYPE_BOOLEAN) is True

    def test_int32_roundtrip(self):
        w = WireWriter()
        encode_value(w, TYPE_INT32, -42)
        r = WireReader(w.to_bytes())
        assert decode_value(r, TYPE_INT32) == -42

    def test_int64_roundtrip(self):
        w = WireWriter()
        encode_value(w, TYPE_INT64, 2**40)
        r = WireReader(w.to_bytes())
        assert decode_value(r, TYPE_INT64) == 2**40

    def test_float64_roundtrip(self):
        w = WireWriter()
        encode_value(w, TYPE_FLOAT64, 3.14159)
        r = WireReader(w.to_bytes())
        assert abs(decode_value(r, TYPE_FLOAT64) - 3.14159) < 1e-10

    def test_text_roundtrip(self):
        w = WireWriter()
        encode_value(w, TYPE_TEXT, "hello world")
        r = WireReader(w.to_bytes())
        assert decode_value(r, TYPE_TEXT) == "hello world"

    def test_text_unicode(self):
        w = WireWriter()
        encode_value(w, TYPE_TEXT, "你好世界")
        r = WireReader(w.to_bytes())
        assert decode_value(r, TYPE_TEXT) == "你好世界"

    def test_timestamp_roundtrip(self):
        ts = 1700000000_000_000
        w = WireWriter()
        encode_value(w, TYPE_TIMESTAMP, ts)
        r = WireReader(w.to_bytes())
        assert decode_value(r, TYPE_TIMESTAMP) == ts

    def test_date_roundtrip(self):
        w = WireWriter()
        encode_value(w, TYPE_DATE, 19723)
        r = WireReader(w.to_bytes())
        assert decode_value(r, TYPE_DATE) == 19723

    def test_bytea_roundtrip(self):
        data = b"\xde\xad\xbe\xef"
        w = WireWriter()
        encode_value(w, TYPE_BYTEA, data)
        r = WireReader(w.to_bytes())
        assert decode_value(r, TYPE_BYTEA) == data


class TestRowCodec:
    def test_row_roundtrip(self):
        col_types = [TYPE_INT64, TYPE_TEXT, TYPE_FLOAT64]
        values = [42, "alice", 3.14]
        w = WireWriter()
        encode_row(w, col_types, values)
        r = WireReader(w.to_bytes())
        decoded = decode_row(r, col_types)
        assert decoded[0] == 42
        assert decoded[1] == "alice"
        assert abs(decoded[2] - 3.14) < 1e-10

    def test_row_with_nulls(self):
        col_types = [TYPE_INT64, TYPE_TEXT, TYPE_BOOLEAN]
        values = [1, None, True]
        w = WireWriter()
        encode_row(w, col_types, values)
        r = WireReader(w.to_bytes())
        decoded = decode_row(r, col_types)
        assert decoded[0] == 1
        assert decoded[1] is None
        assert decoded[2] is True

    def test_row_all_nulls(self):
        col_types = [TYPE_INT32, TYPE_TEXT]
        values = [None, None]
        w = WireWriter()
        encode_row(w, col_types, values)
        r = WireReader(w.to_bytes())
        decoded = decode_row(r, col_types)
        assert decoded == [None, None]
