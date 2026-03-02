'use strict';

// ── Message type tags ─────────────────────────────────────────────────────
const MSG_CLIENT_HELLO   = 0x01;
const MSG_SERVER_HELLO   = 0x02;
const MSG_AUTH_REQUEST   = 0x03;
const MSG_AUTH_RESPONSE  = 0x04;
const MSG_AUTH_OK        = 0x05;
const MSG_AUTH_FAIL      = 0x06;
const MSG_QUERY_REQUEST  = 0x10;
const MSG_QUERY_RESPONSE = 0x11;
const MSG_ERROR_RESPONSE = 0x12;
const MSG_BATCH_REQUEST  = 0x13;
const MSG_BATCH_RESPONSE = 0x14;
const MSG_PING           = 0x20;
const MSG_PONG           = 0x21;
const MSG_DISCONNECT     = 0x30;
const MSG_DISCONNECT_ACK = 0x31;

// ── Type IDs ──────────────────────────────────────────────────────────────
const TYPE_NULL      = 0x00;
const TYPE_BOOLEAN   = 0x01;
const TYPE_INT32     = 0x02;
const TYPE_INT64     = 0x03;
const TYPE_FLOAT64   = 0x04;
const TYPE_TEXT      = 0x05;
const TYPE_TIMESTAMP = 0x06;
const TYPE_DATE      = 0x07;
const TYPE_JSONB     = 0x08;
const TYPE_DECIMAL   = 0x09;
const TYPE_TIME      = 0x0A;
const TYPE_INTERVAL  = 0x0B;
const TYPE_UUID      = 0x0C;
const TYPE_BYTEA     = 0x0D;
const TYPE_ARRAY     = 0x0E;

// ── Feature flags ─────────────────────────────────────────────────────────
const FEATURE_BATCH_INGEST  = 1n << 2n;
const FEATURE_PIPELINE      = 1n << 3n;
const FEATURE_EPOCH_FENCING = 1n << 4n;
const FEATURE_TLS           = 1n << 5n;
const FEATURE_BINARY_PARAMS = 1n << 6n;

// ── Session flags ─────────────────────────────────────────────────────────
const SESSION_AUTOCOMMIT = 1;

// ── Auth methods ──────────────────────────────────────────────────────────
const AUTH_PASSWORD = 0;

const MAX_FRAME_SIZE = 64 * 1024 * 1024; // 64 MiB
const FRAME_HEADER_SIZE = 5;

// ── Wire Writer ───────────────────────────────────────────────────────────

class WireWriter {
  constructor(initialCapacity = 256) {
    this._buf = Buffer.alloc(initialCapacity);
    this._pos = 0;
  }

  _ensure(bytes) {
    while (this._pos + bytes > this._buf.length) {
      const newBuf = Buffer.alloc(this._buf.length * 2);
      this._buf.copy(newBuf);
      this._buf = newBuf;
    }
  }

  u8(v) {
    this._ensure(1);
    this._buf.writeUInt8(v, this._pos);
    this._pos += 1;
  }

  u16(v) {
    this._ensure(2);
    this._buf.writeUInt16LE(v, this._pos);
    this._pos += 2;
  }

  u32(v) {
    this._ensure(4);
    this._buf.writeUInt32LE(v, this._pos);
    this._pos += 4;
  }

  u64(v) {
    this._ensure(8);
    this._buf.writeBigUInt64LE(BigInt(v), this._pos);
    this._pos += 8;
  }

  i32(v) {
    this._ensure(4);
    this._buf.writeInt32LE(v, this._pos);
    this._pos += 4;
  }

  i64(v) {
    this._ensure(8);
    this._buf.writeBigInt64LE(BigInt(v), this._pos);
    this._pos += 8;
  }

  f64(v) {
    this._ensure(8);
    this._buf.writeDoubleLE(v, this._pos);
    this._pos += 8;
  }

  raw(buf) {
    this._ensure(buf.length);
    buf.copy(this._buf, this._pos);
    this._pos += buf.length;
  }

  stringU16(s) {
    const bytes = Buffer.from(s, 'utf-8');
    this.u16(bytes.length);
    this.raw(bytes);
  }

  stringU32(s) {
    const bytes = Buffer.from(s, 'utf-8');
    this.u32(bytes.length);
    this.raw(bytes);
  }

  toBuffer() {
    return this._buf.slice(0, this._pos);
  }
}

// ── Wire Reader ───────────────────────────────────────────────────────────

class WireReader {
  constructor(buf) {
    this._buf = buf;
    this._pos = 0;
  }

  get remaining() {
    return this._buf.length - this._pos;
  }

  u8() {
    const v = this._buf.readUInt8(this._pos);
    this._pos += 1;
    return v;
  }

  u16() {
    const v = this._buf.readUInt16LE(this._pos);
    this._pos += 2;
    return v;
  }

  u32() {
    const v = this._buf.readUInt32LE(this._pos);
    this._pos += 4;
    return v;
  }

  u64() {
    const v = this._buf.readBigUInt64LE(this._pos);
    this._pos += 8;
    return v;
  }

  i32() {
    const v = this._buf.readInt32LE(this._pos);
    this._pos += 4;
    return v;
  }

  i64() {
    const v = this._buf.readBigInt64LE(this._pos);
    this._pos += 8;
    return v;
  }

  f64() {
    const v = this._buf.readDoubleLE(this._pos);
    this._pos += 8;
    return v;
  }

  raw(n) {
    const v = this._buf.subarray(this._pos, this._pos + n);
    this._pos += n;
    return v;
  }

  stringU16() {
    const len = this.u16();
    const s = this._buf.toString('utf-8', this._pos, this._pos + len);
    this._pos += len;
    return s;
  }

  stringU32() {
    const len = this.u32();
    const s = this._buf.toString('utf-8', this._pos, this._pos + len);
    this._pos += len;
    return s;
  }
}

// ── Frame helpers ─────────────────────────────────────────────────────────

function buildFrame(msgType, payload) {
  const header = Buffer.alloc(FRAME_HEADER_SIZE);
  header.writeUInt8(msgType, 0);
  header.writeUInt32LE(payload.length, 1);
  return Buffer.concat([header, payload]);
}

function decodeValue(reader, typeId) {
  switch (typeId) {
    case TYPE_NULL:
      // NULL is normally handled by the null bitmap; this is a safety fallback.
      return null;
    case TYPE_BOOLEAN:
      return reader.u8() !== 0;
    case TYPE_INT32:
      return reader.i32();
    case TYPE_INT64:
      return reader.i64();
    case TYPE_FLOAT64:
      return reader.f64();
    case TYPE_TEXT:
    case TYPE_JSONB:
      return reader.stringU32();
    case TYPE_TIMESTAMP:
    case TYPE_TIME:
      return reader.i64();
    case TYPE_DATE:
      return reader.i32();
    case TYPE_DECIMAL:
      // Decimal: sent as length-prefixed string to preserve precision
      return reader.stringU32();
    case TYPE_INTERVAL:
      // Interval: months(i32) + days(i32) + microseconds(i64)
      return { months: reader.i32(), days: reader.i32(), microseconds: reader.i64() };
    case TYPE_UUID: {
      // UUID: 16 raw bytes → hex string with dashes
      const raw = reader.raw(16);
      const hex = Buffer.from(raw).toString('hex');
      return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
    }
    case TYPE_BYTEA: {
      const len = reader.u32();
      return Buffer.from(reader.raw(len));
    }
    case TYPE_ARRAY: {
      // Array: element_type(u8) + count(u32) + values
      const elemType = reader.u8();
      const count = reader.u32();
      const arr = new Array(count);
      for (let i = 0; i < count; i++) {
        const isNull = reader.u8();
        arr[i] = isNull ? null : decodeValue(reader, elemType);
      }
      return arr;
    }
    default:
      return reader.stringU32();
  }
}

function decodeRow(reader, colTypes) {
  const ncols = colTypes.length;
  const bitmapBytes = Math.ceil(ncols / 8);
  const bitmap = reader.raw(bitmapBytes);
  const values = new Array(ncols);
  for (let i = 0; i < ncols; i++) {
    const isNull = (bitmap[Math.floor(i / 8)] & (1 << (i % 8))) !== 0;
    if (isNull) {
      values[i] = null;
    } else {
      values[i] = decodeValue(reader, colTypes[i]);
    }
  }
  return values;
}

module.exports = {
  // Message types
  MSG_CLIENT_HELLO, MSG_SERVER_HELLO, MSG_AUTH_REQUEST, MSG_AUTH_RESPONSE,
  MSG_AUTH_OK, MSG_AUTH_FAIL, MSG_QUERY_REQUEST, MSG_QUERY_RESPONSE,
  MSG_ERROR_RESPONSE, MSG_BATCH_REQUEST, MSG_BATCH_RESPONSE,
  MSG_PING, MSG_PONG, MSG_DISCONNECT, MSG_DISCONNECT_ACK,
  // Type IDs
  TYPE_NULL, TYPE_BOOLEAN, TYPE_INT32, TYPE_INT64, TYPE_FLOAT64, TYPE_TEXT,
  TYPE_TIMESTAMP, TYPE_DATE, TYPE_JSONB, TYPE_DECIMAL, TYPE_TIME,
  TYPE_INTERVAL, TYPE_UUID, TYPE_BYTEA, TYPE_ARRAY,
  // Feature/session flags
  FEATURE_BATCH_INGEST, FEATURE_PIPELINE, FEATURE_EPOCH_FENCING,
  FEATURE_TLS, FEATURE_BINARY_PARAMS, SESSION_AUTOCOMMIT,
  // Auth
  AUTH_PASSWORD,
  // Constants
  MAX_FRAME_SIZE, FRAME_HEADER_SIZE,
  // Classes
  WireWriter, WireReader,
  // Functions
  buildFrame, decodeRow,
};
