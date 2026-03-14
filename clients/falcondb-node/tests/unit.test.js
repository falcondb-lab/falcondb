'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  FalconError, ProgrammingError, OperationalError, InternalError,
  AuthenticationError, isRetryable, isFailoverCode, makeError,
  ERR_SYNTAX_ERROR, ERR_NOT_LEADER, ERR_FENCED_EPOCH, ERR_READ_ONLY,
  ERR_SERIALIZATION_CONFLICT, ERR_TIMEOUT, ERR_OVERLOADED,
  ERR_AUTH_FAILED, ERR_INTERNAL_ERROR,
} = require('../src/errors');

const { WireWriter, WireReader } = require('../src/wire');

// ── Error tests ───────────────────────────────────────────────────────────

describe('errors', () => {
  it('FalconError has correct properties', () => {
    const err = new FalconError('test error', 1000, '42601', false, 5n);
    assert.equal(err.message, 'test error');
    assert.equal(err.errorCode, 1000);
    assert.equal(err.sqlstate, '42601');
    assert.equal(err.retryable, false);
    assert.equal(err.serverEpoch, 5n);
    assert.ok(err instanceof Error);
  });

  it('ProgrammingError is not retryable', () => {
    const err = new ProgrammingError('syntax', 1000, '42601', 0n);
    assert.equal(err.retryable, false);
    assert.ok(err instanceof FalconError);
    assert.equal(err.name, 'ProgrammingError');
  });

  it('OperationalError can be retryable', () => {
    const err = new OperationalError('not leader', 2000, '08006', true, 0n);
    assert.equal(err.retryable, true);
    assert.ok(err instanceof FalconError);
  });

  it('isRetryable returns correct values', () => {
    assert.equal(isRetryable(ERR_NOT_LEADER), true);
    assert.equal(isRetryable(ERR_FENCED_EPOCH), true);
    assert.equal(isRetryable(ERR_READ_ONLY), true);
    assert.equal(isRetryable(ERR_SERIALIZATION_CONFLICT), true);
    assert.equal(isRetryable(ERR_TIMEOUT), true);
    assert.equal(isRetryable(ERR_OVERLOADED), true);
    assert.equal(isRetryable(ERR_SYNTAX_ERROR), false);
    assert.equal(isRetryable(ERR_AUTH_FAILED), false);
    assert.equal(isRetryable(ERR_INTERNAL_ERROR), false);
  });

  it('isFailoverCode returns correct values', () => {
    assert.equal(isFailoverCode(ERR_NOT_LEADER), true);
    assert.equal(isFailoverCode(ERR_FENCED_EPOCH), true);
    assert.equal(isFailoverCode(ERR_READ_ONLY), true);
    assert.equal(isFailoverCode(ERR_SERIALIZATION_CONFLICT), false);
    assert.equal(isFailoverCode(ERR_TIMEOUT), false);
  });

  it('makeError creates ProgrammingError for 1xxx codes', () => {
    const err = makeError('bad sql', 1000, '42601', 0n);
    assert.ok(err instanceof ProgrammingError);
    assert.equal(err.retryable, false);
  });

  it('makeError creates OperationalError for 2xxx codes', () => {
    const err = makeError('not leader', 2000, '08006', 0n);
    assert.ok(err instanceof OperationalError);
    assert.equal(err.retryable, true);
  });

  it('makeError creates InternalError for 3xxx codes', () => {
    const err = makeError('internal', 3000, 'XX000', 0n);
    assert.ok(err instanceof InternalError);
    assert.equal(err.retryable, false);
  });

  it('makeError creates AuthenticationError for 4xxx codes', () => {
    const err = makeError('auth fail', 4000, '28P01', 0n);
    assert.ok(err instanceof AuthenticationError);
    assert.equal(err.retryable, false);
  });
});

// ── Wire protocol tests ──────────────────────────────────────────────────

describe('wire', () => {
  it('WireWriter/WireReader u8 roundtrip', () => {
    const w = new WireWriter();
    w.u8(0); w.u8(127); w.u8(255);
    const r = new WireReader(w.toBuffer());
    assert.equal(r.u8(), 0);
    assert.equal(r.u8(), 127);
    assert.equal(r.u8(), 255);
  });

  it('WireWriter/WireReader u16 roundtrip', () => {
    const w = new WireWriter();
    w.u16(0); w.u16(1234); w.u16(65535);
    const r = new WireReader(w.toBuffer());
    assert.equal(r.u16(), 0);
    assert.equal(r.u16(), 1234);
    assert.equal(r.u16(), 65535);
  });

  it('WireWriter/WireReader u32 roundtrip', () => {
    const w = new WireWriter();
    w.u32(0); w.u32(123456); w.u32(4294967295);
    const r = new WireReader(w.toBuffer());
    assert.equal(r.u32(), 0);
    assert.equal(r.u32(), 123456);
    assert.equal(r.u32(), 4294967295);
  });

  it('WireWriter/WireReader u64 roundtrip', () => {
    const w = new WireWriter();
    w.u64(0n); w.u64(123456789012345n);
    const r = new WireReader(w.toBuffer());
    assert.equal(r.u64(), 0n);
    assert.equal(r.u64(), 123456789012345n);
  });

  it('WireWriter/WireReader i32 roundtrip', () => {
    const w = new WireWriter();
    w.i32(-1); w.i32(0); w.i32(2147483647);
    const r = new WireReader(w.toBuffer());
    assert.equal(r.i32(), -1);
    assert.equal(r.i32(), 0);
    assert.equal(r.i32(), 2147483647);
  });

  it('WireWriter/WireReader f64 roundtrip', () => {
    const w = new WireWriter();
    w.f64(3.14159); w.f64(-0.0); w.f64(1e308);
    const r = new WireReader(w.toBuffer());
    assert.equal(r.f64(), 3.14159);
    assert.equal(r.f64(), -0.0);
    assert.equal(r.f64(), 1e308);
  });

  it('WireWriter/WireReader stringU16 roundtrip', () => {
    const w = new WireWriter();
    w.stringU16('hello'); w.stringU16(''); w.stringU16('世界');
    const r = new WireReader(w.toBuffer());
    assert.equal(r.stringU16(), 'hello');
    assert.equal(r.stringU16(), '');
    assert.equal(r.stringU16(), '世界');
  });

  it('WireWriter/WireReader stringU32 roundtrip', () => {
    const w = new WireWriter();
    w.stringU32('test string');
    const r = new WireReader(w.toBuffer());
    assert.equal(r.stringU32(), 'test string');
  });

  it('WireWriter/WireReader raw roundtrip', () => {
    const data = Buffer.from([0x01, 0x02, 0x03, 0xff]);
    const w = new WireWriter();
    w.raw(data);
    const r = new WireReader(w.toBuffer());
    assert.deepEqual(r.raw(4), data);
  });

  it('WireReader remaining tracks correctly', () => {
    const w = new WireWriter();
    w.u8(1); w.u16(2); w.u32(3);
    const r = new WireReader(w.toBuffer());
    assert.equal(r.remaining, 7);
    r.u8();
    assert.equal(r.remaining, 6);
    r.u16();
    assert.equal(r.remaining, 4);
    r.u32();
    assert.equal(r.remaining, 0);
  });

  it('WireWriter auto-grows buffer', () => {
    const w = new WireWriter(4); // tiny initial capacity
    for (let i = 0; i < 100; i++) {
      w.u32(i);
    }
    const r = new WireReader(w.toBuffer());
    for (let i = 0; i < 100; i++) {
      assert.equal(r.u32(), i);
    }
  });
});

// ── Connection config defaults ───────────────────────────────────────────

describe('Connection', () => {
  const { Connection } = require('../src/connection');

  it('has sensible defaults', () => {
    const conn = new Connection();
    assert.equal(conn._host, 'localhost');
    assert.equal(conn._port, 6543);
    assert.equal(conn._database, 'falcon');
    assert.equal(conn._user, 'falcon');
    assert.equal(conn._password, '');
    assert.equal(conn.autocommit, true);
    assert.equal(conn.closed, false);
  });

  it('accepts custom config', () => {
    const conn = new Connection({
      host: '10.0.0.1',
      port: 5433,
      database: 'mydb',
      user: 'admin',
      password: 'secret',
      autocommit: false,
    });
    assert.equal(conn._host, '10.0.0.1');
    assert.equal(conn._port, 5433);
    assert.equal(conn._database, 'mydb');
    assert.equal(conn._user, 'admin');
    assert.equal(conn._password, 'secret');
    assert.equal(conn.autocommit, false);
  });

  it('throws when querying a closed connection', () => {
    const conn = new Connection();
    conn._closed = true;
    assert.throws(() => conn._checkOpen(), FalconError);
  });
});

// ── FailoverConnection config ────────────────────────────────────────────

describe('FailoverConnection', () => {
  const { FailoverConnection } = require('../src/ha');

  it('requires at least one seed', () => {
    assert.throws(() => new FailoverConnection({ seeds: [] }), FalconError);
  });

  it('accepts valid config', () => {
    const fc = new FailoverConnection({
      seeds: [{ host: 'node1', port: 6543 }, { host: 'node2', port: 6543 }],
      database: 'mydb',
      maxRetries: 5,
    });
    assert.equal(fc._seeds.length, 2);
    assert.equal(fc._database, 'mydb');
    assert.equal(fc._maxRetries, 5);
    assert.equal(fc.failoverCount, 0);
  });
});

// ── ConnectionPool config ────────────────────────────────────────────────

describe('ConnectionPool', () => {
  const { ConnectionPool } = require('../src/pool');

  it('has sensible defaults', () => {
    const pool = new ConnectionPool({ connection: { host: 'localhost' } });
    assert.equal(pool._minSize, 2);
    assert.equal(pool._maxSize, 20);
    assert.equal(pool.size, 0);
    assert.equal(pool.idleCount, 0);
    assert.equal(pool.activeCount, 0);
  });
});
