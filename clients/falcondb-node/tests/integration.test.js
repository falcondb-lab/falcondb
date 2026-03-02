/**
 * FalconDB Node.js SDK — Integration Tests
 *
 * These tests run against a live FalconDB instance. They are skipped
 * automatically when FALCON_TEST_HOST is not set.
 *
 * Usage:
 *   FALCON_TEST_HOST=127.0.0.1 FALCON_TEST_PORT=5443 npm test -- --grep integration
 *
 * Environment variables:
 *   FALCON_TEST_HOST  — FalconDB host (required to run)
 *   FALCON_TEST_PORT  — PG wire port (default: 5443)
 *   FALCON_TEST_USER  — Username     (default: falcon)
 *   FALCON_TEST_PASS  — Password     (default: "")
 *   FALCON_TEST_DB    — Database     (default: falcon)
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const SKIP = !process.env.FALCON_TEST_HOST;

// Conditionally require to avoid import errors when not running integration
let Connection, ConnectionPool, FailoverConnection;
if (!SKIP) {
  ({ Connection, ConnectionPool, FailoverConnection } = require('../src/index'));
}

const cfg = {
  host: process.env.FALCON_TEST_HOST || '127.0.0.1',
  port: parseInt(process.env.FALCON_TEST_PORT || '5443', 10),
  user: process.env.FALCON_TEST_USER || 'falcon',
  password: process.env.FALCON_TEST_PASS || '',
  database: process.env.FALCON_TEST_DB || 'falcon',
};

// ---------------------------------------------------------------------------
// Helper: create + connect a single connection
// ---------------------------------------------------------------------------
async function connect() {
  const conn = new Connection(cfg);
  await conn.connect();
  return conn;
}

// ---------------------------------------------------------------------------
// Integration test suite
// ---------------------------------------------------------------------------
describe('integration: connection lifecycle', { skip: SKIP }, () => {
  let conn;

  before(async () => {
    conn = await connect();
  });

  after(async () => {
    if (conn) await conn.close();
  });

  it('should connect and ping', async () => {
    const ok = await conn.ping(3000);
    assert.equal(ok, true);
  });

  it('should execute SELECT 1', async () => {
    const result = await conn.query('SELECT 1 AS val');
    assert.ok(result);
    assert.ok(result.rows.length >= 1);
    assert.equal(result.rows[0].val ?? result.rows[0][0], 1);
  });

  it('should handle parameterised queries', async () => {
    const result = await conn.query('SELECT $1::int + $2::int AS sum', [3, 4]);
    assert.ok(result);
    const sum = result.rows[0].sum ?? result.rows[0][0];
    assert.equal(sum, 7);
  });
});

describe('integration: DDL and DML', { skip: SKIP }, () => {
  let conn;
  const table = `_sdk_integ_${Date.now()}`;

  before(async () => {
    conn = await connect();
    await conn.query(`CREATE TABLE IF NOT EXISTS ${table} (id INT PRIMARY KEY, name TEXT)`);
  });

  after(async () => {
    if (conn) {
      await conn.query(`DROP TABLE IF EXISTS ${table}`).catch(() => {});
      await conn.close();
    }
  });

  it('should INSERT and SELECT', async () => {
    await conn.query(`INSERT INTO ${table} (id, name) VALUES (1, 'alice')`);
    const res = await conn.query(`SELECT name FROM ${table} WHERE id = 1`);
    assert.ok(res.rows.length === 1);
    const name = res.rows[0].name ?? res.rows[0][0];
    assert.equal(name, 'alice');
  });

  it('should UPDATE', async () => {
    await conn.query(`UPDATE ${table} SET name = 'bob' WHERE id = 1`);
    const res = await conn.query(`SELECT name FROM ${table} WHERE id = 1`);
    const name = res.rows[0].name ?? res.rows[0][0];
    assert.equal(name, 'bob');
  });

  it('should DELETE', async () => {
    await conn.query(`DELETE FROM ${table} WHERE id = 1`);
    const res = await conn.query(`SELECT count(*) AS cnt FROM ${table}`);
    const cnt = res.rows[0].cnt ?? res.rows[0][0];
    assert.equal(Number(cnt), 0);
  });
});

describe('integration: transactions', { skip: SKIP }, () => {
  let conn;
  const table = `_sdk_integ_txn_${Date.now()}`;

  before(async () => {
    conn = await connect();
    await conn.query(`CREATE TABLE IF NOT EXISTS ${table} (id INT PRIMARY KEY, val INT)`);
  });

  after(async () => {
    if (conn) {
      await conn.query(`DROP TABLE IF EXISTS ${table}`).catch(() => {});
      await conn.close();
    }
  });

  it('should COMMIT a transaction', async () => {
    await conn.query('BEGIN');
    await conn.query(`INSERT INTO ${table} (id, val) VALUES (1, 100)`);
    await conn.query('COMMIT');
    const res = await conn.query(`SELECT val FROM ${table} WHERE id = 1`);
    assert.equal(res.rows[0].val ?? res.rows[0][0], 100);
  });

  it('should ROLLBACK a transaction', async () => {
    await conn.query('BEGIN');
    await conn.query(`UPDATE ${table} SET val = 999 WHERE id = 1`);
    await conn.query('ROLLBACK');
    const res = await conn.query(`SELECT val FROM ${table} WHERE id = 1`);
    assert.equal(res.rows[0].val ?? res.rows[0][0], 100);
  });
});

describe('integration: connection pool', { skip: SKIP }, () => {
  let pool;

  before(() => {
    pool = new ConnectionPool({ ...cfg, minSize: 1, maxSize: 3 });
  });

  after(async () => {
    if (pool) await pool.close();
  });

  it('should acquire and release connections', async () => {
    const conn1 = await pool.acquire();
    assert.ok(conn1);
    const ok = await conn1.ping(3000);
    assert.equal(ok, true);
    pool.release(conn1);
  });

  it('should execute via withConnection helper', async () => {
    const result = await pool.withConnection(async (c) => {
      return c.query('SELECT 42 AS answer');
    });
    assert.ok(result);
    assert.equal(result.rows[0].answer ?? result.rows[0][0], 42);
  });
});

describe('integration: SHOW commands', { skip: SKIP }, () => {
  let conn;

  before(async () => {
    conn = await connect();
  });

  after(async () => {
    if (conn) await conn.close();
  });

  it('should execute SHOW falcon.txn_stats', async () => {
    const res = await conn.query('SHOW falcon.txn_stats');
    assert.ok(res);
  });

  it('should execute SHOW falcon.connections', async () => {
    const res = await conn.query('SHOW falcon.connections');
    assert.ok(res);
  });

  it('should execute SHOW falcon.health_score', async () => {
    const res = await conn.query('SHOW falcon.health_score');
    assert.ok(res);
  });
});

describe('integration: error handling', { skip: SKIP }, () => {
  let conn;

  before(async () => {
    conn = await connect();
  });

  after(async () => {
    if (conn) await conn.close();
  });

  it('should reject invalid SQL with an error', async () => {
    await assert.rejects(
      () => conn.query('SELECTTTT'),
      (err) => {
        assert.ok(err.message);
        return true;
      },
    );
  });

  it('should reject reference to non-existent table', async () => {
    await assert.rejects(
      () => conn.query('SELECT * FROM _nonexistent_table_xyz_12345'),
      (err) => {
        assert.ok(err.message);
        return true;
      },
    );
  });
});
