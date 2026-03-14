# FalconDB Node.js SDK

Native protocol driver for [FalconDB](https://github.com/falcondb-lab/falcondb) with
HA failover, connection pooling, and TypeScript type definitions.

## Installation

```bash
npm install falcondb
```

Or from source:

```bash
cd clients/falcondb-node
npm install
```

## Quick Start

```js
const { connect } = require('falcondb');

async function main() {
  const conn = await connect({
    host: 'localhost',
    port: 6543,
    user: 'falcon',
    password: 'secret',
  });

  try {
    const result = await conn.query('SELECT id, name FROM users WHERE id = 42');
    console.log(result.columns); // [{ name: 'id', ... }, { name: 'name', ... }]
    console.log(result.rows);    // [[42, 'Alice']]
  } finally {
    await conn.close();
  }
}

main().catch(console.error);
```

## Connection Pool

```js
const { ConnectionPool } = require('falcondb');

const pool = new ConnectionPool({
  connection: {
    host: 'localhost',
    port: 6543,
    user: 'falcon',
    password: 'secret',
  },
  minSize: 2,
  maxSize: 20,
});

await pool.init();

// Option 1: manual acquire/release
const conn = await pool.acquire();
try {
  const result = await conn.query('SELECT 1');
} finally {
  pool.release(conn);
}

// Option 2: withConnection helper
const result = await pool.withConnection(async (conn) => {
  return conn.query('INSERT INTO orders (id, total) VALUES (1, 99.95)');
});

await pool.close();
```

## HA Failover

```js
const { FailoverConnection } = require('falcondb');

const conn = new FailoverConnection({
  seeds: [
    { host: 'node1', port: 6543 },
    { host: 'node2', port: 6543 },
    { host: 'node3', port: 6543 },
  ],
  database: 'mydb',
  user: 'falcon',
  password: 'secret',
  maxRetries: 3,
});

await conn.connect();

// Automatic failover on NOT_LEADER / FENCED_EPOCH
const result = await conn.query('SELECT * FROM orders');
console.log(result.rows);

await conn.close();
```

## Transactions

```js
const conn = await connect({ autocommit: false });
try {
  await conn.exec('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
  await conn.exec('UPDATE accounts SET balance = balance + 100 WHERE id = 2');
  await conn.commit();
} catch (err) {
  await conn.rollback();
  throw err;
} finally {
  await conn.close();
}
```

## Error Handling

```js
const { FalconError, ProgrammingError, OperationalError } = require('falcondb');

try {
  await conn.query('SELET 1'); // typo
} catch (err) {
  if (err instanceof ProgrammingError) {
    console.log(err.errorCode); // 1000
    console.log(err.sqlstate);  // 42601
    console.log(err.retryable); // false
  }
}

try {
  await conn.exec('INSERT INTO t VALUES (1)');
} catch (err) {
  if (err instanceof OperationalError && err.retryable) {
    // Safe to retry (e.g., serialization conflict)
  }
}
```

## Error Hierarchy

| Exception | Error Codes | Retryable |
|-----------|------------|-----------|
| `ProgrammingError` | 1000–1999 | No |
| `OperationalError` | 2000–2999 | Yes (NOT_LEADER, FENCED_EPOCH, SERIALIZATION_CONFLICT) |
| `InternalError` | 3000–3999 | Yes (TIMEOUT, OVERLOADED) / No (INTERNAL_ERROR) |
| `AuthenticationError` | 4000–4001 | No |

## TypeScript

Full type definitions are included (`src/index.d.ts`):

```ts
import { connect, QueryResult, FailoverConnection } from 'falcondb';

const conn = await connect({ host: 'localhost', port: 6543 });
const result: QueryResult = await conn.query('SELECT 1 AS num');
```

## Requirements

- Node.js 18+
- No external dependencies (stdlib only)

## Running Tests

```bash
cd clients/falcondb-node
node --test tests/
```

## License

Apache-2.0
