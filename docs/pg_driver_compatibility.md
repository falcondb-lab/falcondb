# Standard PostgreSQL Driver Compatibility

FalconDB implements the PostgreSQL wire protocol (v3), which means **any standard
PostgreSQL client driver can connect directly** to FalconDB without modification.
This document lists tested drivers, connection examples, and known limitations.

## Supported Connection Methods

FalconDB exposes two connection interfaces:

| Interface | Port (default) | Protocol | Use Case |
|-----------|:-------------:|----------|----------|
| PG Wire Protocol | `5433` | PostgreSQL v3 | Standard PG drivers (psql, psycopg2, pgx, node-postgres, pgjdbc) |
| Native Protocol | `6543` | FalconDB binary | Native SDKs (Java, Go, Python, Node.js) — HA-aware, epoch fencing |

> **Recommendation**: Use the PG wire protocol port for ecosystem compatibility.
> Use the native protocol only when you need HA failover or epoch fencing features.

---

## Tested Drivers

### Python — psycopg2 / psycopg3

```bash
pip install psycopg2-binary
# or
pip install psycopg[binary]
```

```python
# psycopg2
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname="falcon",
    user="falcon",
    password="secret",
)
cur = conn.cursor()
cur.execute("SELECT id, name FROM users WHERE id = %s", (42,))
row = cur.fetchone()
print(row)  # (42, 'Alice')
conn.close()
```

```python
# psycopg3 (async)
import psycopg

async with await psycopg.AsyncConnection.connect(
    "host=localhost port=5433 dbname=falcon user=falcon password=secret"
) as conn:
    async with conn.cursor() as cur:
        await cur.execute("SELECT id, name FROM users WHERE id = %s", (42,))
        row = await cur.fetchone()
        print(row)
```

**Status**: ✅ Fully compatible (Simple Query + Extended Query protocol)

---

### Go — pgx

```bash
go get github.com/jackc/pgx/v5
```

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/jackc/pgx/v5"
)

func main() {
    ctx := context.Background()
    conn, err := pgx.Connect(ctx, "postgres://falcon:secret@localhost:5433/falcon")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close(ctx)

    var id int
    var name string
    err = conn.QueryRow(ctx, "SELECT id, name FROM users WHERE id = $1", 42).Scan(&id, &name)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("id=%d name=%s\n", id, name)
}
```

**Status**: ✅ Fully compatible (Extended Query protocol, SCRAM-SHA-256 auth)

---

### Go — database/sql + lib/pq

```go
import (
    "database/sql"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres", "host=localhost port=5433 user=falcon password=secret dbname=falcon sslmode=disable")
```

**Status**: ✅ Compatible (Simple Query protocol)

---

### Node.js — node-postgres (pg)

```bash
npm install pg
```

```js
const { Client } = require('pg');

const client = new Client({
  host: 'localhost',
  port: 5433,
  database: 'falcon',
  user: 'falcon',
  password: 'secret',
});

await client.connect();

const res = await client.query('SELECT id, name FROM users WHERE id = $1', [42]);
console.log(res.rows[0]); // { id: 42, name: 'Alice' }

await client.end();
```

**Connection Pool:**

```js
const { Pool } = require('pg');

const pool = new Pool({
  host: 'localhost',
  port: 5433,
  database: 'falcon',
  user: 'falcon',
  password: 'secret',
  max: 20,
});

const res = await pool.query('SELECT NOW()');
console.log(res.rows[0]);

await pool.end();
```

**Status**: ✅ Fully compatible (Extended Query protocol, parameterized queries)

---

### Java — pgjdbc

```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.1</version>
</dependency>
```

```java
String url = "jdbc:postgresql://localhost:5433/falcon";
Properties props = new Properties();
props.setProperty("user", "falcon");
props.setProperty("password", "secret");

Connection conn = DriverManager.getConnection(url, props);
PreparedStatement ps = conn.prepareStatement("SELECT id, name FROM users WHERE id = ?");
ps.setInt(1, 42);
ResultSet rs = ps.executeQuery();
while (rs.next()) {
    System.out.printf("id=%d name=%s%n", rs.getInt("id"), rs.getString("name"));
}
conn.close();
```

**Status**: ✅ Tested in CI (JDBC smoke tests)

---

### Rust — tokio-postgres

```toml
[dependencies]
tokio-postgres = "0.7"
tokio = { version = "1", features = ["full"] }
```

```rust
let (client, connection) = tokio_postgres::connect(
    "host=localhost port=5433 user=falcon password=secret dbname=falcon",
    tokio_postgres::NoTls,
).await?;

tokio::spawn(async move { connection.await.unwrap(); });

let row = client.query_one("SELECT id, name FROM users WHERE id = $1", &[&42i32]).await?;
let id: i32 = row.get(0);
let name: &str = row.get(1);
```

**Status**: ✅ Compatible (Extended Query protocol)

---

### C# — Npgsql

```csharp
await using var conn = new NpgsqlConnection("Host=localhost;Port=5433;Database=falcon;Username=falcon;Password=secret");
await conn.OpenAsync();

await using var cmd = new NpgsqlCommand("SELECT id, name FROM users WHERE id = @id", conn);
cmd.Parameters.AddWithValue("id", 42);

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine($"id={reader.GetInt32(0)} name={reader.GetString(1)}");
}
```

**Status**: ✅ Expected compatible (PG wire protocol v3)

---

## Authentication Methods

| Method | Standard PG Driver | FalconDB Support |
|--------|:------------------:|:----------------:|
| Trust (no password) | ✅ | ✅ |
| Password (cleartext) | ✅ | ✅ |
| MD5 | ✅ | ✅ |
| SCRAM-SHA-256 | ✅ | ✅ |

---

## Connection String Formats

FalconDB accepts standard PostgreSQL connection strings:

```
# Key-value format
host=localhost port=5433 dbname=falcon user=falcon password=secret sslmode=disable

# URI format
postgresql://falcon:secret@localhost:5433/falcon?sslmode=disable
```

---

## Known Limitations with Standard PG Drivers

| Limitation | Details |
|-----------|---------|
| Stored procedures / PL/pgSQL | Not supported — returns `0A000` |
| LISTEN/NOTIFY | Supported in Simple Query mode only |
| COPY BINARY format | Text COPY supported; binary COPY may have limitations |
| Large objects (lo_*) | Not supported |
| Advisory locks | Not supported |
| `pg_catalog` completeness | Subset implemented — some introspection queries may fail |
| ORM schema introspection | May need manual schema mapping for ORMs relying on `pg_catalog` |

## ORM Compatibility Notes

| ORM/Framework | Status | Notes |
|--------------|--------|-------|
| SQLAlchemy (Python) | ⚠️ Partial | DDL works; `pg_catalog` introspection limited |
| Django ORM | ⚠️ Partial | Migrations require `--fake` for unsupported features |
| GORM (Go) | ⚠️ Partial | Basic CRUD works; AutoMigrate may need manual schema |
| Prisma (Node.js) | ⚠️ Partial | Query engine works; `prisma db push` limited |
| TypeORM (Node.js) | ⚠️ Partial | Repository pattern works; migrations need review |

> **Best practice**: Use standard PG drivers for query execution. For schema
> management, use FalconDB's SQL directly rather than ORM migration tools.

---

## When to Use Native SDK vs Standard PG Driver

| Criteria | Standard PG Driver | FalconDB Native SDK |
|----------|:------------------:|:-------------------:|
| Ecosystem compatibility | ✅ Best | ❌ FalconDB only |
| ORM integration | ✅ Works | ❌ Not supported |
| HA-aware failover | ❌ Manual | ✅ Built-in |
| Epoch fencing | ❌ Not available | ✅ Automatic |
| Connection pooling | ✅ Driver-level | ✅ Built-in |
| Zero external deps | ❌ Requires driver pkg | ✅ Stdlib only |
| Batch ingest protocol | ❌ Use COPY | ✅ Native batch |

---

## FalconDB JDBC Connection URL

```
jdbc:falcondb://host1:port1,host2:port2,host3:port3/dbname?param=value
```

| Part | Required | Default | Description |
|------|----------|---------|-------------|
| `jdbc:falcondb://` | Yes | — | Protocol prefix |
| `host:port` list | Yes (≥1) | port 5443 | Comma-separated seed gateways |
| `/dbname` | No | `falcon` | Target database |
| `?params` | No | — | Key=value pairs, `&`-separated |

The host list is a **seed list** — client connects to any one available host; the gateway handles routing transparently.

### Error Codes & Retry Contract

| Code | SQLSTATE | Retryable | Delay | Description |
|------|----------|-----------|-------|-------------|
| `NOT_LEADER` | FD001 | Yes | 0ms | Leader changed; hint provided |
| `NO_ROUTE` | FD002 | Yes | 100ms | No route to shard; topology updating |
| `OVERLOADED` | FD003 | Yes | 500ms | Gateway at capacity; backoff |
| `TIMEOUT` | FD004 | Yes | 200ms | Request timed out |
| `FATAL` | FD000 | **No** | — | Bad SQL, auth failure, etc. |

Max retries per request: 3. OVERLOADED requires backoff delay. NOT_LEADER uses hint, not random reconnect.

### Seed List Failover

Client tries seeds in order; after 3 consecutive failures on one seed, switches to next. If all seeds exhausted → `ConnectionError`.

### JDBC URL Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `connectTimeout` | 5000 | Connection timeout (ms) |
| `socketTimeout` | 30000 | Socket read timeout (ms) |
| `user` | — | Username |
| `password` | — | Password |
