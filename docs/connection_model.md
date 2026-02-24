# FalconDB — Connection Model

## Overview

FalconDB exposes a **PostgreSQL wire-compatible** endpoint on port 5443.
Any PG-compatible driver (JDBC, psycopg2, node-postgres, Go pgx, etc.)
can connect using standard PG connection strings.

This document defines the **entry semantics** so that application
developers and DBAs have zero ambiguity about what happens when they
connect.

---

## Key Principle: No Global Master Node

FalconDB is designed around **sharded, peer-to-peer topology**:

- There is **no single "master"** that all clients must connect to.
- Each node owns one or more shards.
- Each shard has a **leader** (read-write) and zero or more **followers** (read-only).
- Leader assignment can change due to failover, rebalance, or manual transfer.

> **Implication**: A JDBC URL pointing to node A does not guarantee that
> node A is the leader for the shard your query needs.

---

## Connection Modes

### Standalone (Single Node)

```
jdbc:postgresql://host:5443/falcon
psql -h host -p 5443 -U falcon
```

- One node, one shard, one leader. No ambiguity.
- All reads and writes go to the local engine.
- This is the default mode (`role = "standalone"`).

### Primary / Replica

```
# Primary (read-write)
psql -h primary-host -p 5443 -U falcon

# Replica (read-only)
psql -h replica-host -p 5443 -U falcon
```

- Primary accepts reads and writes.
- Replica accepts reads only. Writes return an error:
  `ERROR: cannot execute INSERT in a read-only transaction`
- Replica lag is observable via `SHOW falcon.replica_stats`.

### Multi-Shard Cluster

```
# Connect to any node — the node routes internally
psql -h node-a -p 5443 -U falcon
```

- Queries are planned locally, then dispatched to shard leaders.
- If the local node is not the leader for a shard, the query is
  **forwarded** to the correct leader via the distributed query engine.
- If forwarding fails (leader unreachable, timeout), the client receives
  an explicit error with a routing hint.

---

## What Happens When You Connect to the "Wrong" Node?

| Scenario | Behavior |
|----------|----------|
| Single-shard, standalone | Always local — no wrong node |
| Multi-shard, local node is leader | Execute locally |
| Multi-shard, local node is NOT leader | **Forward** to leader |
| Leader is unreachable | **Fail-fast** with `SQLSTATE 08006` + hint |
| Leader is migrating (rebalance) | **Fail-fast** or retry after short delay |
| Node is replica | Reads succeed; writes return read-only error |

### Fail-Fast Guarantees (v1.0.5+)

- **No silent hang**: If a query cannot be routed within the configured
  timeout, the client receives an explicit error.
- **Routing hints**: Error messages include `HINT: leader=<node>` so
  PG-aware proxies and smart drivers can redirect.
- **Retryable errors**: Errors during leader transitions include
  `retry_after_ms` to guide client backoff.

---

## Recommended Client Configurations

### Phase A: Single Entry Point (Current Recommendation)

Use a load balancer or DNS name as the single entry point:

```
jdbc:postgresql://falcon.internal:5443/mydb
```

- The LB health-checks `/ready` on each node.
- During failover, the LB drains the old leader (returns 503 on `/ready`).
- Simplest setup. Works with any PG driver.

### Phase B: Multi-Host Seed List (Future)

Some PG drivers support multi-host URLs:

```
jdbc:postgresql://node-a:5443,node-b:5443,node-c:5443/mydb?targetServerType=primary
```

- Driver tries each host in order until one accepts.
- `targetServerType=primary` ensures writes go to a primary node.
- Provides client-side failover without a load balancer.

> **Note**: FalconDB does not currently advertise server type in the
> startup parameters. Use Phase A (LB) for production today.

---

## Driver-Specific Notes

### JDBC (PostgreSQL JDBC Driver)

```java
String url = "jdbc:postgresql://falcon-host:5443/mydb";
Properties props = new Properties();
props.setProperty("user", "falcon");
props.setProperty("password", System.getenv("FALCON_DB_PASSWORD"));
props.setProperty("connectTimeout", "5");      // 5 seconds
props.setProperty("socketTimeout", "30");       // 30 seconds
props.setProperty("loginTimeout", "10");        // 10 seconds

Connection conn = DriverManager.getConnection(url, props);
```

**Recommended settings**:
- `connectTimeout=5` — fail fast on unreachable node
- `socketTimeout=30` — bound query execution time
- `ApplicationName=myapp` — visible in `SHOW falcon.connections`

### psycopg2 (Python)

```python
import psycopg2

conn = psycopg2.connect(
    host="falcon-host",
    port=5443,
    user="falcon",
    password=os.environ["FALCON_DB_PASSWORD"],
    connect_timeout=5,
    options="-c statement_timeout=30000",
)
```

### node-postgres (Node.js)

```javascript
const { Pool } = require('pg');

const pool = new Pool({
  host: 'falcon-host',
  port: 5443,
  user: 'falcon',
  password: process.env.FALCON_DB_PASSWORD,
  connectionTimeoutMillis: 5000,
  query_timeout: 30000,
  max: 20,
});
```

### Go pgx

```go
config, _ := pgx.ParseConfig("postgres://falcon:pass@falcon-host:5443/mydb")
config.ConnectTimeout = 5 * time.Second
conn, err := pgx.ConnectConfig(ctx, config)
```

---

## Fail / Retry Semantics

### Retryable Errors

| Error | SQLSTATE | Meaning | Action |
|-------|----------|---------|--------|
| `leader unavailable` | `08006` | Shard leader unreachable | Retry with backoff |
| `leader transferring` | `40001` | Leader is changing | Retry after `retry_after_ms` |
| `connection refused` | `08001` | Node is down | Try next seed / wait |
| `read-only transaction` | `25006` | Connected to replica | Reconnect to primary |

### Non-Retryable Errors

| Error | SQLSTATE | Meaning |
|-------|----------|---------|
| `authentication failed` | `28P01` | Bad credentials |
| `relation does not exist` | `42P01` | Table not found |
| `syntax error` | `42601` | SQL syntax error |

### Client Retry Pattern

```
max_retries = 3
backoff = [100ms, 500ms, 2000ms]

for attempt in 0..max_retries:
    try:
        result = execute(query)
        return result
    catch retryable_error:
        if attempt < max_retries - 1:
            sleep(backoff[attempt])
        else:
            raise
```

---

## Monitoring Connection Health

```sql
-- Active connections
SHOW falcon.connections;

-- Server status (from any node)
SHOW falcon.status;

-- Replica replication lag
SHOW falcon.replica_stats;
```

```powershell
# HTTP health probes
curl http://falcon-host:8080/health   # liveness
curl http://falcon-host:8080/ready    # readiness (false during shutdown)
curl http://falcon-host:8080/status   # full SSOT status JSON
```

---

## Summary

| Question | Answer |
|----------|--------|
| Is there a global master? | **No** — shards have individual leaders |
| Can I connect to any node? | **Yes** — queries are routed internally |
| What if the leader is down? | **Fail-fast** with error + routing hint |
| Will my query hang? | **No** — bounded by `statement_timeout` / forwarding timeout |
| How do I configure failover? | Use LB with `/ready` health check (Phase A) |
| Do I need a special driver? | **No** — any PG-compatible driver works |
