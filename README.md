# FalconDB
<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="License" />
</p>
[![CI](https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg)](https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml)
![MSRV](https://img.shields.io/badge/MSRV-1.75-blue)
![License](https://img.shields.io/badge/license-Apache--2.0-green)

**PG-Compatible Distributed In-Memory OLTP Database** — written in Rust.

FalconDB provides stable OLTP, fast/slow-path transactions, WAL-based
primary–replica replication with gRPC streaming, promote/failover, MVCC
garbage collection, and reproducible benchmarks.

### Supported Platforms

| Platform | Build | Test | Status |
|----------|:-----:|:----:|--------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | Primary CI target |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | CI target |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | Community tested |

**MSRV**: Rust **1.75** (`rust-version = "1.75"` in `Cargo.toml`)

### PG Protocol Compatibility

| Feature | Status | Notes |
|---------|:------:|-------|
| Simple query protocol | ✅ | Single + multi-statement |
| Extended query (Parse/Bind/Execute) | ✅ | Prepared statements + portals |
| Auth: Trust | ✅ | Any user accepted |
| Auth: MD5 | ✅ | PG auth type 5 |
| Auth: SCRAM-SHA-256 | ✅ | PG 10+ compatible |
| Auth: Password (cleartext) | ✅ | PG auth type 3 |
| TLS/SSL | ✅ | SSLRequest → upgrade when configured |
| COPY IN/OUT | ✅ | Text and CSV formats |
| `psql` 12+ | ✅ | Fully tested |
| `pgbench` (init + run) | ✅ | Built-in scripts work |
| JDBC (pgjdbc 42.x) | ✅ | Tested with 42.7+ |
| Cancel request | ⚠️ | Accepted, not acted upon |
| LISTEN/NOTIFY | ❌ | Not implemented |
| Logical replication protocol | ❌ | Uses gRPC instead |

See [docs/protocol_compatibility.md](docs/protocol_compatibility.md) for full test procedures.

### SQL Coverage

| Category | Supported |
|----------|-----------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (incl. ON CONFLICT, RETURNING, SELECT), UPDATE (incl. FROM, RETURNING), DELETE (incl. USING, RETURNING), COPY |
| **Queries** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOINs (INNER/LEFT/RIGHT/FULL/CROSS/NATURAL), subqueries (scalar/IN/EXISTS/correlated), CTEs (incl. RECURSIVE), UNION/INTERSECT/EXCEPT, window functions |
| **Aggregates** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **Types** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **Transactions** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, per-txn timeout, Read Committed, Snapshot Isolation |
| **Functions** | 500+ scalar functions (string, math, date/time, crypto, JSON, array) |
| **Observability** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### Not Supported (current)

- Stored procedures / PL/pgSQL
- Triggers
- Materialized views
- Foreign data wrappers (FDW)
- Logical replication / CDC
- Online schema change (concurrent index build)
- Row-level security (column-level grants implemented; row-level not yet)
- Automatic rebalancing (manual shard split only)
- Custom types (beyond JSONB)
- Full-text search (tsvector/tsquery)
- Partitioned tables (PARTITION BY)

---

## 1. Building

```bash
# Prerequisites: Rust 1.75+ (rustup), C/C++ toolchain (MSVC on Windows, gcc/clang on Linux/macOS)

# Build all crates (debug)
cargo build --workspace

# Build release
cargo build --release --workspace

# Run tests (1,976 tests across 13 crates + root integration)
cargo test --workspace

# Lint
cargo clippy --workspace
```

---

## 2. Starting a Cluster (Primary + Replica)

### Single-node (development)

```bash
# In-memory mode (no WAL, fastest)
cargo run -p falcon_server -- --no-wal

# With WAL persistence
cargo run -p falcon_server -- --data-dir ./falcon_data

# Connect via psql
psql -h 127.0.0.1 -p 5433 -U falcon
```

### Multi-node deployment (M2 — gRPC WAL streaming)

**Via config files** (recommended):
```bash
# Primary — accepts writes, streams WAL to replicas
cargo run -p falcon_server -- -c examples/primary.toml

# Replica — receives WAL from primary, serves read-only queries
cargo run -p falcon_server -- -c examples/replica.toml
```

**Via CLI flags:**
```bash
# Primary on port 5433, gRPC on 50051
cargo run -p falcon_server -- --role primary --pg-addr 0.0.0.0:5433 \
  --grpc-addr 0.0.0.0:50051 --data-dir ./node1

# Replica on port 5434, connects to primary's gRPC
cargo run -p falcon_server -- --role replica --pg-addr 0.0.0.0:5434 \
  --primary-endpoint http://127.0.0.1:50051 --data-dir ./node2

# Second replica on port 5435
cargo run -p falcon_server -- --role replica --pg-addr 0.0.0.0:5435 \
  --primary-endpoint http://127.0.0.1:50051 --data-dir ./node3
```

> **Note**: M2 gRPC WAL streaming is in progress. The `--role` flag is
> accepted but actual network replication requires `protoc` and tonic
> codegen (`cargo build -p falcon_cluster --features grpc-codegen`).
> M1 in-process replication remains available via the Rust API.

### Programmatic cluster setup (Rust API)

```rust
use falcon_cluster::replication::ShardReplicaGroup;

// Creates 1 primary + 1 replica with shared schema
let mut group = ShardReplicaGroup::new(ShardId(0), &[schema]).unwrap();

// Ship WAL records from primary to replica
group.ship_wal_record(wal_record);

// Catch up replica to latest LSN
group.catch_up_replica(0).unwrap();
```

---

## 3. Primary / Replica Configuration

### Configuration file (`falcon.toml`)

```toml
[server]
pg_listen_addr = "0.0.0.0:5433"   # PostgreSQL wire protocol listen address
admin_listen_addr = "0.0.0.0:8080" # Admin/metrics endpoint
node_id = 1                        # Unique node identifier
max_connections = 1024              # Max concurrent PG connections

[storage]
memory_limit_bytes = 0              # 0 = unlimited
wal_enabled = true                  # Enable write-ahead log
data_dir = "./falcon_data"           # WAL and checkpoint directory

[wal]
group_commit = true                 # Batch WAL writes for throughput
flush_interval_us = 1000            # Group commit flush interval (µs)
sync_mode = "fdatasync"             # "fsync", "fdatasync", or "none"
segment_size_bytes = 67108864       # WAL segment size (64MB default)

[gc]
enabled = true                      # Enable MVCC garbage collection
interval_ms = 1000                  # GC sweep interval (milliseconds)
batch_size = 0                      # 0 = unlimited (sweep all chains per cycle)
min_chain_length = 2                # Skip chains shorter than this

[replication]
commit_ack = "primary_durable"      # Scheme A: commit ack = primary WAL fsync
                                    # RPO > 0 possible (documented tradeoff)
```

### CLI Options

```
falcon [OPTIONS]

Options:
  -c, --config <FILE>        Config file [default: falcon.toml]
      --pg-addr <ADDR>       PG listen address (overrides config)
      --data-dir <DIR>       Data directory (overrides config)
      --no-wal               Disable WAL (pure in-memory)
      --metrics-addr <ADDR>  Metrics endpoint [default: 0.0.0.0:9090]
      --replica              Start in replica mode
      --primary-addr <ADDR>  Primary address for replication
```

### Replication semantics (M1)

- **Commit Ack (Scheme A)**: `commit ack = primary WAL durable (fsync)`.
  The primary does not wait for replicas before acknowledging commits.
  RPO may be > 0 in the event of primary failure before replication.
- **WAL shipping**: `WalChunk` frames with `start_lsn`, `end_lsn`, CRC32 checksum.
- **Ack tracking**: replicas report `applied_lsn`; primary tracks per-replica ack LSNs for reconnect/resume from `ack_lsn + 1`.
- **Replica read-only**: replicas start in `read_only` mode. Writes are rejected until promoted.

---

## 4. Promote / Failover

### Promote operation

```rust
// Programmatic API
group.promote(replica_index).unwrap();
```

Promote semantics:
1. **Fence old primary** — marks it `read_only`, rejecting new writes.
2. **Catch up replica** — applies remaining WAL to reach latest LSN.
3. **Swap roles** — atomically swaps primary and replica.
4. **Unfence new primary** — new primary accepts writes.
5. **Update shard map** — routes new writes to promoted node.

### Failover exercise (end-to-end)

See `crates/falcon_cluster/examples/failover_exercise.rs` for a self-contained example that:

1. Creates a cluster (1 primary + 1 replica)
2. Writes test data on primary
3. Replicates to replica
4. Fences (kills) primary
5. Promotes replica
6. Writes new data on promoted primary
7. Verifies data integrity (zero data loss for committed data)

```bash
# Run the failover exercise example
cargo run -p falcon_cluster --example failover_exercise

# Run failover-related tests
cargo test -p falcon_cluster -- promote_fencing_tests
cargo test -p falcon_cluster -- m1_full_lifecycle
```

### Failover observability

```sql
-- View failover / replication metrics
SHOW falcon.replication_stats;
```

| Metric | Description |
|--------|-------------|
| `promote_count` | Total promote operations completed |
| `last_failover_time_ms` | Duration of last failover (ms) |

---

## 5. Running Benchmarks

### YCSB-style workload

```bash
# Default: 10k ops, 50% reads, 80% local txns, 4 shards
cargo run -p falcon_bench -- --ops 10000

# Custom mix
cargo run -p falcon_bench -- --ops 50000 --read-pct 80 --local-pct 90 --shards 4

# Export as CSV or JSON
cargo run -p falcon_bench -- --ops 10000 --export csv
cargo run -p falcon_bench -- --ops 10000 --export json
```

### Fast-Path ON vs OFF comparison (Chart 2: p99 latency)

```bash
cargo run -p falcon_bench -- --ops 10000 --compare --export csv
```

Output: TPS, commit counts, latency p50/p95/p99 for fast-path vs all-global.

### Scale-out benchmark (Chart 1: TPS vs shard count)

```bash
# Runs 1/2/4/8 shard configurations automatically
cargo run -p falcon_bench -- --scaleout --ops 5000 --export csv
```

Output: `shards,ops,elapsed_ms,tps,scatter_gather_total_us,...`

### Failover benchmark (Chart 3: before/after latency)

```bash
cargo run -p falcon_bench -- --failover --ops 10000 --export csv
```

Output: before-failover TPS/latency, failover duration, after-failover TPS/latency, data integrity check.

### Benchmark parameters (frozen for M1)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--ops` | 10000 | Total operations per run |
| `--read-pct` | 50 | Read percentage (0–100) |
| `--local-pct` | 80 | Local (single-shard) txn percentage |
| `--shards` | 4 | Logical shard count |
| `--record-count` | 1000 | Pre-loaded rows |
| `--isolation` | rc | `rc` (ReadCommitted) or `si` (SnapshotIsolation) |
| `--export` | text | `text`, `csv`, or `json` |

Random seed is fixed at 42 for reproducibility.

---

## 6. Viewing Metrics

### SQL observability commands

Connect via `psql -h 127.0.0.1 -p 5433 -U falcon` and run:

```sql
-- Transaction statistics (commit/abort counts, latency percentiles)
SHOW falcon.txn_stats;

-- Recent transaction history (per-txn records)
SHOW falcon.txn_history;

-- Active transactions
SHOW falcon.txn;

-- GC statistics (safepoint, reclaimed versions/bytes, chain length)
SHOW falcon.gc_stats;

-- GC safepoint diagnostics (long-txn detection, stall indicator)
SHOW falcon.gc_safepoint;

-- Replication / failover metrics
SHOW falcon.replication_stats;

-- Scatter/gather execution stats
SHOW falcon.scatter_stats;
```

### `SHOW falcon.txn_stats` output

| Metric | Description |
|--------|-------------|
| `total_committed` | Total committed transactions |
| `fast_path_commits` | Commits via fast-path (LocalTxn) |
| `slow_path_commits` | Commits via slow-path (GlobalTxn) |
| `total_aborted` | Total aborted transactions |
| `occ_conflicts` | OCC serialization failures |
| `constraint_violations` | Unique constraint violations |
| `active_count` | Currently active transactions |
| `fast_p50/p95/p99_us` | Fast-path commit latency percentiles |
| `slow_p50/p95/p99_us` | Slow-path commit latency percentiles |

### `SHOW falcon.gc_stats` output

| Metric | Description |
|--------|-------------|
| `gc_safepoint_ts` | Current GC watermark timestamp |
| `active_txn_count` | Active transactions blocking GC |
| `oldest_txn_ts` | Oldest active transaction timestamp |
| `total_sweeps` | Total GC sweep cycles completed |
| `reclaimed_version_count` | Total MVCC versions reclaimed |
| `reclaimed_memory_bytes` | Total bytes freed by GC |
| `last_sweep_duration_us` | Duration of last GC sweep |
| `max_chain_length` | Longest version chain observed |

### `SHOW falcon.gc_safepoint` output

| Metric | Description |
|--------|-------------|
| `active_txn_count` | Active transactions blocking GC |
| `longest_txn_age_us` | Age of the longest-running active transaction (µs) |
| `min_active_start_ts` | Start timestamp of the oldest active transaction |
| `current_ts` | Current timestamp allocator value |
| `stalled` | Whether the GC safepoint is stalled by long-running txns |

---

## Supported SQL

### DDL

```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    total FLOAT8 DEFAULT 0.0,
    CHECK (total >= 0)
);
DROP TABLE users;
DROP TABLE IF EXISTS users;
TRUNCATE TABLE users;

-- Indexes
CREATE INDEX idx_name ON users (name);
CREATE UNIQUE INDEX idx_email ON users (email);
DROP INDEX idx_name;

-- ALTER TABLE
ALTER TABLE users ADD COLUMN email TEXT;
ALTER TABLE users DROP COLUMN email;
ALTER TABLE users RENAME COLUMN name TO full_name;
ALTER TABLE users RENAME TO people;
ALTER TABLE users ALTER COLUMN age SET NOT NULL;
ALTER TABLE users ALTER COLUMN age DROP NOT NULL;
ALTER TABLE users ALTER COLUMN age SET DEFAULT 0;
ALTER TABLE users ALTER COLUMN age TYPE BIGINT;

-- Sequences
CREATE SEQUENCE user_id_seq START 1;
SELECT nextval('user_id_seq');
SELECT currval('user_id_seq');
SELECT setval('user_id_seq', 100);
```

### DML

```sql
-- INSERT (single, multi-row, DEFAULT, RETURNING, ON CONFLICT)
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users (name, age) VALUES ('Bob', 25), ('Eve', 22);
INSERT INTO users VALUES (1, 'Alice', 30) ON CONFLICT DO NOTHING;
INSERT INTO users VALUES (1, 'Alice', 30)
    ON CONFLICT (id) DO UPDATE SET name = excluded.name;
INSERT INTO users VALUES (2, 'Bob', 25) RETURNING *;
INSERT INTO users VALUES (3, 'Eve', 22) RETURNING id, name;
INSERT INTO orders SELECT id, name FROM staging;  -- INSERT ... SELECT

-- UPDATE (single-table, multi-table FROM, RETURNING)
UPDATE users SET age = 31 WHERE id = 1;
UPDATE users SET age = 31 WHERE id = 1 RETURNING id, age;
UPDATE products SET price = p.new_price
    FROM price_updates p WHERE products.id = p.id;

-- DELETE (single-table, multi-table USING, RETURNING)
DELETE FROM users WHERE id = 2;
DELETE FROM users WHERE id = 2 RETURNING *;
DELETE FROM employees USING terminated
    WHERE employees.id = terminated.emp_id;

-- COPY (stdin/stdout, CSV/text formats)
COPY users FROM STDIN;
COPY users TO STDOUT WITH (FORMAT csv, HEADER true);
COPY (SELECT * FROM users WHERE age > 25) TO STDOUT;
```

### Queries

```sql
-- Basic SELECT with filtering, ordering, pagination
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 25 ORDER BY name LIMIT 10 OFFSET 5;
SELECT DISTINCT department FROM employees;

-- Expressions: CASE, COALESCE, NULLIF, CAST, BETWEEN, IN, LIKE/ILIKE
SELECT CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END FROM users;
SELECT COALESCE(nickname, name) FROM users;
SELECT * FROM users WHERE age BETWEEN 20 AND 30;
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE name ILIKE '%alice%';
SELECT CAST(age AS TEXT) FROM users;

-- Aggregates and GROUP BY / HAVING
SELECT dept, COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary)
    FROM employees GROUP BY dept HAVING COUNT(*) > 5;
SELECT BOOL_AND(active), BOOL_OR(active) FROM users;
SELECT ARRAY_AGG(name) FROM users;

-- Window functions
SELECT name, salary,
    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC),
    RANK() OVER (ORDER BY salary DESC),
    DENSE_RANK() OVER (ORDER BY salary DESC),
    LAG(salary) OVER (ORDER BY salary),
    LEAD(salary) OVER (ORDER BY salary),
    SUM(salary) OVER (PARTITION BY dept)
FROM employees;

-- Joins (INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL, USING)
SELECT * FROM orders JOIN users ON orders.user_id = users.id;
SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.id;
SELECT * FROM orders NATURAL JOIN users;
SELECT * FROM t1 JOIN t2 USING (id);

-- Subqueries (scalar, IN, EXISTS, correlated)
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);
SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id);
SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) FROM users;

-- Set operations (UNION, INTERSECT, EXCEPT — with ALL)
SELECT name FROM employees UNION SELECT name FROM contractors;
SELECT id FROM t1 INTERSECT SELECT id FROM t2;
SELECT id FROM t1 EXCEPT ALL SELECT id FROM t2;

-- CTEs (WITH, recursive)
WITH active AS (SELECT * FROM users WHERE active = true)
SELECT * FROM active WHERE age > 25;
WITH RECURSIVE nums AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 10
) SELECT * FROM nums;

-- Arrays
SELECT ARRAY[1, 2, 3];
SELECT arr[1] FROM t;
SELECT UNNEST(ARRAY[1, 2, 3]);
SELECT ARRAY_AGG(name) FROM users;

-- ANY / ALL operators
SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3]);
SELECT * FROM users WHERE age > ALL(SELECT min_age FROM rules);

-- IS DISTINCT FROM
SELECT * FROM t WHERE a IS DISTINCT FROM b;

-- Transactions
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 28);
COMMIT;  -- or ROLLBACK;

-- EXPLAIN
EXPLAIN SELECT * FROM users WHERE id = 1;

-- Observability
SHOW falcon.txn_stats;
SHOW falcon.gc_stats;
SHOW falcon.replication_stats;
```

### Supported Types

| Type | PG Equivalent |
|------|--------------|
| `INT` / `INTEGER` | `integer` / `int4` |
| `BIGINT` | `bigint` / `int8` |
| `FLOAT8` / `DOUBLE PRECISION` | `double precision` |
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | `numeric` (i128 mantissa + u8 scale) |
| `TEXT` / `VARCHAR` | `text` / `varchar` |
| `BOOLEAN` | `boolean` |
| `TIMESTAMP` | `timestamp without time zone` |
| `DATE` | `date` |
| `SERIAL` | auto-incrementing `int4` |
| `BIGSERIAL` | auto-incrementing `int8` |
| `INT[]` / `TEXT[]` / ... | one-dimensional arrays |

### Scalar Functions (500+)

Core PG-compatible functions including: `UPPER`, `LOWER`, `LENGTH`, `SUBSTRING`, `CONCAT`, `REPLACE`, `TRIM`, `LPAD`, `RPAD`, `LEFT`, `RIGHT`, `REVERSE`, `INITCAP`, `POSITION`, `SPLIT_PART`, `ABS`, `ROUND`, `CEIL`, `FLOOR`, `POWER`, `SQRT`, `LN`, `LOG`, `EXP`, `MOD`, `SIGN`, `PI`, `GREATEST`, `LEAST`, `TO_CHAR`, `TO_NUMBER`, `TO_DATE`, `TO_TIMESTAMP`, `NOW`, `CURRENT_DATE`, `CURRENT_TIME`, `DATE_TRUNC`, `DATE_PART`, `EXTRACT`, `AGE`, `MD5`, `SHA256`, `ENCODE`, `DECODE`, `GEN_RANDOM_UUID`, `RANDOM`, `REGEXP_REPLACE`, `REGEXP_MATCH`, `REGEXP_COUNT`, `STARTS_WITH`, `ENDS_WITH`, `PG_TYPEOF`, and extensive array/string/math/statistical functions.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│           PG Wire Protocol (TCP)            │
├─────────────────────────────────────────────┤
│   SQL Frontend (sqlparser-rs → Binder)      │
├─────────────────────────────────────────────┤
│   Planner / Router                          │
├─────────────────────────────────────────────┤
│   Executor (row-at-a-time, expressions)     │
├──────────────────┬──────────────────────────┤
│   Txn Manager    │   Storage Engine         │
│   (MVCC, OCC)    │   (MemTable + WAL + GC)  │
├──────────────────┴──────────────────────────┤
│  Cluster (ShardMap, Replication, Failover)  │
└─────────────────────────────────────────────┘
```

### Crate Structure

| Crate | Responsibility |
|-------|---------------|
| `falcon_common` | Shared types, errors, config, datum, schema |
| `falcon_storage` | In-memory tables, MVCC version chains, indexes, WAL, GC |
| `falcon_txn` | Transaction lifecycle, OCC validation, timestamp allocation |
| `falcon_sql_frontend` | SQL parsing (sqlparser-rs) + binding/analysis |
| `falcon_planner` | Logical → physical plan, routing hints |
| `falcon_executor` | Operator execution, expression evaluation |
| `falcon_protocol_pg` | PostgreSQL wire protocol codec + TCP server |
| `falcon_raft` | Consensus trait + single-node stub |
| `falcon_cluster` | Shard map, replication, failover, scatter/gather |
| `falcon_observability` | Metrics (Prometheus), structured logging, tracing |
| `falcon_server` | Main binary, wires all components |
| `falcon_bench` | YCSB-style benchmark harness |

---

## Transaction Model

- **LocalTxn (fast-path)**: single-shard transactions commit with OCC under Snapshot Isolation — no 2PC overhead. `TxnContext.txn_path = Fast`.
- **GlobalTxn (slow-path)**: cross-shard transactions use XA-2PC with prepare/commit. `TxnContext.txn_path = Slow`.
- **TxnContext**: carried through all layers with hard invariant validation at commit time:
  - LocalTxn → `involved_shards.len() == 1`, must use fast-path
  - GlobalTxn → must not use fast-path
  - Violations return `InternalError` (not just debug_assert)
- **Unified commit entry**: `StorageEngine::commit_txn(txn_id, commit_ts, txn_type)`. Raw `commit_txn_local`/`commit_txn_global` are `pub(crate)` only.

---

## MVCC Garbage Collection

- **Safepoint**: `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **WAL-aware**: never reclaims uncommitted or aborted versions
- **Replication-safe**: respects replica applied timestamps
- **Lock-free per key**: no global lock, no stop-the-world pauses
- **Background runner**: `GcRunner` thread with configurable interval
- **Observability**: `SHOW falcon.gc_stats`

---

## Quick Start Demo

### Linux / macOS / WSL

```bash
chmod +x scripts/demo_standalone.sh
./scripts/demo_standalone.sh
```

Builds FalconDB, starts a standalone node, runs SQL smoke tests via `psql`,
and executes a quick benchmark — all in one command.

### Windows (PowerShell)

```powershell
.\scripts\demo_standalone.ps1
```

### Primary + Replica replication demo

```bash
chmod +x scripts/demo_replication.sh
./scripts/demo_replication.sh
```

Starts a primary and replica via gRPC, writes data, verifies replication,
and shows replication metrics.

### E2E Failover Demo (two-node, closed-loop)

```bash
# Linux / macOS / WSL
chmod +x scripts/e2e_two_node_failover.sh
./scripts/e2e_two_node_failover.sh

# Windows PowerShell
.\scripts\e2e_two_node_failover.ps1
```

Full closed-loop test: start primary → start replica → write data → verify
replication → kill primary → promote replica → verify old data readable →
write new data → output PASS/FAIL. On failure, prints last 50 lines of each
node's log plus port/PID diagnostics.

---

## Developer Setup

### Windows

```powershell
.\scripts\setup_windows.ps1
```

Checks/installs: MSVC C++ build tools, Rust toolchain, protoc (vendored),
psql client, git EOL config. See `scripts/setup_windows.ps1` for details.

### Linux / macOS

```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# psql (for testing)
sudo apt install postgresql-client    # Debian/Ubuntu
brew install libpq                    # macOS

# Build
cargo build --workspace
```

### Default config template

```bash
cargo run -p falcon_server -- --print-default-config > falcon.toml
```

---

## Roadmap

| Phase | Scope | Details |
|-------|-------|---------|
| **v0.1–v0.4** ✅ | OLTP foundation, WAL, failover, gRPC streaming, TLS, columnstore, multi-tenancy | Released |
| **v0.4.x** ✅ | Production hardening: error model, crash domain, unwrap=0 in core crates | Released |
| **v0.5** ✅ | Operationally usable: cluster admin, rebalance, scale-out/in, ops playbook | Released |
| **v0.6** ✅ | Latency-controlled OLTP: priority scheduler, token bucket, backpressure | Released |
| **v0.7** ✅ | Deterministic 2PC: decision log, layered timeouts, slow-shard tracker | Released |
| **v0.8** ✅ | Chaos-ready: fault injection, network partition, CPU/IO jitter, observability pass | Released |
| **v0.9** ✅ | Production candidate: security hardening, WAL versioning, wire compat, config compat | Released |
| **v1.0 Phase 1** ✅ | LSM kernel: disk-backed OLTP, MVCC encoding, idempotency, TPC-B benchmark | 1,917 tests |
| **v1.0 Phase 2** ✅ | SQL completeness: DECIMAL, composite indexes, RBAC, txn READ ONLY, governor v2 | 1,976 tests |
| **v1.0.0** 📋 | Production-grade database kernel — all gates pass | [docs/roadmap.md](docs/roadmap.md) |

See [docs/roadmap.md](docs/roadmap.md) for detailed acceptance criteria per milestone.

### RPO / RTO

FalconDB supports three durability policies: `local-fsync` (default, RPO > 0 possible),
`quorum-ack` (RPO = 0 with quorum), and `all-ack` (strongest, highest latency).
See [docs/rpo_rto.md](docs/rpo_rto.md) for full RPO/RTO analysis and recommendations.

---

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System architecture, crate structure, data flow |
| [docs/roadmap.md](docs/roadmap.md) | Milestone definitions and acceptance criteria |
| [docs/rpo_rto.md](docs/rpo_rto.md) | RPO/RTO guarantees per durability policy |
| [docs/show_commands_schema.md](docs/show_commands_schema.md) | Stable output schema for all `SHOW falcon.*` commands |
| [docs/protocol_compatibility.md](docs/protocol_compatibility.md) | PG client compatibility matrix (psql, JDBC, pgbench) |
| [docs/feature_gap_analysis.md](docs/feature_gap_analysis.md) | Known gaps and improvement areas |
| [docs/error_model.md](docs/error_model.md) | Unified error model, SQLSTATE mapping, retry hints |
| [docs/observability.md](docs/observability.md) | Prometheus metrics, SHOW commands, slow query log |
| [docs/production_readiness.md](docs/production_readiness.md) | Production readiness checklist |
| [docs/production_readiness_report.md](docs/production_readiness_report.md) | Full production readiness audit |
| [docs/ops_playbook.md](docs/ops_playbook.md) | Scale-out/in, failover, rolling upgrade procedures |
| [docs/chaos_matrix.md](docs/chaos_matrix.md) | 30 chaos scenarios with expected behavior |
| [docs/security.md](docs/security.md) | Security features, RBAC, SQL firewall, audit |
| [docs/wire_compatibility.md](docs/wire_compatibility.md) | WAL/snapshot/wire/config compatibility policy |
| [docs/performance_baseline.md](docs/performance_baseline.md) | P99 latency targets and benchmark methodology |
| [CHANGELOG.md](CHANGELOG.md) | Semantic versioning changelog (v0.1–v0.9) |

---

## Testing

```bash
# Run all tests (1,976 total)
cargo test --workspace

# By crate
cargo test -p falcon_storage    # 226 tests (MVCC, WAL, GC, indexes incl. composite/covering/prefix, LSM engine)
cargo test -p falcon_cluster    # 412 tests (replication, failover, scatter/gather, 2PC, admission DDL permits, node mode)
cargo test -p falcon_server     # 208 tests (SQL end-to-end, error paths, SHOW commands)
cargo test -p falcon_txn        # 61 tests (txn lifecycle, OCC, stats, READ ONLY mode, timeout, exec summary)
cargo test -p falcon_planner    # 89 tests (routing hints, distributed wrapping, shard key inference)
cargo test -p falcon_executor   # 180 tests (governor v2 abort reasons, priority scheduler, vectorized)
cargo test -p falcon_common     # 203 tests (error model, config, RBAC, RoleCatalog, PrivilegeManager, Decimal)
cargo test -p falcon_sql_frontend # 141 tests (binder, predicate normalization, param inference)
cargo test -p falcon_protocol_pg  # 147 tests (SHOW commands, error paths, txn lifecycle, handler)
cargo test --test integration_test  # 12 tests (root integration: DDL, DML, RETURNING clause, transactions)

# Lint
cargo clippy --workspace       # must be 0 warnings
```

---

## License

Apache-2.0
