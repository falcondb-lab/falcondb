# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>PG-Compatible · Distributed · Memory-First · Deterministic Transaction Semantics</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="Version" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="License" />
</p>

> FalconDB is a **PG-compatible, distributed, memory-first OLTP database** with
> deterministic transaction semantics. Benchmarked against PostgreSQL, VoltDB,
> and SingleStore — see **[Benchmark Matrix](benchmarks/README.md)**.
>
> - ✅ **Low latency** — single-shard fast-path commits bypass 2PC entirely
> - ✅ **Provable consistency** — MVCC/OCC under Snapshot Isolation, CI-verified ACID
> - ✅ **Operability** — 50+ SHOW commands, Prometheus metrics, failover CI gate
> - ✅ **Determinism** — hardened state machine, bounded in-doubt, idempotent retry
> - ❌ Not HTAP — no analytical workloads
> - ❌ Not full PG — [see unsupported list below](#not-supported)

FalconDB provides OLTP with fast/slow-path transactions, WAL-based primary–replica
replication (gRPC streaming), promote/failover, MVCC garbage collection, and
reproducible benchmarks.

### Deterministic Commit Guarantee (DCG)

> **If FalconDB returns "committed", that transaction will survive any single-node crash,
> any failover, and any recovery — with zero exceptions.**

This is FalconDB's core engineering property, called the **Deterministic Commit Guarantee (DCG)**.
It is not a configuration option — it is the default behavior under the `LocalWalSync` commit policy.

- **Prove it yourself**: [falcondb-poc-dcg/](falcondb-poc-dcg/) — one-click demo: write 1,000 orders → kill -9 primary → verify zero data loss
- **Benchmark it yourself**: [falcondb-poc-pgbench/](falcondb-poc-pgbench/) — pgbench comparison vs PostgreSQL under identical durability settings
- **Crash under load**: [falcondb-poc-failover-under-load/](falcondb-poc-failover-under-load/) — kill -9 primary during sustained writes → verify zero data loss + automatic recovery
- **See inside**: [falcondb-poc-observability/](falcondb-poc-observability/) — live Grafana dashboard, Prometheus metrics, operational controls
- **Migrate from PG**: [falcondb-poc-migration/](falcondb-poc-migration/) — migrate a real PostgreSQL app by changing only the connection string
- **Recover after disaster**: [falcondb-poc-backup-pitr/](falcondb-poc-backup-pitr/) — destroy the database, restore from backup, replay to exact second, verify every row matches

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
| Cancel request | ✅ | AtomicBool polling, 50ms latency, simple + extended query |
| LISTEN/NOTIFY | ✅ | In-memory broadcast hub, LISTEN/UNLISTEN/NOTIFY |
| Logical replication protocol | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

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

### <a id="not-supported"></a>Not Supported (v1.2)

The following features are **explicitly out of scope** for v1.2.
Attempting to use them returns a clear `ErrorResponse` with the appropriate SQLSTATE code.

| Feature | Error Code | Error Message |
|---------|-----------|---------------|
| Stored procedures / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| Triggers | `0A000` | `triggers are not supported` |
| Materialized views | `0A000` | `materialized views are not supported` |
| Foreign data wrappers (FDW) | `0A000` | `foreign data wrappers are not supported` |
| Full-text search (tsvector/tsquery) | `0A000` | `full-text search is not supported` |
| Online DDL (concurrent index build) | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore analytics | — | Feature-gated off at compile time |
| Automatic rebalancing | — | Manual shard split only |
| Custom types (beyond JSONB) | `0A000` | `custom types are not supported` |

> **Scope Guard**: HTAP, ColumnStore, disk tier/spill, and online DDL are all
> either `feature = "off"` or stub-only. Raft consensus is a single-node stub
> (NOT on the production path).

### Planned — NOT Implemented (P2 roadmap, no code on default build path)

| Feature | Module Status | Notes |
|---------|:------------:|-------|
| Raft consensus replication | STUB | `falcon_raft` is a single-node no-op stub. Not on production path. No target milestone |
| Disk spill / tiered storage | STUB | `disk_rowstore.rs`, `columnstore.rs` — code exists but not on production path |
| LSM-tree storage engine | EXPERIMENTAL | `lsm/` — compile-gated, not default; USTM-integrated for both Rowstore and LSM |
| Online DDL (non-blocking ALTER) | STUB | `online_ddl.rs` — state machine scaffolding only |
| HTAP / ColumnStore analytics | STUB | `columnstore.rs` — not wired to query planner |
| Transparent Data Encryption | STUB | `encryption.rs` — key manager scaffolding only |
| Point-in-Time Recovery | STUB | `pitr.rs` — WAL archiver scaffolding only |
| Auto shard rebalancing | — | Not started |
| Multi-tenant resource isolation | STUB | `resource_isolation.rs`, `tenant_registry.rs` — not enforced |

---

## 1. Building

```bash
# Prerequisites: Rust 1.75+ (rustup), C/C++ toolchain (MSVC on Windows, gcc/clang on Linux/macOS)

# Build all crates (debug)
cargo build --workspace

# Build release (default: Rowstore only)
cargo build --release --workspace

# Build with LSM storage engine enabled
cargo build --release -p falcon_server --features lsm

# Run tests (3,972 tests across 16 crates + root integration)
cargo test --workspace

# Lint
cargo clippy --workspace
```

---

## 1.1 Storage Engines

FalconDB supports multiple storage engines. Each table can independently choose its engine via the `ENGINE=` clause in `CREATE TABLE`.

### Engine Comparison

| Engine | Storage | Persistence | Data Limit | Best For |
|--------|---------|-------------|------------|----------|
| **Rowstore** (default) | In-memory (MVCC version chains) | WAL only (crash-safe if WAL enabled) | Limited by available RAM | Low-latency OLTP, hot data |
| **LSM** | Disk (LSM-Tree + WAL) | Full disk persistence | Limited by disk space | Large datasets, cold data, persistence-first workloads |

### Rowstore (Default — In-Memory)

Rowstore is the default engine. No special build flags or configuration needed.

```sql
-- These two are equivalent:
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
CREATE TABLE users (id INT PRIMARY KEY, name TEXT) ENGINE=rowstore;
```

**Characteristics:**
- All data resides in memory (MVCC version chains in `DashMap`)
- Fastest read/write latency (~1ms per 500-row INSERT)
- Data survives restarts only if WAL is enabled (`wal.sync_mode` in `falcon.toml`)
- Memory backpressure: configurable soft/hard limits in `[memory]` section of `falcon.toml`

**Memory protection** — when data approaches memory limits:

| State | Condition | Behavior |
|-------|-----------|----------|
| Normal | usage < soft_limit | No restrictions |
| Pressure | soft ≤ usage < hard | Delay/reject new write transactions, accelerate GC |
| Critical | usage ≥ hard_limit | Reject all new transactions |

Configure in `falcon.toml`:
```toml
[memory]
shard_soft_limit_bytes = 4294967296   # 4 GB
shard_hard_limit_bytes = 6442450944   # 6 GB
pressure_policy = "Reject"            # or "Delay"
```

### LSM (Disk-Backed)

LSM uses a Log-Structured Merge-Tree for disk-based storage. **Requires the `lsm` feature flag at compile time.**

**Step 1: Build with LSM enabled**
```bash
cargo build --release -p falcon_server --features lsm
```

**Step 2: Create tables with `ENGINE=lsm`**
```sql
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    payload TEXT,
    created_at TIMESTAMP DEFAULT NOW()
) ENGINE=lsm;
```

**Characteristics:**
- Data persisted to disk as SST files (`falcon_data/lsm_table_<id>/`)
- Not limited by RAM — can store datasets much larger than available memory
- Higher latency than Rowstore due to disk I/O
- Integrated with USTM Warm Zone for read caching
- LSM compaction runs in the background

Configure LSM sync behavior in `falcon.toml`:
```toml
[storage]
lsm_sync_writes = false   # true = fsync every write (safer, slower)
```

### Mixing Engines

Different tables in the same database can use different engines:

```sql
-- Hot data: in-memory for speed
CREATE TABLE sessions (id INT PRIMARY KEY, token TEXT, expires_at TIMESTAMP);

-- Cold data: disk-backed for capacity
CREATE TABLE audit_log (id BIGSERIAL PRIMARY KEY, event TEXT, ts TIMESTAMP) ENGINE=lsm;

-- Queries work identically regardless of engine
SELECT * FROM sessions s JOIN audit_log a ON s.id = a.user_id;
```

### Engine Selection Guide

| Scenario | Recommended Engine |
|----------|-------------------|
| Low-latency OLTP (< 10ms p99) | Rowstore |
| Data fits in memory (< available RAM) | Rowstore |
| Large datasets (> available RAM) | LSM |
| Compliance / must persist to disk | LSM |
| Mixed: hot tables + cold tables | Rowstore + LSM |

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

[ustm]
enabled = true                      # Enable USTM page cache (replaces mmap)
hot_capacity_bytes = 536870912      # 512 MB — MemTable + index internals (never evicted)
warm_capacity_bytes = 268435456     # 256 MB — SST page cache (LIRS-2 eviction)
lirs_lir_capacity = 4096            # Protected high-frequency pages
lirs_hir_capacity = 1024            # Eviction candidate pages
background_iops_limit = 500         # Max IOPS for compaction/GC I/O
prefetch_iops_limit = 200           # Max IOPS for query-driven prefetch
prefetch_enabled = true             # Enable query-aware prefetcher
page_size = 8192                    # Default page size (8 KB)

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

### Bulk Insert + Query (1M rows)

```bash
# Build release binaries
cargo build --release -p falcon_server -p falcon_bench

# Start FalconDB
./target/release/falcon --no-wal &

# Run 1M row benchmark (DDL + 100 INSERT batches + aggregate/scan queries)
./target/release/falcon_bench --bulk \
  --bulk-file benchmarks/bulk_insert_1m.sql \
  --bulk-host localhost --bulk-port 5433 \
  --bulk-sslmode disable --export text
```

**1M Row Benchmark Results** (vs PostgreSQL 16):

| Metric | FalconDB | PostgreSQL | Ratio |
|--------|----------|------------|-------|
| Total (DDL + INSERT + queries) | ~6.4s | ~6.1s | 1.05x |
| INSERT phase (100 batches × 10K rows) | ~2.9s | ~5.4s | **0.54x (faster)** |
| Query phase (COUNT, ORDER BY LIMIT, aggregates, GROUP BY, WHERE) | ~2.8s | ~0.65s | 4.3x |
| Rows/s (INSERT) | ~340K | ~185K | **1.84x (faster)** |

> **Note**: FalconDB's INSERT throughput significantly exceeds PostgreSQL due to
> in-memory MVCC with zero disk I/O. Query phase is slower due to DashMap pointer-chasing
> vs PostgreSQL's sequential heap pages, but has been optimized from 12x → 4.3x gap
> via fused streaming aggregates, zero-copy MVCC iteration, and bounded-heap top-K.

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

### Additional benchmarks

```bash
# Fast-path vs slow-path p99 comparison
cargo run -p falcon_bench -- --ops 10000 --compare --export csv

# Scale-out (1/2/4/8 shards)
cargo run -p falcon_bench -- --scaleout --ops 5000 --export csv

# Failover benchmark
cargo run -p falcon_bench -- --failover --ops 10000 --export csv
```

Key parameters: `--ops`, `--read-pct`, `--local-pct`, `--shards`, `--isolation` (rc/si), `--export` (text/csv/json). Random seed fixed at 42.

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

See [docs/observability.md](docs/observability.md) for full metric descriptions.

---

## Supported SQL

For full reference see [docs/sql_compatibility.md](docs/sql_compatibility.md). Quick summary:

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
┌─────────────────────────────────────────────────────────┐
│  PG Wire Protocol (TCP)  │  Native Protocol (TCP/TLS)  │
├──────────────────────────┴─────────────────────────────┤
│         SQL Frontend (sqlparser-rs → Binder)            │
├────────────────────────────────────────────────────────┤
│         Planner / Router                               │
├────────────────────────────────────────────────────────┤
│         Executor (row-at-a-time + fused streaming agg)  │
├──────────────────┬─────────────────────────────────────┤
│   Txn Manager    │   Storage Engine                    │
│   (MVCC, OCC)    │   (MemTable + LSM + WAL + GC)      │
│                  ├─────────────────────────────────────┤
│                  │   USTM — User-Space Tiered Memory   │
│                  │   Hot(DRAM) │ Warm(LIRS-2) │ Cold   │
│                  │   IoScheduler │ QueryPrefetcher     │
├──────────────────┴─────────────────────────────────────┤
│  Cluster (ShardMap, Replication, Failover, Epoch)      │
└────────────────────────────────────────────────────────┘
```

### Crate Structure

| Crate | Responsibility |
|-------|---------------|
| `falcon_common` | Shared types, errors, config, datum, schema, RLS, RBAC |
| `falcon_storage` | In-memory tables, LSM engine, MVCC, indexes, WAL, GC, TDE, partitioning, PITR, CDC, **USTM page cache** |
| `falcon_txn` | Transaction lifecycle, OCC validation, timestamp allocation |
| `falcon_sql_frontend` | SQL parsing (sqlparser-rs) + binding/analysis |
| `falcon_planner` | Logical → physical plan, routing hints |
| `falcon_executor` | Operator execution, expression evaluation, governor, fused streaming aggregates |
| `falcon_protocol_pg` | PostgreSQL wire protocol codec + TCP server |
| `falcon_protocol_native` | FalconDB native binary protocol — encode/decode, compression, type mapping |
| `falcon_native_server` | Native protocol server — session management, executor bridge, nonce anti-replay |
| `falcon_raft` | Consensus stub (NOT on production path — single-node no-op) |
| `falcon_cluster` | Shard map, replication, failover, scatter/gather, epoch, migration, supervisor, stability hardening, failover×txn test matrix |
| `falcon_observability` | Metrics (Prometheus), structured logging, tracing |
| `falcon_server` | Main binary, wires all components |
| `falcon_bench` | YCSB-style benchmark harness |

### Java JDBC Driver (`clients/falcondb-jdbc/`)

| Module | Responsibility |
|--------|---------------|
| `io.falcondb.jdbc` | JDBC Driver, Connection, Statement, PreparedStatement, ResultSet, DataSource |
| `io.falcondb.jdbc.protocol` | Native protocol wire format, TCP connection, handshake, auth |
| `io.falcondb.jdbc.ha` | HA-aware failover: ClusterTopologyProvider, PrimaryResolver, FailoverRetryPolicy |

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

All milestones through v1.2 are released. Current test count: **3,972** across 16 crates.

| Milestone | Highlights |
|-----------|------------|
| **v0.1–v0.9** ✅ | OLTP foundation, WAL, failover, gRPC, security, chaos-hardening |
| **v1.0** ✅ | LSM engine, SQL completeness, enterprise features (RLS/TDE/PITR/CDC), distributed hardening |
| **v1.0.1–v1.0.3** ✅ | Zero-panic, failover×txn matrix, determinism & trust hardening |
| **v1.1–v1.2** ✅ | USTM tiered memory, fused streaming aggregates, near-PG query parity |

### RPO / RTO

FalconDB supports two production durability policies: `local-fsync` (default, RPO > 0 possible)
and `sync-replica` (primary waits for replica WAL ack, RPO ≈ 0). See [docs/rpo_rto.md](docs/rpo_rto.md) for details.

---

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System architecture, crate structure, data flow |
| [docs/rpo_rto.md](docs/rpo_rto.md) | RPO/RTO guarantees per durability policy |
| [docs/error_model.md](docs/error_model.md) | Unified error model, SQLSTATE mapping, retry hints |
| [docs/observability.md](docs/observability.md) | Prometheus metrics, SHOW commands, slow query log |
| [docs/security.md](docs/security.md) | Security features, RBAC, SQL firewall, audit |
| [docs/ops_runbook.md](docs/ops_runbook.md) | Operations runbook: failover, rolling upgrade, scale-out |
| [docs/backup_restore.md](docs/backup_restore.md) | Backup and restore procedures |
| [docs/sql_compatibility.md](docs/sql_compatibility.md) | SQL compatibility reference |
| [docs/INSTALL.md](docs/INSTALL.md) | Installation guide |
| [CHANGELOG.md](CHANGELOG.md) | Semantic versioning changelog (v0.1–v1.2) |

---

## Benchmarks

FalconDB ships with a reproducible benchmark suite for pgbench comparison, failover validation, and kernel performance.

```bash
# pgbench: FalconDB vs PostgreSQL (requires both servers running)
./scripts/bench_pgbench_vs_postgres.sh

# Failover under load (starts 2 FalconDB nodes, kills primary, checks determinism)
./scripts/bench_failover_under_load.sh

# Internal kernel benchmark (no server required)
./scripts/bench_kernel_falcon_bench.sh

# Quick smoke test (CI)
./scripts/ci_bench_smoke.sh
```

Results are written to `bench_out/<timestamp>/` with logs, JSON metrics, and a Markdown report.

> Windows users: use `scripts/bench_pgbench_vs_postgres.ps1` (PowerShell 7+).

---

## Testing

```bash
# Run all tests (3,972 total)
cargo test --workspace

# By crate (key ones)
cargo test -p falcon_cluster   # 585 tests
cargo test -p falcon_server    # 383 tests
cargo test -p falcon_storage   # 364 tests
cargo test -p falcon_common    # 252 tests

# Lint
cargo clippy --workspace       # must be 0 warnings
```

---

## License

Apache-2.0
