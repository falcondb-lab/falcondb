# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">English | <a href="README_CN.md">简体中文</a></p>

<p align="center">
  <strong>PG-Compatible · Distributed · Memory-First · Deterministic Transaction Semantics</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.3.0-blue" alt="Version" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="License" />
</p>

> FalconDB is a **PG-compatible, distributed, memory-first OLTP database** with
> deterministic transaction semantics. Benchmarked against PostgreSQL, VoltDB,
> and SingleStore — see **[Benchmark Matrix](benchmarks/README.md)**.
>
> - ✅ **Low latency** — single-shard fast-path commits bypass 2PC entirely; Hot Row Optimization keeps version chain depth-1 under high-contention update workloads
> - ✅ **Provable consistency** — MVCC/OCC under Snapshot Isolation, Wound-Wait deadlock prevention, CI-verified ACID
> - ✅ **Vectorized execution** — Filter + Project + Aggregate full vectorized path on `RecordBatch`; fused streaming aggregates for large scans
> - ✅ **Operability** — 50+ SHOW commands, Prometheus metrics, AIOps engine, failover CI gate
> - ✅ **Determinism** — hardened state machine, bounded in-doubt, idempotent retry, WAL Pipeline Commit
> - ❌ Not HTAP — no analytical workloads
> - ❌ Not full PG — [see unsupported list below](#not-supported)

FalconDB provides OLTP with fast/slow-path transactions, WAL-based primary–replica
replication (gRPC streaming), promote/failover, MVCC garbage collection, vectorized
query execution, and reproducible benchmarks.

### Deterministic Commit Guarantee (DCG)

> **If FalconDB returns "committed", that transaction will survive any single-node crash,
> any failover, and any recovery — with zero exceptions.**

This is FalconDB's core engineering property, called the **Deterministic Commit Guarantee (DCG)**.
It is not a configuration option — it is the default behavior under the `LocalWalSync` commit policy.

- **Prove it yourself**: [pocs/falcondb-poc-dcg/](pocs/falcondb-poc-dcg/) — one-click demo: write 1,000 orders → kill -9 primary → verify zero data loss
- **Benchmark it yourself**: [pocs/falcondb-poc-pgbench/](pocs/falcondb-poc-pgbench/) — pgbench comparison vs PostgreSQL under identical durability settings
- **Crash under load**: [pocs/falcondb-poc-failover-under-load/](pocs/falcondb-poc-failover-under-load/) — kill -9 primary during sustained writes → verify zero data loss + automatic recovery
- **See inside**: [pocs/falcondb-poc-observability/](pocs/falcondb-poc-observability/) — live Grafana dashboard, Prometheus metrics, operational controls
- **Migrate from PG**: [pocs/falcondb-poc-migration/](pocs/falcondb-poc-migration/) — migrate a real PostgreSQL app by changing only the connection string
- **Recover after disaster**: [pocs/falcondb-poc-backup-pitr/](pocs/falcondb-poc-backup-pitr/) — destroy the database, restore from backup, replay to exact second, verify every row matches

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
| **Types** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL, user-defined ENUM |
| **Transactions** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, per-txn timeout, Read Committed, Snapshot Isolation |
| **Functions** | 500+ scalar functions (string, math, date/time, crypto, JSON, array) |
| **Observability** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE, pg_stat_statements |

### <a id="not-supported"></a>Not Supported (v1.2)

The following features are **explicitly out of scope** for v1.2.
Attempting to use them returns a clear `ErrorResponse` with the appropriate SQLSTATE code.

| Feature | Error Code | Error Message |
|---------|-----------|---------------|
| ~~Stored procedures / PL/pgSQL~~ | ✅ | **Implemented** — `CREATE [OR REPLACE] FUNCTION ... LANGUAGE SQL/plpgsql`, `DROP FUNCTION`, `CALL`, PL/pgSQL: DECLARE, IF/ELSIF/ELSE, WHILE/FOR LOOP, RETURN, RAISE, PERFORM |
| ~~Triggers~~ | ✅ | **Implemented** — `CREATE TRIGGER`, `DROP TRIGGER [IF EXISTS]`, BEFORE/AFTER INSERT/UPDATE/DELETE, FOR EACH ROW/STATEMENT, PL/pgSQL trigger functions. Enable via `[triggers]` config section (`enabled = true`). |
| ~~Custom types (beyond JSONB)~~ | ✅ | **Implemented** — `CREATE TYPE name AS ENUM (...)`, `DROP TYPE [IF EXISTS]`, enum values usable as column defaults and in expressions. |
| ~~Materialized views~~ | ✅ | **Implemented** — `CREATE MATERIALIZED VIEW ... AS SELECT`, `DROP MATERIALIZED VIEW [IF EXISTS]`, `REFRESH MATERIALIZED VIEW` |
| Foreign data wrappers (FDW) | `0A000` | `foreign data wrappers are not supported` |
| ~~Full-text search~~ | ✅ | **Implemented** — `tsvector`/`tsquery` types, `@@` operator, `to_tsvector`/`to_tsquery`/`ts_rank`/`ts_headline` + 10 more FTS functions |
| HTAP / ColumnStore analytics | — | ColumnStore storage + vectorized AGG pushdown implemented; full analytics pipeline in progress |
| ~~Automatic rebalancing~~ | ✅ | **Implemented** — production-grade `RebalanceRunner` with policy-driven shard migration, batched row transfer, pause/resume, Prometheus metrics, Raft-aware rebalance, 47 chaos tests. Enable via `[rebalance]` config section. |
| Full-text search (advanced) | — | Basic FTS implemented; phrase search, custom dictionaries in progress |

> **Scope Guard**: Full HTAP analytics pipeline is in progress. ColumnStore
> storage and vectorized GROUP BY AGG pushdown are implemented. Raft consensus
> is available via `replication.role = "raft_member"` (see [ARCHITECTURE.md](ARCHITECTURE.md) §2.7).
>
> **Standard Edition**: The Standard edition uses RocksDB as the default storage engine — disk-first, distributed, and fully PostgreSQL compatible. Deploy with `--features standard` or `cargo build --features edition_standard`.

### Optional / Non-Default Features (implemented, not on default build path)

| Feature | Module Status | Notes |
|---------|:------------:|-------|
| Raft consensus replication | PRODUCTION | `falcon_raft` — `SingleNodeConsensus` (no-op default) or `RaftConsensus` (multi-node via `role = raft_member`). 32 tests, leader election, failover, gRPC transport. |
| LSM-tree storage engine | DEFAULT | `lsm/` — enabled by default (`default = ["lsm"]`); USTM-integrated for both Rowstore and LSM |
| RocksDB storage engine | EXPERIMENTAL | `rocksdb_table.rs` — compile-gated (`--features rocksdb`); full MVCC integration, same version-chain as LSM |
| redb storage engine | EXPERIMENTAL | `redb_table.rs` — compile-gated (`--features redb`); pure Rust, zero C deps |
| Auto shard rebalancing | PRODUCTION | `rebalancer.rs` + `raft_rebalance.rs` — policy-driven background rebalancer with batched row migration, pause/resume, Prometheus metrics, exponential backoff. Enable via `[rebalance]` config section. |

### Non-Default Features (implemented, feature-gated or not yet on default production path)

| Feature | Module Status | Notes |
|---------|:------------:|-------|
| Disk B-tree rowstore | EXPERIMENTAL | `disk_rowstore.rs` — 966-line page-based B-tree engine with page splits, LRU buffer pool, leaf chain scan, 9 tests. Feature-gated (`--features disk_rowstore`). |
| ColumnStore analytics | PARTIAL | `columnstore.rs` storage + `executor_columnar.rs` vectorized GROUP BY AGG pushdown implemented; full scan/filter pipeline in progress |
| Transparent Data Encryption (TDE) | IMPLEMENTED | `encryption.rs` — 886 lines, 22 tests. AES-256-GCM with PBKDF2 key derivation, per-scope DEKs, key rotation, re-encryption. Wired into WAL + SST + engine. Feature-gated (`--features encryption_tde`). |
| Point-in-Time Recovery (PITR) | IMPLEMENTED | `pitr.rs` — 598 lines, 9 tests. WAL archiver, base backup management, named restore points, recovery executor with 4 targets (Latest/Time/LSN/XID). Feature-gated (`--features pitr`). |
| Multi-tenant resource isolation | EXPERIMENTAL | `resource_isolation.rs` (547 lines) — I/O token-bucket + CPU concurrency limiter. `tenant_registry.rs` (515 lines) — tenant lifecycle, QPS/memory/txn quota enforcement. Feature-gated (`--features resource_isolation,tenant`). |

---

## 1. Building

```bash
# Prerequisites: Rust 1.75+ (rustup), C/C++ toolchain (MSVC on Windows, gcc/clang on Linux/macOS)

# Build all crates (debug)
cargo build --workspace

# Build release (default: Rowstore + LSM)
cargo build --release --workspace

# Build with RocksDB engine (requires LLVM + MSVC on Windows)
cargo build --release -p falcon_server --features rocksdb

# Build with redb engine (pure Rust, no C deps)
cargo build --release -p falcon_server --features redb

# Run tests (4,350+ tests across 18 crates + root integration)
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
| **RocksDB** | Disk (RocksDB library) | Full disk persistence | Limited by disk space | Production disk-backed OLTP, proven LSM compaction |
| **redb** | Disk (pure-Rust embedded KV) | Full disk persistence | Limited by disk space | Lightweight embedded, zero C dependencies |

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

LSM uses a Log-Structured Merge-Tree for disk-based storage. **Enabled by default** (the `lsm` feature is in the default feature set).

```sql
-- Create tables with ENGINE=lsm
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

### RocksDB (Disk-Backed, Feature-Gated)

RocksDB uses the actual RocksDB C++ library. **Requires `--features rocksdb` at compile time** and LLVM + MSVC build environment on Windows.

```bash
cargo build --release -p falcon_server --features rocksdb
```

```sql
CREATE TABLE orders (id BIGINT PRIMARY KEY, data TEXT) ENGINE=rocksdb;
```

**Characteristics:**
- Full MVCC integration (same version-chain encoding as LSM)
- write_buffer_size: 64MB, LZ4 compression, max_background_jobs: 4
- Production-proven compaction via RocksDB

### redb (Disk-Backed, Feature-Gated)

redb is a pure-Rust embedded key-value store. **Requires `--features redb` at compile time.**

```bash
cargo build --release -p falcon_server --features redb
```

```sql
CREATE TABLE logs (id BIGINT PRIMARY KEY, msg TEXT) ENGINE=redb;
```

**Characteristics:**
- Zero C/C++ dependencies — pure Rust build
- ACID transactions with MVCC integration
- Suitable for lightweight embedded deployments

### Mixing Engines

Different tables in the same database can use different engines:

```sql
-- Hot data: in-memory for speed
CREATE TABLE sessions (id INT PRIMARY KEY, token TEXT, expires_at TIMESTAMP);

-- Cold data: disk-backed for capacity
CREATE TABLE audit_log (id BIGSERIAL PRIMARY KEY, event TEXT, ts TIMESTAMP) ENGINE=lsm;

-- RocksDB-backed for proven durability
CREATE TABLE trades (id BIGINT PRIMARY KEY, symbol TEXT, qty INT) ENGINE=rocksdb;

-- Queries work identically regardless of engine
SELECT * FROM sessions s JOIN audit_log a ON s.id = a.user_id;
```

### Engine Selection Guide

| Scenario | Recommended Engine |
|----------|-------------------|
| Low-latency OLTP (< 10ms p99) | Rowstore |
| Data fits in memory (< available RAM) | Rowstore |
| Large datasets (> available RAM) | LSM / RocksDB |
| Compliance / must persist to disk | LSM / RocksDB / redb |
| Production disk OLTP with proven compaction | RocksDB |
| Lightweight embedded, no C deps | redb |
| Mixed: hot tables + cold tables | Rowstore + LSM/RocksDB |

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

## 3. Configuration & Replication

### Minimal `falcon.toml`

```toml
[server]
pg_listen_addr = "0.0.0.0:5433"
admin_listen_addr = "0.0.0.0:8080"
node_id = 1

[storage]
wal_enabled = true
data_dir = "./falcon_data"

[replication]
commit_ack = "primary_durable"      # RPO > 0 possible
```

Generate a full default config: `cargo run -p falcon_server -- --print-default-config > falcon.toml`. See [docs/OPERATIONS.md](docs/OPERATIONS.md) for all config sections (`[wal]`, `[gc]`, `[ustm]`, `[rebalance]`, `[memory]`).

### CLI Options

```
falcon -c <FILE> --pg-addr <ADDR> --data-dir <DIR> --no-wal --replica --primary-addr <ADDR>
```

### Replication (M1)

- **Commit Ack**: primary WAL fsync → ack client (RPO > 0 if primary fails before shipping)
- **WAL shipping**: `WalChunk` frames with LSN range + CRC32
- **Ack tracking**: replicas report `applied_lsn`; primary resumes from `ack_lsn + 1`
- **Replica read-only**: writes rejected until promoted

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

> **Query Performance Roadmap**: The remaining 4.3x query gap is a known priority.
> Planned optimizations include: (1) vectorized batch execution to amortize per-row
> overhead, (2) column-oriented read cache for scan-heavy queries, (3) prefetch-aware
> DashMap iteration to reduce pointer-chasing latency. Track progress in
> [CHANGELOG.md](CHANGELOG.md).

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

## AI Query Optimizer

FalconDB includes a built-in **online-learning query optimizer** that sits on top of the rule-based planner. It requires no external services and adds zero cold-start latency — the rule-based plan is always the fallback until the model has enough data.

### How It Works

```
SQL → Binder → LogicalPlan
                    │
          extract_features()          ← 15-dim numeric vector
                    │
          generate_candidates()       ← rule-based plan + alternatives
                    │
          AiOptimizer::select_plan()  ← linear cost model (SGD)
                    │
             Executor runs plan
                    │
          record_feedback(actual_us)  ← online SGD weight update
```

### Feature Vector (15 dimensions)

| Index | Feature |
|-------|---------|
| 0 | log₂(estimated output rows + 1) |
| 1 | join count |
| 2 | filter predicate count |
| 3 | has GROUP BY (0/1) |
| 4 | has ORDER BY (0/1) |
| 5 | has LIMIT (0/1) |
| 6 | index available on any scanned table (0/1) |
| 7 | log₂(largest table rows + 1) |
| 8 | log₂(second-largest table rows + 1) |
| 9 | cross-table selectivity estimate (0..1) |
| 10 | projection column count |
| 11 | has aggregation (0/1) |
| 12 | has DISTINCT (0/1) |
| 13 | subquery depth |
| 14 | log₂(total bytes estimate + 1) |

### Cost Model

- **Algorithm**: online SGD (stochastic gradient descent) with L2 regularization
- **Prediction target**: `log₂(execution_time_μs)`
- **Plan-kind bias**: 7 one-hot dimensions for `SeqScan / IndexScan / IndexRangeScan / NestedLoopJoin / HashJoin / MergeSortJoin / Other`
- **Model dimension**: 23 weights (15 features + 7 plan-kind one-hot + 1 bias)
- **Warm-up threshold**: 50 samples — model falls back to rule-based plan until trained
- **Per-query fingerprint history**: tracks average execution time per query shape for secondary statistics

### Candidate Plan Generation

When the rule-based optimizer selects an `IndexScan`, the AI layer also generates a `SeqScan` candidate. The model scores both and picks the lower predicted cost. This lets the system learn when stale index statistics make a full scan faster.

### Observability

```sql
-- View AI optimizer diagnostics
SHOW AI STATS;
```

Returns:

| metric | description |
|--------|-------------|
| `enabled` | Whether AI plan selection is active |
| `samples_trained` | Total SGD updates performed |
| `model_ready` | Whether warm-up threshold (50 samples) is met |
| `ema_mae_log2` | Exponentially-weighted mean absolute error (log₂ cost units) |
| `query_fingerprints` | Number of distinct query shapes tracked |

### Enable / Disable

AI plan selection is **enabled by default**. It has no observable effect until the warm-up threshold is reached — during warm-up the rule-based plan is always used.

```sql
-- The optimizer self-tunes continuously; no manual config needed.
-- Use SHOW AI STATS to monitor training progress.
SHOW AI STATS;
```

### Weight Persistence

Model weights can be exported and imported via the Rust API for cross-restart learning:

```rust
// Export weights for persistence
let weights = executor.ai_optimizer.export_weights();

// Import previously saved weights
executor.ai_optimizer.import_weights(weights);
```

---

## AIOps Engine

FalconDB includes a built-in **AIOps (Autonomous Database Operations)** engine that runs entirely in-process with no external dependencies. It automatically monitors query behavior, detects anomalies, and provides actionable recommendations — all accessible via SQL.

### Four Subsystems

#### 1. Slow Query Detector

Adaptive threshold based on an **EMA p95 baseline** — not a static timeout.

- Initial floor: 100 ms (absolute minimum to flag)
- Adaptive threshold: `max(100ms, ema_p95 × 2.0)` — tightens as workload normalizes
- Warms up after 100 samples; uses absolute floor during warm-up
- Retains the 200 most recent slow query records (rolling)
- Records truncated original SQL text (first 200 chars) and table name per entry

#### 2. Anomaly Detector

**Welford online variance** (3σ rule) on two independent signals:

| Signal | Description |
|--------|-------------|
| TPS | Per-second query throughput |
| Latency | Per-second average query duration (µs) |

- Requires ≥ 30 per-second observations to activate (no cold-start false positives)
- Fires `WARNING` on TPS spikes or latency anomalies
- Fires `CRITICAL` when latency exceeds 5× the running mean
- Fires `WARNING` when ≥ 5 **consecutive** slow queries are detected (source: `AnomalyDetector/SlowQuery`)
- Retains 500 most recent alerts (rolling)

#### 3. Index Advisor

Tracks **full-scan events** tagged by table name:

- Fires a recommendation when a table accumulates ≥ 10 full scans with avg duration ≥ 50 ms
- Identifies top-3 filter columns from WHERE clause usage patterns
- Generates a ready-to-run `CREATE INDEX` statement
- Ranks recommendations by impact score (`full_scan_count × avg_duration_us`)

#### 4. Workload Profiler

Per-query **fingerprint aggregation** (normalized SQL, first 120 chars):

- Tracks: call count, total/avg/max latency, **p95/p99** (reservoir sampling, 100 samples), error rate per fingerprint
- Supports up to 1000 distinct fingerprints
- Top-N queries by total time or call count for capacity planning

### SQL Interface

```sql
-- AIOps engine summary (12 metrics)
SHOW AIOPS STATS;

-- Recent anomaly alerts (up to 50, most recent first)
SHOW AIOPS ALERTS;

-- Index recommendations ranked by impact
SHOW AIOPS INDEX ADVICE;

-- Top 20 query fingerprints by total execution time (with p95/p99)
SHOW AIOPS WORKLOAD;

-- Recent slow queries (up to 100, newest first) with SQL text
SHOW AIOPS SLOW QUERIES;
```

#### `SHOW AIOPS STATS` output

| metric | description |
|--------|-------------|
| `slow_query_samples` | Total queries processed by slow query detector |
| `slow_query_threshold_us` | Current adaptive slow query threshold (µs) |
| `slow_query_ema_baseline_us` | EMA p95 baseline used for threshold calculation |
| `slow_query_logged` | Number of slow queries in rolling buffer |
| `anomaly_tps_mean` | Running mean TPS |
| `anomaly_tps_stddev` | Running TPS standard deviation |
| `anomaly_latency_mean_us` | Running mean latency (µs) |
| `anomaly_latency_stddev_us` | Running latency standard deviation |
| `anomaly_alerts_total` | Total anomaly alerts fired |
| `index_advisor_tables_tracked` | Tables with full-scan history |
| `index_advisor_advice_count` | Active index recommendations |
| `workload_fingerprints` | Distinct query shapes tracked |

#### `SHOW AIOPS ALERTS` output

| column | description |
|--------|-------------|
| `id` | Monotonically increasing alert ID |
| `ts_unix_ms` | Alert timestamp (Unix ms) |
| `severity` | `WARNING` or `CRITICAL` |
| `source` | `AnomalyDetector/TPS` or `AnomalyDetector/Latency` |
| `message` | Human-readable description with observed vs. baseline values |

#### `SHOW AIOPS INDEX ADVICE` output

| column | description |
|--------|-------------|
| `table_name` | Table with frequent full scans |
| `full_scan_count` | Number of full scans recorded |
| `avg_scan_duration_us` | Average full scan duration (µs) |
| `column_hints` | Top filter columns from WHERE clauses |
| `suggestion` | Ready-to-run `CREATE INDEX` statement |

#### `SHOW AIOPS WORKLOAD` output

| column | description |
|--------|-------------|
| `fingerprint` | Normalized SQL fingerprint (first 120 chars) |
| `call_count` | Total invocation count |
| `avg_us` | Average execution time (µs) |
| `p95_us` | p95 latency estimate (reservoir sampling) |
| `p99_us` | p99 latency estimate (reservoir sampling) |
| `max_us` | Maximum observed execution time (µs) |
| `total_us` | Total CPU time consumed (µs) |
| `error_count` | Number of executions that returned an error |

#### `SHOW AIOPS SLOW QUERIES` output

| column | description |
|--------|-------------|
| `ts_unix_ms` | When the slow query was detected (Unix ms) |
| `duration_us` | Actual execution time (µs) |
| `threshold_us` | Adaptive threshold at time of detection (µs) |
| `plan_kind` | Physical plan type (e.g. `SeqScan`, `IndexScan`) |
| `table_name` | Primary table scanned |
| `sql_text` | Truncated original SQL (first 200 chars) |

### Data Flow

```
Every query execution (Executor::execute)
        │
        ├─ SlowQueryDetector.record(fingerprint, duration_us, plan_kind)
        │       └─ updates EMA baseline, appends to rolling buffer if slow
        │
        ├─ AnomalyDetector.record(duration_us)
        │       └─ per-second TPS+latency accumulation → Welford update → 3σ alert
        │
        ├─ IndexAdvisor.record_full_scan(table, duration_us, filter_cols)
        │       └─ only for SeqScan plans; increments per-table accumulators
        │
        └─ WorkloadProfiler.record(sql, duration_us, is_error)
                └─ normalizes fingerprint, updates per-fingerprint stats
```

### Integration Point

The AIOps engine uses a **global singleton** (`global_aiops()`) shared across all executor instances in a process, so statistics accumulate across sessions without any configuration.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  PG Wire Protocol (TCP)  │  Native Protocol (TCP/TLS)  │
├──────────────────────────┴─────────────────────────────┤
│         SQL Frontend (sqlparser-rs → Binder)            │
├────────────────────────────────────────────────────────┤
│         Planner / CBO / AI Optimizer / Router            │
├────────────────────────────────────────────────────────┤
│  Executor (row-at-a-time + vectorized Filter/Project/Agg)│
│  RecordBatch path (≥256 rows) · fused streaming agg     │
│  Hash Agg Spill-to-disk · vectorized_project / filter   │
├──────────────────┬─────────────────────────────────────┤
│   Txn Manager    │   Storage Engine                    │
│   (MVCC, OCC)    │   engine_tables: DashMap<TableHandle>│
│   Wound-Wait     │     ├─ Rowstore(MemTable)  ← fast   │
│   LockManager    │     │    Hot Row Opt (depth-1 chain) │
│   Timestamp      │     ├─ Lsm / RocksDb / Redb         │
│   batch=64       │     │    └→ dyn StorageTable trait   │
│                  │     └─ Columnstore (AGG pushdown)    │
│                  ├─────────────────────────────────────┤
│                  │   USTM — User-Space Tiered Memory   │
│                  │   Hot(DRAM) │ Warm(LIRS-2) │ Cold   │
│                  │   prefetch_hint / scan_prefetch_hint │
├──────────────────┴─────────────────────────────────────┤
│  Cluster (ShardMap, Replication, Failover, Epoch)       │
│  WAL Pipeline Commit · Fuzzy Checkpoint                 │
└────────────────────────────────────────────────────────┘
```

### Crate Structure

| Crate | Responsibility |
|-------|---------------|
| `falcon_common` | Shared types, errors, config, datum (incl. Decimal), schema, RLS, RBAC |
| `falcon_storage` | Multi-engine storage: unified `TableHandle` dispatch → `StorageTable` trait; Rowstore (in-memory), LSM, RocksDB, redb; MVCC, secondary indexes, WAL, GC, USTM prefetch, TDE, CDC |
| `falcon_txn` | Transaction lifecycle, OCC validation, timestamp allocation |
| `falcon_sql_frontend` | SQL parsing (sqlparser-rs) + binding/analysis |
| `falcon_planner` | Plan generation, cost-based optimizer (selectivity, scan cost, plan_optimized), AI optimizer (online SGD cost model, candidate plan selection, execution feedback), routing hints, distributed wrapping, view/DDL plans |
| `falcon_executor` | Operator execution, expression evaluation, governor, fused streaming aggregates, vectorized Filter+Project+Agg full path (`RecordBatch`), Hash Aggregation Spill-to-disk, FTS engine, vectorized columnstore AGG pushdown |
| `falcon_protocol_pg` | PostgreSQL wire protocol codec + TCP server |
| `falcon_protocol_native` | FalconDB native binary protocol — encode/decode, compression, type mapping |
| `falcon_native_server` | Native protocol server — session management, executor bridge, nonce anti-replay |
| `falcon_raft` | Raft consensus — `SingleNodeConsensus` (standalone default) or `RaftConsensus` (production multi-node via `role = raft_member`) |
| `falcon_cluster` | Shard map, replication, failover, scatter/gather, epoch, migration, supervisor, stability hardening, failover×txn test matrix |
| `falcon_observability` | Metrics (Prometheus), structured logging, tracing, pg_stat_statements |
| `falcon_proto` | Protobuf definitions + tonic gRPC codegen (replication, Raft) |
| `falcon_segment_codec` | Segment-level compression (Zstd, LZ4, dictionary, streaming, CRC) |
| `falcon_enterprise` | Enterprise features: control plane HA, security (AuthN/AuthZ, TLS rotation), ops (auto-rebalance, SLO engine) |
| `falcon_server` | Main binary, wires all components |
| `falcon_cli` | Interactive CLI client — REPL, cluster management, import/export, failover, consistency checks |
| `falcon_bench` | YCSB-style + 6-scenario OLTP benchmark suite (`bench_oltp_scenarios`: point select, insert, update by PK, short transaction, batch insert, hot row update) |

### Client SDKs (`clients/`)

| SDK | Language | Features |
|-----|----------|----------|
| `falcondb-jdbc` | Java | JDBC 4.2, HA failover, connection pooling |
| `falcondb-go` | Go | Native protocol, HA seed list, pooling |
| `falcondb-python` | Python | DB-API 2.0, HA failover, pooling |
| `falcondb-node` | Node.js | Async, TLS, HA failover, TypeScript types |

All SDKs support: native binary protocol, HA-aware failover with seed lists, connection pooling, and epoch fencing.

**Standard PG drivers** (psycopg2, pgx, node-postgres, pgjdbc, tokio-postgres, Npgsql) also connect directly via PG wire protocol on port 5433. See [`docs/pg_driver_compatibility.md`](docs/pg_driver_compatibility.md) for details.

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

```bash
# Standalone (build + start + SQL smoke test + benchmark)
./scripts/demo_standalone.sh          # Linux/macOS/WSL
.\scripts\demo_standalone.ps1         # Windows

# Primary + Replica replication
./scripts/demo_replication.sh

# E2E Failover (two-node closed-loop: write → kill primary → promote → verify)
./scripts/e2e_two_node_failover.sh    # Linux/macOS/WSL
.\scripts\e2e_two_node_failover.ps1   # Windows
```

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

---

## Roadmap

All milestones through v1.3 are released. Current test count: **4,380+** across 18 crates (417 `.rs` files).

| Milestone | Highlights |
|-----------|------------|
| **v0.1–v0.9** ✅ | OLTP foundation, WAL, failover, gRPC, security, chaos-hardening |
| **v1.0** ✅ | LSM engine, SQL completeness, enterprise features (RLS/TDE/PITR/CDC), distributed hardening |
| **v1.0.1–v1.0.3** ✅ | Zero-panic, failover×txn matrix, determinism & trust hardening |
| **v1.1** ✅ | USTM tiered memory, fused streaming aggregates, near-PG query parity |
| **v1.2** ✅ | Standard edition (RocksDB default engine), edition feature flags, PL/pgSQL triggers, ColumnStore GC & truncate, `CREATE TYPE ... AS ENUM`, AI optimizer (online SGD, `SHOW AI STATS`), RocksDB GC flush + checkpoint replica sync |
| **v1.3** ✅ | **OLTP engine parity with SingleStore Rowstore**: Wound-Wait deadlock prevention (O(1) vs WFG cycle-detection), timestamp batch allocation (batch=64, 98% reduction in contention), Fuzzy Checkpoint (dirty-page tracking + background flush), WAL Pipeline Commit (group commit double-buffer, 500-spin fast path), Hash Aggregation Spill-to-disk (grace hash spill + merge), Hot Row Optimization (CoalescedUpdateSlot, in-place version update, depth-1 chain maintenance), Vectorized execution full path Filter+Project+Agg on `RecordBatch`, 6-scenario OLTP benchmark suite (`cargo bench --bench bench_oltp_scenarios`) |

### RPO / RTO

FalconDB supports two production durability policies: `local-fsync` (default, RPO > 0 possible)
and `sync-replica` (primary waits for replica WAL ack, RPO ≈ 0). See [docs/rpo_rto.md](docs/rpo_rto.md) for details.

---

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System architecture, crate structure, data flow |
| [ARCHITECTURE_zh.md](ARCHITECTURE_zh.md) | 系统架构文档（中文） |
| [docs/INSTALL.md](docs/INSTALL.md) | Installation & uninstall (Windows + Linux) |
| [docs/UPGRADE.md](docs/UPGRADE.md) | Upgrade, rolling upgrade & version management |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Service management, monitoring, config management |
| [docs/cluster_ops.md](docs/cluster_ops.md) | Cluster operations, 2PC, failover & ops runbook |
| [docs/security.md](docs/security.md) | Auth, RBAC, TLS, SQL firewall, audit |
| [docs/observability.md](docs/observability.md) | Metrics, SHOW commands, SLA/SLO, perf guardrails |
| [docs/failover_behavior.md](docs/failover_behavior.md) | Failover invariants, replication integrity |
| [docs/pg_driver_compatibility.md](docs/pg_driver_compatibility.md) | PG driver compatibility & JDBC connection |
| [docs/sql_compatibility.md](docs/sql_compatibility.md) | SQL compatibility reference |
| [docs/error_model.md](docs/error_model.md) | Unified error model, SQLSTATE mapping, retry hints |
| [docs/rpo_rto.md](docs/rpo_rto.md) | RPO/RTO guarantees per durability policy |
| [docs/backup_restore.md](docs/backup_restore.md) | Backup and restore procedures |
| [docs/opentelemetry.md](docs/opentelemetry.md) | OpenTelemetry (OTLP) tracing, metrics & logs export |
| [docs/audit_log.md](docs/audit_log.md) | Audit logging: config, SIEM integration, retention |
| [docs/production_checklist.md](docs/production_checklist.md) | Production readiness checklist |
| [docs/cli.md](docs/cli.md) | CLI reference (falcon-cli) |
| [docs/self_healing.md](docs/self_healing.md) | Self-healing architecture |
| [docs/design/](docs/design/) | Design docs: MVCC, WAL, sharding, vectorized exec |
| [docs/adr/](docs/adr/) | Architecture Decision Records (ADR-001–007) |
| [CHANGELOG.md](CHANGELOG.md) | Semantic versioning changelog (v0.1–v1.2) |
| [README_CN.md](README_CN.md) | 中文用户手册 |

---

## Testing

```bash
# Run all tests (4,380+ total)
cargo test --workspace

# By crate (key ones)
cargo test -p falcon_cluster   # 1,050+ tests
cargo test -p falcon_storage   # 820+ tests
cargo test -p falcon_server    # 420+ tests
cargo test -p falcon_executor  # 320+ tests
cargo test -p falcon_common    # 250+ tests

# Lint
cargo clippy --workspace       # must be 0 warnings
```

---

## Enterprise Features (PostgreSQL Enterprise Parity)

FalconDB implements PostgreSQL Enterprise-equivalent capabilities when using RocksDB as the storage engine. All features are accessible via standard SQL — no external services required.

### Row Level Security (RLS)

```sql
-- Enable RLS on a table
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;

-- Create a policy: each user sees only their own rows
CREATE POLICY tenant_isolation ON orders
  FOR SELECT
  USING (tenant_id = current_user);

-- Restrictive policy (all policies must pass)
CREATE POLICY write_guard ON orders
  AS RESTRICTIVE
  FOR INSERT
  WITH CHECK (status = 'pending');

-- Drop a policy
DROP POLICY tenant_isolation ON orders;

-- Inspect policies
SHOW POLICIES ON orders;
-- columns: table_name, policy_name, command, permissive, using_expr, check_expr, rls_enabled

-- Disable RLS
ALTER TABLE orders DISABLE ROW LEVEL SECURITY;
```

### Logical Replication Slots

```sql
-- Create a logical replication slot (compatible with pg_logical / pgoutput)
CREATE REPLICATION SLOT my_slot LOGICAL pgoutput;
-- Returns: slot_name, plugin, confirmed_flush_lsn

-- List all slots
SHOW REPLICATION SLOTS;
-- columns: slot_name, plugin, confirmed_flush_lsn, restart_lsn, active, created_at_unix_ms

-- Drop a slot
DROP REPLICATION SLOT my_slot;
```

### DML Audit Log

The executor maintains an in-process ring buffer (capacity 4096) of audit events, recorded for every DML operation.

```sql
-- Show most recent 100 audit events
SHOW AUDIT LOG;

-- Show last 50 events
SHOW AUDIT LOG LIMIT 50;

-- Filter by table name (matches events whose SQL references the table)
SHOW AUDIT LOG FOR TABLE orders LIMIT 20;
-- columns: event_id, timestamp_ms, event_type, role_name, detail, sql, success
```

### RocksDB Enterprise Storage Engine

When compiled with `--features rocksdb`, the `rocksdb_engine.rs` backend provides:

| Feature | Implementation |
|---------|---------------|
| **Column Family isolation** | System CF / user-data CF / secondary-index CF / TTL CF |
| **Tiered compression** | LZ4 for L0–L2 (hot), Zstd for L3+ (cold) |
| **TTL / Row Expiry** | RocksDB `CompactionFilter` on TTL CF, key-encoded expiry timestamp |
| **Secondary indexes** | Dedicated index CF, index-only scan path |
| **TDE hooks** | `encryption.rs` AES-256-GCM DEK integration point for RocksDB Env |
| **MVCC** | Per-PK shard locks, atomic commit/abort batches, version chain encoding |

### Online DDL

```sql
-- Monitor progress of background schema changes
SHOW DDL STATUS;
-- columns: id, operation, phase, table_id, rows_processed, rows_total, elapsed_ms, error
```

Phases: `pending → running → backfilling → completed` (metadata-only ops skip to `completed` immediately).

---

## License

Apache-2.0
