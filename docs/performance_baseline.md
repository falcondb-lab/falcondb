# FalconDB Performance Baseline

> **Version**: v0.9.0  
> **Updated**: 2026-02-21  
> **Purpose**: Establish P99 latency targets and baseline measurements for v1.0.0 gate validation.

---

## SLO Targets (v1.0.0 Gate)

| Scenario | Metric | Target | Gate Threshold |
|----------|--------|--------|----------------|
| Point lookup (empty load) | P99 latency | < 1 ms | < 5 ms |
| Point lookup (mixed load) | P99 latency | < 5 ms | < 20 ms |
| INSERT single row (empty load) | P99 latency | < 2 ms | < 10 ms |
| Txn commit (mixed OLTP) | P99 latency | < 10 ms | < 50 ms |
| Failover RTO | Recovery time | < 5 s | < 15 s |
| Failover RPO (quorum-ack) | Data loss | 0 bytes | 0 bytes |
| In-doubt convergence | Resolution time | < 30 s | < 60 s |
| GC pause impact | P99 delta | < 1 ms | < 5 ms |

---

## Benchmark Methodology

### Hardware Reference

| Component | Specification |
|-----------|--------------|
| CPU | 8+ cores, x86_64 (Intel Xeon / AMD EPYC or equivalent) |
| Memory | 16+ GB DDR4 |
| Storage | NVMe SSD (for WAL fsync) |
| Network | Loopback (single-node) or 1 Gbps (multi-node) |
| OS | Linux 5.15+ (Ubuntu 22.04) or Windows Server 2022 |

### Workload Profiles

#### Profile 1: Point Lookup (Read-Only)

```sql
-- Schema
CREATE TABLE bench_kv (k INT PRIMARY KEY, v TEXT);
-- Pre-load 100,000 rows
INSERT INTO bench_kv SELECT i, repeat('x', 100) FROM generate_series(1, 100000) AS i;

-- Workload: random point lookups
SELECT v FROM bench_kv WHERE k = $1;  -- $1 = random(1, 100000)
```

- **Concurrency**: 1, 4, 16, 64 clients
- **Duration**: 60 seconds per concurrency level
- **Measurement**: P50, P95, P99, P99.9 latency; throughput (QPS)

#### Profile 2: Insert-Heavy (Write-Only)

```sql
-- Schema
CREATE TABLE bench_insert (id SERIAL PRIMARY KEY, payload TEXT, ts TIMESTAMP DEFAULT NOW());

-- Workload: sequential inserts
INSERT INTO bench_insert (payload) VALUES ($1);  -- $1 = random 100-byte string
```

- **Concurrency**: 1, 4, 16 clients
- **Duration**: 60 seconds per concurrency level
- **Measurement**: P50, P95, P99 commit latency; WAL flush rate

#### Profile 3: Mixed OLTP (Read-Write)

```sql
-- 80% reads, 20% writes (YCSB-B like)
-- Read: SELECT v FROM bench_kv WHERE k = $1;
-- Write: UPDATE bench_kv SET v = $2 WHERE k = $1;
```

- **Concurrency**: 4, 16, 64 clients
- **Duration**: 120 seconds per concurrency level
- **Measurement**: P50, P95, P99 latency (read/write separate); throughput

#### Profile 4: Failover Under Load

```
1. Start primary + replica (quorum-ack)
2. Run Profile 3 at 16 clients
3. Kill primary (SIGKILL)
4. Measure: time until replica promoted and serving writes (RTO)
5. Verify: no committed data lost (RPO = 0)
```

#### Profile 5: Backpressure Stress

```
1. Configure memory budget: shard_soft_limit = 50MB, shard_hard_limit = 100MB
2. Run Profile 2 at 64 clients (sustained overload)
3. Verify: stable rejection rate, no OOM, no panic
4. Measure: rejection latency, recovery time after load drops
```

---

## Benchmark Tool

### Built-in YCSB Harness

```bash
# Point lookup benchmark
cargo run -p falcon_bench --release -- \
  --workload read-only \
  --record-count 100000 \
  --operation-count 1000000 \
  --threads 16

# Mixed OLTP benchmark
cargo run -p falcon_bench --release -- \
  --workload mixed \
  --read-proportion 0.8 \
  --record-count 100000 \
  --operation-count 500000 \
  --threads 16

# Insert-heavy benchmark
cargo run -p falcon_bench --release -- \
  --workload insert-only \
  --operation-count 500000 \
  --threads 4
```

### pgbench (External)

```bash
# Initialize
pgbench -h 127.0.0.1 -p 5433 -U falcon -i -s 10

# Run TPC-B like workload
pgbench -h 127.0.0.1 -p 5433 -U falcon -c 16 -j 4 -T 60 -P 5

# Read-only
pgbench -h 127.0.0.1 -p 5433 -U falcon -c 16 -j 4 -T 60 -S -P 5
```

---

## Baseline Measurements (v0.9.0)

### Single-Node, In-Memory (No WAL)

| Workload | Concurrency | P50 | P95 | P99 | QPS |
|----------|-------------|-----|-----|-----|-----|
| Point lookup | 1 | < 0.1 ms | < 0.2 ms | < 0.5 ms | ~50,000 |
| Point lookup | 16 | < 0.2 ms | < 0.5 ms | < 1 ms | ~200,000 |
| INSERT | 1 | < 0.1 ms | < 0.3 ms | < 0.5 ms | ~30,000 |
| INSERT | 16 | < 0.3 ms | < 0.8 ms | < 2 ms | ~100,000 |
| Mixed (80/20) | 16 | < 0.2 ms | < 0.5 ms | < 2 ms | ~150,000 |

### Single-Node, WAL Enabled (fdatasync)

| Workload | Concurrency | P50 | P95 | P99 | QPS |
|----------|-------------|-----|-----|-----|-----|
| Point lookup | 16 | < 0.2 ms | < 0.5 ms | < 1 ms | ~200,000 |
| INSERT | 1 | < 0.5 ms | < 1 ms | < 2 ms | ~5,000 |
| INSERT (group commit) | 16 | < 0.5 ms | < 1.5 ms | < 3 ms | ~50,000 |
| Mixed (80/20) | 16 | < 0.3 ms | < 1 ms | < 3 ms | ~100,000 |

### Two-Node (Primary + Replica, quorum-ack)

| Workload | Concurrency | P50 | P95 | P99 | QPS |
|----------|-------------|-----|-----|-----|-----|
| Point lookup (primary) | 16 | < 0.2 ms | < 0.5 ms | < 1 ms | ~180,000 |
| INSERT (quorum-ack) | 16 | < 1 ms | < 3 ms | < 5 ms | ~30,000 |
| Mixed (80/20) | 16 | < 0.5 ms | < 2 ms | < 5 ms | ~80,000 |
| Failover RTO | — | — | — | < 5 s | — |

> **Note**: These are estimated baseline targets. Actual measurements should be collected
> on reference hardware using the benchmark methodology above and recorded in CI artifacts.

---

## Regression Detection

### CI Integration

```bash
# Run performance baseline check (future CI gate)
# 1. Run benchmark suite
# 2. Compare P99 against thresholds
# 3. Fail if any P99 exceeds gate threshold

cargo run -p falcon_bench --release -- \
  --workload mixed \
  --record-count 100000 \
  --operation-count 100000 \
  --threads 16 \
  --assert-p99-ms 10
```

### Monitoring in Production

| Prometheus Metric | Alert Threshold |
|-------------------|----------------|
| `falcon_txn_commit_latency_p99` | > 10 ms |
| `falcon_query_latency_p99` | > 20 ms |
| `falcon_wal_flush_latency_p99` | > 5 ms |
| `falcon_replication_lag_ms` | > 1000 ms |
| `falcon_admission_rejected_total` (rate) | > 100/s sustained |

---

## Optimization Levers

| Lever | Impact | Config |
|-------|--------|--------|
| **Group commit** | Amortizes fsync across N txns | `wal.group_commit = true` |
| **Priority scheduler** | Protects OLTP from large queries | `PrioritySchedulerConfig` |
| **Token bucket** | Throttles DDL/rebalance | `TokenBucketConfig::rebalance()` |
| **Memory budget** | Prevents OOM via backpressure | `memory.shard_soft_limit_bytes` |
| **Circuit breaker** | Isolates failing shards | `ShardCircuitBreaker` |
| **Connection pooling** | Reduces connection overhead | External (PgBouncer) |
| **Fused streaming aggregates** | Eliminates row cloning for COUNT/SUM/AVG/MIN/MAX/GROUP BY | Automatic (executor fast path) |
| **Zero-copy MVCC iteration** | Avoids OwnedRow clone during full scans | Automatic (`for_each_visible`) |
| **Bounded-heap top-K** | O(N log K) instead of O(N log N) for ORDER BY LIMIT | Automatic (`scan_top_k_by_pk`) |
| **MVCC Arc-clone elimination** | Skips Arc atomic inc/dec for single-version rows | Automatic (head-first visibility check) |

---

## Bulk Insert + Query Benchmark (1M Rows)

> **Updated**: 2026-02-23
> **Workload**: `benchmarks/bulk_insert_1m.sql` — 1 DROP + 1 CREATE + 100 INSERT batches (10K rows each) + 10 analytical queries (COUNT, ORDER BY LIMIT, aggregates with CASE WHEN, GROUP BY, WHERE BETWEEN)

### Results (Single-Node, In-Memory, No WAL)

| Metric | FalconDB | PostgreSQL 16 | Ratio |
|--------|----------|---------------|-------|
| **Total elapsed** | ~6,400 ms | ~6,100 ms | 1.05x |
| **INSERT phase** (100 × 10K rows) | ~2,900 ms | ~5,400 ms | **0.54x (faster)** |
| **Query phase** (10 queries) | ~2,800 ms | ~650 ms | 4.3x |
| **Rows/s** (INSERT) | ~340,000 | ~185,000 | **1.84x (faster)** |

### Query Phase Breakdown

| Query Type | Executor Path | Estimated Time |
|------------|--------------|----------------|
| `COUNT(*)` | `count_visible` fast path | ~400 ms |
| `SELECT * ORDER BY id LIMIT 10` (×2) | `scan_top_k_by_pk` bounded heap | ~400 ms each |
| Multi-aggregate (9 projections, CASE WHEN) | `exec_fused_aggregate` | ~500 ms |
| `GROUP BY active` + aggregates | `exec_fused_aggregate` | ~500 ms |
| `COUNT(*) WHERE value BETWEEN` (×2) | `exec_fused_aggregate` | ~500 ms each |

### Optimization History

| Version | Total | Query Phase | vs PostgreSQL |
|---------|-------|-------------|---------------|
| Before optimization | 11,922 ms | ~7,762 ms | 1.96x slower |
| + Fused streaming aggregate | 7,696 ms | ~4,085 ms | 1.27x slower |
| + Bounded-heap top-K | 7,380 ms | ~3,284 ms | 1.22x slower |
| + MVCC Arc-clone elimination | **6,355 ms** | **~2,800 ms** | **~1.05x (near parity)** |

### Remaining Bottleneck

The query phase (~2.8s for 7 full scans of 1M rows) is bounded by **memory latency** from
DashMap pointer-chasing across ~324MB of scattered heap allocations. Each row access involves
3-4 pointer indirections (DashMap entry → Arc\<VersionChain\> → RwLock\<Arc\<Version\>\> →
OwnedRow → Vec\<Datum\>). PostgreSQL's sequential heap pages (~8KB tuples) are inherently
more cache-friendly for full table scans. Further improvement requires architectural changes
(arena allocation, columnar storage, or parallel shard iteration).

### Running the Benchmark

```bash
# Build
cargo build --release -p falcon_server -p falcon_bench

# Start FalconDB (in-memory, no WAL)
./target/release/falcon --no-wal &

# Run benchmark
./target/release/falcon_bench --bulk \
  --bulk-file benchmarks/bulk_insert_1m.sql \
  --bulk-host localhost --bulk-port 5433 \
  --bulk-sslmode disable --export text

# Compare with PostgreSQL
psql -h localhost -U postgres -f benchmarks/bulk_insert_1m.sql
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| v0.9.0 | 2026-02-21 | Initial baseline document |
| latest | 2026-02-23 | Added 1M row bulk insert benchmark, optimization levers for fused aggregates/MVCC fast path |
