# FalconDB Financial Edition — Product Definition

> **FalconDB is not "a database". It is the OLTP engine purpose-built for
> financial trading infrastructure.**

---

## 1. What FalconDB Financial Edition Is

A **drop-in OLTP kernel** for financial systems that require:

- Sub-millisecond write latency under production load
- Zero data loss under failover (RPO = 0)
- Deterministic transaction outcomes (no phantom commits)
- Graceful degradation during market spikes (not crash, not stall)
- PostgreSQL wire compatibility (no application rewrite)

## 2. Core Use Cases

### UC-1: Order Entry & Position Keeping

| Aspect | Specification |
|--------|--------------|
| **Tables** | `orders`, `positions`, `executions` |
| **Write model** | Burst at market open (10K+ TPS), sustained during trading hours |
| **Txn pattern** | `BEGIN → INSERT order → UPDATE position → INSERT execution → COMMIT` |
| **Latency** | P99 < 1ms write, P99.9 < 5ms |
| **Concurrency** | 100–10,000 sessions |
| **Durability** | Every committed trade survives node failure |

### UC-2: Real-Time Risk Limit Check

| Aspect | Specification |
|--------|--------------|
| **Tables** | `risk_exposures`, `risk_limits`, `risk_alerts` |
| **Write model** | Continuous high-frequency updates (position delta per trade) |
| **Txn pattern** | `SELECT exposure → compute risk → UPDATE exposure` |
| **Latency** | P99 < 500µs for accept/reject decision |
| **Concurrency** | Per-instrument parallel, cross-instrument serial |
| **Correctness** | OCC detects stale reads; no silent over-limit |

### UC-3: Clearing & Settlement Ledger

| Aspect | Specification |
|--------|--------------|
| **Tables** | `ledger`, `settlements`, `audit_trail` |
| **Write model** | Intraday real-time + end-of-day batch |
| **Txn pattern** | Multi-row ACID: `debit(A) + credit(B)` atomic |
| **Durability** | RPO = 0, every entry auditable |
| **Regulatory** | Full WAL + audit trail for compliance |

## 3. Transaction Model

| Property | FalconDB Financial Edition |
|----------|---------------------------|
| **Isolation** | Snapshot Isolation (SI) via MVCC |
| **Concurrency control** | Optimistic (OCC) — no lock waits on hot path |
| **Conflict resolution** | First-committer-wins; loser gets retryable abort |
| **Determinism** | Transaction outcome is deterministic given initial state |
| **Failover behavior** | Committed = durable; Aborted = invisible; In-doubt = explicitly tracked |

## 4. Write Model

```
Client → PostgreSQL wire protocol → FalconDB
  → Memory-first INSERT/UPDATE (no disk I/O)
  → WAL append (group commit, fdatasync)
  → Replication to standby (synchronous optional)
  → Backpressure if memory pressure (delay → reject → shed)
```

**Key property**: The hot path is entirely in-memory. Disk is only touched at
WAL flush (batched) and cold segment compaction (background).

## 5. SLA Commitments

| Metric | Target | Evidence |
|--------|--------|----------|
| Write latency P99 | < 1 ms | `benchmarks/` (W1 single-table) |
| Write latency P99.9 | < 5 ms | `benchmarks/` (W1 single-table) |
| Txn success rate | > 99.9% | `evidence/failover/` (0 phantom commits) |
| Failover impact window | < 3 seconds | `docs/failover_determinism_report.md` |
| RPO (data loss) | 0 (sync replication) | WAL + sync replica ack |
| RTO (recovery time) | < 30 seconds | Automated promote + catch-up |
| Memory pressure behavior | Documented, never OOM | `docs/memory_backpressure.md` (P1-1) |
| Stability (72h continuous) | No leak, no drift | `scripts/run_stability_test.sh` (P1-2) |

## 6. Default Configuration (Industry Best Practice)

FalconDB Financial Edition ships with defaults tuned for financial workloads.
**No tuning required for the first PoC.**

```toml
# falcondb-financial.toml — Industry defaults

[server]
pg_listen_addr = "0.0.0.0:5432"
max_connections = 1000

[storage]
wal_enabled = true

[wal]
sync_mode = "fdatasync"           # durability: every group commit is fsync'd
group_commit_window_us = 100      # aggressive: 100µs window for sub-ms latency

[memory]
shard_soft_limit_bytes = 8589934592   # 8 GB — enters throttle
shard_hard_limit_bytes = 12884901888  # 12 GB — enters reject

[replication]
mode = "sync"                     # RPO=0: wait for replica ack before commit
max_lag_before_alert_us = 1000000 # alert if replica > 1s behind

[compression]
compression_profile = "off"       # no compression overhead on hot path
```

## 7. What FalconDB Financial Edition Is NOT

| It is NOT | Use instead |
|-----------|-------------|
| An analytics engine | ClickHouse, DuckDB, Snowflake |
| A time-series database | TimescaleDB, InfluxDB |
| A full SQL database (JOINs, subqueries) | PostgreSQL, CockroachDB |
| A distributed NewSQL | TiDB, CockroachDB, YugabyteDB |
| A managed cloud service (yet) | Self-hosted or partner-hosted |

## 8. PostgreSQL Compatibility

| Feature | Status |
|---------|--------|
| Wire protocol (libpq, JDBC, psycopg2) | ✅ Full |
| Simple query protocol | ✅ |
| Extended query protocol (prepared statements) | ✅ |
| `BEGIN` / `COMMIT` / `ROLLBACK` | ✅ |
| `INSERT` / `UPDATE` / `DELETE` / `SELECT` | ✅ |
| `CREATE TABLE` / `DROP TABLE` / `ALTER TABLE` | ✅ |
| Secondary indexes | ✅ |
| `EXPLAIN` | ✅ |
| JOINs | ⚠️ Limited (single-table focus for v1.x) |
| Subqueries | ⚠️ Limited |
| Stored procedures | ❌ Not planned for v1.x |
| Extensions (PostGIS, etc.) | ❌ Not applicable |

## 9. Deployment Model

```
┌─────────────┐     ┌─────────────┐
│  Primary     │────▶│  Standby    │  (sync replication, RPO=0)
│  FalconDB    │     │  FalconDB   │
└─────────────┘     └─────────────┘
       │
       ▼
┌─────────────┐
│  Client App  │  (OMS, Risk Engine, Clearing)
│  via libpq   │
└─────────────┘
```

- **Primary**: Handles all reads and writes
- **Standby**: Synchronous replica for failover
- **Client**: Any PostgreSQL client library
- **Failover**: Automatic promote with zero phantom commits
