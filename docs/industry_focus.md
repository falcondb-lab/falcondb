# FalconDB — Industry Focus Decision

> **Decision**: FalconDB targets **Financial Trading & Risk Control OLTP** as its
> sole industry vertical. This is a binding strategic decision, not a marketing
> suggestion.

---

## 1. The Choice

| Field | Value |
|-------|-------|
| **Target Industry** | Financial services — trading, risk control, clearing |
| **Target Systems** | Order Management (OMS), Risk Engine, Position Keeper, Clearing Ledger |
| **Target Users** | Quantitative trading desks, risk departments, clearing houses, digital banks |
| **NOT For** | General-purpose web apps, analytics/OLAP, content management, IoT telemetry |

## 2. Why Financial OLTP — First Principles

FalconDB's architecture was built memory-first with deterministic transaction
outcomes. These are not generic database features — they are the **exact
requirements** of financial trading infrastructure:

| FalconDB Architecture | Financial Requirement |
|-----------------------|----------------------|
| Memory-first storage (no disk I/O on hot path) | Sub-millisecond order entry latency |
| Deterministic failover (P0-2: zero phantom commits) | Regulatory requirement: no lost/duplicated trades |
| MVCC with OCC (optimistic concurrency) | High-throughput concurrent position updates |
| 4-tier backpressure (P1-1: throttle → reject → shed) | Graceful degradation during market spikes |
| WAL with group commit | Durability without latency sacrifice |
| Replication with integrity checking (P1-3) | Primary–DR site consistency for regulators |
| SLA admission control | Priority lanes: market orders > batch reconciliation |

## 3. Why NOT the Other Candidates

### Industrial / MES / SCADA — Rejected

| Factor | Assessment |
|--------|-----------|
| Write pattern | Append-only time-series (not transactional) |
| Concurrency | Low (one PLC per line) |
| Latency tolerance | 10–100ms acceptable |
| FalconDB fit | Over-engineered; TimescaleDB/InfluxDB better fit |
| Market | Fragmented, low ACV, long sales cycles |

### Energy / Power / Metering — Rejected

| Factor | Assessment |
|--------|-----------|
| Write pattern | Bulk meter reads (batch, not real-time txn) |
| Transaction complexity | Simple inserts, no multi-table ACID needed |
| Latency tolerance | Seconds acceptable |
| FalconDB fit | Memory-first is wasted; PostgreSQL + partitioning sufficient |
| Market | Regulated procurement, 18+ month cycles |

## 4. Why PostgreSQL / Oracle / MySQL Fall Short

### PostgreSQL

| Financial Requirement | PostgreSQL Gap |
|----------------------|---------------|
| P99 write latency < 1ms | PG disk-first: typical P99 = 5–20ms with fsync |
| Deterministic failover | PG streaming replication: async by default, risk of data loss |
| Backpressure under spike | PG has no built-in admission control; OOM or connection exhaustion |
| Position update throughput | PG MVCC bloat under high-update workloads; requires aggressive VACUUM |
| Failover time | PG manual/Patroni failover: 5–30 seconds typical |

### Oracle

| Financial Requirement | Oracle Gap |
|----------------------|-----------|
| Deployment agility | Oracle licensing: $47K/core + 22% annual support |
| Cloud-native | Oracle RAC requires shared storage; not cloud-friendly |
| Latency | Oracle Exadata optimized for throughput, not single-txn latency |
| Vendor lock-in | Proprietary protocol, no wire-compatible migration path |

### MySQL

| Financial Requirement | MySQL Gap |
|----------------------|-----------|
| MVCC correctness | MySQL InnoDB: phantom reads under REPEATABLE READ |
| Failover determinism | MySQL Group Replication: split-brain risk under network partition |
| Transaction complexity | MySQL: limited CHECK constraints, no advanced isolation |

## 5. FalconDB's Irreplaceable Advantages

1. **Sub-millisecond P99 writes** — memory-first, no disk on hot path
2. **Zero phantom commits under failover** — proven by 9-experiment matrix (P0-2)
3. **Backpressure that protects, not crashes** — 4-tier governor (P1-1)
4. **72h+ stability without degradation** — proven by long-run framework (P1-2)
5. **Primary–replica integrity guarantee** — checksum-verified replication (P1-3)
6. **PostgreSQL wire compatibility** — zero migration cost for existing PG clients
7. **Apache 2.0 core** — no licensing tax on compute

## 6. Target System Architectures

### 6.1 Order Management System (OMS)

```
Market Feed → [FalconDB: orders table] → Matching Engine
                    ↓
              [FalconDB: positions table] → Risk Check
                    ↓
              [FalconDB: executions table] → Clearing
```

- **Write pattern**: Burst (market open), sustained (trading hours), quiet (overnight)
- **Txn pattern**: `BEGIN → INSERT order → UPDATE position → INSERT execution → COMMIT`
- **Concurrency**: 100–10,000 concurrent sessions
- **Latency SLA**: P99 < 1ms write, P99.9 < 5ms

### 6.2 Real-Time Risk Engine

```
Position Updates → [FalconDB: risk_exposures] → Limit Check → Accept/Reject
                         ↓
                   [FalconDB: risk_alerts] → Dashboard
```

- **Write pattern**: Continuous high-frequency updates
- **Txn pattern**: `SELECT exposure → compute → UPDATE exposure` (hot row contention)
- **Concurrency**: Per-instrument parallelism, cross-instrument serialization
- **Latency SLA**: P99 < 500µs for limit check

### 6.3 Clearing & Settlement Ledger

```
Trade Confirmations → [FalconDB: ledger] → Netting → Settlement Instructions
                           ↓
                     [FalconDB: audit_trail] → Regulatory Report
```

- **Write pattern**: Batch at end-of-day, real-time for intraday clearing
- **Txn pattern**: Multi-row ACID (debit + credit must be atomic)
- **Durability**: RPO = 0 (zero data loss)
- **Audit**: Every write must be traceable

## 7. Explicit Non-Goals

| We do NOT target | Reason |
|-----------------|--------|
| Web application backends | Too generic; PostgreSQL is good enough |
| Analytics / OLAP | FalconDB is row-store OLTP; use ClickHouse/DuckDB |
| IoT / time-series | Append-only workloads; use TimescaleDB/InfluxDB |
| Document stores | No JSON-first design; use MongoDB |
| Graph workloads | No graph model; use Neo4j |
| Multi-model | Focus beats breadth at this stage |

## 8. Decision Finality

This decision is **irreversible for the current funding cycle**. All product,
engineering, marketing, and sales efforts align to Financial Trading OLTP.

Revisiting the industry choice requires:
- Evidence that financial OLTP TAM is insufficient
- A board-level strategic review
- At minimum 2 paying customer rejections citing industry mismatch

---

*Approved: v1.2.0 · Decision date: 2025-02*
