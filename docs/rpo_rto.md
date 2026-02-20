# RPO / RTO Guarantees and Commit-Ack Modes

This document defines FalconDB's Recovery Point Objective (RPO) and Recovery
Time Objective (RTO) under each replication and durability configuration.

---

## Definitions

| Term | Meaning |
|------|---------|
| **RPO** | Maximum data loss window after failure (seconds or transactions) |
| **RTO** | Maximum time to restore service after failure |
| **Commit Ack** | When the primary tells the client "COMMIT OK" |
| **WAL durable** | WAL record fsynced to local disk |
| **Replica ack** | Replica has received and applied the WAL record |

---

## Current Guarantees (M1 / M2)

### Durability Policy: `local-fsync` (default)

```toml
[wal]
durability_policy = "local-fsync"
```

| Scenario | RPO | RTO | Notes |
|----------|-----|-----|-------|
| Primary crash, WAL intact | **0** | Seconds (WAL replay) | Full recovery from WAL |
| Primary crash, disk lost | **> 0** | Minutes (replica promote) | Committed txns not yet replicated are lost |
| Replica crash | **0** | Seconds (reconnect + catch-up) | Replica re-syncs from primary WAL |
| Network partition (P→R) | **0** (primary side) | N/A | Primary continues; replica stalls, catches up on reconnect |

**Key tradeoff**: Commit latency is low (local fsync only), but RPO > 0 is
possible if the primary fails before replication delivers the latest WAL to
at least one replica.

### Durability Policy: `quorum-ack`

```toml
[wal]
durability_policy = "quorum-ack"
```

| Scenario | RPO | RTO | Notes |
|----------|-----|-----|-------|
| Primary crash, disk lost | **0** | Minutes (replica promote) | Quorum has the data |
| Single replica crash | **0** | Seconds | Quorum still available |
| All replicas crash | **> 0** | Depends on recovery | Falls back to local-fsync behavior |

**Key tradeoff**: Higher commit latency (must wait for quorum ack), but RPO = 0
as long as a quorum of replicas survives.

### Durability Policy: `all-ack`

```toml
[wal]
durability_policy = "all-ack"
```

| Scenario | RPO | RTO | Notes |
|----------|-----|-----|-------|
| Any single node crash | **0** | Seconds | All replicas have every committed record |
| All replicas unreachable | Blocked | N/A | Commits stall until a replica reconnects |

**Key tradeoff**: Strongest guarantee (RPO = 0 unconditionally), but highest
commit latency and availability risk if any replica is slow or unreachable.

---

## RTO Breakdown

| Recovery Action | Estimated RTO | Bottleneck |
|-----------------|---------------|------------|
| WAL replay (local crash) | 1–10s | WAL size, I/O speed |
| Replica promote (fencing protocol) | 2–5s | 5-step fencing + catch-up |
| Full checkpoint restore | 10–60s | Checkpoint size, network (gRPC streaming) |
| New replica bootstrap | 30s–5min | Full checkpoint download + WAL catch-up |

---

## Admission Control (WAL Backpressure)

FalconDB can reject new write transactions when replication lag grows too large,
bounding the RPO window even under `local-fsync`:

```toml
[wal]
# Reject new writes if WAL backlog exceeds 256 MB
backlog_admission_threshold_bytes = 268435456

# Reject new writes if slowest replica lags > 5 seconds
replication_lag_admission_threshold_ms = 5000
```

This gives a **bounded RPO** under `local-fsync`: even though commit ack doesn't
wait for replicas, the admission gate ensures replicas are never more than
`threshold` behind.

---

## Roadmap: Commit-Ack Modes

### M2 (current) — Implemented

| Mode | Config Value | Commit Waits For | RPO |
|------|-------------|------------------|-----|
| Local fsync | `local-fsync` | WAL fsync on primary | > 0 possible |
| Quorum ack | `quorum-ack` | WAL fsync + majority replica ack | 0 (quorum alive) |
| All ack | `all-ack` | WAL fsync + all replica acks | 0 (all alive) |

### M3 (planned) — Enhancements

| Feature | Description | Status |
|---------|-------------|--------|
| **Per-transaction override** | `SET falcon.commit_ack = 'quorum-ack'` per session | Planned |
| **Synchronous standby list** | `synchronous_standby_names` equivalent | Planned |
| **Witness node** | Non-data quorum participant for 2-node clusters | Planned |
| **Async commit** | `SET falcon.commit_ack = 'async'` — no fsync, no wait | Planned |
| **Lag-aware routing** | Read queries routed to replicas within lag threshold | Planned |

### M4 (future)

| Feature | Description |
|---------|-------------|
| **Cross-region quorum** | Quorum across geographically distributed replicas |
| **Tiered durability** | Different durability per table (hot = async, cold = sync) |

---

## Monitoring RPO in Production

```sql
-- Current replication lag (primary side)
SHOW falcon.replication_stats;

-- WAL backlog (bytes not yet replicated)
SHOW falcon.wal_stats;

-- Replica applied LSN (replica side)
SHOW falcon.node_role;
```

### Prometheus Metrics

| Metric | Description |
|--------|-------------|
| `falcon_replication_lag_us` | Current replication lag in µs |
| `falcon_replication_max_lag_us` | Peak replication lag |
| `falcon_wal_backlog_bytes` | WAL bytes written but not yet replicated |

---

## Recommendations

| Use Case | Recommended Policy | Why |
|----------|-------------------|-----|
| Development / testing | `local-fsync` | Lowest latency, no replica needed |
| Production (latency-sensitive) | `local-fsync` + admission control | Bounded RPO, low commit latency |
| Production (durability-critical) | `quorum-ack` | RPO = 0 with acceptable latency |
| Financial / compliance | `all-ack` | Strongest guarantee, accept latency |
