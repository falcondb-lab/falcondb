# FalconDB Self-Healing Architecture

> v1.0.9 — "Cluster problems are solved by the cluster, not by humans."

## Overview

FalconDB's self-healing subsystem automatically detects node failures, elects new leaders, catches up lagging replicas, and adjusts admission under pressure — all without manual SSH or firefighting.

## Node Liveness State Machine

Every node in the cluster is tracked by the `ClusterFailureDetector` with one of five states:

```
                  heartbeat_timeout
    ┌──────┐    ─────────────────►   ┌─────────┐    failure_timeout    ┌──────┐
    │ ALIVE │                        │ SUSPECT  │  ────────────────►   │ DEAD │
    └───┬───┘   ◄─────────────────   └────┬────┘                      └──┬───┘
        │        heartbeat_restored       │                              │
        │                                 │   heartbeat_restored         │
        │                                 └──────────────────────────────┘
        │                                           │
        ▼                                           ▼
    ┌──────────┐                              ┌──────────┐
    │ DRAINING │                              │ JOINING  │
    └──────────┘                              └──────────┘
```

| State | Meaning | Reads | Writes |
|-------|---------|-------|--------|
| **ALIVE** | Heartbeat on time | ✅ | ✅ |
| **SUSPECT** | Heartbeat late (> `suspect_threshold`) | ✅ | ⚠️ paused |
| **DEAD** | No heartbeat (> `failure_timeout`) | ❌ | ❌ |
| **DRAINING** | Graceful shutdown in progress | ❌ new | ❌ new |
| **JOINING** | Syncing data into cluster | ❌ | ❌ |

## Configuration

```toml
[cluster.failure_detector]
heartbeat_interval = "1s"
suspect_threshold = "3s"
failure_timeout = "10s"
max_consecutive_misses = 5
auto_declare_dead = true
```

## Epoch Fencing

Every state transition bumps a monotonic **epoch** counter. Stale primaries with lower epoch are rejected. This prevents split-brain scenarios where a partitioned old leader continues accepting writes.

## Automatic Leader Re-election

When the failure detector declares a shard's leader DEAD:

1. Election coordinator starts a new **term** (monotonically increasing)
2. Surviving replicas submit candidacy with their current LSN
3. Highest-LSN candidate wins (quorum can be required)
4. New leader is installed; old term operations are rejected

### Configuration

```toml
[cluster.election]
election_timeout = "5s"
election_max_duration = "15s"
require_quorum = true
```

### Anti-Split-Brain

- **Term validation**: all writes carry the current term; stale-term writes are rejected
- **Epoch fencing**: epoch is bumped on every failover; old-epoch primaries cannot write
- **Quorum requirement**: optional majority quorum before election resolves

## Automatic Replica Catch-up

When a replica falls behind, the catch-up coordinator classifies the lag:

| Lag Class | Condition | Strategy |
|-----------|-----------|----------|
| `CAUGHT_UP` | lag = 0 | No action |
| `WAL_REPLAY` | lag ≤ `snapshot_threshold_lsn` | Stream WAL entries |
| `SNAPSHOT_REQUIRED` | lag > `snapshot_threshold_lsn` | Full snapshot + WAL tail |

### Configuration

```toml
[cluster.catchup]
snapshot_threshold_lsn = 100000
wal_batch_size = 1000
snapshot_rate_limit = 0          # bytes/sec, 0 = unlimited
catch_up_timeout = "300s"
allow_leader_impact = false
```

## Metrics

All self-healing components expose metrics via `FailureDetectorMetrics`, `ElectionMetrics`, `CatchUpMetrics`:

| Metric | Description |
|--------|-------------|
| `heartbeats_received` | Total heartbeats processed |
| `suspect_transitions` | ALIVE → SUSPECT transitions |
| `dead_transitions` | SUSPECT/ALIVE → DEAD transitions |
| `elections_started` | Elections initiated |
| `elections_completed` | Successfully resolved elections |
| `elections_timed_out` | Elections that exceeded max duration |
| `wal_replays_started` | WAL-based catch-ups initiated |
| `snapshots_started` | Snapshot-based catch-ups initiated |
| `bytes_streamed` | Total bytes streamed for catch-up |

## Ops Audit

Every state change is recorded in the `OpsAuditLog` ring buffer. See [ops_runbook.md](ops_runbook.md) for event types.
