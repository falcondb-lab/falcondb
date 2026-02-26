# FalconDB — Cluster Operations Guide

> **Audience**: SREs, DBAs, and operators managing FalconDB distributed clusters.

---

## Table of Contents

1. [Cluster Architecture Overview](#1-cluster-architecture-overview)
2. [Epoch / Fencing Model](#2-epoch--fencing-model)
3. [Replication & Commit Policies](#3-replication--commit-policies)
4. [Failover Procedure](#4-failover-procedure)
5. [Cross-Shard Transactions (2PC)](#5-cross-shard-transactions-2pc)
6. [Membership Lifecycle](#6-membership-lifecycle)
7. [Shard Migration](#7-shard-migration)
8. [Cluster Status & Observability](#8-cluster-status--observability)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Cluster Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   Control Plane                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │Controller│  │Controller│  │Controller│  (3-node) │
│  │  (Leader)│  │(Follower)│  │(Follower)│           │
│  └────┬─────┘  └──────────┘  └──────────┘          │
│       │ Raft consensus for metadata                  │
└───────┼─────────────────────────────────────────────┘
        │
┌───────┼─────────────────────────────────────────────┐
│       ▼           Data Plane                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐             │
│  │ Node 1  │  │ Node 2  │  │ Node 3  │             │
│  │ Shard 1 │  │ Shard 1 │  │ Shard 2 │             │
│  │(Primary)│  │(Replica) │  │(Primary)│             │
│  │ Shard 2 │  │ Shard 2 │  │ Shard 1 │             │
│  │(Replica) │  │(Primary)│  │(Replica) │             │
│  └─────────┘  └─────────┘  └─────────┘             │
└─────────────────────────────────────────────────────┘
```

**Key components**:
- **Control Plane**: 3-node Raft group managing metadata (shard map, node registry, config)
- **Data Plane**: N nodes hosting shard replicas with WAL-based replication
- **Epoch**: Monotonically increasing term for each shard, bumped on every leadership change
- **Storage Epoch Fence**: Final write barrier in the WAL that rejects stale-epoch writes

---

## 2. Epoch / Fencing Model

### What is an Epoch?

Every shard has an **authoritative epoch** (a monotonically increasing `u64`). The epoch is bumped on every leadership change (failover, promotion, manual transfer).

### Two-Layer Fencing

| Layer | Component | Location | Purpose |
|-------|-----------|----------|---------|
| **Cluster** | `SplitBrainDetector` | `falcon_cluster` | Rejects stale-epoch RPCs at the cluster routing layer |
| **Storage** | `StorageEpochFence` | `falcon_storage` | Rejects stale-epoch writes at the WAL write barrier |

### How It Works

1. **Normal operation**: All writes carry `writer_epoch`. Both layers check `writer_epoch >= current_epoch`.
2. **Promotion**: Epoch bumped → `advance_epoch(new)` called on both `SplitBrainDetector` and `StorageEpochFence`.
3. **Stale leader**: Old leader's writes have `old_epoch < new_epoch` → rejected at both layers with:
   - Error: `split-brain rejected: writer node N epoch X < current epoch Y`
   - Log: `EPOCH FENCE: stale-epoch WAL write rejected at storage barrier`
   - Metric: `epoch_fence_rejections` counter incremented

### Verification

```bash
# Unit tests (6 tests):
cargo test -p falcon_storage --lib -- epoch_fence

# Integration tests (3 tests):
cargo test -p falcon_cluster --test distributed_enhancements_integration test_p01
```

---

## 3. Replication & Commit Policies

### Commit Policies

| Policy | Acks Required | Latency | Durability |
|--------|--------------|---------|------------|
| `Local` | 0 (primary only) | Lowest | Single-node |
| `Quorum` | ⌊N/2⌋ + 1 | Medium | Majority |
| `All` | N (all replicas) | Highest | Full |

### Invariants Enforced

1. **Prefix Property**: `committed(replica) ⊆ committed(primary)`
   - A replica's committed LSN must never exceed the primary's committed LSN.
   - Violation indicates replication bug or clock skew.

2. **No Phantom Commits**: A replica must not make a transaction visible before the primary has durably committed it.
   - `replica_visible_lsn ≤ primary_durable_lsn`

3. **Visibility Binding**: Client visibility is bound to the commit policy.
   - `Local`: visible after primary WAL sync.
   - `Quorum`: visible after majority replica ACK.
   - `All`: visible after all replica ACKs.

### Verification

```bash
# Policy × crashpoint matrix (3 tests):
cargo test -p falcon_cluster --test distributed_enhancements_integration test_p02
```

---

## 4. Failover Procedure

### Formalized Stages

```
detect_failure → freeze_writes → seal_epoch → catch_up → promote → reopen → verify → complete
```

| Stage | What Happens | Duration Target |
|-------|-------------|-----------------|
| **detect_failure** | Failure detector triggers (heartbeat timeout) | < 3s |
| **freeze_writes** | Reject new writes on the shard | < 100ms |
| **seal_epoch** | Bump epoch, fence old primary | < 100ms |
| **catch_up** | Best replica applies remaining WAL entries | Depends on lag |
| **promote** | Swap replica role to primary | < 100ms |
| **reopen** | Accept writes under new epoch | < 100ms |
| **verify** | Check all invariants (prefix, phantom, promotion safety) | < 500ms |
| **complete** | Emit summary audit event | < 10ms |

### Audit Trail

Every failover emits structured audit events with:
- `seq`: Monotonic sequence number
- `stage`: Which failover stage
- `timestamp_ms`: Wall-clock time
- `elapsed_us`: Microseconds since failover start
- `description`: Human-readable message
- `success`: Boolean
- `data`: Key-value metadata (durations, errors)

### Verification Report

After failover, a `FailoverVerificationReport` is generated containing:
- Old/new epoch and primary node IDs
- Per-stage durations
- Invariant check results (prefix, phantom, promotion safety, visibility)
- Full audit trail
- Overall pass/fail with failure reason

```bash
# Gameday drill (2 tests):
cargo test -p falcon_cluster --test distributed_enhancements_integration test_p03
```

---

## 5. Cross-Shard Transactions (2PC)

### Architecture

```
Client → Coordinator → Prepare(shard₁, shard₂, ...) → Decision Log → Commit/Abort → Participants
```

### Phase Model

| Phase | Description | Crash Recovery |
|-------|-------------|---------------|
| `PrePrepare` | Before any prepare sent | Abort on recovery |
| `Preparing` | Prepare sent, waiting responses | Abort (no decision yet) |
| `DecisionPending` | All prepared, writing decision | Check decision log |
| `Applying` | Sending commit/abort to participants | Re-send (idempotent) |
| `Applied` | All participants acknowledged | No action needed |

### Key Components

- **`CoordinatorDecisionLog`**: Durable log written *before* sending commit/abort to participants. Survives coordinator crashes.
- **`ParticipantIdempotencyRegistry`**: Ensures each participant processes a decision exactly once, even under retry/recovery.
- **`TwoPcRecoveryCoordinator`**: On restart, reads unapplied decisions from the log and re-sends to participants.

### Participant Idempotency

```
check_and_record(txn_id, shard_id, decision) → bool
  false = first apply (proceed)
  true  = duplicate (skip — already applied)
```

### Verification

```bash
# Crash injection at each phase + idempotency (2 tests):
cargo test -p falcon_cluster --test distributed_enhancements_integration test_p04
```

---

## 6. Membership Lifecycle

### Node States

```
              ┌──────────┐
              │ Joining  │ ← join_node()
              └────┬─────┘
                   │ activate_node()
              ┌────▼─────┐
              │  Active  │ ← serving traffic
              └────┬─────┘
                   │ drain_node()
              ┌────▼─────┐
              │ Draining │ ← completing inflight, migrating shards
              └────┬─────┘
                   │ remove_node() (when drain complete)
              ┌────▼─────┐
              │ Removed  │
              └──────────┘
```

### Valid Transitions

| From | To | Trigger |
|------|----|---------|
| (new) | Joining | `join_node()` |
| Joining | Active | `activate_node()` (bootstrap complete) |
| Active | Draining | `drain_node()` (admin command) |
| Draining | Removed | `remove_node()` (drain complete) |
| Active | Removed | `remove_node()` (emergency) |
| Joining | Removed | `remove_node()` (bootstrap failed) |

### Drain Completion Criteria

A drain is complete when:
1. All shards migrated away (`shard_assignments` empty)
2. All inflight transactions completed (`inflight_txns == 0`)

### Operations

```bash
# Add a node:
# 1. Start the new node with cluster bootstrap config
# 2. Control plane registers it as Joining
# 3. Snapshot transfer begins
# 4. After bootstrap: activate_node() → Active

# Replace a node:
# 1. Add replacement node (Joining → Active)
# 2. Drain old node (Active → Draining)
# 3. Migrate shards from old to new
# 4. Remove old node (Draining → Removed)

# Emergency remove:
# 1. remove_node() transitions directly Active → Removed
# 2. Shards are re-assigned to remaining nodes
```

### Verification

```bash
# Lifecycle tests (2 tests):
cargo test -p falcon_cluster --test distributed_enhancements_integration test_p11
```

---

## 7. Shard Migration

### Protocol

```
Planning → Freeze → Snapshot → CatchUp → SwitchRouting → Verify → Complete
                                                                      │
                                                              (or RolledBack)
```

| Phase | Description | Writes Blocked? |
|-------|-------------|-----------------|
| Planning | Validate source/target, estimate size | No |
| Freeze | Brief write pause on source shard | **Yes** (bounded) |
| Snapshot | Transfer shard data to target | No (writes resume) |
| CatchUp | Apply WAL entries accumulated during snapshot | No |
| SwitchRouting | Atomically redirect traffic to target | Brief pause |
| Verify | Confirm target serving correctly | No |
| Complete | Clean up source data | No |

### Bounded Disruption

- **Max freeze duration**: Configurable (default 5s). If exceeded, migration auto-rolls back.
- **Max concurrent migrations**: Configurable (default 3). Prevents resource exhaustion.
- **Progress tracking**: Bytes transferred and WAL entries applied reported per-migration.

### Verification

```bash
# Migration lifecycle + invariants (2 tests):
cargo test -p falcon_cluster --test distributed_enhancements_integration test_p12
```

---

## 8. Cluster Status & Observability

### Unified Cluster Status View

A single `ClusterStatusView` provides all distributed signals:

| Signal | Source | Meaning |
|--------|--------|---------|
| `shard_epochs` | `StorageEpochFence` / `SplitBrainDetector` | Current leader epoch per shard |
| `replication_lag_ms` | Replication transport | Lag between primary and replica per shard |
| `twopc_inflight` | `HardenedTwoPhaseCoordinator` | Active cross-shard transactions |
| `indoubt_count` | `InDoubtResolver` | Transactions needing recovery |
| `recent_failover_events` | `FailoverRunbook` | Last N failover audit events |
| `routing_version` | `ShardMap` | Current routing table version |
| `membership_summary` | `MembershipLifecycle` | Node count by state |
| `active_migrations` | `ShardMigrationCoordinator` | In-progress shard migrations |
| `split_brain_events` | `SplitBrainDetector` | Total split-brain detections |
| `epoch_fence_rejections` | `StorageEpochFence` | WAL-level stale-epoch rejections |
| `idempotency_duplicates` | `ParticipantIdempotencyRegistry` | 2PC duplicate apply count |

### Health Derivation

| Status | Condition |
|--------|-----------|
| **Healthy** | No critical signals, lag < 1s, no in-doubt txns |
| **Degraded** | In-doubt txns > 0, active migrations, or lag > 1s |
| **Critical** | Split-brain events > 0 or epoch fence rejections > 0 |

### Verification

```bash
# Status view tests (2 tests):
cargo test -p falcon_cluster --test distributed_enhancements_integration test_p13
```

---

## 9. Troubleshooting

### Split-Brain Detection

**Symptom**: Logs show `SPLIT-BRAIN DETECTED: stale-epoch write rejected` or `EPOCH FENCE: stale-epoch WAL write rejected`.

**Cause**: A node with a stale epoch attempted to write. This is expected after failover if the old leader was partitioned.

**Action**:
1. Verify the old leader has been fenced (its epoch is older than current).
2. Confirm the new leader is accepting writes at the new epoch.
3. If the old leader is still running, stop it or wait for it to detect the new epoch.

### Replication Invariant Violation

**Symptom**: Logs show `PREFIX VIOLATION` or `PHANTOM COMMIT`.

**Action**:
1. **PREFIX VIOLATION**: Replica has commits not on primary. Stop the replica, investigate WAL divergence.
2. **PHANTOM COMMIT**: Replica exposed uncommitted data. Check commit policy configuration.
3. Run invariant verification: `cargo test -p falcon_cluster --test distributed_enhancements_integration test_p02`

### 2PC In-Doubt Transactions

**Symptom**: `indoubt_count > 0` in cluster status.

**Action**:
1. Check coordinator decision log for the transaction's decision.
2. If decision exists: the in-doubt resolver will automatically re-send it.
3. If no decision: transaction will be aborted after timeout.
4. Monitor `recovery_attempts` and `recovery_failures` metrics.

### Migration Stuck

**Symptom**: Migration phase doesn't advance.

**Action**:
1. Check `active_migrations` in cluster status for the shard.
2. If in `Freeze` phase too long: migration will auto-rollback (max freeze duration).
3. If in `CatchUp` phase: check replication lag on the target node.
4. Manual rollback: `rollback_migration(shard_id, reason)`.

### Node Drain Not Completing

**Symptom**: Node stuck in `Draining` state.

**Action**:
1. Check `is_drain_complete()` — requires: inflight txns = 0, shard assignments = empty.
2. If inflight txns > 0: wait for transactions to complete or timeout.
3. If shards remain: ensure migrations are running for all assigned shards.

---

## Quick Reference: Test Commands

```bash
# All distributed enhancement unit tests (27 tests):
cargo test -p falcon_cluster --lib -- distributed_enhancements

# All epoch fence tests (6 tests):
cargo test -p falcon_storage --lib -- epoch_fence

# All integration tests (17 tests):
cargo test -p falcon_cluster --test distributed_enhancements_integration

# Full gameday drill (all distributed):
cargo test -p falcon_cluster --test distributed_enhancements_integration -- --nocapture

# E2E lifecycle test:
cargo test -p falcon_cluster --test distributed_enhancements_integration test_e2e -- --nocapture
```
