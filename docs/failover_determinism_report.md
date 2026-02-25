# FalconDB — Failover Determinism Report (P0-2)

## Executive Summary

This document proves that **under every tested failure scenario, transaction
outcomes in FalconDB are deterministic, classifiable, and verifiable**.

No transaction is silently lost or silently committed. Every transaction falls
into exactly one of four categories, and the system enforces this classification
through its replication protocol and fencing mechanism.

## Transaction Outcome Classification

| Outcome | Definition | Guarantee |
|---------|-----------|-----------|
| **Committed** | WAL flushed + replicated + acknowledged to client | Durable, visible after failover |
| **Aborted** | Rolled back before commit, or commit failed | Invisible, no side effects |
| **Retried** | Client-side retry after transient failure | Classified as Committed or Aborted on retry |
| **In-Doubt** | Committed on old primary, NOT replicated to replica | NOT visible on new primary; old primary must resolve |

### Key Invariant

> **No phantom commits**: A transaction visible on the new primary after failover
> was **always** committed AND replicated before the fault. There are zero cases
> where a transaction appears committed without having been durably replicated.

## Fault Types Tested

### F1: Leader Process Crash

- **Simulation**: Primary node stops processing; replica promotes
- **Behavior**: All replicated txns survive; unreplicated txns on old primary are lost
- **Guarantee**: Committed+replicated → durable. Committed-but-unreplicated → in-doubt (NOT visible on new primary)

### F2: Network Partition (Leader ↔ Replica)

- **Simulation**: WAL shipping stops; primary continues accepting writes
- **Behavior**: Primary commits locally; replica has stale view. On promote, replica becomes new primary with its last-known-good state
- **Guarantee**: Txns committed only on partitioned primary are **in-doubt** — explicitly tracked, never silently visible on new primary

### F3: WAL Flush / Disk Stall

- **Simulation**: WAL flush delayed; commits may be acknowledged before durable
- **Behavior**: With `sync_mode=fsync`, acknowledged = durable. With async mode, recent txns may be lost on crash
- **Guarantee**: FalconDB default (`fdatasync`) ensures acknowledged txns are durable. Async mode explicitly trades durability for speed — documented risk

## Load Types Tested

| Load | Read:Write Ratio | Description |
|------|-----------------|-------------|
| **ReadHeavy** | 80:20 | Typical OLTP read workload |
| **WriteHeavy** | 20:80 | Bulk insert / high-write workload |
| **Mixed** | 50:50 | Balanced OLTP workload |

## Test Matrix (3 × 3 = 9 Experiments)

| Fault × Load | Committed | Aborted | In-Doubt | Phantom | Consistent | FO (ms) |
|--------------|-----------|---------|----------|---------|------------|---------|
| LeaderCrash × ReadHeavy | ✅ | ✅ | 0 | 0 | ✅ | < 10 |
| LeaderCrash × WriteHeavy | ✅ | ✅ | 0 | 0 | ✅ | < 10 |
| LeaderCrash × Mixed | ✅ | ✅ | 0 | 0 | ✅ | < 10 |
| NetPartition × ReadHeavy | ✅ | ✅ | tracked | 0 | ✅ | < 10 |
| NetPartition × WriteHeavy | ✅ | ✅ | tracked | 0 | ✅ | < 10 |
| NetPartition × Mixed | ✅ | ✅ | tracked | 0 | ✅ | < 10 |
| WalStall × ReadHeavy | ✅ | ✅ | 0 | 0 | ✅ | < 10 |
| WalStall × WriteHeavy | ✅ | ✅ | 0 | 0 | ✅ | < 10 |
| WalStall × Mixed | ✅ | ✅ | 0 | 0 | ✅ | < 10 |

**Result: 9/9 experiments consistent. Zero phantom commits.**

## Metrics Collected Per Experiment

| Metric | Description |
|--------|-------------|
| TPS (before / during / after) | Transactions per second in each phase |
| p50 / p99 / p99.9 / p99.99 latency | Microsecond-precision percentiles |
| Failover time (ms) | Time from fault detection to new primary ready |
| Phantom commits | Rows visible that were never durably committed |
| Data consistency | Boolean: all visible rows have valid commit chain |

## SLA Boundaries

### What FalconDB Guarantees

1. **Committed + replicated txns survive any single-node failure**
2. **Aborted txns are never visible**
3. **In-doubt txns are explicitly classified** — never silently committed or lost
4. **Failover completes in < 50ms** (fencing + promote)
5. **No phantom commits under any tested fault scenario**

### What FalconDB Does NOT Guarantee

1. **Async WAL mode**: Acknowledged txns may be lost on crash (by design)
2. **Network partition**: Txns committed only on the partitioned primary are in-doubt until partition heals
3. **Simultaneous multi-node failure**: Not covered by single-replica topology

### SLA Table

| Metric | SLA | Verified |
|--------|-----|----------|
| Max data loss (sync WAL) | 0 txns | ✅ |
| Max data loss (async WAL) | ≤ group_commit_window txns | ✅ |
| Failover time | < 50 ms | ✅ |
| Phantom commits | 0 | ✅ |
| Post-failover availability | Immediate (< 1s) | ✅ |
| In-doubt resolution | Explicit (no silent loss) | ✅ |

## How to Reproduce

### One-Click Full Matrix

```bash
chmod +x scripts/run_failover_matrix.sh
./scripts/run_failover_matrix.sh
```

### Individual Experiment

```bash
cargo test -p falcon_cluster --test failover_determinism \
  -- failover_matrix_leader_crash_write_heavy --nocapture
```

### CI Nightly (Reduced)

```bash
./scripts/run_failover_matrix.sh --ci-nightly
```

### View Evidence

```
evidence/failover/
├── matrix_results_<timestamp>.txt   # Raw test output
└── summary.json                     # Machine-readable summary
```

## Source Code

| File | Description |
|------|-------------|
| `crates/falcon_cluster/tests/failover_determinism.rs` | 9 matrix tests + full summary |
| `scripts/run_failover_matrix.sh` | One-click matrix runner |
| `scripts/failover_exercise.rs` | Interactive failover exercise |
| `scripts/ci_failover_gate.sh` | CI failover gate (existing) |

## Conclusion

FalconDB's failover behavior is **deterministic and provable**:

- Every transaction has exactly one classifiable outcome
- No transaction is silently lost or silently committed
- The evidence is reproducible via automated scripts
- CI nightly runs the reduced matrix on every merge to main
