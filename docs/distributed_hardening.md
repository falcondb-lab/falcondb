# Distributed & Failover Hardening — From "能用" to "无脑稳"

> **Status**: Production-hardened  
> **Last updated**: 2026-02-25  
> **Test evidence**: 26 unit tests (`dist_hardening::tests`) + 20 integration tests (`dist_hardening_integration.rs`)

---

## Overview

FalconDB's distributed layer was functional ("能用") but lacked the edge-case
guards required for "rock-solid stable" ("无脑稳") operation. This hardening
pass closes five concrete gaps:

| # | Gap | Component | Effect |
|---|-----|-----------|--------|
| G1 | Unsafe promotions (lagging replica, no quorum) | `FailoverPreFlight` | Blocks data-loss-risk promotions |
| G2 | Split-brain after partition heal | `SplitBrainDetector` | Rejects stale-epoch writes |
| G3 | Permanent bg task death | `AutoRestartSupervisor` | Bounded auto-restart with backoff |
| G4 | False-positive failure detection | `HealthCheckHysteresis` | Debounced health transitions |
| G5 | Partial promotion → total outage | `PromotionSafetyGuard` | Atomic-or-rollback promotion |

All five components are **wired into `HAReplicaGroup`** and exercised by
automated tests.

---

## G1: Failover Pre-Flight Check

### Problem

The original `promote_best()` only checked cooldown. A promotion could proceed
even when the candidate replica was far behind the primary (data loss) or when
there weren't enough healthy replicas post-promotion (reduced durability).

### Solution: `FailoverPreFlight`

Before any promotion, a 5-point checklist must pass:

| Check | Rejects When |
|-------|-------------|
| Primary actually failed | Primary is still healthy (false alarm) |
| Candidate exists | No replica available for promotion |
| Replica lag ≤ max | Candidate too far behind primary LSN |
| Post-promotion quorum | Not enough replicas remaining |
| Cooldown elapsed | Too soon after last failover |

### Configuration

```rust
PreFlightConfig {
    max_promotion_lag: 1000,        // max LSN gap
    min_post_promotion_replicas: 0, // min replicas after promotion
    cooldown: Duration::from_secs(30),
}
```

### Test Evidence

| Test | Property |
|------|----------|
| DH-01 | Hardened promotion succeeds with caught-up replica |
| DH-02 | Rejects when primary is healthy |
| DH-03 | Respects cooldown between failovers |
| DH-18 | Rejects lagging replica (lag 50 > max 10) |
| DH-19 | Rejects insufficient quorum |

---

## G2: Split-Brain Detection

### Problem

After a network partition heals, the old primary (fenced) might attempt to
re-assert itself. If epoch checking is weak, both nodes could serve writes,
causing data divergence.

### Solution: `SplitBrainDetector`

Every shard tracks an **authoritative epoch** (monotonically increasing).
Any write arriving with `writer_epoch < current_epoch` is rejected as a
split-brain symptom.

Wired into `HAReplicaGroup::check_write_epoch()` and automatically updated
after each `hardened_promote_best()`.

### Guarantees

- **Epoch monotonicity**: epoch strictly increases across promotions.
- **Stale writes rejected**: writes from old epoch return error immediately.
- **Event logging**: every split-brain detection is logged at ERROR level
  with writer node, writer epoch, and current epoch.
- **Bounded history**: up to 100 events retained for diagnostics.

### Test Evidence

| Test | Property |
|------|----------|
| DH-04 | Stale-epoch writes rejected after promotion |
| DH-05 | Detector tracks events and metrics |
| DH-17 | Multiple failovers maintain epoch monotonicity |

---

## G3: Auto-Restart Supervisor

### Problem

`BgTaskSupervisor` could detect failed critical tasks and mark the node as
`Degraded`, but had **no mechanism to restart them**. A single OOM or I/O
error in a background task (WAL writer, GC, replication) was permanent.

### Solution: `AutoRestartSupervisor`

Wraps task health monitoring with:

- **Bounded restart attempts** (default: 5)
- **Exponential backoff** (1s → 2s → 4s → ... → 60s cap)
- **Permanent failure** after exhausting restarts (requires manual intervention)
- **Success reset**: if a task runs stably for `success_reset_duration`
  (default: 5 minutes), its restart counter resets to 0.

### Configuration

```rust
AutoRestartConfig {
    max_restarts: 5,
    initial_backoff: Duration::from_secs(1),
    max_backoff: Duration::from_secs(60),
    backoff_multiplier: 2.0,
    success_reset_duration: Duration::from_secs(300),
}
```

### Test Evidence

| Test | Property |
|------|----------|
| DH-06 | Exponential backoff sequence (100→200→400→800→1600ms) |
| DH-07 | Counter resets after stable run |
| DH-08 | Multiple tasks fail independently |

---

## G4: Health-Check Hysteresis

### Problem

The `FailureDetector` transitions directly from Healthy to Suspect on a
single missed heartbeat, and Suspect to Failed after a time threshold.
A single network hiccup could trigger a false failover.

### Solution: `HealthCheckHysteresis`

Requires **N consecutive** missed heartbeats before transitioning:

| Transition | Default Threshold |
|-----------|-------------------|
| Healthy → Suspect | 2 consecutive misses |
| Suspect → Failed | 5 consecutive misses |
| Failed → Healthy | 3 consecutive successes |

A single success resets the miss counter. A single miss resets the
success counter. This prevents both false alarms and premature recovery.

### Metrics

- `total_evaluations`: how many heartbeat results processed
- `total_transitions`: how many state changes occurred
- `false_alarms_prevented`: single misses that didn't trigger a transition

### Test Evidence

| Test | Property |
|------|----------|
| DH-09 | Single miss stays Healthy (false alarm prevented) |
| DH-10 | Transitions through Suspect → Failed correctly |
| DH-11 | Recovery needs consecutive successes; miss resets |
| DH-12 | Multi-node tracking is independent |

---

## G5: Promotion Safety Guard

### Problem

The original `promote()` protocol (fence → catch-up → swap → unfence) had
no rollback. If catch-up or swap failed partway, the old primary remained
fenced (read-only) and the new primary never unfenced → **total write outage**.

### Solution: `PromotionSafetyGuard`

Tracks every step of the promotion protocol. If any step fails, the caller
rolls back:

```
Idle → OldPrimaryFenced → CandidateCaughtUp → RolesSwapped → NewPrimaryUnfenced → Complete
          ↓ (failure)          ↓ (failure)
      ROLLBACK: unfence    ROLLBACK: unfence
      old primary          old primary
```

The `hardened_promote_best()` method uses this guard:
- If catch-up fails → unfence old primary, rollback.
- If candidate LSN < target → unfence old primary, rollback.
- Only after all steps succeed → mark complete.

### Test Evidence

| Test | Property |
|------|----------|
| DH-13 | Full lifecycle tracking (Idle → Complete) |
| DH-14 | Rollback preserves old primary on catch-up failure |
| DH-15 | Retry succeeds after rollback |

---

## Forbidden States

| # | Forbidden State | Prevention | Test Evidence |
|---|----------------|------------|---------------|
| FS-1 | Promote a lagging replica (data loss) | `FailoverPreFlight` lag check | DH-18 |
| FS-2 | Two nodes accept writes at same epoch | `SplitBrainDetector` | DH-04 |
| FS-3 | Critical bg task dies permanently without restart | `AutoRestartSupervisor` | DH-06, DH-07 |
| FS-4 | False failover from single missed heartbeat | `HealthCheckHysteresis` | DH-09 |
| FS-5 | Partial promotion → total write outage | `PromotionSafetyGuard` rollback | DH-14 |
| FS-6 | Epoch goes backwards | `SplitBrainDetector::advance_epoch` uses `fetch_max` | DH-17 |
| FS-7 | Rapid failover oscillation | `FailoverPreFlight` cooldown | DH-03 |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  HAReplicaGroup (per shard)                                 │
│                                                             │
│  ┌──────────────────┐  ┌───────────────────────────────┐    │
│  │ FailureDetector  │  │ HealthCheckHysteresis         │    │
│  │ (heartbeat-based)│  │ (debounce: suspect=2, fail=5) │    │
│  └────────┬─────────┘  └───────────────┬───────────────┘    │
│           │                            │                    │
│           ▼                            ▼                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ FailoverPreFlight                                    │   │
│  │ ✓ Primary actually failed?                           │   │
│  │ ✓ Candidate exists?                                  │   │
│  │ ✓ Replica lag ≤ max?                                 │   │
│  │ ✓ Post-promotion quorum?                             │   │
│  │ ✓ Cooldown elapsed?                                  │   │
│  └────────────────────────┬─────────────────────────────┘   │
│                           │ PASS                            │
│                           ▼                                 │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ PromotionSafetyGuard                                 │   │
│  │ 1. Fence old primary                                 │   │
│  │ 2. Catch up candidate (rollback on fail)             │   │
│  │ 3. Verify LSN (rollback if lag)                      │   │
│  │ 4. Swap roles                                        │   │
│  │ 5. Unfence new primary                               │   │
│  │ 6. Complete                                          │   │
│  └────────────────────────┬─────────────────────────────┘   │
│                           │ SUCCESS                         │
│                           ▼                                 │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ SplitBrainDetector                                   │   │
│  │ advance_epoch(new_epoch)                             │   │
│  │ → reject all writes with epoch < new_epoch           │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ AutoRestartSupervisor (node-wide)                    │   │
│  │ • Bounded restart (max 5, exp backoff 1→60s)         │   │
│  │ • Success reset after 5min stable run                │   │
│  │ • Permanent failure → manual intervention            │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## `hardened_promote_best()` vs `promote_best()`

| Aspect | `promote_best()` | `hardened_promote_best()` |
|--------|-------------------|---------------------------|
| Pre-flight checks | Cooldown only | 5-point checklist |
| Evaluate detector | No | Yes (`evaluate()` called) |
| Split-brain guard | No | Epoch advanced + detector updated |
| Rollback on failure | No (total outage) | Yes (unfence old primary) |
| LSN verification | No | Candidate LSN ≥ primary LSN |
| Metrics | Basic | Full lifecycle tracking |

**Recommendation**: Use `hardened_promote_best()` in production. The original
`promote_best()` is retained for backward compatibility only.

---

## Related Documentation

- [Memory Pressure & Disk Spill](memory_pressure_spill.md)
- [Failover Behavior](failover_behavior.md)
- [Failover Partition SLA](failover_partition_sla.md)
