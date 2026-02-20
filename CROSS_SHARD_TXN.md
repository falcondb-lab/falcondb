# FalconDB — Cross-Shard Transaction Specification

> **Status**: Normative. Every invariant has a corresponding test.
> **Module**: `falcon_cluster::cross_shard`, `falcon_cluster::two_phase`
> **Test suites**: `cross_shard_hardened.rs`, `cross_shard_bench.rs`

---

## 1. Transaction Model & Protocol

### 1.1 Primary Path: Hybrid Fast/Slow

| Path | Scope | Protocol | Coordinator |
|------|-------|----------|-------------|
| **Fast path** | Single-shard | Local OCC + commit | None (direct) |
| **Slow path** | Multi-shard | Two-Phase Commit (2PC) | `HardenedTwoPhaseCoordinator` |

Classification is determined at planning time by `TxnClassification`:
- `Local(shard_id)` → fast path
- `Global(shard_ids, SlowPathMode::Xa2Pc)` → slow path via 2PC

### 1.2 2PC Protocol Phases

```
Client → Coordinator → [Phase 1: Prepare] → [Commit Barrier] → [Phase 2: Commit/Abort] → Client
```

| Phase | What happens | Timeout | On failure |
|-------|-------------|---------|------------|
| **Admission** | Coordinator concurrency limiter check | `coord_queue` | Reject (Transient) |
| **Prepare** | Begin txn on each shard, execute writes | `prepare` per shard | Abort all |
| **Commit Barrier** | Coordinator decides commit/abort | Instant | N/A |
| **Commit** | Commit on all prepared shards | `commit` per shard | Log + retry |
| **Abort** | Abort all prepared shards | Best-effort | Log |

### 1.3 Visibility & Failure Semantics

| Guarantee | Value |
|-----------|-------|
| Commit semantics | **At-most-once** per txn_id |
| Coordinator crash before CoordinatorCommit WAL | All participants abort |
| Coordinator crash after CoordinatorCommit WAL | Recovery completes commit |
| Shard crash during prepare | Abort all (prepare failed) |
| Network partition during commit | Indeterminate — client must check status |
| Idempotent retry | Via unique `txn_id` (new txn_id per retry) |
| Epoch fencing | Stale-epoch writes rejected |

### 1.4 Client Error Codes

| SQLSTATE | Meaning | Client Action |
|----------|---------|---------------|
| `00000` | Committed | None |
| `40001` | Serialization failure (conflict) | Retry with backoff |
| `40000` | Transaction aborted | Retry or abort |
| `53300` | WAL backlog exceeded | Retry later |
| `53301` | Replication lag exceeded | Retry later |
| `08006` | Connection failure (indeterminate) | Check txn status |
| `58000` | Consensus error | Check cluster health |

---

## 2. Critical Path Latency Breakdown

Every cross-shard transaction produces a `CrossShardLatencyBreakdown` with per-phase microsecond timing:

| Field | Description | Prometheus metric |
|-------|-------------|-------------------|
| `coord_queue_us` | Coordinator admission queue wait | `falcon_xs_coord_queue_us` |
| `route_plan_us` | Shard routing / classification | `falcon_xs_route_plan_us` |
| `shard_rpc_us[]` | Per-shard RPC round-trip | `falcon_xs_shard_rpc_us` |
| `read_phase_us` | Read phase (across all shards) | `falcon_xs_read_phase_us` |
| `lock_validate_us` | Lock/OCC validation | `falcon_xs_lock_validate_us` |
| `prepare_wal_us` | Prepare phase WAL writes | `falcon_xs_prepare_wal_us` |
| `commit_barrier_us` | Commit/abort decision | `falcon_xs_commit_barrier_us` |
| `commit_wal_us` | Commit phase WAL writes | `falcon_xs_commit_wal_us` |
| `replication_ack_us` | Replication ACK wait | `falcon_xs_replication_ack_us` |
| `gc_impact_us` | GC/vacuum interference | `falcon_xs_gc_impact_us` |
| `retry_backoff_us` | Cumulative retry backoff | `falcon_xs_retry_backoff_us` |
| `total_us` | End-to-end latency | `falcon_xs_total_us` |

### Dominant Phase Attribution

`CrossShardLatencyBreakdown::dominant_phase()` returns the phase that contributed most to latency. This is the **primary tool for tail latency root-cause analysis**.

### Waterfall Format

```
queue=100|route=50|rpc_max=500|read=0|lock=0|prep=800|barrier=0|commit=200|repl=0|retry=0|total=1650
```

---

## 3. Layered Timeout Framework

Each phase has an independent, configurable timeout. No single global timeout.

| Phase | Default | Config key |
|-------|---------|------------|
| Coordinator queue | 500ms | `timeouts.coord_queue` |
| Shard RPC | 2s | `timeouts.shard_rpc` |
| Prepare (per shard) | 5s | `timeouts.prepare` |
| Commit (per shard) | 5s | `timeouts.commit` |
| Lock/validate | 1s | `timeouts.lock_validate` |
| Total (hard ceiling) | 30s | `timeouts.total` |
| Retry ceiling | 10s | `timeouts.retry_ceiling` |

### Timeout Policy

| Policy | Behavior |
|--------|----------|
| `FailFast` (default) | Abort immediately on any phase timeout |
| `BestEffort` | Allow phase overshooting if total timeout holds |

Every timeout produces an explainable `TimeoutPhase` enum value identifying exactly which phase timed out.

---

## 4. Admission Control & Backpressure

### 4.1 Coordinator Concurrency Limiter

Semaphore-based with RAII guard:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `hard_limit` | 128 | Reject above this (immediate) |
| `soft_limit` | 96 | Start degrading/queueing above this |

Metrics: `in_flight`, `total_admitted`, `total_rejected`, `total_queued`

### 4.2 Queue Policy

| Policy | Description |
|--------|-------------|
| `Fifo` (default) | First-in, first-out |
| `ShortTxnPriority` | Fewer-shard transactions get priority |

### 4.3 Queue Overflow Action

| Action | Description |
|--------|-------------|
| `Reject` (default) | New requests rejected |
| `ShedLoad` | Drop lowest-priority requests |
| `Degrade` | Allow with reduced guarantees |

---

## 5. Retry Framework

### 5.1 Error Classification

Every error is classified into exactly one of:

| Class | Retryable | Examples |
|-------|:-:|---------|
| `Transient` | Yes (immediate) | Memory pressure, WAL backlog, replication lag |
| `Conflict` | Yes (with backoff) | Write-write conflict, OCC validation failure |
| `Timeout` | **No** (indeterminate) | Coordinator timeout, commit phase timeout |
| `Permanent` | **No** | Constraint violation, duplicate key, schema error |

### 5.2 Exponential Backoff + Jitter

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_retries` | 3 | Maximum retry attempts |
| `initial_backoff` | 10ms | First retry delay |
| `max_backoff` | 2s | Backoff cap |
| `multiplier` | 2.0 | Exponential factor |
| `jitter_ratio` | 0.25 | Random ±25% of base |
| `retry_budget` | 10s | Maximum cumulative retry time |

Formula: `backoff(n) = min(initial * multiplier^n, max_backoff) + uniform(0, jitter_ratio * base)`

### 5.3 Anti-Thundering-Herd: Circuit Breaker

| Parameter | Default | Description |
|-----------|---------|-------------|
| `failure_threshold` | 5 | Consecutive failures to trip |
| `recovery_timeout` | 10s | Time before half-open probe |

States: `Closed` → `Open` (after threshold) → `HalfOpen` (after recovery timeout) → `Closed` (on probe success)

All requests are **immediately rejected** when the circuit breaker is Open.

---

## 6. Conflict & Hotspot Management

### 6.1 Per-Shard Conflict Tracker

Tracks per-shard:
- `total_txns`, `conflict_aborts`, `timeout_aborts`
- `conflict_rate` = conflict_aborts / total_txns
- `abort_rate` = (conflict_aborts + timeout_aborts) / total_txns
- `avg_lock_wait_us`

### 6.2 Adaptive Concurrency Control

Adjusts per-shard concurrency limits based on observed conflict rates:

| Condition | Action |
|-----------|--------|
| `conflict_rate > 15%` | Reduce concurrency by `step_size` (default 4) |
| `conflict_rate < 5%` | Increase concurrency by `step_size` |
| Between 5%–15% | No change |

Clamped between `min_concurrency` (4) and `max_concurrency` (256).

---

## 7. Deadlock Detection

### 7.1 Timeout-Based Deadlock Breaker

Any cross-shard transaction waiting longer than `deadlock_timeout` (default 10s) is classified as potentially deadlocked and aborted with an **explainable abort reason**.

### 7.2 Stalled Transaction Diagnosis

Each stalled transaction report includes:
- `txn_id`
- `involved_shards`
- `wait_reason` (one of: `CoordinatorQueue`, `ShardLockWait(shard)`, `Validate`, `WalWrite`, `ReplicationAck`, `CommitBarrier`)
- `wait_duration_us`

---

## 8. Bench Suite & Acceptance Criteria

### 8.1 Standard Scenarios

| Scenario | Description | Key distribution | Shard pattern |
|----------|-------------|-----------------|---------------|
| `uniform_low_contention` | Low conflict baseline | Uniform | Round-robin pairs |
| `zipfian_hotspot` | Hotspot stress | Zipfian (power-law) | Fixed shard pair |
| `mixed_80_20` | Realistic mix | Sequential | 80% single / 20% cross |
| `pure_cross_shard` | Worst-case | Sequential | All 4 shards |
| `fault_injection` | DuplicatePK collisions | Mixed | 2 shards |
| `high_concurrency` | Multi-threaded stress | Partitioned | Thread-local pairs |

### 8.2 Output Metrics (per scenario)

| Metric | Description |
|--------|-------------|
| `total_txns` | Total transactions attempted |
| `committed` / `aborted` | Outcome counts |
| `retried` | Transactions that required retry |
| `throughput_tps` | Committed transactions per second |
| `p50_us` / `p95_us` / `p99_us` / `p999_us` | Latency percentiles |
| `dominant_phases` | Per-txn dominant latency phase |
| `duration_ms` | Total wall-clock time |

### 8.3 Acceptance Criteria

| Criterion | Threshold |
|-----------|-----------|
| Uniform commit rate | ≥ 90% |
| Mixed commit rate | ≥ 85% |
| Pure cross-shard commit rate | ≥ 80% |
| High concurrency commit rate | ≥ 80% |
| P999 divergence | No sustained exponential growth |
| Fault injection stability | System stays responsive, majority commits |
| Circuit breaker | Trips after threshold, recovers after timeout |
| Retry storm | Budget-capped, never unbounded |

---

## 9. Observability & Prometheus Metrics

### 9.1 Coordinator Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `falcon_xs_total_attempted` | Counter | — |
| `falcon_xs_total_committed` | Counter | — |
| `falcon_xs_total_aborted` | Counter | `reason` |
| `falcon_xs_total_retried` | Counter | — |
| `falcon_xs_coord_in_flight` | Gauge | — |
| `falcon_xs_coord_rejected` | Counter | — |
| `falcon_xs_circuit_breaker_state` | Gauge | `state` |
| `falcon_xs_circuit_breaker_trips` | Counter | — |

### 9.2 Per-Phase Latency Histograms

| Metric | Type |
|--------|------|
| `falcon_xs_total_us` | Histogram |
| `falcon_xs_coord_queue_us` | Histogram |
| `falcon_xs_prepare_wal_us` | Histogram |
| `falcon_xs_commit_wal_us` | Histogram |
| `falcon_xs_shard_rpc_us` | Histogram (label: `shard_id`) |
| `falcon_xs_retry_backoff_us` | Histogram |

### 9.3 Per-Shard Conflict Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `falcon_xs_shard_conflict_rate` | Gauge | `shard_id` |
| `falcon_xs_shard_abort_rate` | Gauge | `shard_id` |
| `falcon_xs_shard_lock_wait_us` | Histogram | `shard_id` |

### 9.4 Deadlock Metrics

| Metric | Type |
|--------|------|
| `falcon_xs_deadlocks_detected` | Counter |
| `falcon_xs_stalled_txns` | Gauge |

---

## Appendix: Test Coverage Matrix

| ID | Invariant | Test File | Test Function |
|----|-----------|-----------|---------------|
| H2PC-1 | Commit with full latency breakdown | `cross_shard_hardened.rs` | `test_h2pc1_commit_with_latency_breakdown` |
| H2PC-2 | Abort with per-phase latency + reason | `cross_shard_hardened.rs` | `test_h2pc2_abort_with_latency_and_reason` |
| H2PC-3 | Admission control rejects when saturated | `cross_shard_hardened.rs` | `test_h2pc3_admission_control_rejects` |
| H2PC-4 | Circuit breaker trips after failures | `cross_shard_hardened.rs` | `test_h2pc4_circuit_breaker_trips` |
| H2PC-5 | Retry succeeds on transient | `cross_shard_hardened.rs` | `test_h2pc5_retry_succeeds` |
| H2PC-6 | Permanent errors not retried | `cross_shard_hardened.rs` | `test_h2pc6_permanent_not_retried` |
| H2PC-7 | Conflict tracker records rates | `cross_shard_hardened.rs` | `test_h2pc7_conflict_tracker` |
| H2PC-8 | Deadlock detector detects stalls | `cross_shard_hardened.rs` | `test_h2pc8_deadlock_detector` |
| H2PC-9 | Adaptive concurrency adjusts | `cross_shard_hardened.rs` | `test_h2pc9_adaptive_concurrency` |
| H2PC-10 | Waterfall is explainable | `cross_shard_hardened.rs` | `test_h2pc10_latency_waterfall_explainable` |
| H2PC-11 | Observability stats snapshot | `cross_shard_hardened.rs` | `test_h2pc11_observability_stats` |
| BENCH-1 | Uniform low contention | `cross_shard_bench.rs` | `bench_uniform_low_contention` |
| BENCH-2 | Zipfian hotspot | `cross_shard_bench.rs` | `bench_zipfian_hotspot` |
| BENCH-3 | Mixed 80/20 | `cross_shard_bench.rs` | `bench_mixed_workload` |
| BENCH-4 | Pure cross-shard | `cross_shard_bench.rs` | `bench_pure_cross_shard` |
| BENCH-5 | Fault injection | `cross_shard_bench.rs` | `bench_fault_injection` |
| BENCH-6 | High concurrency | `cross_shard_bench.rs` | `bench_high_concurrency` |
| XS-* | Cross-shard atomicity/at-most-once | `consistency_replication.rs` | `test_xs1_*`, `test_xs2_*` |
| Unit | Latency breakdown, timeouts, limiter, retry, circuit breaker, conflict tracker, deadlock, adaptive | `cross_shard.rs` | 27 unit tests |
