# Stability Report — FalconDB v1.0.4

> **Purpose**: Evidence that v1.0.4 determinism and failure safety goals are met.
> Run `scripts/ci_v104_determinism_gate.sh` to reproduce all gates.

---

## 1. Resource Exhaustion Determinism

| Gate | Pass Criteria | Status |
|------|--------------|--------|
| All 9 resource contracts valid | `validate_contracts()` returns Ok | ☐ |
| Every contract has 5-char SQLSTATE | SQLSTATE.len() == 5 for all | ☐ |
| Every contract is SHOW-visible | `show_visible == true` for all | ☐ |
| No duplicate resource names | Unique resource_name set | ☐ |
| Rejection latency ≤ 100µs | `max_reject_latency_us` enforced | ☐ |

**Soak test**: 10,000 concurrent queries with admission limit of 100.
Expected: 9,900 rejections, 0 implicit blocks, 0 panics.

## 2. Transaction Terminal State Coverage

| Gate | Pass Criteria | Status |
|------|--------------|--------|
| Every FalconError maps to a TxnTerminalState | `classify_error()` total coverage | ☐ |
| Every terminal state has valid SQLSTATE | 5-char code for all variants | ☐ |
| Committed → NoRetry | `RetryPolicy::NoRetry` | ☐ |
| AbortedRetryable → should_retry() == true | Verified | ☐ |
| AbortedNonRetryable → should_retry() == false | Verified | ☐ |
| Rejected → should_retry() == true | Verified | ☐ |
| Indeterminate → ExponentialBackoff | Verified | ☐ |

## 3. Failover × Commit Invariants

| Gate | Pass Criteria | Status |
|------|--------------|--------|
| FC-1: crash before CP-D → rolled back | `validate_failover_invariants()` | ☐ |
| FC-2: crash after CP-D → survives | `validate_failover_invariants()` | ☐ |
| FC-3: no CP-V without CP-D | `validate_commit_timeline()` | ☐ |
| FC-4: replay is idempotent | `IdempotentReplayValidator` | ☐ |
| Policy-dependent phase handled | WalLogged + no-fsync → either outcome OK | ☐ |

## 4. Queue Depth Bounds

| Gate | Pass Criteria | Status |
|------|--------------|--------|
| QueueDepthGuard rejects at capacity | `try_enqueue()` fails at hard_capacity | ☐ |
| No silent growth beyond capacity | `depth() <= hard_capacity` always | ☐ |
| Peak tracking correct | `peak()` tracks historical max | ☐ |
| RAII dequeue on drop | `QueueSlot` drop decrements depth | ☐ |

## 5. Observability

| Gate | Pass Criteria | Status |
|------|--------------|--------|
| `falcon_txn_terminal_total` emitted for all outcomes | Metric exists with type/reason/sqlstate labels | ☐ |
| `falcon_admission_rejection_total` per resource | Metric exists with resource/sqlstate labels | ☐ |
| `falcon_failover_recovery_duration_ms` emitted | Histogram with outcome label | ☐ |
| `falcon_queue_depth` per queue | Gauge with queue label | ☐ |
| `falcon_replay_violation_total` emitted | Gauge exists | ☐ |

## 6. SQL Compatibility

| Gate | Pass Criteria | Status |
|------|--------------|--------|
| All §1–§4 features tested | Handler + integration tests | ☐ |
| All §5 unsupported features return 0A000 | Explicit rejection tests | ☐ |
| No unsupported SQL causes panic | Fuzz + negative tests | ☐ |

## 7. Documentation Contract

| Gate | Pass Criteria | Status |
|------|--------------|--------|
| CONSISTENCY.md matches code | Every invariant has code ref | ☐ |
| Forbidden states enumerated | FS-1 through FS-10 | ☐ |
| sql_compatibility.md frozen | Matrix matches handler tests | ☐ |

---

## Soak Test Configuration

```toml
[soak]
duration = "30m"
concurrency = 200
target_qps = 5000
seed = 42
workload = "mixed"  # 70% read, 25% write, 5% DDL

[limits]
max_inflight_queries = 100
max_inflight_writes_per_shard = 50
memory_budget_bytes = 2147483648  # 2GB
wal_flush_queue_limit_bytes = 67108864  # 64MB

[assertions]
max_p99_latency_ms = 50
min_tps = 1000
max_memory_growth_pct = 5  # over 30min
max_rejection_rate_pct = 10
zero_panics = true
zero_unknown_outcomes = true
zero_state_regressions = true
```

## Sign-off

| Role | Name | Date | Approved |
|------|------|------|----------|
| Author | — | — | ☐ |
| Reviewer | — | — | ☐ |
