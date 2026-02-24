# CONSISTENCY.md — Normative Consistency Contract (v1.0.4)

> **This document is normative.** Every statement here has a corresponding code
> reference and test reference. If docs and code disagree, it is a bug.

---

## 1. Commit Point Model

Every transaction passes through three ordered commit points:

```
CP-L (LogicalCommit) ≤ CP-D (DurableCommit) ≤ CP-V (ClientVisibleCommit)
```

| Invariant | Statement | Code Reference | Test Reference |
|-----------|-----------|----------------|----------------|
| **CP-1** | CP-L ≤ CP-D ≤ CP-V (strict ordering) | `consistency.rs §1` | `validate_commit_timeline()` |
| **CP-2** | No CP-V before CP-D under active policy | `consistency.rs §1` | `validate_commit_timeline()` |
| **CP-3** | Crash after CP-L but before CP-D MAY lose txn | `consistency.rs §5 CrashPoint` | `CrashPoint::must_survive_recovery()` |
| **CP-4** | Crash after CP-D MUST NOT lose txn | `consistency.rs §5 CrashPoint` | `CrashPoint::must_survive_recovery()` |

**Code**: `falcon_common::consistency::CommitPoint`, `validate_commit_timeline()`

## 2. Transaction Terminal States

Every transaction MUST end in exactly one terminal state. No exceptions.

| Terminal State | SQLSTATE | Retryable | Meaning |
|---------------|----------|-----------|---------|
| **Committed** | `00000` | No | Durable and visible. MUST survive any single-node crash. |
| **Aborted (retryable)** | `40001` | Yes | Write conflict, serialization failure, or timeout. Client SHOULD retry. |
| **Aborted (non-retryable)** | `23000`/`25006` | No | Constraint violation, read-only violation, or invariant breach. |
| **Rejected (pre-admission)** | `53200`/`53300` | Yes (with backoff) | Resource exhaustion. Transaction never started. |
| **Indeterminate** | `08006` | Yes (check first) | Crash/partition during commit. Client MUST check status. |

**Invariant TTS-1**: No transaction may end without a terminal classification.
**Invariant TTS-2**: Each terminal state maps to exactly one SQLSTATE + retry policy.
**Invariant TTS-3**: `Indeterminate` MUST NOT occur during normal operation.

**Code**: `falcon_cluster::determinism_hardening::TxnTerminalState`, `classify_error()`

## 3. Resource Exhaustion Contract

Every exhaustible resource has a deterministic rejection path:

| Resource | SQLSTATE | Retry Policy | Code Reference |
|----------|----------|-------------|----------------|
| Memory (hard limit) | `53200` | Exponential backoff 100ms–5s | `admission.rs::check_memory()` |
| Memory (soft pressure) | `53200` | Retry after 50ms | `admission.rs::check_memory()` |
| WAL backlog | `53300` | Retry after 200ms | `admission.rs::check_wal_backlog()` |
| Replication lag | `57P03` | Retry after 500ms | `admission.rs::check_replication_lag()` |
| Connection limit | `53300` | Exponential backoff 50ms–2s | `admission.rs::acquire_connection()` |
| Query concurrency | `53300` | Retry after 50ms | `admission.rs::acquire_query()` |
| Write concurrency/shard | `53300` | Retry after 50ms | `admission.rs::acquire_write()` |
| Cross-shard txn | `53300` | Retry after 100ms | `cross_shard_throttle.rs` |
| DDL concurrency | `53300` | Retry after 200ms | `admission.rs::acquire_ddl()` |

**Invariant RES-1**: No resource exhaustion path may block implicitly.
**Invariant RES-2**: Every rejection increments a per-resource counter visible via metrics.
**Invariant RES-3**: No queue may grow without bound.

**Code**: `falcon_cluster::determinism_hardening::resource_contracts()`, `QueueDepthGuard`

## 4. Failover × Transaction Invariants

| Invariant | Statement | Code Reference |
|-----------|-----------|----------------|
| **FC-1** | Crash before CP-D → txn MUST be rolled back | `determinism_hardening::CommitPhase::expected_recovery()` |
| **FC-2** | Crash after CP-D → txn MUST survive recovery | `determinism_hardening::validate_failover_invariants()` |
| **FC-3** | No txn may be acknowledged (CP-V) without CP-D | `consistency.rs::validate_commit_timeline()` |
| **FC-4** | WAL replay is idempotent | `determinism_hardening::IdempotentReplayValidator` |
| **FAIL-1** | After promote, new primary commits ⊆ old primary commits | `consistency.rs::validate_promote_commit_set()` |
| **FAIL-2** | ACK'd txns under replica-ack policy survive failover | `consistency.rs §7 failover_invariants` |
| **FAIL-3** | Old primary MUST be fenced after promote | `consistency.rs §7 failover_invariants` |

**Additional v1.0.4 invariants**:
- Failover drain: active txns are fail-fast aborted within bounded time (default 5s).
- In-doubt TTL: prepared txns have max lifetime (default 60s), then forced abort.
- Failover damper: min 10s between failover events to prevent oscillation.

**Code**: `falcon_cluster::failover_txn_hardening`

## 5. WAL Invariants

| Invariant | Statement |
|-----------|-----------|
| **WAL-1** | Every WAL entry has a unique, monotonically increasing LSN |
| **WAL-2** | WAL replay is idempotent |
| **WAL-3** | Commit timestamps are monotonically increasing in WAL order |
| **WAL-4** | Repeated WAL replay converges to the same state |
| **WAL-5** | Recovery makes all committed txns visible, all uncommitted invisible |
| **WAL-6** | CRC32 checksum on every record; corruption halts recovery |

**Code**: `falcon_common::consistency::wal_invariants`

## 6. Replication Invariants

| Invariant | Statement |
|-----------|-----------|
| **REP-1** | Replica committed set is always a prefix of primary committed set |
| **REP-2** | Replica never commits a txn that primary has not committed |
| **REP-3** | Replica applies WAL entries in primary's write order |
| **REP-4** | Replica ACK means WAL entry has been applied, not just received |

**Code**: `falcon_common::consistency::replication_invariants`

## 7. Cross-Shard Transaction Invariants

| Invariant | Statement |
|-----------|-----------|
| **XS-1** | Cross-shard txn commits on all shards or aborts on all |
| **XS-2** | Cross-shard txn commits at most once (no duplicate commits) |
| **XS-3** | Coordinator crash recovery resolves in-doubt txns deterministically |
| **XS-4** | Participant holds PREPARED txn until coordinator resolves |
| **XS-5** | Timeout aborts on all participants — no hanging locks |

**Code**: `falcon_common::consistency::cross_shard_invariants`

## 8. Forbidden States

> A forbidden state is one that MUST NEVER be observed. If observed, it is a
> correctness bug with severity P0.

| ID | Forbidden State | Detection | Mitigation |
|----|----------------|-----------|------------|
| **FS-1** | Client receives COMMIT OK but txn is rolled back after recovery | `validate_failover_invariants()` | CP-D must precede CP-V |
| **FS-2** | Transaction ends without terminal classification | `TxnTerminalState` enum exhaustiveness | All paths through TxnManager produce terminal state |
| **FS-3** | Queue grows without bound under sustained load | `QueueDepthGuard::try_enqueue()` hard cap | Every queue has `hard_capacity`; rejection is immediate |
| **FS-4** | Resource exhaustion causes implicit blocking (no timeout) | `ResourceExhaustionContract::max_reject_latency_us` | All admission checks are non-blocking |
| **FS-5** | Duplicate commit of same txn_id | `RetryGuard` + `FailoverOutcomeGuard` | Epoch-based dedup + idempotent replay |
| **FS-6** | Partial commit (some shards committed, others not) | XS-1 atomicity invariant | 2PC coordinator WAL + forced abort on timeout |
| **FS-7** | State regression (Committed → Aborted or Aborted → Active) | `TxnStateGuard` forward-only enforcement | `StateOrdinal::rank()` monotonicity check |
| **FS-8** | WAL replay produces different state on second replay | `IdempotentReplayValidator` | `WAL-2` idempotency invariant |
| **FS-9** | Replica has commit not present on primary | `validate_prefix_property()` | REP-1/REP-2 prefix property |
| **FS-10** | Unsupported SQL causes panic or state corruption | SQL compatibility matrix | `SQLSTATE 0A000` for all unsupported features |

## 9. Observability Contract

Every bad thing MUST be visible within 60 seconds:

| What | Where | Metric |
|------|-------|--------|
| Transaction rejected by admission | Prometheus + `SHOW falcon.admission` | `falcon_admission_rejection_total{resource}` |
| Transaction aborted by conflict | Prometheus + txn stats | `falcon_txn_terminal_total{type="aborted_retryable"}` |
| Failover recovery duration | Prometheus | `falcon_failover_recovery_duration_ms` |
| Queue at capacity | Prometheus | `falcon_queue_depth{queue}` |
| Idempotency violation | Prometheus + FATAL log | `falcon_replay_violation_total` |
| Circuit breaker open | Prometheus + `SHOW falcon.circuit_breakers` | `falcon_chaos_partition_active` |

**Code**: `falcon_observability::record_txn_terminal_state()`, `record_admission_rejection()`, `record_failover_recovery_duration_ms()`, `record_queue_depth_metrics()`, `record_replay_metrics()`

---

*Last updated: v1.0.4. Every invariant in this document has a code reference and test reference. If you find one without, file a P0 bug.*
