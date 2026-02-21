# FalconDB Chaos Matrix

## Overview

This document defines the chaos scenario matrix for FalconDB production readiness validation.
Each scenario specifies: the fault injected, the expected system behavior, the acceptance criteria,
and the verification command.

---

## Scenario Matrix

### Category 1: Node Failures

| ID | Scenario | Fault | Expected Behavior | Acceptance Criteria | Script |
|----|----------|-------|-------------------|---------------------|--------|
| C1-01 | Kill primary during write | `kill -9 <primary>` | Replica promotes within RTO | RTO < 5s, no data loss (quorum-ack) | `ci_failover_gate.sh` |
| C1-02 | Kill primary during 2PC | `kill -9 <coordinator>` | InDoubtResolver converges | All in-doubt txns resolved within 30s | `ci_failover_gate.sh` |
| C1-03 | Kill replica during replication | `kill -9 <replica>` | Primary continues, replica rejoins | Replica catches up after restart, lag=0 | `local_cluster_harness.sh` |
| C1-04 | Kill all replicas | `kill -9 <all replicas>` | Primary continues standalone | Writes succeed, replication resumes on rejoin | manual |
| C1-05 | Restart primary (graceful) | `SIGTERM <primary>` | Graceful shutdown, WAL flushed | No data loss, replica lag=0 after restart | `rolling_upgrade_smoke.sh` |
| C1-06 | Restart loop (3x in 60s) | repeated `kill -9 + restart` | Panic throttle activates | No OOM, system stabilizes, throttle logged | `chaos_injector.sh run kill_restart` |

### Category 2: Network Failures

| ID | Scenario | Fault | Expected Behavior | Acceptance Criteria | Script |
|----|----------|-------|-------------------|---------------------|--------|
| C2-01 | Network delay primary→replica | `tc netem delay 100ms ±20ms` | Replication lag increases, writes continue | Lag recovers within 10s of delay removal | `chaos_injector.sh delay` |
| C2-02 | Packet loss primary→replica | `tc netem loss 5%` | Retransmits, lag spikes | System recovers, no data loss | `chaos_injector.sh loss` |
| C2-03 | Network partition (split-brain) | block all traffic between nodes | Minority partition rejects writes | Minority returns `Retryable`, majority continues | manual / fault_injection |
| C2-04 | High latency (200ms) | `tc netem delay 200ms` | 2PC tail latency increases | P99 < 500ms, no timeouts causing data loss | `chaos_injector.sh delay` |
| C2-05 | Intermittent connectivity | `tc netem loss 20%` | Circuit breaker opens for affected shard | Retryable errors returned, circuit closes on recovery | `chaos_injector.sh loss` |

### Category 3: Resource Exhaustion

| ID | Scenario | Fault | Expected Behavior | Acceptance Criteria | Script |
|----|----------|-------|-------------------|---------------------|--------|
| C3-01 | Memory pressure (>80% budget) | sustained writes with small budget | Backpressure activates | `Transient` errors returned, no OOM | `ci_production_gate.sh` |
| C3-02 | Memory hard limit (>95%) | burst writes | All writes rejected | `Transient` with `retry_after_ms=500`, no crash | `ci_production_gate.sh` |
| C3-03 | WAL flush queue full | sustained writes faster than flush | WAL backpressure activates | `Transient` errors, queue drains, system recovers | `ci_production_gate.sh` |
| C3-04 | Connection limit reached | >max_connections clients | New connections rejected | `Transient` error on connect, existing connections unaffected | admission tests |
| C3-05 | Query concurrency limit | >max_inflight_queries | New queries rejected | `Transient` error, in-flight queries complete normally | admission tests |
| C3-06 | Disk full (WAL) | fill disk during WAL write | WAL write fails gracefully | Error returned to client, no silent data loss | manual |

### Category 4: Workload Isolation

| ID | Scenario | Fault | Expected Behavior | Acceptance Criteria | Script |
|----|----------|-------|-------------------|---------------------|--------|
| C4-01 | Large query + OLTP mix | run full table scan + concurrent point lookups | Governor limits large query | Small txn P99 < 10ms, large query gets `Transient` if over limit | `ci_production_gate.sh` |
| C4-02 | DDL during OLTP | `ALTER TABLE` during write load | DDL uses low-priority pool | OLTP P99 unaffected (< 2x baseline) | `ci_production_gate.sh` |
| C4-03 | Rebalance during OLTP | trigger rebalance under load | Rebalance rate-limited | OLTP P99 < 20ms during rebalance | `local_cluster_harness.sh` |
| C4-04 | Query timeout | query exceeds `max_execution_time_ms` | Governor kills query | `Transient` error with timeout message, resources freed | governor tests |

### Category 5: 2PC / Transaction Failures

| ID | Scenario | Fault | Expected Behavior | Acceptance Criteria | Script |
|----|----------|-------|-------------------|---------------------|--------|
| C5-01 | Coordinator crash after prepare | kill coordinator post-prepare | InDoubtResolver aborts | All participants abort within 30s | `ci_failover_gate.sh` |
| C5-02 | Participant crash during prepare | kill one participant | Coordinator aborts all | All other participants abort, no partial commit | `ci_failover_gate.sh` |
| C5-03 | Write-write conflict storm | concurrent writes to same key | OCC detects conflicts | `Retryable` errors, no deadlock, system stable | txn tests |
| C5-04 | Long-running txn + GC | txn holds old MVCC versions | GC safepoint blocked | GC lag metric increases, alert fires, txn eventually commits/aborts | manual |
| C5-05 | Outcome cache storm | many participants query coordinator | TxnOutcomeCache absorbs load | Cache hit rate > 90%, coordinator not overloaded | indoubt_resolver tests |

### Category 6: Recovery

| ID | Scenario | Fault | Expected Behavior | Acceptance Criteria | Script |
|----|----------|-------|-------------------|---------------------|--------|
| C6-01 | Clean restart | normal shutdown + restart | WAL replay, full recovery | Recovery completes in < 30s (1M rows), all data intact | `local_cluster_harness.sh` |
| C6-02 | Crash recovery (mid-write) | `kill -9` during write | WAL replay recovers committed txns | Committed data present, in-flight txns rolled back | `ci_failover_gate.sh` |
| C6-03 | Corrupt WAL segment | truncate last WAL segment | Recovery detects corruption | Error logged, last segment discarded, recovery continues | manual |
| C6-04 | Follower with large lag promoted | promote replica with lag=10000 | Promotion rejected or catch-up first | Either: catch-up completes before promotion, or promotion rejected | `ci_failover_gate.sh` |

---

## Acceptance Thresholds

| Metric | Threshold | Notes |
|--------|-----------|-------|
| Failover RTO | < 5s | Time from primary death to writes resuming |
| RPO (quorum-ack) | 0 bytes | No data loss in quorum-ack mode |
| RPO (async) | < 1s of writes | Async replication window |
| In-doubt resolution | < 30s | After coordinator restart |
| P99 (OLTP, no chaos) | < 10ms | Point lookup + simple INSERT |
| P99 (OLTP, during chaos) | < 50ms | Under kill_restart scenario |
| P99 (OLTP, during rebalance) | < 20ms | Under rebalance load |
| Memory pressure recovery | < 5s | After load drops below soft limit |
| Circuit breaker recovery | < 15s | After shard recovers |
| Panic count (per scenario) | 0 | No process panics during chaos |

---

## CI Smoke Scenarios (30-60s each)

These are the scenarios run in `ci_production_gate.sh`:

```bash
# Gate 4: Chaos smoke (30s)
./scripts/chaos_injector.sh run kill_restart .chaos_pids 30
./scripts/chaos_injector.sh report .chaos_logs chaos_report.txt

# Gate 5: Tail latency smoke
# Run YCSB-A for 60s, measure P95/P99
# Assert P99 < TAIL_LATENCY_P99_THRESHOLD_MS

# Gate 6: Observability check
# Assert metrics fields exist, slowlog has stage breakdown
```

---

## Fault Injection Implementation

### Process Kill
```bash
./scripts/chaos_injector.sh kill <pid_file>
./scripts/chaos_injector.sh restart <pid_file> "<start_cmd>"
```

### Network Faults (Linux only, requires `tc`)
```bash
./scripts/chaos_injector.sh delay lo 50 10    # 50ms ±10ms
./scripts/chaos_injector.sh loss  lo 5         # 5% packet loss
./scripts/chaos_injector.sh clear lo           # remove rules
```

### In-Process Fault Injection
FalconDB includes `falcon_cluster::fault_injection` for in-process simulation:
- `FaultInjector::inject_delay(shard_id, duration)`
- `FaultInjector::inject_error(shard_id, error)`
- `FaultInjector::inject_partition(shard_ids)`

---

## Reporting

Each chaos run produces:
- `chaos_report.txt` — summary of errors, warnings, and log lines
- `diag_bundle_<timestamp>.json` — full diagnostic bundle (metrics, topology, inflight state)
- Exit code 0 = all acceptance criteria met, non-zero = failure

Artifacts are uploaded by CI to the job artifact store for post-mortem analysis.
