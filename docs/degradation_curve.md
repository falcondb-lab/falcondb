# Degradation Curve Report — FalconDB P2

> Version: P2-draft  
> Scope: Provable pressure → reject predictability

---

## 1. Overview

FalconDB's SLA-aware admission controller guarantees a **predictable degradation
curve**: as load increases beyond capacity, the system rejects requests
deterministically rather than allowing unbounded queueing.

**Key invariant**: Latency stays near SLA targets; excess load manifests as
rejection rate increase, not latency explosion.

---

## 2. Degradation Model

```
                    ┌─────────────────────────────┐
  Latency (p99)     │    ┌── SLA target ──┐        │
                    │    │                │        │
                    │    │     FLAT       │  FLAT  │
                    │    │   (accepted)   │(reject)│
                    │────┘                └────────│
                    └─────────────────────────────┘
                         Normal    Overload
                    ◄──────────►◄─────────────────►
                      QPS increasing →

  Reject Rate       │                    ╱─────────│
                    │                   ╱          │
                    │                  ╱           │
                    │                 ╱            │
                    │________________╱             │
                    └─────────────────────────────┘
                         Normal    Overload
```

**Anti-pattern** (what we prevent):
```
  Latency (p99)     │                         ╱│
                    │                        ╱ │  ← "cliff"
                    │                       ╱  │
                    │                      ╱   │
                    │_____________________╱    │
                    └─────────────────────────┘
```

---

## 3. Test Workloads

### Workload A: Fast-Path Small Writes
- Single-shard INSERT/UPDATE, < 10 rows per txn
- Expected: FastPath classification, highest admission priority
- Load progression: 100 → 500 → 1000 → 2000 → 5000 TPS

### Workload B: Mixed Read/Write
- 60% SELECT, 20% INSERT, 15% UPDATE, 5% DELETE
- Mix of single-shard and cross-shard queries
- Load progression: 100 → 500 → 1000 → 2000 → 5000 TPS

### Workload C: Cross-Shard Heavy
- 50% cross-shard transactions (2-4 shards per txn)
- Includes distributed aggregation queries
- Load progression: 50 → 200 → 500 → 1000 → 2000 TPS

---

## 4. Metrics Collected at Each Load Level

| Metric | Source |
|--------|--------|
| p50 latency (ms) | `falcon_sla_p99_us` / LatencyTracker |
| p99 latency (ms) | `falcon_sla_p99_us` |
| p999 latency (ms) | `falcon_sla_p999_us` |
| Accept rate (%) | `falcon_sla_total_accepted` / total |
| Reject rate (%) | `falcon_sla_total_rejected` / total |
| Reject by signal | `falcon_sla_rejection_total{signal=*}` |
| Inflight fast/slow/ddl | `falcon_sla_inflight_*` |
| GC sweep duration (us) | `falcon_gc_sweep_duration_us` |
| GC budget exhausted | `falcon_gc_budget_exhausted_total` |
| Max chain length | `falcon_gc_max_chain_length` |
| Network bytes in/out | `falcon_dist_query_bytes_*_total` |
| Abort/retry rate | `falcon_txn_aborted_total` |

---

## 5. Expected Results Template

### Workload A: Fast-Path Writes

| QPS | p50 (ms) | p99 (ms) | p999 (ms) | Accept % | Reject % | Top Reject Signal |
|-----|----------|----------|-----------|----------|----------|-------------------|
| 100 | _TBD_ | _TBD_ | _TBD_ | ~100% | ~0% | — |
| 500 | _TBD_ | _TBD_ | _TBD_ | ~100% | ~0% | — |
| 1000 | _TBD_ | _TBD_ | _TBD_ | >95% | <5% | — |
| 2000 | _TBD_ | _TBD_ | _TBD_ | >80% | <20% | inflight_limit |
| 5000 | _TBD_ | _TBD_ | _TBD_ | >50% | <50% | p999_exceeded |

### Workload B: Mixed Read/Write

| QPS | p50 (ms) | p99 (ms) | p999 (ms) | Accept % | Reject % | Top Reject Signal |
|-----|----------|----------|-----------|----------|----------|-------------------|
| 100 | _TBD_ | _TBD_ | _TBD_ | ~100% | ~0% | — |
| 500 | _TBD_ | _TBD_ | _TBD_ | >98% | <2% | — |
| 1000 | _TBD_ | _TBD_ | _TBD_ | >90% | <10% | slow_path_quota |
| 2000 | _TBD_ | _TBD_ | _TBD_ | >70% | <30% | p99_exceeded |
| 5000 | _TBD_ | _TBD_ | _TBD_ | >40% | <50% | p999_exceeded |

### Workload C: Cross-Shard Heavy

| QPS | p50 (ms) | p99 (ms) | p999 (ms) | Accept % | Reject % | Top Reject Signal |
|-----|----------|----------|-----------|----------|----------|-------------------|
| 50 | _TBD_ | _TBD_ | _TBD_ | ~100% | ~0% | — |
| 200 | _TBD_ | _TBD_ | _TBD_ | >95% | <5% | — |
| 500 | _TBD_ | _TBD_ | _TBD_ | >80% | <20% | slow_path_quota |
| 1000 | _TBD_ | _TBD_ | _TBD_ | >60% | <40% | p99_exceeded |
| 2000 | _TBD_ | _TBD_ | _TBD_ | >30% | <50% | p999_exceeded |

---

## 6. Validation Criteria

### PASS if:
1. **No latency cliff**: p99 does NOT exhibit step-function increase > 3x
   between adjacent QPS levels
2. **Reject absorbs overload**: reject rate increases monotonically with load
3. **p99 stays bounded**: p99 < 2x `target_p99_ms` at all load levels
4. **p999 stays bounded**: p999 < 2x `target_p999_ms` at all load levels
5. **Fast-path priority**: under overload, fast-path accept rate > slow-path accept rate
6. **GC does not spike**: GC sweep duration stays within budget at all load levels

### FAIL if:
1. p99 jumps > 5x between adjacent load levels (cliff)
2. Reject rate exceeds `max_reject_rate` (50%) without corresponding latency
   protection
3. GC budget exhaustion rate > 30% during soak test
4. Any unexplained latency spike without a logged rejection reason

---

## 7. Reproducing

```bash
# Run degradation curve benchmark
./scripts/degradation_curve_bench.sh

# Or manually:
cargo run -p falcon_bench -- \
  --config bench_configs/latency/workload_a.toml \
  --ramp-up \
  --duration 60
```

---

## 8. Architecture Reference

| Component | Module | Role |
|-----------|--------|------|
| SLA Admission | `falcon_cluster::sla_admission` | p99/p999-driven 3-state admission |
| Latency Tracker | `falcon_cluster::sla_admission::LatencyTracker` | Sliding window percentile tracking |
| Txn Classification | `falcon_cluster::sla_admission::classify_txn` | Fast/slow/DDL/system priority |
| GC Budget | `falcon_storage::gc_budget` | Time+key bounded incremental GC |
| Chain Histogram | `falcon_storage::gc_budget::ChainLengthHistogram` | MVCC chain distribution |
| Network Stats | `falcon_cluster::distributed_exec::network_stats` | Per-query byte tracking |
| Pushdown Estimator | `...::network_stats::estimate_pushdown_benefit` | Cost model for pushdown |
