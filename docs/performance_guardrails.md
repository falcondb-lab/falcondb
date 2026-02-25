# FalconDB Performance Guardrails Guide

## Overview
v1.1.1 guarantees that p99/p999 latency never "silently degrades."
Every hot path has a guardrail with automatic backpressure and event recording.

## Guarded Paths

| Path | Default p99 | Default p999 | Absolute Max | Backpressure |
|------|-------------|--------------|--------------|--------------|
| WAL Commit | 5ms | 20ms | 50ms | Yes |
| Gateway Forward | 10ms | 50ms | 100ms | Yes |
| Cold Decompress | 20ms | 100ms | 200ms | Yes |
| Replication Apply | 5ms | 25ms | 75ms | Yes |
| Index Lookup | 10ms | 50ms | 100ms | Optional |
| Txn Commit | 10ms | 50ms | 100ms | Yes |

## Breach Types

| Type | Trigger | Action |
|------|---------|--------|
| `P99_EXCEEDED` | Rolling p99 > threshold | Event recorded, optional backpressure |
| `P999_EXCEEDED` | Rolling p999 > threshold | Event recorded, backpressure |
| `ABS_MAX_EXCEEDED` | Single op > absolute max | Immediate backpressure + event |

## Backpressure Behavior
When activated:
- New write transactions may be rejected or delayed
- Background tasks are throttled more aggressively
- Cleared manually or automatically when metrics recover

## Background Task Resource Isolation

### Task Types & Quotas

| Task | Default CPU | Default IO | Max Concurrent | Dynamic Throttle |
|------|-------------|------------|----------------|------------------|
| Compaction | 20% | 50 MB/s | 2 | Yes |
| Cold Migration | 15% | 30 MB/s | 1 | Yes |
| Snapshot | 10% | 30 MB/s | 1 | Yes |
| Rebalance | 20% | 40 MB/s | 2 | Yes |
| Backup | 10% | 20 MB/s | 1 | Yes |
| Index Build | 15% | 25 MB/s | 1 | Yes |
| GC | 10% | 20 MB/s | 1 | No |

### Dynamic Throttle
When `dynamic_throttle = true` and foreground load > 50%:
- Effective IO limit = quota × (1 - foreground_load)
- At 80% foreground load → background gets 20% of its quota
- At 90% foreground load → background gets 10% of its quota

### Throttle Decisions
- **Allow**: proceed at full speed
- **Throttle(ms)**: sleep for N ms before proceeding
- **Reject**: quota exceeded, retry later

## Verification
Under mixed foreground + background workload:
- Foreground p99 must remain within guardrails
- Background tasks complete (with degraded throughput)
- No starvation in either direction

## Metrics

| Metric | Description |
|--------|-------------|
| `samples_recorded` | Total latency samples |
| `p99_breaches` | p99 guardrail breaches |
| `p999_breaches` | p999 guardrail breaches |
| `absolute_breaches` | Absolute max breaches |
| `backpressure_activations` | Backpressure trigger count |
| `requests_allowed` | BG task ops allowed |
| `requests_throttled` | BG task ops throttled |
| `requests_rejected` | BG task ops rejected |
| `dynamic_reductions` | Dynamic throttle activations |
