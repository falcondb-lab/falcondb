# FalconDB — 7-Day Stability Test Report

| Field | Value |
|-------|-------|
| **Version** | <!-- auto-filled --> |
| **Git Hash** | <!-- auto-filled --> |
| **Start Time** | <!-- auto-filled --> |
| **End Time** | <!-- auto-filled --> |
| **Duration** | 7 days (168 hours) |
| **Hardware** | <!-- CPU, RAM, Disk --> |

## Summary

| Criterion | Result | Status |
|-----------|--------|--------|
| Memory leak (RSS growth > 20%) | — | ✅ / ❌ |
| Performance drift (TPS variance > 15%) | — | ✅ / ❌ |
| Error rate | — | ✅ / ❌ |
| WAL backlog growth | — | ✅ / ❌ |
| Replication lag growth | — | ✅ / ❌ |
| Unplanned restarts | 0 | ✅ / ❌ |

## Memory Profile

| Metric | Value |
|--------|-------|
| Initial RSS | — KB |
| Final RSS | — KB |
| Peak RSS | — KB |
| Growth | — KB (—%) |
| Conclusion | No leak / Suspect leak |

## Performance Profile (Daily)

| Day | TPS (avg) | p99 Latency (ms) | Abort Rate (%) | Error Count |
|-----|-----------|-------------------|----------------|-------------|
| 1 | — | — | — | — |
| 2 | — | — | — | — |
| 3 | — | — | — | — |
| 4 | — | — | — | — |
| 5 | — | — | — | — |
| 6 | — | — | — | — |
| 7 | — | — | — | — |

## WAL & Replication (Daily)

| Day | WAL Backlog (KB) | Replication Lag (µs) | Leader Changes |
|-----|-----------------|---------------------|----------------|
| 1 | — | — | — |
| 2 | — | — | — |
| 3 | — | — | — |
| 4 | — | — | — |
| 5 | — | — | — |
| 6 | — | — | — |
| 7 | — | — | — |

## Conclusion

<!-- One of: PASS / WARN / FAIL with justification -->

## Reproduction

```bash
./scripts/run_stability_test.sh --duration 7d
```

## Raw Data

- Metrics CSV: `evidence/stability/7d_metrics_*.csv`
