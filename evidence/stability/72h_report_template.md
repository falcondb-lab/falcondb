# FalconDB — 72-Hour Stability Test Report

| Field | Value |
|-------|-------|
| **Version** | <!-- auto-filled --> |
| **Git Hash** | <!-- auto-filled --> |
| **Start Time** | <!-- auto-filled --> |
| **End Time** | <!-- auto-filled --> |
| **Duration** | 72 hours |
| **Hardware** | <!-- CPU, RAM, Disk --> |

## Summary

| Criterion | Result | Status |
|-----------|--------|--------|
| Memory leak (RSS growth > 20%) | — | ✅ / ❌ |
| Performance drift (TPS variance > 15%) | — | ✅ / ❌ |
| Error rate | — | ✅ / ❌ |
| WAL backlog growth | — | ✅ / ❌ |
| Unplanned restarts | 0 | ✅ / ❌ |

## Memory Profile

| Metric | Value |
|--------|-------|
| Initial RSS | — KB |
| Final RSS | — KB |
| Peak RSS | — KB |
| Growth | — KB (—%) |
| Conclusion | No leak / Suspect leak |

## Performance Profile

| Hour | TPS (avg) | p99 Latency (ms) | Error Count |
|------|-----------|-------------------|-------------|
| 0–1 | — | — | — |
| 12 | — | — | — |
| 24 | — | — | — |
| 36 | — | — | — |
| 48 | — | — | — |
| 60 | — | — | — |
| 72 | — | — | — |

## WAL & Replication

| Metric | Start | End | Trend |
|--------|-------|-----|-------|
| WAL backlog (bytes) | — | — | Stable / Growing |
| Replication lag (µs) | — | — | Stable / Growing |

## Conclusion

<!-- One of: PASS / WARN / FAIL -->

## Reproduction

```bash
./scripts/run_stability_test.sh --duration 72h
```

## Raw Data

- Metrics CSV: `evidence/stability/72h_metrics_*.csv`
