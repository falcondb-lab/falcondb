# FalconDB SLA/SLO Metrics

> v1.0.9 — Built-in observability for production SLA contracts.

## Overview

FalconDB exposes real-time SLA/SLO metrics via the `SloTracker`. These metrics power the `/admin/slo` endpoint and drive admission control decisions.

## Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| `write_availability` | Fraction of successful writes (0.0–1.0) | ≥ 0.999 |
| `read_availability` | Fraction of successful reads (0.0–1.0) | ≥ 0.9999 |
| `write_p99_us` | 99th percentile write latency (µs) | ≤ 20ms |
| `read_p99_us` | 99th percentile read latency (µs) | ≤ 10ms |
| `replication_lag_max_lsn` | Maximum replication lag across all replicas (LSN) | ≤ 1000 |
| `gateway_reject_rate` | Fraction of requests rejected by admission (0.0–1.0) | ≤ 0.01 |
| `window_secs` | Measurement window duration | configurable |
| `timestamp` | Unix timestamp of the snapshot | — |

## `/admin/slo` Endpoint

```
GET /admin/slo
```

Response (JSON):

```json
{
  "write_availability": 0.9997,
  "read_availability": 0.9999,
  "write_p99_us": 15200,
  "read_p99_us": 4800,
  "replication_lag_max_lsn": 42,
  "gateway_reject_rate": 0.002,
  "window_secs": 60,
  "timestamp": 1718000000
}
```

## How Metrics Are Collected

- **Write/Read latency**: Recorded per-operation via `record_write(latency_us, success)` and `record_read(latency_us, success)`
- **Gateway rejects**: Recorded via `record_gateway_request(rejected: bool)`
- **Replication lag**: Updated via `update_replication_lag(lag)` from the catch-up coordinator
- **P99 calculation**: Sorted snapshot of the sliding window buffer (default 10,000 samples)

## SLA Contract Recommendations

| Tier | Write Avail | Read Avail | Write P99 | Read P99 | Repl Lag |
|------|-------------|------------|-----------|----------|----------|
| **Standard** | 99.9% | 99.99% | 50ms | 20ms | 5000 LSN |
| **Premium** | 99.95% | 99.999% | 20ms | 10ms | 1000 LSN |
| **Critical** | 99.99% | 99.999% | 10ms | 5ms | 100 LSN |

## Integration with Backpressure

When SLO metrics degrade, the backpressure controller (`BackpressureController`) adjusts admission rates:

| Pressure Level | Admission Rate | Trigger |
|---------------|----------------|---------|
| **Normal** | 100% | All signals below thresholds |
| **Elevated** | 80% | Any signal above elevated threshold |
| **High** | 50% | Any signal above high threshold |
| **Critical** | 10% | Any signal above critical threshold |

## Alerting

Monitor these metrics with your alerting system:

```
ALERT write_availability < 0.999 FOR 5m → PAGE
ALERT write_p99_us > 50000 FOR 2m → WARN
ALERT replication_lag_max_lsn > 10000 FOR 1m → WARN
ALERT gateway_reject_rate > 0.05 FOR 1m → PAGE
```
