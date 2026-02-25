# FalconDB Postmortem & Incident Replay Guide

## Overview
v1.1.1 makes every incident reproducible. The system automatically captures
decision points, metric changes, and event timelines so postmortems can be
generated without manual log-digging.

## Decision Points

Every automated system decision is recorded:
```
[timestamp] LEADER_ELECTION: Node 2 heartbeat timeout
  trigger: heartbeat_gap_ms = 5000 > 3000
  action: initiated leader election for shard 0
  outcome: Leader elected in 200ms, 0 transactions lost
```

### Captured Fields
| Field | Description |
|-------|-------------|
| `decision` | Decision type (LEADER_ELECTION, BACKPRESSURE, etc.) |
| `reason` | Human-readable reason |
| `trigger_metric` | Metric that triggered the decision |
| `trigger_value` | Observed value |
| `threshold` | Threshold that was breached |
| `action_taken` | What the system did |
| `outcome` | Result of the action |

## Metric Snapshots

Key metrics are continuously sampled for trend analysis:
- latency_p99_ms
- throughput_qps
- memory_usage_bytes
- wal_backlog_bytes
- replication_lag_ms
- connection_count

## Postmortem Report

### Auto-Generated Sections

1. **Timeline** — chronological event sequence with severity
2. **Metric Changes** — before vs during comparison with delta %
3. **Decision Points** — all automated decisions in the window
4. **Root Cause** — (optional, filled by operator)
5. **Impact Summary** — (optional, filled by operator)
6. **Recommendations** — (auto-generated or manual)

### Example Report
```
=== POSTMORTEM: Node 2 Crash — Production Incident ===
Incident ID: 42
Window: 1718000000 — 1718003600
Generated: 1718010000

--- TIMELINE ---
[1718000000] [CRITICAL] NODE_FAILURE — Node 2 process crashed (SIGKILL)
[1718000002] [HIGH] LEADER_ELECTION — Shard 0 leader transferred to node 3
[1718000005] [HIGH] CLIENT_ERRORS — 15 client connections reset
[1718000010] [MEDIUM] BACKPRESSURE — Gateway backpressure activated
[1718000020] [INFO] RECOVERY — Latency returning to normal

--- METRIC CHANGES ---
latency_p99_ms: 5.00 → 55.00 (+1000.0%)
throughput_qps: 10000.00 → 5000.00 (-50.0%)

--- DECISION POINTS ---
[1718000002] LEADER_ELECTION: Node 2 heartbeat timeout
  (trigger: heartbeat_gap_ms=5000.00 > 3000.00) → initiated election
[1718000010] BACKPRESSURE: p99 exceeded guardrail
  (trigger: latency_p99_ms=70.00 > 20.00) → activated backpressure
```

## CLI Commands

```
falconctl postmortem list                    # list recent incidents
falconctl postmortem show <id>               # show report
falconctl postmortem export <id> --format text   # export as text
falconctl postmortem export <id> --format json   # export as JSON
```

## Hardened Audit Log

### Unified Event Schema
Every audit event carries:
| Field | Description |
|-------|-------------|
| `trace_id` | Distributed trace ID |
| `span_id` | Span within the trace |
| `category` | AUTH, DATA, OPS, SECURITY, etc. |
| `severity` | INFO, WARN, ERROR, CRITICAL |
| `actor` | Who performed the action |
| `source_ip` | Client IP |
| `action` | What was done |
| `resource` | What was affected |
| `outcome` | OK, DENIED, ERROR |
| `component` | gateway, executor, storage, etc. |
| `duration_us` | Operation duration |

### Trace Correlation
Query all events for a distributed request:
```
audit.query_by_trace("trace-abc-123")
```
Returns events across gateway → executor → storage → replication.

### SIEM Export
Export as JSON-lines for ELK/Splunk/SIEM:
```
falconctl audit export --since-id 1000 --format jsonl
```
