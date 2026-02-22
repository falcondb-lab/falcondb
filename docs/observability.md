# FalconDB Observability Guide

## Overview

FalconDB provides three observability layers:
1. **Structured Logging** — tracing-based, JSON-compatible, per-request context
2. **Prometheus Metrics** — scraped at `/metrics` (HTTP admin port)
3. **Slow Query Log** — configurable threshold, stage breakdown

---

## Request Identifiers

Every request carries a set of identifiers propagated through all log/trace/metric output:

| Field | Type | Description |
|-------|------|-------------|
| `request_id` | `u64` | Unique per TCP connection request (monotonic) |
| `session_id` | `u64` | Unique per client session (connection lifetime) |
| `query_id` | `u64` | Unique per SQL query within a session |
| `txn_id` | `u64` | Transaction ID (0 for auto-commit) |
| `shard_id` | `u64` | Target shard (0 for single-shard) |

---

## Stage Latency Breakdown

Every query records latency at these stages (microseconds):

| Stage | Description |
|-------|-------------|
| `parse_us` | SQL text → AST |
| `bind_us` | AST → BoundStatement (name resolution, type checking) |
| `plan_us` | BoundStatement → PhysicalPlan |
| `execute_us` | PhysicalPlan → ExecutionResult |
| `commit_us` | Transaction commit (WAL flush + replication ack) |
| `total_us` | End-to-end wall time |
| `wal_append_us` | WAL record serialization + append |
| `wal_flush_us` | WAL fdatasync |
| `replication_ack_us` | Time waiting for replica ack (sync mode only) |

---

## Prometheus Metrics

### Transaction Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_txn_committed_total` | Counter | Total committed transactions |
| `falcon_txn_aborted_total` | Counter | Total aborted transactions |
| `falcon_txn_fast_path_total` | Counter | Fast-path (OCC) commits |
| `falcon_txn_slow_path_total` | Counter | Slow-path (2PC) commits |
| `falcon_txn_occ_conflicts_total` | Counter | OCC write conflicts |
| `falcon_txn_active` | Gauge | Currently active transactions |
| `falcon_txn_commit_latency_us` | Histogram | Commit latency distribution |

### WAL Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_wal_bytes_written_total` | Counter | Total WAL bytes written |
| `falcon_wal_segments_total` | Counter | Total WAL segments created |
| `falcon_wal_flush_latency_us` | Histogram | WAL flush (fdatasync) latency |
| `falcon_wal_backlog_bytes` | Gauge | Current WAL backlog size |
| `falcon_wal_enabled` | Gauge | 1 if WAL is enabled |

### Replication Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_replication_lag_lsn` | Gauge | Replica lag in LSN units |
| `falcon_replication_applied_lsn` | Gauge | Last applied LSN on replica |
| `falcon_replication_chunks_applied_total` | Counter | WAL chunks applied |
| `falcon_replication_reconnects_total` | Counter | Replica reconnect count |

### Query Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_query_total` | Counter | Total queries executed |
| `falcon_query_latency_us` | Histogram | Query end-to-end latency |
| `falcon_slow_query_total` | Counter | Queries exceeding slow threshold |
| `falcon_query_errors_total` | Counter | Query errors by kind (user/retryable/transient/bug) |

### Memory Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_memory_used_bytes` | Gauge | Current memory usage |
| `falcon_memory_limit_bytes` | Gauge | Memory budget limit |
| `falcon_memory_pressure_total` | Counter | Times memory pressure triggered |
| `falcon_memory_evictions_total` | Counter | GC/eviction events |

### GC Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_gc_versions_reclaimed_total` | Counter | MVCC versions reclaimed |
| `falcon_gc_bytes_reclaimed_total` | Counter | Bytes reclaimed by GC |
| `falcon_gc_safepoint_ts` | Gauge | Current GC safepoint timestamp |
| `falcon_gc_active_txns` | Gauge | Active transactions (GC blocker) |

### Admission Control Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_admission_rejected_total` | Counter | Requests rejected by admission control |
| `falcon_admission_queued` | Gauge | Requests waiting for permits |
| `falcon_admission_wait_us` | Histogram | Time waiting for admission permit |

### Crash Domain Metrics (v0.5)

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_crash_domain_panic_count` | Gauge | Total panics caught by crash domain |
| `falcon_crash_domain_throttle_window_count` | Gauge | Panics in current throttle window |
| `falcon_crash_domain_throttle_triggered_count` | Gauge | Times panic throttle triggered |
| `falcon_crash_domain_recent_panic_count` | Gauge | Recent panic count |

### Priority Scheduler Metrics (v0.6)

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_scheduler_high_active` | Gauge | Active queries in High (OLTP) lane |
| `falcon_scheduler_high_admitted` | Gauge | Total admitted to High lane |
| `falcon_scheduler_high_rejected` | Gauge | Total rejected from High lane |
| `falcon_scheduler_normal_active` | Gauge | Active queries in Normal lane |
| `falcon_scheduler_normal_max` | Gauge | Max concurrency for Normal lane |
| `falcon_scheduler_normal_admitted` | Gauge | Total admitted to Normal lane |
| `falcon_scheduler_normal_rejected` | Gauge | Total rejected from Normal lane |
| `falcon_scheduler_normal_wait_us` | Gauge | Total wait time in Normal lane (µs) |
| `falcon_scheduler_low_active` | Gauge | Active queries in Low lane |
| `falcon_scheduler_low_max` | Gauge | Max concurrency for Low lane |
| `falcon_scheduler_low_admitted` | Gauge | Total admitted to Low lane |
| `falcon_scheduler_low_rejected` | Gauge | Total rejected from Low lane |
| `falcon_scheduler_low_wait_us` | Gauge | Total wait time in Low lane (µs) |

### Token Bucket Metrics (v0.6)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `falcon_token_bucket_rate_per_sec` | Gauge | `bucket` | Configured rate (tokens/sec) |
| `falcon_token_bucket_burst` | Gauge | `bucket` | Burst capacity |
| `falcon_token_bucket_available` | Gauge | `bucket` | Currently available tokens |
| `falcon_token_bucket_paused` | Gauge | `bucket` | 1 if paused, 0 if active |
| `falcon_token_bucket_consumed` | Gauge | `bucket` | Total tokens consumed |
| `falcon_token_bucket_acquired` | Gauge | `bucket` | Total successful acquisitions |
| `falcon_token_bucket_rejected` | Gauge | `bucket` | Total rejected acquisitions |
| `falcon_token_bucket_wait_us` | Gauge | `bucket` | Total wait time (µs) |

### 2PC Deterministic Transaction Metrics (v0.7)

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_2pc_decision_log_total_logged` | Gauge | Total coordinator decisions logged |
| `falcon_2pc_decision_log_total_applied` | Gauge | Total decisions fully applied |
| `falcon_2pc_decision_log_entries` | Gauge | Current entries in decision log |
| `falcon_2pc_decision_log_unapplied` | Gauge | Unapplied decisions (crash recovery) |
| `falcon_2pc_soft_timeouts` | Gauge | Total soft timeouts (→ Retryable) |
| `falcon_2pc_hard_timeouts` | Gauge | Total hard timeouts (→ abort + resolver) |
| `falcon_2pc_shard_timeouts` | Gauge | Total per-shard RPC timeouts |
| `falcon_2pc_slow_shard_detected` | Gauge | Total slow shard detections |
| `falcon_2pc_slow_shard_fast_aborts` | Gauge | Fast-aborts due to slow shards |
| `falcon_2pc_slow_shard_hedged_sent` | Gauge | Hedged requests sent |
| `falcon_2pc_slow_shard_hedged_won` | Gauge | Hedged requests that won |
| `falcon_2pc_slow_shard_hedged_inflight` | Gauge | Currently in-flight hedged requests |

### Chaos / Fault Injection Metrics (v0.8)

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_chaos_faults_fired` | Gauge | Total fault injection events fired |
| `falcon_chaos_partition_active` | Gauge | 1 if network partition active |
| `falcon_chaos_partition_count` | Gauge | Total partition events |
| `falcon_chaos_partition_heal_count` | Gauge | Total partition heals |
| `falcon_chaos_partition_events` | Gauge | Total partition-related events |
| `falcon_chaos_jitter_enabled` | Gauge | 1 if CPU/IO jitter enabled |
| `falcon_chaos_jitter_events` | Gauge | Total jitter events fired |

### Security Hardening Metrics (v0.9)

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_security_auth_checks` | Gauge | Total auth rate limit checks |
| `falcon_security_auth_lockouts` | Gauge | Total lockout events triggered |
| `falcon_security_auth_failures` | Gauge | Total auth failures recorded |
| `falcon_security_auth_active_lockouts` | Gauge | Currently active lockouts |
| `falcon_security_password_checks` | Gauge | Total password policy checks |
| `falcon_security_password_rejections` | Gauge | Passwords rejected by policy |
| `falcon_security_firewall_checks` | Gauge | Total SQL firewall checks |
| `falcon_security_firewall_blocked` | Gauge | Total statements blocked |
| `falcon_security_firewall_injection` | Gauge | SQL injection patterns detected |
| `falcon_security_firewall_dangerous` | Gauge | Dangerous statements blocked |
| `falcon_security_firewall_stacking` | Gauge | Statement stacking blocked |

### Version / Compatibility Metrics (v0.9)

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_compat_wal_format_version` | Gauge | Current WAL format version |
| `falcon_compat_snapshot_format_version` | Gauge | Current snapshot format version |

---

## Slow Query Log

### Configuration

```toml
[slow_query]
enabled = true
threshold_ms = 100        # Log queries slower than this
max_entries = 1000        # Ring buffer size
include_plan = true       # Include plan summary
include_stage_breakdown = true
```

### Log Format

```json
{
  "timestamp": "2026-02-21T11:43:00.123Z",
  "level": "WARN",
  "event": "slow_query",
  "session_id": 42,
  "query_id": 1337,
  "txn_id": 9001,
  "sql_digest": "SELECT * FROM orders WHERE customer_id = ?",
  "total_us": 450000,
  "stage_breakdown": {
    "parse_us": 120,
    "bind_us": 340,
    "plan_us": 890,
    "execute_us": 448000,
    "commit_us": 650
  },
  "plan_summary": "SeqScan(orders) filter=customer_id=? rows_est=50000",
  "shard_id": 0,
  "wait_reason": "io",
  "rows_returned": 12345,
  "error": null
}
```

### SHOW Commands

```sql
-- View slow query log
SHOW falcon.slow_queries;

-- View current threshold
SHOW log_min_duration_statement;

-- Set threshold (ms)
SET log_min_duration_statement = 50;
```

---

## Structured Log Fields

All log lines include:

```
timestamp  level  target  session_id  query_id  txn_id  shard_id  message
```

### Key Log Events

| Event | Level | Description |
|-------|-------|-------------|
| `query.start` | DEBUG | Query received |
| `query.complete` | DEBUG | Query completed with latency |
| `query.slow` | WARN | Query exceeded slow threshold |
| `query.error` | ERROR | Query failed with error |
| `txn.begin` | DEBUG | Transaction started |
| `txn.commit` | DEBUG | Transaction committed |
| `txn.abort` | WARN | Transaction aborted (with reason) |
| `txn.conflict` | WARN | Write conflict detected |
| `wal.flush` | DEBUG | WAL segment flushed |
| `wal.rotate` | INFO | WAL segment rotated |
| `replication.connected` | INFO | Replica connected to primary |
| `replication.disconnected` | WARN | Replica disconnected |
| `replication.lag` | WARN | Replica lag exceeds threshold |
| `failover.start` | WARN | Failover initiated |
| `failover.complete` | INFO | Failover completed (with RTO) |
| `gc.sweep` | DEBUG | GC sweep completed |
| `admission.rejected` | WARN | Request rejected by admission control |
| `memory.pressure` | WARN | Memory pressure threshold reached |
| `memory.limit` | ERROR | Memory hard limit reached |
| `internal_bug` | ERROR | InternalBug error triggered |

---

## Diagnostic Bundle

The diagnostic bundle captures a point-in-time snapshot for post-mortem analysis.

### HTTP Admin API

```
GET /admin/diagnostic_bundle
```

### Contents

```json
{
  "timestamp": "2026-02-21T11:43:00Z",
  "version": "0.4.0",
  "node_role": "primary",
  "epoch": 42,
  "membership": { "shards": [...], "nodes": [...] },
  "inflight_txns": [
    { "txn_id": 9001, "started_at": "...", "state": "active", "age_ms": 1234 }
  ],
  "slow_queries_recent": [...],
  "metrics_snapshot": {
    "txn_committed": 100000,
    "txn_aborted": 234,
    "wal_backlog_bytes": 0,
    "replication_lag_lsn": 0,
    "memory_used_bytes": 104857600
  },
  "recent_errors": [
    { "timestamp": "...", "kind": "Retryable", "count": 5 }
  ]
}
```

### CLI

```bash
# Via SHOW command
SHOW falcon.health_score;
SHOW falcon.slow_queries;
SHOW falcon.txn_stats;
SHOW falcon.wal_stats;
SHOW falcon.replication_stats;
SHOW falcon.gc_stats;
SHOW falcon.memory_pressure;
```

---

## SHOW Command Catalog

All `SHOW falcon.*` commands available via any PostgreSQL client (`psql`, JDBC, etc.):

### Core Database

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.version` | Server version and build info | v0.1 |
| `SHOW falcon.health` | Basic health status | v0.1 |
| `SHOW falcon.node_role` | Current node role (primary/replica/analytics) | v0.1 |

### Transactions

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.txn` | Current session transaction state | v0.1 |
| `SHOW falcon.txn_stats` | Transaction statistics (committed, aborted, OCC conflicts) | v0.1 |
| `SHOW falcon.txn_history` | Recent transaction history | v0.1 |
| `SHOW falcon.slow_txns` | Currently slow/long-running transactions | v0.3 |

### Storage & WAL

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.wal_stats` | WAL write/flush/backlog statistics | v0.1 |
| `SHOW falcon.gc_stats` | GC sweep statistics (versions reclaimed, bytes freed) | v0.1 |
| `SHOW falcon.gc_safepoint` | Current GC safepoint and active txn count | v0.1 |
| `SHOW falcon.memory_pressure` | Memory usage, limits, pressure state | v0.1 |
| `SHOW falcon.checkpoint_stats` | Checkpoint statistics | v0.2 |

### Replication & HA

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.replication_stats` | Replication lag, applied LSN, reconnects | v0.1 |
| `SHOW falcon.replication` | Replication configuration and state | v0.2 |
| `SHOW falcon.replica_stats` | Per-replica statistics | v0.2 |
| `SHOW falcon.replica_health` | Replica health and connectivity | v0.3 |
| `SHOW falcon.ha_status` | HA group status and failover readiness | v0.3 |

### Cluster & Sharding

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.cluster` | Cluster topology and membership | v0.2 |
| `SHOW falcon.shards` | Shard map and distribution | v0.2 |
| `SHOW falcon.shard_stats` | Per-shard statistics | v0.2 |
| `SHOW falcon.scatter_stats` | Scatter/gather query statistics | v0.2 |
| `SHOW falcon.query_routing` | Query routing decisions | v0.2 |
| `SHOW falcon.dist_capabilities` | Distributed query capabilities | v0.2 |
| `SHOW falcon.rebalance_status` | Shard rebalancer status | v0.3 |
| `SHOW falcon.two_phase` | 2PC coordinator statistics | v0.3 |

### Query Processing

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.slow_queries` | Slow query log entries | v0.3 |
| `SHOW falcon.plan_cache` | Plan cache hit/miss statistics | v0.3 |

### Multi-Tenancy

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.tenants` | Registered tenants | v0.4 |
| `SHOW falcon.tenant_usage` | Per-tenant resource usage | v0.4 |

### Enterprise & Security

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.license` | License information and edition | v0.4 |
| `SHOW falcon.metering` | Resource metering per tenant | v0.4 |
| `SHOW falcon.security` | Security posture (TLS, encryption, IP allowlist) | v0.4 |
| `SHOW falcon.health_score` | Weighted health score with recommendations | v0.4 |
| `SHOW falcon.compat` | Compatibility information | v0.4 |
| `SHOW falcon.audit_log` | Recent audit log entries | v0.4 |
| `SHOW falcon.sla_stats` | SLA priority latency statistics | v0.4 |

### Production Hardening (v0.5+)

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.admission` | Admission control permits and thresholds | v0.5 |
| `SHOW falcon.hotspots` | Hotspot detection results | v0.5 |
| `SHOW falcon.verification` | Consistency verification status | v0.5 |
| `SHOW falcon.latency_contract` | Latency contract and SLO targets | v0.5 |
| `SHOW falcon.cluster_events` | Cluster event log (scale-out/in, rebalance) | v0.5 |
| `SHOW falcon.node_lifecycle` | Node lifecycle state machines | v0.5 |
| `SHOW falcon.rebalance_plan` | Rebalance migration plan | v0.5 |

### Tail Latency Governance (v0.6)

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.priority_scheduler` | Priority queue scheduler metrics (High/Normal/Low lanes) | v0.6 |
| `SHOW falcon.token_bucket` | Token bucket rate limiter metrics | v0.6 |

### Deterministic 2PC (v0.7)

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.two_phase_config` | Decision log, layered timeouts, slow-shard policy | v0.7 |

### Chaos Engineering (v0.8)

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.fault_injection` | Fault injector state (partition, jitter, leader kill) | v0.8 |

### Security Hardening (v0.9)

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.security_audit` | Auth rate limiter, password policy, SQL firewall metrics | v0.9 |

### Release Engineering (v0.9)

| Command | Description | Since |
|---------|-------------|-------|
| `SHOW falcon.wire_compat` | Version, WAL format, snapshot format, min compatible version | v0.9 |

---

## SLO Targets

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| P99 txn commit latency (empty load) | < 1ms | > 5ms |
| P99 txn commit latency (mixed load) | < 10ms | > 50ms |
| P99 query latency (point lookup) | < 5ms | > 20ms |
| Failover RTO | < 5s | > 15s |
| RPO (quorum-ack mode) | 0 | any data loss |
| Replication lag | < 100ms | > 1s |
| GC safepoint lag | < 10s | > 60s |

---

## OpenTelemetry (Optional)

When `FALCON_OTEL_ENDPOINT` is set, traces are exported via OTLP:

```bash
export FALCON_OTEL_ENDPOINT=http://jaeger:4317
```

Span hierarchy:
```
query [session_id, query_id]
  ├── parse
  ├── bind
  ├── plan
  ├── execute
  │   ├── scan [table_id, shard_id]
  │   └── aggregate
  └── commit
      ├── wal_append
      ├── wal_flush
      └── replication_ack
```

---

## Native Protocol Observability

The FalconDB native binary protocol (`falcon_protocol_native` + `falcon_native_server`) provides additional observability for non-PG clients (e.g. the Java JDBC driver).

### Session Tracking

Each native protocol session tracks:

| Field | Description |
|-------|-------------|
| `session_id` | Unique session identifier |
| `client_name` | Client-reported driver name (e.g. `falcondb-jdbc/0.1`) |
| `database` | Target database |
| `user` | Authenticated user |
| `feature_flags` | Negotiated feature flags (bitmask) |
| `request_count` | Total requests processed in this session |
| `state` | Session state (Connected → Handshake → Authenticated → Ready → Disconnected) |

### Nonce Anti-Replay

The server tracks handshake nonces to prevent replay attacks:

| Metric | Description |
|--------|-------------|
| `nonce_tracker.len()` | Number of nonces currently tracked |
| Replay detection | Duplicate nonces within the 5-minute window are rejected with `ERR_AUTH_FAILED` |
| Zero-nonce bypass | `[0; 16]` nonces are always allowed (test/dev mode) |

### Error Code Classification

Native protocol errors carry structured metadata for client-side retry logic:

| Error Code | Name | Retryable | Client Action |
|-----------|------|:---------:|---------------|
| 1000 | `SYNTAX_ERROR` | ❌ | Report to user |
| 1001 | `INVALID_PARAM` | ❌ | Report to user |
| 2000 | `NOT_LEADER` | ✅ | Failover to new primary |
| 2001 | `FENCED_EPOCH` | ✅ | Refresh topology, reconnect |
| 2002 | `READ_ONLY` | ✅ | Route to primary |
| 2003 | `SERIALIZATION_CONFLICT` | ✅ | Retry transaction |
| 3000 | `INTERNAL_ERROR` | ❌ | Log and report |
| 3001 | `TIMEOUT` | ✅ | Retry with backoff |
| 3002 | `OVERLOADED` | ✅ | Retry with backoff |
| 4000 | `AUTH_FAILED` | ❌ | Check credentials |
| 4001 | `PERMISSION_DENIED` | ❌ | Check grants |

### Compression

When LZ4 compression is negotiated (via `FEATURE_COMPRESSION_LZ4` flag), payloads larger than 256 bytes are compressed. The compression tag byte in the frame header indicates whether the payload is compressed:

- `0x00` — uncompressed
- `0x01` — LZ4-compressed (first 4 bytes = uncompressed size LE)

See [native_protocol.md](native_protocol.md) and [native_protocol_compat.md](native_protocol_compat.md) for full protocol specification.
