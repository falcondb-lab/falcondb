# FalconDB — Replication Drift & Data Integrity Guard (P1-3)

## Overview

FalconDB provides a **Replication Integrity Checker** that periodically verifies
primary–replica consistency through LSN alignment, row count comparison, and
row-level checksum matching. Drift is detected, logged, and actionable.

## Integrity Checks

| Check | Method | Non-blocking |
|-------|--------|-------------|
| **Row count** | `scan()` on both nodes, compare counts | ✅ Read-only |
| **Checksum** | Sort rows by PK, hash all column values, compare digests | ✅ Read-only |
| **LSN alignment** | Compare `flushed_lsn` on primary vs replica | ✅ Metadata only |

## Drift Detection

### When Drift Occurs

1. **Network partition**: WAL records not shipped during partition window
2. **Replica crash**: Replica loses uncommitted WAL tail on restart
3. **Bug**: Replication logic skips or double-applies a record

### Detection Flow

```
Primary.scan(table) → rows_p, checksum_p
Replica.scan(table) → rows_r, checksum_r

if rows_p ≠ rows_r → DRIFT: row count mismatch
if checksum_p ≠ checksum_r → DRIFT: data mismatch
else → CONSISTENT
```

### Recovery Paths

| Drift Type | Recovery | Automated |
|-----------|----------|-----------|
| Replica behind (row count < primary) | Re-ship missing WAL tail | ✅ `catch_up_replica()` |
| Replica diverged (checksum mismatch) | Full rebuild from snapshot | Manual trigger |
| LSN gap | Identify missing range, re-stream | ✅ Replication protocol |

## Scheduling

The integrity checker can run:

- **On demand**: `SHOW REPLICATION INTEGRITY` (planned)
- **Periodic**: Every N minutes via background task
- **Post-failover**: Automatically after any promote event

## Constraints

- ❌ Does NOT block the primary write path
- ❌ Does NOT introduce latency spikes (read-only scans)
- ✅ Runs concurrently with normal operations
- ✅ Supports online execution

## Metrics

| Metric | Description |
|--------|-------------|
| `falcon_replication_lag_us` | Current replication lag |
| `falcon_replication_leader_changes` | Total leader changes |
| `falcon_replication_promote_count` | Total promotions |

## Tests

| Test | Description |
|------|-------------|
| `empty_cluster_is_consistent` | Empty primary + replica → consistent |
| `after_replication_data_is_identical` | 100 rows replicated → row count + checksum match |
| `drift_detected_when_replica_misses_records` | 10 unshipped rows → drift flagged |
| `integrity_survives_failover` | Promote + new writes → new primary correct |
| `large_dataset_checksum_consistency` | 1000 rows → checksum match |

## Source

- `crates/falcon_cluster/tests/replication_integrity.rs` — 5 integration tests
