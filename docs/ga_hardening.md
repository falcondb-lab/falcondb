# FalconDB GA Hardening Guide

## Overview
v1.1.1 eliminates all unpredictable behavior. The system must behave the same
at 3 a.m. on day 300 as it did on day 1.

## Crash / Restart Hardening

### Component Lifecycle
Every component follows an explicit state machine:
```
INIT → STARTING → RUNNING → SHUTTING_DOWN → STOPPED
                         └→ FAILED
```

### Startup Order (deterministic)
1. Controller
2. DataNode
3. ReplicationWorker
4. Gateway
5. Compactor
6. SnapshotWorker
7. BackupWorker
8. AuditDrain

Shutdown order is the reverse.

### Recovery Actions
On crash recovery, the system explicitly logs every action taken:
- **WAL Replay**: records replayed, LSN range
- **Checkpoint Restore**: checkpoint LSN
- **Cold Segment Rebuild**: segments rebuilt
- **Replication Resume**: from LSN
- **In-Doubt Txn Resolution**: committed/aborted counts
- **Index Rebuild**: indexes rebuilt

### Shutdown Sentinel
A file-based sentinel detects clean vs crash shutdown:
- On graceful shutdown: sentinel file written
- On startup: if sentinel missing → previous instance crashed
- Sentinel cleared immediately after detection

### Verification
After `kill -9` or power loss, restart must:
- Cluster available (all components RUNNING)
- State consistent (WAL replay deterministic)
- Zero manual intervention required

## Config Change Consistency & Rollback

### Every Config Change Is:
- **Versioned**: monotonically increasing version counter
- **Checksummed**: djb2 hash of key+value
- **Attributed**: who changed it, when
- **Staged**: supports rollout lifecycle

### Staged Rollout
```
STAGED → CANARY → ROLLING_OUT → APPLIED
                              └→ ROLLED_BACK
```

### Rollback
- `rollback(key)` — revert to previous version
- `rollback_to_version(key, version)` — revert to specific version
- Both complete within milliseconds (in-memory operation)

### Guarantee
Bad config on one key never cascades to other keys.
Controller is the single source of truth.

## Resource Leak Zero-Tolerance

### Tracked Resources
| Resource | Metric |
|----------|--------|
| File Descriptors | Count |
| TCP Connections | Count |
| Threads | Count |
| Hot Memory | Bytes |
| Cold Memory | Bytes |
| Cache | Bytes |

### Detection Method
Linear regression over hourly samples:
- Growth rate per hour computed
- R² confidence score
- Alarm if: growth > threshold AND confidence > 0.7

### Default Thresholds
| Resource | Growth/Hour Threshold |
|----------|----------------------|
| FD | 0.5 |
| Thread | 0.2 |
| Hot Memory | 1 MB |

### 72h+ Verification
After 72 hours of continuous operation:
- RSS must be stable (±5%)
- FD count must be stable (±2)
- Thread count must be stable (±1)
