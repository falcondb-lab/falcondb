# FalconDB Enterprise Architecture

## Version
v1.1.0 — Enterprise Control Plane, Security, Lifecycle, Predictable Ops

## One-Line Definition
> FalconDB is a database system acceptable to IT / security departments.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                   CONTROL PLANE (HA)                      │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐         │
│  │ Controller │  │ Controller │  │ Controller │         │
│  │  (Leader)  │  │ (Follower) │  │ (Follower) │         │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘         │
│        └───────────┬────┴───────────────┘                │
│             Raft Consensus Log                           │
│        ┌─────────────────────────┐                       │
│        │ Consistent Metadata     │                       │
│        │ - cluster config        │                       │
│        │ - shard placement       │                       │
│        │ - version epoch         │                       │
│        │ - security policy       │                       │
│        └─────────────────────────┘                       │
└──────────────────────┬───────────────────────────────────┘
                       │ heartbeat / config / commands
          ┌────────────┼────────────┐
          ▼            ▼            ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Data Node 1 │ │  Data Node 2 │ │  Data Node 3 │
│  (Gateway)   │ │  (Gateway)   │ │  (Gateway)   │
│  Shards 0,1  │ │  Shards 2,3  │ │  Shards 4,5  │
└──────────────┘ └──────────────┘ └──────────────┘
        ▲                ▲                ▲
        │   TLS (all links)               │
        └────────────────┴────────────────┘
              Client Connections
```

## Control Plane

### Responsibilities
- **Topology management**: node registration, heartbeat, liveness detection
- **Configuration distribution**: versioned, dynamic config push
- **Version/capability negotiation**: protocol compatibility checks
- **Ops command dispatch**: drain, upgrade, rebalance via controller

### HA Model
- 3-node minimum for production
- Raft-based leader election among controllers
- Controller restart ≠ data-plane interruption
- Data nodes cache config and continue serving when controller is temporarily unavailable

### Consistent Metadata Store
- Raft-replicated key-value store
- Domains: ClusterConfig, ShardPlacement, VersionEpoch, SecurityPolicy, NodeRegistry
- Write path: leader-only, CAS for conflict resolution
- Read consistency: Leader (strong), Any (eventual), BoundedStaleness

## Security Model

### Authentication (AuthN)
- Password (SCRAM-SHA-256, MD5)
- Bearer token (JWT/opaque)
- Mutual TLS (client certificate)
- Account lockout after configurable failed attempts

### Authorization (AuthZ)
- RBAC at three scopes: Cluster, Database, Table
- Superuser bypass for emergency access
- Wildcard resource grants for broad permissions
- Least privilege principle enforced

### Full-Chain TLS
- Client ↔ Gateway
- Gateway ↔ Data Node
- Data Node ↔ Replica
- Data Node ↔ Controller
- Certificate hot-reload without restart
- Enterprise CA support

## Data Protection

### Backup/Restore
- Full backup (data + metadata + cold segments)
- Incremental backup (WAL/change log)
- Point-in-Time Recovery (PITR)
- Storage targets: local filesystem, S3, OSS
- Observable restore with progress tracking

### Audit Log
- Tamper-proof hash chain (HMAC)
- Categories: AuthN, AuthZ, DataAccess, SchemaChange, PrivilegeChange, Ops, Topology, Security, BackupRestore, ConfigChange
- SIEM-ready JSON-lines export
- Severity levels: Info, Warning, Critical

## Operational Automation

### Auto-Rebalance
- Triggered on node join/leave or load imbalance
- Rate-limited (configurable bytes/sec)
- SLA-safe: pauses when latency impact detected
- Concurrent migration limit

### Capacity Planning
- Resource tracking: WAL size, memory, cold storage, shards, connections
- Linear regression forecasting
- Alert levels: OK, Watch, Warning, Critical
- Time-to-exhaustion prediction

### SLO Engine
- Define SLOs: availability, latency p99/p999, durability, replication lag, error rate
- Rolling window evaluation
- Error budget tracking
- Prometheus/OpenTelemetry export

### Incident Timeline
- Auto-correlate failures, leader changes, ops actions, metric anomalies
- Incident lifecycle: create → correlate → resolve
- Root cause and impact tracking

## Admin Console API
- `/admin/cluster` — Cluster overview
- `/admin/nodes` — Node list and details
- `/admin/shards` — Shard map and details
- `/admin/rebalance` — Rebalance status
- `/admin/backups` — Backup history
- `/admin/slo` — SLO evaluations
- `/admin/audit` — Audit log export
- `/admin/incidents` — Incident timeline
- `/admin/capacity` — Capacity forecast
- `/admin/config` — Dynamic configuration
