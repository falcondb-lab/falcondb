# FalconDB Control Plane Guide

## Overview
The control plane separates "control logic" from the "data plane" so that
data nodes do not carry operational complexity. It is an independent process
(`falcon-controller`) that can be deployed in an HA group (3+ nodes).

## Components

### Controller HA Group
- Raft-based leader election among 3+ controller nodes
- `ControllerHAGroup`: manages nodes, terms, leader state
- Heartbeat tracking between controller peers
- Metrics: elections held, leader changes, heartbeats sent/received

### Node Registry
- Data-plane nodes register with the controller on startup
- Heartbeat-based liveness detection: Online → Suspect → Offline
- Configurable thresholds: `suspect_threshold`, `offline_threshold`
- State management: Online, Suspect, Offline, Draining, Joining, Upgrading
- Config version tracking per node

### Dynamic Configuration
- Versioned key-value config store (`ConfigStore`)
- Config changes increment a global version counter
- Nodes report their config version via heartbeat
- Controller identifies nodes needing config updates
- Change history retained for audit

### Consistent Metadata Store
- Raft-replicated key-value store for cluster state
- Five domains: ClusterConfig, ShardPlacement, VersionEpoch, SecurityPolicy, NodeRegistry
- Write operations: Put, Delete, Compare-And-Swap (CAS)
- Read consistency levels: Leader (strong), Any (eventual), BoundedStaleness
- Only the Raft leader can write; followers reject with `NotLeader`

### Shard Placement Manager
- Authoritative shard → node mapping
- Tracks leader and replicas per shard
- Health evaluation: Healthy, UnderReplicated, Migrating, Orphaned
- `remove_node_from_all()` for node departure
- `under_replicated()` for rebalance planning

### Command Dispatcher
- Queues ops commands: Drain, Join, Upgrade, Rebalance, SetConfig, ForceElection
- Each command gets a unique ID
- Lifecycle: Submit → Execute → Complete
- Metrics: accepted, rejected, completed

## Invariants

1. **Controller restart ≠ data-plane interruption**
   - Data nodes cache config and continue serving reads/writes
   - Short controller unavailability does not affect client operations

2. **All falconctl operations go through the controller**
   - No direct node-to-node operational commands
   - Central audit trail for all ops

3. **Metadata never drifts**
   - Raft consensus ensures strong consistency for writes
   - CAS operations prevent concurrent topology corruption

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `suspect_threshold` | 3s | Time without heartbeat before Suspect |
| `offline_threshold` | 10s | Time without heartbeat before Offline |
| `ha_group_size` | 3 | Number of controller nodes |

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `registrations` | Counter | Node registrations |
| `deregistrations` | Counter | Node removals |
| `heartbeats` | Counter | Heartbeats received |
| `state_transitions` | Counter | Node state changes |
| `writes` | Counter | Metadata writes |
| `reads` | Counter | Metadata reads |
| `cas_attempts` / `cas_failures` | Counter | CAS operations |
| `not_leader_rejections` | Counter | Writes rejected (not leader) |
