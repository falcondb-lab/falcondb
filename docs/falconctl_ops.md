# falconctl Ops Command Reference

> v1.0.9 — Cluster operations CLI for FalconDB.

## Overview

`falconctl` provides subcommands for cluster management, node lifecycle, rolling upgrades, and operational diagnostics. All commands are non-destructive reads unless explicitly stated.

## Cluster Commands

### `falconctl cluster status`

Show cluster-wide status summary.

```
$ falconctl cluster status
Cluster: FalconDB v1.0.9
Epoch:   42
Term:    17
Nodes:   3 alive, 0 suspect, 0 dead, 0 draining, 0 joining
Shards:  8 total, 8 with leader
SLO:     write_avail=99.97%  read_avail=99.99%  write_p99=12.3ms  repl_lag=42
```

### `falconctl cluster health`

Detailed health assessment with per-node breakdown.

```
$ falconctl cluster health
Overall: HEALTHY
Pressure: NORMAL (admission_rate=100%)

Nodes:
  Node 1  ALIVE   leader(shard 0,1)   LSN=45000  uptime=3d12h  v1.0.9
  Node 2  ALIVE   follower(shard 0,1) LSN=44998  uptime=3d12h  v1.0.9
  Node 3  ALIVE   follower(shard 0,1) LSN=44995  uptime=3d12h  v1.0.9

Active Operations: 0 elections, 0 catch-ups, 0 drains, 0 joins
```

## Node Commands

### `falconctl node drain <node_id>`

Gracefully drain a node. Phases: RejectingNew → DrainingInflight → TransferringShards → Drained.

```
$ falconctl node drain 3
Draining node 3...
  Phase: REJECTING_NEW
  Inflight remaining: 42
  Shards to transfer: [0, 1]
```

### `falconctl node drain-status <node_id>`

Check drain progress.

```
$ falconctl node drain-status 3
Node 3: TRANSFERRING_SHARDS
  Inflight: 0
  Shards transferred: [0] (1/2)
  Elapsed: 12s
```

### `falconctl node join <node_id>`

Join a node into the cluster. Triggers data sync for assigned shards.

```
$ falconctl node join 5
Joining node 5...
  Phase: SYNCING
  Shards assigned: [2, 3]
  Shards synced: 0/2
```

### `falconctl node join-status <node_id>`

Check join progress.

```
$ falconctl node join-status 5
Node 5: READY
  Shards synced: [2, 3] (2/2)
  Elapsed: 45s
  Ready to activate.
```

## Upgrade Commands

### `falconctl upgrade plan --target-version <version>`

Validate compatibility and show upgrade plan.

```
$ falconctl upgrade plan --target-version 1.0.10
Protocol: 1.9 → 1.10 (compatible ✅)
Upgrade order:
  1. Node 2 (follower)  1.0.9 → 1.0.10  [PENDING]
  2. Node 4 (gateway)   1.0.9 → 1.0.10  [PENDING]
  3. Node 1 (leader)    1.0.9 → 1.0.10  [PENDING]
```

### `falconctl upgrade apply`

Execute the upgrade plan.

```
$ falconctl upgrade apply
Upgrading Node 2 (follower)... DRAINING → UPGRADING → REJOINING → COMPLETE ✅
Upgrading Node 4 (gateway)...  DRAINING → UPGRADING → REJOINING → COMPLETE ✅
Upgrading Node 1 (leader)...   DRAINING → UPGRADING → REJOINING → COMPLETE ✅
Upgrade complete: 3/3 nodes at v1.0.10
```

### `falconctl upgrade status`

Check in-progress upgrade.

```
$ falconctl upgrade status
Progress: 2/3 nodes
  Node 2 (follower)  COMPLETE
  Node 4 (gateway)   COMPLETE
  Node 1 (leader)    REJOINING
```

## Ops Commands

### `falconctl ops audit [--last N]`

Show recent ops audit events.

```
$ falconctl ops audit --last 5
ID  TIMESTAMP            EVENT                           INITIATOR
5   2024-06-10T12:00:05  LEADER_ELECTED shard=0 node=2  election_coordinator
4   2024-06-10T12:00:03  LEADER_LOST shard=0 old=1      election_coordinator
3   2024-06-10T12:00:02  NODE_DOWN node=1                failure_detector
2   2024-06-10T11:55:00  NODE_SUSPECT node=1             failure_detector
1   2024-06-10T10:00:00  NODE_UP node=1                  system
```

### `falconctl ops slo`

Show current SLO metrics (equivalent to `GET /admin/slo`).

```
$ falconctl ops slo
Write Availability:  99.97%
Read Availability:   99.99%
Write P99:           15.2ms
Read P99:            4.8ms
Replication Lag:     42 LSN
Gateway Reject Rate: 0.2%
Window:              60s
```

## OpsCommand Enum

The following commands are modeled in the `OpsCommand` enum:

| Command | Description |
|---------|-------------|
| `ClusterStatus` | Cluster-wide summary |
| `ClusterHealth` | Detailed health with per-node info |
| `NodeDrain(NodeId)` | Initiate graceful drain |
| `NodeJoin(NodeId)` | Initiate node join |
| `UpgradePlan` | Validate and show upgrade plan |
| `UpgradeApply` | Execute upgrade |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Command failed (see stderr) |
| 2 | Cluster unhealthy (health check commands) |
| 3 | Upgrade incompatible |
