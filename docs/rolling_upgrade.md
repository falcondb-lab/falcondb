# FalconDB Rolling Upgrade Guide

> v1.0.9 — Zero-downtime upgrades with protocol compatibility guards.

## Overview

FalconDB supports rolling upgrades where nodes are upgraded one at a time without taking the cluster offline. The upgrade coordinator enforces a strict ordering and protocol version compatibility checks.

## Upgrade Ordering

Nodes are upgraded in this order to minimize disruption:

```
1. Followers   — least disruptive, no client-facing impact
2. Gateways    — brief forwarding interruption, clients retry
3. Leaders     — triggers leader re-election, brief write pause
```

Each node goes through these phases:

```
PENDING → DRAINING → UPGRADING → REJOINING → COMPLETE
                                              or FAILED
```

## Protocol Version Compatibility

FalconDB uses a `ProtocolVersion` (major.minor) for wire compatibility:

- **Same major, minor within 1 step**: compatible (rolling upgrade allowed)
- **Different major**: incompatible (full cluster restart required)
- **Minor > 1 step apart**: incompatible (upgrade in multiple steps)

Example:
| From | To | Compatible? |
|------|----|-------------|
| 1.9 | 1.10 | ✅ Yes |
| 1.9 | 1.11 | ❌ No (2 steps) |
| 1.9 | 2.0 | ❌ No (major change) |

## Upgrade Workflow

### 1. Plan

```
falconctl upgrade plan --target-version 1.0.10
```

This validates protocol compatibility and produces an ordered plan:

```
Node 1 (follower)  → 1.0.9 → 1.0.10  [PENDING]
Node 2 (gateway)   → 1.0.9 → 1.0.10  [PENDING]
Node 3 (leader)    → 1.0.9 → 1.0.10  [PENDING]
```

### 2. Apply

```
falconctl upgrade apply
```

For each node in order:

1. **Drain** — stop accepting new requests, wait for inflight to complete
2. **Upgrade** — replace binary, update config
3. **Rejoin** — start the node, trigger catch-up for any WAL gap
4. **Verify** — health check passes, mark COMPLETE

### 3. Monitor

```
falconctl upgrade status
```

Shows progress: `(2/3) nodes upgraded, Node 3 REJOINING`

## Failure Handling

If a node fails health check after upgrade:

- Node is marked `FAILED` with reason
- Upgrade pauses — operator must investigate
- Rollback: reinstall previous binary, restart node

## Configuration

```toml
[cluster.upgrade]
protocol_version_major = 1
protocol_version_minor = 9
```

## Metrics

| Metric | Description |
|--------|-------------|
| `nodes_upgraded` | Nodes successfully upgraded |
| `nodes_failed` | Nodes that failed upgrade |
| `total_drain_time_ms` | Cumulative drain time |
| `total_rejoin_time_ms` | Cumulative rejoin time |

## Safety Guarantees

1. **No data loss** — WAL catch-up ensures no entries are lost during node restart
2. **No split-brain** — epoch fencing prevents stale nodes from accepting writes
3. **No client errors** — gateway retry handles brief leader unavailability
4. **Rollback-safe** — previous binary can always be reinstalled
