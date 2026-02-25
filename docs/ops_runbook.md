# FalconDB Operations Runbook

> v1.0.9 — Procedures for common operational scenarios.

## Ops Audit Log

Every cluster operation is recorded in the `OpsAuditLog` ring buffer (default 4096 events). Events are queryable via `falconctl ops audit`.

### Event Types

| Event | Trigger | Fields |
|-------|---------|--------|
| `NODE_UP` | Heartbeat restored or node registered | `node_id` |
| `NODE_DOWN` | Failure detector declares DEAD | `node_id` |
| `NODE_SUSPECT` | Heartbeat late | `node_id` |
| `LEADER_ELECTED` | Election resolved | `shard_id`, `leader`, `term` |
| `LEADER_LOST` | Leader declared DEAD | `shard_id`, `old_leader` |
| `DRAIN_STARTED` | `falconctl node drain` | `node_id` |
| `DRAIN_COMPLETED` | All inflight drained, shards transferred | `node_id` |
| `JOIN_STARTED` | `falconctl node join` | `node_id` |
| `JOIN_COMPLETED` | All shards synced, node activated | `node_id` |
| `UPGRADE_STARTED` | `falconctl upgrade apply` | `target_version` |
| `UPGRADE_NODE_COMPLETE` | Single node upgraded | `node_id`, `version` |
| `UPGRADE_COMPLETE` | All nodes upgraded | `version` |
| `CONFIG_CHANGED` | Config key modified | `key`, `old_value`, `new_value` |
| `BACKPRESSURE_CHANGED` | Pressure level transition | `old_level`, `new_level` |
| `CATCHUP_STARTED` | Replica catch-up initiated | `node_id`, `shard_id`, `strategy` |
| `CATCHUP_COMPLETED` | Replica caught up | `node_id`, `shard_id` |

## Scenario: Node Failure

### Automatic Response

1. Failure detector transitions node: ALIVE → SUSPECT → DEAD
2. Election coordinator triggers re-election for affected shards
3. Highest-LSN surviving replica becomes new leader
4. Catch-up coordinator syncs lagging replicas

### Manual Verification

```bash
falconctl cluster status          # Check node states
falconctl cluster health          # Detailed health check
falconctl ops audit --last 20     # Recent events
```

## Scenario: Planned Maintenance (Node Drain)

```bash
# 1. Drain the node (stops new requests, waits for inflight, transfers shards)
falconctl node drain <node_id>

# 2. Monitor drain progress
falconctl node drain-status <node_id>

# 3. Once DRAINED, perform maintenance
# ... (OS patching, hardware, etc.)

# 4. Rejoin the node
falconctl node join <node_id>

# 5. Monitor sync progress
falconctl node join-status <node_id>
```

## Scenario: Rolling Upgrade

```bash
# 1. Check compatibility
falconctl upgrade plan --target-version 1.0.10

# 2. Apply (automated: follower → gateway → leader)
falconctl upgrade apply

# 3. Monitor
falconctl upgrade status

# 4. If a node fails, investigate:
falconctl ops audit --last 10
```

## Scenario: High Latency / Backpressure

```bash
# 1. Check SLO metrics
curl http://localhost:8080/admin/slo

# 2. Check pressure signals
falconctl cluster health

# 3. Identify the signal
#    - CPU > 85%: scale out or optimize queries
#    - Memory > 85%: increase node_limit_bytes or add nodes
#    - WAL backlog > 50MB: check disk I/O, increase group_commit_window
#    - Replication lag > 10K LSN: check network, replica health

# 4. If backpressure is CRITICAL and admission rate is 10%:
#    - Address root cause ASAP
#    - Consider draining slow nodes
#    - Monitor gateway_reject_rate
```

## Scenario: Split-Brain Suspicion

```bash
# 1. Check epoch and terms
falconctl cluster status

# 2. Verify single leader per shard
falconctl cluster health

# 3. If two nodes claim leadership:
#    - The one with lower epoch/term is stale
#    - Stale node will be rejected by epoch fencing
#    - Force-fence if needed: falconctl epoch fence <node_id>
```

## Health Check Thresholds

| Check | Healthy | Degraded | Critical |
|-------|---------|----------|----------|
| Dead nodes | 0 | 1 | ≥ 2 |
| Suspect nodes | 0 | 1–2 | ≥ 3 |
| Pressure level | Normal | Elevated/High | Critical |
| Write availability | > 99.9% | 99–99.9% | < 99% |
| Replication lag | < 1000 | 1000–10000 | > 10000 |
