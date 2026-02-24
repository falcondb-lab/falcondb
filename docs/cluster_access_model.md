# FalconDB Cluster Access Model v1

## Overview

FalconDB v1.0.8 introduces a **formal Cluster Access Model** that defines how clients connect to a FalconDB cluster. The core principle:

> **Clients connect to a Gateway. The Gateway absorbs topology complexity.**

There is **no global unique primary node**. Any gateway-capable node is a valid entry point.

## Gateway Roles

| Role | Accepts Clients | Owns Shards | Use Case |
|------|----------------|-------------|----------|
| `smart_gateway` | Yes | Yes | Default — every node routes + computes |
| `dedicated_gateway` | Yes | No | Routing-only nodes for large clusters |
| `compute_only` | No | Yes | Backend compute nodes behind gateways |

### Configuration

```toml
[gateway]
role = "smart_gateway"          # or "dedicated_gateway" / "compute_only"
max_inflight = 10000            # 0 = unlimited
max_forwarded = 5000            # 0 = unlimited
forward_timeout_ms = 5000
topology_staleness_secs = 30
```

## Gateway Responsibilities

1. **Request Classification** — every request is classified as:
   - `LOCAL_EXEC` — execute on this node (shard leader is local)
   - `FORWARD_TO_LEADER` — forward to the correct shard leader
   - `REJECT_NO_ROUTE` — no known leader for target shard
   - `REJECT_OVERLOADED` — admission control limit exceeded

2. **Topology Cache** — epoch-versioned shard→leader mapping:
   - Leader changes detected via epoch bumps
   - Stale entries invalidated on NOT_LEADER responses
   - Node crash invalidates all entries for that node

3. **Admission Control** — protects against overload:
   - `max_inflight` — total concurrent requests
   - `max_forwarded` — concurrent forwarded requests
   - Exceeded → immediate `REJECT_OVERLOADED` (no queuing)

## Non-Gateway Node Restrictions

Nodes with `role = "compute_only"`:
- Do NOT accept external client connections
- Only receive forwarded requests from gateways
- Are invisible to JDBC clients

## Official Client Promise

- **JDBC/client never needs to know the leader** — the gateway resolves it
- **Leader changes ≠ client disconnection** — gateway updates its cache
- **No blind forwarding** — gateway verifies target node is alive
- **No silent retry** — errors are explicit with retry hints
- **No infinite queuing** — overloaded → immediate reject with backoff hint

## Recommended Deployment Topologies

### Small Cluster (1–3 nodes)

```
[Client] ──→ [Node1: SmartGateway]
              [Node2: SmartGateway]
              [Node3: SmartGateway]
```

All nodes accept clients and own shards. JDBC URL:
```
jdbc:falcondb://node1:5443,node2:5443,node3:5443/falcon
```

### Medium Cluster (4–8 nodes)

```
[Client] ──→ [Node1: SmartGateway]  ──→ [Node3–8: SmartGateway]
              [Node2: SmartGateway]
```

All SmartGateway. Consider 1–2 DedicatedGateway for routing isolation if write-heavy.

### Large Cluster (9+ nodes)

```
[Client] ──→ [GW1: DedicatedGateway] ──→ [Compute1–N: ComputeOnly]
              [GW2: DedicatedGateway]
```

Dedicated gateway tier for routing isolation. Compute nodes handle data only.

## Observability

`GET /admin/status` includes a `smart_gateway` section:

```json
{
  "smart_gateway": {
    "role": "smart_gateway",
    "epoch": 42,
    "requests_total": 150000,
    "local_exec_total": 95000,
    "forward_total": 50000,
    "reject_no_route": 100,
    "reject_overloaded": 50,
    "topology": {
      "shard_count": 8,
      "node_count": 3,
      "cache_hit_rate": 0.998,
      "leader_changes": 5,
      "invalidations": 2
    }
  }
}
```

## Failure Modes

| Failure | Client Impact | Gateway Behavior |
|---------|--------------|-----------------|
| Leader change | None (transparent) | Cache update, re-route |
| Gateway crash | Failover to next seed | Other gateways continue |
| All gateways down | Connection refused | Client exhausts seed list |
| Network partition | Timeout → retry | Reject with TIMEOUT |
