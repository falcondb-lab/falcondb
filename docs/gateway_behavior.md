# FalconDB Smart Gateway Behavior

## Overview

The Smart Gateway is the request router inside every gateway-capable FalconDB node. It classifies, routes, and monitors every client request with zero ambiguity.

## Request Lifecycle

```
Client Request
    │
    ▼
┌─────────────────────┐
│  Admission Control   │──→ REJECT_OVERLOADED (if over limit)
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  Topology Lookup     │──→ REJECT_NO_ROUTE (if shard unknown)
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  Leader == Local?    │
└─────┬─────────┬─────┘
      │ Yes     │ No
      ▼         ▼
 LOCAL_EXEC  ┌──────────────────┐
             │ Target Alive?    │──→ REJECT_NO_ROUTE (if dead)
             └──────┬───────────┘
                    ▼
             FORWARD_TO_LEADER
```

## Classification Rules

### LOCAL_EXEC
- Shard leader is this node
- Execute directly on local storage engine
- No network hop, lowest latency

### FORWARD_TO_LEADER
- Shard leader is another node
- Target node is verified alive (no blind forward)
- Forward slot is available (admission check)
- Tracks forward latency for SLO monitoring

### REJECT_NO_ROUTE
- Shard has no known leader in topology cache
- Target leader node is marked dead
- Topology entry was invalidated (e.g., after NOT_LEADER response)

### REJECT_OVERLOADED
- `max_inflight` limit reached (total concurrent requests)
- `max_forwarded` limit reached (concurrent forward operations)
- **Immediate reject** — no queuing, no waiting

## Behavioral Guarantees

### No Blind Forwarding
Every forward operation checks:
1. Topology cache has an entry for the target shard
2. The leader node is marked alive in the node directory
3. A forwarded slot is available

If any check fails → explicit rejection with appropriate error code.

### No Silent Retry
The gateway never silently retries a failed forward. Instead:
- Returns the error to the client with retry hints
- Client decides whether to retry per the JDBC retry contract
- Prevents hidden latency amplification

### No Infinite Queuing
When limits are exceeded:
- `max_inflight > 0` and current inflight ≥ limit → immediate reject
- `max_forwarded > 0` and current forwarded ≥ limit → immediate reject
- `max_queue_depth = 0` (default) → no pending queue at all

### Leader Switch Transparency
During a leader switch:
1. Gateway receives NOT_LEADER from old leader (or detects via epoch)
2. Invalidates stale cache entry
3. If new leader hint provided → updates cache eagerly
4. Next request re-routes to new leader
5. **Client sees either success or fast explicit failure — never a hang**

## Topology Cache

### Structure
- `shard_id → (leader_node, leader_addr, epoch, last_verified)`
- Epoch is monotonically increasing (bumped on leader changes)
- Thread-safe via RwLock (read-biased for high throughput)

### Invalidation Triggers
| Trigger | Action |
|---------|--------|
| NOT_LEADER response | Invalidate shard entry, update if hint provided |
| Node marked dead | Invalidate all entries pointing to that node |
| Epoch mismatch | Bump epoch, update entry |
| Staleness timeout | Entry flagged for refresh (background) |

### Cache Metrics
- `cache_hits` / `cache_misses` → hit rate
- `leader_changes` → frequency of leader movement
- `invalidations` → topology churn indicator
- `epoch_bumps` → cluster stability indicator

## Admission Control

### Configuration
```toml
[gateway]
max_inflight = 10000      # Total concurrent requests (0 = unlimited)
max_forwarded = 5000       # Concurrent forwards (0 = unlimited)
forward_timeout_ms = 5000  # Forward operation timeout
```

### Behavior Under Load
| Load Level | inflight/max | Behavior |
|-----------|--------------|----------|
| Normal | < 80% | All requests accepted |
| High | 80–99% | Requests accepted, monitor closely |
| Saturated | = 100% | New requests immediately rejected |

### Why No Queuing
Queuing causes **p99 explosion**: a 100ms queue adds 100ms to every request's tail latency. FalconDB chooses fast-fail over slow-success:
- Client retries with backoff → controlled load
- No hidden latency accumulation
- Predictable p99 under all conditions

## Metrics & Observability

### Counters (atomic, lock-free)
| Metric | Description |
|--------|-------------|
| `local_exec_total` | Requests executed locally |
| `forward_total` | Requests forwarded to leader |
| `reject_no_route_total` | Rejections: no route |
| `reject_overloaded_total` | Rejections: overloaded |
| `reject_timeout_total` | Rejections: timeout |
| `forward_failed` | Forward operations that failed |
| `client_connect_total` | Total client connections |
| `client_failover_total` | Client failover events |

### Latency
| Metric | Description |
|--------|-------------|
| `forward_latency_avg_us` | Average forward latency |
| `forward_latency_peak_us` | Peak forward latency |

### Topology
| Metric | Description |
|--------|-------------|
| `epoch` | Current topology epoch |
| `shard_count` | Shards in cache |
| `node_count` | Nodes in directory |
| `cache_hit_rate` | Hit rate (0.0–1.0) |
| `leader_changes` | Total leader changes observed |
| `invalidations` | Total cache invalidations |

All available via `GET /admin/status` → `smart_gateway` section.
