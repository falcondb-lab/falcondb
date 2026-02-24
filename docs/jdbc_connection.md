# FalconDB JDBC Connection Guide

## Connection URL Format

```
jdbc:falcondb://host1:port1,host2:port2,host3:port3/dbname?param=value
```

### Components

| Part | Required | Default | Description |
|------|----------|---------|-------------|
| `jdbc:falcondb://` | Yes | — | Protocol prefix |
| `host:port` list | Yes (≥1) | port 5443 | Comma-separated seed gateways |
| `/dbname` | No | `falcon` | Target database |
| `?params` | No | — | Key=value pairs, `&`-separated |

### Examples

```
# Single node
jdbc:falcondb://localhost:5443/mydb

# 3-node cluster
jdbc:falcondb://node1:5443,node2:5443,node3:5443/falcon

# With parameters
jdbc:falcondb://gw1:5443,gw2:5443/falcon?user=admin&password=secret

# Default port and database
jdbc:falcondb://myhost
```

## Connection Semantics

### Host List = Seed Gateway List

The host list is a **seed list**, not a routing list:
- Client connects to **any one** available host
- The connected gateway handles all routing transparently
- Client does NOT need to connect to the shard leader

### Initial Connection

1. Client tries seeds in order: `host1`, then `host2`, then `host3`
2. First successful connection wins
3. All subsequent queries go through that gateway

### Gateway Unavailable

If the active gateway becomes unavailable:
1. Client detects connection failure
2. Switches to next seed in list (round-robin)
3. Continues with the new gateway — no data loss

### Leader Change

When a shard leader changes:
- **Client does NOT need to reconnect**
- Gateway updates its topology cache
- Next request to that shard is re-routed automatically
- If gateway discovers stale route → returns `NOT_LEADER` with new leader hint

## Error Code & Retry Contract

Every error from FalconDB carries a `GatewayErrorCode` with explicit retry semantics.

### Error Codes

| Code | SQLSTATE | Retryable | Delay | Description |
|------|----------|-----------|-------|-------------|
| `NOT_LEADER` | FD001 | Yes | 0ms (immediate) | Leader changed; hint provided |
| `NO_ROUTE` | FD002 | Yes | 100ms | No route to shard; topology updating |
| `OVERLOADED` | FD003 | Yes | 500ms | Gateway at capacity; backoff |
| `TIMEOUT` | FD004 | Yes | 200ms | Request timed out |
| `FATAL` | FD000 | **No** | — | Bad SQL, auth failure, etc. |

### Retry Contract

```
on error:
  if error.code == FATAL:
    return error to application  // DO NOT retry
  if error.code == NOT_LEADER:
    update leader hint if provided
    retry immediately (0ms delay)
  if error.code == NO_ROUTE:
    wait 100ms, retry
  if error.code == OVERLOADED:
    wait 500ms + jitter, retry
  if error.code == TIMEOUT:
    wait 200ms, retry
  
  max_retries = 3  // per-request limit
  if retries exhausted:
    return error to application
```

### Forbidden Client Behaviors

- **No infinite retry loops** — always bounded by max_retries
- **No avalanche retry** — OVERLOADED requires backoff delay
- **No blind reconnect** — NOT_LEADER uses hint, not random reconnect

## Failover Behavior

### Seed List Failover

```
seeds = [node1:5443, node2:5443, node3:5443]
max_failures_per_seed = 3

1. Connect to node1
2. node1 fails 3 times → switch to node2
3. node2 fails 3 times → switch to node3
4. node3 fails 3 times → back to node1
5. If all seeds exhausted → raise ConnectionError
```

### What Triggers Failover

- TCP connection refused / reset
- 3 consecutive request failures on same gateway
- Gateway returns persistent OVERLOADED

### What Does NOT Trigger Failover

- Single transient error (retry on same gateway)
- NOT_LEADER error (route correction, not gateway failure)
- Application-level SQL errors (FATAL)

## Configuration Reference

### Server-Side (falcon.toml)

```toml
[gateway]
role = "smart_gateway"
max_inflight = 10000
max_forwarded = 5000
forward_timeout_ms = 5000
```

### Client-Side (JDBC URL)

```
jdbc:falcondb://host1:5443,host2:5443/falcon?connectTimeout=5000&socketTimeout=30000
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `connectTimeout` | 5000 | Connection timeout (ms) |
| `socketTimeout` | 30000 | Socket read timeout (ms) |
| `user` | — | Username |
| `password` | — | Password |

## Troubleshooting

### "Connection refused" on all seeds

All gateway nodes are down. Check node health:
```
curl http://node1:8080/health
```

### Frequent NOT_LEADER errors

Normal during leader elections. If persistent:
- Check network partitions between nodes
- Verify topology cache staleness setting
- Check `/admin/status` → `smart_gateway.topology.leader_changes`

### OVERLOADED errors

Gateway is at capacity:
- Increase `gateway.max_inflight`
- Add more gateway nodes
- Check `/admin/status` → `smart_gateway.inflight`
