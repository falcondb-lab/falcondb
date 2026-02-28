# FalconDB Go SDK — Compatibility Matrix

## Driver Version: 0.2.0

### Protocol Compatibility

| Feature | Protocol Minor 0 | Protocol Minor 1 |
|---------|-----------------|-----------------|
| Handshake + Auth | ✅ | ✅ |
| Query (SELECT/DML/DDL) | ✅ | ✅ |
| Ping/Pong | ✅ | ✅ |
| Disconnect | ✅ | ✅ |
| Batch Ingest | ❌ | ❌ (planned) |
| Epoch Fencing | ❌ | ✅ |
| Binary Params | ❌ | ❌ (planned) |
| Compression (LZ4) | ❌ | ❌ (planned) |
| TLS | ✅ | ✅ (`TLSConfig != nil`) |

### API Coverage

| Feature | Status | Notes |
|---------|--------|-------|
| `Connect()` | ✅ | Direct connection to single node |
| `Conn.Query()` | ✅ | Returns `*Rows` |
| `Conn.Exec()` | ✅ | Returns `(int64, error)` |
| `Conn.Ping()` | ✅ | Context-aware, native Ping/Pong |
| `Conn.Close()` | ✅ | Graceful disconnect |
| `Rows.Next()` | ✅ | Iterator pattern |
| `Rows.Values()` | ✅ | `[]interface{}` |
| `Rows.Reset()` | ✅ | Rewind cursor |
| `NewPool()` | ✅ | Thread-safe, health-checked |
| `Pool.Acquire()` | ✅ | Context-aware |
| `Pool.Release()` | ✅ | Returns connection to pool |
| `ConnectFailover()` | ✅ | Auto-reconnect on NOT_LEADER/FENCED_EPOCH |
| `FailoverConn.Query()` | ✅ | Automatic failover + retry |
| `FailoverConn.Exec()` | ✅ | Automatic failover + retry |

### HA / Failover

| Feature | Status | Notes |
|---------|--------|-------|
| Seed node rotation | ✅ | Round-robin across seeds |
| NOT_LEADER failover | ✅ | Reconnect to next seed |
| FENCED_EPOCH failover | ✅ | Reconnect to next seed |
| Exponential backoff | ✅ | Configurable base/max delay |
| Serialization retry | ✅ | Same-node retry |
| Timeout retry | ✅ | Same-node retry |
| Context cancellation | ✅ | Respects `context.Context` deadlines |

### Go Version Support

| Go Version | Status |
|-----------|--------|
| Go 1.21+ | ✅ Supported (target, generics required) |
| Go 1.18–1.20 | ❌ Not tested |

### Dependencies

- **None** — pure stdlib implementation (no external packages required)

### Known Limitations

1. **No binary params**: Parameters are not yet sent as typed binary values.
2. **No streaming**: All rows are materialized in memory.
3. **No batch protocol**: Use individual `Exec()` calls.
4. **No `database/sql` driver**: Native API only (planned).
5. **No COPY protocol**: Use INSERT for bulk loading.
