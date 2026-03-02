# FalconDB Node.js SDK — Compatibility Matrix

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
| TLS | ✅ | ✅ (`ssl: true`) |

### API Coverage

| Feature | Status | Notes |
|---------|--------|-------|
| `connect()` | ✅ | Async connection to single node |
| `Connection.query()` | ✅ | Returns `QueryResult` |
| `Connection.exec()` | ✅ | Returns `number` (rows affected) |
| `Connection.ping()` | ✅ | Native Ping/Pong with timeout |
| `Connection.close()` | ✅ | Graceful disconnect |
| `Connection.commit()` | ✅ | Explicit commit |
| `Connection.rollback()` | ✅ | Explicit rollback |
| `ConnectionPool` | ✅ | Health-checked, min/max size |
| `ConnectionPool.acquire()` | ✅ | With timeout |
| `ConnectionPool.release()` | ✅ | Returns connection to pool |
| `ConnectionPool.withConnection()` | ✅ | Auto acquire/release helper |
| `FailoverConnection` | ✅ | Auto-reconnect on NOT_LEADER/FENCED_EPOCH |
| `FailoverConnection.query()` | ✅ | Automatic failover + retry |
| `FailoverConnection.exec()` | ✅ | Automatic failover + retry |

### HA / Failover

| Feature | Status | Notes |
|---------|--------|-------|
| Seed node rotation | ✅ | Round-robin across seeds |
| NOT_LEADER failover | ✅ | Reconnect to next seed |
| FENCED_EPOCH failover | ✅ | Reconnect to next seed |
| Exponential backoff | ✅ | Configurable base/max delay |
| Serialization retry | ✅ | Same-node retry |
| Timeout retry | ✅ | Same-node retry |

### Node.js Version Support

| Node.js Version | Status |
|----------------|--------|
| Node.js 18+ | ✅ Supported (LTS, `node:test` required) |
| Node.js 16 | ❌ Not supported (`node:test` unavailable) |

### TypeScript

| Feature | Status |
|---------|--------|
| Type definitions (`index.d.ts`) | ✅ |
| Connection config types | ✅ |
| Error class types | ✅ |
| Query result types | ✅ |
| Pool/HA config types | ✅ |

### Dependencies

- **None** — pure Node.js stdlib implementation (no external packages required)

### Known Limitations

1. **No binary params**: Parameters are not yet sent as typed binary values.
2. **No streaming**: All rows are materialized in memory.
3. **No batch protocol**: Use individual `exec()` calls.
4. **No COPY protocol**: Use INSERT for bulk loading.
5. **No prepared statements**: Planned for v0.3.
