# FalconDB Python SDK — Compatibility Matrix

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
| TLS | ✅ | ✅ (`ssl_enabled=True`) |

### API Coverage

| Feature | Status | Notes |
|---------|--------|-------|
| `connect()` | ✅ | Direct connection to single node |
| `Connection.execute()` | ✅ | Shorthand: create cursor + execute |
| `Connection.cursor()` | ✅ | DB-API 2.0 inspired |
| `Connection.commit()` | ✅ | |
| `Connection.rollback()` | ✅ | |
| `Connection.ping()` | ✅ | Native Ping/Pong |
| `Connection.close()` | ✅ | Graceful disconnect |
| `Cursor.execute()` | ✅ | |
| `Cursor.executemany()` | ✅ | Client-side loop |
| `Cursor.fetchone()` | ✅ | |
| `Cursor.fetchmany()` | ✅ | |
| `Cursor.fetchall()` | ✅ | |
| `Cursor.description` | ✅ | DB-API 2.0 compatible |
| `Cursor.__iter__` | ✅ | Iterate over rows |
| `ConnectionPool` | ✅ | Thread-safe, health-checked |
| `FailoverConnection` | ✅ | Auto-reconnect on NOT_LEADER/FENCED_EPOCH |

### HA / Failover

| Feature | Status | Notes |
|---------|--------|-------|
| Seed node rotation | ✅ | Round-robin across seeds |
| NOT_LEADER failover | ✅ | Reconnect to next seed |
| FENCED_EPOCH failover | ✅ | Reconnect to next seed |
| Exponential backoff | ✅ | Configurable base/max delay |
| Serialization retry | ✅ | Same-node retry |
| Timeout retry | ✅ | Same-node retry |

### Error Hierarchy

| Exception | Error Codes | Retryable |
|-----------|------------|-----------|
| `ProgrammingError` | 1000–1999 | ❌ |
| `OperationalError` | 2000–2999 | ✅ partial |
| `InternalError` | 3000–3999 | ✅ partial |
| `AuthenticationError` | 4000–4001 | ❌ |

### Python Version Support

| Python Version | Status |
|---------------|--------|
| Python 3.9+ | ✅ Supported (target) |
| Python 3.8 | ❌ Not tested |
| Python 3.13+ | ✅ Compatible |

### Dependencies

- **None** — pure stdlib implementation (no external packages required)

### Known Limitations

1. **No binary params**: Parameters are not yet sent as typed binary values.
2. **No streaming**: All rows are materialized in memory.
3. **No batch protocol**: `executemany()` loops client-side.
4. **No async support**: Synchronous I/O only (asyncio planned).
5. **No COPY protocol**: Use INSERT for bulk loading.
