# FalconDB Native Protocol Specification v0

## Overview

The FalconDB Native Protocol is a binary, length-prefixed, little-endian protocol
designed for high-throughput batch ingest and low tail-latency OLTP workloads.
It coexists with the PostgreSQL wire protocol and provides:

- Binary column encoding (no text serialization overhead)
- Pipelined request/response (multiple in-flight requests)
- HA-aware failover with epoch fencing
- Version negotiation and feature flags for forward compatibility

## 1. Framing

Every message is framed as:

```
+--------+--------+------------------+
| msg_type (u8)   | length (u32 LE)  | payload (length bytes) |
+--------+--------+------------------+
```

- **msg_type**: 1-byte message type tag
- **length**: 4-byte little-endian payload length (NOT including the 5-byte header)
- **payload**: `length` bytes of message-specific data
- **Max frame size**: 64 MiB (67,108,864 bytes). Frames exceeding this MUST be rejected.
- **Byte order**: All multi-byte integers are **little-endian**.

## 2. Message Types

| Tag | Name | Direction | Description |
|-----|------|-----------|-------------|
| 0x01 | ClientHello | C→S | Handshake initiation |
| 0x02 | ServerHello | S→C | Handshake response |
| 0x03 | AuthRequest | S→C | Authentication challenge |
| 0x04 | AuthResponse | C→S | Authentication credentials |
| 0x05 | AuthOk | S→C | Authentication success |
| 0x06 | AuthFail | S→C | Authentication failure |
| 0x10 | QueryRequest | C→S | SQL query execution |
| 0x11 | QueryResponse | S→C | Query result (schema + rows) |
| 0x12 | ErrorResponse | S→C | Error with code + message |
| 0x13 | BatchRequest | C→S | Batch parameter sets |
| 0x14 | BatchResponse | S→C | Batch execution results |
| 0x20 | Ping | C→S | Keepalive ping |
| 0x21 | Pong | S→C | Keepalive pong |
| 0x30 | Disconnect | C→S | Graceful close |
| 0x31 | DisconnectAck | S→C | Close acknowledged |
| 0xFE | StartTls | C→S | TLS upgrade request |
| 0xFF | StartTlsAck | S→C | TLS upgrade accepted |

## 3. Handshake State Machine

```
Client                          Server
  |                               |
  |--- ClientHello -------------->|
  |                               |
  |<-- ServerHello / AuthRequest -|
  |                               |
  |--- AuthResponse ------------->|
  |                               |
  |<-- AuthOk / AuthFail ---------|
  |                               |
  |=== Session Ready =============|
```

### 3.1 ClientHello

```
protocol_version_major: u16 LE
protocol_version_minor: u16 LE
feature_flags:          u64 LE
client_name_len:        u16 LE
client_name:            [u8; client_name_len]  (UTF-8)
database_len:           u16 LE
database:               [u8; database_len]     (UTF-8)
user_len:               u16 LE
user:                   [u8; user_len]         (UTF-8)
nonce:                  [u8; 16]               (client random for anti-replay)
num_params:             u16 LE
params:                 repeated { key_len: u16, key: [u8], val_len: u16, val: [u8] }
```

### 3.2 ServerHello

```
protocol_version_major: u16 LE
protocol_version_minor: u16 LE
feature_flags:          u64 LE
server_epoch:           u64 LE
server_node_id:         u64 LE
server_nonce:           [u8; 16]
num_params:             u16 LE
params:                 repeated { key_len: u16, key: [u8], val_len: u16, val: [u8] }
```

### 3.3 Feature Flags (u64 bitset)

| Bit | Name | Description |
|-----|------|-------------|
| 0 | COMPRESSION_LZ4 | LZ4 compression supported |
| 1 | COMPRESSION_ZSTD | ZSTD compression supported |
| 2 | BATCH_INGEST | Batch insert protocol |
| 3 | PIPELINE | Pipelined requests |
| 4 | EPOCH_FENCING | Per-request epoch validation |
| 5 | TLS | TLS transport |
| 6 | BINARY_PARAMS | Binary parameter encoding |

## 4. Authentication

### 4.1 AuthRequest

```
auth_method: u8    (0 = password, 1 = token, 2 = scram-sha-256)
challenge:   [u8; remaining]  (method-specific challenge data)
```

### 4.2 AuthResponse

```
auth_method: u8
credential:  [u8; remaining]  (method-specific credential)
```

For password auth: credential = UTF-8 password bytes.
For token auth: credential = opaque token bytes.

## 5. Query Protocol

### 5.1 QueryRequest

```
request_id:     u64 LE
epoch:          u64 LE        (0 if epoch fencing disabled)
sql_len:        u32 LE
sql:            [u8; sql_len] (UTF-8)
num_params:     u16 LE
params:         repeated { type_id: u8, value: encoded_datum }
session_flags:  u32 LE       (bit 0 = autocommit, bit 1 = read_only)
```

### 5.2 QueryResponse

```
request_id:     u64 LE
num_columns:    u16 LE
columns:        repeated {
    name_len:   u16 LE
    name:       [u8; name_len]  (UTF-8)
    type_id:    u8
    nullable:   u8              (0 = not null, 1 = nullable)
    precision:  u16 LE          (0 if N/A)
    scale:      u16 LE          (0 if N/A)
}
num_rows:       u32 LE
rows:           repeated {
    null_bitmap: [u8; ceil(num_columns / 8)]
    values:      repeated { encoded_datum (only for non-null columns) }
}
rows_affected:  u64 LE
```

### 5.3 ErrorResponse

```
request_id:     u64 LE
error_code:     u32 LE
sqlstate:       [u8; 5]       (ASCII SQLSTATE)
retryable:      u8            (0 = no, 1 = yes)
server_epoch:   u64 LE        (current server epoch, for fencing)
message_len:    u16 LE
message:        [u8; message_len]  (UTF-8)
```

### 5.4 Error Codes

| Code | Name | Retryable | Description |
|------|------|-----------|-------------|
| 1000 | SYNTAX_ERROR | No | SQL syntax error |
| 1001 | INVALID_PARAM | No | Invalid parameter |
| 2000 | NOT_LEADER | Yes | Node is not the primary |
| 2001 | FENCED_EPOCH | Yes | Epoch mismatch (stale write) |
| 2002 | READ_ONLY | Yes | Node is read-only |
| 2003 | SERIALIZATION_CONFLICT | Yes | OCC conflict |
| 3000 | INTERNAL_ERROR | No | Server internal error |
| 3001 | TIMEOUT | Yes | Query timeout |
| 3002 | OVERLOADED | Yes | Server overloaded |
| 4000 | AUTH_FAILED | No | Authentication failed |
| 4001 | PERMISSION_DENIED | No | Insufficient privileges |

## 6. Batch Protocol

### 6.1 BatchRequest

```
request_id:     u64 LE
epoch:          u64 LE
sql_len:        u32 LE
sql:            [u8; sql_len]   (SQL template with ? placeholders)
num_columns:    u16 LE          (params per row)
column_types:   [u8; num_columns]  (type_id per column)
num_rows:       u32 LE
rows:           repeated {
    null_bitmap: [u8; ceil(num_columns / 8)]
    values:      repeated { encoded_datum }
}
options:        u32 LE          (bit 0 = continue_on_error)
```

### 6.2 BatchResponse

```
request_id:     u64 LE
num_counts:     u32 LE
counts:         [i64 LE; num_counts]   (rows affected per statement, -1 = error)
has_error:      u8
error:          (optional ErrorResponse payload if has_error = 1)
```

## 7. Type IDs and Binary Encoding

| type_id | DataType | Encoding |
|---------|----------|----------|
| 0x00 | Null | (no bytes — indicated by null bitmap) |
| 0x01 | Boolean | 1 byte (0 = false, 1 = true) |
| 0x02 | Int32 | 4 bytes LE |
| 0x03 | Int64 | 8 bytes LE |
| 0x04 | Float64 | 8 bytes LE (IEEE 754) |
| 0x05 | Text | u32 LE length + UTF-8 bytes |
| 0x06 | Timestamp | 8 bytes LE (microseconds since epoch) |
| 0x07 | Date | 4 bytes LE (days since epoch) |
| 0x08 | Jsonb | u32 LE length + UTF-8 JSON bytes |
| 0x09 | Decimal | 1 byte scale + 16 bytes LE i128 mantissa |
| 0x0A | Time | 8 bytes LE (microseconds since midnight) |
| 0x0B | Interval | 4 bytes LE months + 4 bytes LE days + 8 bytes LE microseconds |
| 0x0C | Uuid | 16 bytes (big-endian, RFC 4122) |
| 0x0D | Bytea | u32 LE length + raw bytes |
| 0x0E | Array | u8 elem_type_id + u32 LE count + repeated encoded_datum |

## 8. Version Negotiation

- Client sends `(major, minor)` in ClientHello.
- Server responds with its `(major, minor)`.
- **Major mismatch**: connection MUST be rejected with `UnsupportedVersion`.
- **Minor mismatch**: server uses `min(client_minor, server_minor)`.
- Current version: **major=0, minor=1**.

## 9. TLS Upgrade

If TLS is required:
1. Client sends `StartTls` message before `ClientHello`.
2. Server responds with `StartTlsAck`.
3. Both sides perform TLS handshake on the raw socket.
4. Client then sends `ClientHello` over the TLS channel.

## 10. Compression

When compression is negotiated via `FEATURE_COMPRESSION_LZ4` (bit 0) in the handshake feature flags, message payloads may be compressed. Compressed frames use an extended format:

```
+----------+-----------+----------------+-------------------+
| msg_type | length    | compress_tag   | payload           |
| (u8)     | (u32 LE)  | (u8)           | (length-1 bytes)  |
+----------+-----------+----------------+-------------------+
```

- **compress_tag = 0x00**: payload is uncompressed (identical to standard framing)
- **compress_tag = 0x01**: payload is LZ4-compressed
  - First 4 bytes of payload: uncompressed size (u32 LE)
  - Remaining bytes: compressed data

### Negotiation Rules

1. Client advertises `FEATURE_COMPRESSION_LZ4` in `ClientHello.feature_flags`.
2. Server includes `FEATURE_COMPRESSION_LZ4` in `ServerHello.feature_flags` only if it supports it.
3. Compression is active only if both sides agree (intersection of flags).
4. Payloads smaller than **256 bytes** SHOULD NOT be compressed (overhead exceeds savings).
5. If compression doesn't reduce size, the sender MUST use `compress_tag = 0x00`.

### Supported Algorithms

| Tag | Algorithm | Status |
|-----|-----------|--------|
| 0x00 | None | Always supported |
| 0x01 | LZ4 | Supported (feature flag bit 0) |
| 0x02 | ZSTD | Reserved (feature flag bit 1, not yet implemented) |

## 11. Nonce Anti-Replay

The `ClientHello.nonce` field (16 bytes) prevents handshake replay attacks:

1. Server records each nonce in a sliding time window (default: 5 minutes, 10K max entries).
2. If a nonce has been seen before within the window, the server rejects the handshake with `ERR_AUTH_FAILED` and message `"nonce replay detected"`.
3. **Special case**: all-zero nonce `[0x00; 16]` is always allowed without recording (test/dev mode).
4. Expired nonces are evicted automatically. If the tracker reaches capacity, the oldest entry is evicted.

## 12. Ping/Pong

- **Ping**: empty payload. Client sends periodically for keepalive.
- **Pong**: empty payload. Server responds immediately.
