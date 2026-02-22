# FalconDB Native Protocol — Compatibility Matrix

## Version Negotiation

| Client Major | Server Major | Behavior |
|-------------|-------------|----------|
| 0 | 0 | Compatible. Use `min(client_minor, server_minor)`. |
| 0 | 1+ | Server rejects with `UnsupportedVersion`. |
| 1+ | 0 | Server rejects with `UnsupportedVersion`. |
| N | N | Compatible. Minor negotiation applies. |
| N | M (N≠M) | **Rejected.** Major version mismatch is always a hard error. |

## Minor Version Negotiation

When major versions match, the effective minor version is `min(client_minor, server_minor)`.
Features introduced in a higher minor version are unavailable if either side is lower.

| Minor | Features Added |
|-------|---------------|
| 0 | Base protocol: handshake, auth, query, error, ping/pong, disconnect |
| 1 | Batch ingest, epoch fencing, feature flags, binary params |

## Feature Flags

Feature flags are negotiated during handshake. The server returns the **intersection**
of client-requested and server-supported flags. If a client requests a feature the
server doesn't support, that feature is silently disabled (no error).

| Flag | Bit | Since Minor | Description |
|------|-----|-------------|-------------|
| COMPRESSION_LZ4 | 0 | — | LZ4 payload compression (not yet implemented) |
| COMPRESSION_ZSTD | 1 | — | ZSTD payload compression (not yet implemented) |
| BATCH_INGEST | 2 | 1 | Batch insert protocol |
| PIPELINE | 3 | 1 | Pipelined (concurrent in-flight) requests |
| EPOCH_FENCING | 4 | 1 | Per-request epoch validation |
| TLS | 5 | — | TLS transport (not yet implemented) |
| BINARY_PARAMS | 6 | 1 | Binary parameter encoding in queries |

## Breaking Change Policy

- **Major bump** (e.g. 0→1): May change framing, message layout, type IDs, or remove messages.
  Old clients **MUST** be rejected immediately at handshake.
- **Minor bump** (e.g. 0.1→0.2): May add new message types, new fields at the end of
  existing messages, new feature flags, new error codes. Old clients continue to work
  (unknown fields are ignored via length-prefix framing).

## Driver ↔ Server Compatibility

| Driver Version | Server 0.x | Server 1.x |
|---------------|-----------|-----------|
| JDBC 0.1.x | ✅ Full | ❌ Rejected |
| JDBC 1.0.x | ❌ Rejected | ✅ Full |

## Graceful Degradation

When a client connects to a server with a higher minor version:
1. Server returns its `(major, minor)` in ServerHello.
2. Client uses `min(client_minor, server_minor)` as the effective version.
3. Features from higher minor versions are unavailable but no error occurs.
4. Client SHOULD log a warning suggesting upgrade.

When a client connects to a server with a lower minor version:
1. Same negotiation applies.
2. Client MUST NOT send messages or use features from its higher minor version.

## Error Handling

| Scenario | Error Code | Client Action |
|----------|-----------|---------------|
| Major version mismatch | `UnsupportedVersion` in ErrorResponse | Abort connection. Log error. |
| Auth failure | `AuthFail` message | Abort. Do not retry with same credentials. |
| Epoch mismatch | `FENCED_EPOCH` (2001) | Refresh topology, reconnect to new primary, retry. |
| Not leader | `NOT_LEADER` (2000) | Refresh topology, reconnect to new primary, retry. |
| Read-only node | `READ_ONLY` (2002) | Route to primary or wait for promotion. |
| OCC conflict | `SERIALIZATION_CONFLICT` (2003) | Retry transaction from beginning. |
