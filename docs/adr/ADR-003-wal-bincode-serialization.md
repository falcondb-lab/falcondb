# ADR-003: WAL Record Serialization with bincode

- **Status**: Accepted
- **Date**: 2024-07-10
- **Authors**: FalconDB Core Team

## Context

The Write-Ahead Log is on the critical path of every committed transaction. Serialization format directly impacts:

1. **Commit latency**: Serialization CPU time is part of the fsync-bounded commit path.
2. **WAL volume**: Larger records mean more I/O, faster segment rotation, and higher replication bandwidth.
3. **Recovery speed**: WAL replay deserializes every record; format efficiency affects startup time.

The initial prototype used `serde_json` for WAL records due to debuggability. As the project matured, profiling showed JSON serialization consuming ~8% of commit-path CPU and producing records 3–5× larger than necessary.

## Decision

Use **bincode** (via `serde` derive) as the WAL record serialization format:

1. `WalRecord` enum derives `Serialize, Deserialize` and is serialized with `bincode::serialize()`.
2. All nested types (`TableSchema`, `ColumnDef`, `AlterTableOp`, `AlterRoleOpts`, `OwnedRow`, `Datum`) are serialized natively by bincode — no intermediate JSON strings.
3. On-disk record format: `[len:4 LE][crc32:4 LE][payload:len]` where `payload` is raw bincode (or `nonce(12) || ciphertext+tag` when TDE is active).
4. `WAL_FORMAT_VERSION` is bumped on any backward-incompatible change to `WalRecord`.

### Eliminated Patterns
- `CreateTable { schema_json: String }` → `CreateTable { schema: TableSchema }` — no more `serde_json::to_string` + double serialization.
- `AlterTable { operation_json: String }` → `AlterTable { op: AlterTableOp }` — type-safe enum replaces free-form JSON.
- `AlterRole { options_json: String }` → `AlterRole { opts: AlterRoleOpts }` — structured struct replaces JSON.

## Consequences

### Positive
- **~60% smaller WAL records** for DDL operations (bincode struct vs JSON string-in-bincode).
- **~3× faster serialization** on the commit path (no JSON encode/decode overhead).
- **Type safety**: `AlterTableOp` enum is exhaustively matched at compile time; impossible to write an unknown operation type.
- **Simpler recovery code**: Pattern matching replaces `serde_json::Value` parsing with `unwrap_or("")` fallbacks.

### Negative
- **Not human-readable**: WAL segments cannot be inspected with `cat` or `jq`. Mitigated by `falcon_cli wal dump` command.
- **Version coupling**: Any change to `WalRecord` enum requires a `WAL_FORMAT_VERSION` bump and migration consideration.

### Migration
- `WAL_FORMAT_VERSION` 3 → 4 is a breaking change. Existing v3 WAL files must be consumed by checkpoint before upgrading. The upgrade guide documents this requirement.

## Alternatives Considered

1. **Protocol Buffers (protobuf)**: Higher ecosystem complexity (codegen, `.proto` files); bincode is simpler for Rust-only internal format.
2. **MessagePack**: Similar size to bincode but slower in benchmarks; no significant advantage.
3. **FlatBuffers**: Zero-copy deserialization is appealing but adds complexity for the WAL's append-only sequential access pattern.
4. **Keep JSON for DDL, bincode for DML**: Rejected — mixed formats complicate recovery and replication; consistency is more valuable than marginal debuggability.
