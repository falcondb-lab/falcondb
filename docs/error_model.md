# FalconDB Error Model

## Overview

All errors in FalconDB are typed as `FalconError` (defined in `falcon_common::error`).
Every error carries an `ErrorKind` classification that drives retry/escalation decisions.

## Error Classification

| Kind | Description | Client Action | PG Severity |
|------|-------------|---------------|-------------|
| `UserError` | Bad input, SQL syntax, permission denied, constraint violation | Do not retry; fix the request | `ERROR` |
| `Retryable` | Write conflict, leader change, epoch mismatch, serialization failure | Retry with back-off; use `retry_after_ms` hint | `ERROR` |
| `Transient` | Timeout, memory pressure, WAL backlog, replication lag | Retry after back-off; reduce load | `ERROR` |
| `InternalBug` | Should never occur; indicates a code defect | Alert on-call; do not retry | `FATAL` |

## Error Type Hierarchy

```
FalconError
├── Storage(StorageError)        — storage layer failures
├── Txn(TxnError)                — transaction lifecycle failures
├── Sql(SqlError)                — SQL parse/bind/semantic errors
├── Protocol(ProtocolError)      — wire protocol / IO errors
├── Execution(ExecutionError)    — query execution errors
├── Cluster(ClusterError)        — shard/node/consensus errors
├── Retryable { reason, shard_id, epoch, leader_hint, retry_after_ms }
├── Transient { reason, retry_after_ms }
├── InternalBug { error_code, message, debug_context }
├── Internal(String)             — legacy; being phased out
└── ReadOnly(String)             — write on read-only node
```

## SQLSTATE Mapping

| FalconError variant | SQLSTATE | Description |
|--------------------|----------|-------------|
| `Sql::Parse` | `42601` | syntax_error |
| `Sql::UnknownTable` | `42P01` | undefined_table |
| `Sql::UnknownColumn` | `42703` | undefined_column |
| `Sql::TypeMismatch` | `42804` | datatype_mismatch |
| `Sql::Unsupported` | `0A000` | feature_not_supported |
| `Sql::AmbiguousColumn` | `42702` | ambiguous_column |
| `Storage::TableNotFound` | `42P01` | undefined_table |
| `Storage::TableAlreadyExists` | `42P07` | duplicate_table |
| `Storage::DuplicateKey` | `23505` | unique_violation |
| `Storage::UniqueViolation` | `23505` | unique_violation |
| `Storage::SerializationFailure` | `40001` | serialization_failure |
| `Storage::MemoryPressure` | `53200` | out_of_memory |
| `Txn::WriteConflict` | `40001` | serialization_failure |
| `Txn::SerializationConflict` | `40001` | serialization_failure |
| `Txn::ConstraintViolation` | `23000` | integrity_constraint_violation |
| `Txn::Timeout` | `57014` | query_canceled |
| `Txn::MemoryPressure` | `53200` | out_of_memory |
| `Txn::WalBacklogExceeded` | `53300` | too_many_connections |
| `Txn::ReplicationLagExceeded` | `57P03` | cannot_connect_now |
| `ReadOnly` | `25006` | read_only_sql_transaction |
| `Protocol::AuthFailed` | `28P01` | invalid_password |
| `Protocol::InvalidMessage` | `08P01` | protocol_violation |
| `Protocol::ConnectionClosed` | `08006` | connection_failure |
| `Execution::DivisionByZero` | `22012` | division_by_zero |
| `Execution::TypeError` | `22000` | data_exception |
| `Retryable` | `40001` | serialization_failure |
| `Transient` | `53000` | insufficient_resources |
| `InternalBug` | `XX000` | internal_error |

## Retry Semantics

### Retryable Errors
- Client SHOULD retry after `retry_after_ms` milliseconds.
- Carry routing hints: `shard_id`, `epoch`, `leader_hint` (optional node address).
- Use exponential back-off with jitter: `min(retry_after_ms * 2^attempt, 5000ms)`.
- Max retries: 5 for write conflicts; 3 for leader changes.

### Transient Errors
- Client MAY retry after `retry_after_ms` milliseconds.
- Indicates resource exhaustion — reduce load before retrying.
- Max retries: 3 with linear back-off.

### Non-Retryable Errors
- `UserError`: Fix the request. Do not retry.
- `InternalBug`: Alert on-call. Do not retry.

## Helper Macros

```rust
use falcon_common::{bail_user, bail_retryable, bail_transient};
use falcon_common::error::ErrorContext;

// User error (SQL/input/permission)
bail_user!("42601", "syntax error near '{}'", token);

// Retryable (leader change, write conflict)
bail_retryable!(shard_id, epoch, "leader changed to {}", new_leader);

// Transient (backpressure, timeout)
bail_transient!(200, "WAL backlog full: {} bytes", backlog_bytes);

// Add context to any Result
some_result.ctx("stage=commit, txn=42, shard=1")?;
some_result.ctx_with(|| format!("stage=commit, txn={}, shard={}", txn_id, shard_id))?;
```

## Error Context Fields

When adding context to errors, use structured key=value format:

```
stage=<parse|bind|plan|execute|commit|abort|wal_append|replicate|recover|failover>
txn=<txn_id>
shard=<shard_id>
session=<session_id>
query=<query_id>
epoch=<epoch>
leader=<node_id>
table=<table_name>
```

## Core Path Invariant

**No `unwrap()`, `expect()`, or `panic!()` in core paths.**

Core paths are defined as:
- Protocol handling (`falcon_protocol_pg`)
- SQL bind/analyze (`falcon_sql_frontend`)
- Planner → Executor (`falcon_planner`, `falcon_executor`)
- Transaction begin/commit/abort (`falcon_txn`)
- WAL append/flush (`falcon_storage`)
- Replication send/apply (`falcon_cluster`)
- Recovery and failover (`falcon_cluster`, `falcon_storage`)

Verification: `./scripts/deny_unwrap.sh`

## Adding New Errors

1. Add variant to the appropriate sub-error enum (`StorageError`, `TxnError`, etc.).
2. Add `ErrorKind` classification in `FalconError::kind()`.
3. Add SQLSTATE mapping in `FalconError::pg_sqlstate()`.
4. Add retry semantics documentation in this file.
5. Add unit test in `falcon_common/src/error.rs` `#[cfg(test)]` block.
