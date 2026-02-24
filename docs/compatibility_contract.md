# FalconDB v1.x Compatibility Contract

> Version: 1.0.0  
> Date: 2026-02-24  
> Status: **P0 Production-Ready**

This document is the normative contract for FalconDB v1.x. Every promise listed
here MUST be true in any release tagged `v1.*`. CI gates enforce this contract;
merges that violate it are blocked.

---

## 1. Cluster Membership

| ID | Promise | Code Reference |
|----|---------|---------------|
| M-1 | `MembershipView.epoch` is monotonically increasing across all mutations | `falcon_cluster::cluster::membership::MembershipManager::bump_epoch` |
| M-2 | Epoch mismatch between coordinator and participant → fail-fast reject | `MembershipManager::validate_epoch` |
| M-3 | Quorum not met → writes rejected (reads allowed with degraded flag) | `MembershipManager::check_write_quorum`, `check_read_allowed` |
| M-4 | Node state transitions are forward-only: Joining→Active→Draining→Leaving→Dead | `NodeState::can_transition_to` |
| M-5 | Every membership mutation logged to event log for observability | `MembershipManager::record_event` |
| M-6 | Heartbeat timeout → node marked Dead automatically | `MembershipManager::detect_dead_nodes` |
| M-7 | Draining node accepts reads but not writes | `NodeState::accepts_writes`, `NodeState::accepts_reads` |

**Test coverage:** 23 unit tests in `falcon_cluster::cluster::membership::tests`

---

## 2. PG SSL/TLS

| ID | Promise | Code Reference |
|----|---------|---------------|
| TLS-1 | If `require_ssl=true`, plaintext connections rejected with FATAL `08P01` | `server.rs` Phase 0 SSL negotiation |
| TLS-2 | Invalid/missing cert/key at startup → fatal boot error (no silent fallback) | `tls::build_tls_acceptor` returns `TlsSetupError` |
| TLS-3 | If TLS is not configured, `SslRequest` gets `N` (backward compatible) | `server.rs` Phase 0, `tls_acceptor.is_none()` branch |
| TLS-4 | TLS upgrade uses tokio-rustls with `ServerConfig::with_no_client_auth` | `tls::build_tls_acceptor` |
| TLS-5 | `PgStream` abstracts plain/TLS transparently for all protocol I/O | `tls::PgStream` implements `AsyncRead + AsyncWrite` |

**Test coverage:** 9 unit tests in `falcon_protocol_pg::tls::tests` + `server::tests`

---

## 3. Result Set Streaming

| ID | Promise | Code Reference |
|----|---------|---------------|
| STREAM-1 | `ChunkedRows::next_batch(limit)` returns at most `limit` rows | `row_stream::ChunkedRows::next_batch` |
| STREAM-2 | After `is_exhausted()` returns true, `next_batch()` returns empty vec | `row_stream::ChunkedRows::next_batch` |
| STREAM-3 | DECLARE CURSOR stores a `CursorStream`; FETCH serves rows in chunks | `handler_session::handle_declare_cursor`, `handle_fetch_cursor` |
| STREAM-4 | MOVE FORWARD/BACKWARD adjusts cursor position without materializing | `CursorStream::advance`, `CursorStream::retreat` |
| STREAM-5 | CLOSE / CLOSE ALL releases cursor resources | `handler_session` CLOSE handling |

**Test coverage:** 11 unit tests in `falcon_executor::row_stream::tests`

---

## 4. Transaction Safety (from v1.0.4)

| ID | Promise | Code Reference |
|----|---------|---------------|
| TS-1 | Every transaction reaches exactly one terminal state | `determinism_hardening::TxnTerminalState` |
| TS-2 | Crash before CP-D (commit-point durable) → rollback | `determinism_hardening::validate_failover_invariants` |
| TS-3 | Crash after CP-D → committed survives recovery | `determinism_hardening::validate_failover_invariants` |
| TS-4 | Resource exhaustion → deterministic SQLSTATE + retry policy | `determinism_hardening::ResourceExhaustionContract` |

---

## 5. Consistency (from CONSISTENCY.md)

| ID | Promise | Code Reference |
|----|---------|---------------|
| FS-1 | No committed transaction may be lost after WAL flush | `wal::WalWriter::flush` |
| FS-2 | No phantom reads within a single transaction | `txn::TxnHandle` snapshot isolation |
| FS-3 | Write-write conflicts detected and aborted | `txn::TxnManager::commit` conflict check |

---

## 6. Wire Protocol

| ID | Promise | Code Reference |
|----|---------|---------------|
| WP-1 | PG wire protocol v3 (version 196608) | `codec::decode_startup` |
| WP-2 | Extended query protocol: Parse/Bind/Describe/Execute/Sync | `server.rs` query loop |
| WP-3 | SCRAM-SHA-256 authentication supported | `server.rs` AuthMethod::ScramSha256 |
| WP-4 | CancelRequest handled on separate connection | `server.rs` Phase 0/1 |
| WP-5 | COPY FROM STDIN sub-protocol | `server.rs` copy handling |

---

## CI Enforcement

The script `scripts/ci_p0_contract_gate.sh` enforces this contract:

1. **Unit tests**: `cargo test -p falcon_cluster` (membership tests)
2. **Unit tests**: `cargo test -p falcon_protocol_pg` (TLS tests)
3. **Unit tests**: `cargo test -p falcon_executor` (streaming tests)
4. **Doc consistency**: grep for contract IDs in code and verify they exist
5. **Full workspace**: `cargo test --workspace` must pass
6. **Clippy**: `cargo clippy --workspace` must pass

---

## Versioning Policy

- **Patch releases** (v1.0.x): Bug fixes only. No contract changes.
- **Minor releases** (v1.x.0): New features allowed. Existing promises MUST NOT be weakened.
- **Major releases** (v2.0.0): Contract may be revised with migration guide.

---

## Out of Scope for v1.x

The following are explicitly NOT promised by this contract:

- New SQL features (window functions beyond current set, recursive CTEs beyond current limit)
- HTAP / columnar store
- LSM-tree storage engine
- Enterprise licensing features
- Multi-region replication
- Raft-based consensus (streaming replication is the supported path)
