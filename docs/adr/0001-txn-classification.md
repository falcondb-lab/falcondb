# ADR-0001: Transaction Classification (LocalTxn / GlobalTxn)

## Status
Accepted (M1)

## Context
Falcon needs to distinguish single-shard transactions (fast-path) from
cross-shard transactions (slow-path / 2PC) to optimize commit latency.

## Decision
- Every transaction carries a `TxnContext` with `TxnType` (Local / Global)
  and `TxnPath` (Fast / Slow).
- `LocalTxn` must involve exactly one shard and use the fast-path commit.
- `GlobalTxn` must not use the fast-path; it goes through XA-2PC.
- Hard invariants are validated at commit time via
  `TxnContext::validate_commit_invariants()`. Violations return
  `InvariantViolation` errors (not debug_assert).

## Consequences
- Single-shard operations get minimal commit latency (no 2PC overhead).
- Cross-shard operations pay the 2PC cost but maintain correctness.
- Mis-classification is caught at commit time, preventing silent corruption.
