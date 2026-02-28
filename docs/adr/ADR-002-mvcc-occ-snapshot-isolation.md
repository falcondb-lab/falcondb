# ADR-002: MVCC with OCC under Snapshot Isolation

- **Status**: Accepted
- **Date**: 2024-06-20
- **Authors**: FalconDB Core Team

## Context

FalconDB needs a concurrency control mechanism that:

1. Supports high-throughput OLTP with minimal contention on read-heavy workloads.
2. Provides Snapshot Isolation (SI) semantics compatible with PostgreSQL's default `READ COMMITTED` and optional `REPEATABLE READ`.
3. Avoids global locks or latches on the hot read path.
4. Integrates cleanly with the WAL-first DCG invariant (ADR-001).

## Decision

Adopt **Multi-Version Concurrency Control (MVCC)** with **Optimistic Concurrency Control (OCC)** validation at commit time:

### Version Chain Design
- Each row key maps to a `VersionChain` — a singly-linked list of `Version` nodes (newest → oldest).
- `Version.commit_ts` is an `AtomicU64` with `Acquire/Release` ordering: `0` = uncommitted, `MAX` = aborted, otherwise = commit timestamp.
- `VersionChain.fast_commit_ts` is an atomic cache that enables a **zero-lock fast path** when the chain has exactly one committed non-tombstone version (the common case after INSERT). This eliminates 2 cache misses per row on scans.

### Visibility Rules
- **Read Committed**: Return the latest version with `commit_ts > 0 && commit_ts <= read_ts`.
- **Own-write visibility**: A transaction always sees its own uncommitted versions (`created_by == txn_id`).
- **Tombstones**: A `Version` with `data = None` represents a DELETE; visible as "row does not exist".
- **Aborted versions**: Marked with `commit_ts = MAX`, skipped during traversal, cleaned by GC.

### OCC Validation
- At commit time, the read set is validated: if any key in the read set has a `commit_ts > snapshot_ts` by another transaction, the committing transaction aborts (`SerializationFailure`).
- Write-write conflicts are detected eagerly: `has_write_conflict()` checks if the head version is uncommitted by a different transaction.

### Garbage Collection
- Background `GcRunner` computes a safepoint from active transactions and replica timestamps, then truncates version chains below the safepoint.
- GC is WAL-aware: never collects uncommitted (`commit_ts == 0`) versions.
- Adaptive frequency: escalates under memory pressure (Normal → Pressure → Critical).

## Consequences

### Positive
- **Lock-free reads**: `RwLock` on `VersionChain.head` is reader-biased; concurrent scans never block each other.
- **Predictable latency**: OCC avoids deadlocks entirely; conflicts are detected at commit, not at statement execution.
- **Cache-friendly fast path**: `fast_commit_ts` eliminates pointer chasing for the majority of rows (single-version chains).

### Negative
- **Abort rate under contention**: OCC aborts transactions on write-write conflicts rather than queueing them. Acceptable for the target workload (financial OLTP with low contention per key).
- **Memory overhead**: Each version carries ~120 bytes of metadata (Arc, AtomicU64, RwLock, Option pointers). Mitigated by aggressive GC.
- **No pessimistic locking**: `SELECT ... FOR UPDATE` is emulated via OCC validation, not true row locks.

## Alternatives Considered

1. **2PL (Two-Phase Locking)**: Rejected — deadlock detection adds latency variance; incompatible with the zero-lock read path goal.
2. **MVOCC with centralized timestamp oracle**: Rejected — single point of contention; `AtomicU64::fetch_add` is sufficient for single-node.
3. **Serializable Snapshot Isolation (SSI)**: Deferred — SI is sufficient for the MVP; SSI can be layered on top of the existing OCC framework.
