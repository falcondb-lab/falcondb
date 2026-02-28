# ADR-001: Deterministic Commit Guarantee (DCG)

- **Status**: Accepted
- **Date**: 2024-06-15
- **Authors**: FalconDB Core Team

## Context

FalconDB targets financial OLTP workloads where **transaction outcome must be deterministic and provable** — once a client receives an ACK, the transaction must survive any single-node failure, and a failover must never surface a committed transaction that was not ACKed or vice versa.

Most PG-compatible databases offer "durability" via WAL + fsync but do not guarantee deterministic visibility ordering across failover boundaries. This leaves a gap for regulated industries that require bit-exact auditability.

## Decision

Adopt a **Deterministic Commit Guarantee (DCG)** as the core architectural invariant:

1. **WAL-first**: Every mutation is appended to the WAL and fsynced (per `SyncMode`) **before** the corresponding MVCC version becomes visible.
2. **ACK-after-durable**: The client receives a success response only after the WAL record is durable and the MVCC version is visible.
3. **Failover-safe ordering**: On promotion, the replica replays the WAL to the exact durable LSN. No transaction can be visible on the replica that was not durable on the primary.

The state machine is strictly forward:

```
WAL append → WAL fsync → MVCC visible → Client ACK
```

No step may be reordered or skipped.

## Consequences

### Positive
- **Provable correctness**: DCG is verified by 19 `ga_release_gate` tests and 5 FDE failover-determinism tests with quantitative SLA assertions.
- **Regulatory compliance**: Financial auditors can verify that no phantom commits exist after failover.
- **Simplified reasoning**: Developers never need to reason about "visible but not yet durable" states.

### Negative
- **Latency floor**: Every commit incurs at least one fsync (mitigated by group commit batching up to 32 records).
- **No async commit mode**: Unlike PostgreSQL's `synchronous_commit = off`, FalconDB does not offer a "fast but unsafe" mode. This is intentional for the target market.

### Risks
- Group commit batch size (default 32) may need tuning for extreme low-latency workloads.
- Future replication modes (async replica) must carefully document that they relax DCG on the replica side.

## Alternatives Considered

1. **PostgreSQL-style async commit**: Rejected — violates the core value proposition for financial workloads.
2. **Raft-based commit quorum before ACK**: Deferred to multi-node replication (slow-path already uses XA-2PC with Raft).
3. **Client-side retry with idempotency keys**: Complementary but not a substitute for server-side DCG.
