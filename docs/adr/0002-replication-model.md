# ADR-0002: Replication Model (WAL Shipping)

## Status
Accepted (M1)

## Context
Cedar requires primary-replica replication for high availability.
The M1 model must be simple, correct, and extensible to gRPC streaming (M2).

## Decision
- WAL-based replication: primary appends WAL records; replicas apply in LSN order.
- Commit ack = primary WAL durable (Scheme A). Primary does NOT wait for replicas.
- RPO may be > 0 if primary crashes before replica catches up.
- Transport abstracted via `ReplicationTransport` trait (sync) and
  `AsyncReplicationTransport` (async, M2-ready).
- `WalChunk` frames carry `start_lsn`, `end_lsn`, CRC32 checksum.
- Replicas report `applied_lsn`; primary tracks per-replica ack LSNs.

## Consequences
- Simple, low-latency commits on primary (no synchronous replication wait).
- Potential data loss window between primary commit and replica apply.
- Clean upgrade path to M2 gRPC streaming via the async transport trait.
