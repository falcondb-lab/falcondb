# ADR-006: In-Memory Row Store Format

- **Status**: Accepted
- **Date**: 2024-07-10
- **Authors**: FalconDB Core Team

## Context

FalconDB is an in-memory OLTP database. The storage format directly impacts
query latency, memory efficiency, and GC overhead. The primary question is
whether to use a row-oriented or columnar format, and how to represent rows
in memory.

### Options Considered

| Option | Pros | Cons |
|--------|------|------|
| **Row store (Datum vec)** | Simple; cache-friendly for point lookups; natural for OLTP writes | Less efficient for analytical scans; per-row overhead |
| **Columnar (Arrow/DataFusion)** | Vectorized scan performance; compression | Write amplification on updates; complex MVCC integration |
| **PAX (hybrid)** | Best of both for mixed workloads | Implementation complexity; unclear win for pure OLTP |
| **Heap + slotted page** | Classic DBMS; proven | Unnecessary for in-memory; page management overhead |

## Decision

Adopt a **row-oriented in-memory store** using `OwnedRow` (a `Vec<Datum>`) as
the primary tuple representation:

1. **OwnedRow**: a row is `Vec<Datum>` where `Datum` is an enum of typed values
   (`Int32`, `Int64`, `Float64`, `Text`, `Boolean`, `Timestamp`, `Date`,
   `Decimal`, `Jsonb`, `Array`, `Null`).

2. **DashMap**: the primary index is `DashMap<PkBytes, VersionChain>` —
   a concurrent hash map with per-shard locking for O(1) point lookups.

3. **PK encoding**: primary key columns are serialized to `PkBytes` (a `Vec<u8>`)
   via `encode_pk`, which produces a byte-comparable encoding for composite keys.

4. **Secondary indexes**: BTreeMap-based secondary indexes map
   `(index_key_bytes → Set<PkBytes>)` for range scans and unique constraints.

5. **No page abstraction**: being fully in-memory, there is no page/buffer pool
   layer. Rows are heap-allocated Rust structs with standard allocator semantics.

## Consequences

### Positive

- Point lookups (GET by PK) are a single DashMap probe — O(1), lock-free reads.
- Row-at-a-time execution is natural for OLTP INSERT/UPDATE/DELETE patterns.
- MVCC version chains are per-row, enabling fine-grained concurrency.
- `Datum` enum is self-describing — no separate schema lookup needed per access.
- Memory layout is Rust-native; no unsafe pointer arithmetic.

### Negative

- Full table scans iterate all DashMap shards — less cache-friendly than columnar.
- `Datum::Text` stores `String` inline — large text values increase row size.
- No compression (acceptable for in-memory OLTP; disk-backed LSM layer uses
  zstd/lz4 for cold data).

### Mitigations

- **Fast-path aggregates**: `compute_simple_aggs` and `exec_fused_aggregate`
  perform single-pass computation over version chains without materializing rows.
- **Zero-copy access**: `with_visible_data` provides closure-based row access
  to avoid cloning.
- **LSM offload**: cold data can be flushed to disk-backed `LsmEngine` with
  `falcon_segment_codec` (lz4/zstd compressed segments).

## Datum Layout

```rust
pub enum Datum {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    Timestamp(i64),      // microseconds since epoch
    Date(i32),           // days since epoch
    Decimal(i128, u8),   // (unscaled_value, scale)
    Jsonb(serde_json::Value),
    Array(Vec<Datum>),
}
```

`Datum` is `Clone + PartialEq + PartialOrd` with type-aware comparison and
arithmetic operations (`add`, `sub`, `mul`, `div`).

## Index Architecture

| Index Type | Implementation | Use Case |
|------------|---------------|----------|
| Primary (hash) | `DashMap<PkBytes, VersionChain>` | Point lookup by PK |
| Secondary (BTree) | `BTreeMap<IndexKey, BTreeSet<PkBytes>>` | Range scan, ORDER BY |
| Unique | Secondary + uniqueness check at commit | UNIQUE constraints |
| Composite | Multi-column `encode_pk` for index key | Multi-column indexes |
| Covering | Index stores full row copy | Index-only scans |

## References

- ARCHITECTURE.md §2.6 (Storage Engine)
- `crates/falcon_common/src/datum.rs` — Datum definition
- `crates/falcon_storage/src/memtable.rs` — MemTable + encode_pk
- `crates/falcon_storage/src/mvcc.rs` — VersionChain
- ADR-002 — MVCC/OCC decision
