# FalconDB Phase 1 Implementation Plan (→ v1.0)

> **Goal**: Industrial/Financial-grade OLTP kernel  
> **Core**: Deterministic txn semantics, predictable P99, explicit failure behavior  
> **Benchmark**: Objectively faster than vanilla PostgreSQL in high-concurrency short-txn OLTP

---

## Gap Analysis (Current State vs Phase 1 Requirements)

### ✅ Already Have (Reusable)
| Capability | Location | Status |
|-----------|----------|--------|
| MVCC version chains | `falcon_storage::memtable::VersionChain` | ✅ In-memory |
| 2PC prepare/commit/abort | `falcon_txn::TxnManager` | ✅ In-memory |
| WAL writer (fsync) | `falcon_storage::wal::WalWriter` | ✅ Append-only |
| Error model (4-tier) | `falcon_common::error` | ✅ Complete |
| Admission control | `falcon_txn` + `falcon_cluster::admission` | ✅ |
| Crash domain / panic hook | `falcon_common::crash_domain` | ✅ |
| Audit log (DDL/auth events) | `falcon_storage::audit` | ✅ Partial |
| Rebalance / scale-out/in | `falcon_cluster::rebalancer` | ✅ |
| InDoubtResolver | `falcon_cluster::indoubt_resolver` | ✅ |
| DiskRowstoreTable (B-tree) | `falcon_storage::disk_rowstore` | ✅ Basic |

### 🔴 Must Build (Phase 1 Gaps)
| # | Requirement | Gap | Priority |
|---|-----------|-----|----------|
| 1 | **LSM StorageEngine** | No LSM tree; data is in-memory MemTable only | P0 |
| 2 | **MVCC on-disk** | Version chains are in-memory; no persistent txn_meta | P0 |
| 3 | **Memory budget enforcement** | MemoryTracker exists but no flush-to-disk path | P0 |
| 4 | **Group commit + batch fsync** | WAL has group_commit_size but no batching | P1 |
| 5 | **Bloom filter + block cache** | No bloom filter; no LRU block cache | P1 |
| 6 | **Idempotency key** | No idempotency key → txn result mapping | P1 |
| 7 | **Txn-level audit** | Audit log records DDL/auth only, not txn outcomes | P1 |
| 8 | **Benchmark harness** | No pgbench-compatible benchmark; no PG comparison | P1 |
| 9 | **CI over-memory smoke** | No test for data > 2× memory budget | P1 |

---

## Implementation Order

### Sprint 1: LSM Storage Engine Core (P0)
1. `falcon_storage::lsm` module — LSM tree with:
   - `LsmMemTable` — sorted in-memory buffer (skip list or BTreeMap)
   - `LsmWal` — WAL for memtable durability
   - `SstWriter` / `SstReader` — sorted string table (SST) file format
   - `BlockCache` — LRU block cache for SST reads
   - `BloomFilter` — per-SST bloom filter for negative lookups
   - `Compactor` — background L0→L1→Ln leveled compaction
   - `LsmEngine` — top-level engine implementing point get/put/delete/scan
2. `StorageBackend` trait — abstract over `InMemoryEngine` and `LsmEngine`
3. Wire `LsmEngine` as default for `StorageType::Rowstore`

### Sprint 2: MVCC on Disk (P0)
4. Persistent `txn_meta` table: txn_id → {outcome, commit_ts, epoch}
5. MVCC version encoding in SST values: {txn_id, status, commit_ts, data}
6. Prepared versions invisible to readers (visibility rule)
7. GC of aborted/old versions during compaction

### Sprint 3: Performance (P1)
8. Group commit — batch WAL fsync across concurrent writers
9. Bloom filter integration in point lookup path
10. Block cache with configurable size + eviction metrics

### Sprint 4: Idempotency + Audit (P1)
11. `IdempotencyStore` — persistent key→txn_result mapping with TTL/GC
12. Transaction-level audit: txn_id, affected_keys, outcome, timestamp

### Sprint 5: Benchmark + CI (P1)
13. `falcon_bench` crate — pgbench TPC-B, KV point ops, write-heavy
14. CI gates: over-memory smoke, kill-9 recovery, benchmark sanity

---

## Key Design Decisions

### LSM vs B-tree
LSM chosen because:
- Write-optimized (sequential I/O for WAL + flush)
- Natural backpressure (memtable size → flush → L0 count → stall)
- Compaction amortizes write amplification
- Bloom filters eliminate most negative lookups

### MVCC Encoding
Each SST value encodes: `[txn_id:8][status:1][commit_ts:8][data_len:4][data...]`
- Status: 0=Prepared, 1=Committed, 2=Aborted
- Prepared versions are skipped by readers
- Compaction removes Aborted versions and old Committed versions below GC safepoint

### Memory Budget
- `memtable_budget`: triggers flush when exceeded
- `block_cache_budget`: LRU eviction
- `l0_stall_count`: stalls writes when L0 file count exceeds threshold
- Total budget enforced: `memtable + block_cache + compaction_buffer ≤ node_limit`
