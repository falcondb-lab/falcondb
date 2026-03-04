//! # Module Status: EXPERIMENTAL — compile-gated (`--features lsm`).
//!
//! LSM-tree storage engine for disk-backed OLTP.
//!
//! ## Architecture
//! ```text
//!   PUT/GET ──► LsmMemTable (sorted, in-memory)
//!                  │  flush (rate-limited)
//!                  ▼
//!              SST L0 (overlapping) ──► block_cache (LRU)
//!                  │  leveled compaction (picker)
//!                  ▼
//!              SST L1..Ln (non-overlapping, GC-filtered)
//! ```
//!
//! ## Modules
//! - `engine`      — core read/write/flush/compaction orchestration
//! - `memtable`    — sorted in-memory skiplist with freeze/rotate
//! - `sst`         — SST file format: data blocks, bloom filter, footer
//! - `block_cache` — LRU cache for SST data blocks, keyed by (sst_id, offset)
//! - `bloom`       — per-SST bloom filter for point-lookup acceleration
//! - `compaction`  — leveled compaction with multi-level picker (L0→L1, Ln→Ln+1)
//! - `manifest`    — append-only JSON log tracking SST additions/removals
//! - `throttle`    — token-bucket rate limiter + backpressure controller
//! - `gc`          — MVCC garbage collection policy applied during compaction
//! - `workers`     — tokio background flush/compaction workers
//! - `mvcc_encoding` — MVCC value encoding, visibility, TxnMetaStore
//! - `idempotency` — exactly-once idempotency key store
//! - `txn_audit`   — transaction audit log
//!
//! ## Key design choices
//! - Write: MemTable → flush to L0 SST → leveled compaction L1+
//! - Read: MemTable → block cache → L0 (newest first) → L1..Ln with bloom
//! - Backpressure: 3 levels (Normal/Delayed/Stopped) based on immutable count, L0 files, compaction backlog
//! - I/O throttling: separate rate limiters for flush and compaction
//! - Manifest: append-only log with compaction; replay on startup, fallback to filename recovery
//! - GC: obsolete MVCC versions and tombstones pruned during compaction

pub mod block_cache;
pub mod bloom;
pub mod compaction;
pub mod engine;
pub mod gc;
pub mod idempotency;
pub mod manifest;
pub mod memtable;
pub mod mvcc_encoding;
pub mod sst;
pub mod throttle;
pub mod txn_audit;
pub mod workers;

pub use block_cache::BlockCache;
pub use bloom::BloomFilter;
pub use compaction::Compactor;
pub use engine::{LsmConfig, LsmEngine};
pub use gc::GcPolicy;
pub use idempotency::{IdempotencyConfig, IdempotencyResult, IdempotencyStore};
pub use manifest::Manifest;
pub use memtable::LsmMemTable;
pub use mvcc_encoding::{MvccStatus, MvccValue, TxnMeta, TxnMetaStore, TxnOutcome};
pub use sst::{SstMeta, SstReadError, SstReader, SstWriter};
pub use throttle::{BackpressureConfig, BackpressureController, RateLimiter};
pub use workers::BgWorkers;
