//! # Module Status: EXPERIMENTAL — compile-gated, not on the production OLTP write path.
//! Do NOT reference from planner/executor/txn for production workloads.
//!
//! LSM-tree storage engine for disk-backed OLTP.
//!
//! Architecture:
//! ```text
//!   PUT/GET ──► LsmMemTable (sorted, in-memory)
//!                  │  (flush when budget exceeded)
//!                  ▼
//!              SST L0 files (unsorted across files)
//!                  │  (compaction)
//!                  ▼
//!              SST L1..Ln (sorted, non-overlapping per level)
//! ```
//!
//! Key design choices:
//! - Write path: WAL → MemTable → flush to L0 SST
//! - Read path: MemTable → L0 (newest first) → L1..Ln with bloom filter
//! - MVCC: each value encodes `(txn_id, status, commit_ts, data)`
//! - Backpressure: memtable budget, L0 stall count, compaction backlog

pub mod block_cache;
pub mod bloom;
pub mod compaction;
pub mod engine;
pub mod idempotency;
pub mod memtable;
pub mod mvcc_encoding;
pub mod sst;
pub mod txn_audit;

pub use block_cache::BlockCache;
pub use bloom::BloomFilter;
pub use compaction::Compactor;
pub use engine::{LsmConfig, LsmEngine};
pub use idempotency::{IdempotencyConfig, IdempotencyResult, IdempotencyStore};
pub use memtable::LsmMemTable;
pub use mvcc_encoding::{MvccStatus, MvccValue, TxnMeta, TxnMetaStore, TxnOutcome};
pub use sst::{SstMeta, SstReadError, SstReader, SstWriter};
