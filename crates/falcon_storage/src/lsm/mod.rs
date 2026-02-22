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

pub mod bloom;
pub mod block_cache;
pub mod sst;
pub mod memtable;
pub mod compaction;
pub mod engine;
pub mod mvcc_encoding;
pub mod idempotency;
pub mod txn_audit;

pub use engine::{LsmEngine, LsmConfig};
pub use memtable::LsmMemTable;
pub use sst::{SstWriter, SstReader, SstMeta, SstReadError};
pub use block_cache::BlockCache;
pub use bloom::BloomFilter;
pub use compaction::Compactor;
pub use mvcc_encoding::{MvccValue, MvccStatus, TxnMeta, TxnMetaStore, TxnOutcome};
pub use idempotency::{IdempotencyStore, IdempotencyConfig, IdempotencyResult};
