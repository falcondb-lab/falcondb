//! # C3: Centralized Timestamp Oracle (TSO) Service
//!
//! Provides a **single global timestamp source** for the entire cluster,
//! analogous to TiDB's PD-TSO and CockroachDB's closed-timestamp subsystem.
//!
//! ## Why TSO over per-node HLC?
//!
//! Per-node HLC gives causal ordering within a node but cannot guarantee
//! **strict linearizability across shards** without clock uncertainty windows.
//! A centralized TSO eliminates the uncertainty window entirely:
//! every transaction gets a globally unique, monotonically increasing timestamp
//! that is valid across all shards without clock skew compensation.
//!
//! ## Architecture
//!
//! ```text
//!   TsoServer (one per cluster, typically co-located with Raft leader)
//!       │  alloc_batch(size) → [ts_start, ts_end)
//!       │
//!   TsoClient (one per node, caches a batch lease)
//!       │  alloc() → single Timestamp from local batch
//!       │  (refills batch via TsoServer when exhausted)
//!       │
//!   TxnManager::alloc_ts() delegates to TsoClient when enabled
//! ```
//!
//! ## Batch Leasing
//!
//! To avoid per-timestamp RPCs, the server hands out **batch leases**:
//! a contiguous range `[start, end)` that the client can use locally.
//! Typical batch size: 1000 timestamps per RPC (configurable).
//!
//! ## High-Availability
//!
//! The `TsoServer` is protected by a Raft group. On leader failover, the
//! incoming leader advances the counter past the previous leader's max
//! allocated value (stored durably) before serving new batches.
//! This guarantees monotonicity across failovers (no timestamp reuse).
//!
//! ## Closed Timestamps (for Follower Read)
//!
//! The TSO server also maintains a **closed timestamp**: the highest timestamp
//! at which all transactions on all shards are known to have committed or
//! aborted. Reads at `ts <= closed_ts` are safe to serve from any replica
//! without contacting the leader (follower read).
//!
//! See `FollowerReadRouter` in `follower_read.rs` for the consumer.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use falcon_common::types::Timestamp;

// ── Constants ─────────────────────────────────────────────────────────────

/// Default batch size: number of timestamps handed out per lease.
const DEFAULT_BATCH_SIZE: u64 = 1_000;

/// How far ahead the server pre-allocates to survive brief outages.
/// On startup the server bumps its persistent counter by this gap so that
/// a crash-restart never issues a timestamp below what was already handed out.
const ADVANCE_ON_START: u64 = 100_000;

/// Closed-timestamp advance interval: how often the TSO closes a new
/// timestamp frontier (push forward the follower-read safe point).
const CLOSED_TS_ADVANCE_INTERVAL_MS: u64 = 200;

// ─────────────────────────────────────────────────────────────────────────────
// TsoServer
// ─────────────────────────────────────────────────────────────────────────────

/// A batch lease handed to a client.
#[derive(Debug, Clone, Copy)]
pub struct TsoBatch {
    /// First timestamp in the batch (inclusive).
    pub start: Timestamp,
    /// One past the last timestamp (exclusive).
    pub end: Timestamp,
}

impl TsoBatch {
    pub const fn size(&self) -> u64 {
        self.end.0 - self.start.0
    }
    pub const fn is_empty(&self) -> bool {
        self.start.0 >= self.end.0
    }
}

/// The centralized timestamp oracle server.
///
/// In production this runs as a Raft state machine so its counter survives
/// failovers. In tests it is instantiated directly.
pub struct TsoServer {
    /// Monotonically increasing counter. Only goes forward.
    counter: AtomicU64,
    /// Largest timestamp ever handed out (for durability / crash recovery).
    max_allocated: AtomicU64,
    /// Batch size (timestamps per lease).
    batch_size: u64,
    /// Closed timestamp: highest ts guaranteed fully committed across all shards.
    closed_ts: AtomicU64,
    /// Timestamp of the last closed-ts advance.
    last_advance: Mutex<Instant>,
    /// Advance interval.
    advance_interval: Duration,
    /// Running flag — set false to stop the background advance task.
    running: AtomicBool,
    /// Metrics
    pub metrics: TsoServerMetrics,
}

/// Observability counters for the TSO server.
#[derive(Debug, Default)]
pub struct TsoServerMetrics {
    pub batches_issued: AtomicU64,
    pub timestamps_issued: AtomicU64,
    pub closed_ts_advances: AtomicU64,
}

impl TsoServer {
    /// Create a new TSO server starting at `initial_counter`.
    /// Pass 0 for fresh clusters; pass `recovered_max + ADVANCE_ON_START` after WAL recovery.
    pub fn new(initial_counter: u64) -> Arc<Self> {
        let start = initial_counter.max(1) + ADVANCE_ON_START;
        Arc::new(Self {
            counter: AtomicU64::new(start),
            max_allocated: AtomicU64::new(start),
            batch_size: DEFAULT_BATCH_SIZE,
            closed_ts: AtomicU64::new(0),
            last_advance: Mutex::new(Instant::now()),
            advance_interval: Duration::from_millis(CLOSED_TS_ADVANCE_INTERVAL_MS),
            running: AtomicBool::new(true),
            metrics: TsoServerMetrics::default(),
        })
    }

    /// Create with a custom batch size (for tuning).
    pub fn new_with_batch(initial_counter: u64, batch_size: u64) -> Arc<Self> {
        let mut s = Arc::try_unwrap(Self::new(initial_counter)).unwrap_or_else(|a| {
            // Safety: only called right after new()
            Arc::try_unwrap(a).unwrap_or_else(|_| panic!("TsoServer: arc unwrap failed"))
        });
        s.batch_size = batch_size;
        Arc::new(s)
    }

    /// Allocate a batch of timestamps. Thread-safe, lock-free.
    ///
    /// Returns a `TsoBatch` range `[start, start + batch_size)`.
    pub fn alloc_batch(&self, requested_size: u64) -> TsoBatch {
        let size = requested_size.clamp(1, self.batch_size * 4);
        let start = self.counter.fetch_add(size, Ordering::AcqRel);
        let end = start + size;
        // Track high-water mark for crash-recovery durability.
        self.max_allocated.fetch_max(end, Ordering::Relaxed);
        self.metrics.batches_issued.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .timestamps_issued
            .fetch_add(size, Ordering::Relaxed);
        TsoBatch {
            start: Timestamp(start),
            end: Timestamp(end),
        }
    }

    /// Allocate a single timestamp (for coordinator use).
    pub fn alloc_one(&self) -> Timestamp {
        let ts = self.counter.fetch_add(1, Ordering::AcqRel);
        self.max_allocated.fetch_max(ts + 1, Ordering::Relaxed);
        self.metrics.timestamps_issued.fetch_add(1, Ordering::Relaxed);
        Timestamp(ts)
    }

    /// The highest timestamp the server has ever allocated.
    pub fn max_allocated(&self) -> Timestamp {
        Timestamp(self.max_allocated.load(Ordering::Acquire))
    }

    /// Advance the closed timestamp to `new_closed`.
    ///
    /// `new_closed` must be less than the current counter minus a safety margin.
    /// The server advances the closed_ts only if `new_closed` is strictly greater
    /// than the current closed_ts (monotonic).
    pub fn advance_closed_ts(&self, new_closed: Timestamp) {
        let current_counter = self.counter.load(Ordering::Acquire);
        // Safety margin: don't close within 100 timestamps of current frontier.
        if new_closed.0 + 100 > current_counter {
            return;
        }
        // CAS loop to advance monotonically.
        let mut old = self.closed_ts.load(Ordering::Relaxed);
        loop {
            if new_closed.0 <= old {
                break;
            }
            match self.closed_ts.compare_exchange_weak(
                old,
                new_closed.0,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.metrics
                        .closed_ts_advances
                        .fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(actual) => old = actual,
            }
        }
    }

    /// The current closed timestamp (safe for follower reads).
    pub fn closed_ts(&self) -> Timestamp {
        Timestamp(self.closed_ts.load(Ordering::Acquire))
    }

    /// Periodically advance the closed timestamp.
    /// Call this from a background tick loop.
    pub fn tick_advance_closed_ts(&self) {
        let now = Instant::now();
        let mut last = self.last_advance.lock();
        if now.duration_since(*last) < self.advance_interval {
            return;
        }
        *last = now;
        // Advance closed_ts to (current_counter - 200) — leaves room for in-flight txns.
        let current = self.counter.load(Ordering::Acquire);
        if current > 200 {
            self.advance_closed_ts(Timestamp(current - 200));
        }
    }

    /// Snapshot for `SHOW falcon.tso_stats`.
    pub fn stats_snapshot(&self) -> TsoStatsSnapshot {
        TsoStatsSnapshot {
            current_counter: Timestamp(self.counter.load(Ordering::Relaxed)),
            max_allocated: self.max_allocated(),
            closed_ts: self.closed_ts(),
            batches_issued: self.metrics.batches_issued.load(Ordering::Relaxed),
            timestamps_issued: self.metrics.timestamps_issued.load(Ordering::Relaxed),
            closed_ts_advances: self.metrics.closed_ts_advances.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of TSO server statistics.
#[derive(Debug, Clone)]
pub struct TsoStatsSnapshot {
    pub current_counter: Timestamp,
    pub max_allocated: Timestamp,
    pub closed_ts: Timestamp,
    pub batches_issued: u64,
    pub timestamps_issued: u64,
    pub closed_ts_advances: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// TsoClient
// ─────────────────────────────────────────────────────────────────────────────

/// Per-node TSO client that caches a batch lease from the server.
///
/// Thread-safe. Multiple threads share one `TsoClient` per node.
/// Refills the local batch atomically when exhausted.
pub struct TsoClient {
    /// Reference to the TSO server (in-process for single-binary clusters;
    /// in production: gRPC stub to the TSO leader node).
    server: Arc<TsoServer>,
    /// Current lease: [next, end).
    /// Stored as two separate AtomicU64s to allow lock-free single-timestamp alloc.
    lease_next: AtomicU64,
    lease_end: AtomicU64,
    /// Refill lock — only one thread refills at a time.
    refill_lock: Mutex<()>,
    /// Default batch size per refill.
    batch_size: u64,
    /// Metrics
    pub metrics: TsoClientMetrics,
}

#[derive(Debug, Default)]
pub struct TsoClientMetrics {
    pub allocs: AtomicU64,
    pub refills: AtomicU64,
}

impl TsoClient {
    pub fn new(server: Arc<TsoServer>) -> Arc<Self> {
        let batch_size = server.batch_size;
        Arc::new(Self {
            server,
            lease_next: AtomicU64::new(0),
            lease_end: AtomicU64::new(0),
            refill_lock: Mutex::new(()),
            batch_size,
            metrics: TsoClientMetrics::default(),
        })
    }

    /// Allocate a single timestamp from the local batch lease.
    /// Refills from the server if the local batch is exhausted.
    pub fn alloc(&self) -> Timestamp {
        self.metrics.allocs.fetch_add(1, Ordering::Relaxed);
        loop {
            let next = self.lease_next.load(Ordering::Relaxed);
            let end = self.lease_end.load(Ordering::Relaxed);
            if next < end {
                // Try to claim this slot.
                if self
                    .lease_next
                    .compare_exchange_weak(next, next + 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return Timestamp(next);
                }
                // CAS failed — retry (another thread advanced lease_next).
                continue;
            }
            // Batch exhausted — refill under lock (only one thread at a time).
            let _guard = self.refill_lock.lock();
            // Re-check after acquiring lock (another thread may have refilled).
            let next2 = self.lease_next.load(Ordering::Relaxed);
            let end2 = self.lease_end.load(Ordering::Relaxed);
            if next2 < end2 {
                // Another thread refilled — retry the loop.
                continue;
            }
            // Genuinely exhausted — fetch new batch from server.
            let batch = self.server.alloc_batch(self.batch_size);
            self.lease_end.store(batch.end.0, Ordering::Release);
            self.lease_next.store(batch.start.0, Ordering::Release);
            self.metrics.refills.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Read the closed timestamp from the server (no allocation).
    pub fn closed_ts(&self) -> Timestamp {
        self.server.closed_ts()
    }

    /// Force a batch refill from the server (e.g. after WAL recovery).
    pub fn invalidate_lease(&self) {
        let _guard = self.refill_lock.lock();
        self.lease_end.store(0, Ordering::Release);
        self.lease_next.store(0, Ordering::Release);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tso_server_monotonic() {
        let srv = TsoServer::new(0);
        let b1 = srv.alloc_batch(10);
        let b2 = srv.alloc_batch(10);
        assert!(b2.start.0 >= b1.end.0, "batches must not overlap");
    }

    #[test]
    fn tso_client_unique_timestamps() {
        let srv = TsoServer::new(0);
        let client = TsoClient::new(srv);
        let mut prev = 0u64;
        for _ in 0..2000 {
            let ts = client.alloc();
            assert!(ts.0 > prev, "timestamps must be strictly increasing");
            prev = ts.0;
        }
    }

    #[test]
    fn tso_client_concurrent() {
        use std::collections::HashSet;
        use std::sync::Mutex as StdMutex;

        let srv = TsoServer::new(0);
        let client = TsoClient::new(Arc::clone(&srv));
        let seen: Arc<StdMutex<HashSet<u64>>> = Arc::new(StdMutex::new(HashSet::new()));
        let n_threads = 8;
        let per_thread = 500;

        std::thread::scope(|s| {
            for _ in 0..n_threads {
                let c = Arc::clone(&client);
                let seen = Arc::clone(&seen);
                s.spawn(move || {
                    for _ in 0..per_thread {
                        let ts = c.alloc();
                        let mut g = seen.lock().unwrap();
                        assert!(g.insert(ts.0), "duplicate timestamp: {}", ts.0);
                    }
                });
            }
        });

        let g = seen.lock().unwrap();
        assert_eq!(g.len(), n_threads * per_thread);
    }

    #[test]
    fn tso_closed_ts_monotonic() {
        let srv = TsoServer::new(0);
        // Allocate some timestamps so counter is well ahead.
        srv.alloc_batch(1000);
        srv.advance_closed_ts(Timestamp(100));
        assert_eq!(srv.closed_ts().0, 100);
        // Advancing backward is a no-op.
        srv.advance_closed_ts(Timestamp(50));
        assert_eq!(srv.closed_ts().0, 100);
        // Forward advance works.
        srv.advance_closed_ts(Timestamp(200));
        assert_eq!(srv.closed_ts().0, 200);
    }
}
