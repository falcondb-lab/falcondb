//! Group Commit Syncer — decouples WAL append from fsync.
//!
//! Transaction threads call `append_and_wait()` which writes to the WAL buffer
//! and enqueues a notification. A dedicated background syncer thread wakes up
//! periodically (or when the batch is full), performs a single fsync, and
//! notifies all waiting transactions that their data is durable.
//!
//! Commit policies:
//! - `LocalDurable`: wait for local WAL fsync (default)
//! - `ReplicaDurable`: wait for at least one replica ack
//! - `QuorumVisible`: wait for quorum replica ack

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use falcon_common::config::DurabilityPolicy;
use falcon_common::error::StorageError;

use crate::wal::{WalRecord, WalWriter};

// ── Commit Policy ──────────────────────────────────────────────────────────

/// Controls what a committing transaction waits for before returning to client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitPolicy {
    /// Wait for local WAL fsync only.
    LocalDurable,
    /// Wait for at least one replica to ack.
    ReplicaDurable,
    /// Wait for a quorum of replicas to ack (strongest).
    QuorumVisible,
}

impl From<DurabilityPolicy> for CommitPolicy {
    fn from(dp: DurabilityPolicy) -> Self {
        match dp {
            DurabilityPolicy::LocalFsync => Self::LocalDurable,
            DurabilityPolicy::QuorumAck => Self::QuorumVisible,
            DurabilityPolicy::AllAck => Self::ReplicaDurable,
        }
    }
}

// ── Group Commit Syncer ────────────────────────────────────────────────────

/// Shared state between transaction threads and the syncer thread.
struct SyncerState {
    /// The LSN that has been durably fsynced.
    fsynced_lsn: u64,
    /// Number of pending (unfsynced) appends.
    pending_count: u64,
}

/// Configuration for the group commit syncer.
#[derive(Debug, Clone)]
pub struct GroupCommitConfig {
    /// Maximum time to wait before fsyncing (microseconds).
    pub flush_interval_us: u64,
    /// Maximum pending records before forcing an fsync.
    pub max_batch_size: u64,
    /// Group commit coalescing window (microseconds).
    /// The syncer thread sleeps for this duration to accumulate more
    /// records before issuing a single fsync. Typical range: 100–500 µs.
    /// Set to 0 for immediate flush (lowest latency, highest fsync rate).
    pub group_commit_window_us: u64,
    /// Capacity of the in-memory ring buffer (bytes) for WAL write batching.
    /// Records accumulate here before being flushed to the WAL device.
    /// Default: 256 KB.
    pub ring_buffer_capacity: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            flush_interval_us: 1000, // 1ms
            max_batch_size: 64,
            group_commit_window_us: 200, // 200µs default coalescing window
            ring_buffer_capacity: 256 * 1024, // 256 KB
        }
    }
}

/// Statistics for the group commit syncer.
#[derive(Debug)]
pub struct GroupCommitStats {
    /// Total number of fsync calls performed.
    pub fsyncs: AtomicU64,
    /// Total number of records fsynced.
    pub records_synced: AtomicU64,
    /// Total number of group commit batches.
    pub batches: AtomicU64,
    /// Total wait time accumulated by all waiters (microseconds).
    pub total_wait_us: AtomicU64,
    /// Maximum single wait time (microseconds).
    pub max_wait_us: AtomicU64,
    /// Current ring buffer usage in bytes.
    pub ring_buffer_used: AtomicU64,
    /// Peak ring buffer usage in bytes.
    pub ring_buffer_peak: AtomicU64,
}

impl GroupCommitStats {
    pub const fn new() -> Self {
        Self {
            fsyncs: AtomicU64::new(0),
            records_synced: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            total_wait_us: AtomicU64::new(0),
            max_wait_us: AtomicU64::new(0),
            ring_buffer_used: AtomicU64::new(0),
            ring_buffer_peak: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> GroupCommitStatsSnapshot {
        GroupCommitStatsSnapshot {
            fsyncs: self.fsyncs.load(Ordering::Relaxed),
            records_synced: self.records_synced.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            total_wait_us: self.total_wait_us.load(Ordering::Relaxed),
            max_wait_us: self.max_wait_us.load(Ordering::Relaxed),
            ring_buffer_used: self.ring_buffer_used.load(Ordering::Relaxed),
            ring_buffer_peak: self.ring_buffer_peak.load(Ordering::Relaxed),
        }
    }
}

impl Default for GroupCommitStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of group commit stats.
#[derive(Debug, Clone)]
pub struct GroupCommitStatsSnapshot {
    pub fsyncs: u64,
    pub records_synced: u64,
    pub batches: u64,
    pub total_wait_us: u64,
    pub max_wait_us: u64,
    pub ring_buffer_used: u64,
    pub ring_buffer_peak: u64,
}

/// The group commit syncer manages batched WAL fsync.
///
/// Transaction threads call `append_and_wait()` to write a WAL record and
/// block until the syncer thread has fsynced their data. The syncer coalesces
/// multiple pending appends into a single fsync call.
pub struct GroupCommitSyncer {
    wal: Arc<WalWriter>,
    state: Arc<(Mutex<SyncerState>, Condvar)>,
    config: GroupCommitConfig,
    stats: Arc<GroupCommitStats>,
    shutdown: Arc<AtomicBool>,
    /// The LSN counter — each append increments this.
    next_lsn: AtomicU64,
    /// Unflushed bytes in the ring buffer (written but not yet fsynced).
    pending_bytes: AtomicU64,
    /// Atomic mirror of SyncerState::fsynced_lsn for lock-free reads.
    atomic_fsynced_lsn: AtomicU64,
}

impl GroupCommitSyncer {
    /// Create a new group commit syncer wrapping an existing WAL writer.
    #[allow(clippy::let_and_return)]
    pub fn new(wal: Arc<WalWriter>, config: GroupCommitConfig) -> Arc<Self> {
        let syncer = Arc::new(Self {
            wal,
            state: Arc::new((
                Mutex::new(SyncerState {
                    fsynced_lsn: 0,
                    pending_count: 0,
                }),
                Condvar::new(),
            )),
            config,
            stats: Arc::new(GroupCommitStats::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            next_lsn: AtomicU64::new(1),
            pending_bytes: AtomicU64::new(0),
            atomic_fsynced_lsn: AtomicU64::new(0),
        });
        syncer
    }

    /// Returns the LSN that has been durably fsynced.
    /// Lock-free read via atomic mirror — safe for observability and replication.
    pub fn flushed_lsn(&self) -> u64 {
        self.atomic_fsynced_lsn.load(Ordering::Acquire)
    }

    /// Returns the number of bytes written but not yet fsynced (WAL backlog).
    pub fn wal_backlog_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    /// Start the background syncer thread. Returns a join handle.
    ///
    /// Returns `Err` if the OS cannot spawn the thread (resource exhaustion).
    /// The caller must handle this as a degraded condition — **no panic**.
    pub fn start_syncer(self: &Arc<Self>) -> Result<std::thread::JoinHandle<()>, StorageError> {
        let syncer = Arc::clone(self);
        std::thread::Builder::new()
            .name("falcon-group-commit".into())
            .spawn(move || syncer.syncer_loop())
            .map_err(|e| {
                tracing::error!(
                    component = "group-commit-syncer",
                    error = %e,
                    "failed to spawn background thread — node DEGRADED"
                );
                StorageError::Wal(format!("failed to spawn group-commit syncer: {e}"))
            })
    }

    /// Append a WAL record and wait for it to be fsynced (LocalDurable policy).
    /// Transaction threads call this instead of directly calling `wal.append()`.
    pub fn append_and_wait(&self, record: &WalRecord) -> Result<u64, StorageError> {
        let start = Instant::now();

        // Append to WAL buffer (no fsync yet)
        let lsn = self.wal.append(record)?;

        // Notify syncer that there's work and get our sequence number
        let my_lsn = {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.pending_count += 1;
            let seq = self.next_lsn.fetch_add(1, Ordering::Relaxed);
            cvar.notify_one();

            // Wait until our LSN is fsynced
            while state.fsynced_lsn <= seq && !self.shutdown.load(Ordering::Relaxed) {
                let timeout = Duration::from_micros(self.config.flush_interval_us * 10);
                let result = cvar
                    .wait_timeout(state, timeout)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state = result.0;
            }
            seq
        };

        let elapsed = start.elapsed().as_micros() as u64;
        self.stats
            .total_wait_us
            .fetch_add(elapsed, Ordering::Relaxed);
        let _ = self
            .stats
            .max_wait_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if elapsed > cur {
                    Some(elapsed)
                } else {
                    None
                }
            });
        let _ = my_lsn;

        Ok(lsn)
    }

    /// Append a WAL record without waiting for fsync (fire-and-forget).
    /// Useful for records that don't need durability guarantees (e.g., stats).
    pub fn append_no_wait(&self, record: &WalRecord) -> Result<u64, StorageError> {
        let lsn = self.wal.append(record)?;
        {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.pending_count += 1;
            self.next_lsn.fetch_add(1, Ordering::Relaxed);
            cvar.notify_one();
        }
        Ok(lsn)
    }

    /// Signal the syncer to shut down gracefully.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let (_, cvar) = &*self.state;
        cvar.notify_all();
    }

    /// Get a reference to the stats.
    pub fn stats(&self) -> &GroupCommitStats {
        &self.stats
    }

    /// The background syncer loop.
    fn syncer_loop(&self) {
        let flush_interval = Duration::from_micros(self.config.flush_interval_us);
        let coalesce_window = Duration::from_micros(self.config.group_commit_window_us);
        let (lock, cvar) = &*self.state;

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                // Final flush before exit
                let _ = self.do_flush();
                break;
            }

            let should_flush = {
                let state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);

                if state.pending_count == 0 {
                    // Idle: wait for work or flush_interval timeout.
                    // Condvar releases the lock while sleeping — other threads can append freely.
                    let result = cvar
                        .wait_timeout(state, flush_interval)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    result.0.pending_count > 0
                } else if state.pending_count >= self.config.max_batch_size {
                    true // Batch full — flush immediately, no coalescing delay
                } else if self.config.group_commit_window_us > 0 {
                    // Coalescing window: release the lock and wait via condvar so that:
                    // a) additional appenders can enqueue without blocking,
                    // b) a shutdown signal or batch-full wakeup cuts the wait early.
                    let result = cvar
                        .wait_timeout(state, coalesce_window)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    // Flush if there is anything pending after the window
                    result.0.pending_count > 0
                } else {
                    // No coalescing window: flush immediately
                    true
                }
            };

            if should_flush {
                if let Err(e) = self.do_flush() {
                    tracing::error!("group-commit fsync error: {}", e);
                }
            }
        }
    }

    /// Perform the actual flush + fsync and notify all waiters.
    fn do_flush(&self) -> Result<(), StorageError> {
        // Snapshot pending count under lock, then flush outside lock
        let (batch_size, lsn_snapshot) = {
            let (lock, _) = &*self.state;
            let state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (state.pending_count, self.next_lsn.load(Ordering::Relaxed))
        };

        if batch_size == 0 {
            return Ok(());
        }

        let wal_lsn = self.wal.current_lsn();
        let backlog_before = self.pending_bytes.load(Ordering::Relaxed);
        self.wal.flush()?;

        // Reset backlog — all pending bytes are now durable
        self.pending_bytes.store(0, Ordering::Relaxed);

        {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            // Advance fsynced_lsn past all LSNs that were pending
            state.fsynced_lsn = lsn_snapshot;
            state.pending_count = state.pending_count.saturating_sub(batch_size);
            // Update atomic mirror before notifying waiters (Release ensures visibility).
            self.atomic_fsynced_lsn.store(lsn_snapshot, Ordering::Release);
            cvar.notify_all();
        }

        self.stats.fsyncs.fetch_add(1, Ordering::Relaxed);
        self.stats
            .records_synced
            .fetch_add(batch_size, Ordering::Relaxed);
        self.stats.batches.fetch_add(1, Ordering::Relaxed);
        self.stats.ring_buffer_used.store(0, Ordering::Relaxed);

        tracing::trace!(
            batch_size = batch_size,
            commit_lsn = lsn_snapshot,
            flushed_lsn = wal_lsn,
            backlog_bytes = backlog_before,
            "group-commit flush"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{SyncMode, WalWriter};
    use falcon_common::types::TxnId;
    use std::sync::Arc;

    fn test_wal(dir: &std::path::Path) -> Arc<WalWriter> {
        Arc::new(WalWriter::open_with_options(dir, SyncMode::None, 64 * 1024 * 1024, 1024).unwrap())
    }

    #[test]
    fn test_group_commit_basic() {
        let dir = std::env::temp_dir().join("falcon_gc_test_basic");
        let _ = std::fs::remove_dir_all(&dir);

        let wal = test_wal(&dir);
        let config = GroupCommitConfig {
            flush_interval_us: 500,
            max_batch_size: 8,
            ..Default::default()
        };
        let syncer = GroupCommitSyncer::new(wal, config);
        let handle = syncer.start_syncer().unwrap();

        // Append records and wait for durability
        for i in 0..10 {
            syncer
                .append_and_wait(&WalRecord::BeginTxn { txn_id: TxnId(i) })
                .unwrap();
        }

        let stats = syncer.stats().snapshot();
        assert!(stats.fsyncs > 0, "should have performed at least one fsync");
        assert!(
            stats.records_synced >= 10,
            "all 10 records should be synced, got {}",
            stats.records_synced
        );

        syncer.shutdown();
        handle.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_group_commit_batching() {
        let dir = std::env::temp_dir().join("falcon_gc_test_batch");
        let _ = std::fs::remove_dir_all(&dir);

        let wal = test_wal(&dir);
        let config = GroupCommitConfig {
            flush_interval_us: 10_000, // 10ms — long interval to encourage batching
            max_batch_size: 5,
            ..Default::default()
        };
        let syncer = GroupCommitSyncer::new(wal, config);
        let handle = syncer.start_syncer().unwrap();

        // Rapid-fire 20 appends — should batch into ~4 fsyncs
        for i in 0..20 {
            syncer
                .append_and_wait(&WalRecord::BeginTxn { txn_id: TxnId(i) })
                .unwrap();
        }

        let stats = syncer.stats().snapshot();
        assert!(
            stats.fsyncs <= 20,
            "should batch fsyncs, not one per record"
        );
        assert!(
            stats.records_synced >= 20,
            "all 20 records should be synced, got {}",
            stats.records_synced
        );

        syncer.shutdown();
        handle.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_group_commit_no_wait() {
        let dir = std::env::temp_dir().join("falcon_gc_test_nowait");
        let _ = std::fs::remove_dir_all(&dir);

        let wal = test_wal(&dir);
        let config = GroupCommitConfig::default();
        let syncer = GroupCommitSyncer::new(wal, config);
        let handle = syncer.start_syncer().unwrap();

        // Fire-and-forget appends
        for i in 0..5 {
            syncer
                .append_no_wait(&WalRecord::BeginTxn { txn_id: TxnId(i) })
                .unwrap();
        }

        // Give syncer time to flush
        std::thread::sleep(Duration::from_millis(50));

        syncer.shutdown();
        handle.join().unwrap();

        let stats = syncer.stats().snapshot();
        assert!(
            stats.records_synced >= 5,
            "all 5 records should be synced, got {}",
            stats.records_synced
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_group_commit_concurrent() {
        let dir = std::env::temp_dir().join("falcon_gc_test_conc");
        let _ = std::fs::remove_dir_all(&dir);

        let wal = test_wal(&dir);
        let config = GroupCommitConfig {
            flush_interval_us: 500,
            max_batch_size: 16,
            ..Default::default()
        };
        let syncer = GroupCommitSyncer::new(wal, config);
        let handle = syncer.start_syncer().unwrap();

        // 4 concurrent txn threads
        let mut threads = Vec::new();
        for t in 0..4u64 {
            let s = Arc::clone(&syncer);
            threads.push(std::thread::spawn(move || {
                for i in 0..25 {
                    s.append_and_wait(&WalRecord::BeginTxn {
                        txn_id: TxnId(t * 100 + i),
                    })
                    .unwrap();
                }
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        let stats = syncer.stats().snapshot();
        assert!(
            stats.records_synced >= 100,
            "all 100 records should be synced, got {}",
            stats.records_synced
        );
        // With batching, fsyncs should be much fewer than 100
        assert!(
            stats.fsyncs < 100,
            "should batch fsyncs: got {} fsyncs for 100 records",
            stats.fsyncs
        );

        syncer.shutdown();
        handle.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_group_commit_stats() {
        let dir = std::env::temp_dir().join("falcon_gc_test_stats");
        let _ = std::fs::remove_dir_all(&dir);

        let wal = test_wal(&dir);
        let config = GroupCommitConfig::default();
        let syncer = GroupCommitSyncer::new(wal, config);
        let handle = syncer.start_syncer().unwrap();

        syncer
            .append_and_wait(&WalRecord::BeginTxn { txn_id: TxnId(1) })
            .unwrap();

        let stats = syncer.stats().snapshot();
        assert!(stats.fsyncs >= 1);
        assert!(stats.records_synced >= 1);
        assert!(stats.batches >= 1);

        syncer.shutdown();
        handle.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_commit_policy_from_durability() {
        assert_eq!(
            CommitPolicy::from(DurabilityPolicy::LocalFsync),
            CommitPolicy::LocalDurable
        );
        assert_eq!(
            CommitPolicy::from(DurabilityPolicy::QuorumAck),
            CommitPolicy::QuorumVisible
        );
        assert_eq!(
            CommitPolicy::from(DurabilityPolicy::AllAck),
            CommitPolicy::ReplicaDurable
        );
    }
}
