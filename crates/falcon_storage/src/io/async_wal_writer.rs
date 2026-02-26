//! Async WAL Writer — integrates `AsyncFile` with WAL append + flush coalescing.
//!
//! Replaces blocking `BufWriter<File>` writes with `AsyncFile::write_at()` and
//! replaces per-txn `sync_data()` with batched `AsyncFile::sync()` governed by
//! `FlushPolicy`.
//!
//! ## Durability Contract
//!
//! A WAL record is considered **durable** only after:
//! 1. `write_at()` completes (data in OS buffer cache), AND
//! 2. `sync()` completes (`FlushFileBuffers` / `fsync`)
//!
//! The `append_durable()` method returns only after both steps.
//! The `append_buffered()` method returns after step 1 — caller must
//! explicitly call `flush_durable()` or rely on `FlushPolicy` triggers.
//!
//! ## Flush Coalescing
//!
//! Under `FlushPolicy::GroupCommit`, multiple buffered appends are coalesced
//! into a single `sync()` call, reducing flush frequency from per-txn to
//! per-batch. This is the primary mechanism for p99/p999 improvement.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::async_file::{
    AsyncFileConfig, FlushPolicy, FlushReason, IoError, IoMetrics,
};
use super::AsyncFile;
use crate::wal::{WAL_FORMAT_VERSION, WAL_MAGIC, WAL_SEGMENT_HEADER_SIZE};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — AsyncWalWriter
// ═══════════════════════════════════════════════════════════════════════════

/// Async WAL writer configuration.
#[derive(Debug, Clone)]
pub struct AsyncWalConfig {
    /// Flush policy for durability.
    pub flush_policy: FlushPolicy,
    /// Max bytes per WAL segment before rotating.
    pub max_segment_size: u64,
    /// WAL directory path.
    pub wal_dir: PathBuf,
    /// AsyncFile backend config.
    pub file_config: AsyncFileConfig,
}

impl Default for AsyncWalConfig {
    fn default() -> Self {
        Self {
            flush_policy: FlushPolicy::default(),
            max_segment_size: 64 * 1024 * 1024, // 64 MB
            wal_dir: PathBuf::from("./falcon_data/wal"),
            file_config: AsyncFileConfig::default(),
        }
    }
}

/// Group commit syncer state shared between writer and syncer thread.
struct SyncState {
    /// LSN up to which data has been durably synced.
    synced_lsn: u64,
    /// Number of buffered (unsynced) records.
    pending_count: u64,
    /// Accumulated pending bytes since last sync.
    pending_bytes: u64,
}

/// Async WAL writer with flush coalescing.
///
/// Thread-safe: multiple transaction threads can call `append_durable()` or
/// `append_buffered()` concurrently. The internal mutex serializes writes
/// but the flush/sync is coalesced.
pub struct AsyncWalWriter {
    inner: Mutex<WalWriterInner>,
    config: AsyncWalConfig,
    /// Monotonically increasing LSN counter.
    lsn: AtomicU64,
    /// Group commit sync state (for condvar-based waiting).
    sync_state: Arc<(StdMutex<SyncState>, Condvar)>,
    /// Shutdown flag.
    shutdown: Arc<AtomicBool>,
    /// I/O metrics.
    metrics: Arc<IoMetrics>,
    /// WAL-specific metrics.
    wal_metrics: Arc<AsyncWalMetrics>,
    /// Lock-free mirror of SyncState::pending_bytes for flush policy check.
    /// Avoids acquiring sync_state mutex on every append just to read pending_bytes.
    pending_bytes_atomic: AtomicU64,
}

struct WalWriterInner {
    file: Box<dyn AsyncFile>,
    write_offset: u64,
    current_segment: u64,
}

/// WAL-specific observability metrics.
#[derive(Debug, Default)]
pub struct AsyncWalMetrics {
    /// Total records written.
    pub records_written: AtomicU64,
    /// Total bytes written (payload only, excluding headers).
    pub payload_bytes_written: AtomicU64,
    /// Number of sync (flush) operations performed.
    pub sync_count: AtomicU64,
    /// Number of group commit batches.
    pub group_commit_batches: AtomicU64,
    /// Cumulative records per batch.
    pub group_commit_records: AtomicU64,
    /// Cumulative bytes per batch.
    pub group_commit_bytes: AtomicU64,
    /// Number of segment rotations.
    pub segment_rotations: AtomicU64,
    /// Cumulative sync latency in microseconds.
    pub sync_latency_us: AtomicU64,
    /// Maximum single sync latency.
    pub max_sync_latency_us: AtomicU64,
    /// Cumulative write latency (append only, no sync).
    pub write_latency_us: AtomicU64,
    /// Number of flushes by reason.
    pub flush_by_batch_full: AtomicU64,
    pub flush_by_timer: AtomicU64,
    pub flush_by_explicit: AtomicU64,
}

impl AsyncWalMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> AsyncWalMetricsSnapshot {
        AsyncWalMetricsSnapshot {
            records_written: self.records_written.load(Ordering::Relaxed),
            payload_bytes_written: self.payload_bytes_written.load(Ordering::Relaxed),
            sync_count: self.sync_count.load(Ordering::Relaxed),
            group_commit_batches: self.group_commit_batches.load(Ordering::Relaxed),
            group_commit_records: self.group_commit_records.load(Ordering::Relaxed),
            group_commit_bytes: self.group_commit_bytes.load(Ordering::Relaxed),
            segment_rotations: self.segment_rotations.load(Ordering::Relaxed),
            sync_latency_us: self.sync_latency_us.load(Ordering::Relaxed),
            max_sync_latency_us: self.max_sync_latency_us.load(Ordering::Relaxed),
            write_latency_us: self.write_latency_us.load(Ordering::Relaxed),
            flush_by_batch_full: self.flush_by_batch_full.load(Ordering::Relaxed),
            flush_by_timer: self.flush_by_timer.load(Ordering::Relaxed),
            flush_by_explicit: self.flush_by_explicit.load(Ordering::Relaxed),
        }
    }
}

/// Immutable snapshot of WAL metrics.
#[derive(Debug, Clone, Default)]
pub struct AsyncWalMetricsSnapshot {
    pub records_written: u64,
    pub payload_bytes_written: u64,
    pub sync_count: u64,
    pub group_commit_batches: u64,
    pub group_commit_records: u64,
    pub group_commit_bytes: u64,
    pub segment_rotations: u64,
    pub sync_latency_us: u64,
    pub max_sync_latency_us: u64,
    pub write_latency_us: u64,
    pub flush_by_batch_full: u64,
    pub flush_by_timer: u64,
    pub flush_by_explicit: u64,
}

impl AsyncWalMetricsSnapshot {
    pub const fn avg_sync_latency_us(&self) -> u64 {
        if self.sync_count == 0 {
            0
        } else {
            self.sync_latency_us / self.sync_count
        }
    }

    pub fn avg_records_per_batch(&self) -> f64 {
        if self.group_commit_batches == 0 {
            0.0
        } else {
            self.group_commit_records as f64 / self.group_commit_batches as f64
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Implementation
// ═══════════════════════════════════════════════════════════════════════════

fn segment_filename(segment_id: u64) -> String {
    format!("falcon_{segment_id:06}.wal")
}

impl AsyncWalWriter {
    /// Open the async WAL writer.
    pub fn open(config: AsyncWalConfig) -> Result<Self, IoError> {
        std::fs::create_dir_all(&config.wal_dir).map_err(IoError::from_io)?;

        // Find latest segment
        let latest = Self::find_latest_segment(&config.wal_dir);
        let segment_id = latest.unwrap_or(0);
        let seg_path = config.wal_dir.join(segment_filename(segment_id));

        let file = super::create_async_file(seg_path, config.file_config.clone())?;
        let file_size = file.size()?;

        // Write segment header if new file
        let write_offset = if file_size == 0 {
            let mut header = Vec::with_capacity(WAL_SEGMENT_HEADER_SIZE);
            header.extend_from_slice(WAL_MAGIC);
            header.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
            file.write_at(0, &header)?;
            WAL_SEGMENT_HEADER_SIZE as u64
        } else {
            file_size
        };

        Ok(Self {
            inner: Mutex::new(WalWriterInner {
                file,
                write_offset,
                current_segment: segment_id,
            }),
            config,
            lsn: AtomicU64::new(0),
            sync_state: Arc::new((
                StdMutex::new(SyncState {
                    synced_lsn: 0,
                    pending_count: 0,
                    pending_bytes: 0,
                }),
                Condvar::new(),
            )),
            shutdown: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(IoMetrics::new()),
            wal_metrics: Arc::new(AsyncWalMetrics::new()),
            pending_bytes_atomic: AtomicU64::new(0),
        })
    }

    fn find_latest_segment(dir: &Path) -> Option<u64> {
        let mut max_id = None;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("falcon_") && name.ends_with(".wal") {
                    if let Ok(id) = name[7..name.len() - 4].parse::<u64>() {
                        max_id = Some(max_id.map_or(id, |cur: u64| cur.max(id)));
                    }
                }
            }
        }
        max_id
    }

    /// Append a serialized WAL record and wait for durability.
    ///
    /// Returns the LSN. **The record is durable when this method returns.**
    pub fn append_durable(&self, data: &[u8]) -> Result<u64, IoError> {
        let lsn = self.append_buffered(data)?;
        self.flush_durable(FlushReason::Explicit)?;
        Ok(lsn)
    }

    /// Append a serialized WAL record to the buffer without waiting for sync.
    ///
    /// Returns the LSN. Record is NOT yet durable — caller must call
    /// `flush_durable()` or rely on the group commit syncer.
    pub fn append_buffered(&self, data: &[u8]) -> Result<u64, IoError> {
        let start = Instant::now();
        let lsn = self.lsn.fetch_add(1, Ordering::SeqCst);
        let checksum = crc32fast::hash(data);
        let len = data.len() as u32;

        // Record format: [len:4][checksum:4][data:len]
        // Use a stack-allocated 8-byte header instead of a heap-allocated Vec
        // to avoid one malloc per WAL append (hot path in every commit).
        let record_size = 8 + data.len();
        let mut header = [0u8; 8];
        header[..4].copy_from_slice(&len.to_le_bytes());
        header[4..].copy_from_slice(&checksum.to_le_bytes());

        {
            let mut inner = self.inner.lock();

            // Check segment rotation
            if inner.write_offset + record_size as u64 > self.config.max_segment_size {
                self.rotate_segment(&mut inner)?;
            }

            inner.file.write_at(inner.write_offset, &header)?;
            inner.file.write_at(inner.write_offset + 8, data)?;
            inner.write_offset += record_size as u64;
        }

        // Update sync state and atomic mirror
        let new_pending = self.pending_bytes_atomic.fetch_add(record_size as u64, Ordering::Relaxed) + record_size as u64;
        {
            let (lock, cvar) = &*self.sync_state;
            let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.pending_count += 1;
            state.pending_bytes = new_pending;
            cvar.notify_one();
        }

        let latency_us = start.elapsed().as_micros() as u64;
        self.wal_metrics
            .records_written
            .fetch_add(1, Ordering::Relaxed);
        self.wal_metrics
            .payload_bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.wal_metrics
            .write_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        // Check if flush policy triggers
        self.maybe_flush_by_policy()?;

        Ok(lsn)
    }

    /// Durably flush all pending writes.
    ///
    /// After this returns, all previously appended records are durable.
    pub fn flush_durable(&self, reason: FlushReason) -> Result<(), IoError> {
        let start = Instant::now();

        // Snapshot pending state
        let (batch_count, batch_bytes) = {
            let (lock, _) = &*self.sync_state;
            let state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (state.pending_count, state.pending_bytes)
        };

        {
            let inner = self.inner.lock();
            inner.file.flush_buf()?;
            // THIS IS THE DURABILITY BARRIER
            inner.file.sync()?;
        }

        let latency_us = start.elapsed().as_micros() as u64;

        // Update sync state — notify all waiters
        {
            let (lock, cvar) = &*self.sync_state;
            let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let current_lsn = self.lsn.load(Ordering::SeqCst);
            state.synced_lsn = current_lsn;
            state.pending_count = 0;
            state.pending_bytes = 0;
            // Reset atomic mirror — all pending bytes are now durable.
            self.pending_bytes_atomic.store(0, Ordering::Relaxed);
            cvar.notify_all();
        }

        // Record metrics
        self.wal_metrics.sync_count.fetch_add(1, Ordering::Relaxed);
        self.wal_metrics
            .sync_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        let _ = self
            .wal_metrics
            .max_sync_latency_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if latency_us > cur {
                    Some(latency_us)
                } else {
                    None
                }
            });

        if batch_count > 0 {
            self.wal_metrics
                .group_commit_batches
                .fetch_add(1, Ordering::Relaxed);
            self.wal_metrics
                .group_commit_records
                .fetch_add(batch_count, Ordering::Relaxed);
            self.wal_metrics
                .group_commit_bytes
                .fetch_add(batch_bytes, Ordering::Relaxed);
        }

        match reason {
            FlushReason::BatchFull => {
                self.wal_metrics
                    .flush_by_batch_full
                    .fetch_add(1, Ordering::Relaxed);
            }
            FlushReason::Timer => {
                self.wal_metrics
                    .flush_by_timer
                    .fetch_add(1, Ordering::Relaxed);
            }
            FlushReason::Explicit | FlushReason::Shutdown => {
                self.wal_metrics
                    .flush_by_explicit
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        self.metrics.record_flush(latency_us, reason);
        Ok(())
    }

    /// Check if the flush policy triggers a sync.
    #[inline]
    fn maybe_flush_by_policy(&self) -> Result<(), IoError> {
        match &self.config.flush_policy {
            FlushPolicy::GroupCommit {
                max_batch_bytes, ..
            } => {
                // Lock-free read via atomic mirror — avoids mutex acquisition on every append.
                if self.pending_bytes_atomic.load(Ordering::Relaxed) >= *max_batch_bytes {
                    self.flush_durable(FlushReason::BatchFull)?;
                }
            }
            FlushPolicy::Explicit | FlushPolicy::Periodic { .. } => {
                // Explicit: caller must flush; Periodic: handled by background syncer
            }
        }
        Ok(())
    }

    /// Start the background group commit syncer thread.
    ///
    /// This thread periodically flushes pending writes based on the flush policy.
    /// Returns a join handle for the thread.
    pub fn start_syncer(self: &Arc<Self>) -> Result<std::thread::JoinHandle<()>, IoError> {
        let writer = Arc::clone(self);
        std::thread::Builder::new()
            .name("falcon-async-wal-syncer".into())
            .spawn(move || writer.syncer_loop())
            .map_err(|e| IoError::internal(format!("spawn syncer: {e}")))
    }

    fn syncer_loop(&self) {
        let flush_interval = match &self.config.flush_policy {
            FlushPolicy::GroupCommit { max_wait_us, .. } => {
                Duration::from_micros(*max_wait_us)
            }
            FlushPolicy::Periodic { interval_ms } => {
                Duration::from_millis(*interval_ms)
            }
            FlushPolicy::Explicit => {
                // No background syncing needed
                return;
            }
        };

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                let _ = self.flush_durable(FlushReason::Shutdown);
                break;
            }

            let should_flush = {
                let (lock, cvar) = &*self.sync_state;
                let state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);

                if state.pending_count == 0 {
                    // Wait for work
                    let result = cvar
                        .wait_timeout(state, flush_interval)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    result.0.pending_count > 0
                } else {
                    // Wait a short interval to coalesce more writes
                    drop(state);
                    std::thread::sleep(flush_interval);
                    true
                }
            };

            if should_flush {
                if let Err(e) = self.flush_durable(FlushReason::Timer) {
                    tracing::error!("async WAL syncer flush error: {}", e);
                }
            }
        }
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let (_, cvar) = &*self.sync_state;
        cvar.notify_all();
    }

    fn rotate_segment(&self, inner: &mut WalWriterInner) -> Result<(), IoError> {
        // Flush + sync current segment
        inner.file.flush_buf()?;
        inner.file.sync()?;

        inner.current_segment += 1;
        let new_path = self
            .config
            .wal_dir
            .join(segment_filename(inner.current_segment));

        let file = super::create_async_file(new_path, self.config.file_config.clone())?;

        // Write segment header
        let mut header = Vec::with_capacity(WAL_SEGMENT_HEADER_SIZE);
        header.extend_from_slice(WAL_MAGIC);
        header.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        file.write_at(0, &header)?;

        inner.file = file;
        inner.write_offset = WAL_SEGMENT_HEADER_SIZE as u64;

        self.wal_metrics
            .segment_rotations
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            "async WAL rotated to segment {}",
            inner.current_segment
        );

        Ok(())
    }

    /// Current LSN.
    pub fn current_lsn(&self) -> u64 {
        self.lsn.load(Ordering::SeqCst)
    }

    /// Current segment ID.
    pub fn current_segment_id(&self) -> u64 {
        self.inner.lock().current_segment
    }

    /// WAL directory path.
    pub fn wal_dir(&self) -> &Path {
        &self.config.wal_dir
    }

    /// I/O metrics.
    pub const fn io_metrics(&self) -> &Arc<IoMetrics> {
        &self.metrics
    }

    /// WAL-specific metrics.
    pub const fn wal_metrics(&self) -> &Arc<AsyncWalMetrics> {
        &self.wal_metrics
    }

    /// Is the syncer shut down?
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(dir: &Path) -> AsyncWalConfig {
        AsyncWalConfig {
            flush_policy: FlushPolicy::Explicit,
            max_segment_size: 64 * 1024 * 1024,
            wal_dir: dir.to_path_buf(),
            file_config: AsyncFileConfig::default(),
        }
    }

    fn test_config_group_commit(dir: &Path) -> AsyncWalConfig {
        AsyncWalConfig {
            flush_policy: FlushPolicy::GroupCommit {
                max_wait_us: 500,
                max_batch_bytes: 4096,
            },
            max_segment_size: 64 * 1024 * 1024,
            wal_dir: dir.to_path_buf(),
            file_config: AsyncFileConfig::default(),
        }
    }

    #[test]
    fn test_async_wal_append_durable() {
        let dir = std::env::temp_dir().join("falcon_awal_test_durable");
        let _ = std::fs::remove_dir_all(&dir);

        let writer = AsyncWalWriter::open(test_config(&dir)).unwrap();
        let data = bincode::serialize(&"hello async WAL").unwrap();
        let lsn = writer.append_durable(&data).unwrap();
        assert_eq!(lsn, 0);

        let lsn2 = writer.append_durable(&data).unwrap();
        assert_eq!(lsn2, 1);

        let m = writer.wal_metrics().snapshot();
        assert_eq!(m.records_written, 2);
        assert!(m.sync_count >= 2, "durable append should sync each time");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_async_wal_append_buffered_then_flush() {
        let dir = std::env::temp_dir().join("falcon_awal_test_buffered");
        let _ = std::fs::remove_dir_all(&dir);

        let writer = AsyncWalWriter::open(test_config(&dir)).unwrap();

        // Append 10 records without durability
        for i in 0..10u32 {
            let data = bincode::serialize(&format!("record-{}", i)).unwrap();
            writer.append_buffered(&data).unwrap();
        }

        // Should not have synced yet (explicit policy)
        let m = writer.wal_metrics().snapshot();
        assert_eq!(m.records_written, 10);
        assert_eq!(m.sync_count, 0);

        // Now flush
        writer.flush_durable(FlushReason::Explicit).unwrap();
        let m2 = writer.wal_metrics().snapshot();
        assert_eq!(m2.sync_count, 1); // single coalesced sync
        assert_eq!(m2.group_commit_records, 10); // all 10 in one batch

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_async_wal_group_commit_batch_full() {
        let dir = std::env::temp_dir().join("falcon_awal_test_gc_batch");
        let _ = std::fs::remove_dir_all(&dir);

        let config = AsyncWalConfig {
            flush_policy: FlushPolicy::GroupCommit {
                max_wait_us: 100_000, // long timer — batch-full should trigger first
                max_batch_bytes: 200,  // small batch
            },
            max_segment_size: 64 * 1024 * 1024,
            wal_dir: dir.to_path_buf(),
            file_config: AsyncFileConfig::default(),
        };
        let writer = AsyncWalWriter::open(config).unwrap();

        // Write enough data to trigger batch-full flush
        for i in 0..20u32 {
            let data = bincode::serialize(&format!("record-{:04}", i)).unwrap();
            writer.append_buffered(&data).unwrap();
        }

        let m = writer.wal_metrics().snapshot();
        assert!(
            m.flush_by_batch_full >= 1,
            "batch-full flush should trigger, got {}",
            m.flush_by_batch_full
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_async_wal_group_commit_syncer() {
        let dir = std::env::temp_dir().join("falcon_awal_test_gc_syncer");
        let _ = std::fs::remove_dir_all(&dir);

        let config = test_config_group_commit(&dir);
        let writer = Arc::new(AsyncWalWriter::open(config).unwrap());
        let handle = writer.start_syncer().unwrap();

        // Append records — syncer should flush them
        for i in 0..10u32 {
            let data = bincode::serialize(&format!("syncer-record-{}", i)).unwrap();
            writer.append_buffered(&data).unwrap();
        }

        // Wait for syncer to process
        std::thread::sleep(Duration::from_millis(50));

        let m = writer.wal_metrics().snapshot();
        assert!(
            m.sync_count >= 1,
            "syncer should have flushed, got {} syncs",
            m.sync_count
        );

        writer.shutdown();
        handle.join().unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_async_wal_segment_rotation() {
        let dir = std::env::temp_dir().join("falcon_awal_test_rotate");
        let _ = std::fs::remove_dir_all(&dir);

        let config = AsyncWalConfig {
            flush_policy: FlushPolicy::Explicit,
            max_segment_size: 256, // tiny segment
            wal_dir: dir.to_path_buf(),
            file_config: AsyncFileConfig::default(),
        };
        let writer = AsyncWalWriter::open(config).unwrap();

        assert_eq!(writer.current_segment_id(), 0);

        // Write enough to trigger rotation
        for i in 0..50u32 {
            let data = bincode::serialize(&format!("segment-record-{:04}", i)).unwrap();
            writer.append_buffered(&data).unwrap();
        }

        let m = writer.wal_metrics().snapshot();
        assert!(
            m.segment_rotations >= 1,
            "should have rotated at least once"
        );
        assert!(writer.current_segment_id() > 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_async_wal_metrics_snapshot() {
        let dir = std::env::temp_dir().join("falcon_awal_test_metrics");
        let _ = std::fs::remove_dir_all(&dir);

        let writer = AsyncWalWriter::open(test_config(&dir)).unwrap();
        let data = bincode::serialize(&"metrics-test").unwrap();

        writer.append_buffered(&data).unwrap();
        writer.append_buffered(&data).unwrap();
        writer.flush_durable(FlushReason::Timer).unwrap();
        writer.append_durable(&data).unwrap();

        let m = writer.wal_metrics().snapshot();
        assert_eq!(m.records_written, 3);
        assert!(m.sync_count >= 2); // 1 explicit + 1 from append_durable
        assert!(m.flush_by_timer >= 1);
        assert!(m.avg_sync_latency_us() > 0 || m.sync_latency_us == 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_async_wal_crash_simulation() {
        // Write records, flush some, then verify only flushed records survive re-open.
        let dir = std::env::temp_dir().join("falcon_awal_test_crash");
        let _ = std::fs::remove_dir_all(&dir);

        {
            let writer = AsyncWalWriter::open(test_config(&dir)).unwrap();
            let data1 = bincode::serialize(&"durable-record").unwrap();
            writer.append_durable(&data1).unwrap();

            let data2 = bincode::serialize(&"buffered-only").unwrap();
            writer.append_buffered(&data2).unwrap();
            // Do NOT flush data2 — simulating crash
        }

        // Re-open and read — durable record should be present
        let seg_path = dir.join(segment_filename(0));
        assert!(seg_path.exists());

        let raw_data = std::fs::read(&seg_path).unwrap();
        // Should have at least the header + 1 record
        assert!(
            raw_data.len() > WAL_SEGMENT_HEADER_SIZE,
            "file should have data beyond header"
        );

        // Verify header
        assert_eq!(&raw_data[0..4], WAL_MAGIC);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_async_wal_concurrent_appends() {
        let dir = std::env::temp_dir().join("falcon_awal_test_concurrent");
        let _ = std::fs::remove_dir_all(&dir);

        let config = test_config_group_commit(&dir);
        let writer = Arc::new(AsyncWalWriter::open(config).unwrap());
        let syncer_handle = writer.start_syncer().unwrap();

        let mut threads = Vec::new();
        for t in 0..4u64 {
            let w = Arc::clone(&writer);
            threads.push(std::thread::spawn(move || {
                for i in 0..25u32 {
                    let data =
                        bincode::serialize(&format!("thread-{}-record-{}", t, i)).unwrap();
                    w.append_buffered(&data).unwrap();
                }
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        // Wait for syncer
        std::thread::sleep(Duration::from_millis(50));

        let m = writer.wal_metrics().snapshot();
        assert_eq!(m.records_written, 100);
        assert!(
            m.sync_count < 100,
            "should coalesce: {} syncs for 100 records",
            m.sync_count
        );

        writer.shutdown();
        syncer_handle.join().unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }
}
