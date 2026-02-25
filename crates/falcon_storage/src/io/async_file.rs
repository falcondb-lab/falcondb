//! Unified async file I/O trait and supporting types.
//!
//! `AsyncFile` is the core abstraction consumed by the WAL writer and
//! snapshot streaming. Platform backends implement this trait.
//!
//! ## Durability Contract
//!
//! **`flush()` must call `FlushFileBuffers` (Windows) or `fsync`/`fdatasync`
//! (Unix). No path may claim durability without flush completion.**
//!
//! ## Threading Model
//!
//! All operations return `Result` and are designed to be called from async
//! contexts. The backends ensure disk I/O does not block the Tokio core
//! threads — either via IOCP completion threads or `spawn_blocking`.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — IoError
// ═══════════════════════════════════════════════════════════════════════════

/// Unified I/O error type for async file operations.
#[derive(Debug)]
pub struct IoError {
    pub kind: IoErrorKind,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Error categories for async file I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoErrorKind {
    /// Operation timed out.
    Timeout,
    /// Operation was cancelled (e.g. shutdown).
    Cancelled,
    /// File/device not accessible.
    Unreachable,
    /// Permission denied.
    PermissionDenied,
    /// Disk full / no space left.
    DiskFull,
    /// Data corruption detected (checksum mismatch).
    Corrupt,
    /// Invalid argument or state.
    InvalidArgument,
    /// Generic internal error.
    Internal,
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for IoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl IoError {
    pub fn new(kind: IoErrorKind, msg: impl Into<String>) -> Self {
        Self {
            kind,
            message: msg.into(),
            source: None,
        }
    }

    pub fn with_source(
        kind: IoErrorKind,
        msg: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            message: msg.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(IoErrorKind::Internal, msg)
    }

    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::new(IoErrorKind::Timeout, msg)
    }

    /// Map a standard `std::io::Error` to `IoError`.
    pub fn from_io(e: std::io::Error) -> Self {
        let kind = match e.kind() {
            std::io::ErrorKind::TimedOut => IoErrorKind::Timeout,
            std::io::ErrorKind::PermissionDenied => IoErrorKind::PermissionDenied,
            std::io::ErrorKind::NotFound => IoErrorKind::Unreachable,
            std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => {
                IoErrorKind::InvalidArgument
            }
            _ => {
                // Check for disk-full on Windows (ERROR_DISK_FULL = 112)
                #[cfg(target_os = "windows")]
                {
                    if let Some(raw) = e.raw_os_error() {
                        if raw == 112 {
                            return Self::with_source(IoErrorKind::DiskFull, "disk full", e);
                        }
                    }
                }
                // Check for disk-full on Unix (ENOSPC = 28)
                #[cfg(not(target_os = "windows"))]
                {
                    if let Some(raw) = e.raw_os_error() {
                        if raw == 28 {
                            return Self::with_source(IoErrorKind::DiskFull, "no space left", e);
                        }
                    }
                }
                IoErrorKind::Internal
            }
        };
        Self::with_source(kind, format!("{e}"), e)
    }
}

impl From<std::io::Error> for IoError {
    fn from(e: std::io::Error) -> Self {
        Self::from_io(e)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — AsyncFile Trait
// ═══════════════════════════════════════════════════════════════════════════

/// Async file I/O interface.
///
/// All operations are synchronous at the Rust type level but internally
/// offload to non-blocking I/O (IOCP or spawn_blocking). The WAL writer
/// calls these from a dedicated task, never from a hot async path.
pub trait AsyncFile: Send + Sync {
    /// Write `buf` at the given byte offset. For append-only files (WAL),
    /// `offset` is the current file size.
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize, IoError>;

    /// Read `len` bytes starting at `offset`.
    fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, IoError>;

    /// Flush all buffered writes to the OS page cache.
    /// Does NOT guarantee durability — call `sync()` for that.
    fn flush_buf(&self) -> Result<(), IoError>;

    /// Durably sync all data to disk.
    /// On Windows: `FlushFileBuffers`.
    /// On Unix: `fsync` or `fdatasync`.
    /// **This is the durability barrier. No commit is durable without this.**
    fn sync(&self) -> Result<(), IoError>;

    /// Get the current file size in bytes.
    fn size(&self) -> Result<u64, IoError>;

    /// Get the file path.
    fn path(&self) -> &std::path::Path;

    /// Get current inflight operation count.
    fn inflight_ops(&self) -> u64;

    /// Get current inflight bytes.
    fn inflight_bytes(&self) -> u64;
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for the async file backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncFileConfig {
    /// Enable true async I/O (IOCP on Windows). When false, uses sync fallback.
    pub async_io_enabled: bool,
    /// Maximum bytes in-flight (backpressure). 0 = unlimited.
    pub max_inflight_bytes: u64,
    /// Maximum concurrent operations. 0 = unlimited.
    pub max_inflight_ops: u32,
    /// Number of IOCP completion threads (Windows only).
    pub iocp_thread_count: u32,
    /// Whether to use FILE_FLAG_WRITE_THROUGH (Windows). Usually false —
    /// rely on explicit FlushFileBuffers instead for better batching.
    pub write_through: bool,
    /// Whether to use FILE_FLAG_SEQUENTIAL_SCAN hint (WAL files).
    pub sequential_scan: bool,
}

impl Default for AsyncFileConfig {
    fn default() -> Self {
        Self {
            async_io_enabled: cfg!(target_os = "windows"),
            max_inflight_bytes: 64 * 1024 * 1024, // 64 MB
            max_inflight_ops: 256,
            iocp_thread_count: 4,
            write_through: false,
            sequential_scan: true,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Flush Policy
// ═══════════════════════════════════════════════════════════════════════════

/// WAL flush policy — controls when `FlushFileBuffers`/`fsync` is called.
///
/// **Invariant**: durable commit requires flush completion regardless of policy.
/// The policy only controls *when* the flush is triggered, not whether it happens.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FlushPolicy {
    /// Group commit: flush when batch is full OR max wait time reached.
    /// This is the production default.
    GroupCommit {
        /// Maximum microseconds to wait before flushing.
        max_wait_us: u64,
        /// Maximum bytes to accumulate before flushing.
        max_batch_bytes: u64,
    },
    /// Periodic: flush at fixed intervals regardless of batch size.
    Periodic {
        /// Flush interval in milliseconds.
        interval_ms: u64,
    },
    /// Explicit: flush only when explicitly requested (tests, admin).
    Explicit,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self::GroupCommit {
            max_wait_us: 1000,              // 1ms
            max_batch_bytes: 4 * 1024 * 1024, // 4MB
        }
    }
}

/// Reason a flush was triggered — used for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// Batch size limit reached.
    BatchFull,
    /// Timer expired.
    Timer,
    /// Explicit flush request (DDL, admin, shutdown).
    Explicit,
    /// Shutdown flush.
    Shutdown,
}

impl FlushReason {
    pub const fn label(&self) -> &'static str {
        match self {
            Self::BatchFull => "batch_full",
            Self::Timer => "timer",
            Self::Explicit => "explicit",
            Self::Shutdown => "shutdown",
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — I/O Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Cumulative I/O metrics for the async file layer.
#[derive(Debug, Default)]
pub struct IoMetrics {
    /// Total bytes written.
    pub bytes_written: AtomicU64,
    /// Total bytes read.
    pub bytes_read: AtomicU64,
    /// Total write operations.
    pub write_ops: AtomicU64,
    /// Total read operations.
    pub read_ops: AtomicU64,
    /// Total flush (sync) operations.
    pub flush_ops: AtomicU64,
    /// Cumulative flush latency in microseconds.
    pub flush_latency_us: AtomicU64,
    /// Maximum single flush latency in microseconds.
    pub max_flush_latency_us: AtomicU64,
    /// Cumulative write latency in microseconds.
    pub write_latency_us: AtomicU64,
    /// Number of flushes triggered by batch-full.
    pub flush_reason_batch_full: AtomicU64,
    /// Number of flushes triggered by timer.
    pub flush_reason_timer: AtomicU64,
    /// Number of explicit flushes.
    pub flush_reason_explicit: AtomicU64,
    /// Current inflight bytes (gauge).
    pub inflight_bytes: AtomicU64,
    /// Current inflight ops (gauge).
    pub inflight_ops: AtomicU64,
}

impl IoMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_write(&self, bytes: u64, latency_us: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        self.write_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
        self.read_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_flush(&self, latency_us: u64, reason: FlushReason) {
        self.flush_ops.fetch_add(1, Ordering::Relaxed);
        self.flush_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        let _ = self
            .max_flush_latency_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if latency_us > cur {
                    Some(latency_us)
                } else {
                    None
                }
            });
        match reason {
            FlushReason::BatchFull => {
                self.flush_reason_batch_full
                    .fetch_add(1, Ordering::Relaxed);
            }
            FlushReason::Timer => {
                self.flush_reason_timer.fetch_add(1, Ordering::Relaxed);
            }
            FlushReason::Explicit | FlushReason::Shutdown => {
                self.flush_reason_explicit.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> IoMetricsSnapshot {
        IoMetricsSnapshot {
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            write_ops: self.write_ops.load(Ordering::Relaxed),
            read_ops: self.read_ops.load(Ordering::Relaxed),
            flush_ops: self.flush_ops.load(Ordering::Relaxed),
            flush_latency_us: self.flush_latency_us.load(Ordering::Relaxed),
            max_flush_latency_us: self.max_flush_latency_us.load(Ordering::Relaxed),
            write_latency_us: self.write_latency_us.load(Ordering::Relaxed),
            flush_reason_batch_full: self.flush_reason_batch_full.load(Ordering::Relaxed),
            flush_reason_timer: self.flush_reason_timer.load(Ordering::Relaxed),
            flush_reason_explicit: self.flush_reason_explicit.load(Ordering::Relaxed),
            inflight_bytes: self.inflight_bytes.load(Ordering::Relaxed),
            inflight_ops: self.inflight_ops.load(Ordering::Relaxed),
        }
    }
}

/// Immutable snapshot of I/O metrics.
#[derive(Debug, Clone, Default)]
pub struct IoMetricsSnapshot {
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub write_ops: u64,
    pub read_ops: u64,
    pub flush_ops: u64,
    pub flush_latency_us: u64,
    pub max_flush_latency_us: u64,
    pub write_latency_us: u64,
    pub flush_reason_batch_full: u64,
    pub flush_reason_timer: u64,
    pub flush_reason_explicit: u64,
    pub inflight_bytes: u64,
    pub inflight_ops: u64,
}

impl IoMetricsSnapshot {
    /// Average flush latency in microseconds.
    pub const fn avg_flush_latency_us(&self) -> u64 {
        if self.flush_ops == 0 {
            0
        } else {
            self.flush_latency_us / self.flush_ops
        }
    }

    /// Average write latency in microseconds.
    pub const fn avg_write_latency_us(&self) -> u64 {
        if self.write_ops == 0 {
            0
        } else {
            self.write_latency_us / self.write_ops
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Snapshot Streaming Config
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for snapshot chunk streaming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStreamConfig {
    /// Size of each snapshot chunk in bytes.
    pub chunk_size_bytes: usize,
    /// Maximum concurrent inflight chunks (backpressure).
    pub max_inflight_chunks: usize,
    /// Read-ahead: prefetch this many chunks.
    pub read_ahead: usize,
}

impl Default for SnapshotStreamConfig {
    fn default() -> Self {
        Self {
            chunk_size_bytes: 1024 * 1024, // 1 MB
            max_inflight_chunks: 4,
            read_ahead: 2,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_from_std() {
        let e = IoError::from(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "nope",
        ));
        assert_eq!(e.kind, IoErrorKind::PermissionDenied);
        assert!(e.message.contains("nope"));
    }

    #[test]
    fn test_io_error_display() {
        let e = IoError::new(IoErrorKind::DiskFull, "no space");
        assert_eq!(format!("{}", e), "DiskFull: no space");
    }

    #[test]
    fn test_io_error_internal() {
        let e = IoError::internal("bad state");
        assert_eq!(e.kind, IoErrorKind::Internal);
    }

    #[test]
    fn test_async_file_config_default() {
        let cfg = AsyncFileConfig::default();
        assert_eq!(cfg.max_inflight_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.max_inflight_ops, 256);
        assert!(!cfg.write_through);
        assert!(cfg.sequential_scan);
    }

    #[test]
    fn test_flush_policy_default() {
        let fp = FlushPolicy::default();
        match fp {
            FlushPolicy::GroupCommit {
                max_wait_us,
                max_batch_bytes,
            } => {
                assert_eq!(max_wait_us, 1000);
                assert_eq!(max_batch_bytes, 4 * 1024 * 1024);
            }
            _ => panic!("expected GroupCommit"),
        }
    }

    #[test]
    fn test_flush_reason_labels() {
        assert_eq!(FlushReason::BatchFull.label(), "batch_full");
        assert_eq!(FlushReason::Timer.label(), "timer");
        assert_eq!(FlushReason::Explicit.label(), "explicit");
        assert_eq!(FlushReason::Shutdown.label(), "shutdown");
    }

    #[test]
    fn test_io_metrics_record() {
        let m = IoMetrics::new();
        m.record_write(4096, 100);
        m.record_write(2048, 200);
        m.record_read(1024);
        m.record_flush(500, FlushReason::BatchFull);
        m.record_flush(1500, FlushReason::Timer);

        let s = m.snapshot();
        assert_eq!(s.bytes_written, 6144);
        assert_eq!(s.bytes_read, 1024);
        assert_eq!(s.write_ops, 2);
        assert_eq!(s.read_ops, 1);
        assert_eq!(s.flush_ops, 2);
        assert_eq!(s.flush_reason_batch_full, 1);
        assert_eq!(s.flush_reason_timer, 1);
        assert_eq!(s.max_flush_latency_us, 1500);
        assert_eq!(s.avg_flush_latency_us(), 1000);
        assert_eq!(s.avg_write_latency_us(), 150);
    }

    #[test]
    fn test_io_metrics_snapshot_zero_division() {
        let s = IoMetricsSnapshot::default();
        assert_eq!(s.avg_flush_latency_us(), 0);
        assert_eq!(s.avg_write_latency_us(), 0);
    }

    #[test]
    fn test_snapshot_stream_config_default() {
        let cfg = SnapshotStreamConfig::default();
        assert_eq!(cfg.chunk_size_bytes, 1024 * 1024);
        assert_eq!(cfg.max_inflight_chunks, 4);
    }
}
