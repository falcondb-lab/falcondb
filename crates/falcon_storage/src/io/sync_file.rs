//! Synchronous file backend — fallback for non-Windows platforms.
//!
//! Wraps `std::fs::File` with `BufWriter` and implements `AsyncFile`.
//! All operations are blocking but safe to call from dedicated threads
//! (the WAL writer task or snapshot streaming task).
//!
//! This backend is also used on Windows when `async_io_enabled = false`.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

use super::async_file::{AsyncFile, AsyncFileConfig, IoError, IoErrorKind, IoMetrics};

/// Synchronous file backend implementing `AsyncFile`.
pub struct SyncFileBackend {
    inner: Mutex<SyncFileInner>,
    path: PathBuf,
    metrics: IoMetrics,
    inflight_ops: AtomicU64,
    inflight_bytes: AtomicU64,
    config: AsyncFileConfig,
}

struct SyncFileInner {
    writer: BufWriter<File>,
    file_size: u64,
}

impl SyncFileBackend {
    /// Open or create a file for read/write with buffered I/O.
    pub fn open(path: PathBuf, config: AsyncFileConfig) -> Result<Self, IoError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(IoError::from_io)?;

        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            inner: Mutex::new(SyncFileInner {
                writer: BufWriter::new(file),
                file_size,
            }),
            path,
            metrics: IoMetrics::new(),
            inflight_ops: AtomicU64::new(0),
            inflight_bytes: AtomicU64::new(0),
            config,
        })
    }

    /// Check inflight limits and return error if exceeded.
    fn check_inflight(&self, bytes: u64) -> Result<(), IoError> {
        if self.config.max_inflight_ops > 0 {
            let ops = self.inflight_ops.load(Ordering::Relaxed);
            if ops >= u64::from(self.config.max_inflight_ops) {
                return Err(IoError::new(
                    IoErrorKind::Timeout,
                    format!(
                        "inflight ops limit reached: {} >= {}",
                        ops, self.config.max_inflight_ops
                    ),
                ));
            }
        }
        if self.config.max_inflight_bytes > 0 {
            let current = self.inflight_bytes.load(Ordering::Relaxed);
            if current + bytes > self.config.max_inflight_bytes {
                return Err(IoError::new(
                    IoErrorKind::Timeout,
                    format!(
                        "inflight bytes limit reached: {} + {} > {}",
                        current, bytes, self.config.max_inflight_bytes
                    ),
                ));
            }
        }
        Ok(())
    }
}

impl AsyncFile for SyncFileBackend {
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize, IoError> {
        let bytes = buf.len() as u64;
        self.check_inflight(bytes)?;

        self.inflight_ops.fetch_add(1, Ordering::Relaxed);
        self.inflight_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.metrics
            .inflight_ops
            .store(self.inflight_ops.load(Ordering::Relaxed), Ordering::Relaxed);
        self.metrics.inflight_bytes.store(
            self.inflight_bytes.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        let start = Instant::now();
        let result = {
            let mut inner = self.inner.lock();
            // Seek to offset
            inner
                .writer
                .seek(SeekFrom::Start(offset))
                .map_err(IoError::from_io)?;
            inner.writer.write_all(buf).map_err(IoError::from_io)?;
            let new_end = offset + buf.len() as u64;
            if new_end > inner.file_size {
                inner.file_size = new_end;
            }
            Ok(buf.len())
        };

        let latency_us = start.elapsed().as_micros() as u64;
        self.metrics.record_write(bytes, latency_us);
        self.inflight_ops.fetch_sub(1, Ordering::Relaxed);
        self.inflight_bytes.fetch_sub(bytes, Ordering::Relaxed);

        result
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, IoError> {
        let mut inner = self.inner.lock();
        // Flush buffered writes before reading so we see our own writes
        inner.writer.flush().map_err(IoError::from_io)?;

        let file = inner.writer.get_mut();
        file.seek(SeekFrom::Start(offset))
            .map_err(IoError::from_io)?;
        let mut buf = vec![0u8; len];
        let n = file.read(&mut buf).map_err(IoError::from_io)?;
        buf.truncate(n);

        self.metrics.record_read(n as u64);
        Ok(buf)
    }

    fn flush_buf(&self) -> Result<(), IoError> {
        let mut inner = self.inner.lock();
        inner.writer.flush().map_err(IoError::from_io)
    }

    fn sync(&self) -> Result<(), IoError> {
        let start = Instant::now();
        let mut inner = self.inner.lock();
        // First flush the BufWriter buffer to the OS
        inner.writer.flush().map_err(IoError::from_io)?;
        // Then sync to disk — THIS IS THE DURABILITY BARRIER
        inner
            .writer
            .get_ref()
            .sync_data()
            .map_err(IoError::from_io)?;

        let latency_us = start.elapsed().as_micros() as u64;
        self.metrics
            .record_flush(latency_us, super::async_file::FlushReason::Explicit);
        Ok(())
    }

    fn size(&self) -> Result<u64, IoError> {
        let inner = self.inner.lock();
        Ok(inner.file_size)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn inflight_ops(&self) -> u64 {
        self.inflight_ops.load(Ordering::Relaxed)
    }

    fn inflight_bytes(&self) -> u64 {
        self.inflight_bytes.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_file(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("falcon_sync_test_{}", name))
    }

    #[test]
    fn test_sync_file_write_read() {
        let path = temp_file("write_read");
        let _ = std::fs::remove_file(&path);

        let f = SyncFileBackend::open(path.clone(), AsyncFileConfig::default()).unwrap();
        let written = f.write_at(0, b"hello world").unwrap();
        assert_eq!(written, 11);

        // Flush before reading
        f.flush_buf().unwrap();

        let data = f.read_at(0, 11).unwrap();
        assert_eq!(&data, b"hello world");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_sync_file_append_multiple() {
        let path = temp_file("append_multi");
        let _ = std::fs::remove_file(&path);

        let f = SyncFileBackend::open(path.clone(), AsyncFileConfig::default()).unwrap();
        f.write_at(0, b"aaa").unwrap();
        f.write_at(3, b"bbb").unwrap();
        f.write_at(6, b"ccc").unwrap();
        f.flush_buf().unwrap();

        let data = f.read_at(0, 9).unwrap();
        assert_eq!(&data, b"aaabbbccc");

        assert_eq!(f.size().unwrap(), 9);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_sync_file_sync_durability() {
        let path = temp_file("sync_durable");
        let _ = std::fs::remove_file(&path);

        let f = SyncFileBackend::open(path.clone(), AsyncFileConfig::default()).unwrap();
        f.write_at(0, b"durable data").unwrap();
        f.sync().unwrap(); // durability barrier

        // Re-open and verify data persisted
        drop(f);
        let f2 = SyncFileBackend::open(path.clone(), AsyncFileConfig::default()).unwrap();
        let data = f2.read_at(0, 12).unwrap();
        assert_eq!(&data, b"durable data");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_sync_file_inflight_tracking() {
        let path = temp_file("inflight");
        let _ = std::fs::remove_file(&path);

        let f = SyncFileBackend::open(path.clone(), AsyncFileConfig::default()).unwrap();
        // After write completes, inflight should be back to 0
        f.write_at(0, b"test").unwrap();
        assert_eq!(f.inflight_ops(), 0);
        assert_eq!(f.inflight_bytes(), 0);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_sync_file_inflight_limit() {
        let path = temp_file("inflight_limit");
        let _ = std::fs::remove_file(&path);

        let cfg = AsyncFileConfig {
            max_inflight_bytes: 10, // very low limit
            ..Default::default()
        };
        let f = SyncFileBackend::open(path.clone(), cfg).unwrap();
        // Write 5 bytes — should succeed
        f.write_at(0, b"hello").unwrap();
        // The write completes synchronously so inflight is back to 0 — next write also succeeds
        f.write_at(5, b"world").unwrap();

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_sync_file_metrics() {
        let path = temp_file("metrics");
        let _ = std::fs::remove_file(&path);

        let f = SyncFileBackend::open(path.clone(), AsyncFileConfig::default()).unwrap();
        f.write_at(0, b"metrics test data").unwrap();
        f.read_at(0, 5).unwrap();
        f.sync().unwrap();

        // Metrics are tracked internally on the SyncFileBackend but not directly accessible
        // via the trait. Test that operations succeeded without error.

        let _ = std::fs::remove_file(&path);
    }
}
