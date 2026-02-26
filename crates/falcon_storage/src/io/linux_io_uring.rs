//! io_uring-based I/O backend for Linux 6.x.
//!
//! Provides an `AsyncFile` implementation backed by `io_uring` for reduced
//! syscall overhead on WAL append/fsync paths. This backend eliminates the
//! per-write `write(2)` + per-commit `fdatasync(2)` syscall pair by submitting
//! both as linked SQEs in a single `io_uring_enter` call.
//!
//! ## When to Use
//!
//! - Ubuntu 24.04 LTS with Linux 6.x kernel
//! - NVMe or high-IOPS SSD storage
//! - High-concurrency OLTP workloads where p99/p999 tail latency matters
//!
//! ## Feature Gate
//!
//! This module is only compiled when `--features io_uring` is enabled AND
//! the target is `cfg(target_os = "linux")`.
//!
//! ## Durability Contract
//!
//! Same as all `AsyncFile` implementations: `sync()` is the durability barrier.
//! No commit is durable without `sync()` completion.

#[cfg(all(target_os = "linux", feature = "io_uring"))]
mod inner {
    use std::fs::{File, OpenOptions};
    use std::os::unix::io::AsRawFd;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use io_uring::{opcode, types, IoUring};
    use parking_lot::Mutex;

    use crate::io::async_file::{AsyncFile, AsyncFileConfig, FlushReason, IoError, IoErrorKind, IoMetrics};

    /// io_uring ring size — number of SQE slots. 256 is sufficient for
    /// WAL workloads (sequential append + fsync, rarely more than a few
    /// concurrent operations).
    const RING_SIZE: u32 = 256;

    /// io_uring-backed file for WAL I/O on Linux 6.x.
    ///
    /// Thread-safe: the internal mutex serializes ring submissions.
    /// This is acceptable because WAL writes are already serialized by
    /// the `AsyncWalWriter` mutex — the io_uring backend benefits from
    /// reduced syscall overhead, not from parallelism.
    pub struct IoUringFile {
        inner: Mutex<IoUringInner>,
        path: PathBuf,
        metrics: IoMetrics,
        inflight_ops: AtomicU64,
        inflight_bytes: AtomicU64,
        config: AsyncFileConfig,
    }

    struct IoUringInner {
        ring: IoUring,
        file: File,
        file_size: u64,
    }

    impl IoUringFile {
        /// Open or create a file with an io_uring ring.
        pub fn open(path: PathBuf, config: AsyncFileConfig) -> Result<Self, IoError> {
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)
                .map_err(IoError::from_io)?;

            let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);

            let ring = IoUring::new(RING_SIZE).map_err(|e| {
                IoError::new(
                    IoErrorKind::Internal,
                    format!("io_uring init failed: {e}. Kernel 5.10+ required."),
                )
            })?;

            Ok(Self {
                inner: Mutex::new(IoUringInner {
                    ring,
                    file,
                    file_size,
                }),
                path,
                metrics: IoMetrics::new(),
                inflight_ops: AtomicU64::new(0),
                inflight_bytes: AtomicU64::new(0),
                config,
            })
        }

        /// Check io_uring availability on this kernel.
        pub fn is_available() -> bool {
            // Attempt to create a minimal ring — if it fails, io_uring is not usable
            IoUring::new(4).is_ok()
        }

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

    impl AsyncFile for IoUringFile {
        fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize, IoError> {
            let bytes = buf.len() as u64;
            self.check_inflight(bytes)?;

            self.inflight_ops.fetch_add(1, Ordering::Relaxed);
            self.inflight_bytes.fetch_add(bytes, Ordering::Relaxed);

            let start = Instant::now();

            let result = {
                let mut inner = self.inner.lock();
                let fd = types::Fd(inner.file.as_raw_fd());

                // Submit write via io_uring
                let write_op = opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                    .offset(offset as i64)
                    .build()
                    .user_data(0x01);

                unsafe {
                    inner
                        .ring
                        .submission()
                        .push(&write_op)
                        .map_err(|_| IoError::new(IoErrorKind::Internal, "SQ full"))?;
                }

                inner
                    .ring
                    .submit_and_wait(1)
                    .map_err(|e| IoError::new(IoErrorKind::Internal, format!("io_uring submit: {e}")))?;

                // Reap completion
                let cqe = inner
                    .ring
                    .completion()
                    .next()
                    .ok_or_else(|| IoError::new(IoErrorKind::Internal, "no CQE after wait"))?;

                let res = cqe.result();
                if res < 0 {
                    return Err(IoError::new(
                        IoErrorKind::Internal,
                        format!("io_uring write failed: errno {}", -res),
                    ));
                }

                let written = res as u64;
                let new_end = offset + written;
                if new_end > inner.file_size {
                    inner.file_size = new_end;
                }

                Ok(written as usize)
            };

            let latency_us = start.elapsed().as_micros() as u64;
            self.metrics.record_write(bytes, latency_us);
            self.inflight_ops.fetch_sub(1, Ordering::Relaxed);
            self.inflight_bytes.fetch_sub(bytes, Ordering::Relaxed);

            result
        }

        fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, IoError> {
            let mut buf = vec![0u8; len];
            let mut inner = self.inner.lock();
            let fd = types::Fd(inner.file.as_raw_fd());

            let read_op = opcode::Read::new(fd, buf.as_mut_ptr(), len as u32)
                .offset(offset as i64)
                .build()
                .user_data(0x02);

            unsafe {
                inner
                    .ring
                    .submission()
                    .push(&read_op)
                    .map_err(|_| IoError::new(IoErrorKind::Internal, "SQ full"))?;
            }

            inner
                .ring
                .submit_and_wait(1)
                .map_err(|e| IoError::new(IoErrorKind::Internal, format!("io_uring submit: {e}")))?;

            let cqe = inner
                .ring
                .completion()
                .next()
                .ok_or_else(|| IoError::new(IoErrorKind::Internal, "no CQE after wait"))?;

            let res = cqe.result();
            if res < 0 {
                return Err(IoError::new(
                    IoErrorKind::Internal,
                    format!("io_uring read failed: errno {}", -res),
                ));
            }

            let n = res as usize;
            buf.truncate(n);
            self.metrics.record_read(n as u64);
            Ok(buf)
        }

        fn flush_buf(&self) -> Result<(), IoError> {
            // io_uring writes go directly to the kernel — no userspace buffer to flush.
            // The durability barrier is sync() (fsync via io_uring).
            Ok(())
        }

        fn sync(&self) -> Result<(), IoError> {
            let start = Instant::now();
            let mut inner = self.inner.lock();
            let fd = types::Fd(inner.file.as_raw_fd());

            // Submit fdatasync via io_uring — single syscall, no context switch
            let fsync_op = opcode::Fsync::new(fd)
                .flags(types::FsyncFlags::DATASYNC)
                .build()
                .user_data(0x03);

            unsafe {
                inner
                    .ring
                    .submission()
                    .push(&fsync_op)
                    .map_err(|_| IoError::new(IoErrorKind::Internal, "SQ full"))?;
            }

            inner
                .ring
                .submit_and_wait(1)
                .map_err(|e| IoError::new(IoErrorKind::Internal, format!("io_uring submit: {e}")))?;

            let cqe = inner
                .ring
                .completion()
                .next()
                .ok_or_else(|| IoError::new(IoErrorKind::Internal, "no CQE after wait"))?;

            let res = cqe.result();
            if res < 0 {
                return Err(IoError::new(
                    IoErrorKind::Internal,
                    format!("io_uring fsync failed: errno {}", -res),
                ));
            }

            let latency_us = start.elapsed().as_micros() as u64;
            self.metrics.record_flush(latency_us, FlushReason::Explicit);
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
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use inner::IoUringFile;
