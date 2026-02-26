//! Unified async file I/O abstraction for FalconDB.
//!
//! Provides an OS-agnostic `AsyncFile` trait with platform-specific backends:
//! - **Windows**: IOCP-based overlapped I/O (`windows_iocp.rs`)
//! - **Fallback**: Tokio spawn_blocking wrapper (`sync_file.rs`)
//!
//! Key design points:
//! - WAL writes never block the async runtime — all disk I/O runs on
//!   dedicated completion threads (IOCP) or the blocking threadpool.
//! - Flush = `FlushFileBuffers` (Windows) / `fsync`/`fdatasync` (Unix).
//!   **No path may claim durability without flush completion.**
//! - Bounded inflight control prevents OOM under write bursts.

pub mod async_file;
pub mod async_wal_writer;
pub mod epoch_fence;
pub mod sync_file;
#[cfg(target_os = "windows")]
pub mod windows_iocp;
#[cfg(target_os = "linux")]
pub mod linux_platform;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub mod linux_io_uring;

pub mod snapshot_stream;

pub use async_file::{AsyncFile, AsyncFileConfig, FlushPolicy, IoError, IoErrorKind, IoMetrics};
pub use sync_file::SyncFileBackend;

/// Create the appropriate `AsyncFile` backend for this platform.
///
/// Selection order:
/// 1. Windows + async_io=true → IOCP backend
/// 2. Linux + io_uring feature + async_io=true → io_uring backend
/// 3. Fallback → synchronous file backend
pub fn create_async_file(
    path: std::path::PathBuf,
    config: AsyncFileConfig,
) -> Result<Box<dyn AsyncFile>, IoError> {
    #[cfg(target_os = "windows")]
    {
        if config.async_io_enabled {
            return windows_iocp::IocpFile::open(path, config).map(|f| Box::new(f) as Box<dyn AsyncFile>);
        }
    }
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        if config.async_io_enabled {
            match linux_io_uring::IoUringFile::open(path.clone(), config.clone()) {
                Ok(f) => return Ok(Box::new(f) as Box<dyn AsyncFile>),
                Err(e) => {
                    tracing::warn!("io_uring backend failed, falling back to sync: {}", e);
                }
            }
        }
    }
    SyncFileBackend::open(path, config).map(|f| Box::new(f) as Box<dyn AsyncFile>)
}
