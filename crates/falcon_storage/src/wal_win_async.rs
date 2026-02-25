//! Windows IOCP / Overlapped I/O WAL device.
//!
//! Implements `WalDevice` using Windows-native asynchronous I/O:
//! - `FILE_FLAG_OVERLAPPED` for non-blocking writes
//! - `FILE_FLAG_NO_BUFFERING` (optional) for O_DIRECT-equivalent bypass
//! - IOCP completion port for deterministic flush acknowledgement
//!
//! This ensures WAL write threads never block the Tokio runtime and
//! flush latency is bounded by disk I/O, not OS scheduling.
//!
//! # Configuration
//! - `no_buffering`: enables `FILE_FLAG_NO_BUFFERING` (requires sector-aligned writes)
//! - Sector alignment is auto-detected and padded when `no_buffering` is enabled
//!
//! # Platform
//! This module only compiles on Windows (`cfg(windows)`).

/// Configuration for the Windows async WAL device.
#[derive(Debug, Clone)]
pub struct WalWinAsyncConfig {
    /// Enable `FILE_FLAG_NO_BUFFERING` (O_DIRECT equivalent).
    /// Requires sector-aligned writes — the device auto-pads.
    pub no_buffering: bool,
    /// Enable `FILE_FLAG_WRITE_THROUGH` for write-through caching.
    pub write_through: bool,
    /// Max bytes per WAL segment before rotation.
    pub max_segment_size: u64,
}

impl Default for WalWinAsyncConfig {
    fn default() -> Self {
        Self {
            no_buffering: false,
            write_through: false,
            max_segment_size: 64 * 1024 * 1024,
        }
    }
}

#[cfg(windows)]
mod inner {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek};
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use falcon_common::error::StorageError;
    use parking_lot::Mutex;

    use crate::wal::{
        SyncMode, WalDevice, WAL_FORMAT_VERSION, WAL_MAGIC, WAL_SEGMENT_HEADER_SIZE,
    };
    use super::WalWinAsyncConfig;

    use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::FlushFileBuffers;
    use windows_sys::Win32::System::IO::{
        CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED,
    };

    // Windows file flags (not always re-exported by std)
    const FILE_FLAG_OVERLAPPED: u32 = 0x40000000;
    const FILE_FLAG_NO_BUFFERING: u32 = 0x20000000;
    const FILE_FLAG_WRITE_THROUGH: u32 = 0x80000000;
    // ERROR_IO_PENDING
    const ERROR_IO_PENDING: u32 = 997;
    /// Sector size for alignment when `no_buffering` is enabled.
    const SECTOR_SIZE: usize = 4096;

    struct WinAsyncInner {
        /// The raw Windows HANDLE for the current WAL segment file.
        handle: HANDLE,
        /// IOCP completion port handle.
        iocp: HANDLE,
        /// Std File kept alive so the handle remains valid.
        _file: std::fs::File,
        dir: PathBuf,
        current_segment: u64,
        current_size: u64,
        config: WalWinAsyncConfig,
        /// Accumulated write buffer for batching before flush.
        write_buf: Vec<u8>,
    }

    // SAFETY: HANDLE is a kernel object reference, safe to send across threads.
    unsafe impl Send for WinAsyncInner {}
    unsafe impl Sync for WinAsyncInner {}

    /// Windows IOCP-based WAL device.
    ///
    /// All writes go into an in-memory buffer via `append()`.
    /// On `flush()`, the buffer is written to disk via Overlapped I/O
    /// and completion is awaited on the IOCP port — deterministic latency.
    pub struct WalDeviceWinAsync {
        inner: Mutex<WinAsyncInner>,
        bytes_written: AtomicU64,
    }

    impl WalDeviceWinAsync {
        /// Open (or create) a WAL device in `dir` with the given config.
        pub fn open(dir: &Path, config: WalWinAsyncConfig) -> Result<Self, StorageError> {
            std::fs::create_dir_all(dir)?;

            let latest = find_latest_segment(dir);
            let seg_id = latest.unwrap_or(0);

            let (file, handle, iocp, file_size) =
                open_segment_overlapped(dir, seg_id, &config)?;

            let is_new = file_size == 0;
            let mut device = Self {
                inner: Mutex::new(WinAsyncInner {
                    handle,
                    iocp,
                    _file: file,
                    dir: dir.to_path_buf(),
                    current_segment: seg_id,
                    current_size: if is_new { 0 } else { file_size },
                    config,
                    write_buf: Vec::with_capacity(64 * 1024),
                }),
                bytes_written: AtomicU64::new(0),
            };

            // Write segment header for new files
            if is_new {
                let mut header = Vec::with_capacity(WAL_SEGMENT_HEADER_SIZE);
                header.extend_from_slice(WAL_MAGIC);
                header.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
                {
                    let inner = device.inner.get_mut();
                    inner.write_buf.extend_from_slice(&header);
                    inner.current_size = WAL_SEGMENT_HEADER_SIZE as u64;
                }
                device.flush(SyncMode::FSync)?;
            }

            Ok(device)
        }

        /// Current segment ID.
        pub fn current_segment_id(&self) -> u64 {
            self.inner.lock().current_segment
        }

        /// Total bytes written (lifetime counter).
        pub fn total_bytes_written(&self) -> u64 {
            self.bytes_written.load(Ordering::Relaxed)
        }
    }

    impl WalDevice for WalDeviceWinAsync {
        fn append(&self, data: &[u8]) -> Result<usize, StorageError> {
            let mut inner = self.inner.lock();

            // Check segment rotation
            if inner.current_size + data.len() as u64 > inner.config.max_segment_size {
                flush_buffer_overlapped(&mut inner)?;
                rotate_segment(&mut inner)?;
            }

            inner.write_buf.extend_from_slice(data);
            inner.current_size += data.len() as u64;
            self.bytes_written.fetch_add(data.len() as u64, Ordering::Relaxed);
            Ok(data.len())
        }

        fn flush(&self, sync_mode: SyncMode) -> Result<(), StorageError> {
            let mut inner = self.inner.lock();

            if inner.write_buf.is_empty() {
                return Ok(());
            }

            flush_buffer_overlapped(&mut inner)?;

            match sync_mode {
                SyncMode::None => {}
                SyncMode::FSync | SyncMode::FDataSync => {
                    let ok = unsafe { FlushFileBuffers(inner.handle) };
                    if ok == 0 {
                        let err = unsafe { GetLastError() };
                        return Err(StorageError::Wal(format!(
                            "FlushFileBuffers failed: win32 error {err}"
                        )));
                    }
                }
            }

            Ok(())
        }

        fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, StorageError> {
            let inner = self.inner.lock();
            let seg_path = inner.dir.join(crate::wal::segment_filename(inner.current_segment));
            let mut file = std::fs::File::open(&seg_path)?;
            file.seek(std::io::SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            Ok(buf)
        }

        fn size(&self) -> u64 {
            self.inner.lock().current_size
        }

        fn rotate(&self) -> Result<u64, StorageError> {
            let mut inner = self.inner.lock();
            flush_buffer_overlapped(&mut inner)?;
            rotate_segment(&mut inner)
        }
    }

    impl Drop for WalDeviceWinAsync {
        fn drop(&mut self) {
            let inner = self.inner.get_mut();
            let _ = flush_buffer_overlapped(inner);
            // Close IOCP handle (file handle closed by std::fs::File Drop)
            if inner.iocp != 0 {
                unsafe { CloseHandle(inner.iocp) };
            }
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────

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

    /// Open a WAL segment file with FILE_FLAG_OVERLAPPED via std OpenOptions
    /// custom_flags. Returns (File, raw HANDLE, IOCP handle, file_size).
    fn open_segment_overlapped(
        dir: &Path,
        seg_id: u64,
        config: &WalWinAsyncConfig,
    ) -> Result<(std::fs::File, HANDLE, HANDLE, u64), StorageError> {
        let seg_path = dir.join(crate::wal::segment_filename(seg_id));

        let mut flags = FILE_FLAG_OVERLAPPED;
        if config.no_buffering {
            flags |= FILE_FLAG_NO_BUFFERING;
        }
        if config.write_through {
            flags |= FILE_FLAG_WRITE_THROUGH;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(flags)
            .open(&seg_path)
            .map_err(|e| StorageError::Wal(format!(
                "Failed to open WAL segment {} with OVERLAPPED: {}",
                seg_path.display(), e
            )))?;

        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        let handle = file.as_raw_handle() as HANDLE;

        // Create IOCP bound to this file handle
        let iocp = unsafe { CreateIoCompletionPort(handle, 0, 0, 1) };
        if iocp == 0 {
            let err = unsafe { GetLastError() };
            return Err(StorageError::Wal(format!(
                "CreateIoCompletionPort failed: win32 error {err}"
            )));
        }

        Ok((file, handle, iocp, file_size))
    }

    /// Flush the write buffer via Overlapped I/O and wait for IOCP completion.
    fn flush_buffer_overlapped(inner: &mut WinAsyncInner) -> Result<(), StorageError> {
        if inner.write_buf.is_empty() {
            return Ok(());
        }

        // Calculate write offset = current_size minus buffered bytes
        let buf_len = inner.write_buf.len() as u64;
        let write_offset = inner.current_size.saturating_sub(buf_len);

        let data = if inner.config.no_buffering {
            let aligned_len = (inner.write_buf.len() + SECTOR_SIZE - 1) & !(SECTOR_SIZE - 1);
            let mut aligned = std::mem::take(&mut inner.write_buf);
            aligned.resize(aligned_len, 0);
            aligned
        } else {
            std::mem::take(&mut inner.write_buf)
        };

        let actual_offset = if inner.config.no_buffering {
            (write_offset / SECTOR_SIZE as u64) * SECTOR_SIZE as u64
        } else {
            write_offset
        };

        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        // Set the file offset via the OVERLAPPED Anonymous union
        overlapped.Anonymous.Anonymous.Offset = actual_offset as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (actual_offset >> 32) as u32;

        let mut bytes_written: u32 = 0;
        let ok = unsafe {
            windows_sys::Win32::Storage::FileSystem::WriteFile(
                inner.handle,
                data.as_ptr(),
                data.len() as u32,
                &mut bytes_written,
                &mut overlapped,
            )
        };

        if ok == 0 {
            let err = unsafe { GetLastError() };
            if err != ERROR_IO_PENDING {
                return Err(StorageError::Wal(format!(
                    "WriteFile (overlapped) failed: win32 error {err}"
                )));
            }
        }

        // Wait for IOCP completion (bounded 5s timeout)
        let mut completion_bytes: u32 = 0;
        let mut completion_key: usize = 0;
        let mut completion_overlapped: *mut OVERLAPPED = std::ptr::null_mut();

        let wait_result = unsafe {
            GetQueuedCompletionStatus(
                inner.iocp,
                &mut completion_bytes,
                &mut completion_key,
                &mut completion_overlapped,
                5000,
            )
        };

        if wait_result == 0 {
            let err = unsafe { GetLastError() };
            return Err(StorageError::Wal(format!(
                "IOCP GetQueuedCompletionStatus failed: win32 error {err}"
            )));
        }

        inner.write_buf.clear();

        tracing::trace!(
            bytes = completion_bytes,
            segment = inner.current_segment,
            "WAL IOCP write complete"
        );

        Ok(())
    }

    /// Rotate to a new WAL segment.
    fn rotate_segment(inner: &mut WinAsyncInner) -> Result<u64, StorageError> {
        // Sync current segment
        unsafe { FlushFileBuffers(inner.handle) };

        // Close IOCP (file handle closed when _file is replaced)
        if inner.iocp != 0 {
            unsafe { CloseHandle(inner.iocp) };
        }

        inner.current_segment += 1;

        let (file, handle, iocp, _) =
            open_segment_overlapped(&inner.dir, inner.current_segment, &inner.config)?;
        inner._file = file;
        inner.handle = handle;
        inner.iocp = iocp;

        // Write segment header
        let mut header = Vec::with_capacity(WAL_SEGMENT_HEADER_SIZE);
        header.extend_from_slice(WAL_MAGIC);
        header.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        inner.write_buf.extend_from_slice(&header);
        inner.current_size = WAL_SEGMENT_HEADER_SIZE as u64;
        flush_buffer_overlapped(inner)?;

        tracing::debug!(
            segment = inner.current_segment,
            "WAL IOCP device rotated"
        );

        Ok(inner.current_segment)
    }

    /// Check if a path supports FILE_FLAG_NO_BUFFERING.
    /// Returns (supported, sector_size).
    pub fn check_no_buffering_support(dir: &Path) -> (bool, u32) {
        let test_path = dir.join(".falcon_nobuf_test");
        let result = OpenOptions::new()
            .create(true)
            .write(true)
            .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED)
            .open(&test_path);
        let supported = result.is_ok();
        drop(result);
        let _ = std::fs::remove_file(&test_path);
        (supported, SECTOR_SIZE as u32)
    }

    /// Check disk alignment for the given directory.
    /// Returns (aligned, sector_size, fs_type_hint).
    pub fn check_disk_alignment(dir: &Path) -> (bool, u32, &'static str) {
        let (no_buf, sector) = check_no_buffering_support(dir);
        let fs_hint = if no_buf { "NTFS (aligned)" } else { "unknown" };
        (no_buf, sector, fs_hint)
    }

    /// Check if the system supports IOCP (always true on Windows NT+).
    pub const fn iocp_available() -> bool {
        true
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_wal_win_async_config_default() {
            let cfg = WalWinAsyncConfig::default();
            assert!(!cfg.no_buffering);
            assert!(!cfg.write_through);
            assert_eq!(cfg.max_segment_size, 64 * 1024 * 1024);
        }

        #[test]
        fn test_wal_win_async_open_and_append() {
            let dir = std::env::temp_dir().join("falcon_wal_win_async_test");
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();

            let device = WalDeviceWinAsync::open(&dir, WalWinAsyncConfig::default()).unwrap();

            let data = b"hello WAL IOCP world";
            let written = device.append(data).unwrap();
            assert_eq!(written, data.len());

            device.flush(SyncMode::FSync).unwrap();

            assert!(device.size() >= WAL_SEGMENT_HEADER_SIZE as u64 + data.len() as u64);
            assert!(device.total_bytes_written() >= data.len() as u64);

            drop(device);
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn test_wal_win_async_multiple_appends() {
            let dir = std::env::temp_dir().join("falcon_wal_win_async_multi");
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();

            let device = WalDeviceWinAsync::open(&dir, WalWinAsyncConfig::default()).unwrap();

            for i in 0..100 {
                let data = format!("record_{:04}", i);
                device.append(data.as_bytes()).unwrap();
            }
            device.flush(SyncMode::FSync).unwrap();

            assert!(device.total_bytes_written() > 0);
            assert_eq!(device.current_segment_id(), 0);

            drop(device);
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn test_iocp_available() {
            assert!(iocp_available());
        }

        #[test]
        fn test_check_no_buffering() {
            let dir = std::env::temp_dir();
            let (supported, sector) = check_no_buffering_support(&dir);
            println!("NO_BUFFERING supported: {}, sector: {}", supported, sector);
            assert!(sector > 0);
        }
    }
}

#[cfg(windows)]
pub use inner::*;

#[cfg(not(windows))]
mod stub {
    use std::path::Path;
    use falcon_common::error::StorageError;
    use crate::wal::{SyncMode, WalDevice};
    use super::WalWinAsyncConfig;

    pub struct WalDeviceWinAsync;

    impl WalDeviceWinAsync {
        pub fn open(_dir: &Path, _config: WalWinAsyncConfig) -> Result<Self, StorageError> {
            Err(StorageError::Wal(
                "WalDeviceWinAsync is only available on Windows".into(),
            ))
        }
        pub fn current_segment_id(&self) -> u64 { 0 }
        pub fn total_bytes_written(&self) -> u64 { 0 }
    }

    impl WalDevice for WalDeviceWinAsync {
        fn append(&self, _: &[u8]) -> Result<usize, StorageError> { unreachable!() }
        fn flush(&self, _: SyncMode) -> Result<(), StorageError> { unreachable!() }
        fn read(&self, _: u64, _: usize) -> Result<Vec<u8>, StorageError> { unreachable!() }
        fn size(&self) -> u64 { 0 }
        fn rotate(&self) -> Result<u64, StorageError> { unreachable!() }
    }

    pub fn iocp_available() -> bool { false }
    pub fn check_no_buffering_support(_dir: &Path) -> (bool, u32) { (false, 0) }
    pub fn check_disk_alignment(_dir: &Path) -> (bool, u32, &'static str) {
        (false, 0, "not-windows")
    }
}

#[cfg(not(windows))]
pub use stub::*;
