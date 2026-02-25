//! USTM Engine — Top-level coordinator for the User-Space Tiered Memory system.
//!
//! The engine ties together the three-zone memory manager, LIRS-2 eviction,
//! I/O scheduler, and query-aware prefetcher into a single unified API that
//! upper layers (executor, txn manager) consume.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::io_scheduler::{IoError, IoRequest, IoPriority, IoScheduler, IoSchedulerConfig, IoSchedulerStats};
use super::page::{AccessPriority, PageData, PageHandle, PageId, PinGuard};
use super::prefetcher::{Prefetcher, PrefetcherConfig, PrefetcherStats, PrefetchSource};
use super::zones::{ZoneConfig, ZoneError, ZoneManager, ZoneStats};

// ── Engine Configuration ─────────────────────────────────────────────────────

/// Top-level configuration for the USTM engine.
#[derive(Debug, Clone)]
pub struct UstmConfig {
    pub zones: ZoneConfig,
    pub io_scheduler: IoSchedulerConfig,
    pub prefetcher: PrefetcherConfig,
    /// Default page size in bytes (used for new allocations).
    pub default_page_size: u32,
}

impl Default for UstmConfig {
    fn default() -> Self {
        Self {
            zones: ZoneConfig::default(),
            io_scheduler: IoSchedulerConfig::default(),
            prefetcher: PrefetcherConfig::default(),
            default_page_size: 8192, // 8 KB
        }
    }
}

// ── Engine Stats ─────────────────────────────────────────────────────────────

/// Unified statistics for the USTM engine.
#[derive(Debug, Clone)]
pub struct UstmStats {
    pub zones: ZoneStats,
    pub io_scheduler: IoSchedulerStats,
    pub prefetcher: PrefetcherStats,
    pub sync_reads: u64,
    pub async_reads: u64,
    pub total_read_latency_us: u64,
}

// ── Fetch Result ─────────────────────────────────────────────────────────────

/// Result of a page fetch operation.
#[derive(Debug)]
pub enum FetchResult {
    /// Page was already resident (Hot or Warm).
    Hit(Arc<PageHandle>),
    /// Page was loaded from disk into Warm zone.
    Loaded(Arc<PageHandle>),
    /// Page could not be loaded.
    Error(UstmError),
}

/// USTM engine errors.
#[derive(Debug, Clone)]
pub enum UstmError {
    ZoneError(ZoneError),
    IoError(String),
    PageNotFound(PageId),
    Shutdown,
}

impl std::fmt::Display for UstmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZoneError(e) => write!(f, "zone error: {e}"),
            Self::IoError(e) => write!(f, "I/O error: {e}"),
            Self::PageNotFound(pid) => write!(f, "page not found: {pid}"),
            Self::Shutdown => write!(f, "engine is shut down"),
        }
    }
}

impl From<ZoneError> for UstmError {
    fn from(e: ZoneError) -> Self {
        Self::ZoneError(e)
    }
}

impl From<IoError> for UstmError {
    fn from(e: IoError) -> Self {
        Self::IoError(e.to_string())
    }
}

// ── Page Location Descriptor ─────────────────────────────────────────────────

/// Describes where a cold page's data can be found on disk.
#[derive(Debug, Clone)]
pub struct PageLocation {
    pub file_path: PathBuf,
    pub offset: u64,
    pub length: u32,
}

// ── USTM Engine ──────────────────────────────────────────────────────────────

/// The User-Space Tiered Memory engine.
///
/// This is the primary entry point for all page-level data access.
pub struct UstmEngine {
    config: UstmConfig,
    zones: ZoneManager,
    io_scheduler: Arc<IoScheduler>,
    prefetcher: Arc<Prefetcher>,
    /// Cold page location directory: page_id → disk location.
    page_directory: dashmap::DashMap<PageId, PageLocation>,
    /// Whether the engine is running.
    running: AtomicBool,
    /// Counters.
    sync_reads: AtomicU64,
    async_reads: AtomicU64,
    total_read_latency_us: AtomicU64,
}

impl UstmEngine {
    pub fn new(config: UstmConfig) -> Self {
        let zones = ZoneManager::new(config.zones.clone());
        let io_scheduler = Arc::new(IoScheduler::new(config.io_scheduler.clone()));
        let prefetcher = Arc::new(Prefetcher::new(config.prefetcher.clone()));
        Self {
            zones,
            io_scheduler,
            prefetcher,
            page_directory: dashmap::DashMap::new(),
            running: AtomicBool::new(true),
            sync_reads: AtomicU64::new(0),
            async_reads: AtomicU64::new(0),
            total_read_latency_us: AtomicU64::new(0),
            config,
        }
    }

    // ── Hot Zone API ─────────────────────────────────────────────────────────

    /// Allocate a page in the Hot zone (MemTable, index internals).
    pub fn alloc_hot(
        &self,
        page_id: PageId,
        data: PageData,
        priority: AccessPriority,
    ) -> Result<Arc<PageHandle>, UstmError> {
        Ok(self.zones.alloc_hot(page_id, data, priority)?)
    }

    /// Free a Hot zone page.
    pub fn free_hot(&self, page_id: PageId) -> u64 {
        self.zones.free_hot(page_id)
    }

    /// Pin a Hot zone page.
    pub fn pin_hot(&self, page_id: PageId) -> Option<PinGuard> {
        self.zones.pin_hot(page_id)
    }

    // ── Warm Zone API ────────────────────────────────────────────────────────

    /// Insert a page into the Warm zone (SST page cache).
    pub fn insert_warm(
        &self,
        page_id: PageId,
        data: PageData,
        priority: AccessPriority,
    ) -> Arc<PageHandle> {
        let h = self.zones.access_warm(page_id, data, priority);
        self.prefetcher.mark_resident(page_id);
        h
    }

    /// Pin a Warm zone page.
    pub fn pin_warm(&self, page_id: PageId) -> Option<PinGuard> {
        self.zones.pin_warm(page_id)
    }

    // ── Unified Fetch API ────────────────────────────────────────────────────

    /// Fetch a page.  Checks Hot → Warm → Cold (disk).
    ///
    /// If the page is in Hot or Warm, returns immediately (cache hit).
    /// If the page is Cold, performs a synchronous read from disk and loads
    /// it into the Warm zone.
    pub fn fetch(&self, page_id: PageId, priority: AccessPriority) -> FetchResult {
        if !self.running.load(Ordering::Acquire) {
            return FetchResult::Error(UstmError::Shutdown);
        }

        // 1. Check Hot zone.
        if let Some(h) = self.zones.get_hot(page_id) {
            return FetchResult::Hit(h);
        }

        // 2. Check Warm zone.
        if let Some(h) = self.zones.get_warm(page_id) {
            return FetchResult::Hit(h);
        }

        // 3. Cold — need to read from disk.
        let location = match self.page_directory.get(&page_id) {
            Some(loc) => loc.clone(),
            None => return FetchResult::Error(UstmError::PageNotFound(page_id)),
        };

        let start = Instant::now();
        let req = IoRequest {
            page_id,
            file_path: location.file_path.clone(),
            offset: location.offset,
            length: location.length,
            priority: IoPriority::Query,
            submitted_at: Instant::now(),
        };

        // Synchronous read (portable fallback).
        let resp = IoScheduler::execute_read_sync(&req);
        let latency = start.elapsed().as_micros() as u64;
        self.sync_reads.fetch_add(1, Ordering::Relaxed);
        self.total_read_latency_us.fetch_add(latency, Ordering::Relaxed);

        match resp.data {
            Ok(data) => {
                let handle = self.zones.access_warm(page_id, data, priority);
                self.prefetcher.mark_resident(page_id);
                FetchResult::Loaded(handle)
            }
            Err(e) => FetchResult::Error(UstmError::IoError(e.to_string())),
        }
    }

    /// Fetch a page and return a PinGuard (page stays resident until guard drops).
    pub fn fetch_pinned(&self, page_id: PageId, priority: AccessPriority) -> Result<PinGuard, UstmError> {
        match self.fetch(page_id, priority) {
            FetchResult::Hit(h)
            | FetchResult::Loaded(h) => Ok(PinGuard::new(h)),
            FetchResult::Error(e) => Err(e),
        }
    }

    // ── Page Directory ───────────────────────────────────────────────────────

    /// Register a cold page's disk location.
    pub fn register_page(&self, page_id: PageId, location: PageLocation) {
        self.zones.register_cold(page_id, AccessPriority::Cold);
        self.page_directory.insert(page_id, location);
    }

    /// Unregister a page (e.g. SST file deleted after compaction).
    pub fn unregister_page(&self, page_id: PageId) {
        self.zones.remove(page_id);
        self.page_directory.remove(&page_id);
        self.prefetcher.mark_evicted(page_id);
        self.io_scheduler.cancel_page(page_id);
    }

    /// Check how many pages are registered.
    pub fn registered_page_count(&self) -> usize {
        self.page_directory.len()
    }

    // ── Prefetch API ─────────────────────────────────────────────────────────

    /// Submit a prefetch hint from the executor.
    pub fn prefetch_hint(&self, source: PrefetchSource, file_path: PathBuf) {
        self.prefetcher
            .hint(source, file_path, self.config.default_page_size);
    }

    /// Process one tick of prefetch requests — drain from prefetcher and submit
    /// to the I/O scheduler.
    pub fn prefetch_tick(&self) {
        let requests = self.prefetcher.drain_tick();
        for req in requests {
            let io_req = IoRequest {
                page_id: req.page_id,
                file_path: req.file_path,
                offset: req.offset,
                length: req.length,
                priority: IoPriority::Prefetch,
                submitted_at: Instant::now(),
            };
            // Best-effort: ignore rate-limit errors for prefetch.
            let _ = self.io_scheduler.submit(io_req);
        }
    }

    // ── I/O Scheduler API ────────────────────────────────────────────────────

    /// Process completed I/O responses and load data into Warm zone.
    pub fn process_io_completions(&self) -> usize {
        let responses = self.io_scheduler.take_completed();
        let count = responses.len();
        for resp in responses {
            if let Ok(data) = resp.data {
                self.zones.access_warm(resp.page_id, data, AccessPriority::Prefetched);
                self.prefetcher.complete(resp.page_id);
                self.prefetcher.mark_resident(resp.page_id);
                self.async_reads.fetch_add(1, Ordering::Relaxed);
                self.total_read_latency_us
                    .fetch_add(resp.latency_us, Ordering::Relaxed);
            }
        }
        count
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    /// Shut down the engine.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Release);
        self.prefetcher.set_enabled(false);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    // ── Stats ────────────────────────────────────────────────────────────────

    pub fn stats(&self) -> UstmStats {
        UstmStats {
            zones: self.zones.stats(),
            io_scheduler: self.io_scheduler.stats.lock().clone(),
            prefetcher: self.prefetcher.stats(),
            sync_reads: self.sync_reads.load(Ordering::Relaxed),
            async_reads: self.async_reads.load(Ordering::Relaxed),
            total_read_latency_us: self.total_read_latency_us.load(Ordering::Relaxed),
        }
    }

    /// Get zone manager reference (for advanced introspection).
    pub const fn zone_manager(&self) -> &ZoneManager {
        &self.zones
    }

    /// Get I/O scheduler reference.
    pub const fn io_scheduler(&self) -> &Arc<IoScheduler> {
        &self.io_scheduler
    }

    /// Get prefetcher reference.
    pub const fn prefetcher(&self) -> &Arc<Prefetcher> {
        &self.prefetcher
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ustm::lirs2::Lirs2Config;
    use crate::ustm::Tier;
    use std::io::Write;

    fn test_config() -> UstmConfig {
        UstmConfig {
            zones: ZoneConfig {
                hot_capacity_bytes: 4096,
                warm_capacity_bytes: 4096,
                lirs2: Lirs2Config {
                    lir_capacity: 8,
                    hir_resident_capacity: 8,
                    hir_nonresident_capacity: 16,
                },
            },
            io_scheduler: IoSchedulerConfig::default(),
            prefetcher: PrefetcherConfig::default(),
            default_page_size: 64,
        }
    }

    #[test]
    fn test_engine_hot_alloc() {
        let engine = UstmEngine::new(test_config());
        let data = PageData::new(vec![42u8; 100]);
        let h = engine
            .alloc_hot(PageId(1), data, AccessPriority::IndexInternal)
            .unwrap();
        assert_eq!(h.tier(), Tier::Hot);

        // Fetch should find it in Hot.
        match engine.fetch(PageId(1), AccessPriority::IndexInternal) {
            FetchResult::Hit(h) => assert_eq!(h.page_id, PageId(1)),
            other => panic!("expected Hit, got {:?}", other),
        }
    }

    #[test]
    fn test_engine_warm_insert_and_fetch() {
        let engine = UstmEngine::new(test_config());
        let data = PageData::new(vec![7u8; 64]);
        engine.insert_warm(PageId(10), data, AccessPriority::HotRow);

        match engine.fetch(PageId(10), AccessPriority::HotRow) {
            FetchResult::Hit(h) => assert_eq!(h.page_id, PageId(10)),
            other => panic!("expected Hit, got {:?}", other),
        }
    }

    #[test]
    fn test_engine_cold_fetch_from_disk() {
        let engine = UstmEngine::new(test_config());

        // Create a temporary file with page data.
        let dir = std::env::temp_dir().join("ustm_test_cold");
        let _ = std::fs::create_dir_all(&dir);
        let file_path = dir.join("test_page.dat");
        {
            let mut f = std::fs::File::create(&file_path).unwrap();
            f.write_all(&vec![0xABu8; 64]).unwrap();
            f.sync_all().unwrap();
        }

        // Register the page.
        engine.register_page(
            PageId(100),
            PageLocation {
                file_path: file_path.clone(),
                offset: 0,
                length: 64,
            },
        );

        // Fetch — should read from disk.
        match engine.fetch(PageId(100), AccessPriority::HotRow) {
            FetchResult::Loaded(h) => {
                assert_eq!(h.page_id, PageId(100));
                assert_eq!(h.tier(), Tier::Warm);
                let guard = h.read().unwrap();
                let data = guard.as_ref().unwrap();
                assert_eq!(data.as_slice()[0], 0xAB);
            }
            other => panic!("expected Loaded, got {:?}", other),
        }

        // Second fetch should be a Hit (now in Warm).
        match engine.fetch(PageId(100), AccessPriority::HotRow) {
            FetchResult::Hit(_) => {}
            other => panic!("expected Hit, got {:?}", other),
        }

        // Cleanup.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_engine_fetch_nonexistent_page() {
        let engine = UstmEngine::new(test_config());
        match engine.fetch(PageId(9999), AccessPriority::Cold) {
            FetchResult::Error(UstmError::PageNotFound(pid)) => {
                assert_eq!(pid, PageId(9999));
            }
            other => panic!("expected PageNotFound, got {:?}", other),
        }
    }

    #[test]
    fn test_engine_unregister() {
        let engine = UstmEngine::new(test_config());
        let data = PageData::new(vec![0u8; 32]);
        engine.insert_warm(PageId(5), data, AccessPriority::HotRow);
        engine.register_page(
            PageId(5),
            PageLocation {
                file_path: PathBuf::from("dummy.dat"),
                offset: 0,
                length: 32,
            },
        );

        engine.unregister_page(PageId(5));
        assert!(engine.zone_manager().lookup(PageId(5)).is_none());
        assert_eq!(engine.registered_page_count(), 0);
    }

    #[test]
    fn test_engine_shutdown() {
        let engine = UstmEngine::new(test_config());
        assert!(engine.is_running());
        engine.shutdown();
        assert!(!engine.is_running());

        match engine.fetch(PageId(1), AccessPriority::Cold) {
            FetchResult::Error(UstmError::Shutdown) => {}
            other => panic!("expected Shutdown, got {:?}", other),
        }
    }

    #[test]
    fn test_engine_stats() {
        let engine = UstmEngine::new(test_config());
        let data = PageData::new(vec![0u8; 50]);
        engine.alloc_hot(PageId(1), data.clone(), AccessPriority::IndexInternal).unwrap();
        engine.insert_warm(PageId(2), data, AccessPriority::HotRow);

        let stats = engine.stats();
        assert_eq!(stats.zones.hot_page_count, 1);
        assert_eq!(stats.zones.warm_page_count, 1);
    }

    #[test]
    fn test_engine_fetch_pinned() {
        let engine = UstmEngine::new(test_config());
        let data = PageData::new(vec![0u8; 32]);
        engine.insert_warm(PageId(7), data, AccessPriority::HotRow);

        {
            let guard = engine.fetch_pinned(PageId(7), AccessPriority::HotRow).unwrap();
            assert_eq!(guard.page_id(), PageId(7));
            assert!(guard.handle().is_pinned());
        }
        // Guard dropped — page should be unpinned.
        let h = engine.zone_manager().get_warm(PageId(7)).unwrap();
        assert!(!h.is_pinned());
    }

    #[test]
    fn test_engine_prefetch_integration() {
        let engine = UstmEngine::new(test_config());

        // Submit a prefetch hint.
        engine.prefetch_hint(
            PrefetchSource::SeqScan {
                start_page: PageId(200),
                count: 3,
            },
            PathBuf::from("sst.dat"),
        );

        // Process prefetch tick — requests should be forwarded to I/O scheduler.
        engine.prefetch_tick();

        let (q, pf, bg) = engine.io_scheduler().pending_count();
        assert_eq!(pf, 3); // 3 prefetch requests
        assert_eq!(q, 0);
        assert_eq!(bg, 0);
    }
}
