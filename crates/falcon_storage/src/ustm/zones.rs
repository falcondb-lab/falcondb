//! Three-Zone Memory Manager — Hot / Warm / Cold.
//!
//! * **Hot Zone** (DRAM arena): MemTable pages, index internal nodes, active
//!   write-sets.  Never evicted; WAL guarantees durability.
//! * **Warm Zone** (DRAM page cache): Recently fetched SST pages managed by
//!   LIRS-2 eviction.
//! * **Cold Zone** (disk): SST files; reached only through async I/O.
//!
//! The `ZoneManager` coordinates budget enforcement across all three zones
//! and exposes a single `fetch` API to the upper layers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

use super::lirs2::{AccessResult, EvictionCandidate, Lirs2Cache, Lirs2Config, Lirs2Stats};
use super::page::{AccessPriority, PageData, PageHandle, PageId, PinGuard, Tier};

// ── Zone Configuration ───────────────────────────────────────────────────────

/// Configuration for the three-zone memory manager.
#[derive(Debug, Clone)]
pub struct ZoneConfig {
    /// Hot zone capacity in bytes (MemTable + index internals).
    pub hot_capacity_bytes: u64,
    /// Warm zone capacity in bytes (SST page cache).
    pub warm_capacity_bytes: u64,
    /// LIRS-2 configuration for the Warm zone.
    pub lirs2: Lirs2Config,
}

impl Default for ZoneConfig {
    fn default() -> Self {
        Self {
            hot_capacity_bytes: 512 * 1024 * 1024,  // 512 MB
            warm_capacity_bytes: 256 * 1024 * 1024,  // 256 MB
            lirs2: Lirs2Config::default(),
        }
    }
}

// ── Zone Stats ───────────────────────────────────────────────────────────────

/// Observable statistics for the zone manager.
#[derive(Debug, Clone)]
pub struct ZoneStats {
    pub hot_used_bytes: u64,
    pub hot_page_count: u64,
    pub warm_used_bytes: u64,
    pub warm_page_count: u64,
    pub cold_page_count: u64,
    pub total_fetches: u64,
    pub warm_hits: u64,
    pub warm_misses: u64,
    pub evictions: u64,
    pub lirs2: Lirs2Stats,
}

// ── Zone Manager ─────────────────────────────────────────────────────────────

/// The three-zone memory manager.
///
/// Thread-safe: all internal state is protected by fine-grained locks.
pub struct ZoneManager {
    config: ZoneConfig,

    // ── Hot Zone ──
    /// Hot pages (MemTable, index internals).  Never evicted.
    hot_pages: DashMap<PageId, Arc<PageHandle>>,
    hot_used_bytes: AtomicU64,

    // ── Warm Zone ──
    /// Warm pages (SST page cache).  Managed by LIRS-2.
    warm_pages: DashMap<PageId, Arc<PageHandle>>,
    warm_used_bytes: AtomicU64,
    /// LIRS-2 eviction controller for the Warm zone.
    lirs2: Mutex<Lirs2Cache>,

    // ── Cold Zone (metadata only — actual data on disk) ──
    /// Known cold pages (not resident, metadata only).
    cold_pages: DashMap<PageId, Arc<PageHandle>>,

    // ── Counters ──
    total_fetches: AtomicU64,
    warm_hits: AtomicU64,
    warm_misses: AtomicU64,
    eviction_count: AtomicU64,
}

impl ZoneManager {
    pub fn new(config: ZoneConfig) -> Self {
        let lirs2 = Lirs2Cache::new(config.lirs2.clone());
        Self {
            hot_pages: DashMap::new(),
            hot_used_bytes: AtomicU64::new(0),
            warm_pages: DashMap::new(),
            warm_used_bytes: AtomicU64::new(0),
            lirs2: Mutex::new(lirs2),
            cold_pages: DashMap::new(),
            total_fetches: AtomicU64::new(0),
            warm_hits: AtomicU64::new(0),
            warm_misses: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            config,
        }
    }

    // ── Hot Zone Operations ──────────────────────────────────────────────────

    /// Allocate a page in the Hot zone (MemTable, index internal).
    /// Returns `Err` if the Hot zone is at capacity.
    pub fn alloc_hot(
        &self,
        page_id: PageId,
        data: PageData,
        priority: AccessPriority,
    ) -> Result<Arc<PageHandle>, ZoneError> {
        let size = data.len() as u64;
        let current = self.hot_used_bytes.load(Ordering::Acquire);
        if current + size > self.config.hot_capacity_bytes {
            return Err(ZoneError::HotZoneFull {
                used: current,
                capacity: self.config.hot_capacity_bytes,
                requested: size,
            });
        }

        let handle = Arc::new(PageHandle::new_with_data(page_id, priority, data));
        handle.set_tier(Tier::Hot);
        self.hot_pages.insert(page_id, handle.clone());
        self.hot_used_bytes.fetch_add(size, Ordering::Release);
        Ok(handle)
    }

    /// Free a page from the Hot zone.
    pub fn free_hot(&self, page_id: PageId) -> u64 {
        if let Some((_, handle)) = self.hot_pages.remove(&page_id) {
            let size = handle.data_size();
            self.hot_used_bytes.fetch_sub(size, Ordering::Release);
            size
        } else {
            0
        }
    }

    /// Get a handle to a Hot zone page.
    pub fn get_hot(&self, page_id: PageId) -> Option<Arc<PageHandle>> {
        self.hot_pages.get(&page_id).map(|r| r.value().clone())
    }

    /// Pin a Hot zone page (returns RAII guard).
    pub fn pin_hot(&self, page_id: PageId) -> Option<PinGuard> {
        self.get_hot(page_id).map(PinGuard::new)
    }

    pub fn hot_used_bytes(&self) -> u64 {
        self.hot_used_bytes.load(Ordering::Acquire)
    }

    pub fn hot_page_count(&self) -> usize {
        self.hot_pages.len()
    }

    // ── Warm Zone Operations ─────────────────────────────────────────────────

    /// Insert or access a page in the Warm zone.
    ///
    /// If the page is already warm, this is a cache hit (LIRS-2 updated).
    /// If the page is new, it is inserted and LIRS-2 may trigger eviction.
    pub fn access_warm(
        &self,
        page_id: PageId,
        data: PageData,
        priority: AccessPriority,
    ) -> Arc<PageHandle> {
        self.total_fetches.fetch_add(1, Ordering::Relaxed);

        // Check if already resident in Warm.
        if let Some(existing) = self.warm_pages.get(&page_id) {
            let handle = existing.value().clone();
            // Update LIRS-2.
            self.lirs2.lock().access(page_id, handle.clone());
            self.warm_hits.fetch_add(1, Ordering::Relaxed);
            return handle;
        }

        // New page — may need to evict to make room.
        let size = data.len() as u64;
        self.ensure_warm_budget(size);

        let handle = Arc::new(PageHandle::new_with_data(page_id, priority, data));
        handle.set_tier(Tier::Warm);

        // Register with LIRS-2.
        let result = self.lirs2.lock().access(page_id, handle.clone());
        self.warm_pages.insert(page_id, handle.clone());
        self.warm_used_bytes.fetch_add(size, Ordering::Release);

        match result {
            AccessResult::Hit => self.warm_hits.fetch_add(1, Ordering::Relaxed),
            _ => self.warm_misses.fetch_add(1, Ordering::Relaxed),
        };

        // Remove from cold tracking if present.
        self.cold_pages.remove(&page_id);

        handle
    }

    /// Get a handle to a Warm zone page (without loading data).
    pub fn get_warm(&self, page_id: PageId) -> Option<Arc<PageHandle>> {
        self.warm_pages.get(&page_id).map(|r| {
            let h = r.value().clone();
            self.lirs2.lock().access(page_id, h.clone());
            h
        })
    }

    /// Pin a Warm zone page.
    pub fn pin_warm(&self, page_id: PageId) -> Option<PinGuard> {
        self.get_warm(page_id).map(PinGuard::new)
    }

    /// Ensure the warm zone has at least `needed` bytes free by evicting pages.
    fn ensure_warm_budget(&self, needed: u64) {
        let target = self.config.warm_capacity_bytes.saturating_sub(needed);
        let mut attempts = 0;
        while self.warm_used_bytes.load(Ordering::Acquire) > target && attempts < 100 {
            attempts += 1;
            let candidate = self.lirs2.lock().evict_one();
            match candidate {
                Some(EvictionCandidate { page_id, handle }) => {
                    let freed = handle.data_size();
                    // The LIRS-2 evict_one already called handle.evict().
                    self.warm_pages.remove(&page_id);
                    self.warm_used_bytes.fetch_sub(freed, Ordering::Release);
                    self.eviction_count.fetch_add(1, Ordering::Relaxed);

                    // Track as cold.
                    let cold_handle =
                        Arc::new(PageHandle::new(page_id, handle.priority));
                    cold_handle.set_tier(Tier::Cold);
                    self.cold_pages.insert(page_id, cold_handle);
                }
                None => break, // nothing to evict
            }
        }
    }

    pub fn warm_used_bytes(&self) -> u64 {
        self.warm_used_bytes.load(Ordering::Acquire)
    }

    pub fn warm_page_count(&self) -> usize {
        self.warm_pages.len()
    }

    // ── Cold Zone Operations ─────────────────────────────────────────────────

    /// Register a cold page (metadata only, no data in memory).
    pub fn register_cold(&self, page_id: PageId, priority: AccessPriority) {
        if !self.hot_pages.contains_key(&page_id)
            && !self.warm_pages.contains_key(&page_id)
        {
            let handle = Arc::new(PageHandle::new(page_id, priority));
            handle.set_tier(Tier::Cold);
            self.cold_pages.insert(page_id, handle);
        }
    }

    /// Check if a page is cold (not resident).
    pub fn is_cold(&self, page_id: PageId) -> bool {
        self.cold_pages.contains_key(&page_id)
    }

    pub fn cold_page_count(&self) -> usize {
        self.cold_pages.len()
    }

    // ── Unified Lookup ───────────────────────────────────────────────────────

    /// Look up a page across all zones.
    /// Returns `Some(handle)` if found in Hot or Warm; `None` if cold/unknown.
    pub fn lookup(&self, page_id: PageId) -> Option<Arc<PageHandle>> {
        // Hot first (fastest).
        if let Some(h) = self.get_hot(page_id) {
            return Some(h);
        }
        // Warm second.
        if let Some(h) = self.get_warm(page_id) {
            return Some(h);
        }
        None
    }

    /// Remove a page from all zones (e.g. SST file deleted).
    pub fn remove(&self, page_id: PageId) {
        if let Some((_, h)) = self.hot_pages.remove(&page_id) {
            let size = h.data_size();
            self.hot_used_bytes.fetch_sub(size, Ordering::Release);
        }
        if let Some((_, h)) = self.warm_pages.remove(&page_id) {
            let size = h.data_size();
            self.warm_used_bytes.fetch_sub(size, Ordering::Release);
        }
        self.cold_pages.remove(&page_id);
        self.lirs2.lock().remove(page_id);
    }

    // ── Stats ────────────────────────────────────────────────────────────────

    pub fn stats(&self) -> ZoneStats {
        let lirs2_stats = self.lirs2.lock().stats.clone();
        ZoneStats {
            hot_used_bytes: self.hot_used_bytes(),
            hot_page_count: self.hot_pages.len() as u64,
            warm_used_bytes: self.warm_used_bytes(),
            warm_page_count: self.warm_pages.len() as u64,
            cold_page_count: self.cold_pages.len() as u64,
            total_fetches: self.total_fetches.load(Ordering::Relaxed),
            warm_hits: self.warm_hits.load(Ordering::Relaxed),
            warm_misses: self.warm_misses.load(Ordering::Relaxed),
            evictions: self.eviction_count.load(Ordering::Relaxed),
            lirs2: lirs2_stats,
        }
    }
}

// ── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ZoneError {
    HotZoneFull {
        used: u64,
        capacity: u64,
        requested: u64,
    },
    PageNotFound(PageId),
}

impl std::fmt::Display for ZoneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HotZoneFull {
                used,
                capacity,
                requested,
            } => write!(
                f,
                "Hot zone full: used={used}B, capacity={capacity}B, requested={requested}B"
            ),
            Self::PageNotFound(pid) => write!(f, "Page not found: {pid}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_config() -> ZoneConfig {
        ZoneConfig {
            hot_capacity_bytes: 1024,
            warm_capacity_bytes: 512,
            lirs2: Lirs2Config {
                lir_capacity: 4,
                hir_resident_capacity: 4,
                hir_nonresident_capacity: 8,
            },
        }
    }

    #[test]
    fn test_hot_alloc_and_free() {
        let zm = ZoneManager::new(small_config());
        let data = PageData::new(vec![42u8; 100]);
        let h = zm
            .alloc_hot(PageId(1), data, AccessPriority::IndexInternal)
            .unwrap();
        assert_eq!(h.tier(), Tier::Hot);
        assert_eq!(zm.hot_used_bytes(), 100);
        assert_eq!(zm.hot_page_count(), 1);

        let freed = zm.free_hot(PageId(1));
        assert_eq!(freed, 100);
        assert_eq!(zm.hot_used_bytes(), 0);
    }

    #[test]
    fn test_hot_zone_capacity_limit() {
        let zm = ZoneManager::new(small_config()); // 1024 bytes
        let data = PageData::new(vec![0u8; 600]);
        zm.alloc_hot(PageId(1), data, AccessPriority::IndexInternal)
            .unwrap();
        let data2 = PageData::new(vec![0u8; 600]);
        let result = zm.alloc_hot(PageId(2), data2, AccessPriority::IndexInternal);
        assert!(result.is_err()); // 600 + 600 > 1024
    }

    #[test]
    fn test_warm_access_hit() {
        let zm = ZoneManager::new(small_config());
        let data = PageData::new(vec![1u8; 64]);
        zm.access_warm(PageId(10), data, AccessPriority::HotRow);
        assert_eq!(zm.warm_page_count(), 1);

        // Second access — cache hit.
        let data2 = PageData::new(vec![2u8; 64]);
        zm.access_warm(PageId(10), data2, AccessPriority::HotRow);
        assert_eq!(zm.warm_page_count(), 1); // still 1

        let stats = zm.stats();
        assert_eq!(stats.warm_hits, 1);
    }

    #[test]
    fn test_warm_eviction_on_overflow() {
        let zm = ZoneManager::new(ZoneConfig {
            hot_capacity_bytes: 1024,
            warm_capacity_bytes: 256,
            lirs2: Lirs2Config {
                lir_capacity: 2,
                hir_resident_capacity: 2,
                hir_nonresident_capacity: 4,
            },
        });

        // Fill warm zone beyond capacity.
        for i in 0..10u64 {
            let data = PageData::new(vec![i as u8; 64]);
            zm.access_warm(PageId(i), data, AccessPriority::WarmScan);
        }

        // Evictions must have occurred — budget enforcement is working.
        let stats = zm.stats();
        assert!(stats.evictions > 0, "expected evictions, got 0");
        // Warm zone should have fewer pages than we inserted (10).
        assert!(
            zm.warm_page_count() < 10,
            "expected fewer than 10 warm pages, got {}",
            zm.warm_page_count()
        );
    }

    #[test]
    fn test_cold_registration() {
        let zm = ZoneManager::new(small_config());
        zm.register_cold(PageId(99), AccessPriority::Cold);
        assert!(zm.is_cold(PageId(99)));
        assert_eq!(zm.cold_page_count(), 1);

        // Loading into warm removes from cold.
        let data = PageData::new(vec![0u8; 32]);
        zm.access_warm(PageId(99), data, AccessPriority::HotRow);
        assert!(!zm.is_cold(PageId(99)));
    }

    #[test]
    fn test_unified_lookup() {
        let zm = ZoneManager::new(small_config());

        // Hot page.
        let data_hot = PageData::new(vec![1u8; 50]);
        zm.alloc_hot(PageId(1), data_hot, AccessPriority::IndexInternal)
            .unwrap();
        assert!(zm.lookup(PageId(1)).is_some());

        // Warm page.
        let data_warm = PageData::new(vec![2u8; 50]);
        zm.access_warm(PageId(2), data_warm, AccessPriority::HotRow);
        assert!(zm.lookup(PageId(2)).is_some());

        // Unknown page.
        assert!(zm.lookup(PageId(999)).is_none());
    }

    #[test]
    fn test_remove_from_all_zones() {
        let zm = ZoneManager::new(small_config());
        let data = PageData::new(vec![0u8; 50]);
        zm.alloc_hot(PageId(1), data.clone(), AccessPriority::IndexInternal)
            .unwrap();
        zm.access_warm(PageId(2), data, AccessPriority::HotRow);
        zm.register_cold(PageId(3), AccessPriority::Cold);

        zm.remove(PageId(1));
        zm.remove(PageId(2));
        zm.remove(PageId(3));

        assert!(zm.lookup(PageId(1)).is_none());
        assert!(zm.lookup(PageId(2)).is_none());
        assert!(!zm.is_cold(PageId(3)));
        assert_eq!(zm.hot_used_bytes(), 0);
    }

    #[test]
    fn test_pin_hot_page() {
        let zm = ZoneManager::new(small_config());
        let data = PageData::new(vec![0u8; 50]);
        zm.alloc_hot(PageId(1), data, AccessPriority::IndexInternal)
            .unwrap();

        {
            let guard = zm.pin_hot(PageId(1)).expect("should pin");
            assert_eq!(guard.page_id(), PageId(1));
            assert!(guard.handle().is_pinned());
        }
        // Guard dropped — unpinned.
        let h = zm.get_hot(PageId(1)).unwrap();
        assert!(!h.is_pinned());
    }
}
