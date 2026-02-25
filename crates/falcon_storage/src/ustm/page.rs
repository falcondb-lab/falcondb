//! USTM Core Types — PageId, PageHandle, AccessPriority, Tier.
//!
//! `PageHandle` is the single abstraction through which the executor and txn
//! layer access data.  Upper layers never know whether the backing bytes live
//! in DRAM, NVMe page-cache, or cold storage.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

// ── Identifiers ──────────────────────────────────────────────────────────────

/// Globally unique page identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(pub u64);

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "page:{}", self.0)
    }
}

// ── Tier ─────────────────────────────────────────────────────────────────────

/// Memory tier where a page currently resides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tier {
    /// Hot: DRAM arena — MemTable, index internal nodes, active write-sets.
    Hot = 0,
    /// Warm: DRAM page-cache — recently fetched SST pages.
    Warm = 1,
    /// Cold: on-disk — SST files, only reachable through async I/O.
    Cold = 2,
    /// Evicted: was in Warm but has been evicted; metadata retained.
    Evicted = 3,
}

impl Tier {
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Hot,
            1 => Self::Warm,
            2 => Self::Cold,
            _ => Self::Evicted,
        }
    }
}

// ── Access Priority ──────────────────────────────────────────────────────────

/// Database-semantic priority that drives eviction decisions.
/// Higher ordinal = more important = harder to evict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum AccessPriority {
    /// Pre-fetched speculatively; may be discarded immediately.
    Prefetched = 0,
    /// Cold / archival data.
    Cold = 1,
    /// Warm scan data (seq-scan pages).
    WarmScan = 2,
    /// Hot rows touched by recent point-lookups.
    HotRow = 3,
    /// B-tree leaf nodes.
    IndexLeaf = 4,
    /// B-tree internal nodes — almost never evicted.
    IndexInternal = 5,
}

impl AccessPriority {
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Prefetched,
            1 => Self::Cold,
            2 => Self::WarmScan,
            3 => Self::HotRow,
            4 => Self::IndexLeaf,
            _ => Self::IndexInternal,
        }
    }
}

// ── Page Data ────────────────────────────────────────────────────────────────

/// The actual page payload — a fixed-size aligned buffer.
#[derive(Clone)]
pub struct PageData {
    /// Raw bytes.  For Hot-zone MemTable pages this is the serialized row
    /// batch; for SST pages it is the on-disk block (possibly compressed).
    buf: Vec<u8>,
}

impl PageData {
    pub const fn new(buf: Vec<u8>) -> Self {
        Self { buf }
    }

    pub fn zeroed(size: usize) -> Self {
        Self {
            buf: vec![0u8; size],
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

impl std::fmt::Debug for PageData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PageData({}B)", self.buf.len())
    }
}

// ── Page Handle ──────────────────────────────────────────────────────────────

/// Thread-safe handle to a cached page.
///
/// The executor obtains a `PageHandle` from the USTM engine, *pins* it for the
/// duration of the access, then releases the pin.  While pinned the page is
/// guaranteed to stay resident in DRAM and will **never** be evicted.
pub struct PageHandle {
    pub page_id: PageId,
    /// Current tier (atomic so we can read without locking).
    tier: AtomicU8,
    /// Pin count — while > 0 the page cannot be evicted.
    pin_count: AtomicU32,
    /// Cumulative access count (used by LIRS-2).
    access_count: AtomicU64,
    /// Last access timestamp in nanoseconds (monotonic clock).
    last_access_ns: AtomicU64,
    /// Semantic priority assigned by the caller.
    pub priority: AccessPriority,
    /// Whether the page has been modified since last flush.
    dirty: AtomicU8,
    /// The actual data (behind Arc for cheap cloning in eviction path).
    data: parking_lot::RwLock<Option<PageData>>,
    /// Size of the page data in bytes (cached for budget accounting).
    data_size: AtomicU64,
}

impl PageHandle {
    pub const fn new(page_id: PageId, priority: AccessPriority) -> Self {
        Self {
            page_id,
            tier: AtomicU8::new(Tier::Cold as u8),
            pin_count: AtomicU32::new(0),
            access_count: AtomicU64::new(0),
            last_access_ns: AtomicU64::new(0),
            priority,
            dirty: AtomicU8::new(0),
            data: parking_lot::RwLock::new(None),
            data_size: AtomicU64::new(0),
        }
    }

    pub fn new_with_data(page_id: PageId, priority: AccessPriority, data: PageData) -> Self {
        let size = data.len() as u64;
        Self {
            page_id,
            tier: AtomicU8::new(Tier::Warm as u8),
            pin_count: AtomicU32::new(0),
            access_count: AtomicU64::new(1),
            last_access_ns: AtomicU64::new(now_ns()),
            priority,
            dirty: AtomicU8::new(0),
            data: parking_lot::RwLock::new(Some(data)),
            data_size: AtomicU64::new(size),
        }
    }

    // ── Pin / Unpin ──

    /// Pin the page — guarantees it stays in DRAM until unpinned.
    pub fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::AcqRel);
        self.record_access();
    }

    /// Unpin the page.  Panics (debug) if pin_count is already 0.
    pub fn unpin(&self) {
        let prev = self.pin_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "unpin on page with pin_count == 0");
    }

    #[inline]
    pub fn pin_count(&self) -> u32 {
        self.pin_count.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.pin_count() > 0
    }

    // ── Tier ──

    #[inline]
    pub fn tier(&self) -> Tier {
        Tier::from_u8(self.tier.load(Ordering::Acquire))
    }

    pub fn set_tier(&self, t: Tier) {
        self.tier.store(t as u8, Ordering::Release);
    }

    // ── Dirty ──

    pub fn mark_dirty(&self) {
        self.dirty.store(1, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        self.dirty.store(0, Ordering::Release);
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire) != 0
    }

    // ── Data access ──

    /// Read the page data.  Returns `None` if the page is not resident.
    pub fn read(&self) -> Option<parking_lot::RwLockReadGuard<'_, Option<PageData>>> {
        let guard = self.data.read();
        if guard.is_some() {
            Some(guard)
        } else {
            None
        }
    }

    /// Write page data (loads the page into Warm tier).
    pub fn load_data(&self, data: PageData) {
        let size = data.len() as u64;
        *self.data.write() = Some(data);
        self.data_size.store(size, Ordering::Release);
        self.set_tier(Tier::Warm);
    }

    /// Evict data from memory (move to Cold tier).
    /// Returns the evicted data size, or 0 if already evicted or pinned.
    pub fn evict(&self) -> u64 {
        if self.is_pinned() {
            return 0;
        }
        let mut guard = self.data.write();
        if guard.is_none() {
            return 0;
        }
        let size = self.data_size.load(Ordering::Acquire);
        *guard = None;
        self.data_size.store(0, Ordering::Release);
        self.set_tier(Tier::Evicted);
        size
    }

    /// Check if data is resident in memory.
    pub fn is_resident(&self) -> bool {
        self.data.read().is_some()
    }

    #[inline]
    pub fn data_size(&self) -> u64 {
        self.data_size.load(Ordering::Acquire)
    }

    // ── Stats ──

    fn record_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
        self.last_access_ns.store(now_ns(), Ordering::Relaxed);
    }

    #[inline]
    pub fn access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn last_access_ns(&self) -> u64 {
        self.last_access_ns.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for PageHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageHandle")
            .field("page_id", &self.page_id)
            .field("tier", &self.tier())
            .field("pin_count", &self.pin_count())
            .field("access_count", &self.access_count())
            .field("priority", &self.priority)
            .field("dirty", &self.is_dirty())
            .field("data_size", &self.data_size())
            .finish()
    }
}

// ── RAII Pin Guard ───────────────────────────────────────────────────────────

/// RAII guard that automatically unpins the page on drop.
pub struct PinGuard {
    handle: Arc<PageHandle>,
}

impl PinGuard {
    pub fn new(handle: Arc<PageHandle>) -> Self {
        handle.pin();
        Self { handle }
    }

    pub fn handle(&self) -> &PageHandle {
        &self.handle
    }

    pub fn page_id(&self) -> PageId {
        self.handle.page_id
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.handle.unpin();
    }
}

impl std::fmt::Debug for PinGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PinGuard({:?})", self.handle.page_id)
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Fast hash of a primary key byte slice for USTM page ID derivation.
pub fn fast_hash_pk(pk: &[u8]) -> u32 {
    // FNV-1a 32-bit hash — fast, no allocation, good distribution.
    let mut h: u32 = 0x811c_9dc5;
    for &b in pk {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Monotonic timestamp in nanoseconds.
fn now_ns() -> u64 {
    use std::time::Instant;
    // Use a thread-local cached epoch so Instant::now() returns ns since
    // process start.  Good enough for ordering; not wall-clock.
    thread_local! {
        static EPOCH: Instant = Instant::now();
    }
    EPOCH.with(|e| e.elapsed().as_nanos() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_handle_pin_unpin() {
        let h = PageHandle::new(PageId(1), AccessPriority::HotRow);
        assert_eq!(h.pin_count(), 0);
        assert!(!h.is_pinned());
        h.pin();
        assert_eq!(h.pin_count(), 1);
        assert!(h.is_pinned());
        h.pin();
        assert_eq!(h.pin_count(), 2);
        h.unpin();
        assert_eq!(h.pin_count(), 1);
        h.unpin();
        assert_eq!(h.pin_count(), 0);
    }

    #[test]
    fn test_page_handle_data_lifecycle() {
        let h = PageHandle::new(PageId(42), AccessPriority::IndexLeaf);
        assert!(!h.is_resident());
        assert_eq!(h.tier(), Tier::Cold);

        h.load_data(PageData::new(vec![1, 2, 3, 4]));
        assert!(h.is_resident());
        assert_eq!(h.tier(), Tier::Warm);
        assert_eq!(h.data_size(), 4);

        let evicted = h.evict();
        assert_eq!(evicted, 4);
        assert!(!h.is_resident());
        assert_eq!(h.tier(), Tier::Evicted);
    }

    #[test]
    fn test_page_handle_pinned_cannot_evict() {
        let h = PageHandle::new(PageId(99), AccessPriority::HotRow);
        h.load_data(PageData::new(vec![0; 1024]));
        h.pin();
        let evicted = h.evict();
        assert_eq!(evicted, 0); // pinned — cannot evict
        assert!(h.is_resident());
        h.unpin();
        let evicted = h.evict();
        assert_eq!(evicted, 1024);
    }

    #[test]
    fn test_pin_guard_raii() {
        let h = Arc::new(PageHandle::new(PageId(7), AccessPriority::Cold));
        assert_eq!(h.pin_count(), 0);
        {
            let _g = PinGuard::new(h.clone());
            assert_eq!(h.pin_count(), 1);
        }
        assert_eq!(h.pin_count(), 0);
    }

    #[test]
    fn test_dirty_flag() {
        let h = PageHandle::new(PageId(1), AccessPriority::HotRow);
        assert!(!h.is_dirty());
        h.mark_dirty();
        assert!(h.is_dirty());
        h.clear_dirty();
        assert!(!h.is_dirty());
    }

    #[test]
    fn test_access_priority_ordering() {
        assert!(AccessPriority::Prefetched < AccessPriority::Cold);
        assert!(AccessPriority::Cold < AccessPriority::WarmScan);
        assert!(AccessPriority::WarmScan < AccessPriority::HotRow);
        assert!(AccessPriority::HotRow < AccessPriority::IndexLeaf);
        assert!(AccessPriority::IndexLeaf < AccessPriority::IndexInternal);
    }

    #[test]
    fn test_tier_roundtrip() {
        for t in [Tier::Hot, Tier::Warm, Tier::Cold, Tier::Evicted] {
            assert_eq!(Tier::from_u8(t as u8), t);
        }
    }
}
