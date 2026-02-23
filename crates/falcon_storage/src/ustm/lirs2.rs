//! LIRS-2 Eviction Algorithm — Scan-resistant cache replacement.
//!
//! Traditional LRU suffers from *cache pollution*: a single sequential scan
//! pushes out all hot data.  LIRS (Low Inter-reference Recency Set) solves
//! this by classifying pages into two sets:
//!
//! * **LIR** (Low Inter-Reference recency) — frequently accessed pages that
//!   are *protected* from eviction.
//! * **HIR** (High Inter-Reference recency) — infrequently accessed or
//!   first-touch pages that are candidates for eviction.
//!
//! A page starts in HIR on first access.  Only on a *second* access does it
//! promote to LIR.  This means a full-table scan (touching each page once)
//! never pollutes the LIR set.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use super::page::{PageHandle, PageId};

/// Configuration for the LIRS-2 cache.
#[derive(Debug, Clone)]
pub struct Lirs2Config {
    /// Maximum number of pages in the LIR set.
    pub lir_capacity: usize,
    /// Maximum number of *resident* HIR pages.
    pub hir_resident_capacity: usize,
    /// Maximum number of *non-resident* HIR entries (metadata only).
    pub hir_nonresident_capacity: usize,
}

impl Default for Lirs2Config {
    fn default() -> Self {
        Self {
            lir_capacity: 4096,
            hir_resident_capacity: 1024,
            hir_nonresident_capacity: 2048,
        }
    }
}

/// A cached entry in the LIRS-2 structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageStatus {
    /// Protected high-frequency page.
    Lir,
    /// Resident but eviction-candidate page.
    HirResident,
    /// Evicted page — metadata only (tracks inter-reference distance).
    HirNonResident,
}

/// Internal metadata for a tracked page.
struct Entry {
    status: PageStatus,
    handle: Arc<PageHandle>,
    /// Position in the recency stack (lower = more recent).
    recency_seq: u64,
}

/// LIRS-2 eviction controller.
///
/// Thread-safety: this struct is **not** internally synchronised.  The caller
/// (`ZoneManager`) must hold a lock before calling any method.
pub struct Lirs2Cache {
    config: Lirs2Config,
    /// All tracked pages.
    entries: HashMap<PageId, Entry>,
    /// LIR pages ordered by recency (front = most recent).
    lir_queue: VecDeque<PageId>,
    /// Resident HIR pages ordered by recency (front = most recent).
    hir_resident_queue: VecDeque<PageId>,
    /// Non-resident HIR page ids (bounded ring buffer).
    hir_nonresident_queue: VecDeque<PageId>,
    /// Monotonically increasing sequence for recency tracking.
    seq: u64,
    /// Cumulative stats.
    pub stats: Lirs2Stats,
}

/// Observable statistics.
#[derive(Debug, Clone, Default)]
pub struct Lirs2Stats {
    pub hits: u64,
    pub misses: u64,
    pub lir_promotions: u64,
    pub evictions: u64,
}

/// Result of an `access` call.
#[derive(Debug, PartialEq, Eq)]
pub enum AccessResult {
    /// Page was already in LIR or HIR-resident — cache hit.
    Hit,
    /// Page was HIR-nonresident — promoted to LIR (second touch).
    PromotedToLir,
    /// Page was not tracked at all — inserted as HIR-resident (first touch).
    Inserted,
}

/// Eviction candidate returned by `evict_one`.
#[derive(Debug)]
pub struct EvictionCandidate {
    pub page_id: PageId,
    pub handle: Arc<PageHandle>,
}

impl Lirs2Cache {
    pub fn new(config: Lirs2Config) -> Self {
        Self {
            entries: HashMap::with_capacity(
                config.lir_capacity + config.hir_resident_capacity + config.hir_nonresident_capacity,
            ),
            lir_queue: VecDeque::with_capacity(config.lir_capacity),
            hir_resident_queue: VecDeque::with_capacity(config.hir_resident_capacity),
            hir_nonresident_queue: VecDeque::with_capacity(config.hir_nonresident_capacity),
            config,
            seq: 0,
            stats: Lirs2Stats::default(),
        }
    }

    /// Notify the cache that `page_id` was accessed.
    ///
    /// * First touch → inserted as HIR-resident.
    /// * Second touch (was HIR-nonresident) → promoted to LIR.
    /// * Already LIR/HIR-resident → refreshed (hit).
    pub fn access(&mut self, page_id: PageId, handle: Arc<PageHandle>) -> AccessResult {
        self.seq += 1;
        let seq = self.seq;

        if let Some(entry) = self.entries.get_mut(&page_id) {
            match entry.status {
                PageStatus::Lir => {
                    // LIR hit — move to front of LIR queue.
                    entry.recency_seq = seq;
                    self.move_to_front_lir(page_id);
                    self.stats.hits += 1;
                    AccessResult::Hit
                }
                PageStatus::HirResident => {
                    // HIR-resident hit — promote to LIR (second touch pattern).
                    entry.status = PageStatus::Lir;
                    entry.recency_seq = seq;
                    self.remove_from_hir_resident(page_id);
                    self.lir_queue.push_front(page_id);
                    self.stats.hits += 1;
                    self.stats.lir_promotions += 1;
                    self.maybe_demote_lir_tail();
                    AccessResult::Hit
                }
                PageStatus::HirNonResident => {
                    // Was evicted, now re-accessed → promote to LIR.
                    entry.status = PageStatus::Lir;
                    entry.handle = handle;
                    entry.recency_seq = seq;
                    self.remove_from_hir_nonresident(page_id);
                    self.lir_queue.push_front(page_id);
                    self.stats.misses += 1;
                    self.stats.lir_promotions += 1;
                    self.maybe_demote_lir_tail();
                    AccessResult::PromotedToLir
                }
            }
        } else {
            // Brand new page — insert as HIR-resident (first touch).
            let entry = Entry {
                status: PageStatus::HirResident,
                handle,
                recency_seq: seq,
            };
            self.entries.insert(page_id, entry);
            self.hir_resident_queue.push_front(page_id);
            self.stats.misses += 1;
            // If HIR-resident overflows, evict the tail.
            if self.hir_resident_queue.len() > self.config.hir_resident_capacity {
                self.evict_hir_resident_tail();
            }
            AccessResult::Inserted
        }
    }

    /// Select and evict the least-valuable page.  Returns `None` if the cache
    /// is empty or all pages are pinned.
    pub fn evict_one(&mut self) -> Option<EvictionCandidate> {
        // First, try to evict from HIR-resident (lowest value pages).
        // Scan from tail (least recently used) and skip pinned pages.
        let mut attempts = self.hir_resident_queue.len();
        while attempts > 0 {
            attempts -= 1;
            if let Some(page_id) = self.hir_resident_queue.pop_back() {
                if let Some(entry) = self.entries.get(&page_id) {
                    if entry.handle.is_pinned() {
                        // Pinned — put it back at front and try next.
                        self.hir_resident_queue.push_front(page_id);
                        continue;
                    }
                    let handle = entry.handle.clone();
                    // Demote to non-resident.
                    self.demote_to_nonresident(page_id);
                    self.stats.evictions += 1;
                    return Some(EvictionCandidate { page_id, handle });
                }
            }
        }

        // If no HIR-resident available, try evicting LIR tail (rare).
        let mut attempts = self.lir_queue.len();
        while attempts > 0 {
            attempts -= 1;
            if let Some(page_id) = self.lir_queue.pop_back() {
                if let Some(entry) = self.entries.get(&page_id) {
                    if entry.handle.is_pinned() {
                        self.lir_queue.push_front(page_id);
                        continue;
                    }
                    let handle = entry.handle.clone();
                    self.demote_to_nonresident(page_id);
                    self.stats.evictions += 1;
                    return Some(EvictionCandidate { page_id, handle });
                }
            }
        }

        None
    }

    /// Remove a page from tracking entirely (e.g. when the SST file is deleted).
    pub fn remove(&mut self, page_id: PageId) {
        self.entries.remove(&page_id);
        self.remove_from_lir(page_id);
        self.remove_from_hir_resident(page_id);
        self.remove_from_hir_nonresident(page_id);
    }

    // ── Queries ──

    pub fn lir_count(&self) -> usize {
        self.lir_queue.len()
    }

    pub fn hir_resident_count(&self) -> usize {
        self.hir_resident_queue.len()
    }

    pub fn hir_nonresident_count(&self) -> usize {
        self.hir_nonresident_queue.len()
    }

    pub fn total_tracked(&self) -> usize {
        self.entries.len()
    }

    pub fn contains(&self, page_id: PageId) -> bool {
        self.entries.contains_key(&page_id)
    }

    pub fn status(&self, page_id: PageId) -> Option<&'static str> {
        self.entries.get(&page_id).map(|e| match e.status {
            PageStatus::Lir => "LIR",
            PageStatus::HirResident => "HIR-resident",
            PageStatus::HirNonResident => "HIR-nonresident",
        })
    }

    // ── Internal helpers ──

    fn move_to_front_lir(&mut self, page_id: PageId) {
        self.remove_from_lir(page_id);
        self.lir_queue.push_front(page_id);
    }

    fn remove_from_lir(&mut self, page_id: PageId) {
        self.lir_queue.retain(|&id| id != page_id);
    }

    fn remove_from_hir_resident(&mut self, page_id: PageId) {
        self.hir_resident_queue.retain(|&id| id != page_id);
    }

    fn remove_from_hir_nonresident(&mut self, page_id: PageId) {
        self.hir_nonresident_queue.retain(|&id| id != page_id);
    }

    /// If LIR set is over capacity, demote the tail entry to HIR-resident.
    fn maybe_demote_lir_tail(&mut self) {
        while self.lir_queue.len() > self.config.lir_capacity {
            if let Some(tail_id) = self.lir_queue.pop_back() {
                if let Some(entry) = self.entries.get_mut(&tail_id) {
                    entry.status = PageStatus::HirResident;
                    self.hir_resident_queue.push_front(tail_id);
                }
                // Chain: if HIR-resident overflows, evict its tail.
                if self.hir_resident_queue.len() > self.config.hir_resident_capacity {
                    self.evict_hir_resident_tail();
                }
            }
        }
    }

    /// Evict the least-recently-used HIR-resident page (skip pinned).
    fn evict_hir_resident_tail(&mut self) {
        let mut rotated = Vec::new();
        while let Some(tail_id) = self.hir_resident_queue.pop_back() {
            if let Some(entry) = self.entries.get(&tail_id) {
                if entry.handle.is_pinned() {
                    rotated.push(tail_id);
                    continue;
                }
            }
            self.demote_to_nonresident(tail_id);
            self.stats.evictions += 1;
            break;
        }
        // Put back pinned pages.
        for id in rotated.into_iter().rev() {
            self.hir_resident_queue.push_back(id);
        }
    }

    /// Move a page from LIR/HIR-resident to HIR-nonresident.
    fn demote_to_nonresident(&mut self, page_id: PageId) {
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.handle.evict();
            entry.status = PageStatus::HirNonResident;
        }
        self.remove_from_lir(page_id);
        self.remove_from_hir_resident(page_id);
        self.hir_nonresident_queue.push_front(page_id);
        // Trim non-resident if over capacity.
        while self.hir_nonresident_queue.len() > self.config.hir_nonresident_capacity {
            if let Some(old_id) = self.hir_nonresident_queue.pop_back() {
                self.entries.remove(&old_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ustm::page::{AccessPriority, PageData, PageHandle};

    fn make_handle(id: u64) -> Arc<PageHandle> {
        let h = Arc::new(PageHandle::new(PageId(id), AccessPriority::HotRow));
        h.load_data(PageData::new(vec![0u8; 64]));
        h
    }

    #[test]
    fn test_first_touch_goes_to_hir() {
        let mut cache = Lirs2Cache::new(Lirs2Config {
            lir_capacity: 4,
            hir_resident_capacity: 2,
            hir_nonresident_capacity: 4,
        });
        let h = make_handle(1);
        let result = cache.access(PageId(1), h);
        assert_eq!(result, AccessResult::Inserted);
        assert_eq!(cache.hir_resident_count(), 1);
        assert_eq!(cache.lir_count(), 0);
        assert_eq!(cache.status(PageId(1)), Some("HIR-resident"));
    }

    #[test]
    fn test_second_touch_promotes_to_lir() {
        let mut cache = Lirs2Cache::new(Lirs2Config {
            lir_capacity: 4,
            hir_resident_capacity: 2,
            hir_nonresident_capacity: 4,
        });
        let h = make_handle(1);
        cache.access(PageId(1), h.clone());
        // Second touch while still HIR-resident → promote to LIR.
        let result = cache.access(PageId(1), h);
        assert_eq!(result, AccessResult::Hit);
        assert_eq!(cache.status(PageId(1)), Some("LIR"));
        assert_eq!(cache.lir_count(), 1);
        assert_eq!(cache.hir_resident_count(), 0);
    }

    #[test]
    fn test_sequential_scan_does_not_pollute_lir() {
        let mut cache = Lirs2Cache::new(Lirs2Config {
            lir_capacity: 2,
            hir_resident_capacity: 2,
            hir_nonresident_capacity: 4,
        });
        // Insert two LIR pages (accessed twice each).
        for id in 1..=2 {
            let h = make_handle(id);
            cache.access(PageId(id), h.clone());
            cache.access(PageId(id), h);
        }
        assert_eq!(cache.lir_count(), 2);

        // Simulate a sequential scan of 10 pages (each touched once).
        for id in 100..110 {
            let h = make_handle(id);
            cache.access(PageId(id), h);
        }

        // LIR should still contain the original 2 hot pages.
        assert_eq!(cache.lir_count(), 2);
        assert_eq!(cache.status(PageId(1)), Some("LIR"));
        assert_eq!(cache.status(PageId(2)), Some("LIR"));
    }

    #[test]
    fn test_evict_one_picks_hir_resident_tail() {
        let mut cache = Lirs2Cache::new(Lirs2Config {
            lir_capacity: 4,
            hir_resident_capacity: 4,
            hir_nonresident_capacity: 4,
        });
        // Insert pages 1-4 as HIR-resident.
        for id in 1..=4 {
            let h = make_handle(id);
            cache.access(PageId(id), h);
        }
        // Evict should pick page 1 (oldest HIR-resident).
        let victim = cache.evict_one().expect("should evict");
        assert_eq!(victim.page_id, PageId(1));
        assert_eq!(cache.status(PageId(1)), Some("HIR-nonresident"));
    }

    #[test]
    fn test_evict_skips_pinned() {
        let mut cache = Lirs2Cache::new(Lirs2Config {
            lir_capacity: 4,
            hir_resident_capacity: 4,
            hir_nonresident_capacity: 4,
        });
        let h1 = make_handle(1);
        let h2 = make_handle(2);
        cache.access(PageId(1), h1.clone());
        cache.access(PageId(2), h2.clone());

        // Pin page 1.
        h1.pin();
        let victim = cache.evict_one().expect("should evict");
        assert_eq!(victim.page_id, PageId(2)); // page 1 is pinned, skip it.
        h1.unpin();
    }

    #[test]
    fn test_nonresident_re_access_promotes_to_lir() {
        let mut cache = Lirs2Cache::new(Lirs2Config {
            lir_capacity: 4,
            hir_resident_capacity: 1,
            hir_nonresident_capacity: 4,
        });
        // Insert page 1 as HIR-resident.
        let h1 = make_handle(1);
        cache.access(PageId(1), h1.clone());
        // Insert page 2 — causes page 1 to be evicted (hir cap = 1).
        let h2 = make_handle(2);
        cache.access(PageId(2), h2);
        assert_eq!(cache.status(PageId(1)), Some("HIR-nonresident"));

        // Re-access page 1 → should promote to LIR.
        let h1_new = make_handle(1);
        let result = cache.access(PageId(1), h1_new);
        assert_eq!(result, AccessResult::PromotedToLir);
        assert_eq!(cache.status(PageId(1)), Some("LIR"));
    }

    #[test]
    fn test_lir_overflow_demotes_tail_to_hir() {
        let mut cache = Lirs2Cache::new(Lirs2Config {
            lir_capacity: 2,
            hir_resident_capacity: 4,
            hir_nonresident_capacity: 4,
        });
        // Promote 3 pages to LIR (exceeds capacity of 2).
        for id in 1..=3 {
            let h = make_handle(id);
            cache.access(PageId(id), h.clone());
            cache.access(PageId(id), h); // second touch → LIR
        }
        // LIR should cap at 2; oldest LIR page demoted to HIR-resident.
        assert_eq!(cache.lir_count(), 2);
        assert_eq!(cache.status(PageId(1)), Some("HIR-resident"));
        assert_eq!(cache.status(PageId(2)), Some("LIR"));
        assert_eq!(cache.status(PageId(3)), Some("LIR"));
    }

    #[test]
    fn test_remove() {
        let mut cache = Lirs2Cache::new(Lirs2Config::default());
        let h = make_handle(1);
        cache.access(PageId(1), h);
        assert!(cache.contains(PageId(1)));
        cache.remove(PageId(1));
        assert!(!cache.contains(PageId(1)));
    }

    #[test]
    fn test_stats() {
        let mut cache = Lirs2Cache::new(Lirs2Config::default());
        let h = make_handle(1);
        cache.access(PageId(1), h.clone()); // miss (insert)
        cache.access(PageId(1), h.clone()); // hit (promote)
        cache.access(PageId(1), h);         // hit (LIR refresh)
        assert_eq!(cache.stats.misses, 1);
        assert_eq!(cache.stats.hits, 2);
        assert_eq!(cache.stats.lir_promotions, 1);
    }
}
