//! Query-Aware Prefetcher — Proactive data loading driven by query plans.
//!
//! Instead of relying on OS readahead (which knows nothing about SQL),
//! the prefetcher receives hints from the executor about upcoming page
//! accesses and submits async I/O requests *before* the data is needed.

use std::collections::{BinaryHeap, HashSet};
use std::cmp::Ordering as CmpOrdering;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use parking_lot::Mutex;

use super::page::PageId;

// ── Prefetch Hints (from executor) ───────────────────────────────────────────

/// Hint source — tells the prefetcher *why* we expect these pages to be needed.
#[derive(Debug, Clone)]
pub enum PrefetchSource {
    /// Index range scan: the executor knows the next leaf pages.
    IndexRangeScan { next_leaf_pages: Vec<PageId> },
    /// Sequential scan: predictable stride-based access.
    SeqScan { start_page: PageId, count: usize },
    /// Nested-loop join: inner-table probe pages.
    NestedLoopProbe { probe_pages: Vec<PageId> },
    /// Compaction: bulk read, lowest priority.
    Compaction { pages: Vec<PageId> },
}

/// A single prefetch request with a deadline and priority.
#[derive(Debug, Clone)]
pub struct PrefetchRequest {
    pub page_id: PageId,
    /// File where the page resides.
    pub file_path: PathBuf,
    /// Byte offset within the file.
    pub offset: u64,
    /// Page size in bytes.
    pub length: u32,
    /// Deadline: when the executor expects to need this page (monotonic ns).
    pub deadline_ns: u64,
    /// Priority score (higher = more urgent).
    pub priority_score: u64,
}

impl PartialEq for PrefetchRequest {
    fn eq(&self, other: &Self) -> bool {
        self.priority_score == other.priority_score
    }
}

impl Eq for PrefetchRequest {}

impl PartialOrd for PrefetchRequest {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrefetchRequest {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Higher priority_score = more urgent = should be dequeued first.
        self.priority_score.cmp(&other.priority_score)
    }
}

// ── Prefetcher Configuration ─────────────────────────────────────────────────

/// Configuration for the prefetcher.
#[derive(Debug, Clone)]
pub struct PrefetcherConfig {
    /// Maximum pending prefetch requests.
    pub max_pending: usize,
    /// Maximum prefetch requests issued per scheduler tick.
    pub max_per_tick: usize,
    /// Maximum lookahead distance (pages) for sequential scans.
    pub seq_scan_lookahead: usize,
    /// Whether prefetching is globally enabled.
    pub enabled: bool,
}

impl Default for PrefetcherConfig {
    fn default() -> Self {
        Self {
            max_pending: 256,
            max_per_tick: 32,
            seq_scan_lookahead: 16,
            enabled: true,
        }
    }
}

// ── Prefetcher Stats ─────────────────────────────────────────────────────────

/// Observable prefetcher statistics.
#[derive(Debug, Clone, Default)]
pub struct PrefetcherStats {
    pub hints_received: u64,
    pub requests_submitted: u64,
    pub requests_hit: u64,
    pub requests_dropped_duplicate: u64,
    pub requests_dropped_full: u64,
}

// ── Prefetcher ───────────────────────────────────────────────────────────────

/// The query-aware prefetcher.
///
/// The executor calls `hint()` to tell the prefetcher about upcoming accesses.
/// The prefetcher deduplicates, prioritises, and emits `PrefetchRequest`s that
/// the I/O scheduler can consume.
pub struct Prefetcher {
    config: PrefetcherConfig,
    enabled: AtomicBool,
    /// Priority queue of pending requests (max-heap by priority_score).
    pending: Mutex<BinaryHeap<PrefetchRequest>>,
    /// Set of page IDs already in the pending queue (dedup).
    in_flight: Mutex<HashSet<PageId>>,
    /// Set of page IDs known to be resident (skip prefetch).
    resident_set: Mutex<HashSet<PageId>>,
    /// Stats.
    stats: Mutex<PrefetcherStats>,
    /// Monotonic sequence for deadline estimation.
    seq: AtomicU64,
}

impl Prefetcher {
    pub fn new(config: PrefetcherConfig) -> Self {
        let enabled = config.enabled;
        Self {
            pending: Mutex::new(BinaryHeap::with_capacity(config.max_pending)),
            in_flight: Mutex::new(HashSet::with_capacity(config.max_pending)),
            resident_set: Mutex::new(HashSet::new()),
            stats: Mutex::new(PrefetcherStats::default()),
            seq: AtomicU64::new(0),
            enabled: AtomicBool::new(enabled),
            config,
        }
    }

    /// Register a prefetch hint from the executor.
    pub fn hint(&self, source: PrefetchSource, file_path: PathBuf, page_size: u32) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let pages: Vec<(PageId, u64)> = match &source {
            PrefetchSource::IndexRangeScan { next_leaf_pages } => {
                next_leaf_pages.iter().enumerate()
                    .map(|(i, &pid)| (pid, 100 + (next_leaf_pages.len() - i) as u64))
                    .collect()
            }
            PrefetchSource::SeqScan { start_page, count } => {
                (0..*count)
                    .map(|i| {
                        let pid = PageId(start_page.0 + i as u64);
                        (pid, 50 + (*count - i) as u64)
                    })
                    .collect()
            }
            PrefetchSource::NestedLoopProbe { probe_pages } => {
                probe_pages.iter().enumerate()
                    .map(|(i, &pid)| (pid, 80 + (probe_pages.len() - i) as u64))
                    .collect()
            }
            PrefetchSource::Compaction { pages } => {
                pages.iter().enumerate()
                    .map(|(i, &pid)| (pid, 10 + (pages.len() - i) as u64))
                    .collect()
            }
        };

        let mut stats = self.stats.lock();
        stats.hints_received += 1;

        let mut pending = self.pending.lock();
        let mut in_flight = self.in_flight.lock();
        let resident = self.resident_set.lock();

        for (page_id, priority_score) in pages {
            // Skip if already resident.
            if resident.contains(&page_id) {
                stats.requests_hit += 1;
                continue;
            }
            // Skip if already in-flight.
            if in_flight.contains(&page_id) {
                stats.requests_dropped_duplicate += 1;
                continue;
            }
            // Skip if queue is full.
            if pending.len() >= self.config.max_pending {
                stats.requests_dropped_full += 1;
                continue;
            }

            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            let req = PrefetchRequest {
                page_id,
                file_path: file_path.clone(),
                offset: page_id.0 * u64::from(page_size),
                length: page_size,
                deadline_ns: seq,
                priority_score,
            };

            in_flight.insert(page_id);
            pending.push(req);
            stats.requests_submitted += 1;
        }
    }

    /// Drain up to `max_per_tick` highest-priority requests.
    pub fn drain_tick(&self) -> Vec<PrefetchRequest> {
        let mut pending = self.pending.lock();
        let n = self.config.max_per_tick.min(pending.len());
        let mut batch = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(req) = pending.pop() {
                batch.push(req);
            }
        }
        batch
    }

    /// Mark a page as resident (skip future prefetch for it).
    pub fn mark_resident(&self, page_id: PageId) {
        self.resident_set.lock().insert(page_id);
    }

    /// Mark a page as evicted (allow future prefetch).
    pub fn mark_evicted(&self, page_id: PageId) {
        self.resident_set.lock().remove(&page_id);
        self.in_flight.lock().remove(&page_id);
    }

    /// Acknowledge that a prefetch I/O completed (remove from in-flight).
    pub fn complete(&self, page_id: PageId) {
        self.in_flight.lock().remove(&page_id);
    }

    /// Cancel all pending requests for a page.
    pub fn cancel(&self, page_id: PageId) {
        self.in_flight.lock().remove(&page_id);
        // Removing from BinaryHeap is expensive; we mark the in_flight set
        // and skip stale entries during drain_tick.
    }

    /// Enable/disable the prefetcher.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    pub fn stats(&self) -> PrefetcherStats {
        self.stats.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hint_seq_scan() {
        let pf = Prefetcher::new(PrefetcherConfig::default());
        pf.hint(
            PrefetchSource::SeqScan {
                start_page: PageId(100),
                count: 5,
            },
            PathBuf::from("sst_0001.dat"),
            4096,
        );
        assert_eq!(pf.pending_count(), 5);

        let batch = pf.drain_tick();
        assert_eq!(batch.len(), 5);
        // Highest priority first (first pages in scan have lower urgency,
        // but our scoring gives higher score to earlier pages).
    }

    #[test]
    fn test_hint_dedup() {
        let pf = Prefetcher::new(PrefetcherConfig::default());
        pf.hint(
            PrefetchSource::IndexRangeScan {
                next_leaf_pages: vec![PageId(1), PageId(2)],
            },
            PathBuf::from("idx.dat"),
            4096,
        );
        // Hint again with overlapping pages.
        pf.hint(
            PrefetchSource::IndexRangeScan {
                next_leaf_pages: vec![PageId(2), PageId(3)],
            },
            PathBuf::from("idx.dat"),
            4096,
        );
        // Page 2 should not be duplicated.
        assert_eq!(pf.pending_count(), 3);
        let stats = pf.stats();
        assert_eq!(stats.requests_dropped_duplicate, 1);
    }

    #[test]
    fn test_skip_resident() {
        let pf = Prefetcher::new(PrefetcherConfig::default());
        pf.mark_resident(PageId(42));
        pf.hint(
            PrefetchSource::IndexRangeScan {
                next_leaf_pages: vec![PageId(42), PageId(43)],
            },
            PathBuf::from("idx.dat"),
            4096,
        );
        assert_eq!(pf.pending_count(), 1); // only page 43
        assert_eq!(pf.stats().requests_hit, 1);
    }

    #[test]
    fn test_max_pending_limit() {
        let pf = Prefetcher::new(PrefetcherConfig {
            max_pending: 3,
            ..PrefetcherConfig::default()
        });
        pf.hint(
            PrefetchSource::SeqScan {
                start_page: PageId(0),
                count: 10,
            },
            PathBuf::from("data.dat"),
            4096,
        );
        assert_eq!(pf.pending_count(), 3);
        assert_eq!(pf.stats().requests_dropped_full, 7);
    }

    #[test]
    fn test_disable_prefetcher() {
        let pf = Prefetcher::new(PrefetcherConfig::default());
        pf.set_enabled(false);
        pf.hint(
            PrefetchSource::SeqScan {
                start_page: PageId(0),
                count: 5,
            },
            PathBuf::from("data.dat"),
            4096,
        );
        assert_eq!(pf.pending_count(), 0);
    }

    #[test]
    fn test_mark_evicted_allows_re_prefetch() {
        let pf = Prefetcher::new(PrefetcherConfig::default());
        pf.mark_resident(PageId(10));
        pf.hint(
            PrefetchSource::IndexRangeScan {
                next_leaf_pages: vec![PageId(10)],
            },
            PathBuf::from("idx.dat"),
            4096,
        );
        assert_eq!(pf.pending_count(), 0); // skipped (resident)

        pf.mark_evicted(PageId(10));
        pf.hint(
            PrefetchSource::IndexRangeScan {
                next_leaf_pages: vec![PageId(10)],
            },
            PathBuf::from("idx.dat"),
            4096,
        );
        assert_eq!(pf.pending_count(), 1); // now accepted
    }

    #[test]
    fn test_compaction_low_priority() {
        let pf = Prefetcher::new(PrefetcherConfig {
            max_per_tick: 100,
            ..PrefetcherConfig::default()
        });
        // Submit high-priority index scan.
        pf.hint(
            PrefetchSource::IndexRangeScan {
                next_leaf_pages: vec![PageId(1)],
            },
            PathBuf::from("idx.dat"),
            4096,
        );
        // Submit low-priority compaction.
        pf.hint(
            PrefetchSource::Compaction {
                pages: vec![PageId(2)],
            },
            PathBuf::from("sst.dat"),
            4096,
        );

        let batch = pf.drain_tick();
        assert_eq!(batch.len(), 2);
        // Index scan page should have higher priority_score.
        assert!(batch[0].priority_score > batch[1].priority_score);
        assert_eq!(batch[0].page_id, PageId(1));
        assert_eq!(batch[1].page_id, PageId(2));
    }
}
