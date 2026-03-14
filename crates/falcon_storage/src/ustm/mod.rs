//! USTM — User-Space Tiered Memory Engine.
//!
//! A replacement for mmap-based storage access that gives the database full
//! control over memory residency, eviction, and I/O scheduling.
//!
//! ## Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────┐
//! │              UstmEngine (coordinator)               │
//! ├────────────────────────────────────────────────────┤
//! │  ZoneManager          │  IoScheduler  │ Prefetcher │
//! │  ┌──────┬──────┬────┐ │  Query > PF   │ Plan-driven│
//! │  │ Hot  │ Warm │Cold│ │  > Background │ async hints│
//! │  │(DRAM)│(DRAM)│disk│ │               │            │
//! │  └──────┴──────┴────┘ │               │            │
//! │         ↕ LIRS-2      │               │            │
//! └────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key properties
//!
//! * **Deterministic latency** — pinned pages never evicted; prefetch hides I/O.
//! * **Scan-resistant** — LIRS-2 prevents sequential scans from polluting cache.
//! * **Priority I/O** — query reads always beat compaction / prefetch.
//! * **Database-aware eviction** — uses `AccessPriority` (index > hot row > scan).
//!
//! ## Usage
//!
//! ```rust,ignore
//! use falcon_storage::ustm::{UstmEngine, UstmConfig, PageId, PageData, AccessPriority};
//!
//! let engine = UstmEngine::new(UstmConfig::default());
//!
//! // Hot zone: MemTable pages (never evicted)
//! engine.alloc_hot(PageId(1), PageData::new(vec![0u8; 8192]), AccessPriority::IndexInternal)?;
//!
//! // Warm zone: SST page cache (managed by LIRS-2)
//! engine.insert_warm(PageId(100), PageData::new(data), AccessPriority::HotRow);
//!
//! // Unified fetch: Hot → Warm → Cold (disk read)
//! let guard = engine.fetch_pinned(PageId(100), AccessPriority::HotRow)?;
//! let bytes = guard.handle().read().unwrap();
//! // guard dropped → page unpinned, eligible for eviction
//! ```

pub mod engine;
pub mod io_scheduler;
pub mod lirs2;
pub mod page;
pub mod prefetcher;
pub mod zones;

// ── Re-exports for convenience ───────────────────────────────────────────────

pub use engine::{FetchResult, PageLocation, UstmConfig, UstmEngine, UstmError, UstmStats};
pub use io_scheduler::{
    IoError, IoPriority, IoRequest, IoResponse, IoScheduler, IoSchedulerConfig, IoSchedulerStats,
    TokenBucket,
};
pub use lirs2::{AccessResult, Lirs2Cache, Lirs2Config, Lirs2Stats};
pub use page::{AccessPriority, PageData, PageHandle, PageId, PinGuard, Tier};
pub use prefetcher::{
    PrefetchRequest, PrefetchSource, Prefetcher, PrefetcherConfig, PrefetcherStats,
};
pub use zones::{ZoneConfig, ZoneError, ZoneManager, ZoneStats};
