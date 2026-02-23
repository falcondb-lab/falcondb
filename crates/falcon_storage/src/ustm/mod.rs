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

pub mod page;
pub mod lirs2;
pub mod io_scheduler;
pub mod prefetcher;
pub mod zones;
pub mod engine;

// ── Re-exports for convenience ───────────────────────────────────────────────

pub use page::{AccessPriority, PageData, PageHandle, PageId, PinGuard, Tier};
pub use lirs2::{Lirs2Cache, Lirs2Config, Lirs2Stats, AccessResult};
pub use io_scheduler::{IoScheduler, IoSchedulerConfig, IoSchedulerStats, IoPriority, IoRequest, IoResponse, IoError, TokenBucket};
pub use prefetcher::{Prefetcher, PrefetcherConfig, PrefetcherStats, PrefetchSource, PrefetchRequest};
pub use zones::{ZoneManager, ZoneConfig, ZoneStats, ZoneError};
pub use engine::{UstmEngine, UstmConfig, UstmStats, UstmError, FetchResult, PageLocation};
