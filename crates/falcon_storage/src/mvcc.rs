//! # Module Status: PRODUCTION
//! Multi-Version Concurrency Control (MVCC) — version chain management for OCC.
//! Core production path: every row mutation creates a new Version in the chain.

use falcon_common::datum::OwnedRow;
use falcon_common::types::{Timestamp, TxnId};
use parking_lot::RwLock;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::cold_store::{ColdHandle, ColdStore};

/// Row payload storage: hot (in-memory), cold (compressed in ColdStore), or tombstone.
#[derive(Debug, Clone)]
pub enum VersionData {
    /// Row lives in hot memory (normal path).
    Hot(Arc<OwnedRow>),
    /// Row payload migrated to cold store; only handle remains in memory.
    Cold(ColdHandle),
    /// Deleted row (tombstone).
    Tombstone,
}

impl VersionData {
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Self::Tombstone)
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        !self.is_tombstone()
    }

    /// Get hot data reference. Returns None for Cold or Tombstone.
    #[inline]
    pub fn as_hot(&self) -> Option<&Arc<OwnedRow>> {
        match self {
            Self::Hot(row) => Some(row),
            _ => None,
        }
    }

    /// Clone the hot Arc, or None.
    #[inline]
    pub fn clone_hot(&self) -> Option<Arc<OwnedRow>> {
        match self {
            Self::Hot(row) => Some(Arc::clone(row)),
            _ => None,
        }
    }

    /// Resolve to OwnedRow: hot returns directly, cold reads from store.
    pub fn resolve(&self, cold_store: &ColdStore) -> Option<Arc<OwnedRow>> {
        match self {
            Self::Hot(row) => Some(Arc::clone(row)),
            Self::Cold(handle) => cold_store.read_row(handle).ok().map(Arc::new),
            Self::Tombstone => None,
        }
    }
}

/// A single version in the MVCC version chain.
pub struct Version {
    /// Transaction that created this version.
    pub created_by: TxnId,
    /// Commit timestamp (set atomically when txn commits; 0 = uncommitted).
    commit_ts: AtomicU64,
    /// Row payload: Hot (in-memory), Cold (compressed handle), or Tombstone.
    /// RwLock allows cold migration to swap Hot→Cold without rebuilding the chain.
    pub data: RwLock<VersionData>,
    /// Lock-free tombstone flag — set at creation, never changes.
    /// Avoids data.read() on commit hot path.
    is_tombstone: bool,
    /// Fast check: true when prev is Some. Avoids RwLock read in commit_no_report.
    has_prev: AtomicBool,
    /// Link to the previous (older) version (RwLock for safe GC truncation).
    prev: RwLock<Option<Arc<Self>>>,
}

impl std::fmt::Debug for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Version")
            .field("created_by", &self.created_by)
            .field("commit_ts", &self.get_commit_ts())
            .field("data", &*self.data.read())
            .finish()
    }
}

impl Version {
    /// Get the commit timestamp (Acquire ordering for visibility).
    #[inline]
    pub fn get_commit_ts(&self) -> Timestamp {
        Timestamp(self.commit_ts.load(Ordering::Acquire))
    }

    /// Set the commit timestamp (Release ordering for visibility).
    /// Only valid transition: 0 → real_ts (commit) or 0 → MAX (abort).
    #[inline]
    pub fn set_commit_ts(&self, ts: Timestamp) {
        self.commit_ts.store(ts.0, Ordering::Release);
    }

    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.is_tombstone
    }

    #[inline]
    pub fn is_live(&self) -> bool {
        !self.is_tombstone
    }

    /// Fast check: true when this version has a predecessor.
    #[inline]
    pub fn has_prev(&self) -> bool {
        self.has_prev.load(Ordering::Acquire)
    }

    /// Get a clone of the prev pointer.
    #[inline]
    pub fn get_prev(&self) -> Option<Arc<Self>> {
        self.prev.read().clone()
    }

    /// Truncate the chain at this version (drop all older versions).
    /// Used by GC.
    #[inline]
    pub fn truncate_prev(&self) {
        *self.prev.write() = None;
        self.has_prev.store(false, Ordering::Release);
    }
}

/// Result of garbage collecting a single version chain.
#[derive(Debug, Clone, Default)]
pub struct GcChainResult {
    pub reclaimed_versions: u64,
    pub reclaimed_bytes: u64,
}

/// Result of migrating old version payloads to the cold store.
#[derive(Debug, Clone, Default)]
pub struct ColdMigrateResult {
    pub inspected: u64,
    pub migrated: u64,
    pub hot_bytes_freed: u64,
    pub cold_bytes_written: u64,
    pub errors: u64,
}

/// A version chain for a single key. Head is the newest version.
#[derive(Debug)]
pub struct VersionChain {
    pub head: RwLock<Option<Arc<Version>>>,
    /// Fast-path cache: commit_ts of the head version when the chain has
    /// exactly one committed non-tombstone version. 0 = fast path unavailable.
    /// Allows `is_visible()` to return `true` without acquiring the RwLock
    /// or chasing the Arc<Version> pointer (eliminates 2 cache misses per row).
    fast_commit_ts: AtomicU64,
}

impl VersionChain {
    pub const fn new() -> Self {
        Self {
            head: RwLock::new(None),
            fast_commit_ts: AtomicU64::new(0),
        }
    }

    /// Prepend a new version to the chain.
    pub fn prepend(&self, txn_id: TxnId, data: Option<OwnedRow>) {
        self.prepend_arc(txn_id, data.map(Arc::new));
    }

    /// Prepend with pre-wrapped Arc data (avoids double-wrapping).
    pub fn prepend_arc(&self, txn_id: TxnId, data: Option<Arc<OwnedRow>>) {
        let tombstone = data.is_none();
        let vdata = match data {
            Some(row) => VersionData::Hot(row),
            None => VersionData::Tombstone,
        };
        self.fast_commit_ts.store(0, Ordering::Release);
        let mut head = self.head.write();
        let prev = head.take();
        let has = prev.is_some();
        let version = Arc::new(Version {
            created_by: txn_id,
            commit_ts: AtomicU64::new(0),
            data: RwLock::new(vdata),
            is_tombstone: tombstone,
            has_prev: AtomicBool::new(has),
            prev: RwLock::new(prev),
        });
        *head = Some(version);
    }

    /// Find the visible version for a given read timestamp under Read Committed.
    /// Returns the latest committed version with commit_ts <= read_ts.
    #[inline]
    pub fn read_committed(&self, read_ts: Timestamp) -> Option<Arc<OwnedRow>> {
        // Ultra-fast path: single committed non-tombstone version.
        let fcts = self.fast_commit_ts.load(Ordering::Acquire);
        if fcts > 0 && Timestamp(fcts) <= read_ts {
            let head = self.head.read();
            if let Some(ref ver) = *head {
                return ver.data.read().clone_hot();
            }
            return None;
        }
        let head = self.head.read();
        if let Some(ref ver) = *head {
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.data.read().clone_hot();
            }
            let mut current = ver.get_prev();
            while let Some(ver) = current {
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts <= read_ts {
                    return ver.data.read().clone_hot();
                }
                current = ver.get_prev();
            }
        }
        None
    }

    /// Find the visible version for a specific transaction (sees own writes).
    #[inline]
    pub fn read_for_txn(&self, txn_id: TxnId, read_ts: Timestamp) -> Option<Arc<OwnedRow>> {
        let fcts = self.fast_commit_ts.load(Ordering::Acquire);
        if fcts > 0 && Timestamp(fcts) <= read_ts {
            let head = self.head.read();
            if let Some(ref ver) = *head {
                return ver.data.read().clone_hot();
            }
            return None;
        }
        let head = self.head.read();
        if let Some(ref ver) = *head {
            if ver.created_by == txn_id {
                return ver.data.read().clone_hot();
            }
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.data.read().clone_hot();
            }
            let mut current = ver.get_prev();
            while let Some(ver) = current {
                if ver.created_by == txn_id {
                    return ver.data.read().clone_hot();
                }
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts <= read_ts {
                    return ver.data.read().clone_hot();
                }
                current = ver.get_prev();
            }
        }
        None
    }

    /// Mark all versions created by txn_id as committed with the given timestamp.
    pub fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) {
        self.commit_and_report(txn_id, commit_ts);
    }

    /// Commit without cloning row data.
    /// Returns row-count delta: +1 = insert, -1 = delete, 0 = update or no-op.
    pub fn commit_no_report(&self, txn_id: TxnId, commit_ts: Timestamp) -> i8 {
        let head = self.head.read();
        if let Some(ref ver) = *head {
            if ver.created_by == txn_id && ver.get_commit_ts().0 == 0 {
                ver.set_commit_ts(commit_ts);
                let is_live = ver.is_live();
                if is_live && !ver.has_prev() {
                    // Fresh INSERT — no prev, no need to check anything else
                    self.fast_commit_ts.store(commit_ts.0, Ordering::Release);
                    return 1;
                }
                let has_prev_committed = ver.has_prev() && {
                    ver.get_prev().is_some_and(|p| {
                        let cts = p.get_commit_ts();
                        cts.0 > 0 && cts != Timestamp::MAX && p.is_live()
                    })
                };
                return match (is_live, has_prev_committed) {
                    (true, false) => 1,   // INSERT (re-insert after delete)
                    (false, true) => -1,  // DELETE
                    _ => 0,              // UPDATE or no previous committed row
                };
            }
        }
        0
    }

    /// Commit without cloning, resolving cold data if needed.
    pub fn commit_no_report_cold(&self, txn_id: TxnId, commit_ts: Timestamp) -> i8 {
        self.commit_no_report(txn_id, commit_ts)
    }

    /// Commit versions for txn_id and return (new_row_data, old_row_data).
    /// - `new_data`: the row data from the newly committed version (None = tombstone/delete).
    /// - `old_data`: the row data from the prior committed version (None = was insert, no prior).
    ///
    /// Used by MemTable to update secondary indexes at commit time (方案A).
    pub fn commit_and_report(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> (Option<Arc<OwnedRow>>, Option<Arc<OwnedRow>>) {
        let head = self.head.read();
        let mut new_data: Option<Arc<OwnedRow>> = None;
        let mut old_data: Option<Arc<OwnedRow>> = None;
        let mut found = false;
        let mut current = head.clone();
        while let Some(ver) = current {
            if ver.created_by == txn_id {
                if ver.get_commit_ts().0 == 0 {
                    ver.set_commit_ts(commit_ts);
                    if !found {
                        new_data = ver.data.read().clone_hot();
                        found = true;
                    }
                }
            } else if old_data.is_none() {
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts != Timestamp::MAX {
                    old_data = ver.data.read().clone_hot();
                }
            }
            current = ver.get_prev();
        }
        if !found {
            return (None, None);
        }
        if new_data.is_some() && old_data.is_none() {
            self.fast_commit_ts.store(commit_ts.0, Ordering::Release);
        }
        (new_data, old_data)
    }

    /// Remove all versions created by txn_id (abort / rollback).
    pub fn abort(&self, txn_id: TxnId) {
        self.abort_and_report(txn_id);
    }

    /// Abort versions for txn_id and return (aborted_row_data, restored_row_data).
    /// - `aborted`: the row data from the head version that was removed (if any).
    /// - `restored`: the row data from the version that is now the new head (if any).
    ///
    /// Used by MemTable to undo/redo secondary index entries on abort.
    pub fn abort_and_report(&self, txn_id: TxnId) -> (Option<Arc<OwnedRow>>, Option<Arc<OwnedRow>>) {
        self.fast_commit_ts.store(0, Ordering::Release);
        let mut head = self.head.write();
        let aborted = match *head {
            Some(ref ver) if ver.created_by == txn_id => ver.data.read().clone_hot(),
            _ => None,
        };
        while let Some(ref ver) = *head {
            if ver.created_by == txn_id {
                *head = ver.get_prev();
            } else {
                break;
            }
        }
        let mut current = head.clone();
        while let Some(ver) = current {
            if ver.created_by == txn_id && ver.get_commit_ts().0 == 0 {
                ver.set_commit_ts(Timestamp::MAX);
            }
            current = ver.get_prev();
        }
        let restored = match *head {
            Some(ref ver) if ver.is_live() => {
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts != Timestamp::MAX && !ver.has_prev() {
                    self.fast_commit_ts.store(cts.0, Ordering::Release);
                }
                ver.data.read().clone_hot()
            }
            _ => None,
        };
        (aborted, restored)
    }

    /// Garbage collect versions older than the given watermark.
    /// Keeps at most one version visible at the watermark.
    /// Returns (reclaimed_versions, estimated_reclaimed_bytes).
    pub fn gc(&self, watermark: Timestamp) -> GcChainResult {
        let head = self.head.write();
        let mut result = GcChainResult::default();
        if let Some(ref ver) = *head {
            Self::gc_chain(ver, watermark, &mut result);
        }
        drop(head);
        result
    }

    fn gc_chain(ver: &Arc<Version>, watermark: Timestamp, result: &mut GcChainResult) {
        // Iterative traversal to avoid stack overflow on long version chains.
        let mut current = Arc::clone(ver);
        loop {
            let cts = current.get_commit_ts();
            // Skip uncommitted versions (cts == 0) — WAL-aware: never GC uncommitted.
            // Skip aborted versions (cts == MAX) — they are invisible but may still
            // be in the chain; they'll be cleaned up when a committed version below
            // them becomes the truncation point.
            if cts.0 > 0 && cts != Timestamp::MAX && cts <= watermark {
                // This version is visible at watermark; drop everything older.
                let mut reclaimed = 0u64;
                let mut reclaimed_bytes = 0u64;
                let mut cur = current.get_prev();
                while let Some(old) = cur {
                    reclaimed += 1;
                    reclaimed_bytes += Self::estimate_version_bytes(&old);
                    cur = old.get_prev();
                }
                current.truncate_prev();
                result.reclaimed_versions += reclaimed;
                result.reclaimed_bytes += reclaimed_bytes;
                return;
            }
            match current.get_prev() {
                Some(prev) => current = prev,
                None => return,
            }
        }
    }

    /// Migrate old committed hot payloads to the cold store.
    ///
    /// Walks the prev chain (skips head — it's the current visible version).
    /// Any committed non-tombstone Hot version older than `age_threshold` gets its
    /// payload stored in `cold_store` and replaced with a `Cold(ColdHandle)`.
    ///
    /// Returns migration stats.
    pub fn migrate_to_cold(
        &self,
        cold_store: &ColdStore,
        age_threshold: Timestamp,
    ) -> ColdMigrateResult {
        let mut result = ColdMigrateResult::default();
        let head = self.head.read();
        let Some(ref head_ver) = *head else { return result };
        // Start from prev — never migrate the head version (it's the live row).
        let mut current = head_ver.get_prev();
        while let Some(ver) = current {
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts != Timestamp::MAX && cts <= age_threshold {
                let is_hot = matches!(*ver.data.read(), VersionData::Hot(_));
                if is_hot {
                    result.inspected += 1;
                    let row_clone = ver.data.read().clone_hot();
                    if let Some(ref row) = row_clone {
                        match cold_store.store_row(row) {
                            Ok(handle) => {
                                let hot_bytes = Self::estimate_version_bytes(&ver);
                                result.migrated += 1;
                                result.hot_bytes_freed += hot_bytes;
                                result.cold_bytes_written += handle.len as u64;
                                *ver.data.write() = VersionData::Cold(handle);
                            }
                            Err(_) => { result.errors += 1; }
                        }
                    }
                }
            }
            current = ver.get_prev();
        }
        result
    }

    /// Estimate the memory footprint of a single Version (header + row data).
    pub fn estimate_version_bytes(ver: &Version) -> u64 {
        let header = mem::size_of::<Version>() as u64;
        let data_bytes = match &*ver.data.read() {
            VersionData::Hot(row) => {
                row.values.iter().map(estimate_datum_bytes).sum::<u64>() + 24
            }
            VersionData::Cold(_) => ColdHandle::SIZE as u64,
            VersionData::Tombstone => 0,
        };
        header + data_bytes
    }

    /// Count the number of versions in this chain (for observability).
    pub fn version_chain_len(&self) -> usize {
        let head = self.head.read();
        let mut count = 0usize;
        let mut current = head.clone();
        while let Some(ver) = current {
            count += 1;
            current = ver.get_prev();
        }
        count
    }

    /// Check if a key has an uncommitted write by another transaction.
    #[inline]
    pub fn has_write_conflict(&self, txn_id: TxnId) -> bool {
        let head = self.head.read();
        if let Some(ref ver) = *head {
            // If the newest version is uncommitted and by a different txn, conflict
            if ver.get_commit_ts().0 == 0 && ver.created_by != txn_id {
                return true;
            }
        }
        false
    }

    /// Read the uncommitted row data written by a specific transaction.
    /// Returns `Some(Some(row))` for insert/update, `Some(None)` for delete/tombstone,
    /// `None` if this txn has no uncommitted write on this chain.
    pub fn read_uncommitted_for_txn(&self, txn_id: TxnId) -> Option<Option<Arc<OwnedRow>>> {
        let head = self.head.read();
        let mut current = head.clone();
        while let Some(ver) = current {
            if ver.created_by == txn_id && ver.get_commit_ts().0 == 0 {
                return Some(ver.data.read().clone_hot());
            }
            current = ver.get_prev();
        }
        None
    }

    /// Read the latest non-tombstone version's data (regardless of visibility).
    /// Used for backfilling secondary indexes.
    pub fn read_latest(&self) -> Option<Arc<OwnedRow>> {
        let head = self.head.read();
        if let Some(ref ver) = *head {
            return ver.data.read().clone_hot();
        }
        None
    }

    /// Replace the data of the latest version in-place.
    /// Used by DDL ALTER COLUMN TYPE to convert existing row data.
    pub fn replace_latest(&self, new_row: OwnedRow) {
        let mut head = self.head.write();
        if let Some(ref ver) = *head {
            let old_prev = ver.get_prev();
            let has = old_prev.is_some();
            let replacement = Arc::new(Version {
                created_by: ver.created_by,
                commit_ts: AtomicU64::new(ver.get_commit_ts().0),
                data: RwLock::new(VersionData::Hot(Arc::new(new_row))),
                is_tombstone: false,
                has_prev: AtomicBool::new(has),
                prev: RwLock::new(old_prev),
            });
            *head = Some(replacement);
        }
    }

    /// Check if any version was committed after `after_ts` by a different transaction.
    /// Used for OCC read-set validation under Snapshot Isolation.
    pub fn has_committed_write_after(&self, exclude_txn: TxnId, after_ts: Timestamp) -> bool {
        let head = self.head.read();
        // Fast path: check head version without Arc clone
        if let Some(ref ver) = *head {
            let cts = ver.get_commit_ts();
            if ver.created_by != exclude_txn && cts.0 > 0 && cts != Timestamp::MAX && cts > after_ts
            {
                return true;
            }
            // Slow path: traverse prev chain
            let mut current = ver.get_prev();
            while let Some(ver) = current {
                let cts = ver.get_commit_ts();
                if ver.created_by != exclude_txn
                    && cts.0 > 0
                    && cts != Timestamp::MAX
                    && cts > after_ts
                {
                    return true;
                }
                current = ver.get_prev();
            }
        }
        false
    }

    /// Check if a non-tombstone version is visible to the given txn/read_ts.
    /// Same logic as read_for_txn but avoids cloning the row data.
    #[inline]
    pub fn is_visible(&self, txn_id: TxnId, read_ts: Timestamp) -> bool {
        // Ultra-fast path: single committed non-tombstone version.
        // Avoids RwLock acquire + Arc<Version> pointer chase (2 fewer cache misses).
        let fcts = self.fast_commit_ts.load(Ordering::Acquire);
        if fcts > 0 && Timestamp(fcts) <= read_ts {
            return true;
        }
        let head = self.head.read();
        if let Some(ref ver) = *head {
            if ver.created_by == txn_id {
                return ver.is_live();
            }
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.is_live();
            }
            let mut current = ver.get_prev();
            while let Some(ver) = current {
                if ver.created_by == txn_id {
                    return ver.is_live();
                }
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts <= read_ts {
                    return ver.is_live();
                }
                current = ver.get_prev();
            }
        }
        false
    }

    /// Call closure with reference to visible row data (avoids clone).
    /// Returns None if no visible version or if it's a tombstone.
    #[inline]
    pub fn with_visible_data<R>(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
        f: impl FnOnce(&OwnedRow) -> R,
    ) -> Option<R> {
        let fcts = self.fast_commit_ts.load(Ordering::Acquire);
        if fcts > 0 && Timestamp(fcts) <= read_ts {
            let head = self.head.read();
            if let Some(ref ver) = *head {
                return ver.data.read().as_hot().map(|row| f(row.as_ref()));
            }
            return None;
        }
        let head = self.head.read();
        if let Some(ref ver) = *head {
            if ver.created_by == txn_id {
                return ver.data.read().as_hot().map(|row| f(row.as_ref()));
            }
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.data.read().as_hot().map(|row| f(row.as_ref()));
            }
            let mut current = ver.get_prev();
            while let Some(ver) = current {
                if ver.created_by == txn_id {
                    return ver.data.read().as_hot().map(|row| f(row.as_ref()));
                }
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts <= read_ts {
                    return ver.data.read().as_hot().map(|row| f(row.as_ref()));
                }
                current = ver.get_prev();
            }
        }
        None
    }

    /// Check if this key has any live version (committed or same-txn uncommitted non-tombstone).
    /// Used by INSERT to detect duplicate primary keys.
    /// Walks past aborted versions (commit_ts == MAX) to find live ones underneath.
    pub fn has_live_version(&self, txn_id: TxnId) -> bool {
        let head = self.head.read();
        let mut current = head.as_ref().map(Arc::clone);
        drop(head);
        while let Some(ver) = current {
            let cts = ver.get_commit_ts();
            if cts.0 == 0 && ver.created_by == txn_id && ver.is_live() {
                return true;
            }
            if cts.0 != 0 && cts != Timestamp::MAX && ver.is_live() {
                return true;
            }
            if cts == Timestamp::MAX {
                current = ver.get_prev();
                continue;
            }
            if ver.is_tombstone() {
                if cts.0 != 0 {
                    return false;
                }
                current = ver.get_prev();
                continue;
            }
            break;
        }
        false
    }
}

impl Default for VersionChain {
    fn default() -> Self {
        Self::new()
    }
}

/// Estimate the heap memory footprint of a single Datum value.
/// Fixed-size scalars (bool, i32, i64, f64, Date, Time, Uuid) cost 8-16 bytes in-enum.
/// Variable-length types (Text, Bytea, Array, Jsonb, Decimal, Interval) add heap allocation overhead.
fn estimate_datum_bytes(d: &falcon_common::datum::Datum) -> u64 {
    use falcon_common::datum::Datum;
    match d {
        Datum::Null => 0,
        Datum::Boolean(_) => 1,
        Datum::Int32(_) => 4,
        Datum::Int64(_) | Datum::Float64(_)
        | Datum::Date(_) | Datum::Time(_) => 8,
        Datum::Timestamp(_) | Datum::Uuid(_) => 16,
        Datum::Interval(_, _, _) | Datum::Decimal(_, _) => 24,
        Datum::Text(s) => s.len() as u64 + 24, // String heap + ptr/len/cap
        Datum::Bytea(b) => b.len() as u64 + 24, // Vec<u8> heap + ptr/len/cap
        Datum::Jsonb(v) => v.to_string().len() as u64 + 24,
        Datum::Array(a) => (a.len() as u64) * 16 + 24, // Vec<Datum> overhead
        Datum::TsVector(v) => (v.len() as u64) * 48 + 24,
        Datum::TsQuery(q) => q.len() as u64 + 24,
    }
}

/// Estimate the memory footprint of an OwnedRow (without Version header overhead).
/// Used by memory accounting to track write-buffer and MVCC allocations.
pub fn estimate_row_bytes(row: &OwnedRow) -> u64 {
    let version_header = mem::size_of::<Version>() as u64;
    let data_bytes: u64 = row.values.iter().map(estimate_datum_bytes).sum::<u64>() + 24; // Vec overhead
    version_header + data_bytes
}
