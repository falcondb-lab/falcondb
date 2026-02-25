//! # Module Status: PRODUCTION
//! Multi-Version Concurrency Control (MVCC) — version chain management for OCC.
//! Core production path: every row mutation creates a new Version in the chain.

use falcon_common::datum::OwnedRow;
use falcon_common::types::{Timestamp, TxnId};
use parking_lot::RwLock;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A single version in the MVCC version chain.
pub struct Version {
    /// Transaction that created this version.
    pub created_by: TxnId,
    /// Commit timestamp (set atomically when txn commits; 0 = uncommitted).
    commit_ts: AtomicU64,
    /// The row data (None = tombstone / deleted).
    pub data: Option<OwnedRow>,
    /// Link to the previous (older) version (RwLock for safe GC truncation).
    prev: RwLock<Option<Arc<Self>>>,
}

impl std::fmt::Debug for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Version")
            .field("created_by", &self.created_by)
            .field("commit_ts", &self.get_commit_ts())
            .field("data", &self.data)
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
    }
}

/// Result of garbage collecting a single version chain.
#[derive(Debug, Clone, Default)]
pub struct GcChainResult {
    pub reclaimed_versions: u64,
    pub reclaimed_bytes: u64,
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
        // Invalidate fast path — chain is now multi-version or has uncommitted head
        self.fast_commit_ts.store(0, Ordering::Release);
        let mut head = self.head.write();
        let prev = head.take();
        let version = Arc::new(Version {
            created_by: txn_id,
            commit_ts: AtomicU64::new(0), // uncommitted
            data,
            prev: RwLock::new(prev),
        });
        *head = Some(version);
    }

    /// Find the visible version for a given read timestamp under Read Committed.
    /// Returns the latest committed version with commit_ts <= read_ts.
    #[inline]
    pub fn read_committed(&self, read_ts: Timestamp) -> Option<OwnedRow> {
        // Ultra-fast path: single committed non-tombstone version.
        let fcts = self.fast_commit_ts.load(Ordering::Acquire);
        if fcts > 0 && Timestamp(fcts) <= read_ts {
            let head = self.head.read();
            return head.as_ref().and_then(|ver| ver.data.clone());
        }
        let head = self.head.read();
        // Fast path: check head version without Arc clone
        if let Some(ref ver) = *head {
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.data.clone();
            }
            // Slow path: traverse prev chain
            let mut current = ver.get_prev();
            while let Some(ver) = current {
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts <= read_ts {
                    return ver.data.clone();
                }
                current = ver.get_prev();
            }
        }
        None
    }

    /// Find the visible version for a specific transaction (sees own writes).
    #[inline]
    pub fn read_for_txn(&self, txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
        let head = self.head.read();
        // Fast path: check head version without Arc clone
        if let Some(ref ver) = *head {
            if ver.created_by == txn_id {
                return ver.data.clone();
            }
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.data.clone();
            }
            // Slow path: traverse prev chain (needs Arc clone)
            let mut current = ver.get_prev();
            while let Some(ver) = current {
                if ver.created_by == txn_id {
                    return ver.data.clone();
                }
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts <= read_ts {
                    return ver.data.clone();
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

    /// Commit versions for txn_id and return (new_row_data, old_row_data).
    /// - `new_data`: the row data from the newly committed version (None = tombstone/delete).
    /// - `old_data`: the row data from the prior committed version (None = was insert, no prior).
    ///
    /// Used by MemTable to update secondary indexes at commit time (方案A).
    pub fn commit_and_report(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> (Option<OwnedRow>, Option<OwnedRow>) {
        let head = self.head.read();
        let mut new_data: Option<OwnedRow> = None;
        let mut old_data: Option<OwnedRow> = None;
        let mut found = false;
        // Single-pass: commit this txn's versions AND find the prior committed version.
        let mut current = head.clone();
        while let Some(ver) = current {
            if ver.created_by == txn_id {
                if ver.get_commit_ts().0 == 0 {
                    ver.set_commit_ts(commit_ts);
                    if !found {
                        new_data = ver.data.clone();
                        found = true;
                    }
                }
            } else if old_data.is_none() {
                // First non-txn version after our versions = the prior committed head
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts != Timestamp::MAX {
                    old_data = ver.data.clone();
                }
            }
            current = ver.get_prev();
        }
        if !found {
            return (None, None);
        }
        // Enable fast_commit_ts when this is a single-version non-tombstone chain
        // (common case: fresh INSERT commit with no prior versions).
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
    pub fn abort_and_report(&self, txn_id: TxnId) -> (Option<OwnedRow>, Option<OwnedRow>) {
        let mut head = self.head.write();
        // Capture the aborted version's data before removing
        let aborted = match *head {
            Some(ref ver) if ver.created_by == txn_id => ver.data.clone(),
            _ => None,
        };
        // Remove from head if it belongs to this txn
        while let Some(ref ver) = *head {
            if ver.created_by == txn_id {
                *head = ver.get_prev();
            } else {
                break;
            }
        }
        // For interior nodes, mark as aborted by setting commit_ts to MAX (never visible).
        let mut current = head.clone();
        while let Some(ver) = current {
            if ver.created_by == txn_id && ver.get_commit_ts().0 == 0 {
                ver.set_commit_ts(Timestamp::MAX);
            }
            current = ver.get_prev();
        }
        // The restored version is whatever is now at the head
        let restored = match *head {
            Some(ref ver) if ver.data.is_some() => ver.data.clone(),
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
        let cts = ver.get_commit_ts();
        // Skip uncommitted versions (cts == 0) — WAL-aware: never GC uncommitted.
        // Skip aborted versions (cts == MAX) — they are invisible but may still
        // be in the chain; they'll be cleaned up when a committed version below
        // them becomes the truncation point.
        if cts.0 > 0 && cts != Timestamp::MAX && cts <= watermark {
            // This version is visible at watermark; drop everything older.
            let mut reclaimed = 0u64;
            let mut reclaimed_bytes = 0u64;
            let mut cur = ver.get_prev();
            while let Some(old) = cur {
                reclaimed += 1;
                reclaimed_bytes += Self::estimate_version_bytes(&old);
                cur = old.get_prev();
            }
            ver.truncate_prev();
            result.reclaimed_versions += reclaimed;
            result.reclaimed_bytes += reclaimed_bytes;
            return;
        }
        if let Some(ref prev) = ver.get_prev() {
            Self::gc_chain(prev, watermark, result);
        }
    }

    /// Estimate the memory footprint of a single Version (header + row data).
    pub fn estimate_version_bytes(ver: &Version) -> u64 {
        let header = mem::size_of::<Version>() as u64;
        let data_bytes = ver.data.as_ref().map_or(0, |row| {
            row.values.iter().map(estimate_datum_bytes).sum::<u64>() + 24 // Vec overhead
        });
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
    pub fn read_uncommitted_for_txn(&self, txn_id: TxnId) -> Option<Option<OwnedRow>> {
        let head = self.head.read();
        let mut current = head.clone();
        while let Some(ver) = current {
            if ver.created_by == txn_id && ver.get_commit_ts().0 == 0 {
                return Some(ver.data.clone());
            }
            current = ver.get_prev();
        }
        None
    }

    /// Read the latest non-tombstone version's data (regardless of visibility).
    /// Used for backfilling secondary indexes.
    pub fn read_latest(&self) -> Option<OwnedRow> {
        let head = self.head.read();
        if let Some(ref ver) = *head {
            return ver.data.clone();
        }
        None
    }

    /// Replace the data of the latest version in-place.
    /// Used by DDL ALTER COLUMN TYPE to convert existing row data.
    pub fn replace_latest(&self, new_row: OwnedRow) {
        let head = self.head.read();
        if let Some(ref ver) = *head {
            // Safety: we construct a new Arc<Version> with updated data, same metadata
            let old_prev = ver.get_prev();
            let replacement = Arc::new(Version {
                created_by: ver.created_by,
                commit_ts: AtomicU64::new(ver.get_commit_ts().0),
                data: Some(new_row),
                prev: RwLock::new(old_prev),
            });
            drop(head);
            let mut head_w = self.head.write();
            *head_w = Some(replacement);
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
        // Fast path: check head version without Arc clone
        if let Some(ref ver) = *head {
            if ver.created_by == txn_id {
                return ver.data.is_some();
            }
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.data.is_some();
            }
            // Slow path: traverse prev chain
            let mut current = ver.get_prev();
            while let Some(ver) = current {
                if ver.created_by == txn_id {
                    return ver.data.is_some();
                }
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts <= read_ts {
                    return ver.data.is_some();
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
        // Ultra-fast path: single committed non-tombstone version (most common after INSERT).
        // fast_commit_ts is set by commit_and_report() only when chain has exactly 1 version.
        // If set and visible to read_ts, we can still need the head lock to get row data,
        // but we skip the version chain traversal entirely.
        let fcts = self.fast_commit_ts.load(Ordering::Acquire);
        if fcts > 0 && Timestamp(fcts) <= read_ts {
            let head = self.head.read();
            if let Some(ref ver) = *head {
                return ver.data.as_ref().map(f);
            }
            return None;
        }
        let head = self.head.read();
        // Fast path: check head version without Arc clone
        if let Some(ref ver) = *head {
            if ver.created_by == txn_id {
                return ver.data.as_ref().map(f);
            }
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.data.as_ref().map(f);
            }
            // Slow path: traverse prev chain
            let mut current = ver.get_prev();
            while let Some(ver) = current {
                if ver.created_by == txn_id {
                    return ver.data.as_ref().map(f);
                }
                let cts = ver.get_commit_ts();
                if cts.0 > 0 && cts <= read_ts {
                    return ver.data.as_ref().map(f);
                }
                current = ver.get_prev();
            }
        }
        None
    }

    /// Check if this key has any live version (committed or same-txn uncommitted non-tombstone).
    /// Used by INSERT to detect duplicate primary keys.
    pub fn has_live_version(&self, txn_id: TxnId) -> bool {
        let head = self.head.read();
        if let Some(ref ver) = *head {
            let cts = ver.get_commit_ts();
            // Same txn uncommitted write that is not a tombstone
            if cts.0 == 0 && ver.created_by == txn_id && ver.data.is_some() {
                return true;
            }
            // Committed version that is not a tombstone
            if cts.0 != 0 && ver.data.is_some() {
                return true;
            }
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
    }
}

/// Estimate the memory footprint of an OwnedRow (without Version header overhead).
/// Used by memory accounting to track write-buffer and MVCC allocations.
pub fn estimate_row_bytes(row: &OwnedRow) -> u64 {
    let version_header = mem::size_of::<Version>() as u64;
    let data_bytes: u64 = row.values.iter().map(estimate_datum_bytes).sum::<u64>() + 24; // Vec overhead
    version_header + data_bytes
}
