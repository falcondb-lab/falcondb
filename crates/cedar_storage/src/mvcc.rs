use cedar_common::datum::OwnedRow;
use cedar_common::types::{Timestamp, TxnId};
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
    prev: RwLock<Option<Arc<Version>>>,
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
    pub fn get_prev(&self) -> Option<Arc<Version>> {
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
}

impl VersionChain {
    pub fn new() -> Self {
        Self {
            head: RwLock::new(None),
        }
    }

    /// Prepend a new version to the chain.
    pub fn prepend(&self, txn_id: TxnId, data: Option<OwnedRow>) {
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
    pub fn read_committed(&self, read_ts: Timestamp) -> Option<OwnedRow> {
        let head = self.head.read();
        let mut current = head.clone();
        while let Some(ver) = current {
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.data.clone();
            }
            current = ver.get_prev();
        }
        None
    }

    /// Find the visible version for a specific transaction (sees own writes).
    pub fn read_for_txn(&self, txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
        let head = self.head.read();
        let mut current = head.clone();
        while let Some(ver) = current {
            // See own uncommitted writes
            if ver.created_by == txn_id {
                return ver.data.clone();
            }
            // See committed versions
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts <= read_ts {
                return ver.data.clone();
            }
            current = ver.get_prev();
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
    pub fn commit_and_report(&self, txn_id: TxnId, commit_ts: Timestamp) -> (Option<OwnedRow>, Option<OwnedRow>) {
        let head = self.head.read();
        let mut new_data: Option<OwnedRow> = None;
        let mut found = false;
        let mut current = head.clone();
        while let Some(ver) = current {
            if ver.created_by == txn_id && ver.get_commit_ts().0 == 0 {
                ver.set_commit_ts(commit_ts);
                if !found {
                    new_data = ver.data.clone();
                    found = true;
                }
            }
            current = ver.get_prev();
        }
        if !found {
            return (None, None);
        }
        // Find the prior committed version (first committed version that is not this txn's)
        let mut old_data: Option<OwnedRow> = None;
        let mut current = head.clone();
        while let Some(ver) = current {
            let cts = ver.get_commit_ts();
            if cts.0 > 0 && cts != Timestamp::MAX && ver.created_by != txn_id {
                old_data = ver.data.clone();
                break;
            }
            // Skip this txn's own versions (just committed or previously committed)
            if ver.created_by == txn_id {
                current = ver.get_prev();
                continue;
            }
            current = ver.get_prev();
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
        let data_bytes = match &ver.data {
            Some(row) => {
                row.values.iter().map(|d| match d {
                    cedar_common::datum::Datum::Text(s) => s.len() as u64 + 24,
                    cedar_common::datum::Datum::Array(a) => (a.len() * 16) as u64 + 24,
                    _ => 8u64,
                }).sum::<u64>() + 24 // Vec overhead
            }
            None => 0,
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
        let mut current = head.clone();
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
        false
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

/// Estimate the memory footprint of an OwnedRow (without Version header overhead).
/// Used by memory accounting to track write-buffer and MVCC allocations.
pub fn estimate_row_bytes(row: &OwnedRow) -> u64 {
    let version_header = mem::size_of::<Version>() as u64;
    let data_bytes: u64 = row.values.iter().map(|d| match d {
        cedar_common::datum::Datum::Text(s) => s.len() as u64 + 24,
        cedar_common::datum::Datum::Array(a) => (a.len() * 16) as u64 + 24,
        _ => 8u64,
    }).sum::<u64>() + 24; // Vec overhead
    version_header + data_bytes
}
