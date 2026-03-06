use std::sync::atomic::{AtomicU64, Ordering};

use super::mvcc_encoding::{MvccStatus, MvccValue};
use super::sst::SstEntry;

/// MVCC garbage collection policy, applied during compaction.
/// Drops aborted versions, old committed versions below safe_gc_ts,
/// and tombstones that have been superseded.
pub struct GcPolicy {
    safe_gc_ts: AtomicU64,
    versions_dropped: AtomicU64,
    tombstones_dropped: AtomicU64,
    bytes_reclaimed: AtomicU64,
}

impl GcPolicy {
    pub fn new(safe_gc_ts: u64) -> Self {
        Self {
            safe_gc_ts: AtomicU64::new(safe_gc_ts),
            versions_dropped: AtomicU64::new(0),
            tombstones_dropped: AtomicU64::new(0),
            bytes_reclaimed: AtomicU64::new(0),
        }
    }

    pub fn set_safe_gc_ts(&self, ts: u64) {
        self.safe_gc_ts.store(ts, Ordering::Relaxed);
    }

    pub fn safe_gc_ts(&self) -> u64 {
        self.safe_gc_ts.load(Ordering::Relaxed)
    }

    /// Filter a sorted, deduplicated entry stream during compaction.
    /// For MVCC-encoded values: drop aborted, drop old committed versions
    /// below safe_gc_ts when a newer version exists, drop old tombstones.
    ///
    /// For non-MVCC values (raw KV), entries pass through unchanged.
    ///
    /// `is_bottom_level` = true means no older data exists below,
    /// so tombstones below safe_gc_ts can be dropped entirely.
    pub fn filter(&self, entries: Vec<SstEntry>, is_bottom_level: bool) -> Vec<SstEntry> {
        let gc_ts = self.safe_gc_ts.load(Ordering::Relaxed);
        if gc_ts == 0 {
            return entries; // GC disabled
        }

        let mut result = Vec::with_capacity(entries.len());

        for entry in entries {
            match MvccValue::decode(&entry.value) {
                Some(mv) => {
                    match mv.status {
                        MvccStatus::Aborted => {
                            self.versions_dropped.fetch_add(1, Ordering::Relaxed);
                            self.bytes_reclaimed
                                .fetch_add(entry.key.len() as u64 + entry.value.len() as u64, Ordering::Relaxed);
                            continue;
                        }
                        MvccStatus::Prepared => {
                            // Keep prepared — resolver must handle
                            result.push(entry);
                        }
                        MvccStatus::Committed => {
                            if mv.is_tombstone && mv.commit_ts.0 < gc_ts && is_bottom_level {
                                self.tombstones_dropped.fetch_add(1, Ordering::Relaxed);
                                self.bytes_reclaimed
                                    .fetch_add(entry.key.len() as u64 + entry.value.len() as u64, Ordering::Relaxed);
                                continue;
                            }
                            result.push(entry);
                        }
                    }
                }
                None => {
                    // Non-MVCC value — pass through (raw KV mode)
                    // Drop empty-value tombstones at bottom level
                    if entry.value.is_empty() && is_bottom_level {
                        self.tombstones_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    result.push(entry);
                }
            }
        }

        result
    }

    /// Filter with multi-version awareness: for the same key, keep only
    /// the latest committed version if all older versions are below gc_ts.
    pub fn filter_multiversion(
        &self,
        entries: Vec<(Vec<u8>, Vec<SstEntry>)>,
        is_bottom_level: bool,
    ) -> Vec<SstEntry> {
        let gc_ts = self.safe_gc_ts.load(Ordering::Relaxed);
        let mut result = Vec::new();

        for (_key, versions) in entries {
            if versions.is_empty() {
                continue;
            }
            if gc_ts == 0 || versions.len() == 1 {
                result.extend(versions);
                continue;
            }

            // Keep the latest committed version always; GC older ones below gc_ts.
            // Prepared versions are always kept (resolver must handle them).
            // Aborted versions are always dropped.
            let mut kept_latest = false;
            for v in versions {
                if let Some(mv) = MvccValue::decode(&v.value) {
                    if mv.status == MvccStatus::Aborted {
                        self.versions_dropped.fetch_add(1, Ordering::Relaxed);
                        self.bytes_reclaimed.fetch_add(
                            v.key.len() as u64 + v.value.len() as u64, Ordering::Relaxed);
                        continue;
                    }
                    if mv.status == MvccStatus::Prepared {
                        result.push(v);
                        continue;
                    }
                    // Committed
                    if !kept_latest {
                        // Drop tombstones at bottom level (no older data can exist below)
                        if mv.is_tombstone && mv.commit_ts.0 < gc_ts && is_bottom_level {
                            self.tombstones_dropped.fetch_add(1, Ordering::Relaxed);
                            self.bytes_reclaimed.fetch_add(
                                v.key.len() as u64 + v.value.len() as u64, Ordering::Relaxed);
                        } else {
                            result.push(v);
                            kept_latest = true;
                        }
                    } else if mv.commit_ts.0 < gc_ts {
                        self.versions_dropped.fetch_add(1, Ordering::Relaxed);
                        self.bytes_reclaimed.fetch_add(
                            v.key.len() as u64 + v.value.len() as u64, Ordering::Relaxed);
                    } else {
                        result.push(v);
                    }
                } else {
                    if !kept_latest {
                        result.push(v);
                        kept_latest = true;
                    }
                }
            }
        }

        result
    }

    pub fn stats(&self) -> GcStats {
        GcStats {
            versions_dropped: self.versions_dropped.load(Ordering::Relaxed),
            tombstones_dropped: self.tombstones_dropped.load(Ordering::Relaxed),
            bytes_reclaimed: self.bytes_reclaimed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GcStats {
    pub versions_dropped: u64,
    pub tombstones_dropped: u64,
    pub bytes_reclaimed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::types::{Timestamp, TxnId};

    fn mvcc_entry(key: &[u8], txn_id: u64, status: MvccStatus, commit_ts: u64, data: &[u8]) -> SstEntry {
        let mv = MvccValue {
            txn_id: TxnId(txn_id),
            status,
            commit_ts: Timestamp(commit_ts),
            is_tombstone: false,
            data: data.to_vec(),
        };
        SstEntry { key: key.to_vec(), value: mv.encode() }
    }

    fn tombstone_entry(key: &[u8], txn_id: u64, commit_ts: u64) -> SstEntry {
        let mv = MvccValue::committed_tombstone(TxnId(txn_id), Timestamp(commit_ts));
        SstEntry { key: key.to_vec(), value: mv.encode() }
    }

    fn aborted_entry(key: &[u8], txn_id: u64) -> SstEntry {
        let mv = MvccValue::aborted(TxnId(txn_id));
        SstEntry { key: key.to_vec(), value: mv.encode() }
    }

    #[test]
    fn test_gc_drops_aborted() {
        let gc = GcPolicy::new(100);
        let entries = vec![
            mvcc_entry(b"k1", 1, MvccStatus::Committed, 50, b"v1"),
            aborted_entry(b"k2", 2),
        ];
        let filtered = gc.filter(entries, false);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].key, b"k1");
    }

    #[test]
    fn test_gc_drops_old_tombstone_at_bottom() {
        let gc = GcPolicy::new(100);
        let entries = vec![tombstone_entry(b"k1", 1, 50)];
        // Not bottom level: keep
        let filtered = gc.filter(entries.clone(), false);
        assert_eq!(filtered.len(), 1);
        // Bottom level: drop
        let filtered = gc.filter(entries, true);
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_gc_keeps_recent_tombstone() {
        let gc = GcPolicy::new(100);
        let entries = vec![tombstone_entry(b"k1", 1, 150)]; // commit_ts > gc_ts
        let filtered = gc.filter(entries, true);
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn test_gc_disabled_when_zero() {
        let gc = GcPolicy::new(0);
        let entries = vec![aborted_entry(b"k1", 1)];
        let filtered = gc.filter(entries, true);
        assert_eq!(filtered.len(), 1); // nothing dropped
    }

    #[test]
    fn test_gc_raw_kv_passthrough() {
        let gc = GcPolicy::new(100);
        let entries = vec![
            SstEntry { key: b"k1".to_vec(), value: b"raw_value".to_vec() },
        ];
        let filtered = gc.filter(entries, false);
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn test_gc_raw_tombstone_at_bottom() {
        let gc = GcPolicy::new(100);
        let entries = vec![
            SstEntry { key: b"k1".to_vec(), value: vec![] }, // empty = tombstone
        ];
        let filtered = gc.filter(entries, true);
        assert_eq!(filtered.len(), 0);
    }
}
