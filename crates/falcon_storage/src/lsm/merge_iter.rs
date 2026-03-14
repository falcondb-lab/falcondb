use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A tagged entry from one source in the merge.
/// Higher `seq` = newer data (wins on duplicate key).
struct HeapEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>, // None = tombstone
    seq: u64,
    /// Index into the `cursors` array (identifies which source produced this).
    source: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq
    }
}
impl Eq for HeapEntry {}

// Min-heap by key, then max by seq (newest first for same key).
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse key order for min-heap (BinaryHeap is max-heap).
        other.key.cmp(&self.key).then(self.seq.cmp(&other.seq)) // higher seq wins (natural order in max-heap)
    }
}

/// Cursor over a pre-sorted entry list from a single source.
struct SourceCursor {
    entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pos: usize,
    seq: u64,
}

impl SourceCursor {
    fn peek(&self) -> Option<(&[u8], &Option<Vec<u8>>)> {
        self.entries.get(self.pos).map(|(k, v)| (k.as_slice(), v))
    }

    fn advance(&mut self) {
        self.pos += 1;
    }
}

/// K-way merge iterator that merges sorted sources by key, deduplicating
/// so the highest-seq entry wins for each key. Tombstones are suppressed
/// in the output.
#[derive(Default)]
pub struct MergeIterator {
    cursors: Vec<SourceCursor>,
    heap: BinaryHeap<HeapEntry>,
}

impl MergeIterator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a sorted source. `seq` determines priority (higher = newer).
    pub fn add_source(&mut self, entries: Vec<(Vec<u8>, Option<Vec<u8>>)>, seq: u64) {
        let idx = self.cursors.len();
        let cursor = SourceCursor {
            entries,
            pos: 0,
            seq,
        };
        // Seed the heap with the first entry from this source
        if let Some((k, v)) = cursor.peek() {
            self.heap.push(HeapEntry {
                key: k.to_vec(),
                value: v.clone(),
                seq,
                source: idx,
            });
        }
        self.cursors.push(cursor);
    }

    /// Drain all entries from the merge, returning deduplicated (key, value) pairs
    /// sorted by key. Tombstones are excluded.
    pub fn collect_merged(mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;

        while let Some(entry) = self.heap.pop() {
            let source = entry.source;

            // Advance the source cursor and push next entry from this source
            self.cursors[source].advance();
            if let Some((k, v)) = self.cursors[source].peek() {
                self.heap.push(HeapEntry {
                    key: k.to_vec(),
                    value: v.clone(),
                    seq: self.cursors[source].seq,
                    source,
                });
            }

            // Dedup: skip if same key as previous output (first occurrence has highest seq)
            if last_key.as_ref().is_some_and(|lk| *lk == entry.key) {
                continue;
            }
            last_key = Some(entry.key.clone());

            // Suppress tombstones
            if let Some(value) = entry.value {
                result.push((entry.key, value));
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_basic() {
        let mut mi = MergeIterator::new();
        mi.add_source(
            vec![
                (b"a".to_vec(), Some(b"v1".to_vec())),
                (b"c".to_vec(), Some(b"v3".to_vec())),
            ],
            2, // newer
        );
        mi.add_source(
            vec![
                (b"a".to_vec(), Some(b"old".to_vec())),
                (b"b".to_vec(), Some(b"v2".to_vec())),
            ],
            1, // older
        );
        let result = mi.collect_merged();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (b"a".to_vec(), b"v1".to_vec())); // newer wins
        assert_eq!(result[1], (b"b".to_vec(), b"v2".to_vec()));
        assert_eq!(result[2], (b"c".to_vec(), b"v3".to_vec()));
    }

    #[test]
    fn test_merge_tombstone_suppressed() {
        let mut mi = MergeIterator::new();
        mi.add_source(vec![(b"a".to_vec(), None)], 2); // tombstone (newer)
        mi.add_source(vec![(b"a".to_vec(), Some(b"old".to_vec()))], 1);
        let result = mi.collect_merged();
        assert!(result.is_empty(), "tombstone should suppress entry");
    }

    #[test]
    fn test_merge_empty_sources() {
        let mut mi = MergeIterator::new();
        mi.add_source(vec![], 1);
        mi.add_source(vec![], 2);
        let result = mi.collect_merged();
        assert!(result.is_empty());
    }

    #[test]
    fn test_merge_single_source() {
        let mut mi = MergeIterator::new();
        mi.add_source(
            vec![
                (b"x".to_vec(), Some(b"1".to_vec())),
                (b"y".to_vec(), Some(b"2".to_vec())),
            ],
            1,
        );
        let result = mi.collect_merged();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_merge_many_sources() {
        let mut mi = MergeIterator::new();
        for seq in 0..5 {
            mi.add_source(
                vec![(
                    format!("k{:02}", seq).into_bytes(),
                    Some(format!("v{}", seq).into_bytes()),
                )],
                seq as u64,
            );
        }
        // Also add a shared key overwritten by highest seq
        let mut mi2 = MergeIterator::new();
        mi2.add_source(vec![(b"shared".to_vec(), Some(b"old".to_vec()))], 1);
        mi2.add_source(vec![(b"shared".to_vec(), Some(b"new".to_vec()))], 10);
        let result = mi2.collect_merged();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, b"new");
    }
}
