//! Deadlock detection via wait-for graph (WFG).
//!
//! Each active transaction that is blocked waiting on a lock held by another
//! transaction creates a directed edge in the WFG: `waiter → holder`.
//! A cycle in the WFG means deadlock. We detect cycles via iterative DFS
//! and abort the youngest transaction in the cycle (smallest commit impact).

use dashmap::DashMap;
use falcon_common::types::TxnId;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A single edge in the wait-for graph: `waiter` is blocked by `holder`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitEdge {
    pub waiter: TxnId,
    pub holder: TxnId,
}

/// Wait-for graph for deadlock detection.
///
/// Uses DashMap for the adjacency list so that add_wait / remove_wait / remove_txn
/// only lock individual entry shards instead of a single global Mutex.
/// detect_cycle snapshots the full graph once (O(V+E) copy) before running DFS.
pub struct WaitForGraph {
    /// Adjacency list: waiter → set of holders it's waiting on.
    /// DashMap<TxnId, Mutex<HashSet<TxnId>>> — per-entry lock for holder set mutations.
    edges: DashMap<TxnId, Mutex<HashSet<TxnId>>>,
}

impl Default for WaitForGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitForGraph {
    pub fn new() -> Self {
        Self {
            edges: DashMap::new(),
        }
    }

    /// Record that `waiter` is blocked waiting for `holder`.
    pub fn add_wait(&self, waiter: TxnId, holder: TxnId) {
        self.edges
            .entry(waiter)
            .or_insert_with(|| Mutex::new(HashSet::new()))
            .lock()
            .insert(holder);
    }

    /// Remove all wait edges for a transaction (e.g., when it commits/aborts
    /// or acquires the lock it was waiting for).
    pub fn remove_txn(&self, txn_id: TxnId) {
        // Remove the waiter entry.
        self.edges.remove(&txn_id);
        // Remove txn_id from all holder sets (iterate shard-by-shard).
        self.edges.iter().for_each(|entry| {
            entry.value().lock().remove(&txn_id);
        });
        // Clean up entries whose holder sets became empty.
        self.edges.retain(|_, v| !v.lock().is_empty());
    }

    /// Remove a specific wait edge.
    pub fn remove_wait(&self, waiter: TxnId, holder: TxnId) {
        if let Some(entry) = self.edges.get(&waiter) {
            let mut holders = entry.value().lock();
            holders.remove(&holder);
            let empty = holders.is_empty();
            drop(holders);
            if empty {
                drop(entry);
                self.edges.remove(&waiter);
            }
        }
    }

    /// Detect deadlock cycles. Returns the first cycle found (if any)
    /// as a list of TxnIds forming the cycle.
    /// Takes a consistent snapshot of the graph before running DFS.
    pub fn detect_cycle(&self) -> Option<Vec<TxnId>> {
        // Snapshot: collect adjacency list under per-entry locks, one shard at a time.
        let snapshot: HashMap<TxnId, HashSet<TxnId>> = self
            .edges
            .iter()
            .map(|entry| (*entry.key(), entry.value().lock().clone()))
            .collect();

        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();
        let mut path = Vec::new();

        for &start in snapshot.keys() {
            if visited.contains(&start) {
                continue;
            }
            if let Some(cycle) =
                Self::dfs(start, &snapshot, &mut visited, &mut in_stack, &mut path)
            {
                return Some(cycle);
            }
        }
        None
    }

    /// DFS from `node`, tracking the current recursion stack.
    fn dfs(
        node: TxnId,
        edges: &HashMap<TxnId, HashSet<TxnId>>,
        visited: &mut HashSet<TxnId>,
        in_stack: &mut HashSet<TxnId>,
        path: &mut Vec<TxnId>,
    ) -> Option<Vec<TxnId>> {
        visited.insert(node);
        in_stack.insert(node);
        path.push(node);

        if let Some(holders) = edges.get(&node) {
            for &holder in holders {
                if !visited.contains(&holder) {
                    if let Some(cycle) = Self::dfs(holder, edges, visited, in_stack, path) {
                        return Some(cycle);
                    }
                } else if in_stack.contains(&holder) {
                    // Found a cycle — extract it from path
                    let cycle_start = if let Some(pos) = path.iter().position(|&t| t == holder) {
                        pos
                    } else {
                        tracing::error!("BUG: holder {:?} in in_stack but not in path", holder);
                        return None;
                    };
                    return Some(path[cycle_start..].to_vec());
                }
            }
        }

        path.pop();
        in_stack.remove(&node);
        None
    }

    /// Choose the victim transaction to abort from a deadlock cycle.
    /// Strategy: abort the transaction with the highest TxnId (youngest).
    pub fn choose_victim(cycle: &[TxnId]) -> TxnId {
        debug_assert!(!cycle.is_empty(), "choose_victim called with empty cycle");
        cycle
            .iter()
            .max_by_key(|t| t.0)
            .copied()
            .unwrap_or(TxnId(0))
    }

    /// Number of edges in the graph (for diagnostics).
    pub fn edge_count(&self) -> usize {
        self.edges
            .iter()
            .map(|entry| entry.value().lock().len())
            .sum()
    }
}

/// SSI predicate lock manager.
///
/// Under Serializable Snapshot Isolation (SSI), we track "predicate locks"
/// which are approximated as range locks on (table_id, column_range) pairs.
/// When a transaction reads a range, we record a predicate lock. When another
/// transaction writes to a range covered by a predicate lock, we detect an
/// rw-antidependency. Two rw-antidependencies forming a "dangerous structure"
/// (T1→rw→T2→rw→T3 where T1 committed before T3 reads) trigger abort.
///
/// # P1-2: Sharded DashMap design
/// The original implementation used two `Mutex<HashMap>` which became a global
/// bottleneck under concurrent Serializable workloads.  We now use two
/// `DashMap`s (256 shards each by default) so that transactions with different
/// TxnId hash buckets never contend at all.
pub struct SsiLockManager {
    /// Read predicates: txn_id → list of (table_id, key_range).
    /// P1-2: DashMap replaces Mutex<HashMap> — each shard is independently locked.
    predicates: DashMap<TxnId, Vec<SsiPredicate>>,
    /// Write sets: txn_id → list of (table_id, key).
    /// P1-2: DashMap replaces Mutex<HashMap>.
    write_intents: DashMap<TxnId, Vec<SsiWriteIntent>>,
}

/// A predicate lock representing a range scan.
#[derive(Debug, Clone)]
pub struct SsiPredicate {
    pub table_id: u64,
    /// Lower bound of the scanned range (None = unbounded).
    pub range_start: Option<Vec<u8>>,
    /// Upper bound of the scanned range (None = unbounded).
    pub range_end: Option<Vec<u8>>,
}

/// A write intent on a specific key.
#[derive(Debug, Clone)]
pub struct SsiWriteIntent {
    pub table_id: u64,
    pub key: Vec<u8>,
}

impl Default for SsiLockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SsiLockManager {
    pub fn new() -> Self {
        Self {
            predicates: DashMap::new(),
            write_intents: DashMap::new(),
        }
    }

    /// Record a read predicate for a transaction.
    pub fn add_predicate(&self, txn_id: TxnId, predicate: SsiPredicate) {
        self.predicates
            .entry(txn_id)
            .or_default()
            .push(predicate);
    }

    /// Record a write intent for a transaction.
    pub fn add_write_intent(&self, txn_id: TxnId, intent: SsiWriteIntent) {
        self.write_intents
            .entry(txn_id)
            .or_default()
            .push(intent);
    }

    /// Check if a transaction's writes conflict with any other transaction's
    /// read predicates (rw-antidependency).
    ///
    /// Returns the list of transactions whose predicates are violated.
    ///
    /// # P1-2 design
    /// We clone only the writing txn's intents (one DashMap shard lock), then
    /// iterate predicates (each shard lock taken/released independently).
    /// No two-lock ordering is needed since we never hold both maps' same shard
    /// simultaneously.
    pub fn check_rw_conflicts(&self, txn_id: TxnId) -> Vec<TxnId> {
        let my_writes = match self.write_intents.get(&txn_id) {
            Some(entry) => entry.clone(),
            None => return vec![],
        };

        let mut conflicting = Vec::new();
        for entry in self.predicates.iter() {
            let other_txn = *entry.key();
            if other_txn == txn_id {
                continue;
            }
            let preds = entry.value();
            'outer: for write in &my_writes {
                for pred in preds {
                    if pred.table_id == write.table_id && Self::key_in_range(&write.key, pred) {
                        conflicting.push(other_txn);
                        break 'outer;
                    }
                }
            }
        }
        conflicting
    }

    /// Remove all locks for a completed transaction.
    pub fn remove_txn(&self, txn_id: TxnId) {
        self.predicates.remove(&txn_id);
        self.write_intents.remove(&txn_id);
    }

    /// Check if a key falls within a predicate's range.
    fn key_in_range(key: &[u8], pred: &SsiPredicate) -> bool {
        if let Some(ref start) = pred.range_start {
            if key < start.as_slice() {
                return false;
            }
        }
        if let Some(ref end) = pred.range_end {
            if key > end.as_slice() {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_deadlock() {
        let wfg = WaitForGraph::new();
        wfg.add_wait(TxnId(1), TxnId(2));
        wfg.add_wait(TxnId(2), TxnId(3));
        assert!(wfg.detect_cycle().is_none());
    }

    #[test]
    fn test_simple_deadlock() {
        let wfg = WaitForGraph::new();
        wfg.add_wait(TxnId(1), TxnId(2));
        wfg.add_wait(TxnId(2), TxnId(1));
        let cycle = wfg.detect_cycle().expect("should detect cycle");
        assert!(cycle.contains(&TxnId(1)));
        assert!(cycle.contains(&TxnId(2)));
    }

    #[test]
    fn test_three_way_deadlock() {
        let wfg = WaitForGraph::new();
        wfg.add_wait(TxnId(1), TxnId(2));
        wfg.add_wait(TxnId(2), TxnId(3));
        wfg.add_wait(TxnId(3), TxnId(1));
        let cycle = wfg.detect_cycle().expect("should detect 3-way cycle");
        assert!(cycle.len() >= 2);
    }

    #[test]
    fn test_remove_breaks_cycle() {
        let wfg = WaitForGraph::new();
        wfg.add_wait(TxnId(1), TxnId(2));
        wfg.add_wait(TxnId(2), TxnId(1));
        assert!(wfg.detect_cycle().is_some());
        wfg.remove_txn(TxnId(1));
        assert!(wfg.detect_cycle().is_none());
    }

    #[test]
    fn test_choose_victim_youngest() {
        let cycle = vec![TxnId(10), TxnId(50), TxnId(30)];
        assert_eq!(WaitForGraph::choose_victim(&cycle), TxnId(50));
    }

    #[test]
    fn test_ssi_no_conflict() {
        let mgr = SsiLockManager::new();
        mgr.add_predicate(
            TxnId(1),
            SsiPredicate {
                table_id: 1,
                range_start: Some(vec![0]),
                range_end: Some(vec![100]),
            },
        );
        mgr.add_write_intent(
            TxnId(2),
            SsiWriteIntent {
                table_id: 2, // different table
                key: vec![50],
            },
        );
        let conflicts = mgr.check_rw_conflicts(TxnId(2));
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_ssi_conflict_detected() {
        let mgr = SsiLockManager::new();
        mgr.add_predicate(
            TxnId(1),
            SsiPredicate {
                table_id: 1,
                range_start: Some(vec![0]),
                range_end: Some(vec![100]),
            },
        );
        mgr.add_write_intent(
            TxnId(2),
            SsiWriteIntent {
                table_id: 1,
                key: vec![50], // within predicate range
            },
        );
        let conflicts = mgr.check_rw_conflicts(TxnId(2));
        assert!(conflicts.contains(&TxnId(1)));
    }

    #[test]
    fn test_ssi_cleanup() {
        let mgr = SsiLockManager::new();
        mgr.add_predicate(
            TxnId(1),
            SsiPredicate {
                table_id: 1,
                range_start: None,
                range_end: None,
            },
        );
        mgr.add_write_intent(
            TxnId(1),
            SsiWriteIntent {
                table_id: 1,
                key: vec![1],
            },
        );
        mgr.remove_txn(TxnId(1));
        let conflicts = mgr.check_rw_conflicts(TxnId(2));
        assert!(conflicts.is_empty());
    }
}

// ── Wound-Wait Manager (C1 Architecture Gap) ─────────────────────────────────
//
// Wound-Wait is a deadlock-*prevention* (not detection) protocol that avoids
// cycles entirely using transaction timestamps (TxnId order = age):
//
//   Older txn (lower TxnId) waits on younger txn (higher TxnId):
//     → WOUND: older *wounds* (aborts) the younger, acquires the lock.
//
//   Younger txn (higher TxnId) waits on older txn (lower TxnId):
//     → WAIT: younger simply waits.  No cycle can form because older always
//       preempts younger, so the wait-for graph is a DAG.
//
// Benefit vs WaitForGraph:
//   - No graph data structure to maintain (O(1) vs O(V+E) per decision).
//   - No periodic cycle-detection scan needed.
//   - Starvation-free: the oldest transaction always makes progress.
//
// The manager tracks "wounded" transactions (those that have been preempted
// and must abort at their next lock-acquire attempt).

/// Wound-Wait deadlock prevention manager.
///
/// Thread-safe: `wounded` is a `DashMap`, making mark/check operations
/// O(1) without a global Mutex.
pub struct WoundWaitManager {
    /// Set of transaction IDs that have been wounded (must abort).
    wounded: DashMap<TxnId, ()>,
}

impl Default for WoundWaitManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WoundWaitManager {
    pub fn new() -> Self {
        Self { wounded: DashMap::new() }
    }

    /// Decide what to do when `waiter` wants a lock held by `holder`.
    ///
    /// Returns `WoundDecision::Wound` when the waiter should abort the holder;
    /// returns `WoundDecision::Wait` when the waiter should block.
    ///
    /// Rule: older (lower TxnId) wounds younger (higher TxnId).
    #[inline]
    pub fn decide(&self, waiter: TxnId, holder: TxnId) -> WoundDecision {
        if waiter.0 < holder.0 {
            WoundDecision::Wound(holder)
        } else {
            WoundDecision::Wait
        }
    }

    /// Mark `txn_id` as wounded — it must abort at its next acquire attempt.
    pub fn mark_wounded(&self, txn_id: TxnId) {
        self.wounded.insert(txn_id, ());
    }

    /// Check whether `txn_id` has been wounded by a competing transaction.
    #[inline]
    pub fn is_wounded(&self, txn_id: TxnId) -> bool {
        self.wounded.contains_key(&txn_id)
    }

    /// Clear the wounded flag when a transaction successfully commits or aborts.
    pub fn clear(&self, txn_id: TxnId) {
        self.wounded.remove(&txn_id);
    }

    /// Number of currently wounded transactions (for diagnostics).
    pub fn wounded_count(&self) -> usize {
        self.wounded.len()
    }
}

/// Decision returned by `WoundWaitManager::decide`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WoundDecision {
    /// Waiter should wound (abort) the given holder and then acquire.
    Wound(TxnId),
    /// Waiter should block until the holder releases.
    Wait,
}

#[cfg(test)]
mod wound_wait_tests {
    use super::*;

    #[test]
    fn older_wounds_younger() {
        let mgr = WoundWaitManager::new();
        let old_txn = TxnId(1);
        let new_txn = TxnId(5);
        assert_eq!(mgr.decide(old_txn, new_txn), WoundDecision::Wound(new_txn));
    }

    #[test]
    fn younger_waits_for_older() {
        let mgr = WoundWaitManager::new();
        let old_txn = TxnId(1);
        let new_txn = TxnId(5);
        assert_eq!(mgr.decide(new_txn, old_txn), WoundDecision::Wait);
    }

    #[test]
    fn mark_and_check_wounded() {
        let mgr = WoundWaitManager::new();
        let txn = TxnId(42);
        assert!(!mgr.is_wounded(txn));
        mgr.mark_wounded(txn);
        assert!(mgr.is_wounded(txn));
        mgr.clear(txn);
        assert!(!mgr.is_wounded(txn));
    }
}
