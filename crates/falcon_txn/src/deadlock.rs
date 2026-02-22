//! Deadlock detection via wait-for graph (WFG).
//!
//! Each active transaction that is blocked waiting on a lock held by another
//! transaction creates a directed edge in the WFG: `waiter → holder`.
//! A cycle in the WFG means deadlock. We detect cycles via iterative DFS
//! and abort the youngest transaction in the cycle (smallest commit impact).

use falcon_common::types::TxnId;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};

/// A single edge in the wait-for graph: `waiter` is blocked by `holder`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitEdge {
    pub waiter: TxnId,
    pub holder: TxnId,
}

/// Wait-for graph for deadlock detection.
pub struct WaitForGraph {
    /// Adjacency list: waiter → set of holders it's waiting on.
    edges: Mutex<HashMap<TxnId, HashSet<TxnId>>>,
}

impl Default for WaitForGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitForGraph {
    pub fn new() -> Self {
        Self {
            edges: Mutex::new(HashMap::new()),
        }
    }

    /// Record that `waiter` is blocked waiting for `holder`.
    pub fn add_wait(&self, waiter: TxnId, holder: TxnId) {
        let mut edges = self.edges.lock();
        edges.entry(waiter).or_default().insert(holder);
    }

    /// Remove all wait edges for a transaction (e.g., when it commits/aborts
    /// or acquires the lock it was waiting for).
    pub fn remove_txn(&self, txn_id: TxnId) {
        let mut edges = self.edges.lock();
        edges.remove(&txn_id);
        // Also remove txn_id from all holder sets
        for holders in edges.values_mut() {
            holders.remove(&txn_id);
        }
    }

    /// Remove a specific wait edge.
    pub fn remove_wait(&self, waiter: TxnId, holder: TxnId) {
        let mut edges = self.edges.lock();
        if let Some(holders) = edges.get_mut(&waiter) {
            holders.remove(&holder);
            if holders.is_empty() {
                edges.remove(&waiter);
            }
        }
    }

    /// Detect deadlock cycles. Returns the first cycle found (if any)
    /// as a list of TxnIds forming the cycle.
    pub fn detect_cycle(&self) -> Option<Vec<TxnId>> {
        let edges = self.edges.lock();
        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();
        let mut path = Vec::new();

        for &start in edges.keys() {
            if visited.contains(&start) {
                continue;
            }
            if let Some(cycle) = Self::dfs(start, &edges, &mut visited, &mut in_stack, &mut path) {
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
                    let cycle_start = match path.iter().position(|&t| t == holder) {
                        Some(pos) => pos,
                        None => {
                            tracing::error!("BUG: holder {:?} in in_stack but not in path", holder);
                            return None;
                        }
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
        let edges = self.edges.lock();
        edges.values().map(|s| s.len()).sum()
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
/// This implementation uses a simplified approach: we track read predicates
/// per-transaction and check for conflicts at commit time.
pub struct SsiLockManager {
    /// Read predicates: txn_id → list of (table_id, key_range).
    predicates: Mutex<HashMap<TxnId, Vec<SsiPredicate>>>,
    /// Write sets: txn_id → list of (table_id, key).
    write_intents: Mutex<HashMap<TxnId, Vec<SsiWriteIntent>>>,
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
            predicates: Mutex::new(HashMap::new()),
            write_intents: Mutex::new(HashMap::new()),
        }
    }

    /// Record a read predicate for a transaction.
    pub fn add_predicate(&self, txn_id: TxnId, predicate: SsiPredicate) {
        self.predicates
            .lock()
            .entry(txn_id)
            .or_default()
            .push(predicate);
    }

    /// Record a write intent for a transaction.
    pub fn add_write_intent(&self, txn_id: TxnId, intent: SsiWriteIntent) {
        self.write_intents
            .lock()
            .entry(txn_id)
            .or_default()
            .push(intent);
    }

    /// Check if a transaction's writes conflict with any other transaction's
    /// read predicates (rw-antidependency).
    ///
    /// Returns the list of transactions whose predicates are violated.
    pub fn check_rw_conflicts(&self, txn_id: TxnId) -> Vec<TxnId> {
        let writes = self.write_intents.lock();
        let predicates = self.predicates.lock();

        let my_writes = match writes.get(&txn_id) {
            Some(w) => w,
            None => return vec![],
        };

        let mut conflicting = Vec::new();
        for (&other_txn, preds) in predicates.iter() {
            if other_txn == txn_id {
                continue;
            }
            for write in my_writes {
                for pred in preds {
                    if pred.table_id == write.table_id && Self::key_in_range(&write.key, pred) {
                        conflicting.push(other_txn);
                        break;
                    }
                }
            }
        }
        conflicting
    }

    /// Remove all locks for a completed transaction.
    pub fn remove_txn(&self, txn_id: TxnId) {
        self.predicates.lock().remove(&txn_id);
        self.write_intents.lock().remove(&txn_id);
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
