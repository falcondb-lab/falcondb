//! # Row-Level Lock Manager (P0-1)
//!
//! Implements pessimistic row-level locking to complement the existing OCC
//! mechanism.  Used for:
//!   - `SELECT FOR UPDATE` / `SELECT FOR SHARE`  (P0-2)
//!   - Hot-row serialization (eliminates OCC retry storms on single rows)
//!
//! ## Lock Modes
//! ```text
//!   Share  (S)  — multiple readers allowed; writers blocked
//!   Update (U)  — intent to update; blocks other U/X, allows S
//!   Exclusive(X)— single writer; blocks S/U/X
//! ```
//!
//! ## Compatibility Matrix
//! ```text
//!         S    U    X
//!   S     ✓    ✓    ✗
//!   U     ✓    ✗    ✗
//!   X     ✗    ✗    ✗
//! ```
//!
//! ## Design
//! - Per-row lock slots stored in a `DashMap<RowKey, RowLockSlot>`.
//! - Each slot contains a `Mutex<RowLockState>` (only contended when the row is
//!   actually locked — rare for OLTP).
//! - Waiting transactions park on a per-slot `Condvar`.  Wakeup is broadcast
//!   on every lock release.
//! - The `WaitForGraph` (deadlock detector) is notified on every wait/release.
//! - Lock table is sharded across `SHARD_COUNT` independent `DashMap`s to
//!   reduce shard-level contention under high concurrency.
//!
//! ## Integration with TxnManager
//! - `TxnManager::acquire_row_lock()` / `release_row_locks()` delegate here.
//! - `TxnHandle` gains a `lock_set: Vec<RowKey>` for cleanup on commit/abort.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};

use falcon_common::error::TxnError;
use falcon_common::types::{TableId, TxnId};

use crate::deadlock::{WaitForGraph, WoundDecision, WoundWaitManager};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Number of independent DashMap shards in the lock table.
/// Must be a power of two.
const SHARD_COUNT: usize = 256;

/// Default lock-wait timeout (milliseconds). 0 = wait indefinitely.
pub const DEFAULT_LOCK_TIMEOUT_MS: u64 = 5_000;

// ── Key Types ─────────────────────────────────────────────────────────────────

/// Composite row identity: table + encoded primary key bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowKey {
    pub table_id: TableId,
    pub pk: Vec<u8>,
}

impl RowKey {
    pub fn new(table_id: TableId, pk: Vec<u8>) -> Self {
        Self { table_id, pk }
    }

    /// Fast shard selector — uses the lower bits of a FNV hash.
    fn shard_idx(&self) -> usize {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        h ^= self.table_id.0;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
        for &b in &self.pk {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (h as usize) & (SHARD_COUNT - 1)
    }
}

// ── Lock Mode ─────────────────────────────────────────────────────────────────

/// Lock mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Shared read lock.  Multiple holders allowed.
    Share,
    /// Update intent lock.  Blocks other U/X; compatible with S.
    Update,
    /// Exclusive write lock.  Blocks all other modes.
    Exclusive,
}

impl LockMode {
    /// Returns true if `self` is compatible with `other` (both can be held simultaneously).
    pub const fn compatible_with(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Share, Self::Share)
                | (Self::Share, Self::Update)
                | (Self::Update, Self::Share)
        )
    }
}

// ── Per-row Lock State ────────────────────────────────────────────────────────

/// A single holder entry inside a `RowLockSlot`.
#[derive(Debug, Clone)]
struct LockHolder {
    txn_id: TxnId,
    mode: LockMode,
}

/// State protected by the per-row mutex.
#[derive(Debug, Default)]
struct RowLockState {
    holders: Vec<LockHolder>,
    /// Number of transactions currently waiting for this slot.
    waiters: usize,
}

impl RowLockState {
    /// Whether `mode` is immediately grantable given current holders.
    fn is_grantable(&self, requesting_txn: TxnId, mode: LockMode) -> bool {
        for h in &self.holders {
            if h.txn_id == requesting_txn {
                continue; // same txn re-acquiring is fine
            }
            if !mode.compatible_with(h.mode) {
                return false;
            }
        }
        true
    }

    /// Grant the lock to `txn_id`.
    fn grant(&mut self, txn_id: TxnId, mode: LockMode) {
        // Upgrade if already held in weaker mode
        for h in &mut self.holders {
            if h.txn_id == txn_id {
                if mode == LockMode::Exclusive
                    || (mode == LockMode::Update && h.mode == LockMode::Share)
                {
                    h.mode = mode;
                }
                return;
            }
        }
        self.holders.push(LockHolder { txn_id, mode });
    }

    /// Release all locks held by `txn_id`.  Returns true if any were released.
    fn release(&mut self, txn_id: TxnId) -> bool {
        let before = self.holders.len();
        self.holders.retain(|h| h.txn_id != txn_id);
        self.holders.len() < before
    }

    fn is_empty(&self) -> bool {
        self.holders.is_empty()
    }
}

// ── Per-row Slot ──────────────────────────────────────────────────────────────

struct RowLockSlot {
    state: Mutex<RowLockState>,
    cvar: Condvar,
}

impl RowLockSlot {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RowLockState::default()),
            cvar: Condvar::new(),
        })
    }
}

// ── Lock Manager ──────────────────────────────────────────────────────────────

/// Row-level lock manager.
///
/// Sharded across `SHARD_COUNT` independent DashMaps to minimise
/// shard-level contention.  Per-row slots are heap-allocated and
/// reference-counted so they can be safely removed when empty.
pub struct LockManager {
    shards: Vec<DashMap<RowKey, Arc<RowLockSlot>>>,
    /// Shared wait-for graph for deadlock detection (legacy, kept for compatibility).
    wfg: Arc<WaitForGraph>,
    /// Wound-Wait manager: O(1) deadlock prevention via timestamp ordering.
    /// Replaces WFG cycle-detection on the hot lock-acquire path.
    wound_wait: WoundWaitManager,
    /// Default lock-wait timeout in milliseconds (0 = no timeout).
    default_timeout_ms: u64,
}

impl LockManager {
    pub fn new(wfg: Arc<WaitForGraph>) -> Self {
        let shards = (0..SHARD_COUNT).map(|_| DashMap::new()).collect();
        Self {
            shards,
            wfg,
            wound_wait: WoundWaitManager::new(),
            default_timeout_ms: DEFAULT_LOCK_TIMEOUT_MS,
        }
    }

    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.default_timeout_ms = timeout_ms;
        self
    }

    /// Acquire a row lock.
    ///
    /// - If the lock is immediately grantable, returns `Ok(())` without blocking.
    /// - Otherwise, parks the calling thread on the slot's condvar until either
    ///   the lock becomes available or the timeout expires.
    /// - Notifies the `WaitForGraph` on wait-start and wait-end for deadlock detection.
    ///
    /// # Errors
    /// - `TxnError::Timeout` — lock-wait timeout exceeded.
    /// - `TxnError::Aborted` — the transaction was chosen as a deadlock victim.
    pub fn acquire(
        &self,
        txn_id: TxnId,
        key: &RowKey,
        mode: LockMode,
        timeout_ms: Option<u64>,
    ) -> Result<(), TxnError> {
        let shard = &self.shards[key.shard_idx()];

        // Get-or-create the slot (lock-free fast path for uncontended rows)
        let slot = shard
            .entry(key.clone())
            .or_insert_with(RowLockSlot::new)
            .clone();

        let timeout_ms = timeout_ms.unwrap_or(self.default_timeout_ms);
        let deadline = if timeout_ms == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_millis(timeout_ms))
        };

        let mut state = slot.state.lock();

        // Fast path: immediately grantable
        if state.is_grantable(txn_id, mode) {
            state.grant(txn_id, mode);
            return Ok(());
        }

        // ── Wound-Wait deadlock prevention (C1 architecture gap fix) ─────────
        // Before parking, apply Wound-Wait: if the waiter is older (lower TxnId)
        // than any conflicting holder, wound (preempt) that holder immediately.
        // This is O(1) per holder comparison — no graph traversal needed.
        let holders: Vec<TxnId> = state.holders.iter().map(|h| h.txn_id).collect();
        for &holder in &holders {
            match self.wound_wait.decide(txn_id, holder) {
                WoundDecision::Wound(victim) => {
                    // Mark victim as wounded — it will abort at its next acquire.
                    self.wound_wait.mark_wounded(victim);
                    tracing::debug!(
                        "Wound-Wait: txn {:?} wounds {:?} (older preempts younger)",
                        txn_id, victim
                    );
                }
                WoundDecision::Wait => {} // younger waits for older — no action
            }
        }

        // Also check if *we* were wounded before we even start waiting.
        if self.wound_wait.is_wounded(txn_id) {
            self.wound_wait.clear(txn_id);
            return Err(TxnError::Aborted(txn_id));
        }

        // Register with legacy WFG (kept for observability / compatibility).
        for holder in &holders {
            self.wfg.add_wait(txn_id, *holder);
        }
        state.waiters += 1;

        loop {
            // Check if we were wounded while waiting.
            if self.wound_wait.is_wounded(txn_id) {
                state.waiters -= 1;
                for holder in &holders {
                    self.wfg.remove_wait(txn_id, *holder);
                }
                self.wound_wait.clear(txn_id);
                slot.cvar.notify_all();
                return Err(TxnError::Aborted(txn_id));
            }

            // Check deadline
            if let Some(dl) = deadline {
                let remaining = dl.checked_duration_since(Instant::now());
                match remaining {
                    None => {
                        state.waiters -= 1;
                        for holder in &holders {
                            self.wfg.remove_wait(txn_id, *holder);
                        }
                        return Err(TxnError::Timeout);
                    }
                    Some(d) => {
                        slot.cvar.wait_for(&mut state, d);
                    }
                }
            } else {
                slot.cvar.wait(&mut state);
            }

            // Check if we were chosen as a deadlock victim
            if state.holders.iter().any(|h| h.txn_id == txn_id) {
                // Already granted during a race — clean up and return
                state.waiters -= 1;
                for holder in &holders {
                    self.wfg.remove_wait(txn_id, *holder);
                }
                return Ok(());
            }

            // Re-check grantability after wakeup
            if state.is_grantable(txn_id, mode) {
                state.grant(txn_id, mode);
                state.waiters -= 1;
                for holder in &holders {
                    self.wfg.remove_wait(txn_id, *holder);
                }
                return Ok(());
            }
        }
    }

    /// Release all locks held by `txn_id` for the given keys.
    /// Broadcasts on each slot's condvar to wake waiting transactions.
    pub fn release_all(&self, txn_id: TxnId, keys: &[RowKey]) {
        for key in keys {
            let shard = &self.shards[key.shard_idx()];
            if let Some(slot_ref) = shard.get(key) {
                let slot = slot_ref.clone();
                drop(slot_ref);
                {
                    let mut state = slot.state.lock();
                    state.release(txn_id);
                    if state.is_empty() && state.waiters == 0 {
                        drop(state);
                        shard.remove(key);
                        continue;
                    }
                }
                slot.cvar.notify_all();
            }
        }
        self.wfg.remove_txn(txn_id);
    }

    /// Force-abort a transaction that was chosen as a deadlock victim.
    /// Sets a poison flag so that the next `acquire` call by that txn returns `Aborted`.
    /// Also wakes all waiters so they can re-check grantability.
    pub fn notify_victim(&self, victim_txn_id: TxnId, held_keys: &[RowKey]) {
        self.release_all(victim_txn_id, held_keys);
    }

    /// Returns the number of currently tracked lock slots across all shards.
    pub fn slot_count(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Snapshot of lock table for observability (`SHOW falcon.lock_stats`).
    pub fn snapshot(&self) -> LockTableSnapshot {
        let mut total_slots = 0usize;
        let mut total_holders = 0usize;
        let mut total_waiters = 0usize;
        for shard in &self.shards {
            for entry in shard.iter() {
                total_slots += 1;
                let state = entry.value().state.lock();
                total_holders += state.holders.len();
                total_waiters += state.waiters;
            }
        }
        LockTableSnapshot {
            total_slots,
            total_holders,
            total_waiters,
        }
    }
}

// ── Table-Level Lock + Escalation (P-escalation) ─────────────────────────────

/// Row count threshold at which a transaction's row locks for one table
/// are escalated to a single table-level lock.
///
/// Default: 1 000 rows.  Above this threshold the per-row slots are released
/// and replaced by a coarser table lock, reducing lock table memory pressure.
pub const ESCALATION_THRESHOLD: usize = 1_000;

/// Table-level lock slot — coarser granularity than row-level.
#[derive(Debug, Default)]
struct TableLockState {
    holders: Vec<LockHolder>,
    waiters: usize,
}

impl TableLockState {
    fn is_grantable(&self, txn_id: TxnId, mode: LockMode) -> bool {
        for h in &self.holders {
            if h.txn_id == txn_id {
                continue;
            }
            if !mode.compatible_with(h.mode) {
                return false;
            }
        }
        true
    }

    fn grant(&mut self, txn_id: TxnId, mode: LockMode) {
        for h in &mut self.holders {
            if h.txn_id == txn_id {
                if mode == LockMode::Exclusive
                    || (mode == LockMode::Update && h.mode == LockMode::Share)
                {
                    h.mode = mode;
                }
                return;
            }
        }
        self.holders.push(LockHolder { txn_id, mode });
    }

    fn release(&mut self, txn_id: TxnId) -> bool {
        let before = self.holders.len();
        self.holders.retain(|h| h.txn_id != txn_id);
        self.holders.len() < before
    }
}

struct TableLockSlot {
    state: Mutex<TableLockState>,
    cvar: Condvar,
}

/// Manages table-level (coarse-grain) locks.
///
/// Used both for DDL exclusive access and for lock escalation: when a
/// transaction accumulates more than `ESCALATION_THRESHOLD` row locks on
/// one table it calls `LockManager::try_escalate()`, which:
/// 1. Acquires the table lock in the same mode.
/// 2. Releases all per-row slots for that table.
/// 3. Records the table lock in `TxnLockSet::table_locks`.
pub struct TableLockManager {
    /// table_id → slot
    slots: DashMap<TableId, Arc<TableLockSlot>>,
    wfg: Arc<WaitForGraph>,
    default_timeout_ms: u64,
}

impl TableLockManager {
    pub fn new(wfg: Arc<WaitForGraph>) -> Self {
        Self {
            slots: DashMap::new(),
            wfg,
            default_timeout_ms: DEFAULT_LOCK_TIMEOUT_MS,
        }
    }

    /// Acquire a table-level lock.
    pub fn acquire(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        mode: LockMode,
        timeout_ms: Option<u64>,
    ) -> Result<(), TxnError> {
        let slot = self
            .slots
            .entry(table_id)
            .or_insert_with(|| {
                Arc::new(TableLockSlot {
                    state: Mutex::new(TableLockState::default()),
                    cvar: Condvar::new(),
                })
            })
            .clone();

        let timeout_ms = timeout_ms.unwrap_or(self.default_timeout_ms);
        let deadline = if timeout_ms == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_millis(timeout_ms))
        };

        let mut state = slot.state.lock();
        if state.is_grantable(txn_id, mode) {
            state.grant(txn_id, mode);
            return Ok(());
        }

        let holders: Vec<TxnId> = state.holders.iter().map(|h| h.txn_id).collect();
        for h in &holders {
            self.wfg.add_wait(txn_id, *h);
        }
        state.waiters += 1;

        loop {
            match deadline {
                Some(dl) => {
                    let rem = dl.checked_duration_since(Instant::now());
                    match rem {
                        None => {
                            state.waiters -= 1;
                            for h in &holders { self.wfg.remove_wait(txn_id, *h); }
                            return Err(TxnError::Timeout);
                        }
                        Some(d) => { slot.cvar.wait_for(&mut state, d); }
                    }
                }
                None => { slot.cvar.wait(&mut state); }
            }

            if state.is_grantable(txn_id, mode) {
                state.grant(txn_id, mode);
                state.waiters -= 1;
                for h in &holders { self.wfg.remove_wait(txn_id, *h); }
                return Ok(());
            }
        }
    }

    /// Release a table-level lock held by `txn_id`.
    pub fn release(&self, txn_id: TxnId, table_id: TableId) {
        if let Some(slot_ref) = self.slots.get(&table_id) {
            let slot = slot_ref.clone();
            drop(slot_ref);
            {
                let mut state = slot.state.lock();
                state.release(txn_id);
                if state.holders.is_empty() && state.waiters == 0 {
                    drop(state);
                    self.slots.remove(&table_id);
                    return;
                }
            }
            slot.cvar.notify_all();
        }
        self.wfg.remove_txn(txn_id);
    }
}

impl LockManager {
    /// Attempt lock escalation for `txn_id` on `table_id`.
    ///
    /// If the number of row-level locks held by `txn_id` on `table_id`
    /// exceeds `ESCALATION_THRESHOLD`, this method:
    /// 1. Acquires a table-level lock (via `table_mgr`) in `mode`.
    /// 2. Releases all per-row slots held by this transaction on that table.
    /// 3. Returns `true` to signal the caller to update `TxnLockSet`.
    ///
    /// Returns `false` if escalation is not needed (below threshold).
    pub fn try_escalate(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        keys: &[RowKey],
        mode: LockMode,
        table_mgr: &TableLockManager,
        timeout_ms: Option<u64>,
    ) -> Result<bool, TxnError> {
        // Count how many keys in this batch belong to `table_id`.
        let table_keys: Vec<&RowKey> = keys
            .iter()
            .filter(|k| k.table_id == table_id)
            .collect();

        if table_keys.len() < ESCALATION_THRESHOLD {
            return Ok(false);
        }

        // Escalate: acquire table lock then release all per-row slots.
        table_mgr.acquire(txn_id, table_id, mode, timeout_ms)?;

        // Release per-row slots for this table.
        let table_row_keys: Vec<RowKey> = table_keys.iter().map(|k| (*k).clone()).collect();
        self.release_all(txn_id, &table_row_keys);

        tracing::debug!(
            "Lock escalation: txn={} table={:?} {} row locks → 1 table lock",
            txn_id, table_id, table_row_keys.len()
        );

        Ok(true)
    }
}

/// Immutable snapshot of lock table statistics.
#[derive(Debug, Clone, Default)]
pub struct LockTableSnapshot {
    /// Number of distinct row slots currently in the lock table.
    pub total_slots: usize,
    /// Total number of active lock holders across all slots.
    pub total_holders: usize,
    /// Total number of transactions currently waiting for a lock.
    pub total_waiters: usize,
}

// ── Per-transaction lock set ──────────────────────────────────────────────────

/// Tracks all row keys locked by a single transaction.
/// Stored inside `TxnHandle` and flushed on commit/abort.
#[derive(Debug, Default, Clone)]
pub struct TxnLockSet {
    pub keys: Vec<RowKey>,
}

impl TxnLockSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, key: RowKey) {
        if !self.keys.contains(&key) {
            self.keys.push(key);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn drain(&mut self) -> Vec<RowKey> {
        std::mem::take(&mut self.keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deadlock::{WaitForGraph, WoundDecision, WoundWaitManager};

    fn make_key(table: u64, pk: u8) -> RowKey {
        RowKey::new(TableId(table), vec![pk])
    }

    fn make_mgr() -> LockManager {
        LockManager::new(Arc::new(WaitForGraph::new())).with_timeout(500)
    }

    #[test]
    fn test_exclusive_acquire_release() {
        let mgr = make_mgr();
        let key = make_key(1, 1);
        mgr.acquire(TxnId(1), &key, LockMode::Exclusive, Some(100))
            .expect("first acquire should succeed");
        mgr.release_all(TxnId(1), &[key.clone()]);
        mgr.acquire(TxnId(2), &key, LockMode::Exclusive, Some(100))
            .expect("acquire after release should succeed");
    }

    #[test]
    fn test_shared_compatibility() {
        let mgr = make_mgr();
        let key = make_key(1, 2);
        mgr.acquire(TxnId(1), &key, LockMode::Share, Some(100))
            .expect("S1 ok");
        mgr.acquire(TxnId(2), &key, LockMode::Share, Some(100))
            .expect("S2 ok (S/S compatible)");
    }

    #[test]
    fn test_exclusive_blocks_timeout() {
        let mgr = make_mgr();
        let key = make_key(1, 3);
        mgr.acquire(TxnId(1), &key, LockMode::Exclusive, Some(100))
            .expect("X1 ok");
        // TxnId(2) should timeout quickly
        let result = mgr.acquire(TxnId(2), &key, LockMode::Exclusive, Some(10));
        assert!(matches!(result, Err(TxnError::Timeout)));
    }

    #[test]
    fn test_update_blocks_update() {
        let mgr = make_mgr();
        let key = make_key(1, 4);
        mgr.acquire(TxnId(1), &key, LockMode::Update, Some(100))
            .expect("U1 ok");
        let result = mgr.acquire(TxnId(2), &key, LockMode::Update, Some(10));
        assert!(matches!(result, Err(TxnError::Timeout)));
    }

    #[test]
    fn test_release_wakes_waiter() {
        let mgr = Arc::new(make_mgr());
        let key = make_key(1, 5);

        mgr.acquire(TxnId(1), &key, LockMode::Exclusive, Some(0))
            .expect("X1 ok");

        let mgr2 = Arc::clone(&mgr);
        let key2 = key.clone();
        let handle = std::thread::spawn(move || {
            mgr2.acquire(TxnId(2), &key2, LockMode::Exclusive, Some(2000))
        });

        std::thread::sleep(Duration::from_millis(30));
        mgr.release_all(TxnId(1), &[key]);

        let result = handle.join().expect("thread panicked");
        assert!(result.is_ok(), "waiter should get lock after release");
    }

    #[test]
    fn test_lock_mode_compatibility_matrix() {
        assert!(LockMode::Share.compatible_with(LockMode::Share));
        assert!(LockMode::Share.compatible_with(LockMode::Update));
        assert!(LockMode::Update.compatible_with(LockMode::Share));
        assert!(!LockMode::Update.compatible_with(LockMode::Update));
        assert!(!LockMode::Update.compatible_with(LockMode::Exclusive));
        assert!(!LockMode::Exclusive.compatible_with(LockMode::Share));
        assert!(!LockMode::Exclusive.compatible_with(LockMode::Exclusive));
    }
}
