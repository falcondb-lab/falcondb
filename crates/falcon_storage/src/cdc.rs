//! Change Data Capture (CDC) — logical decoding stream for external consumers.
//!
//! Production feature for real-time data integration:
//! - Captures INSERT, UPDATE, DELETE, DDL changes at the row level
//! - Provides a replication slot abstraction (like PostgreSQL logical replication)
//! - Supports multiple concurrent consumers with independent progress tracking
//! - Thread-safe: all methods take `&self` using interior mutability
//! - Changes are serialized as structured events for downstream systems
//!   (Kafka, Debezium, data warehouses, search indexes)
//!
//! # ⚠ Limitation: In-Memory Only
//!
//! All CDC events and slot state are held in memory (`VecDeque` + `HashMap`).
//! **Events are lost on process restart.**  This is acceptable for:
//! - Development and testing
//! - Short-lived streaming where the consumer keeps up in real time
//!
//! For production durability, a future version should:
//! 1. Persist slot metadata (confirmed LSN, creation time) to disk or the WAL.
//! 2. Back the event buffer with a spill-to-disk ring buffer or read directly
//!    from WAL segments (like PostgreSQL's logical decoding from WAL).
//! 3. Implement slot retention so events are not discarded until all active
//!    slots have confirmed past them.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};

use falcon_common::datum::OwnedRow;
use falcon_common::types::{TableId, TxnId};

// ── Default configuration ────────────────────────────────────────────────────

/// Default CDC event buffer size.
pub const DEFAULT_CDC_BUFFER_SIZE: usize = 100_000;

// ── Types ────────────────────────────────────────────────────────────────────

/// Unique identifier for a CDC replication slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SlotId(pub u64);

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "slot:{}", self.0)
    }
}

/// Confirmed flush position (consumer has processed up to this LSN).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CdcLsn(pub u64);

impl fmt::Display for CdcLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Type of change captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
    /// DDL change (table created/dropped/altered).
    Ddl,
    /// Transaction begin marker.
    Begin,
    /// Transaction commit marker.
    Commit,
    /// Transaction rollback marker.
    Rollback,
}

impl fmt::Display for ChangeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert => write!(f, "INSERT"),
            Self::Update => write!(f, "UPDATE"),
            Self::Delete => write!(f, "DELETE"),
            Self::Ddl => write!(f, "DDL"),
            Self::Begin => write!(f, "BEGIN"),
            Self::Commit => write!(f, "COMMIT"),
            Self::Rollback => write!(f, "ROLLBACK"),
        }
    }
}

/// A single change event in the CDC stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// LSN of the WAL record that produced this change.
    pub lsn: CdcLsn,
    /// Transaction that made this change.
    pub txn_id: TxnId,
    /// Type of change.
    pub op: ChangeOp,
    /// Table affected (None for txn markers).
    pub table_id: Option<TableId>,
    /// Table name (denormalized for consumer convenience).
    pub table_name: Option<String>,
    /// Wall-clock timestamp of the change (unix millis).
    pub timestamp_ms: u64,
    /// The new row data (for INSERT and UPDATE).
    pub new_row: Option<OwnedRow>,
    /// The old row data (for UPDATE and DELETE; requires REPLICA IDENTITY FULL).
    pub old_row: Option<OwnedRow>,
    /// Primary key values of the affected row (serialized as JSON for portability).
    pub pk_values: Option<String>,
    /// DDL statement text (for DDL changes).
    pub ddl_text: Option<String>,
}

/// Replication slot — tracks a consumer's position in the CDC stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSlot {
    pub id: SlotId,
    /// Slot name (unique, user-defined).
    pub name: String,
    /// The confirmed flush position (consumer has processed up to here).
    pub confirmed_flush_lsn: CdcLsn,
    /// The restart LSN (where to start streaming from on reconnect).
    pub restart_lsn: CdcLsn,
    /// Whether this slot is currently active (a consumer is connected).
    pub active: bool,
    /// Creation timestamp.
    pub created_at_ms: u64,
    /// Optional: filter to specific tables.
    pub table_filter: Option<Vec<TableId>>,
    /// Whether to include transaction markers (BEGIN/COMMIT).
    pub include_tx_markers: bool,
    /// Whether to include old row values (REPLICA IDENTITY FULL).
    pub include_old_values: bool,
}

// ── Metrics ──────────────────────────────────────────────────────────────────

/// CDC operational metrics (lock-free counters).
#[derive(Debug)]
pub struct CdcMetrics {
    /// Total events emitted (lifetime).
    pub events_emitted: AtomicU64,
    /// Total events evicted due to buffer overflow.
    pub events_evicted: AtomicU64,
    /// Total events polled by consumers.
    pub events_polled: AtomicU64,
    /// Total slot create operations.
    pub slots_created: AtomicU64,
    /// Total slot drop operations.
    pub slots_dropped: AtomicU64,
}

impl CdcMetrics {
    fn new() -> Self {
        Self {
            events_emitted: AtomicU64::new(0),
            events_evicted: AtomicU64::new(0),
            events_polled: AtomicU64::new(0),
            slots_created: AtomicU64::new(0),
            slots_dropped: AtomicU64::new(0),
        }
    }
}

// ── CdcManager ───────────────────────────────────────────────────────────────

/// Thread-safe CDC Stream Manager — manages replication slots and the change
/// event buffer.  All public methods take `&self`; interior mutability is
/// provided by `parking_lot` locks and atomics.
pub struct CdcManager {
    /// All replication slots (keyed by SlotId).
    slots: RwLock<HashMap<SlotId, ReplicationSlot>>,
    /// The change event ring buffer (bounded, oldest events evicted).
    buffer: Mutex<VecDeque<ChangeEvent>>,
    /// Maximum buffer size (number of events).
    max_buffer_size: usize,
    /// Global sequence counter.
    next_seq: AtomicU64,
    /// Global LSN counter (simulated; in production, comes from WAL).
    next_lsn: AtomicU64,
    /// Next slot ID.
    next_slot_id: AtomicU64,
    /// Whether CDC is enabled.
    enabled: AtomicBool,
    /// Operational metrics.
    pub metrics: CdcMetrics,
}

impl CdcManager {
    /// Create a new CDC manager with the given buffer capacity.
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
            buffer: Mutex::new(VecDeque::with_capacity(max_buffer_size.min(10_000))),
            max_buffer_size,
            next_seq: AtomicU64::new(1),
            next_lsn: AtomicU64::new(1),
            next_slot_id: AtomicU64::new(1),
            enabled: AtomicBool::new(true),
            metrics: CdcMetrics::new(),
        }
    }

    /// Create a disabled CDC manager (zero overhead when CDC is off).
    pub fn disabled() -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
            buffer: Mutex::new(VecDeque::new()),
            max_buffer_size: 0,
            next_seq: AtomicU64::new(1),
            next_lsn: AtomicU64::new(1),
            next_slot_id: AtomicU64::new(1),
            enabled: AtomicBool::new(false),
            metrics: CdcMetrics::new(),
        }
    }

    /// Whether CDC is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Enable or disable CDC at runtime.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    // ── Slot Management ──────────────────────────────────────────────────

    /// Create a replication slot.
    pub fn create_slot(&self, name: &str) -> Result<SlotId, String> {
        let mut slots = self.slots.write();

        // Check for duplicate name
        if slots.values().any(|s| s.name.eq_ignore_ascii_case(name)) {
            return Err(format!("replication slot '{name}' already exists"));
        }

        let id = SlotId(self.next_slot_id.fetch_add(1, Ordering::Relaxed));
        let now_ms = now_ms();
        let current_lsn = CdcLsn(self.next_lsn.load(Ordering::Relaxed).saturating_sub(1));

        let slot = ReplicationSlot {
            id,
            name: name.to_owned(),
            confirmed_flush_lsn: current_lsn,
            restart_lsn: current_lsn,
            active: false,
            created_at_ms: now_ms,
            table_filter: None,
            include_tx_markers: true,
            include_old_values: false,
        };

        slots.insert(id, slot);
        self.metrics.slots_created.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    /// Drop a replication slot.
    pub fn drop_slot(&self, name: &str) -> Result<ReplicationSlot, String> {
        let mut slots = self.slots.write();

        let id = slots
            .iter()
            .find(|(_, s)| s.name.eq_ignore_ascii_case(name))
            .map(|(id, _)| *id)
            .ok_or_else(|| format!("replication slot '{name}' not found"))?;

        let slot = slots.get(&id).ok_or("slot not found")?;
        if slot.active {
            return Err(format!("cannot drop active replication slot '{name}'"));
        }

        let removed = slots
            .remove(&id)
            .ok_or_else(|| "slot not found".to_owned())?;
        self.metrics.slots_dropped.fetch_add(1, Ordering::Relaxed);
        Ok(removed)
    }

    /// Activate a slot (consumer connects).
    pub fn activate_slot(&self, slot_id: SlotId) -> Result<(), String> {
        let mut slots = self.slots.write();
        let slot = slots
            .get_mut(&slot_id)
            .ok_or_else(|| "slot not found".to_owned())?;
        if slot.active {
            return Err("slot already active".to_owned());
        }
        slot.active = true;
        Ok(())
    }

    /// Deactivate a slot (consumer disconnects).
    pub fn deactivate_slot(&self, slot_id: SlotId) {
        let mut slots = self.slots.write();
        if let Some(slot) = slots.get_mut(&slot_id) {
            slot.active = false;
        }
    }

    /// Set table filter on a slot.
    pub fn set_table_filter(&self, slot_id: SlotId, tables: Vec<TableId>) -> Result<(), String> {
        let mut slots = self.slots.write();
        let slot = slots
            .get_mut(&slot_id)
            .ok_or_else(|| "slot not found".to_owned())?;
        slot.table_filter = Some(tables);
        Ok(())
    }

    /// Advance a slot's confirmed flush position (consumer acknowledges processing).
    pub fn advance_slot(&self, slot_id: SlotId, confirmed_lsn: CdcLsn) -> Result<(), String> {
        let mut slots = self.slots.write();
        let slot = slots
            .get_mut(&slot_id)
            .ok_or_else(|| "slot not found".to_owned())?;
        if confirmed_lsn > slot.confirmed_flush_lsn {
            slot.confirmed_flush_lsn = confirmed_lsn;
        }
        Ok(())
    }

    /// Get a slot by name (returns a clone to avoid holding the lock).
    pub fn get_slot_by_name(&self, name: &str) -> Option<ReplicationSlot> {
        self.slots
            .read()
            .values()
            .find(|s| s.name.eq_ignore_ascii_case(name))
            .cloned()
    }

    /// List all slots (returns clones).
    pub fn list_slots(&self) -> Vec<ReplicationSlot> {
        self.slots.read().values().cloned().collect()
    }

    /// Number of slots.
    pub fn slot_count(&self) -> usize {
        self.slots.read().len()
    }

    // ── Event Emission ───────────────────────────────────────────────────

    /// Emit a change event into the buffer.
    pub fn emit(&self, mut event: ChangeEvent) {
        if !self.is_enabled() {
            return;
        }
        event.seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        event.lsn = CdcLsn(self.next_lsn.fetch_add(1, Ordering::Relaxed));

        let mut buffer = self.buffer.lock();
        if buffer.len() >= self.max_buffer_size {
            buffer.pop_front(); // evict oldest
            self.metrics.events_evicted.fetch_add(1, Ordering::Relaxed);
        }
        buffer.push_back(event);
        self.metrics.events_emitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Convenience: emit an INSERT change.
    pub fn emit_insert(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        table_name: &str,
        row: OwnedRow,
        pk_values: Option<String>,
    ) {
        self.emit(ChangeEvent {
            seq: 0,
            lsn: CdcLsn(0),
            txn_id,
            op: ChangeOp::Insert,
            table_id: Some(table_id),
            table_name: Some(table_name.to_owned()),
            timestamp_ms: now_ms(),
            new_row: Some(row),
            old_row: None,
            pk_values,
            ddl_text: None,
        });
    }

    /// Convenience: emit an UPDATE change.
    pub fn emit_update(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        table_name: &str,
        old_row: Option<OwnedRow>,
        new_row: OwnedRow,
        pk_values: Option<String>,
    ) {
        self.emit(ChangeEvent {
            seq: 0,
            lsn: CdcLsn(0),
            txn_id,
            op: ChangeOp::Update,
            table_id: Some(table_id),
            table_name: Some(table_name.to_owned()),
            timestamp_ms: now_ms(),
            new_row: Some(new_row),
            old_row,
            pk_values,
            ddl_text: None,
        });
    }

    /// Convenience: emit a DELETE change.
    pub fn emit_delete(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        table_name: &str,
        old_row: Option<OwnedRow>,
        pk_values: Option<String>,
    ) {
        self.emit(ChangeEvent {
            seq: 0,
            lsn: CdcLsn(0),
            txn_id,
            op: ChangeOp::Delete,
            table_id: Some(table_id),
            table_name: Some(table_name.to_owned()),
            timestamp_ms: now_ms(),
            new_row: None,
            old_row,
            pk_values,
            ddl_text: None,
        });
    }

    /// Convenience: emit a transaction BEGIN marker.
    pub fn emit_begin(&self, txn_id: TxnId) {
        self.emit(ChangeEvent {
            seq: 0,
            lsn: CdcLsn(0),
            txn_id,
            op: ChangeOp::Begin,
            table_id: None,
            table_name: None,
            timestamp_ms: now_ms(),
            new_row: None,
            old_row: None,
            pk_values: None,
            ddl_text: None,
        });
    }

    /// Convenience: emit a transaction COMMIT marker.
    pub fn emit_commit(&self, txn_id: TxnId) {
        self.emit(ChangeEvent {
            seq: 0,
            lsn: CdcLsn(0),
            txn_id,
            op: ChangeOp::Commit,
            table_id: None,
            table_name: None,
            timestamp_ms: now_ms(),
            new_row: None,
            old_row: None,
            pk_values: None,
            ddl_text: None,
        });
    }

    /// Convenience: emit a transaction ROLLBACK marker.
    pub fn emit_rollback(&self, txn_id: TxnId) {
        self.emit(ChangeEvent {
            seq: 0,
            lsn: CdcLsn(0),
            txn_id,
            op: ChangeOp::Rollback,
            table_id: None,
            table_name: None,
            timestamp_ms: now_ms(),
            new_row: None,
            old_row: None,
            pk_values: None,
            ddl_text: None,
        });
    }

    /// Convenience: emit a DDL change event.
    pub fn emit_ddl(&self, txn_id: TxnId, ddl_text: &str) {
        self.emit(ChangeEvent {
            seq: 0,
            lsn: CdcLsn(0),
            txn_id,
            op: ChangeOp::Ddl,
            table_id: None,
            table_name: None,
            timestamp_ms: now_ms(),
            new_row: None,
            old_row: None,
            pk_values: None,
            ddl_text: Some(ddl_text.to_owned()),
        });
    }

    // ── Consumer Polling ─────────────────────────────────────────────────

    /// Poll changes for a slot since its confirmed flush position.
    /// Returns up to `max_events` change events (cloned).
    pub fn poll_changes(&self, slot_id: SlotId, max_events: usize) -> Vec<ChangeEvent> {
        let slots = self.slots.read();
        let Some(slot) = slots.get(&slot_id) else {
            return vec![];
        };

        let buffer = self.buffer.lock();
        let results: Vec<ChangeEvent> = buffer
            .iter()
            .filter(|e| e.lsn > slot.confirmed_flush_lsn)
            .filter(|e| {
                // Apply table filter
                slot.table_filter.as_ref().is_none_or(|filter| {
                    e.table_id
                        .map_or(slot.include_tx_markers, |tid| filter.contains(&tid))
                })
            })
            .filter(|e| {
                // Filter tx markers
                if !slot.include_tx_markers {
                    !matches!(
                        e.op,
                        ChangeOp::Begin | ChangeOp::Commit | ChangeOp::Rollback
                    )
                } else {
                    true
                }
            })
            .take(max_events)
            .cloned()
            .collect();

        self.metrics
            .events_polled
            .fetch_add(results.len() as u64, Ordering::Relaxed);
        results
    }

    // ── Buffer Management ────────────────────────────────────────────────

    /// Number of events in the buffer.
    pub fn buffer_len(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Garbage-collect events that all slots have already confirmed.
    /// Useful for reclaiming buffer memory when consumers are keeping up.
    pub fn gc_before_lsn(&self, min_lsn: CdcLsn) {
        let mut buffer = self.buffer.lock();
        while let Some(front) = buffer.front() {
            if front.lsn < min_lsn {
                buffer.pop_front();
                self.metrics.events_evicted.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    /// Compute the minimum confirmed flush LSN across all slots.
    /// Returns None if there are no slots.
    pub fn min_confirmed_lsn(&self) -> Option<CdcLsn> {
        self.slots
            .read()
            .values()
            .map(|s| s.confirmed_flush_lsn)
            .min()
    }
}

impl fmt::Debug for CdcManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CdcManager")
            .field("enabled", &self.is_enabled())
            .field("slots", &self.slots.read().len())
            .field("buffer_len", &self.buffer.lock().len())
            .field("max_buffer_size", &self.max_buffer_size)
            .finish()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::{Datum, OwnedRow};

    #[test]
    fn test_create_and_drop_slot() {
        let mgr = CdcManager::new(1000);
        let _id = mgr.create_slot("my_slot").unwrap();
        assert_eq!(mgr.slot_count(), 1);
        assert!(mgr.get_slot_by_name("my_slot").is_some());

        // Duplicate rejected
        assert!(mgr.create_slot("my_slot").is_err());

        // Drop
        let dropped = mgr.drop_slot("my_slot");
        assert!(dropped.is_ok());
        assert_eq!(mgr.slot_count(), 0);
    }

    #[test]
    fn test_emit_and_poll() {
        let mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("consumer1").unwrap();

        // Emit some changes
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("alice".into())]);
        mgr.emit_insert(
            TxnId(1),
            TableId(10),
            "users",
            row.clone(),
            Some("1".into()),
        );
        mgr.emit_insert(
            TxnId(1),
            TableId(10),
            "users",
            row.clone(),
            Some("2".into()),
        );
        mgr.emit_commit(TxnId(1));

        assert_eq!(mgr.buffer_len(), 3);

        // Poll changes
        let changes = mgr.poll_changes(slot_id, 100);
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].op, ChangeOp::Insert);
        assert_eq!(changes[2].op, ChangeOp::Commit);
    }

    #[test]
    fn test_advance_slot() {
        let mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("consumer1").unwrap();

        let row = OwnedRow::new(vec![Datum::Int32(1)]);
        mgr.emit_insert(TxnId(1), TableId(10), "t", row.clone(), None);
        mgr.emit_insert(TxnId(1), TableId(10), "t", row.clone(), None);
        mgr.emit_insert(TxnId(1), TableId(10), "t", row, None);

        // Poll all 3
        let changes = mgr.poll_changes(slot_id, 100);
        assert_eq!(changes.len(), 3);

        // Advance past the first 2
        let advance_to = changes[1].lsn;
        mgr.advance_slot(slot_id, advance_to).unwrap();

        // Now only 1 change should be visible
        let changes = mgr.poll_changes(slot_id, 100);
        assert_eq!(changes.len(), 1);
    }

    #[test]
    fn test_table_filter() {
        let mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("filtered").unwrap();
        mgr.set_table_filter(slot_id, vec![TableId(10)]).unwrap();

        let row = OwnedRow::new(vec![Datum::Int32(1)]);
        mgr.emit_insert(TxnId(1), TableId(10), "users", row.clone(), None);
        mgr.emit_insert(TxnId(1), TableId(20), "orders", row, None);

        // Only table 10 changes visible
        let changes = mgr.poll_changes(slot_id, 100);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].table_name.as_deref(), Some("users"));
    }

    #[test]
    fn test_buffer_eviction() {
        let mgr = CdcManager::new(3); // tiny buffer
        let row = OwnedRow::new(vec![Datum::Int32(1)]);
        for i in 0..5 {
            mgr.emit_insert(TxnId(i), TableId(1), "t", row.clone(), None);
        }
        // Only last 3 should be in buffer
        assert_eq!(mgr.buffer_len(), 3);
        assert_eq!(mgr.metrics.events_evicted.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_activate_deactivate_slot() {
        let mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("s1").unwrap();

        mgr.activate_slot(slot_id).unwrap();
        assert!(mgr.get_slot_by_name("s1").unwrap().active);

        // Can't activate twice
        assert!(mgr.activate_slot(slot_id).is_err());

        // Can't drop active slot
        assert!(mgr.drop_slot("s1").is_err());

        mgr.deactivate_slot(slot_id);
        assert!(!mgr.get_slot_by_name("s1").unwrap().active);

        // Now can drop
        assert!(mgr.drop_slot("s1").is_ok());
    }

    #[test]
    fn test_emit_update_delete() {
        let mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("s").unwrap();
        let old = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("old".into())]);
        let new = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("new".into())]);

        mgr.emit_update(
            TxnId(1),
            TableId(1),
            "t",
            Some(old.clone()),
            new,
            Some("1".into()),
        );
        mgr.emit_delete(TxnId(1), TableId(1), "t", Some(old), Some("1".into()));

        let changes = mgr.poll_changes(slot_id, 100);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].op, ChangeOp::Update);
        assert!(changes[0].old_row.is_some());
        assert!(changes[0].new_row.is_some());
        assert_eq!(changes[1].op, ChangeOp::Delete);
        assert!(changes[1].old_row.is_some());
    }

    #[test]
    fn test_disabled_cdc() {
        let mgr = CdcManager::disabled();
        assert!(!mgr.is_enabled());
        let row = OwnedRow::new(vec![Datum::Int32(1)]);
        mgr.emit_insert(TxnId(1), TableId(1), "t", row, None);
        assert_eq!(mgr.buffer_len(), 0); // nothing emitted
    }

    #[test]
    fn test_change_op_display() {
        assert_eq!(ChangeOp::Insert.to_string(), "INSERT");
        assert_eq!(ChangeOp::Update.to_string(), "UPDATE");
        assert_eq!(ChangeOp::Delete.to_string(), "DELETE");
        assert_eq!(ChangeOp::Commit.to_string(), "COMMIT");
    }

    #[test]
    fn test_emit_begin_commit_rollback() {
        let mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("s").unwrap();

        mgr.emit_begin(TxnId(1));
        mgr.emit_insert(
            TxnId(1),
            TableId(1),
            "t",
            OwnedRow::new(vec![Datum::Int32(1)]),
            None,
        );
        mgr.emit_commit(TxnId(1));

        mgr.emit_begin(TxnId(2));
        mgr.emit_rollback(TxnId(2));

        let changes = mgr.poll_changes(slot_id, 100);
        assert_eq!(changes.len(), 5);
        assert_eq!(changes[0].op, ChangeOp::Begin);
        assert_eq!(changes[2].op, ChangeOp::Commit);
        assert_eq!(changes[3].op, ChangeOp::Begin);
        assert_eq!(changes[4].op, ChangeOp::Rollback);
    }

    #[test]
    fn test_emit_ddl() {
        let mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("s").unwrap();

        mgr.emit_ddl(TxnId(1), "CREATE TABLE users (id INT PRIMARY KEY)");

        let changes = mgr.poll_changes(slot_id, 100);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].op, ChangeOp::Ddl);
        assert_eq!(
            changes[0].ddl_text.as_deref(),
            Some("CREATE TABLE users (id INT PRIMARY KEY)")
        );
    }

    #[test]
    fn test_gc_before_lsn() {
        let mgr = CdcManager::new(1000);
        let row = OwnedRow::new(vec![Datum::Int32(1)]);
        for i in 0..5 {
            mgr.emit_insert(TxnId(i), TableId(1), "t", row.clone(), None);
        }
        assert_eq!(mgr.buffer_len(), 5);

        // GC events with LSN < 3
        mgr.gc_before_lsn(CdcLsn(3));
        assert_eq!(mgr.buffer_len(), 3);
    }

    #[test]
    fn test_metrics() {
        let mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("s").unwrap();

        let row = OwnedRow::new(vec![Datum::Int32(1)]);
        mgr.emit_insert(TxnId(1), TableId(1), "t", row, None);
        mgr.emit_commit(TxnId(1));

        assert_eq!(mgr.metrics.events_emitted.load(Ordering::Relaxed), 2);
        assert_eq!(mgr.metrics.slots_created.load(Ordering::Relaxed), 1);

        let _ = mgr.poll_changes(slot_id, 100);
        assert_eq!(mgr.metrics.events_polled.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_concurrent_emit() {
        use std::sync::Arc;
        let mgr = Arc::new(CdcManager::new(10_000));
        let mut handles = Vec::new();
        for t in 0..4 {
            let mgr_c = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                for i in 0..250u64 {
                    let row = OwnedRow::new(vec![Datum::Int64((t * 250 + i) as i64)]);
                    mgr_c.emit_insert(TxnId(t * 250 + i), TableId(1), "t", row, None);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(mgr.buffer_len(), 1000);
        assert_eq!(mgr.metrics.events_emitted.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn test_set_enabled_runtime() {
        let mgr = CdcManager::new(1000);
        assert!(mgr.is_enabled());

        mgr.set_enabled(false);
        assert!(!mgr.is_enabled());

        let row = OwnedRow::new(vec![Datum::Int32(1)]);
        mgr.emit_insert(TxnId(1), TableId(1), "t", row, None);
        assert_eq!(mgr.buffer_len(), 0);

        mgr.set_enabled(true);
        let row = OwnedRow::new(vec![Datum::Int32(2)]);
        mgr.emit_insert(TxnId(2), TableId(1), "t", row, None);
        assert_eq!(mgr.buffer_len(), 1);
    }
}
