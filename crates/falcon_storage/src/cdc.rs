//! Change Data Capture (CDC) — logical decoding stream for external consumers.
//!
//! Enterprise feature for real-time data integration:
//! - Captures INSERT, UPDATE, DELETE changes at the row level
//! - Provides a replication slot abstraction (like PostgreSQL logical replication)
//! - Supports multiple concurrent consumers with independent progress tracking
//! - Changes are serialized as structured events for downstream systems
//!   (Kafka, Debezium, data warehouses, search indexes)

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use falcon_common::datum::OwnedRow;
use falcon_common::types::{TableId, TxnId};

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

/// CDC Stream Manager — manages replication slots and the change event buffer.
pub struct CdcManager {
    /// All replication slots.
    slots: HashMap<SlotId, ReplicationSlot>,
    /// The change event ring buffer (bounded, oldest events evicted).
    buffer: VecDeque<ChangeEvent>,
    /// Maximum buffer size (number of events).
    max_buffer_size: usize,
    /// Global sequence counter.
    next_seq: AtomicU64,
    /// Global LSN counter (simulated; in production, comes from WAL).
    next_lsn: AtomicU64,
    /// Next slot ID.
    next_slot_id: u64,
    /// Whether CDC is enabled.
    enabled: bool,
}

impl CdcManager {
    /// Create a new CDC manager.
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            slots: HashMap::new(),
            buffer: VecDeque::with_capacity(max_buffer_size.min(10_000)),
            max_buffer_size,
            next_seq: AtomicU64::new(1),
            next_lsn: AtomicU64::new(1),
            next_slot_id: 1,
            enabled: true,
        }
    }

    /// Create a disabled CDC manager.
    pub fn disabled() -> Self {
        Self {
            slots: HashMap::new(),
            buffer: VecDeque::new(),
            max_buffer_size: 0,
            next_seq: AtomicU64::new(1),
            next_lsn: AtomicU64::new(1),
            next_slot_id: 1,
            enabled: false,
        }
    }

    /// Whether CDC is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Create a replication slot.
    pub fn create_slot(&mut self, name: &str) -> Result<SlotId, String> {
        // Check for duplicate name
        if self.slots.values().any(|s| s.name.eq_ignore_ascii_case(name)) {
            return Err(format!("replication slot '{}' already exists", name));
        }

        let id = SlotId(self.next_slot_id);
        self.next_slot_id += 1;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let current_lsn = CdcLsn(self.next_lsn.load(Ordering::Relaxed).saturating_sub(1));
        let slot = ReplicationSlot {
            id,
            name: name.to_string(),
            confirmed_flush_lsn: current_lsn,
            restart_lsn: current_lsn,
            active: false,
            created_at_ms: now_ms,
            table_filter: None,
            include_tx_markers: true,
            include_old_values: false,
        };

        self.slots.insert(id, slot);
        Ok(id)
    }

    /// Drop a replication slot.
    pub fn drop_slot(&mut self, name: &str) -> Result<ReplicationSlot, String> {
        let id = self.slots.iter()
            .find(|(_, s)| s.name.eq_ignore_ascii_case(name))
            .map(|(id, _)| *id)
            .ok_or_else(|| format!("replication slot '{}' not found", name))?;

        let slot = self.slots.get(&id).ok_or("slot not found")?;
        if slot.active {
            return Err(format!("cannot drop active replication slot '{}'", name));
        }

        self.slots.remove(&id).ok_or_else(|| "slot not found".to_string())
    }

    /// Emit a change event into the buffer.
    pub fn emit(&mut self, mut event: ChangeEvent) {
        if !self.enabled {
            return;
        }
        event.seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        event.lsn = CdcLsn(self.next_lsn.fetch_add(1, Ordering::Relaxed));

        if self.buffer.len() >= self.max_buffer_size {
            self.buffer.pop_front(); // evict oldest
        }
        self.buffer.push_back(event);
    }

    /// Convenience: emit an INSERT change.
    pub fn emit_insert(
        &mut self,
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
            table_name: Some(table_name.to_string()),
            timestamp_ms: now_ms(),
            new_row: Some(row),
            old_row: None,
            pk_values,
            ddl_text: None,
        });
    }

    /// Convenience: emit an UPDATE change.
    pub fn emit_update(
        &mut self,
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
            table_name: Some(table_name.to_string()),
            timestamp_ms: now_ms(),
            new_row: Some(new_row),
            old_row,
            pk_values,
            ddl_text: None,
        });
    }

    /// Convenience: emit a DELETE change.
    pub fn emit_delete(
        &mut self,
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
            table_name: Some(table_name.to_string()),
            timestamp_ms: now_ms(),
            new_row: None,
            old_row,
            pk_values,
            ddl_text: None,
        });
    }

    /// Convenience: emit a transaction COMMIT marker.
    pub fn emit_commit(&mut self, txn_id: TxnId) {
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

    /// Poll changes for a slot since its confirmed flush position.
    /// Returns up to `max_events` change events.
    pub fn poll_changes(&self, slot_id: SlotId, max_events: usize) -> Vec<&ChangeEvent> {
        let Some(slot) = self.slots.get(&slot_id) else {
            return vec![];
        };

        self.buffer.iter()
            .filter(|e| e.lsn > slot.confirmed_flush_lsn)
            .filter(|e| {
                // Apply table filter
                if let Some(ref filter) = slot.table_filter {
                    match e.table_id {
                        Some(tid) => filter.contains(&tid),
                        None => slot.include_tx_markers, // tx markers pass if enabled
                    }
                } else {
                    true
                }
            })
            .filter(|e| {
                // Filter tx markers
                if !slot.include_tx_markers {
                    !matches!(e.op, ChangeOp::Begin | ChangeOp::Commit | ChangeOp::Rollback)
                } else {
                    true
                }
            })
            .take(max_events)
            .collect()
    }

    /// Advance a slot's confirmed flush position (consumer acknowledges processing).
    pub fn advance_slot(&mut self, slot_id: SlotId, confirmed_lsn: CdcLsn) -> Result<(), String> {
        let slot = self.slots.get_mut(&slot_id)
            .ok_or_else(|| "slot not found".to_string())?;
        if confirmed_lsn > slot.confirmed_flush_lsn {
            slot.confirmed_flush_lsn = confirmed_lsn;
        }
        Ok(())
    }

    /// Activate a slot (consumer connects).
    pub fn activate_slot(&mut self, slot_id: SlotId) -> Result<(), String> {
        let slot = self.slots.get_mut(&slot_id)
            .ok_or_else(|| "slot not found".to_string())?;
        if slot.active {
            return Err("slot already active".to_string());
        }
        slot.active = true;
        Ok(())
    }

    /// Deactivate a slot (consumer disconnects).
    pub fn deactivate_slot(&mut self, slot_id: SlotId) {
        if let Some(slot) = self.slots.get_mut(&slot_id) {
            slot.active = false;
        }
    }

    /// Set table filter on a slot.
    pub fn set_table_filter(&mut self, slot_id: SlotId, tables: Vec<TableId>) -> Result<(), String> {
        let slot = self.slots.get_mut(&slot_id)
            .ok_or_else(|| "slot not found".to_string())?;
        slot.table_filter = Some(tables);
        Ok(())
    }

    /// Get a slot by name.
    pub fn get_slot_by_name(&self, name: &str) -> Option<&ReplicationSlot> {
        self.slots.values().find(|s| s.name.eq_ignore_ascii_case(name))
    }

    /// List all slots.
    pub fn list_slots(&self) -> Vec<&ReplicationSlot> {
        self.slots.values().collect()
    }

    /// Number of events in the buffer.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Number of slots.
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }
}

impl fmt::Debug for CdcManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CdcManager")
            .field("enabled", &self.enabled)
            .field("slots", &self.slots.len())
            .field("buffer_len", &self.buffer.len())
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
        let mut mgr = CdcManager::new(1000);
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
        let mut mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("consumer1").unwrap();

        // Emit some changes
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("alice".into())]);
        mgr.emit_insert(TxnId(1), TableId(10), "users", row.clone(), Some("1".into()));
        mgr.emit_insert(TxnId(1), TableId(10), "users", row.clone(), Some("2".into()));
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
        let mut mgr = CdcManager::new(1000);
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
        let mut mgr = CdcManager::new(1000);
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
        let mut mgr = CdcManager::new(3); // tiny buffer
        let row = OwnedRow::new(vec![Datum::Int32(1)]);
        for i in 0..5 {
            mgr.emit_insert(TxnId(i), TableId(1), "t", row.clone(), None);
        }
        // Only last 3 should be in buffer
        assert_eq!(mgr.buffer_len(), 3);
    }

    #[test]
    fn test_activate_deactivate_slot() {
        let mut mgr = CdcManager::new(1000);
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
        let mut mgr = CdcManager::new(1000);
        let slot_id = mgr.create_slot("s").unwrap();
        let old = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("old".into())]);
        let new = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("new".into())]);

        mgr.emit_update(TxnId(1), TableId(1), "t", Some(old.clone()), new, Some("1".into()));
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
        let mut mgr = CdcManager::disabled();
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
}
