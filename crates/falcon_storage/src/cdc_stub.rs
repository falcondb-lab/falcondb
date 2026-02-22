//! # Module Status: STUB (feature-gated, default OFF)
//! Minimal CDC type stubs so downstream crates compile without `cdc_stream` feature.
//! All mutating methods are no-ops; all queries return empty/None.

use serde::{Deserialize, Serialize};
use std::fmt;

use falcon_common::datum::OwnedRow;
use falcon_common::types::{TableId, TxnId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SlotId(pub u64);

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "slot:{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CdcLsn(pub u64);

impl fmt::Display for CdcLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeOp {
    Insert, Update, Delete, Ddl, Begin, Commit, Rollback,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub seq: u64,
    pub lsn: CdcLsn,
    pub txn_id: TxnId,
    pub op: ChangeOp,
    pub table_id: Option<TableId>,
    pub table_name: Option<String>,
    pub timestamp_ms: u64,
    pub new_row: Option<OwnedRow>,
    pub old_row: Option<OwnedRow>,
    pub pk_values: Option<String>,
    pub ddl_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSlot {
    pub id: SlotId,
    pub name: String,
    pub confirmed_flush_lsn: CdcLsn,
    pub restart_lsn: CdcLsn,
    pub active: bool,
    pub created_at_ms: u64,
    pub table_filter: Option<Vec<TableId>>,
    pub include_tx_markers: bool,
    pub include_old_values: bool,
}

/// Stub CDC manager — all operations are no-ops / return errors.
#[derive(Debug)]
pub struct CdcManager;

impl CdcManager {
    pub fn new(_max_buffer_size: usize) -> Self { Self }
    pub fn disabled() -> Self { Self }
    pub fn is_enabled(&self) -> bool { false }
    pub fn create_slot(&mut self, name: &str) -> Result<SlotId, String> {
        Err(format!("CDC not available: cannot create slot '{}'", name))
    }
    pub fn drop_slot(&mut self, name: &str) -> Result<ReplicationSlot, String> {
        Err(format!("CDC not available: slot '{}' not found", name))
    }
    pub fn emit(&mut self, _event: ChangeEvent) {}
    pub fn emit_insert(&mut self, _txn_id: TxnId, _table_id: TableId, _table_name: &str, _row: OwnedRow, _pk: Option<String>) {}
    pub fn emit_update(&mut self, _txn_id: TxnId, _table_id: TableId, _table_name: &str, _old: Option<OwnedRow>, _new: OwnedRow, _pk: Option<String>) {}
    pub fn emit_delete(&mut self, _txn_id: TxnId, _table_id: TableId, _table_name: &str, _old: Option<OwnedRow>, _pk: Option<String>) {}
    pub fn emit_commit(&mut self, _txn_id: TxnId) {}
    pub fn poll_changes(&self, _slot_id: SlotId, _max: usize) -> Vec<&ChangeEvent> { vec![] }
    pub fn advance_slot(&mut self, _slot_id: SlotId, _lsn: CdcLsn) -> Result<(), String> {
        Err("CDC not available".to_string())
    }
    pub fn activate_slot(&mut self, _slot_id: SlotId) -> Result<(), String> {
        Err("CDC not available".to_string())
    }
    pub fn deactivate_slot(&mut self, _slot_id: SlotId) {}
    pub fn get_slot_by_name(&self, _name: &str) -> Option<&ReplicationSlot> { None }
    pub fn list_slots(&self) -> Vec<&ReplicationSlot> { vec![] }
    pub fn buffer_len(&self) -> usize { 0 }
    pub fn slot_count(&self) -> usize { 0 }
    pub fn set_table_filter(&mut self, _slot_id: SlotId, _tables: Vec<TableId>) -> Result<(), String> {
        Err("CDC not available".to_string())
    }
}
