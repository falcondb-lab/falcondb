//! # Module Status: STUB (feature-gated, default OFF)
//! Minimal type stubs so `StorageEngine` compiles without `online_ddl_full` feature.

/// Stub DDL operation tracker — no-ops in v1.0 default build.
#[derive(Debug, Default)]
pub struct OnlineDdlManager;

/// DDL operation kind (stub).
#[derive(Debug, Clone)]
pub enum DdlOpKind {
    AddColumn { table_name: String, column_name: String, has_default: bool },
    DropColumn { table_name: String, column_name: String },
    ChangeColumnType { table_name: String, column_name: String, new_type: String },
    MetadataOnly { description: String },
}

/// Backfill batch size (stub constant for compatibility).
pub const BACKFILL_BATCH_SIZE: usize = 1000;

impl OnlineDdlManager {
    pub fn new() -> Self { Self }
    pub fn register(&self, _table_id: falcon_common::types::TableId, _kind: DdlOpKind) -> u64 { 0 }
    pub fn start(&self, _id: u64) {}
    pub fn begin_backfill(&self, _id: u64, _total: u64) {}
    pub fn record_progress(&self, _id: u64, _count: u64) {}
    pub fn complete(&self, _id: u64) {}
    pub fn fail(&self, _id: u64, _reason: String) {}
    pub fn active_operations(&self) -> Vec<(u64, String)> { Vec::new() }
}
