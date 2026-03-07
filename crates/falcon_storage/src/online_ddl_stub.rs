//! # Module Status: STUB (default build, `online_ddl_full` feature OFF)
//!
//! Minimal no-op stubs so `StorageEngine` compiles without the full
//! online DDL state-machine.  Enable `online_ddl_full` to get the real
//! implementation with background backfill tracking and status queries.

/// Stub DDL operation tracker — all methods are no-ops.
#[derive(Debug, Default)]
pub struct OnlineDdlManager;

/// DDL operation kind (stub — mirrors real enum variants for API compat).
#[derive(Debug, Clone)]
pub enum DdlOpKind {
    AddColumn {
        table_name: String,
        column_name: String,
        has_default: bool,
    },
    DropColumn {
        table_name: String,
        column_name: String,
    },
    ChangeColumnType {
        table_name: String,
        column_name: String,
        new_type: String,
    },
    MetadataOnly {
        description: String,
    },
}

/// Stub progress snapshot returned by ddl_progress() when feature is off.
#[derive(Debug, Clone)]
pub struct DdlProgress {
    pub id: u64,
    pub description: String,
    pub phase: String,
    pub rows_processed: u64,
    pub rows_total: u64,
    pub pct: f64,
    pub elapsed_ms: Option<u64>,
    pub error: Option<String>,
}

/// Backfill batch size — must match the real module's value (1024).
pub const BACKFILL_BATCH_SIZE: usize = 1024;

impl OnlineDdlManager {
    pub fn new() -> Self {
        Self
    }
    pub fn register(&self, _table_id: falcon_common::types::TableId, _kind: DdlOpKind) -> u64 {
        0
    }
    pub fn start(&self, _id: u64) {}
    pub fn begin_backfill(&self, _id: u64, _total: u64) {}
    pub fn record_progress(&self, _id: u64, _count: u64) {}
    pub fn complete(&self, _id: u64) {}
    pub fn fail(&self, _id: u64, _reason: String) {}
    pub fn active_operations(&self) -> Vec<(u64, String)> {
        Vec::new()
    }
}
