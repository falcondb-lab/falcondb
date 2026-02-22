//! # Module Status: STUB — not on the production OLTP write path.
//! Do NOT reference from planner/executor/txn for production workloads.
//!
//! Online schema change (DDL) manager.
//!
//! Provides non-blocking ALTER TABLE operations that allow concurrent reads
//! and writes while schema changes are in progress. The design uses a
//! state-machine per DDL operation:
//!
//!   Pending → Running → Backfilling → Completed
//!
//! Metadata-only changes (rename, set/drop NOT NULL, set/drop default) skip
//! straight to `Completed`. Data-affecting changes (add column, drop column,
//! change column type) go through `Backfilling` where a background task
//! converts or fills existing rows in batches.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use falcon_common::types::TableId;

// ── DDL operation state machine ──────────────────────────────────────

/// Current phase of an online DDL operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlPhase {
    /// The operation has been registered but not yet started.
    Pending,
    /// Schema metadata has been updated; the operation is active.
    Running,
    /// Background backfill of existing rows is in progress.
    Backfilling,
    /// The operation is fully complete.
    Completed,
    /// The operation failed and was rolled back.
    Failed,
}

impl std::fmt::Display for DdlPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DdlPhase::Pending => write!(f, "pending"),
            DdlPhase::Running => write!(f, "running"),
            DdlPhase::Backfilling => write!(f, "backfilling"),
            DdlPhase::Completed => write!(f, "completed"),
            DdlPhase::Failed => write!(f, "failed"),
        }
    }
}

/// The kind of DDL operation being tracked.
#[derive(Debug, Clone)]
pub enum DdlOpKind {
    AddColumn {
        table_name: String,
        column_name: String,
        /// Default value to backfill (serialised as Datum debug string for logging).
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
    /// Metadata-only ops that complete instantly.
    MetadataOnly { description: String },
}

impl std::fmt::Display for DdlOpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DdlOpKind::AddColumn {
                table_name,
                column_name,
                ..
            } => {
                write!(f, "ADD COLUMN {}.{}", table_name, column_name)
            }
            DdlOpKind::DropColumn {
                table_name,
                column_name,
            } => {
                write!(f, "DROP COLUMN {}.{}", table_name, column_name)
            }
            DdlOpKind::ChangeColumnType {
                table_name,
                column_name,
                new_type,
            } => {
                write!(
                    f,
                    "ALTER COLUMN {}.{} TYPE {}",
                    table_name, column_name, new_type
                )
            }
            DdlOpKind::MetadataOnly { description } => {
                write!(f, "{}", description)
            }
        }
    }
}

/// A tracked online DDL operation.
#[derive(Debug, Clone)]
pub struct DdlOperation {
    pub id: u64,
    pub kind: DdlOpKind,
    pub phase: DdlPhase,
    pub table_id: TableId,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
    /// Number of rows processed during backfill.
    pub rows_processed: u64,
    /// Total rows that need backfill (0 if unknown or not applicable).
    pub rows_total: u64,
    /// Error message if phase == Failed.
    pub error: Option<String>,
}

impl DdlOperation {
    fn new(id: u64, table_id: TableId, kind: DdlOpKind) -> Self {
        Self {
            id,
            kind,
            phase: DdlPhase::Pending,
            table_id,
            started_at: None,
            completed_at: None,
            rows_processed: 0,
            rows_total: 0,
            error: None,
        }
    }

    /// Advance to Running phase.
    pub fn start(&mut self) {
        self.phase = DdlPhase::Running;
        self.started_at = Some(Instant::now());
    }

    /// Advance to Backfilling phase.
    pub fn begin_backfill(&mut self, total_rows: u64) {
        self.phase = DdlPhase::Backfilling;
        self.rows_total = total_rows;
        self.rows_processed = 0;
    }

    /// Record backfill progress.
    pub fn record_progress(&mut self, rows: u64) {
        self.rows_processed += rows;
    }

    /// Mark as completed.
    pub fn complete(&mut self) {
        self.phase = DdlPhase::Completed;
        self.completed_at = Some(Instant::now());
    }

    /// Mark as failed.
    pub fn fail(&mut self, error: String) {
        self.phase = DdlPhase::Failed;
        self.error = Some(error);
        self.completed_at = Some(Instant::now());
    }

    /// Whether the operation needs background backfill.
    pub fn needs_backfill(&self) -> bool {
        matches!(
            self.kind,
            DdlOpKind::AddColumn {
                has_default: true,
                ..
            } | DdlOpKind::ChangeColumnType { .. }
        )
    }

    /// Elapsed time since start, if started.
    pub fn elapsed_ms(&self) -> Option<u64> {
        self.started_at.map(|s| {
            let end = self.completed_at.unwrap_or_else(Instant::now);
            end.duration_since(s).as_millis() as u64
        })
    }
}

// ── Online DDL Manager ───────────────────────────────────────────────

/// Manages online DDL operations, tracking their lifecycle and providing
/// status queries. The manager itself does not execute DDL — it is a
/// coordination layer used by `StorageEngine` methods.
pub struct OnlineDdlManager {
    next_id: AtomicU64,
    /// Active and recently completed operations.
    operations: Mutex<HashMap<u64, DdlOperation>>,
    /// Maximum number of completed operations to retain for status queries.
    max_history: usize,
}

impl OnlineDdlManager {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            operations: Mutex::new(HashMap::new()),
            max_history: 100,
        }
    }

    /// Register a new DDL operation. Returns the operation ID.
    pub fn register(&self, table_id: TableId, kind: DdlOpKind) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let op = DdlOperation::new(id, table_id, kind);
        let mut ops = self.operations.lock().unwrap_or_else(|p| p.into_inner());
        ops.insert(id, op);
        self.gc_completed(&mut ops);
        id
    }

    /// Transition an operation to Running.
    pub fn start(&self, id: u64) {
        if let Some(op) = self
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&id)
        {
            op.start();
        }
    }

    /// Transition an operation to Backfilling.
    pub fn begin_backfill(&self, id: u64, total_rows: u64) {
        if let Some(op) = self
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&id)
        {
            op.begin_backfill(total_rows);
        }
    }

    /// Record backfill progress.
    pub fn record_progress(&self, id: u64, rows: u64) {
        if let Some(op) = self
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&id)
        {
            op.record_progress(rows);
        }
    }

    /// Transition an operation to Completed.
    pub fn complete(&self, id: u64) {
        if let Some(op) = self
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&id)
        {
            op.complete();
            tracing::info!(
                "Online DDL #{} completed: {} ({}ms)",
                id,
                op.kind,
                op.elapsed_ms().unwrap_or(0)
            );
        }
    }

    /// Transition an operation to Failed.
    pub fn fail(&self, id: u64, error: String) {
        if let Some(op) = self
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&id)
        {
            op.fail(error.clone());
            tracing::warn!("Online DDL #{} failed: {} — {}", id, op.kind, error);
        }
    }

    /// Get a snapshot of a specific operation.
    pub fn get(&self, id: u64) -> Option<DdlOperation> {
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&id)
            .cloned()
    }

    /// List all active (non-completed, non-failed) operations.
    pub fn list_active(&self) -> Vec<DdlOperation> {
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .filter(|op| op.phase != DdlPhase::Completed && op.phase != DdlPhase::Failed)
            .cloned()
            .collect()
    }

    /// List all operations (including completed).
    pub fn list_all(&self) -> Vec<DdlOperation> {
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Garbage-collect old completed operations beyond max_history.
    fn gc_completed(&self, ops: &mut HashMap<u64, DdlOperation>) {
        let completed: Vec<u64> = ops
            .iter()
            .filter(|(_, op)| op.phase == DdlPhase::Completed || op.phase == DdlPhase::Failed)
            .map(|(&id, _)| id)
            .collect();
        if completed.len() > self.max_history {
            let mut to_remove: Vec<u64> = completed;
            to_remove.sort();
            let remove_count = to_remove.len() - self.max_history;
            for id in to_remove.into_iter().take(remove_count) {
                ops.remove(&id);
            }
        }
    }
}

impl Default for OnlineDdlManager {
    fn default() -> Self {
        Self::new()
    }
}

// ── Backfill batch size ──────────────────────────────────────────────

/// Number of rows to process per backfill batch.
/// Keeping this moderate allows interleaving with normal DML.
pub const BACKFILL_BATCH_SIZE: usize = 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ddl_operation_lifecycle() {
        let mgr = OnlineDdlManager::new();
        let table_id = TableId(1);

        let id = mgr.register(
            table_id,
            DdlOpKind::AddColumn {
                table_name: "users".into(),
                column_name: "email".into(),
                has_default: true,
            },
        );

        // Starts as Pending
        let op = mgr.get(id).unwrap();
        assert_eq!(op.phase, DdlPhase::Pending);

        // Transition to Running
        mgr.start(id);
        let op = mgr.get(id).unwrap();
        assert_eq!(op.phase, DdlPhase::Running);
        assert!(op.started_at.is_some());

        // Transition to Backfilling
        mgr.begin_backfill(id, 1000);
        let op = mgr.get(id).unwrap();
        assert_eq!(op.phase, DdlPhase::Backfilling);
        assert_eq!(op.rows_total, 1000);

        // Record progress
        mgr.record_progress(id, 500);
        let op = mgr.get(id).unwrap();
        assert_eq!(op.rows_processed, 500);

        // Complete
        mgr.complete(id);
        let op = mgr.get(id).unwrap();
        assert_eq!(op.phase, DdlPhase::Completed);
        assert!(op.completed_at.is_some());
        let _ = op.elapsed_ms().unwrap(); // just verify it returns a value
    }

    #[test]
    fn test_ddl_operation_failure() {
        let mgr = OnlineDdlManager::new();
        let id = mgr.register(
            TableId(1),
            DdlOpKind::ChangeColumnType {
                table_name: "t".into(),
                column_name: "c".into(),
                new_type: "int".into(),
            },
        );
        mgr.start(id);
        mgr.fail(id, "type conversion error".into());

        let op = mgr.get(id).unwrap();
        assert_eq!(op.phase, DdlPhase::Failed);
        assert_eq!(op.error.as_deref(), Some("type conversion error"));
    }

    #[test]
    fn test_metadata_only_no_backfill() {
        let op = DdlOperation::new(
            1,
            TableId(1),
            DdlOpKind::MetadataOnly {
                description: "RENAME COLUMN x TO y".into(),
            },
        );
        assert!(!op.needs_backfill());
    }

    #[test]
    fn test_list_active_filters_completed() {
        let mgr = OnlineDdlManager::new();
        let id1 = mgr.register(
            TableId(1),
            DdlOpKind::MetadataOnly {
                description: "op1".into(),
            },
        );
        let id2 = mgr.register(
            TableId(1),
            DdlOpKind::MetadataOnly {
                description: "op2".into(),
            },
        );
        mgr.start(id1);
        mgr.complete(id1);
        mgr.start(id2);

        let active = mgr.list_active();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, id2);
    }

    #[test]
    fn test_gc_completed_operations() {
        let mgr = OnlineDdlManager {
            next_id: AtomicU64::new(1),
            operations: Mutex::new(HashMap::new()),
            max_history: 2,
        };

        for _ in 0..5 {
            let id = mgr.register(
                TableId(1),
                DdlOpKind::MetadataOnly {
                    description: "op".into(),
                },
            );
            mgr.start(id);
            mgr.complete(id);
        }

        // After GC, only max_history completed ops should remain
        let all = mgr.list_all();
        assert!(all.len() <= 3); // 2 completed + possible 1 from last register triggering GC
    }
}
