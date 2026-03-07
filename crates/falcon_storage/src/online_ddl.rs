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
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Backfilling => write!(f, "backfilling"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
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
            Self::AddColumn {
                table_name,
                column_name,
                ..
            } => {
                write!(f, "ADD COLUMN {table_name}.{column_name}")
            }
            Self::DropColumn {
                table_name,
                column_name,
            } => {
                write!(f, "DROP COLUMN {table_name}.{column_name}")
            }
            Self::ChangeColumnType {
                table_name,
                column_name,
                new_type,
            } => {
                write!(f, "ALTER COLUMN {table_name}.{column_name} TYPE {new_type}")
            }
            Self::MetadataOnly { description } => {
                write!(f, "{description}")
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
    const fn new(id: u64, table_id: TableId, kind: DdlOpKind) -> Self {
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
    pub const fn begin_backfill(&mut self, total_rows: u64) {
        self.phase = DdlPhase::Backfilling;
        self.rows_total = total_rows;
        self.rows_processed = 0;
    }

    /// Record backfill progress.
    pub const fn record_progress(&mut self, rows: u64) {
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
    pub const fn needs_backfill(&self) -> bool {
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
        let mut ops = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ops.insert(id, op);
        self.gc_completed(&mut ops);
        id
    }

    /// Transition an operation to Running.
    pub fn start(&self, id: u64) {
        if let Some(op) = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&id)
            .cloned()
    }

    /// List all active (non-completed, non-failed) operations.
    pub fn list_active(&self) -> Vec<DdlOperation> {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|op| op.phase != DdlPhase::Completed && op.phase != DdlPhase::Failed)
            .cloned()
            .collect()
    }

    /// List all operations (including completed).
    pub fn list_all(&self) -> Vec<DdlOperation> {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

// ── Progress snapshot for observability ──────────────────────────────

/// Point-in-time progress snapshot for a DDL operation.
/// Suitable for Prometheus metrics export or admin API responses.
#[derive(Debug, Clone)]
pub struct DdlProgress {
    pub id: u64,
    pub description: String,
    pub phase: DdlPhase,
    pub rows_processed: u64,
    pub rows_total: u64,
    /// Progress percentage (0.0 – 100.0). Returns 100.0 for completed ops.
    pub pct: f64,
    pub elapsed_ms: Option<u64>,
    pub error: Option<String>,
}

impl DdlProgress {
    pub fn from_op(op: &DdlOperation) -> Self {
        let pct = if op.phase == DdlPhase::Completed {
            100.0
        } else if op.rows_total > 0 {
            (op.rows_processed as f64 / op.rows_total as f64 * 100.0).min(100.0)
        } else {
            0.0
        };
        Self {
            id: op.id,
            description: format!("{}", op.kind),
            phase: op.phase,
            rows_processed: op.rows_processed,
            rows_total: op.rows_total,
            pct,
            elapsed_ms: op.elapsed_ms(),
            error: op.error.clone(),
        }
    }
}

impl OnlineDdlManager {
    /// Get progress snapshots for all tracked operations (active + completed/failed).
    pub fn all_progress(&self) -> Vec<DdlProgress> {
        let mut ops: Vec<DdlProgress> = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(DdlProgress::from_op)
            .collect();
        ops.sort_by_key(|p| p.id);
        ops
    }

    /// Get progress snapshots for all active operations.
    pub fn active_progress(&self) -> Vec<DdlProgress> {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|op| op.phase != DdlPhase::Completed && op.phase != DdlPhase::Failed)
            .map(DdlProgress::from_op)
            .collect()
    }

    /// Get progress snapshot for a specific operation.
    pub fn progress(&self, id: u64) -> Option<DdlProgress> {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&id)
            .map(DdlProgress::from_op)
    }

    /// Check if any DDL operation is currently running on a specific table.
    pub fn is_table_locked(&self, table_id: TableId) -> bool {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .any(|op| {
                op.table_id == table_id
                    && matches!(op.phase, DdlPhase::Running | DdlPhase::Backfilling)
            })
    }

    /// Cancel a running or backfilling operation (marks as Failed).
    /// Returns true if the operation was found and cancelled.
    pub fn cancel(&self, id: u64) -> bool {
        if let Some(op) = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&id)
        {
            if matches!(
                op.phase,
                DdlPhase::Running | DdlPhase::Backfilling | DdlPhase::Pending
            ) {
                op.fail("Cancelled by user".into());
                tracing::info!("Online DDL #{} cancelled: {}", id, op.kind);
                return true;
            }
        }
        false
    }

    /// Summary metrics for all tracked operations.
    pub fn metrics_summary(&self) -> DdlMetricsSummary {
        let ops = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut summary = DdlMetricsSummary::default();
        for op in ops.values() {
            match op.phase {
                DdlPhase::Pending => summary.pending += 1,
                DdlPhase::Running => summary.running += 1,
                DdlPhase::Backfilling => {
                    summary.backfilling += 1;
                    summary.total_rows_backfilling += op.rows_total;
                    summary.rows_processed_backfilling += op.rows_processed;
                }
                DdlPhase::Completed => summary.completed += 1,
                DdlPhase::Failed => summary.failed += 1,
            }
        }
        summary
    }
}

/// Summary metrics across all DDL operations.
#[derive(Debug, Clone, Default)]
pub struct DdlMetricsSummary {
    pub pending: usize,
    pub running: usize,
    pub backfilling: usize,
    pub completed: usize,
    pub failed: usize,
    pub total_rows_backfilling: u64,
    pub rows_processed_backfilling: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// Background DDL Executor
// ═══════════════════════════════════════════════════════════════════════════

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// A cancellation token for a submitted DDL background task.
/// Dropping the token does NOT cancel the task — call `cancel()` explicitly.
#[derive(Clone)]
pub struct DdlTaskHandle {
    cancelled: Arc<AtomicBool>,
    ddl_id: u64,
}

impl DdlTaskHandle {
    /// Signal the background task to stop at the next batch boundary.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Check whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// The DDL operation ID this handle controls.
    pub fn ddl_id(&self) -> u64 {
        self.ddl_id
    }
}

/// A boxed backfill closure: `(ddl_id, cancelled_flag) -> Result<(), String>`.
/// The closure should check `cancelled.load(Relaxed)` between batches and
/// return early if true.
pub type BackfillFn = Box<dyn FnOnce(u64, Arc<AtomicBool>) -> Result<(), String> + Send + 'static>;

/// Background executor for DDL backfill tasks.
///
/// Runs backfill closures on a dedicated thread pool so that
/// `ALTER TABLE ADD COLUMN ... DEFAULT ...` returns immediately.
/// The `OnlineDdlManager` is updated with progress/completion/failure
/// from within the backfill closure.
pub struct DdlBackgroundExecutor {
    /// Sender side of the task channel.
    tx: Mutex<Option<std::sync::mpsc::Sender<BackfillTask>>>,
    /// Worker join handles (for graceful shutdown).
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    /// Number of tasks currently in-flight.
    inflight: AtomicU64,
}

struct BackfillTask {
    ddl_id: u64,
    cancelled: Arc<AtomicBool>,
    work: BackfillFn,
    manager: Arc<OnlineDdlManager>,
}

impl DdlBackgroundExecutor {
    /// Create a new executor with `num_threads` background workers.
    pub fn new(num_threads: usize) -> Arc<Self> {
        let (tx, rx) = std::sync::mpsc::channel::<BackfillTask>();
        let rx = Arc::new(Mutex::new(rx));

        let mut workers = Vec::with_capacity(num_threads);
        for i in 0..num_threads {
            let rx = rx.clone();
            let handle = std::thread::Builder::new()
                .name(format!("ddl-backfill-{i}"))
                .spawn(move || {
                    loop {
                        let task = {
                            let guard =
                                rx.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            match guard.recv() {
                                Ok(task) => task,
                                Err(_) => break, // channel closed → shutdown
                            }
                        };

                        let ddl_id = task.ddl_id;
                        if task.cancelled.load(Ordering::Acquire) {
                            task.manager.fail(ddl_id, "Cancelled before start".into());
                            continue;
                        }

                        tracing::info!(ddl_id, "DdlBackgroundExecutor: starting backfill");
                        match (task.work)(ddl_id, task.cancelled.clone()) {
                            Ok(()) => {
                                if task.cancelled.load(Ordering::Relaxed) {
                                    task.manager
                                        .fail(ddl_id, "Cancelled during backfill".into());
                                } else {
                                    task.manager.complete(ddl_id);
                                }
                            }
                            Err(e) => {
                                task.manager.fail(ddl_id, e);
                            }
                        }
                        tracing::info!(ddl_id, "DdlBackgroundExecutor: backfill finished");
                    }
                })
                .unwrap_or_else(|e| {
                    tracing::error!("failed to spawn DDL backfill thread {i}: {e}");
                    // Return a dummy thread that immediately exits so the Vec stays consistent
                    std::thread::spawn(|| {})
                });
            workers.push(handle);
        }

        Arc::new(Self {
            tx: Mutex::new(Some(tx)),
            workers: Mutex::new(workers),
            inflight: AtomicU64::new(0),
        })
    }

    /// Submit a backfill task to run in the background.
    ///
    /// Returns a `DdlTaskHandle` that can be used to cancel the task.
    /// The `manager` is used to update DDL operation status on completion/failure.
    pub fn submit(
        &self,
        ddl_id: u64,
        manager: Arc<OnlineDdlManager>,
        work: BackfillFn,
    ) -> Result<DdlTaskHandle, String> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let handle = DdlTaskHandle {
            cancelled: cancelled.clone(),
            ddl_id,
        };

        let task = BackfillTask {
            ddl_id,
            cancelled,
            work,
            manager,
        };

        let guard = self
            .tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tx = guard.as_ref().ok_or("Executor has been shut down")?;
        tx.send(task)
            .map_err(|_| "Executor channel closed".to_string())?;
        self.inflight.fetch_add(1, Ordering::Relaxed);

        Ok(handle)
    }

    /// Number of tasks currently queued or running.
    pub fn inflight_count(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }

    /// Shut down the executor: drop the sender so workers exit, then join all threads.
    pub fn shutdown(&self) {
        {
            let mut guard = self
                .tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard = None; // drop sender → workers will see channel closed
        }
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for handle in workers.drain(..) {
            let _ = handle.join();
        }
    }
}

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

    // ── DdlProgress tests ──────────────────────────────────────────

    #[test]
    fn test_ddl_progress_snapshot() {
        let mgr = OnlineDdlManager::new();
        let id = mgr.register(
            TableId(1),
            DdlOpKind::AddColumn {
                table_name: "users".into(),
                column_name: "email".into(),
                has_default: true,
            },
        );
        mgr.start(id);
        mgr.begin_backfill(id, 1000);
        mgr.record_progress(id, 250);

        let progress = mgr.progress(id).unwrap();
        assert_eq!(progress.phase, DdlPhase::Backfilling);
        assert_eq!(progress.rows_processed, 250);
        assert_eq!(progress.rows_total, 1000);
        assert!((progress.pct - 25.0).abs() < 0.1);
    }

    #[test]
    fn test_ddl_progress_completed_is_100_pct() {
        let mgr = OnlineDdlManager::new();
        let id = mgr.register(
            TableId(1),
            DdlOpKind::MetadataOnly {
                description: "test".into(),
            },
        );
        mgr.start(id);
        mgr.complete(id);

        let progress = mgr.progress(id).unwrap();
        assert!((progress.pct - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_active_progress_filters() {
        let mgr = OnlineDdlManager::new();
        let id1 = mgr.register(
            TableId(1),
            DdlOpKind::MetadataOnly {
                description: "op1".into(),
            },
        );
        let id2 = mgr.register(
            TableId(2),
            DdlOpKind::MetadataOnly {
                description: "op2".into(),
            },
        );
        mgr.start(id1);
        mgr.complete(id1);
        mgr.start(id2);

        let active = mgr.active_progress();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, id2);
    }

    #[test]
    fn test_is_table_locked() {
        let mgr = OnlineDdlManager::new();
        assert!(!mgr.is_table_locked(TableId(1)));

        let id = mgr.register(
            TableId(1),
            DdlOpKind::AddColumn {
                table_name: "t".into(),
                column_name: "c".into(),
                has_default: true,
            },
        );
        // Pending doesn't lock
        assert!(!mgr.is_table_locked(TableId(1)));

        mgr.start(id);
        assert!(mgr.is_table_locked(TableId(1)));
        assert!(!mgr.is_table_locked(TableId(2)));

        mgr.complete(id);
        assert!(!mgr.is_table_locked(TableId(1)));
    }

    #[test]
    fn test_cancel_operation() {
        let mgr = OnlineDdlManager::new();
        let id = mgr.register(
            TableId(1),
            DdlOpKind::AddColumn {
                table_name: "t".into(),
                column_name: "c".into(),
                has_default: true,
            },
        );
        mgr.start(id);
        mgr.begin_backfill(id, 1000);

        assert!(mgr.cancel(id));
        let op = mgr.get(id).unwrap();
        assert_eq!(op.phase, DdlPhase::Failed);
        assert_eq!(op.error.as_deref(), Some("Cancelled by user"));

        // Can't cancel already-failed
        assert!(!mgr.cancel(id));
    }

    #[test]
    fn test_cancel_completed_returns_false() {
        let mgr = OnlineDdlManager::new();
        let id = mgr.register(
            TableId(1),
            DdlOpKind::MetadataOnly {
                description: "done".into(),
            },
        );
        mgr.start(id);
        mgr.complete(id);

        assert!(!mgr.cancel(id));
    }

    #[test]
    fn test_metrics_summary() {
        let mgr = OnlineDdlManager::new();

        let id1 = mgr.register(
            TableId(1),
            DdlOpKind::MetadataOnly {
                description: "op1".into(),
            },
        );
        let id2 = mgr.register(
            TableId(2),
            DdlOpKind::AddColumn {
                table_name: "t".into(),
                column_name: "c".into(),
                has_default: true,
            },
        );
        let id3 = mgr.register(
            TableId(3),
            DdlOpKind::MetadataOnly {
                description: "op3".into(),
            },
        );

        mgr.start(id1);
        mgr.complete(id1);
        mgr.start(id2);
        mgr.begin_backfill(id2, 500);
        mgr.record_progress(id2, 200);
        mgr.start(id3);
        mgr.fail(id3, "err".into());

        let summary = mgr.metrics_summary();
        assert_eq!(summary.completed, 1);
        assert_eq!(summary.backfilling, 1);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.total_rows_backfilling, 500);
        assert_eq!(summary.rows_processed_backfilling, 200);
    }

    // ── DdlBackgroundExecutor tests ──────────────────────────────────

    #[test]
    fn test_background_executor_submit_and_complete() {
        let mgr = Arc::new(OnlineDdlManager::new());
        let executor = DdlBackgroundExecutor::new(1);

        let ddl_id = mgr.register(
            TableId(1),
            DdlOpKind::AddColumn {
                table_name: "t".into(),
                column_name: "c".into(),
                has_default: true,
            },
        );
        mgr.start(ddl_id);
        mgr.begin_backfill(ddl_id, 100);

        let mgr2 = mgr.clone();
        let handle = executor
            .submit(
                ddl_id,
                mgr.clone(),
                Box::new(move |id, _cancelled| {
                    // Simulate backfill work
                    mgr2.record_progress(id, 100);
                    Ok(())
                }),
            )
            .unwrap();

        // Wait for completion
        std::thread::sleep(std::time::Duration::from_millis(200));

        let op = mgr.get(handle.ddl_id()).unwrap();
        assert_eq!(op.phase, DdlPhase::Completed);
        assert_eq!(op.rows_processed, 100);

        executor.shutdown();
    }

    #[test]
    fn test_background_executor_cancellation() {
        let mgr = Arc::new(OnlineDdlManager::new());
        let executor = DdlBackgroundExecutor::new(1);

        let ddl_id = mgr.register(
            TableId(1),
            DdlOpKind::AddColumn {
                table_name: "t".into(),
                column_name: "c".into(),
                has_default: true,
            },
        );
        mgr.start(ddl_id);
        mgr.begin_backfill(ddl_id, 10_000);

        let mgr2 = mgr.clone();
        let handle = executor
            .submit(
                ddl_id,
                mgr.clone(),
                Box::new(move |id, cancelled| {
                    for batch in 0..100 {
                        if cancelled.load(Ordering::Relaxed) {
                            return Ok(()); // early exit on cancel
                        }
                        mgr2.record_progress(id, 100);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        let _ = batch;
                    }
                    Ok(())
                }),
            )
            .unwrap();

        // Cancel after a short delay
        std::thread::sleep(std::time::Duration::from_millis(30));
        handle.cancel();
        assert!(handle.is_cancelled());

        // Wait for the worker to observe cancellation
        std::thread::sleep(std::time::Duration::from_millis(200));

        let op = mgr.get(ddl_id).unwrap();
        assert_eq!(op.phase, DdlPhase::Failed);
        assert!(op.error.as_deref().unwrap().contains("Cancelled"));

        executor.shutdown();
    }

    #[test]
    fn test_background_executor_failure() {
        let mgr = Arc::new(OnlineDdlManager::new());
        let executor = DdlBackgroundExecutor::new(1);

        let ddl_id = mgr.register(
            TableId(1),
            DdlOpKind::ChangeColumnType {
                table_name: "t".into(),
                column_name: "c".into(),
                new_type: "int".into(),
            },
        );
        mgr.start(ddl_id);

        let _handle = executor
            .submit(
                ddl_id,
                mgr.clone(),
                Box::new(|_id, _cancelled| Err("type conversion error".into())),
            )
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(200));

        let op = mgr.get(ddl_id).unwrap();
        assert_eq!(op.phase, DdlPhase::Failed);
        assert_eq!(op.error.as_deref(), Some("type conversion error"));

        executor.shutdown();
    }

    #[test]
    fn test_background_executor_shutdown() {
        let executor = DdlBackgroundExecutor::new(2);
        // Shutdown without submitting any tasks should not panic
        executor.shutdown();
    }
}
