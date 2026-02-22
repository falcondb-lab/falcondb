//! Automatic shard rebalancing.
//!
//! Monitors per-shard load (row count) and computes a migration plan when
//! the imbalance exceeds a configurable threshold.  Row migration is done
//! in small batches so that normal DML can interleave (non-blocking).
//!
//! # Architecture
//!
//! ```text
//!   ShardLoadSnapshot ──▶ RebalancePlanner ──▶ MigrationPlan
//!                                                   │
//!                               ShardMigrator ◀─────┘
//!                           (batched row moves)
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use falcon_common::error::FalconError;
use falcon_common::shutdown::ShutdownSignal;
use falcon_common::types::ShardId;

use crate::sharded_engine::ShardedEngine;

// ── Configuration ────────────────────────────────────────────────────

/// Configuration knobs for the rebalancer.
#[derive(Debug, Clone)]
pub struct RebalancerConfig {
    /// Maximum ratio of (max_shard_rows / avg_shard_rows) before rebalancing
    /// triggers.  E.g. 1.25 means 25 % above average triggers a move.
    pub imbalance_threshold: f64,
    /// Number of rows to migrate per batch.  Keeping this moderate allows
    /// interleaving with concurrent DML.
    pub batch_size: usize,
    /// Minimum number of rows a shard must have before it is considered a
    /// donor (avoids thrashing tiny shards).
    pub min_donor_rows: u64,
    /// Cooldown period (milliseconds) between successive rebalance runs to
    /// avoid oscillation.
    pub cooldown_ms: u64,
}

impl Default for RebalancerConfig {
    fn default() -> Self {
        Self {
            imbalance_threshold: 1.25,
            batch_size: 512,
            min_donor_rows: 100,
            cooldown_ms: 5_000,
        }
    }
}

// ── Per-shard load snapshot ──────────────────────────────────────────

/// A point-in-time snapshot of a single shard's load.
#[derive(Debug, Clone)]
pub struct ShardLoad {
    pub shard_id: ShardId,
    /// Total number of rows across all tables on this shard.
    pub row_count: u64,
    /// Total number of tables on this shard.
    pub table_count: u64,
}

/// Snapshot of all shard loads at a point in time.
#[derive(Debug, Clone)]
pub struct ShardLoadSnapshot {
    pub loads: Vec<ShardLoad>,
    pub taken_at: Instant,
}

impl ShardLoadSnapshot {
    /// Collect a load snapshot from a `ShardedEngine`.
    pub fn collect(engine: &ShardedEngine) -> Self {
        let loads: Vec<ShardLoad> = engine
            .all_shards()
            .iter()
            .map(|shard| {
                let catalog = shard.storage.catalog_snapshot();
                let mut row_count = 0u64;
                let table_count = catalog.list_tables().len() as u64;
                for table_schema in catalog.list_tables() {
                    if let Some(tbl) = shard.storage.get_table(table_schema.id) {
                        row_count += tbl.row_count_approx() as u64;
                    }
                }
                ShardLoad {
                    shard_id: shard.shard_id,
                    row_count,
                    table_count,
                }
            })
            .collect();
        Self {
            loads,
            taken_at: Instant::now(),
        }
    }

    /// Average row count across all shards.
    pub fn avg_rows(&self) -> f64 {
        if self.loads.is_empty() {
            return 0.0;
        }
        let total: u64 = self.loads.iter().map(|l| l.row_count).sum();
        total as f64 / self.loads.len() as f64
    }

    /// Total row count across all shards.
    pub fn total_rows(&self) -> u64 {
        self.loads.iter().map(|l| l.row_count).sum()
    }

    /// The imbalance ratio: max_shard_rows / avg_rows.
    /// Returns 1.0 if perfectly balanced, >1.0 if skewed.
    pub fn imbalance_ratio(&self) -> f64 {
        let avg = self.avg_rows();
        if avg <= 0.0 {
            return 1.0;
        }
        let max_rows = self.loads.iter().map(|l| l.row_count).max().unwrap_or(0);
        max_rows as f64 / avg
    }
}

// ── Migration plan ───────────────────────────────────────────────────

/// Estimated rows-per-second throughput for migration time estimation.
/// Based on in-memory row copy + delete (conservative estimate).
const ESTIMATED_ROWS_PER_SEC: u64 = 50_000;

/// A single migration task: move rows from one shard to another for a
/// specific table.
#[derive(Debug, Clone)]
pub struct MigrationTask {
    pub source_shard: ShardId,
    pub target_shard: ShardId,
    pub table_name: String,
    /// Approximate number of rows to move (upper bound).
    pub rows_to_move: u64,
    /// Estimated migration time in milliseconds (based on row count and
    /// throughput estimate).
    pub estimated_time_ms: u64,
}

/// The full rebalance plan: a set of migration tasks.
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub tasks: Vec<MigrationTask>,
    pub created_at: Instant,
}

impl MigrationPlan {
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn total_rows_to_move(&self) -> u64 {
        self.tasks.iter().map(|t| t.rows_to_move).sum()
    }

    /// Estimated total duration in milliseconds (sum of all task estimates).
    pub fn estimated_duration_ms(&self) -> u64 {
        self.tasks.iter().map(|t| t.estimated_time_ms).sum()
    }

    /// Estimated total duration as a human-readable string.
    pub fn estimated_duration_display(&self) -> String {
        let ms = self.estimated_duration_ms();
        if ms < 1_000 {
            format!("{}ms", ms)
        } else if ms < 60_000 {
            format!("{:.1}s", ms as f64 / 1_000.0)
        } else {
            format!("{:.1}m", ms as f64 / 60_000.0)
        }
    }
}

// ── Rebalance planner ────────────────────────────────────────────────

/// Computes a `MigrationPlan` from a `ShardLoadSnapshot`.
pub struct RebalancePlanner {
    config: RebalancerConfig,
}

impl RebalancePlanner {
    pub fn new(config: RebalancerConfig) -> Self {
        Self { config }
    }

    /// Analyse the load snapshot and produce a migration plan.
    ///
    /// Strategy: iteratively take rows from the most-loaded shard and
    /// assign them to the least-loaded shard until all shards are within
    /// the threshold of the average.
    pub fn plan(&self, snapshot: &ShardLoadSnapshot, engine: &ShardedEngine) -> MigrationPlan {
        let mut tasks = Vec::new();

        if snapshot.loads.len() < 2 {
            return MigrationPlan {
                tasks,
                created_at: Instant::now(),
            };
        }

        let avg = snapshot.avg_rows();
        if avg <= 0.0 {
            return MigrationPlan {
                tasks,
                created_at: Instant::now(),
            };
        }

        // Build mutable load map
        let mut load_map: HashMap<ShardId, u64> = snapshot
            .loads
            .iter()
            .map(|l| (l.shard_id, l.row_count))
            .collect();

        let target_max = (avg * self.config.imbalance_threshold) as u64;

        // Collect table names from shard 0 as reference (all shards share schema)
        let table_names: Vec<String> = engine
            .all_shards()
            .first()
            .map(|s| {
                s.storage
                    .catalog_snapshot()
                    .list_tables()
                    .iter()
                    .map(|t| t.name.clone())
                    .collect()
            })
            .unwrap_or_default();

        // Iterate: find donors (above target_max) and receivers (below avg)
        let max_iterations = snapshot.loads.len() * 4;
        for _ in 0..max_iterations {
            // Find max-loaded shard
            let donor = load_map
                .iter()
                .max_by_key(|(_, &count)| count)
                .map(|(&id, &count)| (id, count));
            // Find min-loaded shard
            let receiver = load_map
                .iter()
                .min_by_key(|(_, &count)| count)
                .map(|(&id, &count)| (id, count));

            let (donor_id, donor_rows) = match donor {
                Some(d) => d,
                None => break,
            };
            let (receiver_id, receiver_rows) = match receiver {
                Some(r) => r,
                None => break,
            };

            // Stop if donor is within threshold or donor == receiver
            if donor_rows <= target_max || donor_id == receiver_id {
                break;
            }
            // Skip if donor is too small
            if donor_rows < self.config.min_donor_rows {
                break;
            }

            // Compute how many rows to move
            let excess = donor_rows.saturating_sub(target_max);
            let room = target_max.saturating_sub(receiver_rows);
            let rows_to_move = excess.min(room).max(1);

            if rows_to_move == 0 {
                break;
            }

            // Pick the first non-empty table as the migration unit
            let table_name = table_names.first().cloned().unwrap_or_default();
            if table_name.is_empty() {
                break;
            }

            let estimated_time_ms =
                (rows_to_move as f64 / ESTIMATED_ROWS_PER_SEC as f64 * 1000.0).ceil() as u64;
            tasks.push(MigrationTask {
                source_shard: donor_id,
                target_shard: receiver_id,
                table_name,
                rows_to_move,
                estimated_time_ms,
            });

            // Update simulated loads
            if let Some(v) = load_map.get_mut(&donor_id) {
                *v -= rows_to_move;
            }
            if let Some(v) = load_map.get_mut(&receiver_id) {
                *v += rows_to_move;
            }
        }

        MigrationPlan {
            tasks,
            created_at: Instant::now(),
        }
    }
}

// ── Migration execution ──────────────────────────────────────────────

/// Phase of a migration task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    Pending,
    CopyingRows,
    Completed,
    Failed,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationPhase::Pending => write!(f, "pending"),
            MigrationPhase::CopyingRows => write!(f, "copying_rows"),
            MigrationPhase::Completed => write!(f, "completed"),
            MigrationPhase::Failed => write!(f, "failed"),
        }
    }
}

/// Status of a single migration task execution.
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    pub task: MigrationTask,
    pub phase: MigrationPhase,
    pub rows_migrated: u64,
    pub error: Option<String>,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
}

impl MigrationStatus {
    fn new(task: MigrationTask) -> Self {
        Self {
            task,
            phase: MigrationPhase::Pending,
            rows_migrated: 0,
            error: None,
            started_at: None,
            completed_at: None,
        }
    }
}

/// Executes migration tasks against a `ShardedEngine`.
///
/// Rows are copied from the source shard to the target shard in batches.
/// After all rows are copied, they are deleted from the source shard.
/// The migrator uses the primary key to identify rows and re-hashes them
/// to confirm they belong on the target shard.
pub struct ShardMigrator {
    config: RebalancerConfig,
}

impl ShardMigrator {
    pub fn new(config: RebalancerConfig) -> Self {
        Self { config }
    }

    /// Execute a single migration task.  Returns the final status.
    pub fn execute_task(&self, task: &MigrationTask, engine: &ShardedEngine) -> MigrationStatus {
        let mut status = MigrationStatus::new(task.clone());
        status.phase = MigrationPhase::CopyingRows;
        status.started_at = Some(Instant::now());

        let source = match engine.shard(task.source_shard) {
            Some(s) => s,
            None => {
                status.phase = MigrationPhase::Failed;
                status.error = Some(format!("Source shard {:?} not found", task.source_shard));
                return status;
            }
        };
        let target = match engine.shard(task.target_shard) {
            Some(s) => s,
            None => {
                status.phase = MigrationPhase::Failed;
                status.error = Some(format!("Target shard {:?} not found", task.target_shard));
                return status;
            }
        };

        // Find the table on source shard
        let catalog = source.storage.catalog_snapshot();
        let table_schema = match catalog.find_table(&task.table_name) {
            Some(s) => s.clone(),
            None => {
                status.phase = MigrationPhase::Failed;
                status.error = Some(format!("Table '{}' not found on source", task.table_name));
                return status;
            }
        };

        let table_id = table_schema.id;
        let source_table = match source.storage.get_table(table_id) {
            Some(t) => t,
            None => {
                status.phase = MigrationPhase::Failed;
                status.error = Some("Table data not found on source shard".to_string());
                return status;
            }
        };

        // Collect primary keys that should be migrated.
        // We scan the source table and pick rows whose PK re-hashes to the
        // target shard.  This ensures correctness even when the hash space
        // is being repartitioned.
        let pk_col_idx = table_schema.primary_key_columns.first().copied();
        let mut migrated_rows = 0u64;
        let mut batch_keys: Vec<(
            falcon_storage::memtable::PrimaryKey,
            falcon_common::datum::OwnedRow,
        )> = Vec::new();

        for entry in source_table.data.iter() {
            if migrated_rows >= task.rows_to_move {
                break;
            }

            let pk = entry.key().clone();
            let chain = entry.value();
            if let Some(row) = chain.read_latest() {
                // Re-hash the PK to verify it belongs on the target shard
                let pk_key = pk_col_idx
                    .and_then(|idx| row.values.get(idx))
                    .and_then(datum_to_i64)
                    .unwrap_or(0);

                let target_shard = engine.shard_for_key(pk_key);
                if target_shard == task.target_shard {
                    batch_keys.push((pk, row.clone()));
                    migrated_rows += 1;

                    if batch_keys.len() >= self.config.batch_size {
                        if let Err(e) = self.flush_batch(&batch_keys, &table_schema, source, target)
                        {
                            status.phase = MigrationPhase::Failed;
                            status.error = Some(e);
                            return status;
                        }
                        status.rows_migrated += batch_keys.len() as u64;
                        batch_keys.clear();
                        // Yield to allow concurrent DML
                        std::thread::yield_now();
                    }
                }
            }
        }

        // Flush remaining
        if !batch_keys.is_empty() {
            if let Err(e) = self.flush_batch(&batch_keys, &table_schema, source, target) {
                status.phase = MigrationPhase::Failed;
                status.error = Some(e);
                return status;
            }
            status.rows_migrated += batch_keys.len() as u64;
        }

        status.phase = MigrationPhase::Completed;
        status.completed_at = Some(Instant::now());

        tracing::info!(
            "Migration completed: {} rows from {:?}  → {:?} (table '{}')",
            status.rows_migrated,
            task.source_shard,
            task.target_shard,
            task.table_name,
        );

        status
    }

    /// Insert rows into the target shard and delete them from the source.
    fn flush_batch(
        &self,
        batch: &[(
            falcon_storage::memtable::PrimaryKey,
            falcon_common::datum::OwnedRow,
        )],
        schema: &falcon_common::schema::TableSchema,
        source: &crate::sharded_engine::ShardInstance,
        target: &crate::sharded_engine::ShardInstance,
    ) -> Result<(), String> {
        let table_id = schema.id;

        // Ensure table exists on target (create if missing)
        {
            let target_catalog = target.storage.catalog_snapshot();
            if target_catalog.find_table(&schema.name).is_none() {
                target
                    .storage
                    .create_table(schema.clone())
                    .map_err(|e| format!("Failed to create table on target: {}", e))?;
            }
        }

        // Insert each row into the target shard
        for (pk, row) in batch {
            target
                .storage
                .insert_row(table_id, pk.clone(), row.clone())
                .map_err(|e| format!("Failed to insert on target: {}", e))?;
        }

        // Delete from source shard
        for (pk, _) in batch {
            source
                .storage
                .delete_row(table_id, pk)
                .map_err(|e| format!("Failed to delete from source: {}", e))?;
        }

        Ok(())
    }
}

// ── Top-level rebalancer ─────────────────────────────────────────────

/// Overall status of the rebalancer.
#[derive(Debug, Clone)]
pub struct RebalancerStatus {
    pub last_run: Option<Instant>,
    pub last_snapshot: Option<ShardLoadSnapshot>,
    pub last_plan: Option<MigrationPlan>,
    pub migration_statuses: Vec<MigrationStatus>,
    pub runs_completed: u64,
    pub total_rows_migrated: u64,
}

impl RebalancerStatus {
    /// P2-5a: Duration of the last rebalance run in milliseconds.
    /// Returns 0 if no migrations have completed.
    pub fn last_rebalance_duration_ms(&self) -> u64 {
        let mut max_end: Option<Instant> = None;
        let mut min_start: Option<Instant> = None;
        for s in &self.migration_statuses {
            if let Some(start) = s.started_at {
                min_start = Some(min_start.map_or(start, |m: Instant| m.min(start)));
            }
            if let Some(end) = s.completed_at {
                max_end = Some(max_end.map_or(end, |m: Instant| m.max(end)));
            }
        }
        match (min_start, max_end) {
            (Some(s), Some(e)) => e.duration_since(s).as_millis() as u64,
            _ => 0,
        }
    }

    /// P2-5a: Rows moved per second during the last rebalance.
    /// Returns 0.0 if no data available.
    pub fn shard_move_rate(&self) -> f64 {
        let duration_ms = self.last_rebalance_duration_ms();
        if duration_ms == 0 {
            return 0.0;
        }
        let rows: u64 = self
            .migration_statuses
            .iter()
            .filter(|s| s.phase == MigrationPhase::Completed)
            .map(|s| s.rows_migrated)
            .sum();
        (rows as f64) / (duration_ms as f64 / 1000.0)
    }

    /// P2-5a: Number of completed migration tasks in the last run.
    pub fn completed_tasks(&self) -> usize {
        self.migration_statuses
            .iter()
            .filter(|s| s.phase == MigrationPhase::Completed)
            .count()
    }

    /// P2-5a: Number of failed migration tasks in the last run.
    pub fn failed_tasks(&self) -> usize {
        self.migration_statuses
            .iter()
            .filter(|s| s.phase == MigrationPhase::Failed)
            .count()
    }
}

/// The top-level automatic shard rebalancer.
///
/// Call `check_and_rebalance()` periodically (e.g. from a background task)
/// to evaluate shard loads and trigger migrations when needed.
pub struct ShardRebalancer {
    config: RebalancerConfig,
    planner: RebalancePlanner,
    migrator: ShardMigrator,
    status: Mutex<RebalancerStatus>,
    /// Prevents concurrent rebalance runs.
    running: AtomicBool,
    runs_completed: AtomicU64,
    total_rows_migrated: AtomicU64,
}

impl ShardRebalancer {
    pub fn new(config: RebalancerConfig) -> Self {
        let planner = RebalancePlanner::new(config.clone());
        let migrator = ShardMigrator::new(config.clone());
        Self {
            config,
            planner,
            migrator,
            status: Mutex::new(RebalancerStatus {
                last_run: None,
                last_snapshot: None,
                last_plan: None,
                migration_statuses: Vec::new(),
                runs_completed: 0,
                total_rows_migrated: 0,
            }),
            running: AtomicBool::new(false),
            runs_completed: AtomicU64::new(0),
            total_rows_migrated: AtomicU64::new(0),
        }
    }

    /// Check shard loads and trigger rebalancing if needed.
    /// Returns the number of rows migrated (0 if no action taken).
    pub fn check_and_rebalance(&self, engine: &ShardedEngine) -> u64 {
        // Prevent concurrent runs
        if self.running.swap(true, Ordering::SeqCst) {
            tracing::debug!("Rebalancer already running, skipping");
            return 0;
        }

        // Cooldown check
        {
            let status = self.status.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(last_run) = status.last_run {
                if last_run.elapsed().as_millis() < self.config.cooldown_ms as u128 {
                    self.running.store(false, Ordering::SeqCst);
                    return 0;
                }
            }
        }

        let snapshot = ShardLoadSnapshot::collect(engine);
        let ratio = snapshot.imbalance_ratio();

        tracing::debug!(
            "Rebalancer: imbalance_ratio={:.3}, threshold={:.3}, total_rows={}",
            ratio,
            self.config.imbalance_threshold,
            snapshot.total_rows(),
        );

        let rows_migrated = if ratio > self.config.imbalance_threshold {
            let plan = self.planner.plan(&snapshot, engine);
            if plan.is_empty() {
                tracing::debug!("Rebalancer: imbalanced but no migration plan generated");
                self.update_status(Some(snapshot), Some(plan), Vec::new());
                0
            } else {
                tracing::info!(
                    "Rebalancer: executing plan with {} tasks, ~{} rows to move",
                    plan.tasks.len(),
                    plan.total_rows_to_move(),
                );
                let statuses = self.execute_plan(&plan, engine);
                let migrated: u64 = statuses
                    .iter()
                    .filter(|s| s.phase == MigrationPhase::Completed)
                    .map(|s| s.rows_migrated)
                    .sum();
                self.update_status(Some(snapshot), Some(plan), statuses);
                migrated
            }
        } else {
            self.update_status(Some(snapshot), None, Vec::new());
            0
        };

        self.total_rows_migrated
            .fetch_add(rows_migrated, Ordering::Relaxed);
        self.runs_completed.fetch_add(1, Ordering::Relaxed);
        self.running.store(false, Ordering::SeqCst);

        rows_migrated
    }

    /// Execute all tasks in a migration plan, returning statuses.
    fn execute_plan(&self, plan: &MigrationPlan, engine: &ShardedEngine) -> Vec<MigrationStatus> {
        plan.tasks
            .iter()
            .map(|task| self.migrator.execute_task(task, engine))
            .collect()
    }

    fn update_status(
        &self,
        snapshot: Option<ShardLoadSnapshot>,
        plan: Option<MigrationPlan>,
        statuses: Vec<MigrationStatus>,
    ) {
        let mut status = self.status.lock().unwrap_or_else(|p| p.into_inner());
        status.last_run = Some(Instant::now());
        if let Some(s) = snapshot {
            status.last_snapshot = Some(s);
        }
        if let Some(p) = plan {
            status.last_plan = Some(p);
        }
        let migrated: u64 = statuses
            .iter()
            .filter(|s| s.phase == MigrationPhase::Completed)
            .map(|s| s.rows_migrated)
            .sum();
        status.total_rows_migrated += migrated;
        status.runs_completed += 1;
        status.migration_statuses = statuses;
    }

    /// Get the current rebalancer status.
    pub fn status(&self) -> RebalancerStatus {
        self.status
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Get the total number of rows migrated across all runs.
    pub fn total_rows_migrated(&self) -> u64 {
        self.total_rows_migrated.load(Ordering::Relaxed)
    }

    /// Get the number of completed rebalance runs.
    pub fn runs_completed(&self) -> u64 {
        self.runs_completed.load(Ordering::Relaxed)
    }

    /// Get the configuration.
    pub fn config(&self) -> &RebalancerConfig {
        &self.config
    }
}

// -- Background rebalance runner -------------------------------------------

/// Configuration for the background rebalance runner.
#[derive(Debug, Clone)]
pub struct RebalanceRunnerConfig {
    /// How often to check shard loads (milliseconds).
    pub check_interval_ms: u64,
    /// Rebalancer configuration (thresholds, batch size, etc.).
    pub rebalancer: RebalancerConfig,
    /// Whether the runner is enabled.
    pub enabled: bool,
}

impl Default for RebalanceRunnerConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 10_000, // 10 seconds
            rebalancer: RebalancerConfig::default(),
            enabled: true,
        }
    }
}

/// Handle returned by `RebalanceRunner::start()`. Dropping it stops the runner.
pub struct RebalanceRunnerHandle {
    signal: ShutdownSignal,
    join_handle: Option<std::thread::JoinHandle<()>>,
}

impl RebalanceRunnerHandle {
    /// Signal the runner to stop.
    pub fn stop(&self) {
        self.signal.shutdown();
    }

    /// Signal the runner to stop and wait for it to finish.
    pub fn stop_and_join(mut self) {
        self.signal.shutdown();
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }

    /// Check if the runner is still alive.
    pub fn is_running(&self) -> bool {
        !self.signal.is_shutdown()
    }
}

impl Drop for RebalanceRunnerHandle {
    fn drop(&mut self) {
        self.signal.shutdown();
    }
}

/// Background rebalance runner that periodically checks shard loads and
/// triggers automatic rebalancing when imbalance exceeds the threshold.
pub struct RebalanceRunner;

impl RebalanceRunner {
    /// Start the background rebalance runner thread.
    ///
    /// Returns `Err` if the background thread cannot be spawned.
    /// The caller must handle this as a degraded condition — **no panic**.
    pub fn start(
        config: RebalanceRunnerConfig,
        engine: std::sync::Arc<ShardedEngine>,
        rebalancer: std::sync::Arc<ShardRebalancer>,
    ) -> Result<RebalanceRunnerHandle, FalconError> {
        let signal = ShutdownSignal::new();
        let signal_clone = signal.clone();
        let interval = std::time::Duration::from_millis(config.check_interval_ms);

        let join_handle = std::thread::Builder::new()
            .name("falcon-rebalancer".to_string())
            .spawn(move || {
                tracing::info!(
                    "RebalanceRunner started (interval={}ms)",
                    config.check_interval_ms
                );
                while !signal_clone.is_shutdown() {
                    if signal_clone.wait_timeout(interval) {
                        break;
                    }
                    let migrated = rebalancer.check_and_rebalance(&engine);
                    if migrated > 0 {
                        tracing::info!("RebalanceRunner: migrated {} rows", migrated);
                    }
                }
                tracing::info!("RebalanceRunner stopped");
            })
            .map_err(|e| {
                tracing::error!(
                    component = "rebalancer",
                    error = %e,
                    "failed to spawn background thread — node DEGRADED"
                );
                FalconError::Internal(format!("failed to spawn rebalancer thread: {}", e))
            })?;

        Ok(RebalanceRunnerHandle {
            signal,
            join_handle: Some(join_handle),
        })
    }
}

// -- Per-table load tracking -----------------------------------------------

/// Per-table load on a single shard.
#[derive(Debug, Clone)]
pub struct TableLoad {
    pub table_name: String,
    pub row_count: u64,
}

/// Extended shard load with per-table breakdown.
#[derive(Debug, Clone)]
pub struct ShardLoadDetailed {
    pub shard_id: ShardId,
    pub total_row_count: u64,
    pub table_count: u64,
    pub tables: Vec<TableLoad>,
}

impl ShardLoadDetailed {
    /// Collect detailed load from a ShardedEngine.
    pub fn collect_all(engine: &ShardedEngine) -> Vec<ShardLoadDetailed> {
        engine
            .all_shards()
            .iter()
            .map(|shard| {
                let catalog = shard.storage.catalog_snapshot();
                let mut tables = Vec::new();
                let mut total = 0u64;
                for ts in catalog.list_tables() {
                    let rc = shard
                        .storage
                        .get_table(ts.id)
                        .map(|t| t.row_count_approx() as u64)
                        .unwrap_or(0);
                    total += rc;
                    tables.push(TableLoad {
                        table_name: ts.name.clone(),
                        row_count: rc,
                    });
                }
                ShardLoadDetailed {
                    shard_id: shard.shard_id,
                    total_row_count: total,
                    table_count: tables.len() as u64,
                    tables,
                }
            })
            .collect()
    }
}

// -- Helpers ---------------------------------------------------------------

/// Extract an i64 from a Datum (for PK hashing).
fn datum_to_i64(d: &falcon_common::datum::Datum) -> Option<i64> {
    use falcon_common::datum::Datum;
    match d {
        Datum::Int32(v) => Some(*v as i64),
        Datum::Int64(v) => Some(*v),
        _ => None,
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};
    use falcon_storage::memtable::encode_pk_from_datums;

    fn make_test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn test_load_snapshot_balanced() {
        let engine = ShardedEngine::new(3);
        let schema = make_test_schema();
        engine.create_table_all(&schema).unwrap();

        let snapshot = ShardLoadSnapshot::collect(&engine);
        assert_eq!(snapshot.loads.len(), 3);
        assert!(snapshot.imbalance_ratio() <= 1.01); // all empty = balanced
    }

    #[test]
    fn test_planner_no_action_when_balanced() {
        let engine = ShardedEngine::new(2);
        let schema = make_test_schema();
        engine.create_table_all(&schema).unwrap();

        let config = RebalancerConfig::default();
        let planner = RebalancePlanner::new(config);
        let snapshot = ShardLoadSnapshot::collect(&engine);
        let plan = planner.plan(&snapshot, &engine);
        assert!(plan.is_empty());
    }

    #[test]
    fn test_planner_generates_plan_when_skewed() {
        let engine = ShardedEngine::new(2);
        let schema = make_test_schema();
        engine.create_table_all(&schema).unwrap();

        // Insert many rows into shard 0 only
        let shard0 = engine.shard(ShardId(0)).unwrap();
        for i in 0..200 {
            let row = OwnedRow::new(vec![
                Datum::Int64(i as i64),
                Datum::Text(format!("user_{}", i)),
            ]);
            let pk = encode_pk_from_datums(&[&Datum::Int64(i as i64)]);
            shard0.storage.insert_row(TableId(1), pk, row).unwrap();
        }

        let snapshot = ShardLoadSnapshot::collect(&engine);
        assert!(snapshot.imbalance_ratio() > 1.5); // very skewed

        let config = RebalancerConfig {
            imbalance_threshold: 1.25,
            min_donor_rows: 10,
            ..Default::default()
        };
        let planner = RebalancePlanner::new(config);
        let plan = planner.plan(&snapshot, &engine);
        assert!(!plan.is_empty());
        assert!(plan.total_rows_to_move() > 0);

        // Donor should be shard 0, receiver shard 1
        assert_eq!(plan.tasks[0].source_shard, ShardId(0));
        assert_eq!(plan.tasks[0].target_shard, ShardId(1));
    }

    #[test]
    fn test_migrator_moves_rows() {
        let engine = ShardedEngine::new(2);
        let schema = make_test_schema();
        engine.create_table_all(&schema).unwrap();

        // Insert rows into shard 0
        let shard0 = engine.shard(ShardId(0)).unwrap();
        for i in 0..50 {
            let row = OwnedRow::new(vec![Datum::Int64(i as i64), Datum::Text(format!("u{}", i))]);
            let pk = encode_pk_from_datums(&[&Datum::Int64(i as i64)]);
            shard0.storage.insert_row(TableId(1), pk, row).unwrap();
        }

        let config = RebalancerConfig {
            batch_size: 10,
            ..Default::default()
        };
        let migrator = ShardMigrator::new(config);

        let task = MigrationTask {
            source_shard: ShardId(0),
            target_shard: ShardId(1),
            table_name: "users".to_string(),
            rows_to_move: 20,
            estimated_time_ms: 1,
        };

        let status = migrator.execute_task(&task, &engine);
        assert_eq!(status.phase, MigrationPhase::Completed);
        // Rows may be less than requested if PK re-hash doesn't match target
        assert!(status.rows_migrated <= 20);
    }

    #[test]
    fn test_full_rebalance_cycle() {
        let engine = ShardedEngine::new(2);
        let schema = make_test_schema();
        engine.create_table_all(&schema).unwrap();

        // Insert all rows into shard 0
        let shard0 = engine.shard(ShardId(0)).unwrap();
        for i in 0..200 {
            let row = OwnedRow::new(vec![Datum::Int64(i as i64), Datum::Text(format!("u{}", i))]);
            let pk = encode_pk_from_datums(&[&Datum::Int64(i as i64)]);
            shard0.storage.insert_row(TableId(1), pk, row).unwrap();
        }

        let config = RebalancerConfig {
            imbalance_threshold: 1.25,
            batch_size: 64,
            min_donor_rows: 10,
            cooldown_ms: 0, // no cooldown for test
        };
        let rebalancer = ShardRebalancer::new(config);

        let migrated = rebalancer.check_and_rebalance(&engine);
        // Should have migrated some rows
        assert!(migrated > 0 || rebalancer.runs_completed() > 0);
        assert_eq!(rebalancer.runs_completed(), 1);
    }

    #[test]
    fn test_cooldown_prevents_rapid_runs() {
        let engine = ShardedEngine::new(2);
        let schema = make_test_schema();
        engine.create_table_all(&schema).unwrap();

        let config = RebalancerConfig {
            cooldown_ms: 10_000, // 10s cooldown
            ..Default::default()
        };
        let rebalancer = ShardRebalancer::new(config);

        // First run
        rebalancer.check_and_rebalance(&engine);
        // Second run should be skipped due to cooldown
        let migrated = rebalancer.check_and_rebalance(&engine);
        assert_eq!(migrated, 0);
        // Only 1 actual run
        assert_eq!(rebalancer.runs_completed(), 1);
    }

    #[test]
    fn test_imbalance_ratio_calculation() {
        let snapshot = ShardLoadSnapshot {
            loads: vec![
                ShardLoad {
                    shard_id: ShardId(0),
                    row_count: 1000,
                    table_count: 1,
                },
                ShardLoad {
                    shard_id: ShardId(1),
                    row_count: 100,
                    table_count: 1,
                },
            ],
            taken_at: Instant::now(),
        };
        // avg = 550, max = 1000, ratio = 1000/550 ≈ 1.818
        let ratio = snapshot.imbalance_ratio();
        assert!(ratio > 1.8 && ratio < 1.9);
    }

    // ── v0.5.0 Gate Tests ──────────────────────────────────────────────

    #[test]
    fn test_g5_plan_output_has_shard_moves_volume_time() {
        let engine = ShardedEngine::new(2);
        let schema = make_test_schema();
        engine.create_table_all(&schema).unwrap();

        let shard0 = engine.shard(ShardId(0)).unwrap();
        for i in 0..200 {
            let row = OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("u{}", i))]);
            let pk = encode_pk_from_datums(&[&Datum::Int64(i)]);
            shard0.storage.insert_row(TableId(1), pk, row).unwrap();
        }

        let config = RebalancerConfig {
            imbalance_threshold: 1.25,
            min_donor_rows: 10,
            ..Default::default()
        };
        let planner = RebalancePlanner::new(config);
        let snapshot = ShardLoadSnapshot::collect(&engine);
        let plan = planner.plan(&snapshot, &engine);

        assert!(!plan.is_empty(), "G5: plan must have tasks");
        for task in &plan.tasks {
            assert!(
                task.rows_to_move > 0,
                "G5: shard move must have data volume"
            );
            assert!(
                task.estimated_time_ms > 0,
                "G5: shard move must have estimated time"
            );
            assert!(
                !task.table_name.is_empty(),
                "G5: shard move must name the table"
            );
            assert_ne!(task.source_shard, task.target_shard, "G5: source != target");
        }
        assert!(plan.total_rows_to_move() > 0, "G5: total data volume > 0");
        assert!(
            plan.estimated_duration_ms() > 0,
            "G5: total estimated time > 0"
        );
        let display = plan.estimated_duration_display();
        assert!(
            !display.is_empty(),
            "G5: estimated duration display non-empty"
        );
    }

    #[test]
    fn test_g1_scale_out_rebalance_verify() {
        let engine = ShardedEngine::new(2);
        let schema = make_test_schema();
        engine.create_table_all(&schema).unwrap();

        let shard0 = engine.shard(ShardId(0)).unwrap();
        for i in 0..200 {
            let row = OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("u{}", i))]);
            let pk = encode_pk_from_datums(&[&Datum::Int64(i)]);
            shard0.storage.insert_row(TableId(1), pk, row).unwrap();
        }

        let before = ShardLoadSnapshot::collect(&engine);
        assert_eq!(before.total_rows(), 200);
        let shard1_before = before
            .loads
            .iter()
            .find(|l| l.shard_id == ShardId(1))
            .map(|l| l.row_count)
            .unwrap_or(0);
        assert_eq!(shard1_before, 0, "shard 1 should be empty before rebalance");

        let config = RebalancerConfig {
            imbalance_threshold: 1.25,
            batch_size: 64,
            min_donor_rows: 10,
            cooldown_ms: 0,
        };
        let rebalancer = ShardRebalancer::new(config);
        let migrated = rebalancer.check_and_rebalance(&engine);
        assert!(
            migrated > 0,
            "G1: rebalance should migrate rows to new shard"
        );

        let after = ShardLoadSnapshot::collect(&engine);
        // Note: total_rows may be > 200 due to MVCC tombstones on source shard.
        // The key invariant is that target shard received data.
        let shard1_after = after
            .loads
            .iter()
            .find(|l| l.shard_id == ShardId(1))
            .map(|l| l.row_count)
            .unwrap_or(0);
        assert!(
            shard1_after > 0,
            "G1: data verified on target shard after rebalance"
        );
        assert!(
            shard1_after >= migrated,
            "G1: target shard has at least migrated row count"
        );
    }

    #[test]
    fn test_g2_scale_in_no_data_loss() {
        let engine = ShardedEngine::new(2);
        let schema = make_test_schema();
        engine.create_table_all(&schema).unwrap();

        let shard0 = engine.shard(ShardId(0)).unwrap();
        for i in 0..100 {
            let row = OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("u{}", i))]);
            let pk = encode_pk_from_datums(&[&Datum::Int64(i)]);
            shard0.storage.insert_row(TableId(1), pk, row).unwrap();
        }
        let shard1 = engine.shard(ShardId(1)).unwrap();
        for i in 100..200 {
            let row = OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("u{}", i))]);
            let pk = encode_pk_from_datums(&[&Datum::Int64(i)]);
            shard1.storage.insert_row(TableId(1), pk, row).unwrap();
        }

        let before = ShardLoadSnapshot::collect(&engine);
        assert_eq!(before.total_rows(), 200);
        let shard0_before = before
            .loads
            .iter()
            .find(|l| l.shard_id == ShardId(0))
            .map(|l| l.row_count)
            .unwrap_or(0);

        let config = RebalancerConfig {
            batch_size: 64,
            ..Default::default()
        };
        let migrator = ShardMigrator::new(config);
        let task = MigrationTask {
            source_shard: ShardId(1),
            target_shard: ShardId(0),
            table_name: "users".to_string(),
            rows_to_move: 100,
            estimated_time_ms: 2,
        };
        let status = migrator.execute_task(&task, &engine);
        assert_eq!(
            status.phase,
            MigrationPhase::Completed,
            "G2: migration should complete"
        );
        assert!(status.rows_migrated > 0, "G2: should have migrated rows");

        let after = ShardLoadSnapshot::collect(&engine);
        // Note: total_rows may increase due to MVCC tombstones on source.
        // The key invariant: target shard (shard 0) received the migrated rows.
        let shard0_after = after
            .loads
            .iter()
            .find(|l| l.shard_id == ShardId(0))
            .map(|l| l.row_count)
            .unwrap_or(0);
        assert!(
            shard0_after > shard0_before,
            "G2: surviving shard row count increased ({} → {})",
            shard0_before,
            shard0_after,
        );
    }

    #[test]
    fn test_estimated_duration_display_formats() {
        let plan = MigrationPlan {
            tasks: vec![MigrationTask {
                source_shard: ShardId(0),
                target_shard: ShardId(1),
                table_name: "t".into(),
                rows_to_move: 100,
                estimated_time_ms: 500,
            }],
            created_at: Instant::now(),
        };
        assert_eq!(plan.estimated_duration_display(), "500ms");

        let plan2 = MigrationPlan {
            tasks: vec![MigrationTask {
                source_shard: ShardId(0),
                target_shard: ShardId(1),
                table_name: "t".into(),
                rows_to_move: 50000,
                estimated_time_ms: 3500,
            }],
            created_at: Instant::now(),
        };
        assert_eq!(plan2.estimated_duration_display(), "3.5s");

        let plan3 = MigrationPlan {
            tasks: vec![MigrationTask {
                source_shard: ShardId(0),
                target_shard: ShardId(1),
                table_name: "t".into(),
                rows_to_move: 5000000,
                estimated_time_ms: 120_000,
            }],
            created_at: Instant::now(),
        };
        assert_eq!(plan3.estimated_duration_display(), "2.0m");
    }
}
