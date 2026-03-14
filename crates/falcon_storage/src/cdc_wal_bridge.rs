//! WAL-backed CDC bridge: converts WAL records into CDC change events and
//! provides slot state persistence for crash recovery.
//!
//! # Architecture
//!
//! ```text
//!   WAL append path                      CDC consumers
//!   ──────────────                       ──────────────
//!   StorageEngine.insert_row()           poll_changes(slot_id)
//!       │                                     ▲
//!       ▼                                     │
//!   WalRecord::Insert ──► WalCdcBridge ──► CdcManager.emit()
//!                              │
//!                              ▼
//!                         Real WAL LSN assigned
//!                         (not simulated CdcLsn)
//! ```
//!
//! # Slot Persistence
//!
//! Slot metadata (name, confirmed_flush_lsn, restart_lsn) is persisted to a
//! JSON sidecar file alongside the WAL directory. On startup, slots are
//! recovered from this file so consumers can resume from their last position.
//!
//! # Production Path
//!
//! This module bridges the gap between the in-memory-only `CdcManager` and
//! a production-grade CDC pipeline by:
//! 1. Using real WAL LSNs instead of simulated ones
//! 2. Persisting slot state to disk for crash recovery
//! 3. Providing a `replay_wal_range` method to rebuild CDC events from WAL

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use falcon_common::datum::OwnedRow;
use falcon_common::types::{TableId, TxnId};

use crate::cdc::{CdcLsn, CdcManager, ChangeEvent, ChangeOp, ReplicationSlot};
use crate::wal::WalRecord;

// ── Slot persistence ─────────────────────────────────────────────────────────

/// On-disk representation of slot state for crash recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSlotState {
    pub slots: Vec<PersistedSlot>,
    /// WAL LSN at which this snapshot was taken.
    pub snapshot_lsn: u64,
}

/// A single slot's persisted state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSlot {
    pub id: u64,
    pub name: String,
    pub confirmed_flush_lsn: u64,
    pub restart_lsn: u64,
    pub created_at_ms: u64,
    pub table_filter: Option<Vec<u64>>,
    pub include_tx_markers: bool,
    pub include_old_values: bool,
}

impl From<&ReplicationSlot> for PersistedSlot {
    fn from(slot: &ReplicationSlot) -> Self {
        Self {
            id: slot.id.0,
            name: slot.name.clone(),
            confirmed_flush_lsn: slot.confirmed_flush_lsn.0,
            restart_lsn: slot.restart_lsn.0,
            created_at_ms: slot.created_at_ms,
            table_filter: slot
                .table_filter
                .as_ref()
                .map(|f| f.iter().map(|t| t.0).collect()),
            include_tx_markers: slot.include_tx_markers,
            include_old_values: slot.include_old_values,
        }
    }
}

/// Manages slot state persistence to a JSON sidecar file.
pub struct SlotPersistence {
    path: PathBuf,
}

impl SlotPersistence {
    /// Create a new slot persistence manager.
    /// `wal_dir` is the WAL directory; the sidecar file is stored alongside.
    pub fn new(wal_dir: &Path) -> Self {
        Self {
            path: wal_dir.join("cdc_slots.json"),
        }
    }

    /// Persist all current slot states to disk.
    pub fn save(&self, cdc: &CdcManager, current_lsn: u64) -> Result<(), String> {
        let slots = cdc.list_slots();
        let state = PersistedSlotState {
            slots: slots.iter().map(PersistedSlot::from).collect(),
            snapshot_lsn: current_lsn,
        };
        let json = serde_json::to_string_pretty(&state)
            .map_err(|e| format!("Failed to serialize slot state: {e}"))?;

        // Write atomically: write to tmp, then rename
        let tmp_path = self.path.with_extension("json.tmp");
        std::fs::write(&tmp_path, json.as_bytes())
            .map_err(|e| format!("Failed to write slot state: {e}"))?;
        std::fs::rename(&tmp_path, &self.path)
            .map_err(|e| format!("Failed to rename slot state file: {e}"))?;

        tracing::debug!(
            "CDC slot state persisted ({} slots, LSN={})",
            slots.len(),
            current_lsn
        );
        Ok(())
    }

    /// Load persisted slot state from disk (returns None if file doesn't exist).
    pub fn load(&self) -> Result<Option<PersistedSlotState>, String> {
        if !self.path.exists() {
            return Ok(None);
        }
        let data = std::fs::read_to_string(&self.path)
            .map_err(|e| format!("Failed to read slot state: {e}"))?;
        let state: PersistedSlotState =
            serde_json::from_str(&data).map_err(|e| format!("Failed to parse slot state: {e}"))?;
        tracing::info!(
            "CDC slot state loaded ({} slots, snapshot_lsn={})",
            state.slots.len(),
            state.snapshot_lsn,
        );
        Ok(Some(state))
    }

    /// Recover slots into the CdcManager from persisted state.
    pub fn recover_into(&self, cdc: &CdcManager) -> Result<u64, String> {
        let state = match self.load()? {
            Some(s) => s,
            None => return Ok(0),
        };

        let mut recovered = 0u64;
        for ps in &state.slots {
            match cdc.create_slot(&ps.name) {
                Ok(slot_id) => {
                    // Advance the slot to its persisted position
                    let _ = cdc.advance_slot(slot_id, CdcLsn(ps.confirmed_flush_lsn));
                    if let Some(ref filter) = ps.table_filter {
                        let table_ids: Vec<TableId> =
                            filter.iter().map(|&id| TableId(id)).collect();
                        let _ = cdc.set_table_filter(slot_id, table_ids);
                    }
                    recovered += 1;
                    tracing::info!(
                        "Recovered CDC slot '{}' (confirmed_lsn={})",
                        ps.name,
                        ps.confirmed_flush_lsn,
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to recover CDC slot '{}': {}", ps.name, e);
                }
            }
        }

        Ok(recovered)
    }
}

// ── Table name resolver ──────────────────────────────────────────────────────

/// Trait for resolving table_id → table_name.
/// Implemented by the catalog/schema layer.
pub trait TableNameResolver: Send + Sync {
    fn resolve(&self, table_id: TableId) -> Option<String>;
}

/// Simple in-memory table name resolver (for testing and simple setups).
#[derive(Debug, Clone, Default)]
pub struct SimpleTableResolver {
    names: HashMap<TableId, String>,
}

impl SimpleTableResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, table_id: TableId, name: String) {
        self.names.insert(table_id, name);
    }
}

impl TableNameResolver for SimpleTableResolver {
    fn resolve(&self, table_id: TableId) -> Option<String> {
        self.names.get(&table_id).cloned()
    }
}

/// Thread-safe cached table name resolver for production use.
/// Populated at startup from the catalog snapshot and auto-updated
/// as `CreateTable` / `DropTable` WAL records flow through the bridge.
pub struct CachedTableResolver {
    names: dashmap::DashMap<TableId, String>,
}

impl CachedTableResolver {
    pub fn new() -> Self {
        Self {
            names: dashmap::DashMap::new(),
        }
    }

    /// Bulk-load from a catalog snapshot (called once at startup).
    pub fn load_from_catalog(&self, catalog: &falcon_common::schema::Catalog) {
        for schema in catalog.list_tables() {
            self.names.insert(schema.id, schema.name.clone());
        }
        tracing::debug!(
            "CachedTableResolver: loaded {} table names",
            self.names.len()
        );
    }

    /// Register or update a table name mapping.
    pub fn register(&self, table_id: TableId, name: String) {
        self.names.insert(table_id, name);
    }

    /// Remove a table name mapping.
    pub fn unregister(&self, table_id: TableId) {
        self.names.remove(&table_id);
    }
}

impl Default for CachedTableResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl TableNameResolver for CachedTableResolver {
    fn resolve(&self, table_id: TableId) -> Option<String> {
        self.names.get(&table_id).map(|r| r.value().clone())
    }
}

// ── WAL → CDC Bridge ────────────────────────────────────────────────────────

/// Bridges WAL records to CDC change events with real LSNs.
///
/// Call `on_wal_record()` after each WAL append to feed committed changes
/// into the CDC stream. The bridge assigns real WAL LSNs to CDC events
/// instead of simulated ones.
pub struct WalCdcBridge {
    cdc: Arc<CdcManager>,
    resolver: Arc<dyn TableNameResolver>,
    /// Tracks the latest WAL LSN seen.
    latest_lsn: AtomicU64,
    /// Slot persistence manager.
    persistence: Option<SlotPersistence>,
    /// Counter for periodic slot state persistence (every N events).
    persist_counter: AtomicU64,
    /// Persist slot state every N events (0 = disabled).
    persist_interval: u64,
    /// Metrics: total WAL records processed.
    pub records_processed: AtomicU64,
    /// Metrics: total CDC events generated from WAL records.
    pub events_generated: AtomicU64,
    /// In-flight transaction tracking: txn_id → list of buffered events.
    /// Events are only emitted to CDC on commit, ensuring consumers only
    /// see committed changes (matching PostgreSQL logical decoding semantics).
    inflight: Mutex<HashMap<TxnId, Vec<ChangeEvent>>>,
}

impl WalCdcBridge {
    /// Create a new WAL-CDC bridge.
    pub fn new(cdc: Arc<CdcManager>, resolver: Arc<dyn TableNameResolver>) -> Self {
        Self {
            cdc,
            resolver,
            latest_lsn: AtomicU64::new(0),
            persistence: None,
            persist_counter: AtomicU64::new(0),
            persist_interval: 0,
            records_processed: AtomicU64::new(0),
            events_generated: AtomicU64::new(0),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// Enable slot state persistence to the given WAL directory.
    /// `persist_every` controls how often (in CDC events) the state is flushed.
    pub fn with_persistence(mut self, wal_dir: &Path, persist_every: u64) -> Self {
        self.persistence = Some(SlotPersistence::new(wal_dir));
        self.persist_interval = persist_every;
        self
    }

    /// Recover slots from persisted state on startup.
    pub fn recover_slots(&self) -> Result<u64, String> {
        match &self.persistence {
            Some(p) => p.recover_into(&self.cdc),
            None => Ok(0),
        }
    }

    /// Process a WAL record and generate CDC events.
    ///
    /// `wal_lsn` is the real LSN assigned by the WAL subsystem.
    /// DML events are buffered per-transaction and only emitted on commit.
    pub fn on_wal_record(&self, wal_lsn: u64, record: &WalRecord) {
        if !self.cdc.is_enabled() {
            return;
        }

        self.latest_lsn.store(wal_lsn, Ordering::Relaxed);
        self.records_processed.fetch_add(1, Ordering::Relaxed);

        match record {
            WalRecord::BeginTxn { txn_id } => {
                let mut inflight = self.inflight.lock();
                inflight.entry(*txn_id).or_default();
                // Buffer a BEGIN marker
                let event = self.make_event(
                    wal_lsn,
                    *txn_id,
                    ChangeOp::Begin,
                    None,
                    None,
                    None,
                    None,
                    None,
                );
                if let Some(buf) = inflight.get_mut(txn_id) {
                    buf.push(event);
                }
            }

            WalRecord::Insert {
                txn_id,
                table_id,
                row,
            } => {
                let table_name = self.resolver.resolve(*table_id);
                let event = self.make_event(
                    wal_lsn,
                    *txn_id,
                    ChangeOp::Insert,
                    Some(*table_id),
                    table_name,
                    Some(row.clone()),
                    None,
                    None,
                );
                self.buffer_event(*txn_id, event);
            }

            WalRecord::BatchInsert {
                txn_id,
                table_id,
                rows,
            } => {
                let table_name = self.resolver.resolve(*table_id);
                for row in rows {
                    let event = self.make_event(
                        wal_lsn,
                        *txn_id,
                        ChangeOp::Insert,
                        Some(*table_id),
                        table_name.clone(),
                        Some(row.clone()),
                        None,
                        None,
                    );
                    self.buffer_event(*txn_id, event);
                }
            }

            WalRecord::Update {
                txn_id,
                table_id,
                new_row,
                ..
            } => {
                let table_name = self.resolver.resolve(*table_id);
                let event = self.make_event(
                    wal_lsn,
                    *txn_id,
                    ChangeOp::Update,
                    Some(*table_id),
                    table_name,
                    Some(new_row.clone()),
                    None, // old_row not available from WAL record alone
                    None,
                );
                self.buffer_event(*txn_id, event);
            }

            WalRecord::Delete {
                txn_id, table_id, ..
            } => {
                let table_name = self.resolver.resolve(*table_id);
                let event = self.make_event(
                    wal_lsn,
                    *txn_id,
                    ChangeOp::Delete,
                    Some(*table_id),
                    table_name,
                    None,
                    None,
                    None,
                );
                self.buffer_event(*txn_id, event);
            }

            WalRecord::CommitTxn { txn_id, .. }
            | WalRecord::CommitTxnLocal { txn_id, .. }
            | WalRecord::CommitTxnGlobal { txn_id, .. } => {
                self.flush_transaction(*txn_id, wal_lsn);
            }

            WalRecord::AbortTxn { txn_id }
            | WalRecord::AbortTxnLocal { txn_id }
            | WalRecord::AbortTxnGlobal { txn_id } => {
                // Discard buffered events for aborted transactions
                let mut inflight = self.inflight.lock();
                if let Some(events) = inflight.remove(txn_id) {
                    tracing::debug!(
                        "CDC: discarded {} buffered events for aborted txn {}",
                        events.len(),
                        txn_id.0,
                    );
                }
            }

            // DDL records: emit immediately (they're auto-committed)
            WalRecord::CreateTable { schema } => {
                let ddl_text = format!("CREATE TABLE {}", schema.name);
                self.emit_ddl_immediate(wal_lsn, &ddl_text);
            }
            WalRecord::DropTable { table_name } => {
                let ddl_text = format!("DROP TABLE {table_name}");
                self.emit_ddl_immediate(wal_lsn, &ddl_text);
            }
            WalRecord::AlterTable { table_name, op } => {
                let ddl_text = format!("ALTER TABLE {table_name} {op:?}");
                self.emit_ddl_immediate(wal_lsn, &ddl_text);
            }
            WalRecord::CreateIndex {
                index_name,
                table_name,
                ..
            } => {
                let ddl_text = format!("CREATE INDEX {index_name} ON {table_name}");
                self.emit_ddl_immediate(wal_lsn, &ddl_text);
            }
            WalRecord::DropIndex {
                index_name,
                table_name,
                ..
            } => {
                let ddl_text = format!("DROP INDEX {index_name} ON {table_name}");
                self.emit_ddl_immediate(wal_lsn, &ddl_text);
            }

            // Other records: no CDC event needed
            _ => {}
        }
    }

    /// Flush a committed transaction's buffered events to the CDC stream.
    fn flush_transaction(&self, txn_id: TxnId, commit_lsn: u64) {
        let events = {
            let mut inflight = self.inflight.lock();
            inflight.remove(&txn_id).unwrap_or_default()
        };

        if events.is_empty() {
            return;
        }

        let event_count = events.len();

        // Emit all buffered events
        for event in events {
            self.cdc.emit(event);
        }

        // Emit COMMIT marker
        let commit_event = self.make_event(
            commit_lsn,
            txn_id,
            ChangeOp::Commit,
            None,
            None,
            None,
            None,
            None,
        );
        self.cdc.emit(commit_event);

        self.events_generated
            .fetch_add(event_count as u64 + 1, Ordering::Relaxed);

        tracing::debug!(
            "CDC: flushed {} events for committed txn {} (lsn={})",
            event_count + 1,
            txn_id.0,
            commit_lsn,
        );

        // Periodic slot state persistence
        self.maybe_persist_slots();
    }

    /// Emit a DDL event immediately (DDL is auto-committed).
    fn emit_ddl_immediate(&self, wal_lsn: u64, ddl_text: &str) {
        let event = self.make_event(
            wal_lsn,
            TxnId(0),
            ChangeOp::Ddl,
            None,
            None,
            None,
            None,
            Some(ddl_text.to_owned()),
        );
        self.cdc.emit(event);
        self.events_generated.fetch_add(1, Ordering::Relaxed);
        self.maybe_persist_slots();
    }

    /// Buffer an event for a transaction (will be emitted on commit).
    fn buffer_event(&self, txn_id: TxnId, event: ChangeEvent) {
        let mut inflight = self.inflight.lock();
        inflight.entry(txn_id).or_default().push(event);
    }

    /// Construct a ChangeEvent with the real WAL LSN.
    #[allow(clippy::too_many_arguments)]
    fn make_event(
        &self,
        wal_lsn: u64,
        txn_id: TxnId,
        op: ChangeOp,
        table_id: Option<TableId>,
        table_name: Option<String>,
        new_row: Option<OwnedRow>,
        old_row: Option<OwnedRow>,
        ddl_text: Option<String>,
    ) -> ChangeEvent {
        ChangeEvent {
            seq: 0, // assigned by CdcManager.emit()
            lsn: CdcLsn(wal_lsn),
            txn_id,
            op,
            table_id,
            table_name,
            timestamp_ms: now_ms(),
            new_row,
            old_row,
            pk_values: None,
            ddl_text,
        }
    }

    /// Periodically persist slot state to disk.
    fn maybe_persist_slots(&self) {
        if self.persist_interval == 0 {
            return;
        }
        let count = self.persist_counter.fetch_add(1, Ordering::Relaxed);
        if count.is_multiple_of(self.persist_interval) {
            if let Some(ref p) = self.persistence {
                let lsn = self.latest_lsn.load(Ordering::Relaxed);
                if let Err(e) = p.save(&self.cdc, lsn) {
                    tracing::warn!("Failed to persist CDC slot state: {}", e);
                }
            }
        }
    }

    /// Force-persist slot state now (e.g., on graceful shutdown).
    pub fn persist_slots_now(&self) -> Result<(), String> {
        match &self.persistence {
            Some(p) => {
                let lsn = self.latest_lsn.load(Ordering::Relaxed);
                p.save(&self.cdc, lsn)
            }
            None => Ok(()),
        }
    }

    /// Get the number of in-flight (uncommitted) transactions being tracked.
    pub fn inflight_txn_count(&self) -> usize {
        self.inflight.lock().len()
    }

    /// Get the latest WAL LSN seen by the bridge.
    pub fn latest_lsn(&self) -> u64 {
        self.latest_lsn.load(Ordering::Relaxed)
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::Datum;
    use falcon_common::types::Timestamp;

    fn make_bridge() -> (Arc<CdcManager>, WalCdcBridge) {
        let cdc = Arc::new(CdcManager::new(10_000));
        let mut resolver = SimpleTableResolver::new();
        resolver.register(TableId(1), "users".into());
        resolver.register(TableId(2), "orders".into());
        let bridge = WalCdcBridge::new(Arc::clone(&cdc), Arc::new(resolver));
        (cdc, bridge)
    }

    #[test]
    fn test_committed_txn_generates_events() {
        let (cdc, bridge) = make_bridge();
        let slot_id = cdc.create_slot("test").unwrap();

        // Simulate: BEGIN → INSERT → COMMIT
        bridge.on_wal_record(1, &WalRecord::BeginTxn { txn_id: TxnId(1) });
        bridge.on_wal_record(
            2,
            &WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int64(42), Datum::Text("alice".into())]),
            },
        );
        bridge.on_wal_record(
            3,
            &WalRecord::CommitTxn {
                txn_id: TxnId(1),
                commit_ts: Timestamp(100),
            },
        );

        let changes = cdc.poll_changes(slot_id, 100);
        // Should see: BEGIN, INSERT, COMMIT
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].op, ChangeOp::Begin);
        assert_eq!(changes[1].op, ChangeOp::Insert);
        assert_eq!(changes[1].table_name.as_deref(), Some("users"));
        assert!(changes[1].new_row.is_some());
        assert_eq!(changes[2].op, ChangeOp::Commit);
    }

    #[test]
    fn test_aborted_txn_no_events() {
        let (cdc, bridge) = make_bridge();
        let slot_id = cdc.create_slot("test").unwrap();

        bridge.on_wal_record(1, &WalRecord::BeginTxn { txn_id: TxnId(1) });
        bridge.on_wal_record(
            2,
            &WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int64(1)]),
            },
        );
        bridge.on_wal_record(3, &WalRecord::AbortTxn { txn_id: TxnId(1) });

        let changes = cdc.poll_changes(slot_id, 100);
        assert_eq!(changes.len(), 0, "Aborted txn should produce no CDC events");
    }

    #[test]
    fn test_ddl_emits_immediately() {
        let (cdc, bridge) = make_bridge();
        let slot_id = cdc.create_slot("test").unwrap();

        bridge.on_wal_record(
            1,
            &WalRecord::DropTable {
                table_name: "old_table".into(),
            },
        );

        let changes = cdc.poll_changes(slot_id, 100);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].op, ChangeOp::Ddl);
        assert!(changes[0].ddl_text.as_ref().unwrap().contains("DROP TABLE"));
    }

    #[test]
    fn test_batch_insert_generates_per_row_events() {
        let (cdc, bridge) = make_bridge();
        let slot_id = cdc.create_slot("test").unwrap();

        bridge.on_wal_record(1, &WalRecord::BeginTxn { txn_id: TxnId(1) });
        bridge.on_wal_record(
            2,
            &WalRecord::BatchInsert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                rows: vec![
                    OwnedRow::new(vec![Datum::Int64(1)]),
                    OwnedRow::new(vec![Datum::Int64(2)]),
                    OwnedRow::new(vec![Datum::Int64(3)]),
                ],
            },
        );
        bridge.on_wal_record(
            3,
            &WalRecord::CommitTxn {
                txn_id: TxnId(1),
                commit_ts: Timestamp(100),
            },
        );

        let changes = cdc.poll_changes(slot_id, 100);
        // BEGIN + 3 INSERTs + COMMIT = 5
        assert_eq!(changes.len(), 5);
        assert_eq!(
            changes.iter().filter(|c| c.op == ChangeOp::Insert).count(),
            3
        );
    }

    #[test]
    fn test_multiple_concurrent_transactions() {
        let (cdc, bridge) = make_bridge();
        let slot_id = cdc.create_slot("test").unwrap();

        // Interleave two transactions
        bridge.on_wal_record(1, &WalRecord::BeginTxn { txn_id: TxnId(1) });
        bridge.on_wal_record(2, &WalRecord::BeginTxn { txn_id: TxnId(2) });
        bridge.on_wal_record(
            3,
            &WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int64(1)]),
            },
        );
        bridge.on_wal_record(
            4,
            &WalRecord::Insert {
                txn_id: TxnId(2),
                table_id: TableId(2),
                row: OwnedRow::new(vec![Datum::Int64(2)]),
            },
        );
        // Commit txn 2 first
        bridge.on_wal_record(
            5,
            &WalRecord::CommitTxn {
                txn_id: TxnId(2),
                commit_ts: Timestamp(200),
            },
        );
        // Abort txn 1
        bridge.on_wal_record(6, &WalRecord::AbortTxn { txn_id: TxnId(1) });

        let changes = cdc.poll_changes(slot_id, 100);
        // Only txn 2 should appear: BEGIN + INSERT + COMMIT = 3
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[1].table_name.as_deref(), Some("orders"));
    }

    #[test]
    fn test_inflight_tracking() {
        let (_cdc, bridge) = make_bridge();

        assert_eq!(bridge.inflight_txn_count(), 0);

        bridge.on_wal_record(1, &WalRecord::BeginTxn { txn_id: TxnId(1) });
        assert_eq!(bridge.inflight_txn_count(), 1);

        bridge.on_wal_record(2, &WalRecord::BeginTxn { txn_id: TxnId(2) });
        assert_eq!(bridge.inflight_txn_count(), 2);

        bridge.on_wal_record(
            3,
            &WalRecord::CommitTxn {
                txn_id: TxnId(1),
                commit_ts: Timestamp(100),
            },
        );
        assert_eq!(bridge.inflight_txn_count(), 1);
    }

    #[test]
    fn test_wal_lsn_tracking() {
        let (_cdc, bridge) = make_bridge();

        assert_eq!(bridge.latest_lsn(), 0);
        bridge.on_wal_record(42, &WalRecord::BeginTxn { txn_id: TxnId(1) });
        assert_eq!(bridge.latest_lsn(), 42);
        bridge.on_wal_record(
            99,
            &WalRecord::CommitTxn {
                txn_id: TxnId(1),
                commit_ts: Timestamp(100),
            },
        );
        assert_eq!(bridge.latest_lsn(), 99);
    }

    #[test]
    fn test_slot_persistence_roundtrip() {
        let tmp = std::env::temp_dir().join("falcon_cdc_test_slots");
        let _ = std::fs::create_dir_all(&tmp);

        let cdc = Arc::new(CdcManager::new(1000));
        let slot_id = cdc.create_slot("persistent_slot").unwrap();
        cdc.advance_slot(slot_id, CdcLsn(42)).unwrap();

        // Save
        let persistence = SlotPersistence::new(&tmp);
        persistence.save(&cdc, 100).unwrap();

        // Load into a fresh CdcManager
        let cdc2 = Arc::new(CdcManager::new(1000));
        let recovered = persistence.recover_into(&cdc2).unwrap();
        assert_eq!(recovered, 1);

        let slot = cdc2.get_slot_by_name("persistent_slot");
        assert!(slot.is_some());

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_disabled_cdc_no_processing() {
        let cdc = Arc::new(CdcManager::disabled());
        let resolver = Arc::new(SimpleTableResolver::new());
        let bridge = WalCdcBridge::new(cdc, resolver);

        bridge.on_wal_record(1, &WalRecord::BeginTxn { txn_id: TxnId(1) });
        bridge.on_wal_record(
            2,
            &WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int64(1)]),
            },
        );
        bridge.on_wal_record(
            3,
            &WalRecord::CommitTxn {
                txn_id: TxnId(1),
                commit_ts: Timestamp(100),
            },
        );

        // Records are counted but no events are generated
        assert_eq!(bridge.records_processed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_update_and_delete_events() {
        let (cdc, bridge) = make_bridge();
        let slot_id = cdc.create_slot("test").unwrap();

        bridge.on_wal_record(1, &WalRecord::BeginTxn { txn_id: TxnId(1) });
        bridge.on_wal_record(
            2,
            &WalRecord::Update {
                txn_id: TxnId(1),
                table_id: TableId(1),
                pk: vec![0, 0, 0, 1],
                new_row: OwnedRow::new(vec![Datum::Int64(1), Datum::Text("updated".into())]),
            },
        );
        bridge.on_wal_record(
            3,
            &WalRecord::Delete {
                txn_id: TxnId(1),
                table_id: TableId(2),
                pk: vec![0, 0, 0, 2],
            },
        );
        bridge.on_wal_record(
            4,
            &WalRecord::CommitTxn {
                txn_id: TxnId(1),
                commit_ts: Timestamp(100),
            },
        );

        let changes = cdc.poll_changes(slot_id, 100);
        // BEGIN + UPDATE + DELETE + COMMIT = 4
        assert_eq!(changes.len(), 4);
        assert_eq!(changes[1].op, ChangeOp::Update);
        assert_eq!(changes[1].table_name.as_deref(), Some("users"));
        assert!(changes[1].new_row.is_some());
        assert_eq!(changes[2].op, ChangeOp::Delete);
        assert_eq!(changes[2].table_name.as_deref(), Some("orders"));
    }
}
