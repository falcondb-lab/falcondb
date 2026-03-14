//! CDC Schema Registry — tracks schema versions for CDC event consumers.
//!
//! When a table schema changes (ADD/DROP/RENAME COLUMN, ALTER COLUMN TYPE),
//! the registry records the new version so that downstream consumers (Kafka,
//! Debezium, data warehouses) can decode row data correctly.
//!
//! Design:
//! - Each table has a monotonically-increasing `schema_version` (u32).
//! - A `SchemaEntry` captures the full column list at that version plus a
//!   wall-clock timestamp and the DDL statement that triggered the change.
//! - `ChangeEvent.schema_version` references the registry so consumers can
//!   look up the schema at decode time.
//! - Thread-safe: all methods take `&self` via `RwLock` / `parking_lot`.

use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use falcon_common::schema::ColumnDef;
use falcon_common::types::TableId;

// ── Types ─────────────────────────────────────────────────────────────────────

/// Schema version identifier for a specific table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SchemaVersion(pub u32);

impl SchemaVersion {
    pub const INITIAL: Self = Self(1);
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// A single schema version entry for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub version: SchemaVersion,
    pub table_id: TableId,
    pub table_name: String,
    /// Columns at this version (in order).
    pub columns: Vec<ColumnDef>,
    /// Wall-clock when this version became active (unix millis).
    pub effective_at_ms: u64,
    /// DDL statement that caused this version (None for initial registration).
    pub ddl: Option<String>,
}

impl SchemaEntry {
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Tracks schema version history per table.
///
/// Maintains a per-table ordered map of `SchemaVersion → SchemaEntry`.
/// The latest version is always at the highest key.
#[derive(Debug)]
pub struct CdcSchemaRegistry {
    /// table_id → (version → entry)
    tables: RwLock<HashMap<TableId, BTreeMap<SchemaVersion, SchemaEntry>>>,
    /// Maximum number of historical versions to retain per table.
    max_versions_per_table: usize,
}

impl Default for CdcSchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CdcSchemaRegistry {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            max_versions_per_table: 64,
        }
    }

    pub fn with_max_versions(max: usize) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            max_versions_per_table: max,
        }
    }

    /// Register or update a table's schema.
    ///
    /// If the table is new, `version = INITIAL(1)`.
    /// If the table already has versions, the next version = current_max + 1.
    /// Returns the new `SchemaVersion`.
    pub fn register(
        &self,
        table_id: TableId,
        table_name: &str,
        columns: Vec<ColumnDef>,
        ddl: Option<&str>,
    ) -> SchemaVersion {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut tables = self.tables.write();
        let versions = tables.entry(table_id).or_insert_with(BTreeMap::new);

        let version = versions
            .keys()
            .next_back()
            .map(|v| SchemaVersion(v.0 + 1))
            .unwrap_or(SchemaVersion::INITIAL);

        let entry = SchemaEntry {
            version,
            table_id,
            table_name: table_name.to_owned(),
            columns,
            effective_at_ms: now_ms,
            ddl: ddl.map(|s| s.to_owned()),
        };

        versions.insert(version, entry);
        self.gc_old_versions(versions);
        version
    }

    /// Get the latest schema version for a table.
    pub fn latest(&self, table_id: TableId) -> Option<SchemaEntry> {
        self.tables
            .read()
            .get(&table_id)?
            .values()
            .next_back()
            .cloned()
    }

    /// Get a specific schema version for a table.
    pub fn get(&self, table_id: TableId, version: SchemaVersion) -> Option<SchemaEntry> {
        self.tables
            .read()
            .get(&table_id)?
            .get(&version)
            .cloned()
    }

    /// Get the latest schema version number for a table.
    pub fn latest_version(&self, table_id: TableId) -> Option<SchemaVersion> {
        self.tables
            .read()
            .get(&table_id)?
            .keys()
            .next_back()
            .copied()
    }

    /// List all version entries for a table (oldest → newest).
    pub fn history(&self, table_id: TableId) -> Vec<SchemaEntry> {
        self.tables
            .read()
            .get(&table_id)
            .map(|v| v.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Remove all schema history for a dropped table.
    pub fn unregister(&self, table_id: TableId) {
        self.tables.write().remove(&table_id);
    }

    /// Return total number of tracked tables.
    pub fn table_count(&self) -> usize {
        self.tables.read().len()
    }

    /// Return total schema entries (all tables, all versions).
    pub fn total_entries(&self) -> usize {
        self.tables.read().values().map(|v| v.len()).sum()
    }

    fn gc_old_versions(&self, versions: &mut BTreeMap<SchemaVersion, SchemaEntry>) {
        while versions.len() > self.max_versions_per_table {
            if let Some(&oldest) = versions.keys().next() {
                versions.remove(&oldest);
            } else {
                break;
            }
        }
    }
}

// ── CDC event schema version enrichment ──────────────────────────────────────

/// Schema version metadata to embed in CDC events.
///
/// Consumers attach this to `ChangeEvent` to know which schema version
/// was active when the event was captured.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSchemaRef {
    pub table_id: TableId,
    pub schema_version: SchemaVersion,
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::schema::ColumnDef;
    use falcon_common::types::{ColumnId, DataType, TableId};

    fn make_cols(names: &[&str]) -> Vec<ColumnDef> {
        names
            .iter()
            .enumerate()
            .map(|(i, &name)| ColumnDef {
                id: ColumnId(i as u32),
                name: name.to_owned(),
                data_type: DataType::Int32,
                nullable: true,
                is_primary_key: i == 0,
                default_value: None,
                is_serial: false,
                max_length: None,
            })
            .collect()
    }

    #[test]
    fn test_register_initial() {
        let reg = CdcSchemaRegistry::new();
        let tid = TableId(1);
        let v = reg.register(tid, "users", make_cols(&["id", "name"]), None);
        assert_eq!(v, SchemaVersion::INITIAL);
        assert_eq!(reg.table_count(), 1);
        assert_eq!(reg.total_entries(), 1);
    }

    #[test]
    fn test_register_evolution() {
        let reg = CdcSchemaRegistry::new();
        let tid = TableId(2);
        let v1 = reg.register(tid, "orders", make_cols(&["id", "total"]), None);
        let v2 = reg.register(tid, "orders", make_cols(&["id", "total", "status"]), Some("ALTER TABLE orders ADD COLUMN status TEXT"));
        assert_eq!(v1, SchemaVersion(1));
        assert_eq!(v2, SchemaVersion(2));

        let latest = reg.latest(tid).unwrap();
        assert_eq!(latest.column_count(), 3);
        assert_eq!(latest.ddl.as_deref(), Some("ALTER TABLE orders ADD COLUMN status TEXT"));
    }

    #[test]
    fn test_get_specific_version() {
        let reg = CdcSchemaRegistry::new();
        let tid = TableId(3);
        reg.register(tid, "t", make_cols(&["id"]), None);
        reg.register(tid, "t", make_cols(&["id", "extra"]), Some("ADD COLUMN extra TEXT"));

        let v1 = reg.get(tid, SchemaVersion(1)).unwrap();
        assert_eq!(v1.column_count(), 1);
        let v2 = reg.get(tid, SchemaVersion(2)).unwrap();
        assert_eq!(v2.column_count(), 2);
    }

    #[test]
    fn test_history_order() {
        let reg = CdcSchemaRegistry::new();
        let tid = TableId(4);
        for i in 0..5 {
            reg.register(tid, "t", make_cols(&["id"]), Some(&format!("ddl_{i}")));
        }
        let h = reg.history(tid);
        assert_eq!(h.len(), 5);
        assert!(h.windows(2).all(|w| w[0].version < w[1].version));
    }

    #[test]
    fn test_gc_old_versions() {
        let reg = CdcSchemaRegistry::with_max_versions(3);
        let tid = TableId(5);
        for _ in 0..6 {
            reg.register(tid, "t", make_cols(&["id"]), None);
        }
        assert!(reg.total_entries() <= 3);
    }

    #[test]
    fn test_unregister() {
        let reg = CdcSchemaRegistry::new();
        let tid = TableId(6);
        reg.register(tid, "temp", make_cols(&["id"]), None);
        assert_eq!(reg.table_count(), 1);
        reg.unregister(tid);
        assert_eq!(reg.table_count(), 0);
        assert!(reg.latest(tid).is_none());
    }

    #[test]
    fn test_find_column() {
        let reg = CdcSchemaRegistry::new();
        let tid = TableId(7);
        reg.register(tid, "t", make_cols(&["id", "email"]), None);
        let entry = reg.latest(tid).unwrap();
        assert!(entry.find_column("email").is_some());
        assert!(entry.find_column("EMAIL").is_some());
        assert!(entry.find_column("phone").is_none());
    }

    #[test]
    fn test_missing_table_returns_none() {
        let reg = CdcSchemaRegistry::new();
        assert!(reg.latest(TableId(999)).is_none());
        assert!(reg.get(TableId(999), SchemaVersion(1)).is_none());
        assert!(reg.latest_version(TableId(999)).is_none());
        assert!(reg.history(TableId(999)).is_empty());
    }
}
