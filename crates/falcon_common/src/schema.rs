use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::datum::Datum;
use crate::types::{ColumnId, DataType, TableId};

/// Sharding policy for a table in a distributed cluster.
/// Modeled after SingleStore's SHARD KEY / REFERENCE semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ShardingPolicy {
    /// Hash-based sharding on the specified shard key columns.
    /// Rows are assigned to shards by hashing the shard key values.
    Hash,
    /// Reference table: fully replicated on every shard.
    /// Ideal for small dimension tables used in JOINs.
    Reference,
    /// No sharding policy (single-node or unsharded table).
    /// Falls back to PK-based hash or shard 0.
    #[default]
    None,
}

/// Storage engine type for a table.
/// Modeled after SingleStore's ROWSTORE / COLUMNSTORE distinction,
/// plus an on-disk B-tree rowstore for larger-than-memory OLTP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum StorageType {
    /// In-memory row store (default, like SingleStore ROWSTORE).
    /// Best for OLTP: point lookups, small range scans, high write throughput.
    #[default]
    Rowstore,
    /// On-disk columnar store (like SingleStore COLUMNSTORE).
    /// Best for analytics: full-column scans, aggregations, compression.
    Columnstore,
    /// On-disk B-tree row store (like InnoDB / traditional disk engine).
    /// Handles larger-than-memory datasets with row-oriented access.
    DiskRowstore,
    /// LSM-tree backed row store (like RocksDB / LevelDB).
    /// Write-optimized with background compaction, suitable for write-heavy OLTP.
    LsmRowstore,
}

/// Column definition in a table schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<Datum>,
    pub is_serial: bool,
}

/// Table schema metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key_columns: Vec<usize>, // indices into `columns`
    /// Next auto-increment value per serial column (col_idx -> next_val)
    #[serde(default)]
    pub next_serial_values: std::collections::HashMap<usize, i64>,
    /// CHECK constraint SQL expressions (stored as raw SQL strings)
    #[serde(default)]
    pub check_constraints: Vec<String>,
    /// UNIQUE constraint column groups (each Vec<usize> is a unique constraint over those column indices)
    #[serde(default)]
    pub unique_constraints: Vec<Vec<usize>>,
    /// FOREIGN KEY constraints: (local_col_indices, referenced_table_name, referenced_col_names)
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKey>,
    /// Storage engine type (default: Rowstore = in-memory).
    #[serde(default)]
    pub storage_type: StorageType,
    /// Shard key column indices (into `columns`).  Empty = use PK or no sharding.
    #[serde(default)]
    pub shard_key: Vec<usize>,
    /// Sharding policy: Hash, Reference, or None.
    #[serde(default)]
    pub sharding_policy: ShardingPolicy,
}

/// Referential action for foreign key ON DELETE / ON UPDATE.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FkAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

/// A foreign key constraint referencing another table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub columns: Vec<usize>,      // local column indices
    pub ref_table: String,        // referenced table name
    pub ref_columns: Vec<String>, // referenced column names
    #[serde(default)]
    pub on_delete: FkAction,
    #[serde(default)]
    pub on_update: FkAction,
}

impl Default for TableSchema {
    fn default() -> Self {
        Self {
            id: TableId(0),
            name: String::new(),
            columns: Vec::new(),
            primary_key_columns: Vec::new(),
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_type: StorageType::Rowstore,
            shard_key: Vec::new(),
            sharding_policy: ShardingPolicy::None,
        }
    }
}

impl TableSchema {
    /// Find column index by name (case-insensitive).
    pub fn find_column(&self, name: &str) -> Option<usize> {
        let lower = name.to_lowercase();
        self.columns
            .iter()
            .position(|c| c.name.to_lowercase() == lower)
    }

    /// Get the primary key column indices.
    pub fn pk_indices(&self) -> &[usize] {
        &self.primary_key_columns
    }

    /// Number of columns.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Effective shard key column indices.
    /// Returns explicit shard_key if set, otherwise falls back to primary key.
    pub fn effective_shard_key(&self) -> &[usize] {
        if self.shard_key.is_empty() {
            &self.primary_key_columns
        } else {
            &self.shard_key
        }
    }

    /// Whether this table is a reference (replicated) table.
    pub fn is_reference_table(&self) -> bool {
        self.sharding_policy == ShardingPolicy::Reference
    }

    /// Whether this table has an explicit hash sharding policy.
    pub fn is_hash_sharded(&self) -> bool {
        self.sharding_policy == ShardingPolicy::Hash
    }
}

/// A view definition: name + the SQL query that defines it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDef {
    pub name: String,
    /// The original SQL query text (e.g. "SELECT id, name FROM users WHERE active").
    pub query_sql: String,
}

/// In-memory catalog: all known tables and views.
/// Thread-safe via external synchronization (DashMap in storage).
/// Tables are stored in a HashMap keyed by lowercase table name for O(1) lookup.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Catalog {
    tables: HashMap<String, TableSchema>,
    next_table_id: u64,
    #[serde(default)]
    pub views: Vec<ViewDef>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            next_table_id: 1,
            views: Vec::new(),
        }
    }

    pub fn next_table_id(&mut self) -> TableId {
        let id = TableId(self.next_table_id);
        self.next_table_id += 1;
        id
    }

    pub fn add_table(&mut self, schema: TableSchema) {
        let key = schema.name.to_lowercase();
        self.tables.insert(key, schema);
    }

    pub fn find_table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(&name.to_lowercase())
    }

    pub fn find_table_by_id(&self, id: TableId) -> Option<&TableSchema> {
        self.tables.values().find(|t| t.id == id)
    }

    pub fn drop_table(&mut self, name: &str) -> bool {
        self.tables.remove(&name.to_lowercase()).is_some()
    }

    /// Returns an iterator over all table schemas.
    pub fn list_tables(&self) -> Vec<&TableSchema> {
        self.tables.values().collect()
    }

    /// Returns the number of tables in the catalog.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn find_table_mut(&mut self, name: &str) -> Option<&mut TableSchema> {
        self.tables.get_mut(&name.to_lowercase())
    }

    /// Rename a table: removes the old key and inserts under the new key.
    /// Returns true if the rename succeeded, false if old name not found.
    pub fn rename_table(&mut self, old_name: &str, new_name: &str) -> bool {
        let old_key = old_name.to_lowercase();
        if let Some(mut schema) = self.tables.remove(&old_key) {
            schema.name = new_name.to_string();
            let new_key = new_name.to_lowercase();
            self.tables.insert(new_key, schema);
            true
        } else {
            false
        }
    }

    /// Direct access to the underlying table map (for iteration by ID, etc.)
    pub fn tables_map(&self) -> &HashMap<String, TableSchema> {
        &self.tables
    }

    // ── View management ──

    pub fn add_view(&mut self, view: ViewDef) {
        self.views.push(view);
    }

    pub fn find_view(&self, name: &str) -> Option<&ViewDef> {
        let lower = name.to_lowercase();
        self.views.iter().find(|v| v.name.to_lowercase() == lower)
    }

    pub fn drop_view(&mut self, name: &str) -> bool {
        let lower = name.to_lowercase();
        let len_before = self.views.len();
        self.views.retain(|v| v.name.to_lowercase() != lower);
        self.views.len() < len_before
    }

    pub fn list_views(&self) -> &[ViewDef] {
        &self.views
    }
}
