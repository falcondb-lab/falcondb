use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::datum::Datum;
use crate::security::{Role, RoleCatalog, RoleId, SUPERUSER_ROLE_ID};
use crate::tenant::SYSTEM_TENANT_ID;
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
    pub const fn num_columns(&self) -> usize {
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

/// A database entry in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseEntry {
    /// Unique database OID (used in pg_catalog.pg_database).
    pub oid: u32,
    /// Database name (case-preserving, lookup is case-insensitive).
    pub name: String,
    /// Owner of the database (defaults to the creating user).
    pub owner: String,
    /// Encoding (always UTF8 for FalconDB).
    pub encoding: String,
}

/// A view definition: name + the SQL query that defines it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDef {
    pub name: String,
    /// The original SQL query text (e.g. "SELECT id, name FROM users WHERE active").
    pub query_sql: String,
}

/// A schema (namespace) entry in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    /// Schema name (case-preserving, lookup is case-insensitive).
    pub name: String,
    /// Owner role name.
    pub owner: String,
}

/// Serializable role entry for WAL/checkpoint persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleEntry {
    pub id: u64,
    pub name: String,
    pub can_login: bool,
    pub is_superuser: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub password_hash: Option<String>,
    pub member_of: Vec<u64>,
}

/// Serializable grant entry for WAL/checkpoint persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantEntryCompact {
    pub grantee_id: u64,
    pub privilege: String,
    pub object_type: String,
    pub object_name: String,
    pub grantor_id: u64,
}

/// In-memory catalog: all known tables, views, schemas, roles, and privileges.
///
/// Thread-safe via external synchronization (RwLock in storage).
/// Tables are stored in a HashMap keyed by lowercase table name for O(1) lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    tables: HashMap<String, TableSchema>,
    next_table_id: u64,
    #[serde(default)]
    pub views: Vec<ViewDef>,
    #[serde(default = "Catalog::default_databases")]
    databases: Vec<DatabaseEntry>,
    #[serde(default = "Catalog::default_next_db_oid")]
    next_database_oid: u32,
    /// Schemas (namespaces). Default: "public".
    #[serde(default = "Catalog::default_schemas")]
    schemas: Vec<SchemaEntry>,
    /// Serializable role entries (rebuilt into RoleCatalog on load).
    #[serde(default = "Catalog::default_role_entries")]
    role_entries: Vec<RoleEntry>,
    /// Next role ID counter.
    #[serde(default = "Catalog::default_next_role_id")]
    next_role_id: u64,
    /// Serializable grant entries.
    #[serde(default)]
    grant_entries: Vec<GrantEntryCompact>,
    /// In-memory role catalog (not serialized — rebuilt from role_entries).
    #[serde(skip)]
    role_catalog: Option<RoleCatalog>,
}

impl Catalog {
    fn default_databases() -> Vec<DatabaseEntry> {
        vec![DatabaseEntry {
            oid: 1,
            name: "falcon".to_owned(),
            owner: "falcon".to_owned(),
            encoding: "UTF8".to_owned(),
        }]
    }

    const fn default_next_db_oid() -> u32 {
        2
    }

    fn default_schemas() -> Vec<SchemaEntry> {
        vec![
            SchemaEntry { name: "public".to_owned(), owner: "falcon".to_owned() },
            SchemaEntry { name: "pg_catalog".to_owned(), owner: "falcon".to_owned() },
            SchemaEntry { name: "information_schema".to_owned(), owner: "falcon".to_owned() },
        ]
    }

    fn default_role_entries() -> Vec<RoleEntry> {
        vec![RoleEntry {
            id: 0,
            name: "falcon".to_owned(),
            can_login: true,
            is_superuser: true,
            can_create_db: true,
            can_create_role: true,
            password_hash: None,
            member_of: Vec::new(),
        }]
    }

    const fn default_next_role_id() -> u64 {
        1
    }

    pub fn new() -> Self {
        let mut catalog = Self {
            tables: HashMap::new(),
            next_table_id: 1,
            views: Vec::new(),
            databases: Self::default_databases(),
            next_database_oid: 2,
            schemas: Self::default_schemas(),
            role_entries: Self::default_role_entries(),
            next_role_id: 1,
            grant_entries: Vec::new(),
            role_catalog: None,
        };
        catalog.rebuild_role_catalog();
        catalog
    }

    /// Rebuild the in-memory RoleCatalog from serialized role_entries.
    fn rebuild_role_catalog(&mut self) {
        use std::collections::HashSet;
        let mut rc = RoleCatalog::new();
        // RoleCatalog::new() already adds superuser. Remove it first to avoid duplicates.
        rc.remove_role(SUPERUSER_ROLE_ID);
        for entry in &self.role_entries {
            let mut role = Role::new_user(
                RoleId(entry.id),
                entry.name.clone(),
                SYSTEM_TENANT_ID,
            );
            role.can_login = entry.can_login;
            role.is_superuser = entry.is_superuser;
            role.can_create_db = entry.can_create_db;
            role.can_create_role = entry.can_create_role;
            role.password_hash = entry.password_hash.clone();
            role.member_of = entry.member_of.iter().map(|&id| RoleId(id)).collect::<HashSet<_>>();
            rc.add_role(role);
        }
        self.role_catalog = Some(rc);
    }

    /// Ensure role_catalog is initialized (called after deserialization).
    pub fn ensure_role_catalog(&mut self) {
        if self.role_catalog.is_none() {
            self.rebuild_role_catalog();
        }
    }

    /// Get a reference to the in-memory role catalog.
    pub fn role_catalog(&mut self) -> &RoleCatalog {
        self.ensure_role_catalog();
        self.role_catalog.as_ref().unwrap()
    }

    /// Get a mutable reference to the in-memory role catalog.
    pub fn role_catalog_mut(&mut self) -> &mut RoleCatalog {
        self.ensure_role_catalog();
        self.role_catalog.as_mut().unwrap()
    }

    pub const fn next_table_id(&mut self) -> TableId {
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
            schema.name = new_name.to_owned();
            let new_key = new_name.to_lowercase();
            self.tables.insert(new_key, schema);
            true
        } else {
            false
        }
    }

    /// Direct access to the underlying table map (for iteration by ID, etc.)
    pub const fn tables_map(&self) -> &HashMap<String, TableSchema> {
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

    // ── Database management ──

    pub fn create_database(&mut self, name: &str, owner: &str) -> Result<u32, String> {
        let lower = name.to_lowercase();
        if self.databases.iter().any(|db| db.name.to_lowercase() == lower) {
            return Err(format!("database \"{name}\" already exists"));
        }
        let oid = self.next_database_oid;
        self.next_database_oid += 1;
        self.databases.push(DatabaseEntry {
            oid,
            name: name.to_owned(),
            owner: owner.to_owned(),
            encoding: "UTF8".to_owned(),
        });
        Ok(oid)
    }

    pub fn drop_database(&mut self, name: &str) -> Result<(), String> {
        let lower = name.to_lowercase();
        if lower == "falcon" {
            return Err("cannot drop the default database \"falcon\"".to_owned());
        }
        let len_before = self.databases.len();
        self.databases.retain(|db| db.name.to_lowercase() != lower);
        if self.databases.len() < len_before {
            Ok(())
        } else {
            Err(format!("database \"{name}\" does not exist"))
        }
    }

    pub fn find_database(&self, name: &str) -> Option<&DatabaseEntry> {
        let lower = name.to_lowercase();
        self.databases.iter().find(|db| db.name.to_lowercase() == lower)
    }

    pub fn list_databases(&self) -> &[DatabaseEntry] {
        &self.databases
    }

    // ── Schema management ──

    pub fn create_schema(&mut self, name: &str, owner: &str) -> Result<(), String> {
        let lower = name.to_lowercase();
        if self.schemas.iter().any(|s| s.name.to_lowercase() == lower) {
            return Err(format!("schema \"{name}\" already exists"));
        }
        self.schemas.push(SchemaEntry {
            name: name.to_owned(),
            owner: owner.to_owned(),
        });
        Ok(())
    }

    pub fn drop_schema(&mut self, name: &str) -> Result<(), String> {
        let lower = name.to_lowercase();
        if lower == "public" || lower == "pg_catalog" || lower == "information_schema" {
            return Err(format!("cannot drop built-in schema \"{name}\""));
        }
        let len_before = self.schemas.len();
        self.schemas.retain(|s| s.name.to_lowercase() != lower);
        if self.schemas.len() < len_before {
            Ok(())
        } else {
            Err(format!("schema \"{name}\" does not exist"))
        }
    }

    pub fn find_schema(&self, name: &str) -> Option<&SchemaEntry> {
        let lower = name.to_lowercase();
        self.schemas.iter().find(|s| s.name.to_lowercase() == lower)
    }

    pub fn list_schemas(&self) -> &[SchemaEntry] {
        &self.schemas
    }

    /// Resolve a potentially schema-qualified table name.
    /// If name contains '.', treat left part as schema prefix and strip it.
    pub fn resolve_table_name(&self, name: &str) -> String {
        name.find('.').map_or_else(|| name.to_owned(), |dot_pos| name[dot_pos + 1..].to_string())
    }

    // ── Role management ──

    pub fn create_role(
        &mut self,
        name: &str,
        can_login: bool,
        is_superuser: bool,
        can_create_db: bool,
        can_create_role: bool,
        password: Option<String>,
    ) -> Result<u64, String> {
        let lower = name.to_lowercase();
        if self.role_entries.iter().any(|r| r.name.to_lowercase() == lower) {
            return Err(format!("role \"{name}\" already exists"));
        }
        let id = self.next_role_id;
        self.next_role_id += 1;
        self.role_entries.push(RoleEntry {
            id,
            name: name.to_owned(),
            can_login,
            is_superuser,
            can_create_db,
            can_create_role,
            password_hash: password,
            member_of: Vec::new(),
        });
        self.rebuild_role_catalog();
        Ok(id)
    }

    pub fn drop_role(&mut self, name: &str) -> Result<(), String> {
        let lower = name.to_lowercase();
        if lower == "falcon" {
            return Err("cannot drop the superuser role \"falcon\"".to_owned());
        }
        let len_before = self.role_entries.len();
        if let Some(entry) = self.role_entries.iter().find(|r| r.name.to_lowercase() == lower) {
            let role_id = entry.id;
            self.grant_entries.retain(|g| g.grantee_id != role_id && g.grantor_id != role_id);
            for r in &mut self.role_entries {
                r.member_of.retain(|&id| id != role_id);
            }
        }
        self.role_entries.retain(|r| r.name.to_lowercase() != lower);
        if self.role_entries.len() < len_before {
            self.rebuild_role_catalog();
            Ok(())
        } else {
            Err(format!("role \"{name}\" does not exist"))
        }
    }

    pub fn alter_role(
        &mut self,
        name: &str,
        password: Option<Option<String>>,
        can_login: Option<bool>,
        is_superuser: Option<bool>,
        can_create_db: Option<bool>,
        can_create_role: Option<bool>,
    ) -> Result<(), String> {
        let lower = name.to_lowercase();
        let entry = self.role_entries.iter_mut()
            .find(|r| r.name.to_lowercase() == lower)
            .ok_or_else(|| format!("role \"{name}\" does not exist"))?;
        if let Some(pw) = password {
            entry.password_hash = pw;
        }
        if let Some(v) = can_login {
            entry.can_login = v;
        }
        if let Some(v) = is_superuser {
            entry.is_superuser = v;
        }
        if let Some(v) = can_create_db {
            entry.can_create_db = v;
        }
        if let Some(v) = can_create_role {
            entry.can_create_role = v;
        }
        self.rebuild_role_catalog();
        Ok(())
    }

    pub fn find_role_by_name(&self, name: &str) -> Option<&RoleEntry> {
        let lower = name.to_lowercase();
        self.role_entries.iter().find(|r| r.name.to_lowercase() == lower)
    }

    pub fn list_role_entries(&self) -> &[RoleEntry] {
        &self.role_entries
    }

    // ── Privilege management ──

    pub fn grant_privilege(
        &mut self,
        grantee_name: &str,
        privilege: &str,
        object_type: &str,
        object_name: &str,
        grantor_name: &str,
    ) -> Result<(), String> {
        let grantee = self.find_role_by_name(grantee_name)
            .ok_or_else(|| format!("role \"{grantee_name}\" does not exist"))?;
        let grantee_id = grantee.id;
        let grantor = self.find_role_by_name(grantor_name)
            .ok_or_else(|| format!("role \"{grantor_name}\" does not exist"))?;
        let grantor_id = grantor.id;

        let exists = self.grant_entries.iter().any(|g| {
            g.grantee_id == grantee_id
                && g.privilege == privilege
                && g.object_type == object_type
                && g.object_name.to_lowercase() == object_name.to_lowercase()
        });
        if !exists {
            self.grant_entries.push(GrantEntryCompact {
                grantee_id,
                privilege: privilege.to_owned(),
                object_type: object_type.to_owned(),
                object_name: object_name.to_owned(),
                grantor_id,
            });
        }
        Ok(())
    }

    pub fn revoke_privilege(
        &mut self,
        grantee_name: &str,
        privilege: &str,
        object_type: &str,
        object_name: &str,
    ) -> Result<(), String> {
        let grantee = self.find_role_by_name(grantee_name)
            .ok_or_else(|| format!("role \"{grantee_name}\" does not exist"))?;
        let grantee_id = grantee.id;
        self.grant_entries.retain(|g| {
            !(g.grantee_id == grantee_id
                && g.privilege == privilege
                && g.object_type == object_type
                && g.object_name.to_lowercase() == object_name.to_lowercase())
        });
        Ok(())
    }

    /// Grant role membership: make `member_name` a member of `group_name`.
    pub fn grant_role_membership(&mut self, member_name: &str, group_name: &str) -> Result<(), String> {
        let member_lower = member_name.to_lowercase();
        let group_lower = group_name.to_lowercase();
        let group_entry = self.role_entries.iter()
            .find(|r| r.name.to_lowercase() == group_lower)
            .ok_or_else(|| format!("role \"{group_name}\" does not exist"))?;
        let group_id = group_entry.id;

        let member_entry = self.role_entries.iter_mut()
            .find(|r| r.name.to_lowercase() == member_lower)
            .ok_or_else(|| format!("role \"{member_name}\" does not exist"))?;
        if !member_entry.member_of.contains(&group_id) {
            member_entry.member_of.push(group_id);
        }
        self.rebuild_role_catalog();
        Ok(())
    }

    /// Revoke role membership.
    pub fn revoke_role_membership(&mut self, member_name: &str, group_name: &str) -> Result<(), String> {
        let member_lower = member_name.to_lowercase();
        let group_lower = group_name.to_lowercase();
        let group_entry = self.role_entries.iter()
            .find(|r| r.name.to_lowercase() == group_lower)
            .ok_or_else(|| format!("role \"{group_name}\" does not exist"))?;
        let group_id = group_entry.id;

        let member_entry = self.role_entries.iter_mut()
            .find(|r| r.name.to_lowercase() == member_lower)
            .ok_or_else(|| format!("role \"{member_name}\" does not exist"))?;
        member_entry.member_of.retain(|&id| id != group_id);
        self.rebuild_role_catalog();
        Ok(())
    }

    pub fn list_grants(&self) -> &[GrantEntryCompact] {
        &self.grant_entries
    }

    /// Check if a role has a specific privilege on an object.
    /// Superusers bypass all checks.
    pub fn check_privilege(
        &mut self,
        role_name: &str,
        privilege: &str,
        object_type: &str,
        object_name: &str,
    ) -> bool {
        let lower = role_name.to_lowercase();
        let entry = match self.role_entries.iter().find(|r| r.name.to_lowercase() == lower) {
            Some(e) => e,
            None => return false,
        };
        if entry.is_superuser {
            return true;
        }
        let role_id = entry.id;

        self.ensure_role_catalog();
        let rc = self.role_catalog.as_ref().unwrap();
        let effective = rc.effective_roles(RoleId(role_id));
        let effective_ids: Vec<u64> = effective.iter().map(|r| r.0).collect();

        let obj_lower = object_name.to_lowercase();
        let priv_upper = privilege.to_uppercase();
        for g in &self.grant_entries {
            if !effective_ids.contains(&g.grantee_id) {
                continue;
            }
            if g.object_type.to_uppercase() != object_type.to_uppercase() {
                continue;
            }
            if g.object_name.to_lowercase() != obj_lower {
                continue;
            }
            if g.privilege.to_uppercase() == priv_upper || g.privilege.to_uppercase() == "ALL" {
                return true;
            }
        }
        false
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
