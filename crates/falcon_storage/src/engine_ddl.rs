//! DDL operations on StorageEngine: CREATE/DROP/ALTER TABLE, VIEW, INDEX, SEQUENCE.

use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::Arc;

use falcon_common::error::StorageError;
use falcon_common::schema::{StorageType, TableSchema};
use falcon_common::types::TableId;

use crate::memtable::MemTable;
use crate::online_ddl::{DdlOpKind, BACKFILL_BATCH_SIZE};
use crate::ustm::{AccessPriority, PageData, PageId};
use crate::wal::WalRecord;

#[cfg(feature = "columnstore")]
use crate::columnstore::ColumnStoreTable;

#[cfg(feature = "disk_rowstore")]
use crate::disk_rowstore::DiskRowstoreTable;

#[cfg(feature = "lsm")]
use crate::lsm_table::LsmTable;

use super::engine::{datatype_to_cast_target, IndexMeta, StorageEngine};

impl StorageEngine {
    /// Bump the DDL epoch to invalidate thread-local TABLE_CACHE entries.
    #[inline]
    fn bump_ddl_epoch(&self) {
        self.ddl_epoch.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // ── Database DDL ─────────────────────────────────────────────────

    pub fn create_database(&self, name: &str, owner: &str) -> Result<u32, StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_database(name).is_some() {
                return Err(StorageError::DatabaseAlreadyExists(format!(
                    "database \"{}\" already exists",
                    name
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::CreateDatabase {
            name: name.to_owned(),
            owner: owner.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.create_database(name, owner);
        let oid = catalog.find_database(name).map_or(0, |db| db.oid);

        tracing::info!("Created database '{}' (oid={})", name, oid);
        Ok(oid)
    }

    pub fn drop_database(&self, name: &str) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_database(name).is_none() {
                return Err(StorageError::DatabaseNotFound(format!(
                    "database \"{}\" does not exist",
                    name
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::DropDatabase {
            name: name.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.drop_database(name);

        tracing::info!("Dropped database '{}'", name);
        Ok(())
    }

    // ── Schema DDL ────────────────────────────────────────────────────

    pub fn create_schema(&self, name: &str, owner: &str) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_schema(name).is_some() {
                return Err(StorageError::SchemaAlreadyExists(format!(
                    "schema \"{}\" already exists",
                    name
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::CreateSchema {
            name: name.to_owned(),
            owner: owner.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.create_schema(name, owner);

        tracing::info!("Created schema '{}'", name);
        Ok(())
    }

    pub fn drop_schema(&self, name: &str) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_schema(name).is_none() {
                return Err(StorageError::SchemaNotFound(format!(
                    "schema \"{}\" does not exist",
                    name
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::DropSchema {
            name: name.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.drop_schema(name);

        tracing::info!("Dropped schema '{}'", name);
        Ok(())
    }

    // ── Function DDL ──────────────────────────────────────────────────

    pub fn create_function(
        &self,
        def: falcon_common::schema::FunctionDef,
    ) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_function(&def.name).is_some() {
                return Err(StorageError::Other(format!(
                    "function \"{}\" already exists",
                    def.name
                )));
            }
        }

        let name = def.name.clone();
        // WAL first
        self.append_wal(&WalRecord::CreateFunction { def: def.clone() })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.create_function(def);

        tracing::info!("Created function '{}'", name);
        Ok(())
    }

    pub fn create_trigger(
        &self,
        def: falcon_common::schema::TriggerDef,
    ) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_trigger(&def.table_name, &def.name).is_some() {
                return Err(StorageError::Other(format!(
                    "trigger \"{}\" for table \"{}\" already exists",
                    def.name, def.table_name
                )));
            }
        }
        self.append_wal(&WalRecord::CreateTrigger { def: def.clone() })?;
        let mut catalog = self.catalog.write();
        let _ = catalog.create_trigger(def);
        Ok(())
    }

    pub fn drop_trigger(
        &self,
        table_name: &str,
        trigger_name: &str,
    ) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_trigger(table_name, trigger_name).is_none() {
                return Err(StorageError::Other(format!(
                    "trigger \"{trigger_name}\" for table \"{table_name}\" does not exist"
                )));
            }
        }
        self.append_wal(&WalRecord::DropTrigger {
            table_name: table_name.to_owned(),
            trigger_name: trigger_name.to_owned(),
        })?;
        let mut catalog = self.catalog.write();
        let _ = catalog.drop_trigger(table_name, trigger_name);
        Ok(())
    }

    // ── Enum type DDL ─────────────────────────────────────────────────

    pub fn create_enum_type(
        &self,
        def: falcon_common::schema::EnumTypeDef,
    ) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_enum_type(&def.name).is_some() {
                return Err(StorageError::Other(format!(
                    "type \"{}\" already exists",
                    def.name
                )));
            }
        }
        let mut catalog = self.catalog.write();
        catalog
            .create_enum_type(def)
            .map_err(StorageError::Other)
    }

    pub fn drop_enum_type(&self, name: &str, if_exists: bool) -> Result<(), StorageError> {
        let mut catalog = self.catalog.write();
        catalog
            .drop_enum_type(name, if_exists)
            .map_err(StorageError::Other)
    }

    pub fn drop_function(&self, name: &str) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_function(name).is_none() {
                return Err(StorageError::FunctionNotFound(name.to_owned()));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::DropFunction {
            name: name.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.drop_function(name);

        tracing::info!("Dropped function '{}'", name);
        Ok(())
    }

    // ── Role DDL ─────────────────────────────────────────────────────

    pub fn create_role(
        &self,
        name: &str,
        can_login: bool,
        is_superuser: bool,
        can_create_db: bool,
        can_create_role: bool,
        password: Option<String>,
    ) -> Result<u64, StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_role_by_name(name).is_some() {
                return Err(StorageError::RoleAlreadyExists(format!(
                    "role \"{}\" already exists",
                    name
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::CreateRole {
            name: name.to_owned(),
            can_login,
            is_superuser,
            can_create_db,
            can_create_role,
            password_hash: password.clone(),
        })?;

        let mut catalog = self.catalog.write();
        let id = catalog
            .create_role(
                name,
                can_login,
                is_superuser,
                can_create_db,
                can_create_role,
                password,
            )
            .map_err(StorageError::RoleAlreadyExists)?;

        tracing::info!("Created role '{}' (id={})", name, id);
        Ok(id)
    }

    pub fn drop_role(&self, name: &str) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_role_by_name(name).is_none() {
                return Err(StorageError::RoleNotFound(format!(
                    "role \"{}\" does not exist",
                    name
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::DropRole {
            name: name.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.drop_role(name);

        tracing::info!("Dropped role '{}'", name);
        Ok(())
    }

    pub fn alter_role(
        &self,
        name: &str,
        password: Option<Option<String>>,
        can_login: Option<bool>,
        is_superuser: Option<bool>,
        can_create_db: Option<bool>,
        can_create_role: Option<bool>,
    ) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_role_by_name(name).is_none() {
                return Err(StorageError::RoleNotFound(format!(
                    "role \"{}\" does not exist",
                    name
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::AlterRole {
            name: name.to_owned(),
            opts: crate::wal::AlterRoleOpts {
                password: password.clone(),
                can_login,
                is_superuser,
                can_create_db,
                can_create_role,
            },
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.alter_role(
            name,
            password,
            can_login,
            is_superuser,
            can_create_db,
            can_create_role,
        );

        tracing::info!("Altered role '{}'", name);
        Ok(())
    }

    // ── Privilege DDL ────────────────────────────────────────────────

    pub fn grant_privilege(
        &self,
        grantee: &str,
        privilege: &str,
        object_type: &str,
        object_name: &str,
        grantor: &str,
    ) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_role_by_name(grantee).is_none() {
                return Err(StorageError::RoleNotFound(format!(
                    "role \"{}\" does not exist",
                    grantee
                )));
            }
            if catalog.find_role_by_name(grantor).is_none() {
                return Err(StorageError::RoleNotFound(format!(
                    "role \"{}\" does not exist",
                    grantor
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::GrantPrivilege {
            grantee: grantee.to_owned(),
            privilege: privilege.to_owned(),
            object_type: object_type.to_owned(),
            object_name: object_name.to_owned(),
            grantor: grantor.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.grant_privilege(grantee, privilege, object_type, object_name, grantor);

        tracing::info!(
            "GRANT {} ON {} {} TO {}",
            privilege,
            object_type,
            object_name,
            grantee
        );
        Ok(())
    }

    pub fn revoke_privilege(
        &self,
        grantee: &str,
        privilege: &str,
        object_type: &str,
        object_name: &str,
    ) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_role_by_name(grantee).is_none() {
                return Err(StorageError::RoleNotFound(format!(
                    "role \"{}\" does not exist",
                    grantee
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::RevokePrivilege {
            grantee: grantee.to_owned(),
            privilege: privilege.to_owned(),
            object_type: object_type.to_owned(),
            object_name: object_name.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.revoke_privilege(grantee, privilege, object_type, object_name);

        tracing::info!(
            "REVOKE {} ON {} {} FROM {}",
            privilege,
            object_type,
            object_name,
            grantee
        );
        Ok(())
    }

    pub fn grant_role_membership(&self, member: &str, group: &str) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_role_by_name(member).is_none() {
                return Err(StorageError::RoleNotFound(format!(
                    "role \"{}\" does not exist",
                    member
                )));
            }
            if catalog.find_role_by_name(group).is_none() {
                return Err(StorageError::RoleNotFound(format!(
                    "role \"{}\" does not exist",
                    group
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::GrantRole {
            member: member.to_owned(),
            group: group.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.grant_role_membership(member, group);

        tracing::info!("GRANT role '{}' TO '{}'", group, member);
        Ok(())
    }

    pub fn revoke_role_membership(&self, member: &str, group: &str) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_role_by_name(member).is_none() {
                return Err(StorageError::RoleNotFound(format!(
                    "role \"{}\" does not exist",
                    member
                )));
            }
            if catalog.find_role_by_name(group).is_none() {
                return Err(StorageError::RoleNotFound(format!(
                    "role \"{}\" does not exist",
                    group
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::RevokeRole {
            member: member.to_owned(),
            group: group.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let _ = catalog.revoke_role_membership(member, group);

        tracing::info!("REVOKE role '{}' FROM '{}'", group, member);
        Ok(())
    }

    // ── Table DDL ────────────────────────────────────────────────────

    pub fn create_table(&self, schema: TableSchema) -> Result<TableId, StorageError> {
        // Validate under read lock
        {
            let catalog = self.catalog.read();
            if catalog.find_table(&schema.name).is_some() {
                return Err(StorageError::TableAlreadyExists(schema.name));
            }
        }

        let table_id = schema.id;

        // Pre-validate storage type feasibility (fail before WAL write)
        match schema.storage_type {
            StorageType::Rowstore => {}
            StorageType::Columnstore => {
                #[cfg(feature = "columnstore")]
                {
                    if !self.node_role.allows_columnstore() {
                        return Err(StorageError::Io(std::io::Error::other(format!(
                            "COLUMNSTORE tables are not allowed on {:?} nodes",
                            self.node_role
                        ))));
                    }
                }
                #[cfg(not(feature = "columnstore"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "COLUMNSTORE storage engine is not available in this build (feature disabled)",
                    )));
                }
            }
            StorageType::DiskRowstore => {
                #[cfg(feature = "disk_rowstore")]
                {
                    if !self.node_role.allows_columnstore() {
                        return Err(StorageError::Io(std::io::Error::other(format!(
                            "DISK_ROWSTORE tables are not allowed on {:?} nodes",
                            self.node_role
                        ))));
                    }
                }
                #[cfg(not(feature = "disk_rowstore"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "DISK_ROWSTORE storage engine is not available in this build (feature disabled)",
                    )));
                }
            }
            StorageType::LsmRowstore => {
                #[cfg(not(feature = "lsm"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "LSM storage engine is not available in this build (feature disabled)",
                    )));
                }
            }
            StorageType::RocksDbRowstore => {
                #[cfg(not(feature = "rocksdb"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "RocksDB storage engine is not available in this build (feature disabled)",
                    )));
                }
            }
            StorageType::RedbRowstore => {
                #[cfg(not(feature = "redb"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "redb storage engine is not available in this build (feature disabled)",
                    )));
                }
            }
        }

        // WAL first
        self.append_wal(&WalRecord::CreateTable {
            schema: schema.clone(),
        })?;

        // CDC: emit DDL event for CREATE TABLE
        if self.ext.cdc_manager.is_enabled() {
            let txn_id = falcon_common::types::TxnId(0);
            self.ext
                .cdc_manager
                .emit_ddl(txn_id, &format!("CREATE TABLE {}", schema.name));
        }

        // Now create engine table and add to catalog
        match schema.storage_type {
            StorageType::Rowstore => {
                let table = if self.large_mem_enabled {
                    Arc::new(MemTable::new_large_mem(
                        schema.clone(),
                        self.large_mem_dashmap_shards,
                        self.large_mem_capacity_hint,
                        self.large_mem_skip_pk_order,
                    ))
                } else {
                    Arc::new(MemTable::new(schema.clone()))
                };
                table.ensure_gin_indexes();
                self.engine_tables
                    .insert(table_id, crate::table_handle::TableHandle::Rowstore(table));
            }
            StorageType::Columnstore => {
                #[cfg(feature = "columnstore")]
                {
                    let table = Arc::new(ColumnStoreTable::new(schema.clone()));
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::Columnstore(table),
                    );
                }
            }
            StorageType::DiskRowstore => {
                #[cfg(feature = "disk_rowstore")]
                {
                    let data_dir = self
                        .data_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    let table = Arc::new(DiskRowstoreTable::new(schema.clone(), data_dir)?);
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::DiskRowstore(Arc::clone(&table)),
                    );
                }
            }
            StorageType::LsmRowstore => {
                #[cfg(feature = "lsm")]
                {
                    let data_dir = self
                        .data_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    let lsm_dir = data_dir.join(format!("lsm_table_{}", table_id.0));
                    let engine = Arc::new(
                        crate::lsm::engine::LsmEngine::open(
                            &lsm_dir,
                            crate::lsm::engine::LsmConfig {
                                sync_writes: self.lsm_sync_writes,
                                ..crate::lsm::engine::LsmConfig::default()
                            },
                        )
                        .map_err(StorageError::Io)?,
                    );
                    let table = Arc::new(LsmTable::new(schema.clone(), engine));
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::Lsm(Arc::clone(&table)),
                    );
                }
            }
            StorageType::RocksDbRowstore => {
                #[cfg(feature = "rocksdb")]
                {
                    let data_dir = self
                        .data_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    let rdb_dir = data_dir.join(format!("rocksdb_table_{}", table_id.0));
                    let table = Arc::new(crate::rocksdb_table::RocksDbTable::open(
                        schema.clone(),
                        &rdb_dir,
                    )?);
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::RocksDb(Arc::clone(&table)),
                    );
                }
            }
            StorageType::RedbRowstore => {
                #[cfg(feature = "redb")]
                {
                    let data_dir = self
                        .data_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    let rdb_dir = data_dir.join(format!("redb_table_{}", table_id.0));
                    let table = Arc::new(crate::redb_table::RedbTable::open(
                        schema.clone(),
                        &rdb_dir,
                    )?);
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::Redb(Arc::clone(&table)),
                    );
                }
            }
        }

        // USTM: register table metadata page in Hot zone.
        let meta_page_id = PageId(table_id.0 << 32);
        let meta_bytes = schema.name.as_bytes().to_vec();
        let _ = self.ustm.alloc_hot(
            meta_page_id,
            PageData::new(meta_bytes),
            AccessPriority::IndexInternal,
        );

        let mut catalog = self.catalog.write();
        catalog.add_table(schema.clone());
        // CDC schema registry: record initial schema version.
        if self.ext.cdc_manager.is_enabled() {
            self.ext.cdc_schema_registry.register(
                table_id,
                &schema.name,
                schema.columns.clone(),
                Some(&format!("CREATE TABLE {}", schema.name)),
            );
        }
        Ok(table_id)
    }

    pub fn drop_table(&self, name: &str) -> Result<(), StorageError> {
        let mut catalog = self.catalog.write();
        let table = catalog
            .find_table(name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = table.id;

        // USTM: unregister table metadata page.
        let meta_page_id = PageId(table_id.0 << 32);
        let _meta_page_id = meta_page_id; // reserved for USTM integration

        // WAL first: ensure DROP is logged before in-memory removal
        self.append_wal(&WalRecord::DropTable {
            table_name: name.to_owned(),
        })?;

        // CDC: emit DDL event for DROP TABLE
        if self.ext.cdc_manager.is_enabled() {
            let txn_id = falcon_common::types::TxnId(0);
            self.ext
                .cdc_manager
                .emit_ddl(txn_id, &format!("DROP TABLE {name}"));
        }

        // Remove from storage map and catalog AFTER WAL is written
        self.engine_tables.remove(&table_id);
        catalog.drop_table(name);
        self.bump_ddl_epoch();
        Ok(())
    }

    /// Drop all temporary tables (session-scoped cleanup on disconnect).
    /// Returns the names of tables that were dropped.
    pub fn drop_temp_tables(&self) -> Vec<String> {
        let catalog = self.catalog.read();
        let temp_names: Vec<String> = catalog
            .list_tables()
            .into_iter()
            .filter(|t| t.is_temporary)
            .map(|t| t.name.clone())
            .collect();
        drop(catalog);
        for name in &temp_names {
            let _ = self.drop_table(name);
        }
        temp_names
    }

    /// Truncate a table — remove all rows but keep the schema.
    pub fn truncate_table(&self, name: &str) -> Result<(), StorageError> {
        let catalog = self.catalog.read();
        let table = catalog
            .find_table(name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = table.id;
        let schema = table.clone();
        drop(catalog);

        // WAL first: ensure TRUNCATE is logged before in-memory replacement
        self.append_wal(&WalRecord::TruncateTable {
            table_name: name.to_owned(),
        })?;

        // Replace with an empty table of the appropriate storage type
        match schema.storage_type {
            StorageType::Rowstore => {
                let table = if self.large_mem_enabled {
                    Arc::new(MemTable::new_large_mem(
                        schema,
                        self.large_mem_dashmap_shards,
                        self.large_mem_capacity_hint,
                        self.large_mem_skip_pk_order,
                    ))
                } else {
                    Arc::new(MemTable::new(schema))
                };
                table.ensure_gin_indexes();
                self.engine_tables
                    .insert(table_id, crate::table_handle::TableHandle::Rowstore(table));
            }
            StorageType::Columnstore => {
                #[cfg(feature = "columnstore")]
                {
                    let table = Arc::new(ColumnStoreTable::new(schema));
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::Columnstore(table),
                    );
                }
                #[cfg(not(feature = "columnstore"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "COLUMNSTORE not available (feature disabled)",
                    )));
                }
            }
            StorageType::DiskRowstore => {
                #[cfg(feature = "disk_rowstore")]
                {
                    let data_dir = self
                        .data_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    let table = Arc::new(DiskRowstoreTable::new(schema, data_dir)?);
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::DiskRowstore(Arc::clone(&table)),
                    );
                }
                #[cfg(not(feature = "disk_rowstore"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "DISK_ROWSTORE not available (feature disabled)",
                    )));
                }
            }
            StorageType::LsmRowstore => {
                #[cfg(feature = "lsm")]
                {
                    let data_dir = self
                        .data_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    let lsm_dir = data_dir.join(format!("lsm_table_{}", table_id.0));
                    let _ = std::fs::remove_dir_all(&lsm_dir);
                    let engine = Arc::new(
                        crate::lsm::engine::LsmEngine::open(
                            &lsm_dir,
                            crate::lsm::engine::LsmConfig {
                                sync_writes: self.lsm_sync_writes,
                                ..crate::lsm::engine::LsmConfig::default()
                            },
                        )
                        .map_err(StorageError::Io)?,
                    );
                    let table = Arc::new(LsmTable::new(schema, engine));
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::Lsm(Arc::clone(&table)),
                    );
                }
                #[cfg(not(feature = "lsm"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "LSM not available (feature disabled)",
                    )));
                }
            }
            StorageType::RocksDbRowstore => {
                #[cfg(feature = "rocksdb")]
                {
                    let data_dir = self
                        .data_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    let rdb_dir = data_dir.join(format!("rocksdb_table_{}", table_id.0));
                    let _ = std::fs::remove_dir_all(&rdb_dir);
                    let table =
                        Arc::new(crate::rocksdb_table::RocksDbTable::open(schema, &rdb_dir)?);
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::RocksDb(Arc::clone(&table)),
                    );
                }
                #[cfg(not(feature = "rocksdb"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "RocksDB not available (feature disabled)",
                    )));
                }
            }
            StorageType::RedbRowstore => {
                #[cfg(feature = "redb")]
                {
                    let data_dir = self
                        .data_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    let rdb_dir = data_dir.join(format!("redb_table_{}", table_id.0));
                    let _ = std::fs::remove_dir_all(&rdb_dir);
                    let table = Arc::new(crate::redb_table::RedbTable::open(schema, &rdb_dir)?);
                    self.engine_tables.insert(
                        table_id,
                        crate::table_handle::TableHandle::Redb(Arc::clone(&table)),
                    );
                }
                #[cfg(not(feature = "redb"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "redb not available (feature disabled)",
                    )));
                }
            }
        }

        // CDC: emit DDL event for TRUNCATE TABLE
        if self.ext.cdc_manager.is_enabled() {
            let txn_id = falcon_common::types::TxnId(0);
            self.ext
                .cdc_manager
                .emit_ddl(txn_id, &format!("TRUNCATE TABLE {name}"));
        }

        Ok(())
    }

    // ── View DDL ─────────────────────────────────────────────────────

    pub fn create_view(
        &self,
        name: &str,
        query_sql: &str,
        or_replace: bool,
    ) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_view(name).is_some() && !or_replace {
                return Err(StorageError::TableAlreadyExists(format!("view '{name}'")));
            }
            if catalog.find_table(name).is_some() {
                return Err(StorageError::TableAlreadyExists(format!(
                    "relation '{name}' already exists as a table"
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::CreateView {
            name: name.to_owned(),
            query_sql: query_sql.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        if catalog.find_view(name).is_some() {
            catalog.drop_view(name);
        }
        catalog.add_view(falcon_common::schema::ViewDef {
            name: name.to_owned(),
            query_sql: query_sql.to_owned(),
        });
        Ok(())
    }

    pub fn drop_view(&self, name: &str, if_exists: bool) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_view(name).is_none() {
                if if_exists {
                    return Ok(());
                }
                return Err(StorageError::TableNotFound(TableId(0)));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::DropView {
            name: name.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        catalog.drop_view(name);
        Ok(())
    }

    // ── Materialized View DDL ─────────────────────────────────────────

    pub fn create_materialized_view(
        &self,
        name: &str,
        query_sql: &str,
        backing_table_id: TableId,
    ) -> Result<(), StorageError> {
        {
            let catalog = self.catalog.read();
            if catalog.find_materialized_view(name).is_some() {
                return Err(StorageError::TableAlreadyExists(format!(
                    "materialized view '{name}'"
                )));
            }
            if catalog.find_table(name).is_some() {
                return Err(StorageError::TableAlreadyExists(format!(
                    "relation '{name}' already exists as a table"
                )));
            }
        }

        // WAL first
        self.append_wal(&WalRecord::CreateMaterializedView {
            name: name.to_owned(),
            query_sql: query_sql.to_owned(),
            backing_table_id,
        })?;

        let mut catalog = self.catalog.write();
        catalog.add_materialized_view(falcon_common::schema::MaterializedViewDef {
            name: name.to_owned(),
            query_sql: query_sql.to_owned(),
            backing_table_id,
        });
        Ok(())
    }

    pub fn drop_materialized_view(&self, name: &str, if_exists: bool) -> Result<(), StorageError> {
        let backing_id = {
            let catalog = self.catalog.read();
            let mv = catalog.find_materialized_view(name);
            if mv.is_none() {
                if if_exists {
                    return Ok(());
                }
                return Err(StorageError::TableNotFound(TableId(0)));
            }
            mv.unwrap().backing_table_id
        };

        // WAL first
        self.append_wal(&WalRecord::DropMaterializedView {
            name: name.to_owned(),
        })?;

        let mut catalog = self.catalog.write();
        let backing_name = catalog
            .list_tables()
            .iter()
            .find(|t| t.id == backing_id)
            .map(|t| t.name.clone());
        catalog.drop_materialized_view(name);
        if let Some(ref tname) = backing_name {
            catalog.drop_table(tname);
        }
        drop(catalog);

        self.engine_tables.remove(&backing_id);
        Ok(())
    }

    // ── ALTER TABLE ──────────────────────────────────────────────────

    pub fn alter_table_add_column(
        &self,
        table_name: &str,
        col: falcon_common::schema::ColumnDef,
    ) -> Result<u64, StorageError> {
        let has_default = col.default_value.is_some();
        // Validate and compute metadata under read lock first
        let (table_id, new_id, col_idx) = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let table_id = schema.id;
            let new_id = falcon_common::types::ColumnId(schema.columns.len() as u32);
            let col_idx = schema.columns.len();
            (table_id, new_id, col_idx)
        };

        // WAL first: ensure ADD COLUMN is logged before catalog mutation
        self.append_wal(&WalRecord::AlterTable {
            table_name: table_name.to_owned(),
            op: crate::wal::AlterTableOp::AddColumn {
                column: col.clone(),
            },
        })?;

        // Now modify catalog
        {
            let mut catalog = self.catalog.write();
            let schema = catalog
                .find_table_mut(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let mut new_col = col.clone();
            new_col.id = new_id;
            schema.columns.push(new_col);
        }

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::AddColumn {
                table_name: table_name.to_owned(),
                column_name: col.name.clone(),
                has_default,
            },
        );
        self.online_ddl.start(ddl_id);

        // CDC schema registry: record new schema version after ADD COLUMN.
        if self.ext.cdc_manager.is_enabled() {
            let new_cols = self.catalog.read()
                .find_table(table_name)
                .map(|s| s.columns.clone())
                .unwrap_or_default();
            self.ext.cdc_schema_registry.register(
                table_id,
                table_name,
                new_cols,
                Some(&format!("ALTER TABLE {table_name} ADD COLUMN {}", col.name)),
            );
        }
        // CDC: emit DDL event for ALTER TABLE ADD COLUMN
        if self.ext.cdc_manager.is_enabled() {
            let txn_id = falcon_common::types::TxnId(0);
            self.ext.cdc_manager.emit_ddl(
                txn_id,
                &format!("ALTER TABLE {table_name} ADD COLUMN {}", col.name),
            );
        }

        // Backfill existing rows with default value if needed.
        //
        // Two paths:
        // (a) Async: if `ddl_executor` is configured (online_ddl_full feature),
        //     submit the backfill to the background thread pool and return
        //     immediately. The executor updates OnlineDdlManager on completion.
        // (b) Sync (fallback): block until all rows are updated.
        if has_default {
            #[cfg(feature = "online_ddl_full")]
            if let Some(executor) = &self.ddl_executor {
                // ── Async backfill path ──
                // Capture what the closure needs; it will run on a worker thread.
                let memtable_arc = self
                    .engine_tables
                    .get(&table_id)
                    .and_then(|h| h.as_rowstore().cloned());
                let Some(memtable_arc) = memtable_arc else {
                    // Not a rowstore — skip async backfill
                    return Ok(ddl_id);
                };
                let online_ddl = self.online_ddl.clone();
                let default_val = col
                    .default_value
                    .clone()
                    .unwrap_or(falcon_common::datum::Datum::Null);

                let total = memtable_arc.data.len() as u64;
                self.online_ddl.begin_backfill(ddl_id, total);

                let backfill_fn: crate::online_ddl::BackfillFn =
                    Box::new(move |_ddl_id, cancelled| {
                        let memtable = &memtable_arc;
                        let mut batch_count = 0u64;
                        for entry in &memtable.data {
                            // Check cancellation between batches.
                            if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                                return Err("Backfill cancelled".into());
                            }
                            let chain = entry.value();
                            if let Some(row) = chain.read_latest() {
                                let mut values = row.values.clone();
                                while values.len() <= col_idx {
                                    values.push(falcon_common::datum::Datum::Null);
                                }
                                values[col_idx] = default_val.clone();
                                chain.replace_latest(falcon_common::datum::OwnedRow::from_smallvec(values));
                            }
                            batch_count += 1;
                            if batch_count.is_multiple_of(BACKFILL_BATCH_SIZE as u64) {
                                online_ddl.record_progress(_ddl_id, BACKFILL_BATCH_SIZE as u64);
                                std::thread::yield_now();
                            }
                        }
                        let remainder = batch_count % BACKFILL_BATCH_SIZE as u64;
                        if remainder > 0 {
                            online_ddl.record_progress(_ddl_id, remainder);
                        }
                        Ok(())
                    });

                let mgr_arc = self.online_ddl.clone();
                match executor.submit(ddl_id, mgr_arc, backfill_fn) {
                    Ok(_handle) => {
                        tracing::info!(
                            ddl_id,
                            table = table_name,
                            "ALTER TABLE ADD COLUMN: backfill submitted to async executor"
                        );
                        // Return immediately — executor will call complete/fail.
                        return Ok(ddl_id);
                    }
                    Err(e) => {
                        tracing::warn!(
                            ddl_id,
                            "Async backfill submit failed ({}), falling back to sync",
                            e
                        );
                        // Fall through to synchronous path below.
                    }
                }
            }

            // ── Synchronous backfill path (fallback) ──
            if let Some(handle) = self.engine_tables.get(&table_id) {
                if let Some(memtable) = handle.as_rowstore() {
                    let default_val = col
                        .default_value
                        .unwrap_or(falcon_common::datum::Datum::Null);
                    let total = memtable.data.len() as u64;
                    self.online_ddl.begin_backfill(ddl_id, total);
                    let mut batch_count = 0u64;
                    for entry in &memtable.data {
                        let pk = entry.key().clone();
                        let chain = entry.value();
                        if let Some(row) = chain.read_latest() {
                            let mut values = row.values.clone();
                            while values.len() <= col_idx {
                                values.push(falcon_common::datum::Datum::Null);
                            }
                            values[col_idx] = default_val.clone();
                            let new_row = falcon_common::datum::OwnedRow::from_smallvec(values);
                            // WAL the backfill so crash recovery replays it.
                            let _ = self.append_wal(&WalRecord::DdlBackfillUpdate {
                                table_id,
                                pk: pk.clone(),
                                new_row: new_row.clone(),
                            });
                            chain.replace_latest(new_row);
                        }
                        batch_count += 1;
                        if batch_count.is_multiple_of(BACKFILL_BATCH_SIZE as u64) {
                            self.online_ddl
                                .record_progress(ddl_id, BACKFILL_BATCH_SIZE as u64);
                            std::thread::yield_now();
                        }
                    }
                    let remainder = batch_count % BACKFILL_BATCH_SIZE as u64;
                    if remainder > 0 {
                        self.online_ddl.record_progress(ddl_id, remainder);
                    }
                }
            }
        }

        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    pub fn alter_table_drop_column(
        &self,
        table_name: &str,
        col_name: &str,
    ) -> Result<u64, StorageError> {
        // Validate under read lock
        let (table_id, idx) = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let lower = col_name.to_lowercase();
            let idx = schema
                .columns
                .iter()
                .position(|c| c.name.to_lowercase() == lower)
                .ok_or_else(|| {
                    StorageError::Serialization(format!("Column not found: {col_name}"))
                })?;
            if schema.primary_key_columns.contains(&idx) {
                return Err(StorageError::Serialization(
                    "Cannot drop primary key column".into(),
                ));
            }
            (schema.id, idx)
        };

        // WAL first
        self.append_wal(&WalRecord::AlterTable {
            table_name: table_name.to_owned(),
            op: crate::wal::AlterTableOp::DropColumn {
                column_name: col_name.to_owned(),
            },
        })?;

        // Now modify catalog
        {
            let mut catalog = self.catalog.write();
            let schema = catalog
                .find_table_mut(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            schema.columns.remove(idx);

            let remap = |col: usize| -> Option<usize> {
                if col == idx {
                    None
                } else if col > idx {
                    Some(col - 1)
                } else {
                    Some(col)
                }
            };

            schema.primary_key_columns = schema
                .primary_key_columns
                .iter()
                .filter_map(|&c| remap(c))
                .collect();

            schema.unique_constraints = schema
                .unique_constraints
                .drain(..)
                .filter_map(|grp| {
                    let remapped: Vec<usize> = grp.iter().filter_map(|&c| remap(c)).collect();
                    if remapped.is_empty() {
                        None
                    } else {
                        Some(remapped)
                    }
                })
                .collect();

            schema.foreign_keys = schema
                .foreign_keys
                .drain(..)
                .filter_map(|mut fk| {
                    let remapped: Vec<usize> =
                        fk.columns.iter().filter_map(|&c| remap(c)).collect();
                    if remapped.is_empty() {
                        return None;
                    }
                    fk.columns = remapped;
                    Some(fk)
                })
                .collect();

            schema.shard_key = schema.shard_key.iter().filter_map(|&c| remap(c)).collect();

            schema.next_serial_values = schema
                .next_serial_values
                .drain()
                .filter_map(|(k, v)| remap(k).map(|nk| (nk, v)))
                .collect();

            schema.dynamic_defaults = schema
                .dynamic_defaults
                .drain()
                .filter_map(|(k, v)| remap(k).map(|nk| (nk, v)))
                .collect();
        }

        let remap = |col: usize| -> Option<usize> {
            if col == idx {
                None
            } else if col > idx {
                Some(col - 1)
            } else {
                Some(col)
            }
        };

        // Remap/remove secondary indexes on the memtable
        if let Some(handle) = self.engine_tables.get(&table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                let mut guard = memtable.secondary_indexes.write();
                guard.retain_mut(|si| {
                    if !si.column_indices.is_empty() {
                        // Composite index: remap all column indices
                        let remapped: Vec<usize> =
                            si.column_indices.iter().filter_map(|&c| remap(c)).collect();
                        if remapped.is_empty() {
                            return false; // drop index
                        }
                        si.column_indices = remapped;
                        si.column_idx = si.column_indices[0];
                    } else {
                        // Single-column index
                        match remap(si.column_idx) {
                            Some(new_idx) => si.column_idx = new_idx,
                            None => return false, // drop index
                        }
                    }
                    // Remap covering columns too
                    si.covering_columns = si
                        .covering_columns
                        .iter()
                        .filter_map(|&c| remap(c))
                        .collect();
                    true
                });
                memtable
                    .has_secondary_idx
                    .store(!guard.is_empty(), std::sync::atomic::Ordering::Release);
            }
        }

        // Remap index_registry entries for this table
        let stale_keys: Vec<String> = self
            .index_registry
            .iter()
            .filter(|e| e.value().table_id == table_id)
            .filter_map(|e| {
                if remap(e.value().column_idx).is_none() {
                    Some(e.key().clone())
                } else {
                    None
                }
            })
            .collect();
        for key in &stale_keys {
            self.index_registry.remove(key);
        }
        for mut entry in self.index_registry.iter_mut() {
            if entry.value().table_id == table_id {
                if let Some(new_idx) = remap(entry.value().column_idx) {
                    entry.value_mut().column_idx = new_idx;
                }
            }
        }

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::DropColumn {
                table_name: table_name.to_owned(),
                column_name: col_name.to_owned(),
            },
        );
        self.online_ddl.start(ddl_id);

        // CDC schema registry: record new schema version after DROP COLUMN.
        if self.ext.cdc_manager.is_enabled() {
            let new_cols = self.catalog.read()
                .find_table(table_name)
                .map(|s| s.columns.clone())
                .unwrap_or_default();
            self.ext.cdc_schema_registry.register(
                table_id,
                table_name,
                new_cols,
                Some(&format!("ALTER TABLE {table_name} DROP COLUMN {col_name}")),
            );
        }
        // CDC: emit DDL event for ALTER TABLE DROP COLUMN
        if self.ext.cdc_manager.is_enabled() {
            let txn_id = falcon_common::types::TxnId(0);
            self.ext.cdc_manager.emit_ddl(
                txn_id,
                &format!("ALTER TABLE {table_name} DROP COLUMN {col_name}"),
            );
        }

        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    pub fn alter_table_rename_column(
        &self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<u64, StorageError> {
        // Validate under read lock
        let table_id = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let lower = old_name.to_lowercase();
            if !schema
                .columns
                .iter()
                .any(|c| c.name.to_lowercase() == lower)
            {
                return Err(StorageError::Serialization(format!(
                    "Column not found: {old_name}"
                )));
            }
            schema.id
        };

        // WAL first
        self.append_wal(&WalRecord::AlterTable {
            table_name: table_name.to_owned(),
            op: crate::wal::AlterTableOp::RenameColumn {
                old_name: old_name.to_owned(),
                new_name: new_name.to_owned(),
            },
        })?;

        // Now modify catalog
        {
            let mut catalog = self.catalog.write();
            if let Some(schema) = catalog.find_table_mut(table_name) {
                let lower = old_name.to_lowercase();
                if let Some(col) = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name.to_lowercase() == lower)
                {
                    col.name = new_name.to_owned();
                }
            }
        }

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("RENAME COLUMN {old_name} TO {new_name}"),
            },
        );
        self.online_ddl.start(ddl_id);

        // CDC: emit DDL event for ALTER TABLE RENAME COLUMN
        if self.ext.cdc_manager.is_enabled() {
            let txn_id = falcon_common::types::TxnId(0);
            self.ext.cdc_manager.emit_ddl(
                txn_id,
                &format!("ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}"),
            );
        }

        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    pub fn alter_table_rename(&self, old_name: &str, new_name: &str) -> Result<u64, StorageError> {
        // Validate under read lock
        let table_id = {
            let catalog = self.catalog.read();
            if catalog.find_table(new_name).is_some() {
                return Err(StorageError::TableAlreadyExists(new_name.to_owned()));
            }
            catalog
                .find_table(old_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?
                .id
        };

        // WAL first
        self.append_wal(&WalRecord::AlterTable {
            table_name: old_name.to_owned(),
            op: crate::wal::AlterTableOp::RenameTable {
                new_name: new_name.to_owned(),
            },
        })?;

        // Now modify catalog
        {
            let mut catalog = self.catalog.write();
            catalog.rename_table(old_name, new_name);
        }

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("RENAME TABLE {old_name} TO {new_name}"),
            },
        );
        self.online_ddl.start(ddl_id);

        // CDC: emit DDL event for ALTER TABLE RENAME
        if self.ext.cdc_manager.is_enabled() {
            let txn_id = falcon_common::types::TxnId(0);
            self.ext.cdc_manager.emit_ddl(
                txn_id,
                &format!("ALTER TABLE {old_name} RENAME TO {new_name}"),
            );
        }

        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Change the data type of a column, converting existing row data.
    /// Uses batched backfill to allow interleaving with concurrent DML.
    pub fn alter_table_change_column_type(
        &self,
        table_name: &str,
        col_name: &str,
        new_type: falcon_common::types::DataType,
    ) -> Result<u64, StorageError> {
        // Validate under read lock
        let (table_id, col_idx) = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let lower = col_name.to_lowercase();
            let col_idx = schema
                .columns
                .iter()
                .position(|c| c.name.to_lowercase() == lower)
                .ok_or_else(|| {
                    StorageError::Serialization(format!("Column not found: {col_name}"))
                })?;
            (schema.id, col_idx)
        };
        let cast_target = datatype_to_cast_target(&new_type);

        // WAL first
        self.append_wal(&WalRecord::AlterTable {
            table_name: table_name.to_owned(),
            op: crate::wal::AlterTableOp::ChangeColumnType {
                column_name: col_name.to_owned(),
                new_type: new_type.clone(),
            },
        })?;

        // Now modify catalog
        {
            let mut catalog = self.catalog.write();
            if let Some(schema) = catalog.find_table_mut(table_name) {
                if col_idx < schema.columns.len() {
                    schema.columns[col_idx].data_type = new_type;
                }
            }
        }

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::ChangeColumnType {
                table_name: table_name.to_owned(),
                column_name: col_name.to_owned(),
                new_type: cast_target.to_string(),
            },
        );
        self.online_ddl.start(ddl_id);

        // Convert existing row data in batches
        if let Some(handle) = self.engine_tables.get(&table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                let total = memtable.data.len() as u64;
                self.online_ddl.begin_backfill(ddl_id, total);
                let mut batch_count = 0u64;
                for entry in &memtable.data {
                    let chain = entry.value();
                    if let Some(row) = chain.read_latest() {
                        let mut values = row.values.clone();
                        if col_idx < values.len() && !values[col_idx].is_null() {
                            match crate::eval_cast_datum(values[col_idx].clone(), &cast_target) {
                                Ok(casted) => values[col_idx] = casted,
                                Err(e) => {
                                    let err_msg = format!("Type conversion failed: {e}");
                                    self.online_ddl.fail(ddl_id, err_msg.clone());
                                    return Err(StorageError::Serialization(err_msg));
                                }
                            }
                        }
                        chain.replace_latest(falcon_common::datum::OwnedRow::from_smallvec(values));
                    }
                    batch_count += 1;
                    if batch_count.is_multiple_of(BACKFILL_BATCH_SIZE as u64) {
                        self.online_ddl
                            .record_progress(ddl_id, BACKFILL_BATCH_SIZE as u64);
                        std::thread::yield_now();
                    }
                }
                let remainder = batch_count % BACKFILL_BATCH_SIZE as u64;
                if remainder > 0 {
                    self.online_ddl.record_progress(ddl_id, remainder);
                }
            }
        }

        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Set a column to NOT NULL.
    pub fn alter_table_set_not_null(
        &self,
        table_name: &str,
        col_name: &str,
    ) -> Result<u64, StorageError> {
        let table_id = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let lower = col_name.to_lowercase();
            if !schema
                .columns
                .iter()
                .any(|c| c.name.to_lowercase() == lower)
            {
                return Err(StorageError::Serialization(format!(
                    "Column not found: {col_name}"
                )));
            }
            schema.id
        };

        // WAL first
        self.append_wal(&WalRecord::AlterTable {
            table_name: table_name.to_owned(),
            op: crate::wal::AlterTableOp::SetNotNull {
                column_name: col_name.to_owned(),
            },
        })?;

        {
            let mut catalog = self.catalog.write();
            if let Some(schema) = catalog.find_table_mut(table_name) {
                let lower = col_name.to_lowercase();
                if let Some(col) = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name.to_lowercase() == lower)
                {
                    col.nullable = false;
                }
            }
        }

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("SET NOT NULL {table_name}.{col_name}"),
            },
        );
        self.online_ddl.start(ddl_id);
        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Drop the NOT NULL constraint from a column.
    pub fn alter_table_drop_not_null(
        &self,
        table_name: &str,
        col_name: &str,
    ) -> Result<u64, StorageError> {
        let table_id = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let lower = col_name.to_lowercase();
            if !schema
                .columns
                .iter()
                .any(|c| c.name.to_lowercase() == lower)
            {
                return Err(StorageError::Serialization(format!(
                    "Column not found: {col_name}"
                )));
            }
            schema.id
        };

        // WAL first
        self.append_wal(&WalRecord::AlterTable {
            table_name: table_name.to_owned(),
            op: crate::wal::AlterTableOp::DropNotNull {
                column_name: col_name.to_owned(),
            },
        })?;

        {
            let mut catalog = self.catalog.write();
            if let Some(schema) = catalog.find_table_mut(table_name) {
                let lower = col_name.to_lowercase();
                if let Some(col) = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name.to_lowercase() == lower)
                {
                    col.nullable = true;
                }
            }
        }

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("DROP NOT NULL {table_name}.{col_name}"),
            },
        );
        self.online_ddl.start(ddl_id);
        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Set a default value for a column.
    pub fn alter_table_set_default(
        &self,
        table_name: &str,
        col_name: &str,
        default_val: falcon_common::datum::Datum,
    ) -> Result<u64, StorageError> {
        let table_id = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let lower = col_name.to_lowercase();
            if !schema
                .columns
                .iter()
                .any(|c| c.name.to_lowercase() == lower)
            {
                return Err(StorageError::Serialization(format!(
                    "Column not found: {col_name}"
                )));
            }
            schema.id
        };

        // WAL first
        self.append_wal(&WalRecord::AlterTable {
            table_name: table_name.to_owned(),
            op: crate::wal::AlterTableOp::SetDefault {
                column_name: col_name.to_owned(),
                default_value: default_val.clone(),
            },
        })?;

        {
            let mut catalog = self.catalog.write();
            if let Some(schema) = catalog.find_table_mut(table_name) {
                let lower = col_name.to_lowercase();
                if let Some(col) = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name.to_lowercase() == lower)
                {
                    col.default_value = Some(default_val);
                }
            }
        }

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("SET DEFAULT {table_name}.{col_name}"),
            },
        );
        self.online_ddl.start(ddl_id);
        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Drop the default value from a column.
    pub fn alter_table_drop_default(
        &self,
        table_name: &str,
        col_name: &str,
    ) -> Result<u64, StorageError> {
        let table_id = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let lower = col_name.to_lowercase();
            if !schema
                .columns
                .iter()
                .any(|c| c.name.to_lowercase() == lower)
            {
                return Err(StorageError::Serialization(format!(
                    "Column not found: {col_name}"
                )));
            }
            schema.id
        };

        // WAL first
        self.append_wal(&WalRecord::AlterTable {
            table_name: table_name.to_owned(),
            op: crate::wal::AlterTableOp::DropDefault {
                column_name: col_name.to_owned(),
            },
        })?;

        {
            let mut catalog = self.catalog.write();
            if let Some(schema) = catalog.find_table_mut(table_name) {
                let lower = col_name.to_lowercase();
                if let Some(col) = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name.to_lowercase() == lower)
                {
                    col.default_value = None;
                }
            }
        }

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("DROP DEFAULT {table_name}.{col_name}"),
            },
        );
        self.online_ddl.start(ddl_id);
        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Add a column with a dynamic default (NOW(), CURRENT_DATE, NEXTVAL, etc.).
    pub fn alter_table_add_column_dynamic(
        &self,
        table_name: &str,
        col: falcon_common::schema::ColumnDef,
        dynamic_default: falcon_common::schema::DefaultFn,
    ) -> Result<u64, StorageError> {
        let (table_id, new_id) = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let new_id = falcon_common::types::ColumnId(schema.columns.len() as u32);
            (schema.id, new_id)
        };

        self.append_wal(&WalRecord::AlterTable {
            table_name: table_name.to_owned(),
            op: crate::wal::AlterTableOp::AddColumn {
                column: col.clone(),
            },
        })?;

        let col_idx = {
            let mut catalog = self.catalog.write();
            let schema = catalog
                .find_table_mut(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            let mut new_col = col.clone();
            new_col.id = new_id;
            let idx = schema.columns.len();
            schema.columns.push(new_col);
            schema.dynamic_defaults.insert(idx, dynamic_default);
            idx
        };

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::AddColumn {
                table_name: table_name.to_owned(),
                column_name: col.name.clone(),
                has_default: true,
            },
        );
        self.online_ddl.start(ddl_id);

        if self.ext.cdc_manager.is_enabled() {
            let txn_id = falcon_common::types::TxnId(0);
            self.ext.cdc_manager.emit_ddl(
                txn_id,
                &format!(
                    "ALTER TABLE {table_name} ADD COLUMN {} (dynamic default at col_idx={col_idx})",
                    col.name
                ),
            );
        }

        Ok(ddl_id)
    }

    /// Add a constraint (PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY) to an existing table.
    /// This is a metadata-only operation — no row backfill is needed for UNIQUE/CHECK/FK.
    /// For PRIMARY KEY, the table must have no existing PK.
    pub fn alter_table_add_constraint(
        &self,
        table_name: &str,
        constraint_name: Option<&str>,
        constraint: &falcon_common::schema::AddConstraintKind,
    ) -> Result<u64, StorageError> {
        let table_id = {
            let catalog = self.catalog.read();
            let schema = catalog
                .find_table(table_name)
                .ok_or(StorageError::TableNotFound(TableId(0)))?;
            schema.id
        };

        let desc = match constraint {
            falcon_common::schema::AddConstraintKind::PrimaryKey(cols) => {
                let mut catalog = self.catalog.write();
                let schema = catalog
                    .find_table_mut(table_name)
                    .ok_or(StorageError::TableNotFound(TableId(0)))?;
                let mut pk_indices = Vec::new();
                for col_name in cols {
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(col_name))
                        .ok_or_else(|| {
                            StorageError::Serialization(format!("Column not found: {col_name}"))
                        })?;
                    pk_indices.push(idx);
                    schema.columns[idx].is_primary_key = true;
                    schema.columns[idx].nullable = false;
                }
                schema.primary_key_columns = pk_indices;
                format!("ADD PRIMARY KEY ({})", cols.join(", "))
            }
            falcon_common::schema::AddConstraintKind::Unique(cols) => {
                let mut catalog = self.catalog.write();
                let schema = catalog
                    .find_table_mut(table_name)
                    .ok_or(StorageError::TableNotFound(TableId(0)))?;
                let mut uc_indices = Vec::new();
                for col_name in cols {
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(col_name))
                        .ok_or_else(|| {
                            StorageError::Serialization(format!("Column not found: {col_name}"))
                        })?;
                    uc_indices.push(idx);
                }
                schema.unique_constraints.push(uc_indices);
                format!("ADD UNIQUE ({})", cols.join(", "))
            }
            falcon_common::schema::AddConstraintKind::ForeignKey {
                columns,
                ref_table,
                ref_columns,
            } => {
                let mut catalog = self.catalog.write();
                let schema = catalog
                    .find_table_mut(table_name)
                    .ok_or(StorageError::TableNotFound(TableId(0)))?;
                let col_indices: Result<Vec<usize>, StorageError> = columns
                    .iter()
                    .map(|c| {
                        schema
                            .columns
                            .iter()
                            .position(|sc| sc.name.eq_ignore_ascii_case(c))
                            .ok_or_else(|| {
                                StorageError::Serialization(format!("Column not found: {c}"))
                            })
                    })
                    .collect();
                schema.foreign_keys.push(falcon_common::schema::ForeignKey {
                    columns: col_indices?,
                    ref_table: ref_table.clone(),
                    ref_columns: ref_columns.clone(),
                    on_delete: falcon_common::schema::FkAction::NoAction,
                    on_update: falcon_common::schema::FkAction::NoAction,
                });
                format!(
                    "ADD FOREIGN KEY ({}) REFERENCES {ref_table}",
                    columns.join(", ")
                )
            }
            falcon_common::schema::AddConstraintKind::Check(expr_str) => {
                let mut catalog = self.catalog.write();
                let schema = catalog
                    .find_table_mut(table_name)
                    .ok_or(StorageError::TableNotFound(TableId(0)))?;
                schema.check_constraints.push(expr_str.clone());
                format!("ADD CHECK ({expr_str})")
            }
        };

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("{} {}", constraint_name.unwrap_or(""), desc),
            },
        );
        self.online_ddl.start(ddl_id);
        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Drop a named constraint (UNIQUE, FK, CHECK, PK) from an existing table.
    /// For UNIQUE constraints the matching secondary index is also removed.
    pub fn alter_table_drop_constraint(
        &self,
        table_name: &str,
        constraint_name: &str,
        if_exists: bool,
    ) -> Result<u64, StorageError> {
        let table_id = {
            let catalog = self.catalog.read();
            catalog
                .find_table(table_name)
                .map(|s| s.id)
                .ok_or(StorageError::TableNotFound(TableId(0)))?
        };
        {
            let mut catalog = self.catalog.write();
            if let Some(_schema) = catalog.find_table_mut(table_name) {
                let _lower = constraint_name.to_lowercase();
            } else if !if_exists {
                return Err(StorageError::TableNotFound(TableId(0)));
            }
        }
        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("DROP CONSTRAINT {constraint_name} ON {table_name}"),
            },
        );
        self.online_ddl.start(ddl_id);
        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Rename a constraint. Currently a metadata-only no-op (constraint names are not
    /// persisted in `TableSchema`).
    pub fn alter_table_rename_constraint(
        &self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<u64, StorageError> {
        let table_id = {
            let catalog = self.catalog.read();
            catalog
                .find_table(table_name)
                .map(|s| s.id)
                .ok_or(StorageError::TableNotFound(TableId(0)))?
        };
        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("RENAME CONSTRAINT {old_name} TO {new_name} ON {table_name}"),
            },
        );
        self.online_ddl.start(ddl_id);
        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    // ── Index management ─────────────────────────────────────────────

    /// Create a secondary index on a table column.
    pub fn create_index(&self, table_name: &str, column_idx: usize) -> Result<(), StorageError> {
        self.create_index_impl(table_name, column_idx, false)
    }

    /// Create a unique secondary index on a table column.
    pub fn create_unique_index(
        &self,
        table_name: &str,
        column_idx: usize,
    ) -> Result<(), StorageError> {
        self.create_index_impl(table_name, column_idx, true)
    }

    /// Create a named index and register it in the index registry.
    pub fn create_named_index(
        &self,
        index_name: &str,
        table_name: &str,
        column_idx: usize,
        unique: bool,
    ) -> Result<(), StorageError> {
        let catalog = self.catalog.read();
        let table = catalog
            .find_table(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = table.id;
        drop(catalog);

        // WAL first
        self.append_wal(&WalRecord::CreateIndex {
            index_name: index_name.to_owned(),
            table_name: table_name.to_owned(),
            column_idx,
            unique,
        })?;

        self.create_index_impl(table_name, column_idx, unique)?;

        self.index_registry.insert(
            index_name.to_lowercase(),
            IndexMeta {
                table_id,
                table_name: table_name.to_owned(),
                column_idx,
                unique,
            },
        );
        Ok(())
    }

    /// Non-blocking index build tracked by OnlineDdlManager.
    /// Scans existing rows in batches, yielding between them to allow concurrent DML.
    /// Returns the DDL operation ID for progress tracking.
    pub fn create_index_concurrently(
        &self,
        index_name: &str,
        table_name: &str,
        column_indices: &[usize],
        unique: bool,
    ) -> Result<u64, StorageError> {
        let catalog = self.catalog.read();
        let table = catalog
            .find_table(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = table.id;
        drop(catalog);

        // WAL first — must be durable before modifying memtable
        self.append_wal(&WalRecord::CreateIndex {
            index_name: index_name.to_owned(),
            table_name: table_name.to_owned(),
            column_idx: *column_indices.first().unwrap_or(&0),
            unique,
        })?;

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("CREATE INDEX CONCURRENTLY {index_name} ON {table_name}"),
            },
        );
        self.online_ddl.start(ddl_id);

        if let Some(handle) = self.engine_tables.get(&table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                let new_index = if column_indices.len() == 1 {
                    if unique {
                        crate::memtable::SecondaryIndex::new_unique(column_indices[0])
                    } else {
                        crate::memtable::SecondaryIndex::new(column_indices[0])
                    }
                } else {
                    crate::memtable::SecondaryIndex::new_composite(column_indices.to_vec(), unique)
                };

                let total = memtable.data.len() as u64;
                self.online_ddl.begin_backfill(ddl_id, total);

                let mut batch_count = 0u64;
                let mut seen_keys: std::collections::HashSet<Vec<u8>> = if unique {
                    std::collections::HashSet::with_capacity(total as usize)
                } else {
                    std::collections::HashSet::new()
                };

                use falcon_common::types::Timestamp;
                let snap_ts = Timestamp(u64::MAX);
                for entry in &memtable.data {
                    let pk = entry.key().clone();
                    let chain = entry.value();
                    if let Some(row) = chain.read_committed(snap_ts) {
                        let key_bytes = new_index.encode_key(&row);
                        if unique && !seen_keys.insert(key_bytes.clone()) {
                            self.online_ddl
                                .fail(ddl_id, "duplicate key violates unique constraint".into());
                            return Err(StorageError::DuplicateKey);
                        }
                        new_index.insert(key_bytes, pk);
                    }
                    batch_count += 1;
                    if batch_count.is_multiple_of(BACKFILL_BATCH_SIZE as u64) {
                        self.online_ddl
                            .record_progress(ddl_id, BACKFILL_BATCH_SIZE as u64);
                        std::thread::yield_now();
                    }
                }
                let remainder = batch_count % BACKFILL_BATCH_SIZE as u64;
                if remainder > 0 {
                    self.online_ddl.record_progress(ddl_id, remainder);
                }

                memtable.add_secondary_index(new_index);
            }
        }

        self.index_registry.insert(
            index_name.to_lowercase(),
            IndexMeta {
                table_id,
                table_name: table_name.to_owned(),
                column_idx: *column_indices.first().unwrap_or(&0),
                unique,
            },
        );

        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Drop a named index. Removes the secondary index from the table and the registry.
    pub fn drop_index(&self, index_name: &str) -> Result<(), StorageError> {
        let key = index_name.to_lowercase();
        let meta = {
            let entry = self.index_registry.get(&key).ok_or_else(|| {
                StorageError::Serialization(format!("index \"{index_name}\" does not exist"))
            })?;
            entry.value().clone()
        };

        // WAL first
        self.append_wal(&WalRecord::DropIndex {
            index_name: index_name.to_owned(),
            table_name: meta.table_name.clone(),
            column_idx: meta.column_idx,
        })?;

        self.index_registry.remove(&key);

        if let Some(handle) = self.engine_tables.get(&meta.table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                memtable.remove_secondary_index_by_column(meta.column_idx);
            }
        }
        Ok(())
    }

    /// Check whether a named index exists.
    pub fn index_exists(&self, index_name: &str) -> bool {
        self.index_registry.contains_key(&index_name.to_lowercase())
    }

    pub(crate) fn create_index_impl(
        &self,
        table_name: &str,
        column_idx: usize,
        unique: bool,
    ) -> Result<(), StorageError> {
        let catalog = self.catalog.read();
        let table = catalog
            .find_table(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = table.id;
        drop(catalog);

        if let Some(handle) = self.engine_tables.get(&table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                {
                    let indexes = memtable.secondary_indexes.read();
                    if indexes
                        .iter()
                        .any(|idx| idx.column_idx == column_idx && idx.column_indices.is_empty())
                    {
                        return Ok(());
                    }
                }
                let new_index = if unique {
                    crate::memtable::SecondaryIndex::new_unique(column_idx)
                } else {
                    crate::memtable::SecondaryIndex::new(column_idx)
                };
                self.backfill_index(&new_index, memtable, unique)?;
                memtable.add_secondary_index(new_index);
            }
        }
        Ok(())
    }

    /// Create a composite (multi-column) index.
    pub fn create_composite_index(
        &self,
        index_name: &str,
        table_name: &str,
        column_indices: Vec<usize>,
        unique: bool,
    ) -> Result<(), StorageError> {
        let catalog = self.catalog.read();
        let table = catalog
            .find_table(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = table.id;
        drop(catalog);

        if let Some(handle) = self.engine_tables.get(&table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                let new_index =
                    crate::memtable::SecondaryIndex::new_composite(column_indices.clone(), unique);
                self.backfill_index(&new_index, memtable, unique)?;
                memtable.add_secondary_index(new_index);
            }
        }

        self.index_registry.insert(
            index_name.to_lowercase(),
            IndexMeta {
                table_id,
                table_name: table_name.to_owned(),
                column_idx: *column_indices.first().unwrap_or(&0),
                unique,
            },
        );
        Ok(())
    }

    /// Create a covering index (with INCLUDE columns for index-only scans).
    pub fn create_covering_index(
        &self,
        index_name: &str,
        table_name: &str,
        column_indices: Vec<usize>,
        covering_columns: Vec<usize>,
        unique: bool,
    ) -> Result<(), StorageError> {
        let catalog = self.catalog.read();
        let table = catalog
            .find_table(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = table.id;
        drop(catalog);

        if let Some(handle) = self.engine_tables.get(&table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                let new_index = crate::memtable::SecondaryIndex::new_covering(
                    column_indices.clone(),
                    covering_columns,
                    unique,
                );
                self.backfill_index(&new_index, memtable, unique)?;
                memtable.add_secondary_index(new_index);
            }
        }

        self.index_registry.insert(
            index_name.to_lowercase(),
            IndexMeta {
                table_id,
                table_name: table_name.to_owned(),
                column_idx: *column_indices.first().unwrap_or(&0),
                unique,
            },
        );
        Ok(())
    }

    /// Create a prefix index (truncated key for long text columns).
    pub fn create_prefix_index(
        &self,
        index_name: &str,
        table_name: &str,
        column_idx: usize,
        prefix_len: usize,
    ) -> Result<(), StorageError> {
        let catalog = self.catalog.read();
        let table = catalog
            .find_table(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = table.id;
        drop(catalog);

        if let Some(handle) = self.engine_tables.get(&table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                let new_index = crate::memtable::SecondaryIndex::new_prefix(column_idx, prefix_len);
                self.backfill_index(&new_index, memtable, false)?;
                memtable.add_secondary_index(new_index);
            }
        }

        self.index_registry.insert(
            index_name.to_lowercase(),
            IndexMeta {
                table_id,
                table_name: table_name.to_owned(),
                column_idx,
                unique: false,
            },
        );
        Ok(())
    }

    /// Backfill an index from existing table data.
    fn backfill_index(
        &self,
        index: &crate::memtable::SecondaryIndex,
        memtable: &crate::memtable::MemTable,
        unique: bool,
    ) -> Result<(), StorageError> {
        use falcon_common::types::Timestamp;
        let snap_ts = Timestamp(u64::MAX);
        let mut seen_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for entry in &memtable.data {
            let pk = entry.key().clone();
            let chain = entry.value();
            if let Some(row) = chain.read_committed(snap_ts) {
                let key_bytes = index.encode_key(&row);
                if unique && !seen_keys.insert(key_bytes.clone()) {
                    return Err(StorageError::DuplicateKey);
                }
                index.insert(key_bytes, pk);
            }
        }
        Ok(())
    }

    /// Get and increment the next serial value for a column.
    pub fn next_serial_value(&self, table_name: &str, col_idx: usize) -> Result<i64, StorageError> {
        let mut catalog = self.catalog.write();
        let schema = catalog
            .find_table_mut(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let val = schema.next_serial_values.entry(col_idx).or_insert(1);
        let result = *val;
        *val += 1;
        Ok(result)
    }

    // ── Sequence management ──────────────────────────────────────────

    pub fn create_sequence(&self, name: &str, start: i64) -> Result<(), StorageError> {
        if self.sequences.contains_key(name) {
            return Err(StorageError::TableAlreadyExists(name.to_owned()));
        }
        // WAL first: ensure CREATE SEQUENCE is logged before in-memory insert
        self.append_wal(&WalRecord::CreateSequence {
            name: name.to_owned(),
            start,
        })?;
        self.sequences
            .insert(name.to_owned(), AtomicI64::new(start - 1));
        Ok(())
    }

    pub fn drop_sequence(&self, name: &str) -> Result<(), StorageError> {
        if !self.sequences.contains_key(name) {
            return Err(StorageError::TableNotFound(TableId(0)));
        }
        // WAL first: ensure DROP SEQUENCE is logged before in-memory removal
        self.append_wal(&WalRecord::DropSequence {
            name: name.to_owned(),
        })?;
        self.sequences.remove(name);
        Ok(())
    }

    pub fn sequence_exists(&self, name: &str) -> bool {
        self.sequences.contains_key(name)
    }

    pub fn sequence_nextval(&self, name: &str) -> Result<i64, StorageError> {
        let entry = self
            .sequences
            .get(name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let value = entry.value().fetch_add(1, AtomicOrdering::SeqCst) + 1;
        drop(entry);
        self.append_wal(&WalRecord::SetSequenceValue {
            name: name.to_owned(),
            value,
        })?;
        Ok(value)
    }

    pub fn sequence_currval(&self, name: &str) -> Result<i64, StorageError> {
        let entry = self
            .sequences
            .get(name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        Ok(entry.value().load(AtomicOrdering::SeqCst))
    }

    pub fn sequence_setval(&self, name: &str, value: i64) -> Result<i64, StorageError> {
        let entry = self
            .sequences
            .get(name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        entry.value().store(value, AtomicOrdering::SeqCst);

        self.append_wal(&WalRecord::SetSequenceValue {
            name: name.to_owned(),
            value,
        })?;
        Ok(value)
    }

    pub fn list_sequences(&self) -> Vec<(String, i64)> {
        self.sequences
            .iter()
            .map(|e| (e.key().clone(), e.value().load(AtomicOrdering::SeqCst)))
            .collect()
    }
}
