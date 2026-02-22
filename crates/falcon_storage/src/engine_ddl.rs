//! DDL operations on StorageEngine: CREATE/DROP/ALTER TABLE, VIEW, INDEX, SEQUENCE.

use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::Arc;

use falcon_common::error::StorageError;
use falcon_common::schema::{StorageType, TableSchema};
use falcon_common::types::TableId;

use crate::memtable::MemTable;
use crate::online_ddl::{DdlOpKind, BACKFILL_BATCH_SIZE};
use crate::wal::WalRecord;

#[cfg(feature = "columnstore")]
use crate::columnstore::ColumnStoreTable;
#[cfg(not(feature = "columnstore"))]
use crate::columnstore_stub::ColumnStoreTable;

#[cfg(feature = "disk_rowstore")]
use crate::disk_rowstore::DiskRowstoreTable;

#[cfg(feature = "lsm")]
use crate::lsm_table::LsmTable;

use super::engine::{datatype_to_cast_target, IndexMeta, StorageEngine};

impl StorageEngine {
    // ── Table DDL ────────────────────────────────────────────────────

    pub fn create_table(&self, schema: TableSchema) -> Result<TableId, StorageError> {
        let mut catalog = self.catalog.write();

        if catalog.find_table(&schema.name).is_some() {
            return Err(StorageError::TableAlreadyExists(schema.name.clone()));
        }

        let table_id = schema.id;

        // Dispatch by storage type.
        // P0-1: Primary nodes are forbidden from creating COLUMNSTORE / DISK_ROWSTORE tables.
        match schema.storage_type {
            StorageType::Rowstore => {
                let table = Arc::new(MemTable::new(schema.clone()));
                self.tables.insert(table_id, table);
            }
            StorageType::Columnstore => {
                #[cfg(feature = "columnstore")]
                {
                    if !self.node_role.allows_columnstore() {
                        return Err(StorageError::Io(std::io::Error::other(format!(
                            "COLUMNSTORE tables are not allowed on {:?} nodes",
                            self.node_role
                        ))));
                    }
                    let table = Arc::new(ColumnStoreTable::new(schema.clone()));
                    self.columnstore_tables.insert(table_id, table);
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
                    let data_dir = self
                        .data_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    let table = Arc::new(DiskRowstoreTable::new(schema.clone(), data_dir)?);
                    self.disk_tables.insert(table_id, table);
                }
                #[cfg(not(feature = "disk_rowstore"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "DISK_ROWSTORE storage engine is not available in this build (feature disabled)",
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
                    let engine = Arc::new(
                        crate::lsm::engine::LsmEngine::open(
                            &lsm_dir,
                            crate::lsm::engine::LsmConfig::default(),
                        )
                        .map_err(StorageError::Io)?,
                    );
                    let table = Arc::new(LsmTable::new(schema.clone(), engine));
                    self.lsm_tables.insert(table_id, table);
                }
                #[cfg(not(feature = "lsm"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "LSM storage engine is not available in this build (feature disabled)",
                    )));
                }
            }
        }

        // WAL + replication observer
        let json = serde_json::to_string(&schema)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        self.append_and_flush_wal(&WalRecord::CreateTable { schema_json: json })?;

        catalog.add_table(schema);
        Ok(table_id)
    }

    pub fn drop_table(&self, name: &str) -> Result<(), StorageError> {
        let mut catalog = self.catalog.write();
        let table = catalog
            .find_table(name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = table.id;
        // Remove from whichever storage map holds it
        self.tables.remove(&table_id);
        self.columnstore_tables.remove(&table_id);
        #[cfg(feature = "disk_rowstore")]
        self.disk_tables.remove(&table_id);
        #[cfg(feature = "lsm")]
        self.lsm_tables.remove(&table_id);

        self.append_and_flush_wal(&WalRecord::DropTable {
            table_name: name.to_string(),
        })?;

        catalog.drop_table(name);
        Ok(())
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

        // Replace with an empty table of the appropriate storage type
        match schema.storage_type {
            StorageType::Rowstore => {
                self.tables
                    .insert(table_id, Arc::new(MemTable::new(schema)));
            }
            StorageType::Columnstore => {
                #[cfg(feature = "columnstore")]
                {
                    self.columnstore_tables
                        .insert(table_id, Arc::new(ColumnStoreTable::new(schema)));
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
                    self.disk_tables.insert(table_id, table);
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
                            crate::lsm::engine::LsmConfig::default(),
                        )
                        .map_err(StorageError::Io)?,
                    );
                    let table = Arc::new(LsmTable::new(schema, engine));
                    self.lsm_tables.insert(table_id, table);
                }
                #[cfg(not(feature = "lsm"))]
                {
                    return Err(StorageError::Io(std::io::Error::other(
                        "LSM not available (feature disabled)",
                    )));
                }
            }
        }

        self.append_and_flush_wal(&WalRecord::TruncateTable {
            table_name: name.to_string(),
        })?;
        Ok(())
    }

    // ── View DDL ─────────────────────────────────────────────────────

    pub fn create_view(
        &self,
        name: &str,
        query_sql: &str,
        or_replace: bool,
    ) -> Result<(), StorageError> {
        let mut catalog = self.catalog.write();
        if let Some(_existing) = catalog.find_view(name) {
            if or_replace {
                catalog.drop_view(name);
            } else {
                return Err(StorageError::TableAlreadyExists(format!("view '{}'", name)));
            }
        }
        // Also reject if a table with the same name exists
        if catalog.find_table(name).is_some() {
            return Err(StorageError::TableAlreadyExists(format!(
                "relation '{}' already exists as a table",
                name
            )));
        }
        catalog.add_view(falcon_common::schema::ViewDef {
            name: name.to_string(),
            query_sql: query_sql.to_string(),
        });
        drop(catalog);

        self.append_and_flush_wal(&WalRecord::CreateView {
            name: name.to_string(),
            query_sql: query_sql.to_string(),
        })?;
        Ok(())
    }

    pub fn drop_view(&self, name: &str, if_exists: bool) -> Result<(), StorageError> {
        let mut catalog = self.catalog.write();
        if catalog.find_view(name).is_none() {
            if if_exists {
                return Ok(());
            }
            return Err(StorageError::TableNotFound(TableId(0)));
        }
        catalog.drop_view(name);
        drop(catalog);

        self.append_and_flush_wal(&WalRecord::DropView {
            name: name.to_string(),
        })?;
        Ok(())
    }

    // ── ALTER TABLE ──────────────────────────────────────────────────

    pub fn alter_table_add_column(
        &self,
        table_name: &str,
        col: falcon_common::schema::ColumnDef,
    ) -> Result<u64, StorageError> {
        let has_default = col.default_value.is_some();
        let mut catalog = self.catalog.write();
        let schema = catalog
            .find_table_mut(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = schema.id;
        // Assign proper column id
        let new_id = falcon_common::types::ColumnId(schema.columns.len() as u32);
        let mut new_col = col.clone();
        new_col.id = new_id;
        let col_idx = schema.columns.len();
        schema.columns.push(new_col);
        drop(catalog);

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::AddColumn {
                table_name: table_name.to_string(),
                column_name: col.name.clone(),
                has_default,
            },
        );
        self.online_ddl.start(ddl_id);

        let col_json =
            serde_json::to_string(&col).map_err(|e| StorageError::Serialization(e.to_string()))?;
        self.append_and_flush_wal(&WalRecord::AlterTable {
            table_name: table_name.to_string(),
            operation_json: format!(r#"{{"op":"add_column","column":{}}}"#, col_json),
        })?;

        // Backfill existing rows with default value if needed
        if has_default {
            if let Some(table_ref) = self.tables.get(&table_id) {
                let default_val = col
                    .default_value
                    .clone()
                    .unwrap_or(falcon_common::datum::Datum::Null);
                let memtable = table_ref.value();
                let total = memtable.data.len() as u64;
                self.online_ddl.begin_backfill(ddl_id, total);
                let mut batch_count = 0u64;
                for entry in memtable.data.iter() {
                    let chain = entry.value();
                    if let Some(row) = chain.read_latest() {
                        let mut values = row.values.clone();
                        // Pad row to new schema width if needed
                        while values.len() <= col_idx {
                            values.push(falcon_common::datum::Datum::Null);
                        }
                        values[col_idx] = default_val.clone();
                        chain.replace_latest(falcon_common::datum::OwnedRow::new(values));
                    }
                    batch_count += 1;
                    if batch_count.is_multiple_of(BACKFILL_BATCH_SIZE as u64) {
                        self.online_ddl
                            .record_progress(ddl_id, BACKFILL_BATCH_SIZE as u64);
                        std::thread::yield_now();
                    }
                }
                // Record remaining
                let remainder = batch_count % BACKFILL_BATCH_SIZE as u64;
                if remainder > 0 {
                    self.online_ddl.record_progress(ddl_id, remainder);
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
        let mut catalog = self.catalog.write();
        let schema = catalog
            .find_table_mut(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = schema.id;
        let lower = col_name.to_lowercase();
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name.to_lowercase() == lower)
            .ok_or_else(|| {
                StorageError::Serialization(format!("Column not found: {}", col_name))
            })?;
        // Don't allow dropping PK columns
        if schema.primary_key_columns.contains(&idx) {
            return Err(StorageError::Serialization(
                "Cannot drop primary key column".into(),
            ));
        }
        schema.columns.remove(idx);
        // Update PK indices
        schema.primary_key_columns = schema
            .primary_key_columns
            .iter()
            .filter_map(|&pk| {
                if pk == idx {
                    None
                } else if pk > idx {
                    Some(pk - 1)
                } else {
                    Some(pk)
                }
            })
            .collect();
        drop(catalog);

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::DropColumn {
                table_name: table_name.to_string(),
                column_name: col_name.to_string(),
            },
        );
        self.online_ddl.start(ddl_id);

        self.append_and_flush_wal(&WalRecord::AlterTable {
            table_name: table_name.to_string(),
            operation_json: format!(r#"{{"op":"drop_column","column_name":"{}"}}"#, col_name),
        })?;

        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    pub fn alter_table_rename_column(
        &self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<u64, StorageError> {
        let mut catalog = self.catalog.write();
        let schema = catalog
            .find_table_mut(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = schema.id;
        let lower = old_name.to_lowercase();
        let col = schema
            .columns
            .iter_mut()
            .find(|c| c.name.to_lowercase() == lower)
            .ok_or_else(|| {
                StorageError::Serialization(format!("Column not found: {}", old_name))
            })?;
        col.name = new_name.to_string();
        drop(catalog);

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("RENAME COLUMN {} TO {}", old_name, new_name),
            },
        );
        self.online_ddl.start(ddl_id);

        self.append_and_flush_wal(&WalRecord::AlterTable {
            table_name: table_name.to_string(),
            operation_json: format!(
                r#"{{"op":"rename_column","old_name":"{}","new_name":"{}"}}"#,
                old_name, new_name
            ),
        })?;

        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    pub fn alter_table_rename(&self, old_name: &str, new_name: &str) -> Result<u64, StorageError> {
        let mut catalog = self.catalog.write();
        // Check new name doesn't already exist
        if catalog.find_table(new_name).is_some() {
            return Err(StorageError::TableAlreadyExists(new_name.to_string()));
        }
        let table_id = catalog
            .find_table(old_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?
            .id;
        catalog.rename_table(old_name, new_name);
        drop(catalog);

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("RENAME TABLE {} TO {}", old_name, new_name),
            },
        );
        self.online_ddl.start(ddl_id);

        self.append_and_flush_wal(&WalRecord::AlterTable {
            table_name: old_name.to_string(),
            operation_json: format!(r#"{{"op":"rename_table","new_name":"{}"}}"#, new_name),
        })?;

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
        let mut catalog = self.catalog.write();
        let schema = catalog
            .find_table_mut(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let lower = col_name.to_lowercase();
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name.to_lowercase() == lower)
            .ok_or_else(|| {
                StorageError::Serialization(format!("Column not found: {}", col_name))
            })?;
        let table_id = schema.id;
        let cast_target = datatype_to_cast_target(&new_type);
        schema.columns[col_idx].data_type = new_type.clone();
        drop(catalog);

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::ChangeColumnType {
                table_name: table_name.to_string(),
                column_name: col_name.to_string(),
                new_type: format!("{}", new_type),
            },
        );
        self.online_ddl.start(ddl_id);

        // Convert existing row data in batches
        if let Some(table_ref) = self.tables.get(&table_id) {
            let memtable = table_ref.value();
            let total = memtable.data.len() as u64;
            self.online_ddl.begin_backfill(ddl_id, total);
            let mut batch_count = 0u64;
            for entry in memtable.data.iter() {
                let chain = entry.value();
                if let Some(row) = chain.read_latest() {
                    let mut values = row.values.clone();
                    if col_idx < values.len() && !values[col_idx].is_null() {
                        match crate::eval_cast_datum(values[col_idx].clone(), &cast_target) {
                            Ok(casted) => values[col_idx] = casted,
                            Err(e) => {
                                let err_msg = format!("Type conversion failed: {}", e);
                                self.online_ddl.fail(ddl_id, err_msg.clone());
                                return Err(StorageError::Serialization(err_msg));
                            }
                        }
                    }
                    chain.replace_latest(falcon_common::datum::OwnedRow::new(values));
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

        self.online_ddl.complete(ddl_id);
        Ok(ddl_id)
    }

    /// Set a column to NOT NULL.
    pub fn alter_table_set_not_null(
        &self,
        table_name: &str,
        col_name: &str,
    ) -> Result<u64, StorageError> {
        let mut catalog = self.catalog.write();
        let schema = catalog
            .find_table_mut(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = schema.id;
        let lower = col_name.to_lowercase();
        let col = schema
            .columns
            .iter_mut()
            .find(|c| c.name.to_lowercase() == lower)
            .ok_or_else(|| {
                StorageError::Serialization(format!("Column not found: {}", col_name))
            })?;
        col.nullable = false;
        drop(catalog);

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("SET NOT NULL {}.{}", table_name, col_name),
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
        let mut catalog = self.catalog.write();
        let schema = catalog
            .find_table_mut(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = schema.id;
        let lower = col_name.to_lowercase();
        let col = schema
            .columns
            .iter_mut()
            .find(|c| c.name.to_lowercase() == lower)
            .ok_or_else(|| {
                StorageError::Serialization(format!("Column not found: {}", col_name))
            })?;
        col.nullable = true;
        drop(catalog);

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("DROP NOT NULL {}.{}", table_name, col_name),
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
        let mut catalog = self.catalog.write();
        let schema = catalog
            .find_table_mut(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = schema.id;
        let lower = col_name.to_lowercase();
        let col = schema
            .columns
            .iter_mut()
            .find(|c| c.name.to_lowercase() == lower)
            .ok_or_else(|| {
                StorageError::Serialization(format!("Column not found: {}", col_name))
            })?;
        col.default_value = Some(default_val);
        drop(catalog);

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("SET DEFAULT {}.{}", table_name, col_name),
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
        let mut catalog = self.catalog.write();
        let schema = catalog
            .find_table_mut(table_name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table_id = schema.id;
        let lower = col_name.to_lowercase();
        let col = schema
            .columns
            .iter_mut()
            .find(|c| c.name.to_lowercase() == lower)
            .ok_or_else(|| {
                StorageError::Serialization(format!("Column not found: {}", col_name))
            })?;
        col.default_value = None;
        drop(catalog);

        let ddl_id = self.online_ddl.register(
            table_id,
            DdlOpKind::MetadataOnly {
                description: format!("DROP DEFAULT {}.{}", table_name, col_name),
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

        self.create_index_impl(table_name, column_idx, unique)?;

        self.index_registry.insert(
            index_name.to_lowercase(),
            IndexMeta {
                table_id,
                table_name: table_name.to_string(),
                column_idx,
                unique,
            },
        );

        self.append_and_flush_wal(&WalRecord::CreateIndex {
            index_name: index_name.to_string(),
            table_name: table_name.to_string(),
            column_idx,
            unique,
        })?;
        Ok(())
    }

    /// Drop a named index. Removes the secondary index from the table and the registry.
    pub fn drop_index(&self, index_name: &str) -> Result<(), StorageError> {
        let key = index_name.to_lowercase();
        let (_, meta) = self.index_registry.remove(&key).ok_or_else(|| {
            StorageError::Serialization(format!("index \"{}\" does not exist", index_name))
        })?;

        if let Some(table_ref) = self.tables.get(&meta.table_id) {
            let memtable = table_ref.value();
            let mut indexes = memtable.secondary_indexes.write();
            indexes.retain(|idx| idx.column_idx != meta.column_idx);
        }

        self.append_and_flush_wal(&WalRecord::DropIndex {
            index_name: index_name.to_string(),
            table_name: meta.table_name.clone(),
            column_idx: meta.column_idx,
        })?;
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

        if let Some(table_ref) = self.tables.get(&table_id) {
            let memtable = table_ref.value();
            {
                let indexes = memtable.secondary_indexes.read();
                if indexes
                    .iter()
                    .any(|idx| idx.column_idx == column_idx && idx.column_indices.is_empty())
                {
                    return Ok(()); // Already indexed
                }
            }
            let new_index = if unique {
                crate::memtable::SecondaryIndex::new_unique(column_idx)
            } else {
                crate::memtable::SecondaryIndex::new(column_idx)
            };
            // Backfill existing rows (and check uniqueness for unique indexes)
            self.backfill_index(&new_index, memtable, unique)?;
            let mut indexes = memtable.secondary_indexes.write();
            indexes.push(new_index);
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

        if let Some(table_ref) = self.tables.get(&table_id) {
            let memtable = table_ref.value();
            let new_index =
                crate::memtable::SecondaryIndex::new_composite(column_indices.clone(), unique);
            self.backfill_index(&new_index, memtable, unique)?;
            let mut indexes = memtable.secondary_indexes.write();
            indexes.push(new_index);
        }

        self.index_registry.insert(
            index_name.to_lowercase(),
            IndexMeta {
                table_id,
                table_name: table_name.to_string(),
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

        if let Some(table_ref) = self.tables.get(&table_id) {
            let memtable = table_ref.value();
            let new_index = crate::memtable::SecondaryIndex::new_covering(
                column_indices.clone(),
                covering_columns,
                unique,
            );
            self.backfill_index(&new_index, memtable, unique)?;
            let mut indexes = memtable.secondary_indexes.write();
            indexes.push(new_index);
        }

        self.index_registry.insert(
            index_name.to_lowercase(),
            IndexMeta {
                table_id,
                table_name: table_name.to_string(),
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

        if let Some(table_ref) = self.tables.get(&table_id) {
            let memtable = table_ref.value();
            let new_index = crate::memtable::SecondaryIndex::new_prefix(column_idx, prefix_len);
            self.backfill_index(&new_index, memtable, false)?;
            let mut indexes = memtable.secondary_indexes.write();
            indexes.push(new_index);
        }

        self.index_registry.insert(
            index_name.to_lowercase(),
            IndexMeta {
                table_id,
                table_name: table_name.to_string(),
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
        let mut seen_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for entry in memtable.data.iter() {
            let pk = entry.key().clone();
            let chain = entry.value();
            if let Some(row) = chain.read_latest() {
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
            return Err(StorageError::TableAlreadyExists(name.to_string()));
        }
        self.sequences
            .insert(name.to_string(), AtomicI64::new(start - 1));

        self.append_and_flush_wal(&WalRecord::CreateSequence {
            name: name.to_string(),
            start,
        })?;
        Ok(())
    }

    pub fn drop_sequence(&self, name: &str) -> Result<(), StorageError> {
        self.sequences
            .remove(name)
            .ok_or(StorageError::TableNotFound(TableId(0)))?;

        self.append_and_flush_wal(&WalRecord::DropSequence {
            name: name.to_string(),
        })?;
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
        Ok(entry.value().fetch_add(1, AtomicOrdering::SeqCst) + 1)
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
            name: name.to_string(),
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
