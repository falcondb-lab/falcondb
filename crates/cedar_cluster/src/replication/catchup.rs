//! WAL record replay logic for replica catch-up.

use std::collections::HashMap;

use cedar_common::error::CedarError;
use cedar_common::schema::TableSchema;
use cedar_common::types::{TableId, TxnId};
use cedar_storage::engine::StorageEngine;
use cedar_storage::wal::WalRecord;

/// A write operation tracked during WAL replay on a replica.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WriteOp {
    table_id: TableId,
    pk: Vec<u8>,
}

/// Apply a single WAL record to a StorageEngine (replica-side replay).
/// This mirrors the recovery logic in StorageEngine::recover.
pub fn apply_wal_record_to_engine(
    engine: &StorageEngine,
    record: &WalRecord,
    write_sets: &mut HashMap<TxnId, Vec<WriteOp>>,
) -> Result<(), CedarError> {
    match record {
        WalRecord::BeginTxn { .. } | WalRecord::PrepareTxn { .. } => {
            // No-op for replica replay.
        }
        WalRecord::CreateTable { schema_json } => {
            let schema: TableSchema = serde_json::from_str(schema_json)
                .map_err(|e| CedarError::Internal(format!("Schema parse error: {}", e)))?;
            // Skip if already exists.
            if engine.get_catalog().find_table(&schema.name).is_none() {
                engine.create_table(schema)?;
            }
        }
        WalRecord::DropTable { table_name } => {
            engine.drop_table(table_name)?;
        }
        WalRecord::Insert { txn_id, table_id, row } => {
            if let Ok(pk) = engine.insert(*table_id, row.clone(), *txn_id) {
                write_sets
                    .entry(*txn_id)
                    .or_default()
                    .push(WriteOp { table_id: *table_id, pk });
            }
        }
        WalRecord::Update { txn_id, table_id, pk, new_row } => {
            if engine.update(*table_id, pk, new_row.clone(), *txn_id).is_ok() {
                write_sets
                    .entry(*txn_id)
                    .or_default()
                    .push(WriteOp { table_id: *table_id, pk: pk.clone() });
            }
        }
        WalRecord::Delete { txn_id, table_id, pk } => {
            if engine.delete(*table_id, pk, *txn_id).is_ok() {
                write_sets
                    .entry(*txn_id)
                    .or_default()
                    .push(WriteOp { table_id: *table_id, pk: pk.clone() });
            }
        }
        WalRecord::CommitTxn { txn_id, commit_ts }
        | WalRecord::CommitTxnLocal { txn_id, commit_ts }
        | WalRecord::CommitTxnGlobal { txn_id, commit_ts } => {
            // Commit the write set on the replica engine.
            // We use TxnType::Local since replica apply is always local.
            let _ = engine.commit_txn(*txn_id, *commit_ts, cedar_common::types::TxnType::Local);
            write_sets.remove(txn_id);
        }
        WalRecord::AbortTxn { txn_id }
        | WalRecord::AbortTxnLocal { txn_id }
        | WalRecord::AbortTxnGlobal { txn_id } => {
            let _ = engine.abort_txn(*txn_id, cedar_common::types::TxnType::Local);
            write_sets.remove(txn_id);
        }
        WalRecord::CreateView { name, query_sql } => {
            let _ = engine.create_view(name, query_sql, true);
        }
        WalRecord::DropView { name } => {
            let _ = engine.drop_view(name, true);
        }
        WalRecord::AlterTable { table_name, operation_json } => {
            // Replay ALTER TABLE by parsing the operation JSON
            if let Ok(op) = serde_json::from_str::<serde_json::Value>(operation_json) {
                let op_type = op.get("op").and_then(|v| v.as_str()).unwrap_or("");
                match op_type {
                    "add_column" => {
                        if let Some(col_val) = op.get("column") {
                            if let Ok(col) = serde_json::from_value::<cedar_common::schema::ColumnDef>(col_val.clone()) {
                                let _ = engine.alter_table_add_column(table_name, col);
                            }
                        }
                    }
                    "drop_column" => {
                        if let Some(col_name) = op.get("column_name").and_then(|v| v.as_str()) {
                            let _ = engine.alter_table_drop_column(table_name, col_name);
                        }
                    }
                    "rename_column" => {
                        let old_name = op.get("old_name").and_then(|v| v.as_str()).unwrap_or("");
                        let new_name = op.get("new_name").and_then(|v| v.as_str()).unwrap_or("");
                        if !old_name.is_empty() && !new_name.is_empty() {
                            let _ = engine.alter_table_rename_column(table_name, old_name, new_name);
                        }
                    }
                    "rename_table" => {
                        if let Some(new_name) = op.get("new_name").and_then(|v| v.as_str()) {
                            let _ = engine.alter_table_rename(table_name, new_name);
                        }
                    }
                    _ => {}
                }
            }
        }
        WalRecord::CreateSequence { name, start } => {
            let _ = engine.create_sequence(name, *start);
        }
        WalRecord::DropSequence { name } => {
            let _ = engine.drop_sequence(name);
        }
        WalRecord::SetSequenceValue { name, value } => {
            let _ = engine.sequence_setval(name, *value);
        }
        WalRecord::TruncateTable { table_name } => {
            let _ = engine.truncate_table(table_name);
        }
        WalRecord::Checkpoint { .. } => {
            // No-op for replica replay.
        }
        WalRecord::CoordinatorPrepare { .. }
        | WalRecord::CoordinatorCommit { .. }
        | WalRecord::CoordinatorAbort { .. } => {
            // P0-3: Coordinator decision records — no-op on replicas.
        }
    }
    Ok(())
}
