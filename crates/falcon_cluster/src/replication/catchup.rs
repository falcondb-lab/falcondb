//! WAL record replay logic for replica catch-up.

use std::collections::HashMap;

use falcon_common::error::FalconError;
use falcon_common::types::{TableId, TxnId};
use falcon_storage::engine::StorageEngine;
use falcon_storage::wal::WalRecord;

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
) -> Result<(), FalconError> {
    match record {
        WalRecord::BeginTxn { .. }
        | WalRecord::PrepareTxn { .. }
        | WalRecord::Checkpoint { .. }
        | WalRecord::CoordinatorPrepare { .. }
        | WalRecord::CoordinatorCommit { .. }
        | WalRecord::CoordinatorAbort { .. } => {
            // No-op for replica replay.
        }
        WalRecord::CreateDatabase { name, owner } => {
            let _ = engine.create_database(name, owner);
        }
        WalRecord::DropDatabase { name } => {
            let _ = engine.drop_database(name);
        }
        WalRecord::CreateTable { schema } => {
            // Skip if already exists.
            if engine.get_catalog().find_table(&schema.name).is_none() {
                engine.create_table(schema.clone())?;
            }
        }
        WalRecord::DropTable { table_name } => {
            engine.drop_table(table_name)?;
        }
        WalRecord::Insert {
            txn_id,
            table_id,
            row,
        } => {
            if let Ok(pk) = engine.insert(*table_id, row.clone(), *txn_id) {
                write_sets.entry(*txn_id).or_default().push(WriteOp {
                    table_id: *table_id,
                    pk,
                });
            }
        }
        WalRecord::BatchInsert {
            txn_id,
            table_id,
            rows,
        } => {
            for row in rows {
                if let Ok(pk) = engine.insert(*table_id, row.clone(), *txn_id) {
                    write_sets.entry(*txn_id).or_default().push(WriteOp {
                        table_id: *table_id,
                        pk,
                    });
                }
            }
        }
        WalRecord::Update {
            txn_id,
            table_id,
            pk,
            new_row,
        } => {
            if engine
                .update(*table_id, pk, new_row.clone(), *txn_id)
                .is_ok()
            {
                write_sets.entry(*txn_id).or_default().push(WriteOp {
                    table_id: *table_id,
                    pk: pk.clone(),
                });
            }
        }
        WalRecord::Delete {
            txn_id,
            table_id,
            pk,
        } => {
            if engine.delete(*table_id, pk, *txn_id).is_ok() {
                write_sets.entry(*txn_id).or_default().push(WriteOp {
                    table_id: *table_id,
                    pk: pk.clone(),
                });
            }
        }
        WalRecord::CommitTxn { txn_id, commit_ts }
        | WalRecord::CommitTxnLocal { txn_id, commit_ts }
        | WalRecord::CommitTxnGlobal { txn_id, commit_ts } => {
            // Commit the write set on the replica engine.
            // We use TxnType::Local since replica apply is always local.
            let _ = engine.commit_txn(*txn_id, *commit_ts, falcon_common::types::TxnType::Local);
            write_sets.remove(txn_id);
        }
        WalRecord::AbortTxn { txn_id }
        | WalRecord::AbortTxnLocal { txn_id }
        | WalRecord::AbortTxnGlobal { txn_id } => {
            let _ = engine.abort_txn(*txn_id, falcon_common::types::TxnType::Local);
            write_sets.remove(txn_id);
        }
        WalRecord::CreateView { name, query_sql } => {
            let _ = engine.create_view(name, query_sql, true);
        }
        WalRecord::DropView { name } => {
            let _ = engine.drop_view(name, true);
        }
        WalRecord::AlterTable {
            table_name,
            op,
        } => {
            use falcon_storage::wal::AlterTableOp;
            match op {
                AlterTableOp::AddColumn { column } => {
                    let _ = engine.alter_table_add_column(table_name, column.clone());
                }
                AlterTableOp::DropColumn { column_name } => {
                    let _ = engine.alter_table_drop_column(table_name, column_name);
                }
                AlterTableOp::RenameColumn { old_name, new_name } => {
                    let _ = engine.alter_table_rename_column(table_name, old_name, new_name);
                }
                AlterTableOp::RenameTable { new_name } => {
                    let _ = engine.alter_table_rename(table_name, new_name);
                }
                AlterTableOp::ChangeColumnType { column_name, new_type } => {
                    let _ = engine.alter_table_change_column_type(table_name, column_name, new_type.clone());
                }
                AlterTableOp::SetNotNull { column_name } => {
                    let _ = engine.alter_table_set_not_null(table_name, column_name);
                }
                AlterTableOp::DropNotNull { column_name } => {
                    let _ = engine.alter_table_drop_not_null(table_name, column_name);
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
        WalRecord::CreateIndex {
            index_name,
            table_name,
            column_idx,
            unique,
        } => {
            if !engine.index_exists(index_name) {
                let _ = engine.create_named_index(index_name, table_name, *column_idx, *unique);
            }
        }
        WalRecord::DropIndex { index_name, .. } => {
            let _ = engine.drop_index(index_name);
        }
        WalRecord::CreateSchema { name, owner } => {
            let _ = engine.create_schema(name, owner);
        }
        WalRecord::DropSchema { name } => {
            let _ = engine.drop_schema(name);
        }
        WalRecord::CreateRole {
            name,
            can_login,
            is_superuser,
            can_create_db,
            can_create_role,
            password_hash,
        } => {
            let _ = engine.create_role(name, *can_login, *is_superuser, *can_create_db, *can_create_role, password_hash.clone());
        }
        WalRecord::DropRole { name } => {
            let _ = engine.drop_role(name);
        }
        WalRecord::AlterRole { name, opts } => {
            let _ = engine.alter_role(name, opts.password.clone(), opts.can_login, opts.is_superuser, opts.can_create_db, opts.can_create_role);
        }
        WalRecord::GrantPrivilege {
            grantee,
            privilege,
            object_type,
            object_name,
            grantor,
        } => {
            let _ = engine.grant_privilege(grantee, privilege, object_type, object_name, grantor);
        }
        WalRecord::RevokePrivilege {
            grantee,
            privilege,
            object_type,
            object_name,
        } => {
            let _ = engine.revoke_privilege(grantee, privilege, object_type, object_name);
        }
        WalRecord::GrantRole { member, group } => {
            let _ = engine.grant_role_membership(member, group);
        }
        WalRecord::RevokeRole { member, group } => {
            let _ = engine.revoke_role_membership(member, group);
        }
    }
    Ok(())
}
