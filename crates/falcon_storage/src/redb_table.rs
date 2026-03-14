//! redb-backed row store table with MVCC visibility.
//!
//! Same version chain encoding as LsmTable:
//! Key: PK bytes (from `encode_pk`)
//! Value: [count:u8][len:u32 LE][MvccValue]... (newest first)

use std::path::Path;
use std::sync::Arc;

use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};

use crate::lsm::mvcc_encoding::{MvccStatus, MvccValue};
use crate::memtable::{encode_pk, PrimaryKey};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

const KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

pub struct RedbTable {
    pub schema: TableSchema,
    pub db: Arc<Database>,
}

fn encode_chain(versions: &[MvccValue]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + versions.len() * 32);
    buf.push(versions.len() as u8);
    for v in versions {
        let enc = v.encode();
        buf.extend_from_slice(&(enc.len() as u32).to_le_bytes());
        buf.extend_from_slice(&enc);
    }
    buf
}

fn decode_chain(raw: &[u8]) -> Vec<MvccValue> {
    if raw.is_empty() {
        return vec![];
    }
    let count = raw[0] as usize;
    let mut pos = 1;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > raw.len() {
            break;
        }
        let len = u32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > raw.len() {
            break;
        }
        if let Some(mv) = MvccValue::decode(&raw[pos..pos + len]) {
            out.push(mv);
        }
        pos += len;
    }
    out
}

fn first_visible(chain: &[MvccValue], txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
    for mv in chain {
        let vis = match mv.status {
            MvccStatus::Committed => read_ts.0 >= mv.commit_ts.0,
            MvccStatus::Prepared => mv.txn_id == txn_id,
            MvccStatus::Aborted => false,
        };
        if vis {
            if mv.is_tombstone {
                return None;
            }
            return bincode::deserialize(&mv.data).ok();
        }
    }
    None
}

fn map_redb_err<E: std::fmt::Display>(e: E) -> StorageError {
    StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        e.to_string(),
    ))
}

impl RedbTable {
    pub fn open(schema: TableSchema, path: &Path) -> Result<Self, StorageError> {
        std::fs::create_dir_all(path).map_err(StorageError::Io)?;
        let db_path = path.join("redb.db");
        let db = Database::create(db_path).map_err(map_redb_err)?;

        // Ensure KV table exists
        let wtxn = db.begin_write().map_err(map_redb_err)?;
        {
            let _ = wtxn.open_table(KV).map_err(map_redb_err)?;
        }
        wtxn.commit().map_err(map_redb_err)?;

        Ok(Self {
            schema,
            db: Arc::new(db),
        })
    }

    pub fn table_id(&self) -> TableId {
        self.schema.id
    }

    fn read_chain(&self, pk: &PrimaryKey) -> Result<Vec<MvccValue>, StorageError> {
        let rtxn = self.db.begin_read().map_err(map_redb_err)?;
        let table = match rtxn.open_table(KV) {
            Ok(t) => t,
            Err(_) => return Ok(vec![]),
        };
        match table.get(pk.as_slice()).map_err(map_redb_err)? {
            Some(v) => Ok(decode_chain(v.value())),
            None => Ok(vec![]),
        }
    }

    fn write_chain(&self, pk: &PrimaryKey, chain: &[MvccValue]) -> Result<(), StorageError> {
        let wtxn = self.db.begin_write().map_err(map_redb_err)?;
        {
            let mut table = wtxn.open_table(KV).map_err(map_redb_err)?;
            if chain.is_empty() {
                let _ = table.remove(pk.as_slice()).map_err(map_redb_err)?;
            } else {
                let encoded = encode_chain(chain);
                let _ = table
                    .insert(pk.as_slice(), encoded.as_slice())
                    .map_err(map_redb_err)?;
            }
        }
        wtxn.commit().map_err(map_redb_err)?;
        Ok(())
    }

    fn ser(row: &OwnedRow) -> Result<Vec<u8>, StorageError> {
        bincode::serialize(row).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    pub fn insert(&self, row: &OwnedRow, txn_id: TxnId) -> Result<PrimaryKey, StorageError> {
        let pk = encode_pk(row, self.schema.pk_indices());
        let mv = MvccValue::prepared(txn_id, Self::ser(row)?);
        let mut chain = self.read_chain(&pk)?;
        chain.insert(0, mv);
        self.write_chain(&pk, &chain)?;
        Ok(pk)
    }

    pub fn update(
        &self,
        pk: &PrimaryKey,
        new_row: &OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), StorageError> {
        let mv = MvccValue::prepared(txn_id, Self::ser(new_row)?);
        let mut chain = self.read_chain(pk)?;
        chain.insert(0, mv);
        self.write_chain(pk, &chain)
    }

    pub fn delete(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError> {
        let mv = MvccValue {
            txn_id,
            status: MvccStatus::Prepared,
            commit_ts: Timestamp(0),
            is_tombstone: true,
            data: Vec::new(),
        };
        let mut chain = self.read_chain(pk)?;
        chain.insert(0, mv);
        self.write_chain(pk, &chain)
    }

    pub fn commit(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        let mut chain = self.read_chain(pk)?;
        let mut changed = false;
        for mv in &mut chain {
            if mv.txn_id == txn_id && mv.status == MvccStatus::Prepared {
                mv.status = MvccStatus::Committed;
                mv.commit_ts = commit_ts;
                changed = true;
                break;
            }
        }
        if changed {
            let mut keep = Vec::with_capacity(chain.len());
            let mut saw_committed = false;
            for mv in chain {
                if mv.status == MvccStatus::Committed {
                    if saw_committed && !mv.is_tombstone {
                        continue;
                    }
                    saw_committed = true;
                }
                if mv.status == MvccStatus::Aborted {
                    continue;
                }
                keep.push(mv);
            }
            self.write_chain(pk, &keep)?;
        }
        Ok(())
    }

    pub fn abort(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError> {
        let mut chain = self.read_chain(pk)?;
        let before = chain.len();
        chain.retain(|mv| !(mv.txn_id == txn_id && mv.status == MvccStatus::Prepared));
        if chain.len() != before {
            self.write_chain(pk, &chain)?;
        }
        Ok(())
    }

    pub fn get(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Option<OwnedRow>, StorageError> {
        let chain = self.read_chain(pk)?;
        Ok(first_visible(&chain, txn_id, read_ts))
    }

    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        let rtxn = match self.db.begin_read() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let table = match rtxn.open_table(KV) {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let iter = match table.iter() {
            Ok(it) => it,
            Err(_) => return Vec::new(),
        };

        let mut results = Vec::new();
        for item in iter {
            let (k, v) = match item {
                Ok(kv) => kv,
                Err(_) => continue,
            };
            let chain = decode_chain(v.value());
            if let Some(row) = first_visible(&chain, txn_id, read_ts) {
                results.push((k.value().to_vec(), row));
            }
        }
        results
    }
}

impl crate::storage_trait::StorageTable for RedbTable {
    fn schema(&self) -> &falcon_common::schema::TableSchema {
        &self.schema
    }

    fn insert(&self, row: &OwnedRow, txn_id: TxnId) -> Result<PrimaryKey, StorageError> {
        self.insert(row, txn_id)
    }

    fn update(
        &self,
        pk: &PrimaryKey,
        new_row: &OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), StorageError> {
        self.update(pk, new_row, txn_id)
    }

    fn delete(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError> {
        self.delete(pk, txn_id)
    }

    fn get(
        &self,
        pk: &[u8],
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Option<OwnedRow>, StorageError> {
        self.get(pk, txn_id, read_ts)
    }

    fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        self.scan(txn_id, read_ts)
    }

    fn commit_key(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        self.commit(pk, txn_id, commit_ts)
    }

    fn abort_key(&self, pk: &PrimaryKey, txn_id: TxnId) {
        let _ = self.abort(pk, txn_id);
    }
}
