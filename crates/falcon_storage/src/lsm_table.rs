//! LSM-backed row store table with MVCC visibility.
//!
//! Key encoding: PK bytes (from `encode_pk`)
//! Value encoding: version chain — [count:u8][len:u32 LE][MvccValue]...
//! Each MvccValue wraps bincode-serialized `OwnedRow` with txn metadata.
//! Newest version first in the chain.

use std::sync::Arc;

use falcon_common::datum::OwnedRow;
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};

use crate::lsm::engine::LsmEngine;
use crate::lsm::mvcc_encoding::{MvccStatus, MvccValue};
use crate::memtable::{encode_pk, PrimaryKey};
use falcon_common::error::StorageError;

pub struct LsmTable {
    pub schema: TableSchema,
    pub engine: Arc<LsmEngine>,
}

// ── Version chain encoding ──────────────────────────────────────────

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
        if pos + 4 > raw.len() { break; }
        let len = u32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > raw.len() { break; }
        if let Some(mv) = MvccValue::decode(&raw[pos..pos + len]) {
            out.push(mv);
        }
        pos += len;
    }
    out
}

/// Find the first visible version in a chain and return the deserialized row.
fn first_visible(chain: &[MvccValue], txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
    for mv in chain {
        let vis = match mv.status {
            MvccStatus::Committed => read_ts.0 >= mv.commit_ts.0,
            MvccStatus::Prepared => mv.txn_id == txn_id,
            MvccStatus::Aborted => false,
        };
        if vis {
            if mv.is_tombstone { return None; }
            return bincode::deserialize(&mv.data).ok();
        }
    }
    None
}

impl LsmTable {
    pub fn new(schema: TableSchema, engine: Arc<LsmEngine>) -> Self {
        Self { schema, engine }
    }

    pub fn table_id(&self) -> TableId {
        self.schema.id
    }

    // ── helpers ──

    fn read_chain(&self, pk: &PrimaryKey) -> Result<Vec<MvccValue>, StorageError> {
        match self.engine.get(pk).map_err(StorageError::Io)? {
            Some(raw) => Ok(decode_chain(&raw)),
            None => Ok(vec![]),
        }
    }

    fn write_chain(&self, pk: &PrimaryKey, chain: &[MvccValue]) -> Result<(), StorageError> {
        if chain.is_empty() {
            self.engine.delete(pk).map_err(StorageError::Io)
        } else {
            self.engine.put(pk, &encode_chain(chain)).map_err(StorageError::Io)
        }
    }

    fn ser(row: &OwnedRow) -> Result<Vec<u8>, StorageError> {
        bincode::serialize(row).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    // ── write path ──

    pub fn insert(&self, row: &OwnedRow, txn_id: TxnId) -> Result<PrimaryKey, StorageError> {
        let pk = encode_pk(row, self.schema.pk_indices());
        let mv = MvccValue::prepared(txn_id, Self::ser(row)?);
        let mut chain = self.read_chain(&pk)?;
        chain.insert(0, mv); // newest first
        self.write_chain(&pk, &chain)?;
        Ok(pk)
    }

    pub fn update(&self, pk: &PrimaryKey, new_row: &OwnedRow, txn_id: TxnId) -> Result<(), StorageError> {
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

    // ── commit / abort ──

    pub fn commit(&self, pk: &PrimaryKey, txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError> {
        self.commit_batch(&[pk.clone()], txn_id, commit_ts)
    }

    /// Commit multiple PKs in a single put_batch call (one memtable lock per shard).
    pub fn commit_batch(&self, pks: &[PrimaryKey], txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError> {
        let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(pks.len());
        let mut deletes: Vec<Vec<u8>> = Vec::new();
        for pk in pks {
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
            if !changed { continue; }
            let mut keep = Vec::with_capacity(chain.len());
            let mut saw_committed = false;
            for mv in chain {
                if mv.status == MvccStatus::Aborted { continue; }
                if mv.status == MvccStatus::Committed {
                    if saw_committed && !mv.is_tombstone { continue; }
                    saw_committed = true;
                }
                keep.push(mv);
            }
            if keep.is_empty() {
                deletes.push(pk.clone());
            } else {
                batch.push((pk.clone(), encode_chain(&keep)));
            }
        }
        for del in deletes {
            self.engine.delete(&del).map_err(StorageError::Io)?;
        }
        if !batch.is_empty() {
            self.engine.put_batch(&batch).map_err(StorageError::Io)?;
        }
        Ok(())
    }

    pub fn abort(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError> {
        self.abort_batch(&[pk.clone()], txn_id)
    }

    pub fn abort_batch(&self, pks: &[PrimaryKey], txn_id: TxnId) -> Result<(), StorageError> {
        let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(pks.len());
        let mut deletes: Vec<Vec<u8>> = Vec::new();
        for pk in pks {
            let mut chain = self.read_chain(pk)?;
            let before = chain.len();
            chain.retain(|mv| !(mv.txn_id == txn_id && mv.status == MvccStatus::Prepared));
            if chain.len() == before { continue; }
            if chain.is_empty() {
                deletes.push(pk.clone());
            } else {
                batch.push((pk.clone(), encode_chain(&chain)));
            }
        }
        for del in deletes {
            self.engine.delete(&del).map_err(StorageError::Io)?;
        }
        if !batch.is_empty() {
            self.engine.put_batch(&batch).map_err(StorageError::Io)?;
        }
        Ok(())
    }

    // ── read path ──

    pub fn get(&self, pk: &PrimaryKey, txn_id: TxnId, read_ts: Timestamp) -> Result<Option<OwnedRow>, StorageError> {
        let chain = self.read_chain(pk)?;
        Ok(first_visible(&chain, txn_id, read_ts))
    }

    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        self.scan_range(None, None, txn_id, read_ts)
    }

    /// Range scan over PK bytes [start, end). Pass None for open-ended bounds.
    pub fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Vec<(PrimaryKey, OwnedRow)> {
        let entries = match self.engine.scan_range(start, end) {
            Ok(e) => e,
            Err(_) => return Vec::new(),
        };
        let mut results = Vec::new();
        for (key, value) in entries {
            let chain = decode_chain(&value);
            if let Some(row) = first_visible(&chain, txn_id, read_ts) {
                results.push((key, row));
            }
        }
        results
    }
}

impl crate::storage_trait::StorageTable for LsmTable {
    fn schema(&self) -> &falcon_common::schema::TableSchema { &self.schema }

    fn insert(&self, row: &OwnedRow, txn_id: TxnId) -> Result<PrimaryKey, StorageError> {
        self.insert(row, txn_id)
    }

    fn update(&self, pk: &PrimaryKey, new_row: &OwnedRow, txn_id: TxnId) -> Result<(), StorageError> {
        self.update(pk, new_row, txn_id)
    }

    fn delete(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError> {
        self.delete(pk, txn_id)
    }

    fn get(&self, pk: &PrimaryKey, txn_id: TxnId, read_ts: Timestamp) -> Result<Option<OwnedRow>, StorageError> {
        self.get(pk, txn_id, read_ts)
    }

    fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        self.scan(txn_id, read_ts)
    }

    fn commit_key(&self, pk: &PrimaryKey, txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError> {
        self.commit(pk, txn_id, commit_ts)
    }

    fn abort_key(&self, pk: &PrimaryKey, txn_id: TxnId) {
        let _ = self.abort(pk, txn_id);
    }
}
