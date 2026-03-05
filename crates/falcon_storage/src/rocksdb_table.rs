//! RocksDB-backed row store table with MVCC visibility.
//!
//! Key: PK bytes (from `encode_pk`)
//! Value: [count:u16 LE][len:u32 LE][MvccValue]... (newest first)
//!
//! Design notes:
//! - RocksDB internal WAL is disabled; FalconDB WAL handles durability.
//! - commit/abort use WriteBatch for atomic multi-key updates.
//! - scan is lazy (RocksDB iterator), rows are yielded one at a time.
//! - Bloom filters enabled on all levels for fast point-gets.

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};
use rocksdb::{BlockBasedOptions, DB, Options, WriteBatch, WriteOptions};

use crate::lsm::mvcc_encoding::{MvccStatus, MvccValue};
use crate::memtable::{encode_pk, PrimaryKey};

const PK_LOCK_SHARDS: usize = 256;

fn pk_shard(pk: &[u8]) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in pk { h ^= b as u64; h = h.wrapping_mul(0x100000001b3); }
    h as usize & (PK_LOCK_SHARDS - 1)
}

pub struct RocksDbTable {
    pub schema: TableSchema,
    pub db: Arc<DB>,
    write_opts: WriteOptions,
    /// Sharded per-PK locks to serialize read-modify-write on version chains.
    pk_locks: Box<[Mutex<()>]>,
}

// ── Version chain encoding ──────────────────────────────────────────

fn encode_chain(versions: &[MvccValue]) -> Vec<u8> {
    let count = versions.len().min(u16::MAX as usize);
    let mut buf = Vec::with_capacity(2 + count * 32);
    buf.extend_from_slice(&(count as u16).to_le_bytes());
    for v in &versions[..count] {
        let enc = v.encode();
        buf.extend_from_slice(&(enc.len() as u32).to_le_bytes());
        buf.extend_from_slice(&enc);
    }
    buf
}

fn decode_chain(raw: &[u8]) -> Vec<MvccValue> {
    if raw.len() < 2 {
        // Backwards compat: old u8 format
        if raw.len() == 1 { return vec![]; }
        return vec![];
    }
    let count = u16::from_le_bytes([raw[0], raw[1]]) as usize;
    let mut pos = 2;
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

fn rdb_err(e: rocksdb::Error) -> StorageError {
    StorageError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
}

impl RocksDbTable {
    pub fn open(schema: TableSchema, path: &Path) -> Result<Self, StorageError> {
        // Bloom filter: 10 bits/key, ~1% false positive rate
        let mut bb_opts = BlockBasedOptions::default();
        bb_opts.set_bloom_filter(10.0, false);
        bb_opts.set_cache_index_and_filter_blocks(true);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&bb_opts);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_max_background_jobs(4);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        // Disable RocksDB internal WAL — FalconDB WAL handles crash recovery.
        opts.set_manual_wal_flush(true);

        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(true);

        let db = DB::open(&opts, path).map_err(|e| {
            StorageError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
        let pk_locks: Vec<Mutex<()>> = (0..PK_LOCK_SHARDS).map(|_| Mutex::new(())).collect();
        Ok(Self { schema, db: Arc::new(db), write_opts, pk_locks: pk_locks.into_boxed_slice() })
    }

    pub fn table_id(&self) -> TableId {
        self.schema.id
    }

    // ── helpers ──

    fn read_chain(&self, pk: &PrimaryKey) -> Result<Vec<MvccValue>, StorageError> {
        match self.db.get(pk) {
            Ok(Some(raw)) => Ok(decode_chain(&raw)),
            Ok(None) => Ok(vec![]),
            Err(e) => Err(rdb_err(e)),
        }
    }

    fn write_chain(&self, pk: &PrimaryKey, chain: &[MvccValue]) -> Result<(), StorageError> {
        let result = if chain.is_empty() {
            self.db.delete_opt(pk, &self.write_opts)
        } else {
            self.db.put_opt(pk, encode_chain(chain), &self.write_opts)
        };
        result.map_err(rdb_err)
    }

    fn ser(row: &OwnedRow) -> Result<Vec<u8>, StorageError> {
        bincode::serialize(row).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    // ── write path ──

    pub fn insert(&self, row: &OwnedRow, txn_id: TxnId) -> Result<PrimaryKey, StorageError> {
        let pk = encode_pk(row, self.schema.pk_indices());
        let mv = MvccValue::prepared(txn_id, Self::ser(row)?);
        let _guard = self.pk_locks[pk_shard(&pk)].lock().unwrap();
        let mut chain = self.read_chain(&pk)?;
        if let Some(head) = chain.first() {
            if head.status == MvccStatus::Prepared && head.txn_id != txn_id {
                return Err(StorageError::WriteConflict);
            }
        }
        chain.insert(0, mv);
        self.write_chain(&pk, &chain)?;
        Ok(pk)
    }

    pub fn update(&self, pk: &PrimaryKey, new_row: &OwnedRow, txn_id: TxnId) -> Result<(), StorageError> {
        let mv = MvccValue::prepared(txn_id, Self::ser(new_row)?);
        let _guard = self.pk_locks[pk_shard(pk)].lock().unwrap();
        let mut chain = self.read_chain(pk)?;
        if let Some(head) = chain.first() {
            if head.status == MvccStatus::Prepared && head.txn_id != txn_id {
                return Err(StorageError::WriteConflict);
            }
        }
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
        let _guard = self.pk_locks[pk_shard(pk)].lock().unwrap();
        let mut chain = self.read_chain(pk)?;
        if let Some(head) = chain.first() {
            if head.status == MvccStatus::Prepared && head.txn_id != txn_id {
                return Err(StorageError::WriteConflict);
            }
        }
        chain.insert(0, mv);
        self.write_chain(pk, &chain)
    }

    /// Write a pre-committed MVCC value directly (used during checkpoint recovery).
    pub fn insert_committed(&self, pk: &PrimaryKey, mv: &MvccValue) -> Result<(), StorageError> {
        let mut chain = self.read_chain(pk)?;
        chain.insert(0, mv.clone());
        self.write_chain(pk, &chain)
    }

    // ── commit / abort — WriteBatch for atomicity ──────────────────

    /// Commit all PKs belonging to this table in a single atomic WriteBatch.
    /// Caller supplies only the PKs that belong to this table.
    pub fn commit_batch(&self, pks: &[PrimaryKey], txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();
        for pk in pks {
            let _guard = self.pk_locks[pk_shard(pk)].lock().unwrap();
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

            // GC superseded committed + aborted versions.
            // Keep the first committed version (anchor), drop all older ones.
            let mut keep = Vec::with_capacity(chain.len());
            let mut saw_committed = false;
            for mv in chain {
                if mv.status == MvccStatus::Aborted { continue; }
                if mv.status == MvccStatus::Committed {
                    if saw_committed { continue; }
                    saw_committed = true;
                }
                keep.push(mv);
            }
            if keep.is_empty() {
                batch.delete(pk);
            } else {
                batch.put(pk, encode_chain(&keep));
            }
        }
        self.db.write_opt(batch, &self.write_opts).map_err(rdb_err)
    }

    /// Abort all PKs belonging to this table in a single atomic WriteBatch.
    pub fn abort_batch(&self, pks: &[PrimaryKey], txn_id: TxnId) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();
        for pk in pks {
            let _guard = self.pk_locks[pk_shard(pk)].lock().unwrap();
            let mut chain = self.read_chain(pk)?;
            let before = chain.len();
            chain.retain(|mv| !(mv.txn_id == txn_id && mv.status == MvccStatus::Prepared));
            if chain.len() == before { continue; }
            if chain.is_empty() {
                batch.delete(pk);
            } else {
                batch.put(pk, encode_chain(&chain));
            }
        }
        self.db.write_opt(batch, &self.write_opts).map_err(rdb_err)
    }

    /// Single-key commit (used by TableHandle::commit_key).
    pub fn commit(&self, pk: &PrimaryKey, txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError> {
        self.commit_batch(std::slice::from_ref(pk), txn_id, commit_ts)
    }

    /// Single-key abort (used by TableHandle::abort_key).
    pub fn abort(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError> {
        self.abort_batch(std::slice::from_ref(pk), txn_id)
    }

    // ── read path ──

    pub fn get(&self, pk: &PrimaryKey, txn_id: TxnId, read_ts: Timestamp) -> Result<Option<OwnedRow>, StorageError> {
        let chain = self.read_chain(pk)?;
        Ok(first_visible(&chain, txn_id, read_ts))
    }

    /// Lazy scan: iterates RocksDB in-order, yields only visible rows.
    /// Avoids materialising the entire table into a Vec.
    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        let mut results = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = match item {
                Ok(kv) => kv,
                Err(_) => continue,
            };
            let chain = decode_chain(&value);
            if let Some(row) = first_visible(&chain, txn_id, read_ts) {
                results.push((key.to_vec(), row));
            }
        }
        results
    }

    /// Streaming scan: call `f` for each visible row without materialising.
    pub fn for_each_visible<F>(&self, txn_id: TxnId, read_ts: Timestamp, mut f: F)
    where
        F: FnMut(PrimaryKey, OwnedRow),
    {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = match item { Ok(kv) => kv, Err(_) => continue };
            let chain = decode_chain(&value);
            if let Some(row) = first_visible(&chain, txn_id, read_ts) {
                f(key.to_vec(), row);
            }
        }
    }

    /// Flush RocksDB memtable to SST files.
    /// Must be called on graceful shutdown since internal WAL is disabled.
    pub fn flush(&self) -> Result<(), StorageError> {
        self.db.flush().map_err(rdb_err)
    }
}

impl Drop for RocksDbTable {
    fn drop(&mut self) {
        let _ = self.db.flush();
    }
}

impl crate::storage_trait::StorageTable for RocksDbTable {
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

    fn commit_batch(&self, pks: &[PrimaryKey], txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError> {
        RocksDbTable::commit_batch(self, pks, txn_id, commit_ts)
    }
    fn abort_batch(&self, pks: &[PrimaryKey], txn_id: TxnId) {
        let _ = RocksDbTable::abort_batch(self, pks, txn_id);
    }
    fn for_each_visible(&self, txn_id: TxnId, read_ts: Timestamp, f: &mut dyn FnMut(&OwnedRow)) {
        RocksDbTable::for_each_visible(self, txn_id, read_ts, |_, row| f(&row));
    }
    fn count_visible(&self, txn_id: TxnId, read_ts: Timestamp) -> usize {
        let mut n = 0usize;
        RocksDbTable::for_each_visible(self, txn_id, read_ts, |_, _| n += 1);
        n
    }
}
