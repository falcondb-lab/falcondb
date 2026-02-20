use crate::memtable::PrimaryKey;

/// Trait for a secondary index structure.
/// MVP: BTreeMap-based index in `MemTable::secondary_indexes`.
/// Future: ART, Bw-Tree, skip list implementations.
pub trait SecondaryIndexEngine: Send + Sync {
    /// Insert a mapping from indexed value to primary key.
    fn insert(&self, index_key: &[u8], pk: &PrimaryKey);

    /// Remove a mapping.
    fn remove(&self, index_key: &[u8], pk: &PrimaryKey);

    /// Lookup all PKs matching an exact key.
    fn lookup(&self, index_key: &[u8]) -> Vec<PrimaryKey>;

    /// Range scan: return all PKs with index key in [start, end).
    fn range_scan(&self, start: &[u8], end: &[u8]) -> Vec<PrimaryKey>;
}
