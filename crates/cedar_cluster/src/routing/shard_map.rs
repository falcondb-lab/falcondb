use cedar_common::types::{NodeId, ShardId, TableId};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

/// Information about a single shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub id: ShardId,
    /// The range of hash values this shard owns: [start, end).
    pub hash_range_start: u64,
    pub hash_range_end: u64,
    /// Leader node for this shard.
    pub leader: NodeId,
    /// Replica nodes (including leader).
    pub replicas: Vec<NodeId>,
}

/// Shard map: maps keys to shards via consistent hashing.
/// MVP: single shard covering the full hash range.
pub struct ShardMap {
    shards: Vec<ShardInfo>,
    num_shards: u64,
}

impl ShardMap {
    /// Create a single-shard map (MVP).
    pub fn single_shard(node_id: NodeId) -> Self {
        let shard = ShardInfo {
            id: ShardId(0),
            hash_range_start: 0,
            hash_range_end: u64::MAX,
            leader: node_id,
            replicas: vec![node_id],
        };
        Self {
            shards: vec![shard],
            num_shards: 1,
        }
    }

    /// Create an N-shard map with uniform hash ranges.
    pub fn uniform(num_shards: u64, node_id: NodeId) -> Self {
        let range_size = u64::MAX / num_shards;
        let shards = (0..num_shards)
            .map(|i| ShardInfo {
                id: ShardId(i),
                hash_range_start: i * range_size,
                hash_range_end: if i == num_shards - 1 {
                    u64::MAX
                } else {
                    (i + 1) * range_size
                },
                leader: node_id,
                replicas: vec![node_id],
            })
            .collect();
        Self { shards, num_shards }
    }

    /// Locate the shard for a given primary key.
    pub fn locate_shard(&self, pk_bytes: &[u8]) -> &ShardInfo {
        let hash = xxh3_64(pk_bytes);
        self.shards
            .iter()
            .find(|s| hash >= s.hash_range_start && hash < s.hash_range_end)
            .unwrap_or(&self.shards[0])
    }

    /// Locate the shard for a given table (for full-table operations).
    /// MVP: all tables on all shards (single shard).
    pub fn shards_for_table(&self, _table_id: TableId) -> Vec<&ShardInfo> {
        self.shards.iter().collect()
    }

    /// Get all shards.
    pub fn all_shards(&self) -> &[ShardInfo] {
        &self.shards
    }

    /// Number of shards.
    pub fn num_shards(&self) -> u64 {
        self.num_shards
    }

    /// Update the leader of a shard (used after promote/failover).
    /// Returns true if the shard was found and updated.
    pub fn update_leader(&mut self, shard_id: ShardId, new_leader: NodeId) -> bool {
        if let Some(shard) = self.shards.iter_mut().find(|s| s.id == shard_id) {
            tracing::info!(
                "ShardMap: shard {:?} leader changed {:?} → {:?}",
                shard_id,
                shard.leader,
                new_leader,
            );
            shard.leader = new_leader;
            true
        } else {
            false
        }
    }

    /// Get shard info by ID.
    pub fn get_shard(&self, shard_id: ShardId) -> Option<&ShardInfo> {
        self.shards.iter().find(|s| s.id == shard_id)
    }

    /// Get mutable shard info by ID.
    pub fn get_shard_mut(&mut self, shard_id: ShardId) -> Option<&mut ShardInfo> {
        self.shards.iter_mut().find(|s| s.id == shard_id)
    }

    /// Split a shard into two halves.  The original shard keeps the lower half
    /// of its hash range; a new shard gets the upper half.
    /// Returns the new shard's ID, or None if the shard was not found or
    /// the range is too small to split.
    pub fn split_shard(&mut self, shard_id: ShardId) -> Option<ShardId> {
        let idx = self.shards.iter().position(|s| s.id == shard_id)?;
        let old = &self.shards[idx];
        let range = old.hash_range_end.wrapping_sub(old.hash_range_start);
        if range < 2 {
            return None; // too small
        }
        let mid = old.hash_range_start + range / 2;
        let new_id = ShardId(self.shards.iter().map(|s| s.id.0).max().unwrap_or(0) + 1);
        let leader = old.leader;
        let replicas = old.replicas.clone();

        // New shard: [mid, old_end)
        let new_shard = ShardInfo {
            id: new_id,
            hash_range_start: mid,
            hash_range_end: old.hash_range_end,
            leader,
            replicas,
        };

        // Shrink original: [old_start, mid)
        self.shards[idx].hash_range_end = mid;

        self.shards.push(new_shard);
        self.num_shards += 1;

        tracing::info!(
            "ShardMap: split shard {:?} at midpoint {} -> new shard {:?}",
            shard_id, mid, new_id
        );

        Some(new_id)
    }

    /// Merge two adjacent shards into one.  The shard with the lower hash
    /// range start absorbs the other.  Returns true on success.
    pub fn merge_shards(&mut self, shard_a: ShardId, shard_b: ShardId) -> bool {
        let idx_a = match self.shards.iter().position(|s| s.id == shard_a) {
            Some(i) => i,
            None => return false,
        };
        let idx_b = match self.shards.iter().position(|s| s.id == shard_b) {
            Some(i) => i,
            None => return false,
        };

        // Determine which shard has the lower start
        let (lo_idx, hi_idx) = if self.shards[idx_a].hash_range_start <= self.shards[idx_b].hash_range_start {
            (idx_a, idx_b)
        } else {
            (idx_b, idx_a)
        };

        // Verify adjacency: lo.end == hi.start
        if self.shards[lo_idx].hash_range_end != self.shards[hi_idx].hash_range_start {
            tracing::warn!(
                "ShardMap: cannot merge non-adjacent shards {:?} and {:?}",
                shard_a, shard_b
            );
            return false;
        }

        let new_end = self.shards[hi_idx].hash_range_end;
        let merged_id = self.shards[lo_idx].id;

        // Expand low shard to cover both ranges
        self.shards[lo_idx].hash_range_end = new_end;

        // Remove high shard
        self.shards.remove(hi_idx);
        self.num_shards -= 1;

        tracing::info!(
            "ShardMap: merged shards {:?} and {:?} -> {:?}",
            shard_a, shard_b, merged_id
        );

        true
    }

    /// Locate the shard for a pre-computed hash value.
    pub fn locate_shard_by_hash(&self, hash: u64) -> &ShardInfo {
        self.shards
            .iter()
            .find(|s| hash >= s.hash_range_start && hash < s.hash_range_end)
            .unwrap_or(&self.shards[0])
    }
}
