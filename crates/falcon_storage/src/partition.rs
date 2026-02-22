//! Table Partitioning — Range, Hash, and List partitioning strategies.
//!
//! Enterprise feature for horizontal data distribution within a single node.
//! Partitioned tables route rows to child partitions based on a partition key,
//! enabling partition pruning during query execution and efficient data lifecycle
//! management (e.g., DROP PARTITION for time-series data).
//!
//! Supported strategies:
//! - **Range**: Partition by value ranges (e.g., date ranges for time-series)
//! - **Hash**: Partition by hash of the key modulo N (uniform distribution)
//! - **List**: Partition by explicit value lists (e.g., region codes)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use falcon_common::datum::Datum;
use falcon_common::types::TableId;

/// Unique identifier for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub u64);

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "partition:{}", self.0)
    }
}

/// Partitioning strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Range partitioning: rows routed by value ranges on the partition key.
    Range,
    /// Hash partitioning: rows routed by hash(partition_key) % num_partitions.
    Hash { modulus: u32 },
    /// List partitioning: rows routed by explicit value lists.
    List,
}

impl fmt::Display for PartitionStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Range => write!(f, "RANGE"),
            Self::Hash { modulus } => write!(f, "HASH({})", modulus),
            Self::List => write!(f, "LIST"),
        }
    }
}

/// A range partition bound (inclusive lower, exclusive upper).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeBound {
    /// Inclusive lower bound. None = MINVALUE (unbounded below).
    pub lower: Option<Datum>,
    /// Exclusive upper bound. None = MAXVALUE (unbounded above).
    pub upper: Option<Datum>,
}

impl RangeBound {
    pub fn new(lower: Option<Datum>, upper: Option<Datum>) -> Self {
        Self { lower, upper }
    }

    /// Check if a value falls within this range [lower, upper).
    pub fn contains(&self, value: &Datum) -> bool {
        let above_lower = match &self.lower {
            Some(lo) => value >= lo,
            None => true,
        };
        let below_upper = match &self.upper {
            Some(hi) => value < hi,
            None => true,
        };
        above_lower && below_upper
    }
}

/// A list partition bound (set of explicit values).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListBound {
    pub values: Vec<Datum>,
}

impl ListBound {
    pub fn new(values: Vec<Datum>) -> Self {
        Self { values }
    }

    pub fn contains(&self, value: &Datum) -> bool {
        self.values.contains(value)
    }
}

/// Partition bound definition (varies by strategy).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionBound {
    Range(RangeBound),
    Hash {
        remainder: u32,
    },
    List(ListBound),
    /// Default partition catches all rows not matching any other partition.
    Default,
}

/// Definition of a single partition within a partitioned table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDef {
    pub id: PartitionId,
    pub name: String,
    /// The actual table that stores this partition's data.
    pub table_id: TableId,
    /// Partition bounds.
    pub bound: PartitionBound,
    /// Whether this partition is attached (active).
    pub attached: bool,
}

/// Partitioned table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionedTableDef {
    /// The parent (partitioned) table ID.
    pub parent_table_id: TableId,
    pub parent_table_name: String,
    /// Column index of the partition key.
    pub partition_key_idx: usize,
    /// Column name of the partition key.
    pub partition_key_name: String,
    /// Partitioning strategy.
    pub strategy: PartitionStrategy,
    /// Child partitions (ordered for range, arbitrary for hash/list).
    pub partitions: Vec<PartitionDef>,
    /// Whether a default partition exists.
    pub has_default: bool,
}

/// Partition Manager — stores partitioned table definitions and provides routing.
#[derive(Debug, Clone, Default)]
pub struct PartitionManager {
    /// Parent table ID → partition definition.
    defs: HashMap<TableId, PartitionedTableDef>,
    /// Next partition ID for auto-increment.
    next_id: u64,
}

impl PartitionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new partitioned table.
    pub fn create_partitioned_table(&mut self, def: PartitionedTableDef) {
        self.defs.insert(def.parent_table_id, def);
    }

    /// Drop a partitioned table definition.
    pub fn drop_partitioned_table(&mut self, parent_id: TableId) -> Option<PartitionedTableDef> {
        self.defs.remove(&parent_id)
    }

    /// Check if a table is partitioned.
    pub fn is_partitioned(&self, table_id: TableId) -> bool {
        self.defs.contains_key(&table_id)
    }

    /// Get the partitioned table definition.
    pub fn get_def(&self, table_id: TableId) -> Option<&PartitionedTableDef> {
        self.defs.get(&table_id)
    }

    /// Add a partition to an existing partitioned table.
    pub fn add_partition(
        &mut self,
        parent_id: TableId,
        name: String,
        child_table_id: TableId,
        bound: PartitionBound,
    ) -> Option<PartitionId> {
        let def = self.defs.get_mut(&parent_id)?;
        let id = PartitionId(self.next_id);
        self.next_id += 1;

        if matches!(bound, PartitionBound::Default) {
            def.has_default = true;
        }

        def.partitions.push(PartitionDef {
            id,
            name,
            table_id: child_table_id,
            bound,
            attached: true,
        });
        Some(id)
    }

    /// Detach a partition (keeps data, removes from routing).
    pub fn detach_partition(
        &mut self,
        parent_id: TableId,
        partition_name: &str,
    ) -> Option<PartitionDef> {
        let def = self.defs.get_mut(&parent_id)?;
        let idx = def
            .partitions
            .iter()
            .position(|p| p.name.eq_ignore_ascii_case(partition_name))?;
        let mut part = def.partitions.remove(idx);
        part.attached = false;
        if matches!(part.bound, PartitionBound::Default) {
            def.has_default = false;
        }
        Some(part)
    }

    /// Route a datum value to the correct partition for a given parent table.
    /// Returns the child TableId to insert into.
    pub fn route(&self, parent_id: TableId, key_value: &Datum) -> Option<TableId> {
        let def = self.defs.get(&parent_id)?;
        let mut default_tid = None;

        for part in &def.partitions {
            if !part.attached {
                continue;
            }
            match &part.bound {
                PartitionBound::Range(range) => {
                    if range.contains(key_value) {
                        return Some(part.table_id);
                    }
                }
                PartitionBound::Hash { remainder } => {
                    if let PartitionStrategy::Hash { modulus } = &def.strategy {
                        let hash = datum_hash(key_value);
                        if hash % (*modulus as u64) == *remainder as u64 {
                            return Some(part.table_id);
                        }
                    }
                }
                PartitionBound::List(list) => {
                    if list.contains(key_value) {
                        return Some(part.table_id);
                    }
                }
                PartitionBound::Default => {
                    default_tid = Some(part.table_id);
                }
            }
        }

        // Fall back to default partition
        default_tid
    }

    /// Prune partitions for a range scan: returns only the child TableIds
    /// that might contain rows matching the given bounds.
    pub fn prune_range(
        &self,
        parent_id: TableId,
        scan_lower: Option<&Datum>,
        scan_upper: Option<&Datum>,
    ) -> Vec<TableId> {
        let Some(def) = self.defs.get(&parent_id) else {
            return vec![];
        };

        let mut result = Vec::new();
        for part in &def.partitions {
            if !part.attached {
                continue;
            }
            match &part.bound {
                PartitionBound::Range(range) => {
                    // Partition overlaps scan range if:
                    // partition.upper > scan_lower AND partition.lower < scan_upper
                    let overlaps_lower = match (&range.upper, scan_lower) {
                        (Some(hi), Some(lo)) => hi > lo,
                        _ => true,
                    };
                    let overlaps_upper = match (&range.lower, scan_upper) {
                        (Some(lo), Some(hi)) => lo < hi,
                        _ => true,
                    };
                    if overlaps_lower && overlaps_upper {
                        result.push(part.table_id);
                    }
                }
                PartitionBound::Default => {
                    result.push(part.table_id);
                }
                _ => {
                    // Hash/List: can't prune with range bounds
                    result.push(part.table_id);
                }
            }
        }
        result
    }

    /// Prune partitions for a list scan (IN (...)).
    pub fn prune_list(&self, parent_id: TableId, values: &[Datum]) -> Vec<TableId> {
        let mut result = Vec::new();
        for value in values {
            if let Some(tid) = self.route(parent_id, value) {
                if !result.contains(&tid) {
                    result.push(tid);
                }
            }
        }
        result
    }

    /// List all partitions for a table.
    pub fn list_partitions(&self, parent_id: TableId) -> Vec<&PartitionDef> {
        self.defs
            .get(&parent_id)
            .map(|def| def.partitions.iter().collect())
            .unwrap_or_default()
    }

    /// Total number of partitioned tables.
    pub fn partitioned_table_count(&self) -> usize {
        self.defs.len()
    }
}

/// Simple hash function for datum values (for hash partitioning).
fn datum_hash(d: &Datum) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    d.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::Datum;

    fn make_partitioned_range_table() -> (PartitionManager, TableId) {
        let mut pm = PartitionManager::new();
        let parent = TableId(100);
        pm.create_partitioned_table(PartitionedTableDef {
            parent_table_id: parent,
            parent_table_name: "orders".into(),
            partition_key_idx: 2,
            partition_key_name: "order_date".into(),
            strategy: PartitionStrategy::Range,
            partitions: vec![],
            has_default: false,
        });
        // Add partitions: 2024, 2025, default
        pm.add_partition(
            parent,
            "orders_2024".into(),
            TableId(101),
            PartitionBound::Range(RangeBound::new(
                Some(Datum::Int32(20240101)),
                Some(Datum::Int32(20250101)),
            )),
        );
        pm.add_partition(
            parent,
            "orders_2025".into(),
            TableId(102),
            PartitionBound::Range(RangeBound::new(
                Some(Datum::Int32(20250101)),
                Some(Datum::Int32(20260101)),
            )),
        );
        pm.add_partition(
            parent,
            "orders_default".into(),
            TableId(199),
            PartitionBound::Default,
        );
        (pm, parent)
    }

    #[test]
    fn test_range_routing() {
        let (pm, parent) = make_partitioned_range_table();
        assert_eq!(
            pm.route(parent, &Datum::Int32(20240615)),
            Some(TableId(101))
        );
        assert_eq!(
            pm.route(parent, &Datum::Int32(20250301)),
            Some(TableId(102))
        );
        // Out of range → default
        assert_eq!(
            pm.route(parent, &Datum::Int32(20230101)),
            Some(TableId(199))
        );
    }

    #[test]
    fn test_range_pruning() {
        let (pm, parent) = make_partitioned_range_table();
        // Scan for 2024 only
        let pruned = pm.prune_range(
            parent,
            Some(&Datum::Int32(20240101)),
            Some(&Datum::Int32(20250101)),
        );
        assert!(pruned.contains(&TableId(101)));
        assert!(!pruned.contains(&TableId(102)));
        assert!(pruned.contains(&TableId(199))); // default always included
    }

    #[test]
    fn test_hash_partitioning() {
        let mut pm = PartitionManager::new();
        let parent = TableId(200);
        pm.create_partitioned_table(PartitionedTableDef {
            parent_table_id: parent,
            parent_table_name: "users".into(),
            partition_key_idx: 0,
            partition_key_name: "user_id".into(),
            strategy: PartitionStrategy::Hash { modulus: 4 },
            partitions: vec![],
            has_default: false,
        });
        for i in 0..4 {
            pm.add_partition(
                parent,
                format!("users_p{}", i),
                TableId(201 + i as u64),
                PartitionBound::Hash { remainder: i },
            );
        }

        // Route various IDs — each should land in exactly one partition
        for id in 0..100 {
            let result = pm.route(parent, &Datum::Int32(id));
            assert!(result.is_some(), "id {} should route to a partition", id);
        }
    }

    #[test]
    fn test_list_partitioning() {
        let mut pm = PartitionManager::new();
        let parent = TableId(300);
        pm.create_partitioned_table(PartitionedTableDef {
            parent_table_id: parent,
            parent_table_name: "sales".into(),
            partition_key_idx: 1,
            partition_key_name: "region".into(),
            strategy: PartitionStrategy::List,
            partitions: vec![],
            has_default: false,
        });
        pm.add_partition(
            parent,
            "sales_us".into(),
            TableId(301),
            PartitionBound::List(ListBound::new(vec![
                Datum::Text("US".into()),
                Datum::Text("CA".into()),
            ])),
        );
        pm.add_partition(
            parent,
            "sales_eu".into(),
            TableId(302),
            PartitionBound::List(ListBound::new(vec![
                Datum::Text("DE".into()),
                Datum::Text("FR".into()),
                Datum::Text("UK".into()),
            ])),
        );
        pm.add_partition(
            parent,
            "sales_default".into(),
            TableId(399),
            PartitionBound::Default,
        );

        assert_eq!(
            pm.route(parent, &Datum::Text("US".into())),
            Some(TableId(301))
        );
        assert_eq!(
            pm.route(parent, &Datum::Text("FR".into())),
            Some(TableId(302))
        );
        assert_eq!(
            pm.route(parent, &Datum::Text("JP".into())),
            Some(TableId(399))
        );
    }

    #[test]
    fn test_detach_partition() {
        let (mut pm, parent) = make_partitioned_range_table();
        assert_eq!(pm.list_partitions(parent).len(), 3);

        let detached = pm.detach_partition(parent, "orders_2024");
        assert!(detached.is_some());
        assert!(!detached.unwrap().attached);
        assert_eq!(pm.list_partitions(parent).len(), 2);

        // Routing for 2024 dates should now go to default
        assert_eq!(
            pm.route(parent, &Datum::Int32(20240615)),
            Some(TableId(199))
        );
    }

    #[test]
    fn test_prune_list_values() {
        let mut pm = PartitionManager::new();
        let parent = TableId(300);
        pm.create_partitioned_table(PartitionedTableDef {
            parent_table_id: parent,
            parent_table_name: "sales".into(),
            partition_key_idx: 1,
            partition_key_name: "region".into(),
            strategy: PartitionStrategy::List,
            partitions: vec![],
            has_default: false,
        });
        pm.add_partition(
            parent,
            "sales_us".into(),
            TableId(301),
            PartitionBound::List(ListBound::new(vec![Datum::Text("US".into())])),
        );
        pm.add_partition(
            parent,
            "sales_eu".into(),
            TableId(302),
            PartitionBound::List(ListBound::new(vec![Datum::Text("DE".into())])),
        );

        let pruned = pm.prune_list(parent, &[Datum::Text("US".into())]);
        assert_eq!(pruned, vec![TableId(301)]);
    }

    #[test]
    fn test_is_partitioned() {
        let (pm, parent) = make_partitioned_range_table();
        assert!(pm.is_partitioned(parent));
        assert!(!pm.is_partitioned(TableId(999)));
    }

    #[test]
    fn test_drop_partitioned_table() {
        let (mut pm, parent) = make_partitioned_range_table();
        assert_eq!(pm.partitioned_table_count(), 1);
        let dropped = pm.drop_partitioned_table(parent);
        assert!(dropped.is_some());
        assert_eq!(pm.partitioned_table_count(), 0);
    }

    #[test]
    fn test_range_bound_contains() {
        let bound = RangeBound::new(Some(Datum::Int32(10)), Some(Datum::Int32(20)));
        assert!(bound.contains(&Datum::Int32(10))); // inclusive lower
        assert!(bound.contains(&Datum::Int32(15)));
        assert!(!bound.contains(&Datum::Int32(20))); // exclusive upper
        assert!(!bound.contains(&Datum::Int32(5)));
    }

    #[test]
    fn test_unbounded_range() {
        let bound = RangeBound::new(None, Some(Datum::Int32(100)));
        assert!(bound.contains(&Datum::Int32(-999)));
        assert!(bound.contains(&Datum::Int32(0)));
        assert!(!bound.contains(&Datum::Int32(100)));

        let bound2 = RangeBound::new(Some(Datum::Int32(100)), None);
        assert!(!bound2.contains(&Datum::Int32(50)));
        assert!(bound2.contains(&Datum::Int32(100)));
        assert!(bound2.contains(&Datum::Int32(9999)));
    }
}
