use std::collections::HashMap;
use std::sync::RwLock;

use falcon_planner::PhysicalPlan;

/// Thread-safe LRU-like query plan cache.
/// Caches `PhysicalPlan` by normalized SQL string.
/// Invalidated on DDL operations (CREATE/DROP/ALTER TABLE, CREATE/DROP VIEW).
pub struct PlanCache {
    inner: RwLock<PlanCacheInner>,
}

struct PlanCacheInner {
    /// SQL -> (plan, access_count)
    entries: HashMap<String, (PhysicalPlan, u64)>,
    /// Maximum number of cached plans.
    capacity: usize,
    /// Total hits (plan found in cache).
    hits: u64,
    /// Total misses (plan not found).
    misses: u64,
    /// Schema generation counter 鈥?incremented on DDL, used to invalidate.
    schema_generation: u64,
    /// Generation at which each entry was cached.
    entry_generations: HashMap<String, u64>,
}

/// Snapshot of plan cache statistics.
#[derive(Debug, Clone)]
pub struct PlanCacheStats {
    pub entries: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate_pct: f64,
}

impl PlanCache {
    /// Create a new plan cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: RwLock::new(PlanCacheInner {
                entries: HashMap::new(),
                capacity,
                hits: 0,
                misses: 0,
                schema_generation: 0,
                entry_generations: HashMap::new(),
            }),
        }
    }

    /// Look up a cached plan by SQL string.
    /// Returns `Some(plan)` if found and still valid, `None` otherwise.
    pub fn get(&self, sql: &str) -> Option<PhysicalPlan> {
        let key = normalize_sql(sql);
        let mut inner = self.inner.write().ok()?;

        // Check if entry exists and is from current schema generation
        let gen_valid = inner.entry_generations.get(&key)
            .map(|g| *g == inner.schema_generation);

        match gen_valid {
            Some(true) => {
                let result = inner.entries.get_mut(&key).map(|(plan, count)| {
                    *count += 1;
                    plan.clone()
                });
                if result.is_some() {
                    inner.hits += 1;
                } else {
                    inner.misses += 1;
                }
                result
            }
            Some(false) => {
                // Stale entry 鈥?remove it
                inner.entries.remove(&key);
                inner.entry_generations.remove(&key);
                inner.misses += 1;
                None
            }
            None => {
                inner.misses += 1;
                None
            }
        }
    }

    /// Insert a plan into the cache.
    pub fn put(&self, sql: &str, plan: PhysicalPlan) {
        let key = normalize_sql(sql);

        // Don't cache DDL or transaction control plans
        if !is_cacheable(&plan) {
            return;
        }

        let Ok(mut inner) = self.inner.write() else { return };

        // Evict if at capacity 鈥?remove least accessed entry
        if inner.entries.len() >= inner.capacity && !inner.entries.contains_key(&key) {
            if let Some(evict_key) = inner.entries.iter()
                .min_by_key(|(_, (_, count))| *count)
                .map(|(k, _)| k.clone())
            {
                inner.entries.remove(&evict_key);
                inner.entry_generations.remove(&evict_key);
            }
        }

        let gen = inner.schema_generation;
        inner.entries.insert(key.clone(), (plan, 1));
        inner.entry_generations.insert(key, gen);
    }

    /// Invalidate all cached plans (called after DDL operations).
    pub fn invalidate(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.schema_generation += 1;
            // Lazily invalidated on next get()
        }
    }

    /// Clear all entries immediately.
    pub fn clear(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.entries.clear();
            inner.entry_generations.clear();
            inner.schema_generation += 1;
        }
    }

    /// Get cache statistics.
    pub fn stats(&self) -> PlanCacheStats {
        let inner = self.inner.read().unwrap_or_else(|p| p.into_inner());
        let total = inner.hits + inner.misses;
        PlanCacheStats {
            entries: inner.entries.len(),
            capacity: inner.capacity,
            hits: inner.hits,
            misses: inner.misses,
            hit_rate_pct: if total > 0 { (inner.hits as f64 / total as f64) * 100.0 } else { 0.0 },
        }
    }
}

/// Normalize SQL for cache key: trim, lowercase, collapse whitespace.
fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Only cache SELECT/INSERT/UPDATE/DELETE plans, not DDL/txn control.
fn is_cacheable(plan: &PhysicalPlan) -> bool {
    matches!(plan,
        PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::NestedLoopJoin { .. }
        | PhysicalPlan::HashJoin { .. }
        | PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. }
        | PhysicalPlan::DistPlan { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};
    use falcon_sql_frontend::types::{BoundProjection, DistinctMode};

    fn dummy_plan() -> PhysicalPlan {
        let schema = TableSchema {
            id: TableId(1),
            name: "t".into(),
            columns: vec![ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            }],
            primary_key_columns: vec![0],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
        ..Default::default()
        };
        PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema,
            projections: vec![BoundProjection::Column(0, "id".into())],
            visible_projection_count: 1,
            filter: None,
            group_by: vec![],
            grouping_sets: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
            distinct: DistinctMode::None,
            ctes: vec![],
            unions: vec![],
            virtual_rows: vec![],
        }
    }

    #[test]
    fn test_put_and_get() {
        let cache = PlanCache::new(10);
        let plan = dummy_plan();
        cache.put("SELECT * FROM t", plan.clone());
        let cached = cache.get("SELECT * FROM t");
        assert!(cached.is_some());
    }

    #[test]
    fn test_miss() {
        let cache = PlanCache::new(10);
        assert!(cache.get("SELECT 1").is_none());
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);
    }

    #[test]
    fn test_invalidate_clears_stale() {
        let cache = PlanCache::new(10);
        cache.put("SELECT * FROM t", dummy_plan());
        assert!(cache.get("SELECT * FROM t").is_some());

        cache.invalidate();
        // After DDL invalidation, cached plan should be gone
        assert!(cache.get("SELECT * FROM t").is_none());
    }

    #[test]
    fn test_capacity_eviction() {
        let cache = PlanCache::new(2);
        cache.put("SELECT 1", dummy_plan());
        cache.put("SELECT 2", dummy_plan());
        // Access SELECT 1 to increase its count
        cache.get("select 1");
        // Adding a third should evict SELECT 2 (lower access count)
        cache.put("SELECT 3", dummy_plan());
        assert!(cache.get("select 1").is_some(), "frequently accessed should survive");
        assert!(cache.get("select 3").is_some());
    }

    #[test]
    fn test_normalize_sql() {
        let cache = PlanCache::new(10);
        cache.put("  SELECT   *   FROM   t  ", dummy_plan());
        assert!(cache.get("SELECT * FROM t").is_some());
    }

    #[test]
    fn test_ddl_not_cached() {
        let cache = PlanCache::new(10);
        let ddl = PhysicalPlan::CreateTable {
            schema: TableSchema {
                id: TableId(1),
                name: "t".into(),
                columns: vec![],
                primary_key_columns: vec![],
                next_serial_values: std::collections::HashMap::new(),
                check_constraints: vec![],
                unique_constraints: vec![],
                foreign_keys: vec![],
            ..Default::default()
            },
            if_not_exists: false,
        };
        cache.put("CREATE TABLE t (id INT)", ddl);
        assert!(cache.get("CREATE TABLE t (id INT)").is_none());
    }

    #[test]
    fn test_stats() {
        let cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_plan());
        cache.get("select 1"); // hit
        cache.get("select 1"); // hit
        cache.get("select 2"); // miss
        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entries, 1);
        assert!(stats.hit_rate_pct > 60.0);
    }
}
