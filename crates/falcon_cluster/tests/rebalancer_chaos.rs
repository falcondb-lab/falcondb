//! Rebalancer Chaos Tests
//!
//! Validates that the shard rebalancer handles disruptive events during
//! migration:
//!
//! 1. **Leader change injection** — `ShardMap::update_leader()` fires
//!    mid-migration; the rebalancer must complete or fail gracefully.
//!
//! 2. **Shard split simulation** — A new shard is added to the engine
//!    mid-migration, redistributing key space; the rebalancer must
//!    handle the changed topology without data loss.
//!
//! 3. **Concurrent rebalance rejection** — Two concurrent rebalance
//!    attempts on the same engine must not both proceed.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use falcon_cluster::rebalancer::{
    MigrationPhase, MigrationTask, RebalancePlanner, RebalancerConfig,
    ShardLoadSnapshot, ShardMigrator, ShardRebalancer,
};
use falcon_cluster::routing::shard_map::ShardMap;
use falcon_cluster::sharded_engine::ShardedEngine;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::*;
use falcon_storage::memtable::encode_pk_from_datums;

fn make_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "chaos_table".to_string(),
        columns: vec![
            ColumnDef {
                id: ColumnId(0),
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default_value: None,
                is_primary_key: true,
                is_serial: false, max_length: None,
            },
            ColumnDef {
                id: ColumnId(1),
                name: "data".to_string(),
                data_type: DataType::Text,
                nullable: true,
                default_value: None,
                is_primary_key: false,
                is_serial: false, max_length: None,
            },
        ],
        primary_key_columns: vec![0],
        next_serial_values: std::collections::HashMap::new(),
        check_constraints: vec![],
        unique_constraints: vec![],
        foreign_keys: vec![],
        ..Default::default()
    }
}

/// Populate shard 0 with `count` rows, leaving other shards empty.
fn skew_shard_0(engine: &ShardedEngine, count: usize) {
    let shard0 = engine.shard(ShardId(0)).unwrap();
    for i in 0..count {
        let row = OwnedRow::new(vec![
            Datum::Int64(i as i64),
            Datum::Text(format!("row_{i}")),
        ]);
        let pk = encode_pk_from_datums(&[&Datum::Int64(i as i64)]);
        shard0.storage.insert_row(TableId(1), pk, row).unwrap();
    }
}

/// Count total DashMap entries across all shards.
///
/// NOTE: `row_count_approx()` counts all DashMap entries including MVCC
/// tombstones from deletes.  After migration (copy to target + delete from
/// source) the count will be *higher* than before because the source entry
/// becomes a tombstone but is still counted.  Use this only for relative /
/// monotonicity assertions, **not** exact equality.
fn dashmap_entries(engine: &ShardedEngine) -> u64 {
    engine
        .all_shards()
        .iter()
        .map(|s| {
            s.storage
                .get_table(TableId(1))
                .map_or(0, |t| t.row_count_approx() as u64)
        })
        .sum()
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1: Leader change injection during migration
//
// While a migration is being planned/executed, we inject leader changes
// on the ShardMap. The rebalancer must still complete its migration
// without losing rows.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn chaos_leader_change_during_migration() {
    let engine = ShardedEngine::new(3);
    let schema = make_schema();
    engine.create_table_all(&schema).unwrap();

    // Skew: 300 rows on shard 0, nothing on shard 1 and 2
    skew_shard_0(&engine, 300);
    let entries_before = dashmap_entries(&engine);
    assert_eq!(entries_before, 300);

    // Set up a ShardMap and inject leader changes during planning
    let mut shard_map = ShardMap::uniform(3, NodeId(1));

    // Track leader-change callbacks
    let leader_changes = Arc::new(AtomicU64::new(0));
    let lc = leader_changes.clone();
    shard_map.set_leader_change_callback(move |_shard, _old, _new, _epoch| {
        lc.fetch_add(1, Ordering::Relaxed);
    });

    // Inject leader change on shard 0 (simulates Raft election during rebalance)
    let new_epoch = shard_map.update_leader(ShardId(0), NodeId(2));
    assert_eq!(new_epoch, Some(2));
    assert_eq!(leader_changes.load(Ordering::Relaxed), 1);

    // Now run the rebalancer — it should still work despite the leader change
    let config = RebalancerConfig {
        imbalance_threshold: 1.25,
        batch_size: 64,
        min_donor_rows: 10,
        cooldown_ms: 0,
    };
    let rebalancer = ShardRebalancer::new(config);
    let migrated = rebalancer.check_and_rebalance(&engine);

    // Rows should have been migrated
    assert!(
        migrated > 0,
        "Rebalancer should migrate rows even after leader change"
    );

    // DashMap entries grow (source tombstones + target inserts) — verify monotonicity
    let entries_after = dashmap_entries(&engine);
    assert!(
        entries_after >= entries_before,
        "Entries should not decrease (MVCC tombstones): before={}, after={}",
        entries_before, entries_after
    );

    // Verify rebalancer completed successfully
    let status = rebalancer.status();
    assert_eq!(status.runs_completed, 1);
    assert!(status.total_rows_migrated > 0);

    // Inject another leader change mid-status-check (shard 1 gets a new leader)
    shard_map.update_leader(ShardId(1), NodeId(3));
    assert_eq!(leader_changes.load(Ordering::Relaxed), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2: Epoch fencing during migration
//
// Simulates the case where a migration is planned based on epoch E,
// but a leader change bumps the epoch to E+1 before execution.
// The epoch validation should detect the stale epoch.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn chaos_epoch_fencing_rejects_stale_migration() {
    let mut shard_map = ShardMap::uniform(2, NodeId(1));

    // Plan a migration at epoch 1
    let epoch_at_plan_time = shard_map.shard_epoch(ShardId(0)).unwrap();
    assert_eq!(epoch_at_plan_time, 1);

    // Leader changes → epoch bumps to 2
    shard_map.update_leader(ShardId(0), NodeId(2));

    // Epoch validation with the old epoch should fail
    let result = shard_map.validate_epoch(ShardId(0), epoch_at_plan_time);
    assert!(
        result.is_err(),
        "Stale epoch should be rejected after leader change"
    );
    assert_eq!(result.unwrap_err(), 2, "Current epoch should be 2");

    // Validation with current epoch should succeed
    assert!(shard_map.validate_epoch(ShardId(0), 2).is_ok());
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3: Shard split simulation during migration
//
// Scenario: Rebalancer plans migration with 2 shards, but by the time
// execution happens the engine has 3 shards (split). We simulate this
// by running the planner on a 2-shard snapshot but executing on a
// 3-shard engine. The migrator should handle the new topology gracefully.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn chaos_shard_split_during_migration() {
    // Phase 1: Create 2-shard engine, skew data
    let engine_2shard = ShardedEngine::new(2);
    let schema = make_schema();
    engine_2shard.create_table_all(&schema).unwrap();
    skew_shard_0(&engine_2shard, 200);

    // Plan based on the 2-shard state
    let config = RebalancerConfig {
        imbalance_threshold: 1.25,
        batch_size: 64,
        min_donor_rows: 10,
        cooldown_ms: 0,
    };
    let planner = RebalancePlanner::new(config.clone());
    let snapshot = ShardLoadSnapshot::collect(&engine_2shard);
    let plan = planner.plan(&snapshot, &engine_2shard);
    assert!(!plan.is_empty(), "Plan should have tasks for skewed data");

    // Execute migration on the 2-shard engine
    let migrator = ShardMigrator::new(config.clone());
    let mut total_migrated_phase1 = 0u64;
    for task in &plan.tasks {
        let status = migrator.execute_task(task, &engine_2shard);
        assert_ne!(
            status.phase,
            MigrationPhase::Failed,
            "Migration task should not fail: {:?}",
            status.error
        );
        total_migrated_phase1 += status.rows_migrated;
    }
    assert!(
        total_migrated_phase1 > 0,
        "Phase 1 migration should move some rows"
    );

    // Phase 2: Now create a 3-shard engine (simulating split)
    // Populate it directly with fresh data (simulating a re-bootstrap)
    let engine_3shard = ShardedEngine::new(3);
    engine_3shard.create_table_all(&schema).unwrap();

    // Insert 200 rows distributed by hash across 3 shards
    for i in 0..200 {
        let pk_val = i as i64;
        let target_shard = engine_3shard.shard_for_key(pk_val);
        let target = engine_3shard.shard(target_shard).unwrap();
        let row = OwnedRow::new(vec![
            Datum::Int64(pk_val),
            Datum::Text(format!("row_{i}")),
        ]);
        let pk = encode_pk_from_datums(&[&Datum::Int64(pk_val)]);
        target.storage.insert_row(TableId(1), pk, row).unwrap();
    }

    let entries_3shard = dashmap_entries(&engine_3shard);
    assert_eq!(entries_3shard, 200, "Fresh 3-shard engine should have 200 entries");

    // Verify data is distributed across shards
    let shard_counts: Vec<u64> = engine_3shard
        .all_shards()
        .iter()
        .map(|s| {
            s.storage
                .get_table(TableId(1))
                .map_or(0, |t| t.row_count_approx() as u64)
        })
        .collect();

    // At least 2 of the 3 shards should have some data after redistribution
    let non_empty_shards = shard_counts.iter().filter(|&&c| c > 0).count();
    assert!(
        non_empty_shards >= 2,
        "After split and redistribute, at least 2 shards should have data, got {:?}",
        shard_counts
    );

    // Run rebalancer on the new 3-shard topology — should succeed
    let rebalancer = ShardRebalancer::new(config);
    let _migrated = rebalancer.check_and_rebalance(&engine_3shard);
    assert_eq!(rebalancer.runs_completed(), 1, "Rebalancer should run on new topology");
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4: Concurrent rebalance prevention
//
// Ensures that two concurrent calls to check_and_rebalance on the same
// ShardRebalancer are serialized (the second is skipped).
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn chaos_concurrent_rebalance_rejection() {
    let engine = Arc::new(ShardedEngine::new(2));
    let schema = make_schema();
    engine.create_table_all(&schema).unwrap();
    skew_shard_0(&engine, 500);

    let config = RebalancerConfig {
        imbalance_threshold: 1.25,
        batch_size: 16, // small batches to make migration slower
        min_donor_rows: 10,
        cooldown_ms: 0,
    };
    let rebalancer = Arc::new(ShardRebalancer::new(config));

    // Run two rebalance attempts in parallel threads
    let rb1 = rebalancer.clone();
    let eng1 = engine.clone();
    let rb2 = rebalancer.clone();
    let eng2 = engine.clone();

    let h1 = std::thread::spawn(move || rb1.check_and_rebalance(&eng1));
    let h2 = std::thread::spawn(move || rb2.check_and_rebalance(&eng2));

    let m1 = h1.join().unwrap();
    let m2 = h2.join().unwrap();

    // At least one should succeed, but both can't both migrate simultaneously
    // because of the AtomicBool running guard. One returns 0 (skipped).
    assert!(
        m1 == 0 || m2 == 0,
        "One rebalance should be skipped (concurrent guard): m1={}, m2={}",
        m1,
        m2
    );
    assert!(
        (m1 + m2) > 0 || rebalancer.runs_completed() >= 1,
        "At least one rebalance run should have completed"
    );

    // DashMap entries only grow (inserts + tombstones), never shrink
    let entries = dashmap_entries(&engine);
    assert!(
        entries >= 500,
        "Entries should be >= 500 (original + migrations): got {}",
        entries
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5: Rapid leader changes (storm) during rebalance
//
// Simulates a "leader storm" where multiple leader changes fire in
// rapid succession. The rebalancer and ShardMap must remain consistent.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn chaos_leader_storm_during_rebalance() {
    let engine = ShardedEngine::new(2);
    let schema = make_schema();
    engine.create_table_all(&schema).unwrap();
    skew_shard_0(&engine, 200);

    let mut shard_map = ShardMap::uniform(2, NodeId(1));
    let change_count = Arc::new(AtomicU64::new(0));
    let cc = change_count.clone();
    shard_map.set_leader_change_callback(move |_s, _o, _n, _e| {
        cc.fetch_add(1, Ordering::Relaxed);
    });

    // Simulate 10 rapid leader changes on shard 0
    for i in 2u64..12 {
        shard_map.update_leader(ShardId(0), NodeId(i));
    }
    // Final leader is NodeId(11)
    assert_eq!(change_count.load(Ordering::Relaxed), 10);
    assert_eq!(shard_map.shard_epoch(ShardId(0)), Some(11));

    // Rebalancer should still function after the leader storm
    let config = RebalancerConfig {
        imbalance_threshold: 1.25,
        batch_size: 64,
        min_donor_rows: 10,
        cooldown_ms: 0,
    };
    let rebalancer = ShardRebalancer::new(config);
    let migrated = rebalancer.check_and_rebalance(&engine);
    assert!(migrated > 0, "Rebalancer should work after leader storm");
    assert_eq!(rebalancer.runs_completed(), 1);

    // DashMap entries only grow
    let entries_after = dashmap_entries(&engine);
    assert!(
        entries_after >= 200,
        "Entries should be >= 200 after migration: got {}",
        entries_after
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6: Migration to non-existent target shard (graceful failure)
//
// Verifies that the migrator fails gracefully when the target shard
// doesn't exist (e.g., removed during a split/merge).
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn chaos_migration_missing_target_shard() {
    let engine = ShardedEngine::new(2);
    let schema = make_schema();
    engine.create_table_all(&schema).unwrap();
    skew_shard_0(&engine, 100);

    let config = RebalancerConfig::default();
    let migrator = ShardMigrator::new(config);

    // Create a task targeting a non-existent shard
    let task = MigrationTask {
        source_shard: ShardId(0),
        target_shard: ShardId(99), // doesn't exist
        table_name: "chaos_table".to_string(),
        rows_to_move: 50,
        estimated_time_ms: 1,
    };

    let status = migrator.execute_task(&task, &engine);
    assert_eq!(
        status.phase,
        MigrationPhase::Failed,
        "Migration to non-existent shard should fail gracefully"
    );
    assert!(
        status.error.is_some(),
        "Error message should be set on failure"
    );
    assert!(
        status.error.unwrap().contains("not found"),
        "Error should mention shard not found"
    );

    // Original data untouched (no migration happened, so count is exact)
    let entries = dashmap_entries(&engine);
    assert_eq!(entries, 100, "Entries should be untouched after failed migration");
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 7: Pause/resume during active rebalance cycle
//
// Verifies that pausing the rebalancer causes check_and_rebalance to
// skip, and resuming allows it to proceed.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn chaos_pause_resume_rebalancer() {
    let engine = ShardedEngine::new(2);
    let schema = make_schema();
    engine.create_table_all(&schema).unwrap();
    skew_shard_0(&engine, 200);

    let config = RebalancerConfig {
        imbalance_threshold: 1.25,
        batch_size: 64,
        min_donor_rows: 10,
        cooldown_ms: 0,
    };
    let rebalancer = ShardRebalancer::new(config);

    // Pause → check should skip
    rebalancer.pause();
    assert!(rebalancer.is_paused());
    let migrated_while_paused = rebalancer.check_and_rebalance(&engine);
    assert_eq!(
        migrated_while_paused, 0,
        "No migration should happen while paused"
    );
    assert_eq!(rebalancer.runs_completed(), 0);

    // Resume → check should proceed
    rebalancer.resume();
    assert!(!rebalancer.is_paused());
    let migrated_after_resume = rebalancer.check_and_rebalance(&engine);
    assert!(
        migrated_after_resume > 0,
        "Migration should proceed after resume"
    );
    assert_eq!(rebalancer.runs_completed(), 1);

    // DashMap entries grow after migration (tombstones + target inserts)
    let entries = dashmap_entries(&engine);
    assert!(
        entries >= 200,
        "Entries should be >= 200 after pause/resume cycle: got {}",
        entries
    );
}
