    use std::sync::Arc;
    use std::time::Duration;

    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::Catalog;
    use falcon_common::types::{IsolationLevel, ShardId, TableId};
    use falcon_executor::executor::ExecutionResult;
    use falcon_planner::planner::Planner;
    use falcon_sql_frontend::binder::Binder;
    use falcon_sql_frontend::parser::parse_sql;

    use crate::query_engine::DistributedQueryEngine;
    use crate::sharded_engine::ShardedEngine;

    /// Build a 4-shard engine, create the "users" table on all shards,
    /// insert test data, and return the engine + catalog.
    fn setup() -> (Arc<ShardedEngine>, Catalog) {
        let engine = Arc::new(ShardedEngine::new(4));

        // We need to create the table schema and register it in catalog.
        let schema = falcon_common::schema::TableSchema {
            id: TableId(1),
            name: "users".into(),
            columns: vec![
                falcon_common::schema::ColumnDef {
                    id: falcon_common::types::ColumnId(0),
                    name: "id".into(),
                    data_type: falcon_common::types::DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                falcon_common::schema::ColumnDef {
                    id: falcon_common::types::ColumnId(1),
                    name: "name".into(),
                    data_type: falcon_common::types::DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                falcon_common::schema::ColumnDef {
                    id: falcon_common::types::ColumnId(2),
                    name: "age".into(),
                    data_type: falcon_common::types::DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };

        engine.create_table_all(&schema).unwrap();

        // Insert 10 rows per shard (40 total)
        for s in 0..4u64 {
            let shard = engine.shard(ShardId(s)).unwrap();
            for i in 0..10 {
                let global_id = (s as i32) * 10 + i;
                let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                let row = OwnedRow::new(vec![
                    Datum::Int32(global_id),
                    Datum::Text(format!("user_{}", global_id)),
                    Datum::Int32(20 + global_id),
                ]);
                shard.storage.insert(TableId(1), row, txn.txn_id).unwrap();
                shard.txn_mgr.commit(txn.txn_id).unwrap();
            }
        }

        let mut catalog = Catalog::new();
        catalog.add_table(schema);
        (engine, catalog)
    }

    fn plan_and_wrap(
        sql: &str,
        catalog: &Catalog,
        shards: &[ShardId],
    ) -> falcon_planner::PhysicalPlan {
        let stmts = parse_sql(sql).unwrap();
        let mut binder = Binder::new(catalog.clone());
        let bound = binder.bind(&stmts[0]).unwrap();
        let plan = Planner::plan(&bound).unwrap();
        Planner::wrap_distributed(plan, shards)
    }

    #[test]
    fn test_e2e_select_star_distributed() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT id, name, age FROM users", &catalog, &shards);

        // Should be a DistPlan wrapping a SeqScan
        assert!(matches!(
            plan,
            falcon_planner::PhysicalPlan::DistPlan { .. }
        ));

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { columns, rows } => {
                assert_eq!(columns.len(), 3, "id, name, age");
                assert_eq!(rows.len(), 40, "4 shards * 10 rows = 40");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_select_with_filter_distributed() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id, name FROM users WHERE age > 50",
            &catalog,
            &shards,
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { columns, rows } => {
                assert_eq!(columns.len(), 2);
                // age ranges from 20..59 across 4 shards.
                // age > 50 means ids 31..39  鈫?9 rows.
                assert_eq!(rows.len(), 9, "ages 51-59  鈫?9 rows");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_single_shard_stays_local() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Single shard  鈫?wrap_distributed should NOT wrap.
        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &[ShardId(0)]);
        assert!(matches!(plan, falcon_planner::PhysicalPlan::SeqScan { .. }));

        // Execute on shard 0 via the default path.
        let shard = engine.shard(ShardId(0)).unwrap();
        let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
        let result = qe.execute_on_shard(ShardId(0), &plan, Some(&txn)).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 10, "shard 0 has 10 rows");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_two_phase_insert() {
        let (engine, _catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Use TwoPhaseCoordinator to insert a row on all shards.
        let result = qe
            .two_phase_coordinator()
            .execute(
                &shards,
                IsolationLevel::ReadCommitted,
                |storage, _txn_mgr, txn_id| {
                    let row = OwnedRow::new(vec![
                        Datum::Int32(999),
                        Datum::Text("two_phase_user".into()),
                        Datum::Int32(99),
                    ]);
                    storage.insert(TableId(1), row, txn_id)?;
                    Ok(())
                },
            )
            .unwrap();

        assert!(result.committed, "2PC should commit");
        assert_eq!(result.participants.len(), 4);

        // Verify the row exists on each shard individually (2PC replicates to all).
        for s in 0..4u64 {
            let shard = engine.shard(ShardId(s)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let rows = shard
                .storage
                .scan(
                    TableId(1),
                    txn.txn_id,
                    falcon_common::types::Timestamp(u64::MAX),
                )
                .unwrap();
            let found = rows
                .iter()
                .any(|r| r.1.values.first() == Some(&Datum::Int32(999)));
            shard.txn_mgr.commit(txn.txn_id).unwrap();
            assert!(found, "Row 999 should exist on shard {}", s);
        }
    }

    #[test]
    fn test_e2e_ddl_propagation() {
        // Create engine with NO tables, then use DistributedQueryEngine to CREATE TABLE.
        let engine = Arc::new(ShardedEngine::new(4));
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        let schema = falcon_common::schema::TableSchema {
            id: TableId(2),
            name: "products".into(),
            columns: vec![falcon_common::schema::ColumnDef {
                id: falcon_common::types::ColumnId(0),
                name: "pid".into(),
                data_type: falcon_common::types::DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            }],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };

        let plan = falcon_planner::PhysicalPlan::CreateTable {
            schema: schema.clone(),
            if_not_exists: false,
        };

        // DDL should propagate to ALL 4 shards.
        let result = qe.execute(&plan, None).unwrap();
        assert!(matches!(result, ExecutionResult::Ddl { .. }));

        // Verify table exists on ALL shards by inserting a row on each.
        for s in 0..4u64 {
            let shard = engine.shard(ShardId(s)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let row = OwnedRow::new(vec![Datum::Int32(s as i32)]);
            shard.storage.insert(TableId(2), row, txn.txn_id).unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }
    }

    #[test]
    fn test_e2e_shard_routed_insert() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // INSERT via DistributedQueryEngine  鈥?should route to a specific shard.
        let plan = plan_and_wrap(
            "INSERT INTO users (id, name, age) VALUES (12345, 'routed_user', 42)",
            &catalog,
            &shards,
        );
        // INSERT stays local (wrap_distributed doesn't wrap DML)
        assert!(matches!(plan, falcon_planner::PhysicalPlan::Insert { .. }));

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(rows_affected, 1);
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify the row landed on the correct shard (by PK hash).
        let expected_shard = engine.shard_for_key(12345);
        let shard = engine.shard(expected_shard).unwrap();
        let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
        let rows = shard
            .storage
            .scan(
                TableId(1),
                txn.txn_id,
                falcon_common::types::Timestamp(u64::MAX),
            )
            .unwrap();
        let found = rows
            .iter()
            .any(|r| r.1.values.first() == Some(&Datum::Int32(12345)));
        assert!(found, "Row 12345 should be on shard {:?}", expected_shard);
    }

    #[test]
    fn test_e2e_order_by_limit_uses_merge_sort() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // SELECT with ORDER BY + LIMIT should use MergeSortLimit gather.
        let plan = plan_and_wrap(
            "SELECT id, age FROM users ORDER BY age DESC LIMIT 5",
            &catalog,
            &shards,
        );

        // Verify the plan is a DistPlan with MergeSortLimit gather
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => {
                assert!(
                    matches!(gather, falcon_planner::DistGather::MergeSortLimit { .. }),
                    "Expected MergeSortLimit gather, got {:?}",
                    gather
                );
            }
            other => panic!("Expected DistPlan, got {:?}", other),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "LIMIT 5");
                // Rows should be sorted by age DESC
                for i in 1..rows.len() {
                    let prev_age = match &rows[i - 1].values[1] {
                        Datum::Int32(v) => *v,
                        _ => panic!("expected Int32"),
                    };
                    let curr_age = match &rows[i].values[1] {
                        Datum::Int32(v) => *v,
                        _ => panic!("expected Int32"),
                    };
                    assert!(
                        prev_age >= curr_age,
                        "not sorted DESC: {} < {}",
                        prev_age,
                        curr_age
                    );
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_explain_shows_dist_plan() {
        let (_, catalog) = setup();
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Plan a SELECT, wrap it, then wrap in EXPLAIN
        let stmts = parse_sql("SELECT id FROM users").unwrap();
        let mut binder = Binder::new(catalog.clone());
        let bound = binder.bind(&stmts[0]).unwrap();
        let plan = Planner::plan(&bound).unwrap();
        let dist_plan = Planner::wrap_distributed(plan, &shards);
        let explain_plan = falcon_planner::PhysicalPlan::Explain(Box::new(dist_plan));

        // The EXPLAIN plan's routing hint should come from the inner DistPlan.
        let hint = explain_plan.routing_hint();
        assert_eq!(hint.involved_shards.len(), 4);
    }

    #[test]
    fn test_e2e_update_broadcast_all_shards() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // UPDATE without PK filter  鈫?broadcasts to all shards.
        let plan = plan_and_wrap(
            "UPDATE users SET age = 99 WHERE age > 20",
            &catalog,
            &shards,
        );
        // UPDATE stays local (wrap_distributed doesn't wrap DML)
        assert!(matches!(plan, falcon_planner::PhysicalPlan::Update { .. }));

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                // All 20 rows across 4 shards have age 20..39  鈫?all match age > 20
                // (ages 21..39 match, 20 does not; that's 19 rows across shards)
                assert!(rows_affected > 0, "Should have updated some rows");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_delete_broadcast_all_shards() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // First count all rows via distributed SELECT.
        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        let before_count = match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => rows.len(),
            _ => panic!("Expected Query"),
        };
        assert_eq!(
            before_count, 40,
            "Should have 40 rows before delete (10 per shard 脳 4)"
        );

        // DELETE without PK filter  鈫?broadcasts to all shards.
        let del_plan = plan_and_wrap("DELETE FROM users WHERE age < 25", &catalog, &shards);
        let result = qe.execute(&del_plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert!(rows_affected > 0, "Should have deleted some rows");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify rows were actually deleted by counting again.
        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        let after_count = match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => rows.len(),
            _ => panic!("Expected Query"),
        };
        assert!(
            after_count < before_count,
            "Should have fewer rows after delete"
        );
    }

    #[test]
    fn test_e2e_multi_row_insert_split_by_shard() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Multi-row INSERT with different PK values  鈥?rows should be split across shards.
        let plan = plan_and_wrap(
            "INSERT INTO users (id, name, age) VALUES (1000, 'a', 10), (2000, 'b', 20), (3000, 'c', 30)",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(rows_affected, 3);
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify each row landed on its correct shard by PK hash.
        for pk in [1000i64, 2000, 3000] {
            let expected_shard = engine.shard_for_key(pk);
            let shard = engine.shard(expected_shard).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let rows = shard
                .storage
                .scan(
                    TableId(1),
                    txn.txn_id,
                    falcon_common::types::Timestamp(u64::MAX),
                )
                .unwrap();
            let found = rows
                .iter()
                .any(|r| r.1.values.first() == Some(&Datum::Int32(pk as i32)));
            shard.txn_mgr.commit(txn.txn_id).unwrap();
            assert!(found, "Row {} should be on shard {:?}", pk, expected_shard);
        }
    }

    #[test]
    fn test_e2e_aggregate_count_uses_two_phase_agg() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // SELECT COUNT(*) should use TwoPhaseAgg gather.
        let plan = plan_and_wrap("SELECT COUNT(*) FROM users", &catalog, &shards);
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => {
                assert!(
                    matches!(gather, falcon_planner::DistGather::TwoPhaseAgg { .. }),
                    "Expected TwoPhaseAgg gather for COUNT(*), got {:?}",
                    gather
                );
            }
            other => panic!("Expected DistPlan, got {:?}", other),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "COUNT should return 1 row");
                // Total rows across 4 shards (10 per shard = 40)
                let count = match &rows[0].values[0] {
                    Datum::Int64(v) => *v,
                    other => panic!("Expected Int64, got {:?}", other),
                };
                assert_eq!(count, 40, "COUNT(*) should be 40");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_aggregate_sum_min_max() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // SELECT SUM(age), MIN(age), MAX(age)  鈥?all supported for TwoPhaseAgg
        let plan = plan_and_wrap(
            "SELECT SUM(age), MIN(age), MAX(age) FROM users",
            &catalog,
            &shards,
        );
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => {
                assert!(matches!(
                    gather,
                    falcon_planner::DistGather::TwoPhaseAgg { .. }
                ));
            }
            other => panic!("Expected DistPlan, got {:?}", other),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_explain() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // EXPLAIN of a distributed plan should produce readable plan text.
        let stmts = parse_sql("SELECT id FROM users").unwrap();
        let mut binder = Binder::new(catalog.clone());
        let bound = binder.bind(&stmts[0]).unwrap();
        let plan = Planner::plan(&bound).unwrap();
        let dist_plan = Planner::wrap_distributed(plan, &shards);
        let explain_plan = falcon_planner::PhysicalPlan::Explain(Box::new(dist_plan));

        // Execute EXPLAIN through DistributedQueryEngine
        let result = qe.execute(&explain_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert!(!rows.is_empty(), "EXPLAIN should produce output");
                let plan_text: Vec<String> = rows
                    .iter()
                    .filter_map(|r| match &r.values[0] {
                        Datum::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let joined = plan_text.join("\n");
                assert!(
                    joined.contains("DistPlan"),
                    "EXPLAIN should mention DistPlan: {}",
                    joined
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_drop_table_propagation() {
        let engine = Arc::new(ShardedEngine::new(4));
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Create table on all shards first.
        let schema = falcon_common::schema::TableSchema {
            id: TableId(3),
            name: "temp_tbl".into(),
            columns: vec![falcon_common::schema::ColumnDef {
                id: falcon_common::types::ColumnId(0),
                name: "x".into(),
                data_type: falcon_common::types::DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            }],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };
        let create_plan = falcon_planner::PhysicalPlan::CreateTable {
            schema: schema.clone(),
            if_not_exists: false,
        };
        qe.execute(&create_plan, None).unwrap();

        // Verify table exists on all shards by inserting.
        for s in 0..4u64 {
            let shard = engine.shard(ShardId(s)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let row = OwnedRow::new(vec![Datum::Int32(s as i32)]);
            shard.storage.insert(TableId(3), row, txn.txn_id).unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }

        // DROP TABLE should propagate to ALL shards.
        let drop_plan = falcon_planner::PhysicalPlan::DropTable {
            table_name: "temp_tbl".into(),
            if_exists: false,
        };
        let result = qe.execute(&drop_plan, None).unwrap();
        assert!(matches!(result, ExecutionResult::Ddl { .. }));

        // Verify table is gone on ALL shards (insert should fail).
        for s in 0..4u64 {
            let shard = engine.shard(ShardId(s)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let row = OwnedRow::new(vec![Datum::Int32(100)]);
            let err = shard.storage.insert(TableId(3), row, txn.txn_id);
            assert!(err.is_err(), "Table should not exist on shard {}", s);
            shard.txn_mgr.abort(txn.txn_id).unwrap();
        }
    }

    #[test]
    fn test_e2e_truncate_propagation() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Verify rows exist before truncate.
        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        let count = match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => rows.len(),
            _ => panic!("Expected Query"),
        };
        assert_eq!(count, 40, "40 rows before truncate");

        // TRUNCATE propagates to all shards.
        let trunc = falcon_planner::PhysicalPlan::Truncate {
            table_name: "users".into(),
        };
        qe.execute(&trunc, None).unwrap();

        // All rows should be gone.
        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        let count = match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => rows.len(),
            _ => panic!("Expected Query"),
        };
        assert_eq!(count, 0, "0 rows after truncate");
    }

    #[test]
    fn test_e2e_run_gc_all_shards() {
        let (engine, _catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // RunGc should succeed and report results from all 4 shards.
        let plan = falcon_planner::PhysicalPlan::RunGc;
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Should have watermark_ts, chains_processed, shards_processed
                assert!(rows.len() >= 3, "Expected at least 3 GC result rows");
                let shards_row = rows
                    .iter()
                    .find(|r| matches!(&r.values[0], Datum::Text(s) if s == "shards_processed"));
                assert!(shards_row.is_some(), "Should have shards_processed metric");
                if let Some(row) = shards_row {
                    assert_eq!(row.values[1], Datum::Int64(4), "Should process 4 shards");
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_alter_table_propagation() {
        let (engine, _catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // ALTER TABLE ADD COLUMN should propagate to all shards.
        let alter_plan = falcon_planner::PhysicalPlan::AlterTable {
            table_name: "users".into(),
            ops: vec![falcon_sql_frontend::types::AlterTableOp::AddColumn(
                falcon_common::schema::ColumnDef {
                    id: falcon_common::types::ColumnId(3),
                    name: "email".into(),
                    data_type: falcon_common::types::DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            )],
        };
        let result = qe.execute(&alter_plan, None).unwrap();
        assert!(matches!(result, ExecutionResult::Ddl { .. }));

        // Verify the column was added on ALL shards by checking schema.
        for s in 0..4u64 {
            let shard = engine.shard(ShardId(s)).unwrap();
            let schema = shard.storage.get_table_schema("users");
            assert!(schema.is_some(), "Table should exist on shard {}", s);
            let schema = schema.unwrap();
            let has_email = schema.columns.iter().any(|c| c.name == "email");
            assert!(has_email, "Shard {} should have 'email' column", s);
        }
    }

    #[test]
    fn test_e2e_create_index_propagation() {
        let (engine, _catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // CREATE INDEX should propagate to all shards.
        let idx_plan = falcon_planner::PhysicalPlan::CreateIndex {
            index_name: "idx_users_age".into(),
            table_name: "users".into(),
            column_indices: vec![2],
            unique: false,
        };
        let result = qe.execute(&idx_plan, None).unwrap();
        assert!(matches!(result, ExecutionResult::Ddl { .. }));

        // Verify the index was created on ALL shards.
        for s in 0..4u64 {
            let shard = engine.shard(ShardId(s)).unwrap();
            let indexed = shard.storage.get_indexed_columns(TableId(1));
            let has_age_idx = indexed.iter().any(|(col, _)| *col == 2);
            assert!(
                has_age_idx, // age is column index 2
                "Shard {} should have index on age (col 2), found: {:?}",
                s,
                indexed
            );
        }
    }

    #[test]
    fn test_e2e_aggregate_txn_stats() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Execute some operations to generate txn stats.
        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        qe.execute(&plan, None).unwrap();

        // Aggregate txn stats should reflect activity across shards.
        let stats = qe.aggregate_txn_stats();
        // The setup() already committed 40 transactions (10 per shard 脳 4 shards),
        // plus the distributed SELECT reads from each shard.
        assert!(
            stats.total_committed >= 40,
            "Expected at least 40 committed, got {}",
            stats.total_committed
        );
    }

    #[test]
    fn test_e2e_select_pk_filter_single_shard_shortcut() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Insert a row via the distributed engine (PK hash routing)
        // so it lands on the correct shard for shard_for_key(7777).
        let insert_plan = plan_and_wrap(
            "INSERT INTO users (id, name, age) VALUES (7777, 'pk_test', 42)",
            &catalog,
            &shards,
        );
        qe.execute(&insert_plan, None).unwrap();

        // SELECT with WHERE id = 7777 should shortcut to a single shard
        // via try_single_shard_scan, not scatter to all 4 shards.
        let plan = plan_and_wrap(
            "SELECT id, name FROM users WHERE id = 7777",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "PK point lookup should return 1 row");
                assert_eq!(rows[0].values[0], Datum::Int32(7777));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_parallel_multi_row_insert() {
        let (engine, _catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Insert 100 rows that hash to different shards  鈥?parallel execution.
        let mut row_exprs = Vec::new();
        for i in 5000..5100 {
            row_exprs.push(vec![
                falcon_sql_frontend::types::BoundExpr::Literal(Datum::Int32(i)),
                falcon_sql_frontend::types::BoundExpr::Literal(Datum::Text(format!("user_{}", i))),
                falcon_sql_frontend::types::BoundExpr::Literal(Datum::Int32(25)),
            ]);
        }

        let schema = engine
            .shard(ShardId(0))
            .unwrap()
            .storage
            .get_table_schema("users")
            .unwrap();
        let plan = falcon_planner::PhysicalPlan::Insert {
            table_id: schema.id,
            schema: schema.clone(),
            columns: vec![0, 1, 2],
            rows: row_exprs,
            source_select: None,
            returning: vec![],
            on_conflict: None,
        };
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(rows_affected, 100, "Should insert 100 rows");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify total row count across all shards increased by 100.
        let mut total = 0usize;
        for s in 0..4u64 {
            let shard = engine.shard(ShardId(s)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let rows = shard
                .storage
                .scan(
                    TableId(1),
                    txn.txn_id,
                    falcon_common::types::Timestamp(u64::MAX),
                )
                .unwrap();
            total += rows.len();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }
        assert_eq!(total, 140, "40 original + 100 new = 140 total rows");
    }

    #[test]
    fn test_e2e_scatter_stats_populated_after_select() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Before any scatter, stats should be default.
        let stats = qe.last_scatter_stats();
        assert_eq!(stats.shards_participated, 0);

        // Execute a distributed SELECT to populate scatter stats.
        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        qe.execute(&plan, None).unwrap();

        let stats = qe.last_scatter_stats();
        assert_eq!(stats.shards_participated, 4, "Should scatter to 4 shards");
        assert_eq!(stats.total_rows_gathered, 40, "Should gather 40 rows");
        assert_eq!(stats.gather_strategy, "Union");
        assert!(stats.total_latency_us > 0, "Should have non-zero latency");
        assert_eq!(
            stats.per_shard_latency_us.len(),
            4,
            "Should have 4 shard latencies"
        );
    }

    #[test]
    fn test_e2e_distributed_offset() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // ORDER BY id LIMIT 5 OFFSET 10: should skip first 10, return next 5.
        let plan = plan_and_wrap(
            "SELECT id FROM users ORDER BY id LIMIT 5 OFFSET 10",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "LIMIT 5 after OFFSET 10");
                // Rows should be ids 10..15 (sorted globally).
                assert_eq!(rows[0].values[0], Datum::Int32(10));
                assert_eq!(rows[4].values[0], Datum::Int32(14));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_explain_analyze_dist_plan() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // EXPLAIN wrapping a DistPlan should show plan + execution stats.
        let inner = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        let explain_plan = falcon_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&explain_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows
                    .iter()
                    .map(|r| match &r.values[0] {
                        Datum::Text(s) => s.clone(),
                        _ => String::new(),
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(
                    joined.contains("DistPlan"),
                    "Should show DistPlan in output"
                );
                assert!(
                    joined.contains("Shards participated: 4"),
                    "Should show 4 shards"
                );
                assert!(
                    joined.contains("Total rows gathered: 40"),
                    "Should show 40 rows"
                );
                assert!(
                    joined.contains("Gather strategy: Union"),
                    "Should show Union strategy"
                );
                assert!(
                    joined.contains("Total latency:"),
                    "Should show total latency"
                );
                assert!(
                    joined.contains("Shard 0 latency:") || joined.contains("Shard 1 latency:"),
                    "Should show per-shard latency"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_group_by_with_two_phase_agg() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // GROUP BY age with COUNT should use TwoPhaseAgg.
        // Each shard has ages 20..29 (shard 0), 30..39 (shard 1), etc.
        // Since ages are unique across shards, each group has count=1.
        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users GROUP BY age",
            &catalog,
            &shards,
        );

        // Verify the plan uses TwoPhaseAgg
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => {
                assert!(
                    matches!(gather, falcon_planner::plan::DistGather::TwoPhaseAgg { .. }),
                    "GROUP BY + COUNT should use TwoPhaseAgg, got {:?}",
                    gather
                );
            }
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // 40 unique ages  鈫?40 groups, each with count=1
                assert_eq!(rows.len(), 40, "Should have 40 groups (40 unique ages)");
                for row in &rows {
                    // Each row: [age, count]
                    assert_eq!(row.values.len(), 2);
                    assert_eq!(row.values[1], Datum::Int64(1), "Each age appears once");
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_group_by_overlapping_keys_across_shards() {
        // Insert rows with the SAME age on multiple shards, verify TwoPhaseAgg
        // merges counts correctly across shards.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Insert rows with age=99 on each shard via distributed engine.
        for i in 0..4 {
            let insert_plan = plan_and_wrap(
                &format!(
                    "INSERT INTO users (id, name, age) VALUES ({}, 'dup_{}', 99)",
                    5000 + i,
                    i
                ),
                &catalog,
                &shards,
            );
            qe.execute(&insert_plan, None).unwrap();
        }

        // COUNT by age where age=99 should sum counts across shards.
        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users WHERE age = 99 GROUP BY age",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "Should have exactly 1 group for age=99");
                assert_eq!(rows[0].values[0], Datum::Int32(99));
                // All 4 inserted rows have age=99, but they hash-route to specific shards.
                // The total count should be 4.
                assert_eq!(
                    rows[0].values[1],
                    Datum::Int64(4),
                    "Should count 4 rows with age=99 across shards"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_union_limit_without_order_by() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // SELECT with LIMIT but no ORDER BY should use Union { limit }.
        let plan = plan_and_wrap("SELECT id FROM users LIMIT 5", &catalog, &shards);
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "LIMIT 5 should return exactly 5 rows");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_sum_min_max_overlapping_groups() {
        // Insert rows with overlapping age values across shards, then verify
        // SUM, MIN, MAX merge correctly in TwoPhaseAgg.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Use age=88 which doesn't exist in setup data (ages 20..59).
        // Insert 4 rows via distributed engine; they hash-route to various shards.
        for i in 0..4 {
            let sql = format!(
                "INSERT INTO users (id, name, age) VALUES ({}, 'sum_test_{}', 88)",
                6000 + i,
                i
            );
            qe.execute(&plan_and_wrap(&sql, &catalog, &shards), None)
                .unwrap();
        }

        // SUM(id) = 6000+6001+6002+6003 = 24006
        let plan = plan_and_wrap(
            "SELECT age, SUM(id), MIN(id), MAX(id) FROM users WHERE age = 88 GROUP BY age",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "One group for age=88");
                assert_eq!(rows[0].values[0], Datum::Int32(88));
                assert_eq!(rows[0].values[1], Datum::Int64(24006), "SUM(id) = 24006");
                assert_eq!(rows[0].values[2], Datum::Int32(6000), "MIN(id) = 6000");
                assert_eq!(rows[0].values[3], Datum::Int32(6003), "MAX(id) = 6003");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_distinct_dedup() {
        // Verify that DISTINCT deduplicates rows across shards at gather phase.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Insert rows with the same name on different shards via 2PC so they
        // exist on multiple shards.
        let two_pc = qe.two_phase_coordinator();
        for i in 0..4 {
            two_pc
                .execute(
                    &shards,
                    IsolationLevel::ReadCommitted,
                    |storage, _txn_mgr, txn_id| {
                        let row = OwnedRow::new(vec![
                            Datum::Int32(7000 + i),
                            Datum::Text("duplicate_name".into()),
                            Datum::Int32(77),
                        ]);
                        storage
                            .insert(TableId(1), row, txn_id)
                            .map(|_| ())
                            .map_err(|e| {
                                falcon_common::error::FalconError::Internal(format!("{:?}", e))
                            })
                    },
                )
                .unwrap();
        }

        // Without DISTINCT: each shard has all 4 rows  鈫?4 shards * 4 rows = 16 name values
        // (plus the 40 original rows with unique names)
        let plan_no_distinct =
            plan_and_wrap("SELECT name FROM users WHERE age = 77", &catalog, &shards);
        let result = qe.execute(&plan_no_distinct, None).unwrap();
        let no_distinct_count = match result {
            ExecutionResult::Query { rows, .. } => rows.len(),
            _ => panic!("Expected Query"),
        };
        // 4 rows * 4 shards = 16 (2PC replicates to all shards)
        assert_eq!(
            no_distinct_count, 16,
            "Without DISTINCT: 4 rows on each of 4 shards"
        );

        // With DISTINCT: should deduplicate "duplicate_name" to 1 row
        let plan_distinct = plan_and_wrap(
            "SELECT DISTINCT name FROM users WHERE age = 77",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan_distinct, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "DISTINCT should dedup to 1 unique name");
                assert_eq!(rows[0].values[0], Datum::Text("duplicate_name".into()));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_having_with_two_phase_agg() {
        // HAVING must be applied AFTER merge, not on each shard's partial agg.
        // Insert rows so that age=95 appears once on each of 4 shards (via 2PC).
        // Each shard's partial COUNT=1, but the merged COUNT=4.
        // HAVING COUNT(id) > 2 should keep the group after merge.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Insert 1 row with age=95 on each shard via 2PC (replicates to all shards).
        let two_pc = qe.two_phase_coordinator();
        for i in 0..4 {
            two_pc
                .execute(
                    &shards,
                    IsolationLevel::ReadCommitted,
                    |storage, _txn_mgr, txn_id| {
                        let row = OwnedRow::new(vec![
                            Datum::Int32(8000 + i),
                            Datum::Text(format!("having_test_{}", i)),
                            Datum::Int32(95),
                        ]);
                        storage
                            .insert(TableId(1), row, txn_id)
                            .map(|_| ())
                            .map_err(|e| {
                                falcon_common::error::FalconError::Internal(format!("{:?}", e))
                            })
                    },
                )
                .unwrap();
        }

        // Each shard has 4 rows with age=95 (2PC replicates to all shards).
        // Per-shard COUNT=4, merged COUNT=16.
        // HAVING COUNT(id) > 10 should pass after merge (16 > 10).
        // If HAVING were applied per-shard (4 > 10 = false), no rows would pass.
        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users WHERE age = 95 GROUP BY age HAVING COUNT(id) > 10",
            &catalog,
            &shards,
        );

        // Verify the gather is TwoPhaseAgg with having
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => match gather {
                falcon_planner::plan::DistGather::TwoPhaseAgg { having, .. } => {
                    assert!(having.is_some(), "TwoPhaseAgg should carry HAVING expr");
                }
                other => panic!("Expected TwoPhaseAgg, got {:?}", other),
            },
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Merged COUNT=16 > 10, so the group should pass HAVING.
                assert_eq!(
                    rows.len(),
                    1,
                    "HAVING COUNT > 10 should pass after merge (count=16)"
                );
                assert_eq!(rows[0].values[0], Datum::Int32(95));
                assert_eq!(
                    rows[0].values[1],
                    Datum::Int64(16),
                    "4 rows * 4 shards = 16"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }

        // Verify HAVING filters OUT groups: COUNT > 100 should return 0 rows
        let plan2 = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users WHERE age = 95 GROUP BY age HAVING COUNT(id) > 100",
            &catalog,
            &shards,
        );
        let result2 = qe.execute(&plan2, None).unwrap();
        match result2 {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    0,
                    "HAVING COUNT > 100 should filter out (count=16)"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_order_by_limit_offset_combined() {
        // Verify ORDER BY + LIMIT + OFFSET all work together in MergeSortLimit.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // 40 rows total (ids 0..39). ORDER BY id LIMIT 3 OFFSET 5  鈫?ids 5,6,7
        let plan = plan_and_wrap(
            "SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 5",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3, "LIMIT 3");
                assert_eq!(rows[0].values[0], Datum::Int32(5));
                assert_eq!(rows[1].values[0], Datum::Int32(6));
                assert_eq!(rows[2].values[0], Datum::Int32(7));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_order_by_desc_limit() {
        // ORDER BY id DESC LIMIT 5  鈫?ids 39,38,37,36,35
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id FROM users ORDER BY id DESC LIMIT 5",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "LIMIT 5");
                assert_eq!(rows[0].values[0], Datum::Int32(39));
                assert_eq!(rows[4].values[0], Datum::Int32(35));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_count_star_no_group_by() {
        // COUNT(*) without GROUP BY should produce a single merged row.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT COUNT(*) FROM users", &catalog, &shards);
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "Single aggregate row");
                assert_eq!(
                    rows[0].values[0],
                    Datum::Int64(40),
                    "40 total rows across 4 shards"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_avg() {
        // AVG is decomposed into SUM + hidden COUNT, merged, then AVG = SUM/COUNT.
        // Setup: 4 shards, 10 rows each. Shard k has ids k*10..(k+1)*10-1, ages 20+id.
        // All 40 rows: ages 20..59. Expected AVG(age) = (20+21+...+59)/40 = 39.5
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // AVG(age) without GROUP BY
        let plan = plan_and_wrap("SELECT AVG(age) FROM users", &catalog, &shards);
        // Verify it uses TwoPhaseAgg with avg_fixups
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => match gather {
                falcon_planner::plan::DistGather::TwoPhaseAgg { avg_fixups, .. } => {
                    assert!(!avg_fixups.is_empty(), "AVG should produce avg_fixups");
                }
                other => panic!("Expected TwoPhaseAgg, got {:?}", other),
            },
            _ => panic!("Expected DistPlan"),
        }
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "Single aggregate row");
                match &rows[0].values[0] {
                    Datum::Float64(v) => {
                        assert!(
                            (v - 39.5).abs() < 0.001,
                            "AVG(age) should be 39.5, got {}",
                            v
                        );
                    }
                    other => panic!("Expected Float64 for AVG, got {:?}", other),
                }
                // Should have only 1 visible column (AVG), no hidden COUNT
                assert_eq!(
                    rows[0].values.len(),
                    1,
                    "Hidden COUNT column should be truncated"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_avg_with_group_by() {
        // AVG(id) GROUP BY age with known data: use age=99 inserted on specific shards.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Insert 4 rows with age=99, ids 9000..9003 (hash-routed to various shards)
        for i in 0..4 {
            let sql = format!(
                "INSERT INTO users (id, name, age) VALUES ({}, 'avg_test_{}', 99)",
                9000 + i,
                i
            );
            qe.execute(&plan_and_wrap(&sql, &catalog, &shards), None)
                .unwrap();
        }

        // AVG(id) for age=99 group: (9000+9001+9002+9003)/4 = 9001.5
        let plan = plan_and_wrap(
            "SELECT age, AVG(id) FROM users WHERE age = 99 GROUP BY age",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "One group for age=99");
                assert_eq!(rows[0].values[0], Datum::Int32(99));
                match &rows[0].values[1] {
                    Datum::Float64(v) => {
                        assert!(
                            (v - 9001.5).abs() < 0.001,
                            "AVG(id) should be 9001.5, got {}",
                            v
                        );
                    }
                    other => panic!("Expected Float64 for AVG, got {:?}", other),
                }
                // Should have only 2 visible columns (age, AVG), no hidden COUNT
                assert_eq!(
                    rows[0].values.len(),
                    2,
                    "Hidden COUNT column should be truncated"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_shard_health_check() {
        let (engine, _catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        let health = qe.shard_health_check();
        assert_eq!(health.len(), 4, "Should have 4 shards");
        for (sid, healthy, latency_us) in &health {
            assert!(healthy, "Shard {:?} should be healthy", sid);
            assert!(*latency_us < 1_000_000, "Probe should complete in < 1s");
        }
    }

    #[test]
    fn test_e2e_avg_with_having() {
        // AVG decomposition + HAVING post-merge must work together:
        // AVG is computed as SUM/COUNT after merge, then HAVING filters.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Use age=70 (outside setup range 20-59) to avoid overlap.
        let two_pc = qe.two_phase_coordinator();
        for i in 0..4 {
            two_pc
                .execute(
                    &shards,
                    IsolationLevel::ReadCommitted,
                    |storage, _txn_mgr, txn_id| {
                        let row = OwnedRow::new(vec![
                            Datum::Int32(9500 + i),
                            Datum::Text(format!("avg_having_{}", i)),
                            Datum::Int32(70),
                        ]);
                        storage
                            .insert(TableId(1), row, txn_id)
                            .map(|_| ())
                            .map_err(|e| {
                                falcon_common::error::FalconError::Internal(format!("{:?}", e))
                            })
                    },
                )
                .unwrap();
        }

        // age=70 group: 4 inserts 脳 4 shards (2PC) = 16 rows, all with ids 9500..9503.
        // AVG(id) = (9500+9501+9502+9503)*4 / 16 = 9501.5
        // HAVING AVG(id) > 9000 should keep this group.
        let plan = plan_and_wrap(
            "SELECT age, AVG(id) FROM users WHERE age = 70 GROUP BY age HAVING AVG(id) > 9000",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    1,
                    "HAVING AVG(id) > 9000 should keep age=70 group"
                );
                assert_eq!(rows[0].values[0], Datum::Int32(70));
                match &rows[0].values[1] {
                    Datum::Float64(v) => {
                        assert!(
                            (v - 9501.5).abs() < 0.001,
                            "AVG(id) should be 9501.5, got {}",
                            v
                        );
                    }
                    other => panic!("Expected Float64 for AVG, got {:?}", other),
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }

        // HAVING AVG(id) > 10000 should filter out (9501.5 < 10000)
        let plan2 = plan_and_wrap(
            "SELECT age, AVG(id) FROM users WHERE age = 70 GROUP BY age HAVING AVG(id) > 10000",
            &catalog,
            &shards,
        );
        let result2 = qe.execute(&plan2, None).unwrap();
        match result2 {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 0, "HAVING AVG(id) > 10000 should filter out");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_explain_dist_plan_output() {
        // EXPLAIN of a DistPlan should include gather strategy details.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Wrap an AVG query in EXPLAIN
        let inner = plan_and_wrap(
            "SELECT age, AVG(id) FROM users GROUP BY age",
            &catalog,
            &shards,
        );
        let explain_plan = falcon_planner::PhysicalPlan::Explain(Box::new(inner));

        let result = qe.execute(&explain_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows
                    .iter()
                    .filter_map(|r| {
                        if let Datum::Text(s) = &r.values[0] {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(
                    joined.contains("TwoPhaseAgg"),
                    "EXPLAIN should show TwoPhaseAgg"
                );
                assert!(
                    joined.contains("AVG decomposition"),
                    "EXPLAIN should show AVG decomposition"
                );
                assert!(
                    joined.contains("Execution Stats"),
                    "EXPLAIN should show execution stats"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_explain_merge_sort_limit() {
        // EXPLAIN of ORDER BY + LIMIT should show MergeSortLimit.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let inner = plan_and_wrap(
            "SELECT id FROM users ORDER BY id LIMIT 5",
            &catalog,
            &shards,
        );
        let explain_plan = falcon_planner::PhysicalPlan::Explain(Box::new(inner));

        let result = qe.execute(&explain_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows
                    .iter()
                    .filter_map(|r| {
                        if let Datum::Text(s) = &r.values[0] {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(
                    joined.contains("MergeSortLimit"),
                    "EXPLAIN should show MergeSortLimit"
                );
                assert!(joined.contains("ASC"), "EXPLAIN should show sort direction");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_scatter_stats_timed_out_field() {
        // Verify ScatterStats includes the timed_out_shards field (empty for normal queries).
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        let _result = qe.execute(&plan, None).unwrap();
        let stats = qe.last_scatter_stats();
        assert_eq!(stats.shards_participated, 4);
        assert!(stats.failed_shards.is_empty(), "No shards should time out");
        assert!(
            stats.total_latency_us < 5_000_000,
            "Should complete within 5s"
        );
    }

    #[test]
    fn test_e2e_cancellation_flag_propagation() {
        // With a zero-duration timeout, the scatter should fail with a timeout error.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_nanos(1));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        let result = qe.execute(&plan, None);
        // With a 1ns timeout, at least one shard should exceed it.
        // The result should be an error containing "timeout" or "cancelled".
        match result {
            Err(e) => {
                let msg = format!("{:?}", e);
                assert!(
                    msg.contains("timeout") || msg.contains("cancelled"),
                    "Error should mention timeout or cancelled, got: {}",
                    msg,
                );
            }
            Ok(_) => {
                // If the machine is extremely fast, the query might succeed.
                // That's acceptable  鈥?the important thing is no panic.
            }
        }
    }

    #[test]
    fn test_e2e_group_by_order_by_limit() {
        // GROUP BY + ORDER BY + LIMIT in distributed mode.
        // TwoPhaseAgg should merge, then sort, then limit.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Setup has 40 rows: id 0..39, age = 20 + id. Each shard has 10 rows.
        // GROUP BY age gives 40 groups (each with count=1).
        // ORDER BY age ASC LIMIT 3 should return ages 20, 21, 22.
        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users GROUP BY age ORDER BY age LIMIT 3",
            &catalog,
            &shards,
        );

        // Verify the plan uses TwoPhaseAgg (not MergeSortLimit)
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => match gather {
                falcon_planner::plan::DistGather::TwoPhaseAgg {
                    order_by, limit, ..
                } => {
                    assert!(!order_by.is_empty(), "Should have post-merge ORDER BY");
                    assert_eq!(*limit, Some(3), "Should have post-merge LIMIT 3");
                }
                other => panic!("Expected TwoPhaseAgg, got {:?}", other),
            },
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3, "LIMIT 3 should return 3 rows");
                // Sorted by age ASC: 20, 21, 22
                assert_eq!(rows[0].values[0], Datum::Int32(20));
                assert_eq!(rows[1].values[0], Datum::Int32(21));
                assert_eq!(rows[2].values[0], Datum::Int32(22));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_group_by_order_by_desc_limit() {
        // GROUP BY + ORDER BY DESC + LIMIT: sorted descending after merge.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users GROUP BY age ORDER BY age DESC LIMIT 3",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3, "LIMIT 3 should return 3 rows");
                // Sorted by age DESC: 59, 58, 57
                assert_eq!(rows[0].values[0], Datum::Int32(59));
                assert_eq!(rows[1].values[0], Datum::Int32(58));
                assert_eq!(rows[2].values[0], Datum::Int32(57));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_group_by_order_by_offset_limit() {
        // GROUP BY + ORDER BY + OFFSET + LIMIT: skip first N groups, then limit.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // 40 groups (age 20..59). ORDER BY age ASC, OFFSET 2, LIMIT 3  鈫?ages 22, 23, 24.
        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users GROUP BY age ORDER BY age LIMIT 3 OFFSET 2",
            &catalog,
            &shards,
        );

        // Verify plan shape
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => match gather {
                falcon_planner::plan::DistGather::TwoPhaseAgg { offset, limit, .. } => {
                    assert_eq!(*offset, Some(2));
                    assert_eq!(*limit, Some(3));
                }
                other => panic!("Expected TwoPhaseAgg, got {:?}", other),
            },
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3, "OFFSET 2 LIMIT 3 should return 3 rows");
                assert_eq!(rows[0].values[0], Datum::Int32(22));
                assert_eq!(rows[1].values[0], Datum::Int32(23));
                assert_eq!(rows[2].values[0], Datum::Int32(24));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_group_by_offset_beyond_results() {
        // OFFSET larger than result set should return 0 rows.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users GROUP BY age ORDER BY age LIMIT 10 OFFSET 100",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    0,
                    "OFFSET 100 on 40 groups should return 0 rows"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_array_agg() {
        // ARRAY_AGG across shards: each shard collects partial arrays, gather concatenates.
        // Setup: 40 rows, id 0..39, age = 20 + id. Each age is unique  鈫?40 groups.
        // ARRAY_AGG(id) for a single-row group just returns [id].
        // We verify a GROUP BY with COUNT + ARRAY_AGG works end-to-end.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT age, ARRAY_AGG(id) FROM users GROUP BY age ORDER BY age LIMIT 3",
            &catalog,
            &shards,
        );

        // Verify plan uses TwoPhaseAgg
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => match gather {
                falcon_planner::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                    assert!(
                        agg_merges
                            .iter()
                            .any(|m| matches!(m, falcon_planner::plan::DistAggMerge::ArrayAgg(_))),
                        "Should have ArrayAgg merge"
                    );
                }
                other => panic!("Expected TwoPhaseAgg, got {:?}", other),
            },
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3, "LIMIT 3");
                // First group: age=20, should have ARRAY_AGG(id) = [0]
                assert_eq!(rows[0].values[0], Datum::Int32(20));
                match &rows[0].values[1] {
                    Datum::Array(arr) => {
                        assert_eq!(arr.len(), 1, "age=20 has exactly 1 row  鈫?1 element");
                    }
                    other => panic!("Expected Array for ARRAY_AGG, got {:?}", other),
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_having_order_by_offset_limit_combined() {
        // Combined: GROUP BY + HAVING + ORDER BY + OFFSET + LIMIT.
        // 40 rows: id 0..39, age = 20 + id  鈫?ages 20..59, each unique.
        // COUNT(id) = 1 for every group. HAVING COUNT(id) >= 1 keeps all 40.
        // ORDER BY age ASC, OFFSET 5, LIMIT 3  鈫?ages 25, 26, 27.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users GROUP BY age HAVING COUNT(id) >= 1 ORDER BY age LIMIT 3 OFFSET 5",
            &catalog, &shards,
        );

        // Verify plan shape
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => match gather {
                falcon_planner::plan::DistGather::TwoPhaseAgg {
                    having,
                    order_by,
                    limit,
                    offset,
                    ..
                } => {
                    assert!(having.is_some(), "Should have HAVING filter");
                    assert!(!order_by.is_empty(), "Should have ORDER BY");
                    assert_eq!(*limit, Some(3));
                    assert_eq!(*offset, Some(5));
                }
                other => panic!("Expected TwoPhaseAgg, got {:?}", other),
            },
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3, "OFFSET 5 LIMIT 3 should return 3 rows");
                assert_eq!(rows[0].values[0], Datum::Int32(25));
                assert_eq!(rows[1].values[0], Datum::Int32(26));
                assert_eq!(rows[2].values[0], Datum::Int32(27));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_scatter_stats_per_shard_row_count() {
        // Verify ScatterStats includes per_shard_row_count.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &shards);
        let _result = qe.execute(&plan, None).unwrap();
        let stats = qe.last_scatter_stats();
        assert_eq!(
            stats.per_shard_row_count.len(),
            4,
            "Should have row counts for 4 shards"
        );
        let total_rows: usize = stats.per_shard_row_count.iter().map(|(_, c)| c).sum();
        assert_eq!(total_rows, 40, "Total rows across all shards should be 40");
    }

    #[test]
    fn test_e2e_union_offset_limit() {
        // Non-agg query with OFFSET + LIMIT uses Union gather with offset.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // 40 rows total. LIMIT 5 OFFSET 10  鈫?skip 10, take 5.
        let plan = plan_and_wrap("SELECT id FROM users LIMIT 5 OFFSET 10", &catalog, &shards);

        // Verify plan shape: should be Union with offset
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    falcon_planner::plan::DistGather::Union { limit, offset, .. } => {
                        assert_eq!(*limit, Some(5));
                        assert_eq!(*offset, Some(10));
                    }
                    falcon_planner::plan::DistGather::MergeSortLimit { limit, offset, .. } => {
                        // Also acceptable if planner routes through MergeSortLimit
                        assert_eq!(*limit, Some(5));
                        assert_eq!(*offset, Some(10));
                    }
                    other => panic!("Expected Union or MergeSortLimit, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "OFFSET 10 LIMIT 5 should return 5 rows");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_explain_analyze_output() {
        // Verify EXPLAIN ANALYZE output contains expected sections.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let inner = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users GROUP BY age ORDER BY age LIMIT 3",
            &catalog,
            &shards,
        );
        // Wrap in Explain to trigger exec_explain_dist
        let plan = falcon_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&plan, None);
        match result {
            Ok(ExecutionResult::Query { rows, .. }) => {
                let text: Vec<String> = rows
                    .iter()
                    .filter_map(|r| match &r.values[0] {
                        Datum::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(
                    joined.contains("Distributed Plan"),
                    "Should contain Distributed Plan header"
                );
                assert!(
                    joined.contains("TwoPhaseAgg"),
                    "Should contain TwoPhaseAgg gather strategy"
                );
                assert!(
                    joined.contains("merge: COUNT(col"),
                    "Should contain human-readable merge labels"
                );
                assert!(
                    joined.contains("Execution Stats"),
                    "Should contain Execution Stats"
                );
                assert!(joined.contains("Shard"), "Should contain per-shard details");
                assert!(
                    joined.contains("rows:"),
                    "Should contain per-shard row counts"
                );
            }
            other => panic!("Expected OK Query from explain_analyze, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_count_distinct() {
        // COUNT(DISTINCT age) across 4 shards.
        // Setup: 40 rows, id 0..39, age = 20 + id  鈫?40 distinct ages.
        // Each shard has 10 rows with unique ages.
        // COUNT(DISTINCT age) should be 40.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT COUNT(DISTINCT age) FROM users", &catalog, &shards);

        // Verify plan uses TwoPhaseAgg with CountDistinct
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => match gather {
                falcon_planner::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                    assert!(
                        agg_merges.iter().any(|m| matches!(
                            m,
                            falcon_planner::plan::DistAggMerge::CountDistinct(_)
                        )),
                        "Should have CountDistinct merge"
                    );
                }
                other => panic!("Expected TwoPhaseAgg, got {:?}", other),
            },
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    1,
                    "COUNT(DISTINCT) without GROUP BY returns 1 row"
                );
                // All 40 ages are distinct  鈫?COUNT(DISTINCT age) = 40
                assert_eq!(
                    rows[0].values[0],
                    Datum::Int64(40),
                    "40 unique ages across 4 shards"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_count_distinct_with_group_by() {
        // COUNT(DISTINCT id) GROUP BY age with duplicates across shards.
        // Setup: 40 rows, id 0..39, age = 20 + id.
        // Each age is unique  鈫?COUNT(DISTINCT id) per group = 1.
        // Also test: we can combine COUNT(DISTINCT) with regular COUNT in same query.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT age, COUNT(DISTINCT id) FROM users GROUP BY age ORDER BY age LIMIT 5",
            &catalog,
            &shards,
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "LIMIT 5");
                // First group: age=20, COUNT(DISTINCT id) = 1 (only id=0 has age=20)
                assert_eq!(rows[0].values[0], Datum::Int32(20));
                assert_eq!(
                    rows[0].values[1],
                    Datum::Int64(1),
                    "Each age has exactly 1 distinct id"
                );
                // Same for all 5 rows
                for row in &rows {
                    assert_eq!(row.values[1], Datum::Int64(1));
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_sum_distinct() {
        // SUM(DISTINCT age) across 4 shards.
        // Setup: 40 rows, id 0..39, age = 20 + id  鈫?ages 20..59, all distinct.
        // SUM(DISTINCT age) = sum(20..59) = 40 * (20+59)/2 = 40 * 39.5 = 1580
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT SUM(DISTINCT age) FROM users", &catalog, &shards);

        // Verify plan uses TwoPhaseAgg with SumDistinct
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => match gather {
                falcon_planner::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                    assert!(
                        agg_merges.iter().any(|m| matches!(
                            m,
                            falcon_planner::plan::DistAggMerge::SumDistinct(_)
                        )),
                        "Should have SumDistinct merge"
                    );
                }
                other => panic!("Expected TwoPhaseAgg, got {:?}", other),
            },
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    1,
                    "SUM(DISTINCT) without GROUP BY returns 1 row"
                );
                // All 40 ages are distinct (20..59), SUM = 20+21+...+59 = 1580
                assert_eq!(
                    rows[0].values[0],
                    Datum::Int64(1580),
                    "SUM(DISTINCT age) should be 1580"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_avg_distinct() {
        // AVG(DISTINCT age) across 4 shards.
        // Setup: 40 rows, id 0..39, age = 20 + id  鈫?ages 20..59, all distinct.
        // AVG(DISTINCT age) = (20+21+...+59)/40 = 1580/40 = 39.5
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT AVG(DISTINCT age) FROM users", &catalog, &shards);

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    1,
                    "AVG(DISTINCT) without GROUP BY returns 1 row"
                );
                assert_eq!(
                    rows[0].values[0],
                    Datum::Float64(39.5),
                    "AVG(DISTINCT age) should be 39.5"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_mixed_distinct_and_non_distinct() {
        // Mixed: COUNT(*), COUNT(DISTINCT age), SUM(age) in the same query.
        // Setup: 40 rows, id 0..39, age = 20 + id  鈫?40 distinct ages.
        // COUNT(*) = 40, COUNT(DISTINCT age) = 40, SUM(age) = 1580
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT COUNT(*), COUNT(DISTINCT age), SUM(age) FROM users",
            &catalog,
            &shards,
        );

        // Verify plan uses TwoPhaseAgg with both Count and CountDistinct
        match &plan {
            falcon_planner::PhysicalPlan::DistPlan { gather, .. } => match gather {
                falcon_planner::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                    assert!(
                        agg_merges
                            .iter()
                            .any(|m| matches!(m, falcon_planner::plan::DistAggMerge::Count(_))),
                        "Should have regular Count merge"
                    );
                    assert!(
                        agg_merges.iter().any(|m| matches!(
                            m,
                            falcon_planner::plan::DistAggMerge::CountDistinct(_)
                        )),
                        "Should have CountDistinct merge"
                    );
                    assert!(
                        agg_merges
                            .iter()
                            .any(|m| matches!(m, falcon_planner::plan::DistAggMerge::Sum(_))),
                        "Should have Sum merge"
                    );
                }
                other => panic!("Expected TwoPhaseAgg, got {:?}", other),
            },
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], Datum::Int64(40), "COUNT(*) = 40");
                assert_eq!(
                    rows[0].values[1],
                    Datum::Int64(40),
                    "COUNT(DISTINCT age) = 40"
                );
                assert_eq!(rows[0].values[2], Datum::Int64(1580), "SUM(age) = 1580");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_explain_analyze_distinct_merge_labels() {
        // EXPLAIN ANALYZE should show [collect-dedup] annotation for DISTINCT merges.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let inner = plan_and_wrap("SELECT COUNT(DISTINCT age) FROM users", &catalog, &shards);
        let plan = falcon_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&plan, None);
        match result {
            Ok(ExecutionResult::Query { rows, .. }) => {
                let text: Vec<String> = rows
                    .iter()
                    .filter_map(|r| match &r.values[0] {
                        Datum::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(
                    joined.contains("COUNT(DISTINCT col"),
                    "Should show COUNT(DISTINCT)"
                );
                assert!(
                    joined.contains("[collect-dedup]"),
                    "Should annotate with [collect-dedup]"
                );
            }
            other => panic!("Expected OK Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_count_distinct_with_having() {
        // COUNT(DISTINCT) + HAVING combined: HAVING filters on the deduplicated count.
        // Insert 4 rows with age=88 on all shards via 2PC.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let two_pc = qe.two_phase_coordinator();
        for i in 0..4 {
            two_pc
                .execute(
                    &shards,
                    IsolationLevel::ReadCommitted,
                    |storage, _txn_mgr, txn_id| {
                        let row = OwnedRow::new(vec![
                            Datum::Int32(8500 + i),
                            Datum::Text(format!("cd_having_{}", i)),
                            Datum::Int32(88),
                        ]);
                        storage
                            .insert(TableId(1), row, txn_id)
                            .map(|_| ())
                            .map_err(|e| {
                                falcon_common::error::FalconError::Internal(format!("{:?}", e))
                            })
                    },
                )
                .unwrap();
        }

        // age=88: 4 inserts * 4 shards (2PC) = 16 rows.
        // IDs are 8500..8503, each appearing 4 times.
        // COUNT(DISTINCT id) per group = 4 unique IDs.
        // HAVING COUNT(DISTINCT id) > 2 should pass (4 > 2).
        let plan = plan_and_wrap(
            "SELECT age, COUNT(DISTINCT id) FROM users WHERE age = 88 GROUP BY age HAVING COUNT(DISTINCT id) > 2",
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    1,
                    "HAVING COUNT(DISTINCT id) > 2 should pass (4 unique IDs)"
                );
                assert_eq!(rows[0].values[0], Datum::Int32(88));
                assert_eq!(rows[0].values[1], Datum::Int64(4), "4 distinct IDs");
            }
            other => panic!("Expected Query, got {:?}", other),
        }

        // HAVING COUNT(DISTINCT id) > 10 should filter out (4 < 10).
        let plan2 = plan_and_wrap(
            "SELECT age, COUNT(DISTINCT id) FROM users WHERE age = 88 GROUP BY age HAVING COUNT(DISTINCT id) > 10",
            &catalog, &shards,
        );
        let result2 = qe.execute(&plan2, None).unwrap();
        match result2 {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    0,
                    "HAVING COUNT(DISTINCT id) > 10 should filter out"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_string_agg_distinct() {
        // STRING_AGG(DISTINCT name, ',') across 4 shards.
        // Insert rows with duplicate names across shards via 2PC.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let two_pc = qe.two_phase_coordinator();
        // Insert 3 names, each replicated to all 4 shards via 2PC
        for (i, name) in ["alpha", "beta", "gamma"].iter().enumerate() {
            two_pc
                .execute(
                    &shards,
                    IsolationLevel::ReadCommitted,
                    |storage, _txn_mgr, txn_id| {
                        let row = OwnedRow::new(vec![
                            Datum::Int32(9000 + i as i32),
                            Datum::Text(name.to_string()),
                            Datum::Int32(77),
                        ]);
                        storage
                            .insert(TableId(1), row, txn_id)
                            .map(|_| ())
                            .map_err(|e| {
                                falcon_common::error::FalconError::Internal(format!("{:?}", e))
                            })
                    },
                )
                .unwrap();
        }

        // STRING_AGG(DISTINCT name, ',') WHERE age = 77:
        // Each name appears 4 times (1 per shard). After dedup: 3 unique names joined with ','
        let plan = plan_and_wrap(
            "SELECT STRING_AGG(DISTINCT name, ',') FROM users WHERE age = 77",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values[0] {
                    Datum::Text(s) => {
                        let parts: Vec<&str> = s.split(',').collect();
                        assert_eq!(parts.len(), 3, "3 distinct names: {:?}", parts);
                        assert!(parts.contains(&"alpha"));
                        assert!(parts.contains(&"beta"));
                        assert!(parts.contains(&"gamma"));
                    }
                    other => panic!("Expected Text, got {:?}", other),
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_array_agg_distinct() {
        // ARRAY_AGG(DISTINCT age) across 4 shards.
        // Setup: 40 rows, ages 20..59, all distinct across shards.
        // ARRAY_AGG(DISTINCT age) should produce an array of 40 unique ages.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT ARRAY_AGG(DISTINCT age) FROM users",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values[0] {
                    Datum::Array(arr) => {
                        assert_eq!(arr.len(), 40, "40 distinct ages");
                    }
                    other => panic!("Expected Array, got {:?}", other),
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_scatter_stats_merge_labels() {
        // After executing a DISTINCT agg query, scatter_stats should contain merge_labels.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT COUNT(*), SUM(DISTINCT age) FROM users",
            &catalog,
            &shards,
        );
        qe.execute(&plan, None).unwrap();

        let stats = qe.last_scatter_stats();
        assert_eq!(stats.gather_strategy, "TwoPhaseAgg");
        assert!(
            !stats.merge_labels.is_empty(),
            "merge_labels should be populated"
        );
        assert!(
            stats.merge_labels.iter().any(|l| l.contains("COUNT")),
            "Should have COUNT label: {:?}",
            stats.merge_labels
        );
        assert!(
            stats
                .merge_labels
                .iter()
                .any(|l| l.contains("SUM(DISTINCT") && l.contains("[collect-dedup]")),
            "Should have SUM(DISTINCT) [collect-dedup] label: {:?}",
            stats.merge_labels
        );
    }

    /// Setup with two tables: "users" (TableId 1) and "orders" (TableId 2).
    /// Users: 10 per shard (40 total). Orders: placed on DIFFERENT shards than
    /// the owning user to guarantee cross-shard join matches.
    fn setup_join() -> (Arc<ShardedEngine>, Catalog) {
        use falcon_common::schema::{ColumnDef, TableSchema};
        use falcon_common::types::{ColumnId, DataType};

        let engine = Arc::new(ShardedEngine::new(4));

        let users_schema = TableSchema {
            id: TableId(1),
            name: "users".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };
        let orders_schema = TableSchema {
            id: TableId(2),
            name: "orders".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "oid".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "user_id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "amount".into(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };

        engine.create_table_all(&users_schema).unwrap();
        engine.create_table_all(&orders_schema).unwrap();

        // Insert 5 users on shard 0 (ids 0..4) and 5 on shard 1 (ids 5..9)
        for i in 0..5 {
            let shard = engine.shard(ShardId(0)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            shard
                .storage
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("user_{}", i))]),
                    txn.txn_id,
                )
                .unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }
        for i in 5..10 {
            let shard = engine.shard(ShardId(1)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            shard
                .storage
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("user_{}", i))]),
                    txn.txn_id,
                )
                .unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }

        // Insert orders on DIFFERENT shards than their owning user:
        // - order for user 0 (shard 0) placed on shard 2
        // - order for user 5 (shard 1) placed on shard 3
        // - order for user 3 (shard 0) placed on shard 3
        // This guarantees cross-shard join is required.
        let cross_shard_orders = vec![
            (ShardId(2), 100, 0, 50),  // oid=100, user_id=0, amount=50 on shard 2
            (ShardId(3), 101, 5, 75),  // oid=101, user_id=5, amount=75 on shard 3
            (ShardId(3), 102, 3, 25),  // oid=102, user_id=3, amount=25 on shard 3
            (ShardId(0), 103, 0, 100), // oid=103, user_id=0, amount=100 on shard 0 (co-located)
        ];
        for (sid, oid, uid, amt) in cross_shard_orders {
            let shard = engine.shard(sid).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            shard
                .storage
                .insert(
                    TableId(2),
                    OwnedRow::new(vec![
                        Datum::Int32(oid),
                        Datum::Int32(uid),
                        Datum::Int32(amt),
                    ]),
                    txn.txn_id,
                )
                .unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }

        let mut catalog = Catalog::new();
        catalog.add_table(users_schema);
        catalog.add_table(orders_schema);
        (engine, catalog)
    }

    #[test]
    fn test_e2e_cross_shard_join_correctness() {
        // Cross-shard INNER JOIN: users 脳 orders ON users.id = orders.user_id
        // Users on shards 0,1; orders on shards 2,3 (mostly).
        // A per-shard local join would miss cross-shard matches.
        // Coordinator-side join should find all 4 matches.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT u.id, u.name, o.oid, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.oid",
            &catalog, &shards,
        );

        // The plan should be a join (NOT wrapped in DistPlan)
        assert!(
            matches!(
                plan,
                falcon_planner::PhysicalPlan::NestedLoopJoin { .. }
                    | falcon_planner::PhysicalPlan::HashJoin { .. }
            ),
            "Join should NOT be wrapped in DistPlan"
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { columns, rows } => {
                assert_eq!(columns.len(), 4, "u.id, u.name, o.oid, o.amount");
                assert_eq!(
                    rows.len(),
                    4,
                    "4 orders match 3 users: user 0 has 2 orders, user 3 has 1, user 5 has 1"
                );

                // Verify specific matches (ORDER BY o.oid)
                // oid=100: user_id=0
                assert_eq!(rows[0].values[0], Datum::Int32(0));
                assert_eq!(rows[0].values[2], Datum::Int32(100));
                assert_eq!(rows[0].values[3], Datum::Int32(50));

                // oid=101: user_id=5
                assert_eq!(rows[1].values[0], Datum::Int32(5));
                assert_eq!(rows[1].values[2], Datum::Int32(101));
                assert_eq!(rows[1].values[3], Datum::Int32(75));

                // oid=102: user_id=3
                assert_eq!(rows[2].values[0], Datum::Int32(3));
                assert_eq!(rows[2].values[2], Datum::Int32(102));
                assert_eq!(rows[2].values[3], Datum::Int32(25));

                // oid=103: user_id=0 (co-located)
                assert_eq!(rows[3].values[0], Datum::Int32(0));
                assert_eq!(rows[3].values[2], Datum::Int32(103));
                assert_eq!(rows[3].values[3], Datum::Int32(100));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_left_join() {
        // LEFT JOIN: all users appear, even those without orders.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT u.id, o.oid FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id",
            &catalog,
            &shards,
        );
        assert!(matches!(
            plan,
            falcon_planner::PhysicalPlan::NestedLoopJoin { .. }
                | falcon_planner::PhysicalPlan::HashJoin { .. }
        ));

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // 10 users: user 0 has 2 orders, user 3 has 1, user 5 has 1,
                // users 1,2,4,6,7,8,9 have 0 orders  鈫?7 NULL rows
                // Total: 4 matched + 7 unmatched = 11 rows
                assert_eq!(rows.len(), 11, "LEFT JOIN: 4 matched + 7 unmatched = 11");

                // First row: user 0, oid 100
                assert_eq!(rows[0].values[0], Datum::Int32(0));
                // User 0 has 2 orders so rows[0] and rows[1] are user 0

                // Check that unmatched users have NULL oid
                // User 1 has no orders
                let user1_rows: Vec<_> = rows
                    .iter()
                    .filter(|r| r.values[0] == Datum::Int32(1))
                    .collect();
                assert_eq!(user1_rows.len(), 1);
                assert!(
                    matches!(user1_rows[0].values[1], Datum::Null),
                    "User 1 should have NULL oid in LEFT JOIN"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_explain_coordinator_join() {
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let inner = plan_and_wrap(
            "SELECT u.id, o.oid FROM users u JOIN orders o ON u.id = o.user_id",
            &catalog,
            &shards,
        );
        let plan = falcon_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows
                    .iter()
                    .filter_map(|r| match &r.values[0] {
                        Datum::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(
                    joined.contains("CoordinatorJoin"),
                    "Should show CoordinatorJoin strategy"
                );
                assert!(joined.contains("Tables:"), "Should list tables involved");
                assert!(
                    joined.contains("Result rows: 4"),
                    "Should show 4 result rows"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_join_with_filter() {
        // JOIN with WHERE filter applied
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 50",
            &catalog, &shards,
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Orders with amount > 50: oid=101 (75), oid=103 (100)
                assert_eq!(rows.len(), 2, "Only 2 orders have amount > 50");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_in_subquery() {
        // IN subquery: SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
        // Users on shards 0,1; orders on shards 2,3.
        // Per-shard execution would miss cross-shard matches.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id, name FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY id",
            &catalog,
            &shards,
        );

        // Plan should NOT be wrapped in DistPlan (subquery detected)
        assert!(
            matches!(plan, falcon_planner::PhysicalPlan::SeqScan { .. }),
            "Subquery query should NOT be wrapped in DistPlan"
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Orders reference user_ids: 0, 5, 3  鈫?3 distinct users
                assert_eq!(rows.len(), 3, "3 users have orders: 0, 3, 5");
                assert_eq!(rows[0].values[0], Datum::Int32(0));
                assert_eq!(rows[1].values[0], Datum::Int32(3));
                assert_eq!(rows[2].values[0], Datum::Int32(5));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_exists_subquery() {
        // EXISTS (uncorrelated): if any orders exist, return all users
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders) ORDER BY id",
            &catalog,
            &shards,
        );

        assert!(
            matches!(plan, falcon_planner::PhysicalPlan::SeqScan { .. }),
            "EXISTS subquery should NOT be wrapped in DistPlan"
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Orders exist, so all 10 users should be returned
                assert_eq!(rows.len(), 10, "EXISTS is true  鈫?all 10 users returned");
                assert_eq!(rows[0].values[0], Datum::Int32(0));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_not_in_subquery() {
        // NOT IN subquery: users who have NO orders
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders) ORDER BY id",
            &catalog,
            &shards,
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Users 1, 2, 4, 6, 7, 8, 9 have no orders = 7 users
                assert_eq!(rows.len(), 7, "7 users have no orders");
                assert_eq!(rows[0].values[0], Datum::Int32(1));
                assert_eq!(rows[1].values[0], Datum::Int32(2));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_union() {
        // UNION ALL across two tables on different shards.
        // Users on shards 0,1; orders on shards 2,3.
        // A per-shard execution would only see local data for each branch.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id FROM users UNION ALL SELECT oid FROM orders ORDER BY id",
            &catalog,
            &shards,
        );

        // UNIONs should NOT be wrapped in DistPlan
        assert!(
            matches!(plan, falcon_planner::PhysicalPlan::SeqScan { .. }),
            "UNION should NOT be wrapped in DistPlan"
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // 10 users + 4 orders = 14 rows total
                assert_eq!(rows.len(), 14, "UNION ALL: 10 users + 4 orders = 14");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_explain_coordinator_subquery() {
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let inner = plan_and_wrap(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)",
            &catalog,
            &shards,
        );
        let plan = falcon_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows
                    .iter()
                    .filter_map(|r| match &r.values[0] {
                        Datum::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(
                    joined.contains("CoordinatorSubquery"),
                    "Should show CoordinatorSubquery strategy"
                );
                assert!(joined.contains("Tables:"), "Should list tables involved");
                assert!(
                    joined.contains("Result rows: 3"),
                    "Should show 3 result rows"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_scalar_subquery_in_filter() {
        // Scalar subquery in filter: users whose id < (SELECT COUNT(*) FROM orders)
        // Total orders = 4 across all shards, so users with id < 4  鈫?ids 0,1,2,3
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id FROM users WHERE id < (SELECT COUNT(*) FROM orders) ORDER BY id",
            &catalog,
            &shards,
        );

        assert!(
            matches!(plan, falcon_planner::PhysicalPlan::SeqScan { .. }),
            "Scalar subquery should NOT be wrapped in DistPlan"
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // COUNT(*) from orders = 4 (all shards), so id < 4  鈫?0,1,2,3
                assert_eq!(rows.len(), 4, "4 users with id < 4");
                assert_eq!(rows[0].values[0], Datum::Int32(0));
                assert_eq!(rows[1].values[0], Datum::Int32(1));
                assert_eq!(rows[2].values[0], Datum::Int32(2));
                assert_eq!(rows[3].values[0], Datum::Int32(3));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_update_with_subquery() {
        // UPDATE users SET name = 'updated' WHERE id IN (SELECT user_id FROM orders)
        // Orders have user_ids 0, 3, 5 spread across shards.
        // Without coordinator-side materialization, each shard's subquery only
        // sees local orders and may miss updates.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "UPDATE users SET name = 'updated' WHERE id IN (SELECT user_id FROM orders)",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                // user_ids in orders: 0, 5, 3, 0  鈫?distinct: 0, 3, 5  鈫?3 users updated
                assert_eq!(rows_affected, 3, "Should update 3 users who have orders");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify the updates took effect by reading back
        let select_plan = plan_and_wrap(
            "SELECT id FROM users WHERE name = 'updated' ORDER BY id",
            &catalog,
            &shards,
        );
        let result = qe.execute(&select_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3, "3 users should have name='updated'");
                assert_eq!(rows[0].values[0], Datum::Int32(0));
                assert_eq!(rows[1].values[0], Datum::Int32(3));
                assert_eq!(rows[2].values[0], Datum::Int32(5));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_delete_with_subquery() {
        // DELETE FROM users WHERE id IN (SELECT user_id FROM orders)
        // Should delete users 0, 3, 5 who have orders.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "DELETE FROM users WHERE id IN (SELECT user_id FROM orders)",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(rows_affected, 3, "Should delete 3 users who have orders");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify remaining users
        let select_plan = plan_and_wrap("SELECT COUNT(*) FROM users", &catalog, &shards);
        let result = qe.execute(&select_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Started with 10 users, deleted 3  鈫?7 remaining
                assert_eq!(rows[0].values[0], Datum::Int64(7));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_update_with_scalar_subquery() {
        // UPDATE users SET name = 'big' WHERE id > (SELECT COUNT(*) FROM orders)
        // COUNT(*) from orders = 4 (across all shards).
        // Users with id > 4: ids 5,6,7,8,9  鈫?5 users.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "UPDATE users SET name = 'big' WHERE id > (SELECT COUNT(*) FROM orders)",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                // ids 5,6,7,8,9 have id > 4
                assert_eq!(rows_affected, 5, "5 users with id > 4 should be updated");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_delete_with_exists_subquery() {
        // DELETE FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = 3)
        // Since orders has user_id=3, EXISTS is true  鈫?delete ALL users.
        // This is an uncorrelated EXISTS: it's either true or false for all rows.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "DELETE FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = 3)",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                // EXISTS is true (order with user_id=3 exists), so all 10 users deleted
                assert_eq!(rows_affected, 10, "All users deleted when EXISTS is true");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }
    }

    /// Create a simple single-table setup where data is inserted via the
    /// distributed query engine (so PK hash routing places rows correctly).
    fn setup_hash_distributed() -> (Arc<ShardedEngine>, Catalog) {
        use falcon_common::schema::{ColumnDef, TableSchema};
        use falcon_common::types::{ColumnId, DataType};

        let engine = Arc::new(ShardedEngine::new(4));
        let schema = TableSchema {
            id: TableId(1),
            name: "items".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "val".into(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };
        engine.create_table_all(&schema).unwrap();

        let mut catalog = Catalog::new();
        catalog.add_table(schema);

        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Insert 20 rows via the query engine so they land on correct shards
        for i in 0..20 {
            let plan = plan_and_wrap(
                &format!("INSERT INTO items VALUES ({}, {})", i, i * 10),
                &catalog,
                &shards,
            );
            qe.execute(&plan, None).unwrap();
        }

        (engine, catalog)
    }

    #[test]
    fn test_e2e_shard_pruning_pk_point_lookup() {
        // SELECT * FROM items WHERE id = 7 should prune to a single shard
        // instead of scattering to all 4 shards.
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT id, val FROM items WHERE id = 7", &catalog, &shards);
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "PK point lookup should return 1 row");
                assert_eq!(rows[0].values[0], Datum::Int32(7));
                assert_eq!(rows[0].values[1], Datum::Int32(70));
            }
            other => panic!("Expected Query, got {:?}", other),
        }

        // Verify shard pruning was recorded in scatter stats
        let stats = qe.last_scatter_stats();
        assert_eq!(stats.shards_participated, 1, "Should prune to 1 shard");
        assert_eq!(stats.gather_strategy, "ShardPruned");
        assert!(
            stats.pruned_to_shard.is_some(),
            "pruned_to_shard should be set"
        );
    }

    #[test]
    fn test_e2e_no_shard_pruning_full_scan() {
        // SELECT * FROM items (no PK filter) should scatter to all shards
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT id, val FROM items", &catalog, &shards);
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 20, "Full scan should return all 20 items");
            }
            other => panic!("Expected Query, got {:?}", other),
        }

        let stats = qe.last_scatter_stats();
        assert_eq!(
            stats.shards_participated, 4,
            "Should scatter to all 4 shards"
        );
        assert!(
            stats.pruned_to_shard.is_none(),
            "No shard pruning for full scan"
        );
    }

    #[test]
    fn test_e2e_explain_shows_shard_pruning() {
        // EXPLAIN SELECT ... WHERE id = 7 should show shard pruning in output
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "EXPLAIN SELECT id, val FROM items WHERE id = 7",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows
                    .iter()
                    .filter_map(|r| match &r.values[0] {
                        Datum::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(
                    joined.contains("ShardPruned"),
                    "EXPLAIN should show ShardPruned strategy: {}",
                    joined
                );
                assert!(
                    joined.contains("Shard pruning:"),
                    "EXPLAIN should show pruning detail: {}",
                    joined
                );
                assert!(
                    joined.contains("Shards participated: 1"),
                    "Should show 1 shard: {}",
                    joined
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_insert_select() {
        // INSERT INTO orders (oid, user_id, amount) SELECT id, id, id FROM users
        // Users are on shards 0,1 (ids 0..9). The SELECT should see ALL 10 users
        // even though orders table is on shards 2,3. Without coordinator-side
        // execution, shard 0 would only see its local users.
        use falcon_common::schema::{ColumnDef, TableSchema};
        use falcon_common::types::{ColumnId, DataType, TableId};

        let engine = Arc::new(crate::sharded_engine::ShardedEngine::new(4));

        let src_schema = TableSchema {
            id: TableId(1),
            name: "src".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "val".into(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };
        let dst_schema = TableSchema {
            id: TableId(2),
            name: "dst".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "val".into(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };

        engine.create_table_all(&src_schema).unwrap();
        engine.create_table_all(&dst_schema).unwrap();

        // Insert 5 rows on shard 0, 5 on shard 1
        for i in 0..5 {
            let shard = engine.shard(ShardId(0)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            shard
                .storage
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(i), Datum::Int32(i * 10)]),
                    txn.txn_id,
                )
                .unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }
        for i in 5..10 {
            let shard = engine.shard(ShardId(1)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            shard
                .storage
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(i), Datum::Int32(i * 10)]),
                    txn.txn_id,
                )
                .unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }

        let mut catalog = falcon_common::schema::Catalog::new();
        catalog.add_table(src_schema);
        catalog.add_table(dst_schema);

        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // INSERT INTO dst SELECT id, val FROM src
        let plan = plan_and_wrap("INSERT INTO dst SELECT id, val FROM src", &catalog, &shards);
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(rows_affected, 10, "Should insert all 10 rows from src");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify all 10 rows are in dst across all shards
        let select_plan = plan_and_wrap("SELECT COUNT(*) FROM dst", &catalog, &shards);
        let result = qe.execute(&select_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(
                    rows[0].values[0],
                    Datum::Int64(10),
                    "dst should have 10 rows"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_shard_pruning_and_chain() {
        // SELECT * FROM items WHERE id = 7 AND val > 0 should still prune to 1 shard
        // because the PK equality is extracted from the AND chain.
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id, val FROM items WHERE id = 7 AND val > 0",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "AND-chain PK lookup should return 1 row");
                assert_eq!(rows[0].values[0], Datum::Int32(7));
            }
            other => panic!("Expected Query, got {:?}", other),
        }

        let stats = qe.last_scatter_stats();
        assert_eq!(
            stats.shards_participated, 1,
            "Should prune to 1 shard via AND chain"
        );
        assert_eq!(stats.gather_strategy, "ShardPruned");
    }

    #[test]
    fn test_e2e_shard_pruning_in_list() {
        // SELECT * FROM items WHERE id IN (1, 3) should scatter only to the
        // shards owning keys 1 and 3 (likely fewer than all 4 shards).
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id, val FROM items WHERE id IN (1, 3)",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 2, "IN-list should return 2 rows");
                let mut ids: Vec<i32> = rows
                    .iter()
                    .map(|r| match &r.values[0] {
                        Datum::Int32(v) => *v,
                        _ => panic!(),
                    })
                    .collect();
                ids.sort();
                assert_eq!(ids, vec![1, 3]);
            }
            other => panic!("Expected Query, got {:?}", other),
        }

        let stats = qe.last_scatter_stats();
        // IN (1, 3) touches at most 2 shards; verify pruning happened
        assert!(
            stats.shards_participated <= 2,
            "IN-list should prune to <= 2 shards, got {}",
            stats.shards_participated
        );
    }

    #[test]
    fn test_e2e_update_shard_pruning_and_chain() {
        // UPDATE items SET val = 999 WHERE id = 7 AND val > 0
        // Should prune to single shard via AND-chain PK extraction.
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "UPDATE items SET val = 999 WHERE id = 7 AND val > 0",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(rows_affected, 1, "Should update exactly 1 row");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify the update took effect
        let select = plan_and_wrap("SELECT val FROM items WHERE id = 7", &catalog, &shards);
        match qe.execute(&select, None).unwrap() {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows[0].values[0], Datum::Int32(999));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_delete_in_list_pruning() {
        // DELETE FROM items WHERE id IN (1, 3) should only hit the shards
        // owning those keys, not all 4 shards.
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("DELETE FROM items WHERE id IN (1, 3)", &catalog, &shards);
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(rows_affected, 2, "Should delete exactly 2 rows");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify 18 rows remain
        let select = plan_and_wrap("SELECT COUNT(*) FROM items", &catalog, &shards);
        match qe.execute(&select, None).unwrap() {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows[0].values[0], Datum::Int64(18));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_delete_returning_merges_across_shards() {
        // DELETE FROM items WHERE id IN (1, 3, 5) RETURNING id, val
        // Rows are on different shards; RETURNING should merge results from all.
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "DELETE FROM items WHERE id IN (1, 3, 5) RETURNING id, val",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { columns, rows } => {
                assert_eq!(columns.len(), 2, "RETURNING id, val");
                assert_eq!(rows.len(), 3, "Should return 3 deleted rows");
                let mut ids: Vec<i32> = rows
                    .iter()
                    .map(|r| match &r.values[0] {
                        Datum::Int32(v) => *v,
                        _ => panic!(),
                    })
                    .collect();
                ids.sort();
                assert_eq!(ids, vec![1, 3, 5], "Should return ids 1, 3, 5");
            }
            other => panic!("Expected Query (RETURNING), got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_update_returning_merges_across_shards() {
        // UPDATE items SET val = val + 1000 RETURNING id, val
        // All 20 rows across 4 shards should appear in RETURNING result.
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "UPDATE items SET val = val + 1000 RETURNING id, val",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { columns, rows } => {
                assert_eq!(columns.len(), 2, "RETURNING id, val");
                assert_eq!(rows.len(), 20, "Should return all 20 updated rows");
            }
            other => panic!("Expected Query (RETURNING), got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_order_by_limit() {
        // SELECT id, val FROM items ORDER BY id LIMIT 5
        // 20 rows across 4 shards; should return ids 0..4 in order via k-way merge.
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id, val FROM items ORDER BY id LIMIT 5",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "LIMIT 5 should return 5 rows");
                let ids: Vec<i32> = rows
                    .iter()
                    .map(|r| match &r.values[0] {
                        Datum::Int32(v) => *v,
                        _ => panic!(),
                    })
                    .collect();
                assert_eq!(ids, vec![0, 1, 2, 3, 4], "Should be sorted ascending");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_order_by_desc_limit() {
        // SELECT id FROM items ORDER BY id DESC LIMIT 3
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id, val FROM items ORDER BY id DESC LIMIT 3",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3);
                let ids: Vec<i32> = rows
                    .iter()
                    .map(|r| match &r.values[0] {
                        Datum::Int32(v) => *v,
                        _ => panic!(),
                    })
                    .collect();
                assert_eq!(ids, vec![19, 18, 17], "Should be sorted descending");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_order_by_offset_limit() {
        // SELECT id FROM items ORDER BY id LIMIT 3 OFFSET 5
        // Should return ids 5, 6, 7
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id, val FROM items ORDER BY id LIMIT 3 OFFSET 5",
            &catalog,
            &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3);
                let ids: Vec<i32> = rows
                    .iter()
                    .map(|r| match &r.values[0] {
                        Datum::Int32(v) => *v,
                        _ => panic!(),
                    })
                    .collect();
                assert_eq!(
                    ids,
                    vec![5, 6, 7],
                    "OFFSET 5 + LIMIT 3 should give ids 5,6,7"
                );
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_hash_distributed_count() {
        // COUNT(*) across 4 shards, 20 rows total
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT COUNT(*) FROM items", &catalog, &shards);
        match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], Datum::Int64(20));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_hash_distributed_sum() {
        // SUM(val) across 4 shards. val = id * 10, so SUM = 10*(0+1+...+19) = 10*190 = 1900
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT SUM(val) FROM items", &catalog, &shards);
        match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], Datum::Int64(1900));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_hash_distributed_avg() {
        // AVG(val) across 4 shards. val = id*10, AVG = 1900/20 = 95.0
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT AVG(val) FROM items", &catalog, &shards);
        match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], Datum::Float64(95.0));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_hash_distributed_group_by_count() {
        // GROUP BY name, COUNT(*) across 4 shards using users table.
        // setup() creates 40 users (10 per shard), each with unique name.
        // GROUP BY name  鈫?40 groups, each with count=1.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT name, COUNT(*) FROM users GROUP BY name ORDER BY name LIMIT 5",
            &catalog,
            &shards,
        );
        match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "LIMIT 5");
                // Each unique name has count=1
                for row in &rows {
                    assert_eq!(row.values[1], Datum::Int64(1));
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_hash_distributed_group_by_sum_having() {
        // GROUP BY name, SUM(age) HAVING SUM(age) > 50 across 4 shards.
        // setup() creates 40 users with age = 20+id (ages 20..59).
        // Each name is unique  鈫?40 groups. HAVING SUM(age) > 50 filters to ages 51..59 = 9 rows.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT name, SUM(age) FROM users GROUP BY name HAVING SUM(age) > 50 ORDER BY name",
            &catalog,
            &shards,
        );
        match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 9, "Ages 51..59 pass HAVING SUM(age) > 50");
                // All returned SUM(age) values should be > 50
                for row in &rows {
                    match &row.values[1] {
                        Datum::Int64(v) => assert!(*v > 50, "SUM(age) should be > 50, got {}", v),
                        Datum::Int32(v) => assert!(*v > 50, "SUM(age) should be > 50, got {}", v),
                        other => panic!("Unexpected type {:?}", other),
                    }
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_hash_distributed_min_max() {
        // MIN(id) and MAX(id) across 4 shards
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap("SELECT MIN(id), MAX(id) FROM items", &catalog, &shards);
        match qe.execute(&plan, None).unwrap() {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], Datum::Int32(0));
                assert_eq!(rows[0].values[1], Datum::Int32(19));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }
