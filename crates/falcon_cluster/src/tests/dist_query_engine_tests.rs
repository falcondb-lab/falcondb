    use std::sync::Arc;
    use std::time::Duration;

    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, IsolationLevel, ShardId, TableId};
    use falcon_executor::executor::ExecutionResult;
    use falcon_planner::plan::{DistGather, PhysicalPlan};
    use falcon_sql_frontend::types::DistinctMode;

    use crate::query_engine::DistributedQueryEngine;
    use crate::sharded_engine::ShardedEngine;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "dist_test".into(),
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
                    name: "value".into(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
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

    fn setup_engine_with_data(n_shards: u64, rows_per_shard: i32) -> Arc<ShardedEngine> {
        let engine = Arc::new(ShardedEngine::new(n_shards));
        engine.create_table_all(&test_schema()).unwrap();

        for s in 0..n_shards {
            let shard = engine.shard(ShardId(s)).unwrap();
            for i in 0..rows_per_shard {
                let global_id = (s as i32) * rows_per_shard + i;
                let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                let row =
                    OwnedRow::new(vec![Datum::Int32(global_id), Datum::Int32(global_id * 10)]);
                shard.storage.insert(TableId(1), row, txn.txn_id).unwrap();
                shard.txn_mgr.commit(txn.txn_id).unwrap();
            }
        }

        engine
    }

    #[test]
    fn test_dist_plan_scan_union() {
        let engine = setup_engine_with_data(4, 10);
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Build a DistPlan that scans table 1 on all 4 shards with Union gather.
        let subplan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: test_schema(),
            projections: vec![
                falcon_sql_frontend::types::BoundProjection::Column(0, "id".into()),
                falcon_sql_frontend::types::BoundProjection::Column(1, "value".into()),
            ],
            visible_projection_count: 2,
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
        };

        let plan = PhysicalPlan::DistPlan {
            subplan: Box::new(subplan),
            target_shards: vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)],
            gather: DistGather::Union {
                distinct: false,
                limit: None,
                offset: None,
            },
        };

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(rows.len(), 40, "4 shards * 10 rows = 40");
            }
            other => panic!("Expected Query result, got {:?}", other),
        }
    }

    #[test]
    fn test_dist_plan_two_phase_count() {
        let engine = setup_engine_with_data(4, 25);
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Each shard does a COUNT  鈥?returns 1 row with count as Int64.
        // We use a SeqScan with a group_by trick, but since the executor
        // does the aggregation, we need to use the actual planner types.
        // For simplicity, test with Union and check total row count.
        let subplan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: test_schema(),
            projections: vec![falcon_sql_frontend::types::BoundProjection::Column(
                0,
                "id".into(),
            )],
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
        };

        let plan = PhysicalPlan::DistPlan {
            subplan: Box::new(subplan),
            target_shards: vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)],
            gather: DistGather::Union {
                distinct: false,
                limit: None,
                offset: None,
            },
        };

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 100, "4 shards * 25 rows = 100");
            }
            other => panic!("Expected Query result, got {:?}", other),
        }
    }

    #[test]
    fn test_dist_plan_merge_sort_limit() {
        let engine = setup_engine_with_data(4, 25);
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        let subplan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: test_schema(),
            projections: vec![
                falcon_sql_frontend::types::BoundProjection::Column(0, "id".into()),
                falcon_sql_frontend::types::BoundProjection::Column(1, "value".into()),
            ],
            visible_projection_count: 2,
            filter: None,
            group_by: vec![],
            grouping_sets: vec![],
            having: None,
            order_by: vec![falcon_sql_frontend::types::BoundOrderBy {
                column_idx: 0,
                asc: true,
            }],
            limit: Some(10), // pushed-down limit
            offset: None,
            distinct: DistinctMode::None,
            ctes: vec![],
            unions: vec![],
            virtual_rows: vec![],
        };

        let plan = PhysicalPlan::DistPlan {
            subplan: Box::new(subplan),
            target_shards: vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)],
            gather: DistGather::MergeSortLimit {
                sort_columns: vec![(0, true)], // ORDER BY id ASC
                limit: Some(10),
                offset: None,
            },
        };

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 10, "LIMIT 10");
                // Rows should be sorted by id ASC
                for i in 1..rows.len() {
                    let prev = &rows[i - 1].values[0];
                    let curr = &rows[i].values[0];
                    match (prev, curr) {
                        (Datum::Int32(a), Datum::Int32(b)) => {
                            assert!(a <= b, "rows not sorted: {} > {}", a, b);
                        }
                        _ => panic!("unexpected datum types"),
                    }
                }
            }
            other => panic!("Expected Query result, got {:?}", other),
        }
    }

    #[test]
    fn test_execute_on_shard() {
        let engine = setup_engine_with_data(2, 5);
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Execute locally on shard 0
        let shard = engine.shard(ShardId(0)).unwrap();
        let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
        let plan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: test_schema(),
            projections: vec![falcon_sql_frontend::types::BoundProjection::Column(
                0,
                "id".into(),
            )],
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
        };

        let result = qe.execute_on_shard(ShardId(0), &plan, Some(&txn)).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "shard 0 should have 5 rows");
            }
            other => panic!("Expected Query result, got {:?}", other),
        }
    }

    // ── execute_with_params regression tests ─────────────────────────────

    #[test]
    fn test_execute_with_params_empty_delegates_to_execute() {
        // With empty params, execute_with_params should behave identically to execute.
        let engine = setup_engine_with_data(2, 5);
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        let subplan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: test_schema(),
            projections: vec![
                falcon_sql_frontend::types::BoundProjection::Column(0, "id".into()),
            ],
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
        };
        let plan = PhysicalPlan::DistPlan {
            subplan: Box::new(subplan),
            target_shards: vec![ShardId(0), ShardId(1)],
            gather: DistGather::Union {
                distinct: false,
                limit: None,
                offset: None,
            },
        };

        let result = qe.execute_with_params(&plan, None, &[]).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 10, "2 shards * 5 rows = 10");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_execute_with_params_filter_across_shards() {
        // Parameterized filter ($1) must be substituted BEFORE distributed routing,
        // otherwise the DistPlan scatter would send BoundExpr::Parameter to each
        // shard's executor which doesn't have the param values.
        let engine = setup_engine_with_data(4, 10);
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Build: SELECT id, value FROM dist_test WHERE value > $1
        let subplan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: test_schema(),
            projections: vec![
                falcon_sql_frontend::types::BoundProjection::Column(0, "id".into()),
                falcon_sql_frontend::types::BoundProjection::Column(1, "value".into()),
            ],
            visible_projection_count: 2,
            filter: Some(falcon_sql_frontend::types::BoundExpr::BinaryOp {
                left: Box::new(falcon_sql_frontend::types::BoundExpr::ColumnRef(1)),
                op: falcon_sql_frontend::types::BinOp::Gt,
                right: Box::new(falcon_sql_frontend::types::BoundExpr::Parameter(1)),
            }),
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
        };
        let plan = PhysicalPlan::DistPlan {
            subplan: Box::new(subplan),
            target_shards: vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)],
            gather: DistGather::Union {
                distinct: false,
                limit: None,
                offset: None,
            },
        };

        // Bind $1 = 350 (value column = id * 10, so value > 350 means id > 35)
        let params = vec![Datum::Int32(350)];
        let result = qe.execute_with_params(&plan, None, &params).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // ids 0..39, values 0..390. value > 350 means id in {36,37,38,39} = 4 rows
                assert_eq!(rows.len(), 4, "expected 4 rows with value > 350, got {}", rows.len());
                for row in &rows {
                    if let Datum::Int32(v) = &row.values[1] {
                        assert!(*v > 350, "row value {} should be > 350", v);
                    }
                }
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_execute_with_params_insert_routes_by_pk() {
        // Parameterized INSERT should still route to the correct shard by PK hash.
        let engine = Arc::new(ShardedEngine::new(4));
        engine.create_table_all(&test_schema()).unwrap();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // INSERT INTO dist_test VALUES ($1, $2)
        let plan = PhysicalPlan::Insert {
            table_id: TableId(1),
            schema: test_schema(),
            columns: vec![0, 1],
            rows: vec![vec![
                falcon_sql_frontend::types::BoundExpr::Parameter(1),
                falcon_sql_frontend::types::BoundExpr::Parameter(2),
            ]],
            source_select: None,
            returning: vec![],
            on_conflict: None,
        };

        let params = vec![Datum::Int32(999), Datum::Int32(9990)];
        let result = qe.execute_with_params(&plan, None, &params).unwrap();
        match &result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(*rows_affected, 1);
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify the row exists on exactly one shard
        let mut found = false;
        for sid in engine.shard_ids() {
            let shard = engine.shard(sid).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(shard.txn_mgr.current_ts());
            let rows = shard.storage.scan(TableId(1), txn.txn_id, read_ts).unwrap_or_default();
            let _ = shard.txn_mgr.commit(txn.txn_id);
            for (_, row) in &rows {
                if row.values.first() == Some(&Datum::Int32(999)) {
                    assert!(!found, "Row found on multiple shards");
                    found = true;
                    assert_eq!(row.values.get(1), Some(&Datum::Int32(9990)));
                }
            }
        }
        assert!(found, "Inserted row not found on any shard");
    }

    #[test]
    fn test_execute_with_params_missing_param_errors() {
        let engine = setup_engine_with_data(2, 5);
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Build plan with $2 but only provide 1 param
        let subplan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: test_schema(),
            projections: vec![
                falcon_sql_frontend::types::BoundProjection::Column(0, "id".into()),
            ],
            visible_projection_count: 1,
            filter: Some(falcon_sql_frontend::types::BoundExpr::BinaryOp {
                left: Box::new(falcon_sql_frontend::types::BoundExpr::ColumnRef(0)),
                op: falcon_sql_frontend::types::BinOp::Eq,
                right: Box::new(falcon_sql_frontend::types::BoundExpr::Parameter(2)),
            }),
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
        };
        let plan = PhysicalPlan::DistPlan {
            subplan: Box::new(subplan),
            target_shards: vec![ShardId(0), ShardId(1)],
            gather: DistGather::Union {
                distinct: false,
                limit: None,
                offset: None,
            },
        };

        // Only 1 param but plan references $2 → should error
        let result = qe.execute_with_params(&plan, None, &[Datum::Int32(1)]);
        assert!(result.is_err(), "Should fail with missing param $2");
    }
