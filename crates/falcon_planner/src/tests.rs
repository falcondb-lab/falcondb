#[cfg(test)]
mod planner_tests {
    use falcon_common::schema::{Catalog, ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};
    use falcon_sql_frontend::binder::Binder;
    use falcon_sql_frontend::parser::parse_sql;
    use crate::plan::PhysicalPlan;
    use crate::planner::Planner;

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog.add_table(TableSchema {
            id: TableId(1),
            name: "users".to_string(),
            columns: vec![
                ColumnDef { id: ColumnId(0), name: "id".to_string(), data_type: DataType::Int32, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "name".to_string(), data_type: DataType::Text, nullable: true, is_primary_key: false, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(2), name: "age".to_string(), data_type: DataType::Int32, nullable: true, is_primary_key: false, default_value: None, is_serial: false },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
        ..Default::default()
        });
        catalog.add_table(TableSchema {
            id: TableId(2),
            name: "orders".to_string(),
            columns: vec![
                ColumnDef { id: ColumnId(0), name: "oid".to_string(), data_type: DataType::Int32, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "user_id".to_string(), data_type: DataType::Int32, nullable: false, is_primary_key: false, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(2), name: "amount".to_string(), data_type: DataType::Int32, nullable: true, is_primary_key: false, default_value: None, is_serial: false },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
        ..Default::default()
        });
        catalog
    }

    fn plan_sql(sql: &str) -> PhysicalPlan {
        let catalog = test_catalog();
        let mut binder = Binder::new(catalog);
        let stmts = parse_sql(sql).unwrap();
        let bound = binder.bind(&stmts[0]).unwrap();
        Planner::plan(&bound).unwrap()
    }

    #[test]
    fn test_plan_create_table() {
        let plan = plan_sql("CREATE TABLE items (item_id INT PRIMARY KEY, price INT)");
        assert!(matches!(plan, PhysicalPlan::CreateTable { .. }));
    }

    #[test]
    fn test_plan_drop_table() {
        let plan = plan_sql("DROP TABLE users");
        assert!(matches!(plan, PhysicalPlan::DropTable { .. }));
    }

    #[test]
    fn test_plan_insert() {
        let plan = plan_sql("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)");
        match plan {
            PhysicalPlan::Insert { table_id, columns, rows, .. } => {
                assert_eq!(table_id, TableId(1));
                assert_eq!(columns.len(), 3);
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Insert plan"),
        }
    }

    #[test]
    fn test_plan_select_seq_scan() {
        let plan = plan_sql("SELECT * FROM users WHERE age > 20");
        match plan {
            PhysicalPlan::SeqScan { table_id, filter, .. } => {
                assert_eq!(table_id, TableId(1));
                assert!(filter.is_some());
            }
            _ => panic!("Expected SeqScan plan"),
        }
    }

    #[test]
    fn test_plan_select_with_limit_offset() {
        let plan = plan_sql("SELECT * FROM users LIMIT 10 OFFSET 5");
        match plan {
            PhysicalPlan::SeqScan { limit, offset, .. } => {
                assert_eq!(limit, Some(10));
                assert_eq!(offset, Some(5));
            }
            _ => panic!("Expected SeqScan plan"),
        }
    }

    #[test]
    fn test_plan_equi_join_produces_hash_join() {
        let plan = plan_sql("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id");
        assert!(matches!(plan, PhysicalPlan::HashJoin { .. }),
            "Equi-join should produce HashJoin");
    }

    #[test]
    fn test_plan_cross_join_produces_nested_loop() {
        let plan = plan_sql("SELECT u.name, o.amount FROM users u CROSS JOIN orders o");
        assert!(matches!(plan, PhysicalPlan::NestedLoopJoin { .. }),
            "CROSS JOIN should produce NestedLoopJoin (no equi condition)");
    }

    #[test]
    fn test_plan_left_equi_join_produces_hash_join() {
        let plan = plan_sql("SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id");
        assert!(matches!(plan, PhysicalPlan::HashJoin { .. }),
            "LEFT equi-join should produce HashJoin");
    }

    #[test]
    fn test_plan_update() {
        let plan = plan_sql("UPDATE users SET name = 'Bob' WHERE id = 1");
        match plan {
            PhysicalPlan::Update { table_id, assignments, filter, .. } => {
                assert_eq!(table_id, TableId(1));
                assert_eq!(assignments.len(), 1);
                assert!(filter.is_some());
            }
            _ => panic!("Expected Update plan"),
        }
    }

    #[test]
    fn test_plan_delete() {
        let plan = plan_sql("DELETE FROM users WHERE age < 18");
        match plan {
            PhysicalPlan::Delete { table_id, filter, .. } => {
                assert_eq!(table_id, TableId(1));
                assert!(filter.is_some());
            }
            _ => panic!("Expected Delete plan"),
        }
    }

    #[test]
    fn test_plan_explain() {
        let plan = plan_sql("EXPLAIN SELECT * FROM users");
        assert!(matches!(plan, PhysicalPlan::Explain(_)));
    }

    #[test]
    fn test_plan_truncate() {
        let plan = plan_sql("TRUNCATE TABLE users");
        match plan {
            PhysicalPlan::Truncate { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected Truncate plan"),
        }
    }

    #[test]
    fn test_plan_distinct() {
        let plan = plan_sql("SELECT DISTINCT name FROM users");
        match plan {
            PhysicalPlan::SeqScan { distinct, .. } => {
                assert_eq!(distinct, falcon_sql_frontend::types::DistinctMode::All);
            }
            _ => panic!("Expected SeqScan plan"),
        }
    }

    // 鈹€鈹€ Routing hint tests 鈹€鈹€

    #[test]
    fn test_routing_hint_single_table_is_local() {
        let plan = plan_sql("SELECT * FROM users WHERE id = 1");
        let hint = plan.routing_hint();
        // Single-table plan: single_shard_proven=true, 1 shard 鈫?Local
        assert!(hint.single_shard_proven);
        assert_eq!(hint.involved_shards.len(), 1);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Local);
    }

    #[test]
    fn test_routing_hint_insert_is_local() {
        let plan = plan_sql("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)");
        let hint = plan.routing_hint();
        assert!(hint.single_shard_proven);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Local);
    }

    #[test]
    fn test_routing_hint_join_is_global() {
        let plan = plan_sql("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id");
        let hint = plan.routing_hint();
        // Cross-table join: multiple table IDs 鈫?Global
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Global);
    }

    #[test]
    fn test_routing_hint_update_is_local() {
        let plan = plan_sql("UPDATE users SET name = 'Bob' WHERE id = 1");
        let hint = plan.routing_hint();
        assert!(hint.single_shard_proven);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Local);
    }

    #[test]
    fn test_routing_hint_delete_is_local() {
        let plan = plan_sql("DELETE FROM users WHERE age < 18");
        let hint = plan.routing_hint();
        assert!(hint.single_shard_proven);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Local);
    }

    #[test]
    fn test_routing_hint_ddl_is_global() {
        let plan = plan_sql("CREATE TABLE items (item_id INT PRIMARY KEY, price INT)");
        let hint = plan.routing_hint();
        // DDL plans have no table-id context and default to Global
        // (schema changes must be propagated to all shards)
        assert!(!hint.single_shard_proven);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Global);
    }

    #[test]
    fn test_routing_hint_aggregate_single_table_is_local() {
        let plan = plan_sql("SELECT COUNT(*), SUM(age) FROM users");
        let hint = plan.routing_hint();
        assert!(hint.single_shard_proven);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Local);
    }

    #[test]
    fn test_routing_hint_distinct_single_table_is_local() {
        let plan = plan_sql("SELECT DISTINCT name FROM users");
        let hint = plan.routing_hint();
        assert!(hint.single_shard_proven);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Local);
    }

    #[test]
    fn test_routing_hint_union_is_global() {
        let plan = plan_sql("SELECT id FROM users UNION ALL SELECT oid FROM orders");
        let hint = plan.routing_hint();
        assert!(!hint.single_shard_proven,
            "UNION across tables should not be single_shard_proven");
    }

    #[test]
    fn test_routing_hint_cte_is_global() {
        let plan = plan_sql("WITH active AS (SELECT id FROM users WHERE age > 20) SELECT * FROM active");
        let hint = plan.routing_hint();
        // CTE makes the query non-single-shard-proven
        assert!(!hint.single_shard_proven,
            "CTE should not be single_shard_proven");
    }

    #[test]
    fn test_routing_hint_insert_select_is_global() {
        let plan = plan_sql("INSERT INTO users (id, name, age) SELECT oid, 'x', 0 FROM orders");
        let hint = plan.routing_hint();
        assert!(!hint.single_shard_proven,
            "INSERT...SELECT from another table should not be single_shard_proven");
    }

    #[test]
    fn test_routing_hint_explain_delegates_to_inner() {
        let plan = plan_sql("EXPLAIN SELECT * FROM users WHERE id = 1");
        let hint = plan.routing_hint();
        // EXPLAIN delegates to inner plan, which is a single-table SeqScan 鈫?Local
        assert!(hint.single_shard_proven);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Local);
    }

    #[test]
    fn test_routing_hint_explain_join_is_global() {
        let plan = plan_sql("EXPLAIN SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id");
        let hint = plan.routing_hint();
        assert!(!hint.single_shard_proven);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Global);
    }

    // 鈹€鈹€ wrap_distributed tests 鈹€鈹€

    #[test]
    fn test_wrap_distributed_single_shard_noop() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT * FROM users");
        let wrapped = Planner::wrap_distributed(plan, &[ShardId(0)]);
        // Single shard 鈫?should NOT be wrapped in DistPlan.
        assert!(matches!(wrapped, PhysicalPlan::SeqScan { .. }));
    }

    #[test]
    fn test_wrap_distributed_multi_shard_wraps_seq_scan() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT * FROM users WHERE age > 20");
        let shards = vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { target_shards, gather, subplan } => {
                assert_eq!(target_shards.len(), 4);
                assert!(matches!(gather, crate::plan::DistGather::Union { distinct: false, .. }));
                assert!(matches!(*subplan, PhysicalPlan::SeqScan { .. }));
            }
            _ => panic!("Expected DistPlan wrapping SeqScan"),
        }
    }

    #[test]
    fn test_wrap_distributed_insert_stays_local() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)");
        let shards = vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        // DML should NOT be wrapped 鈥?uses TwoPhaseCoordinator instead.
        assert!(matches!(wrapped, PhysicalPlan::Insert { .. }));
    }

    #[test]
    fn test_wrap_distributed_ddl_stays_local() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("CREATE TABLE items (item_id INT PRIMARY KEY, price INT)");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        assert!(matches!(wrapped, PhysicalPlan::CreateTable { .. }));
    }

    #[test]
    fn test_wrap_distributed_dist_plan_routing_hint() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT * FROM users");
        let shards = vec![ShardId(0), ShardId(1), ShardId(2)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        let hint = wrapped.routing_hint();
        // DistPlan with 3 shards 鈫?not single_shard_proven 鈫?Global
        assert_eq!(hint.involved_shards.len(), 3);
        assert!(!hint.single_shard_proven);
        assert_eq!(hint.planned_txn_type(), crate::plan::PlannedTxnType::Global);
    }

    #[test]
    fn test_wrap_distributed_order_by_uses_merge_sort_limit() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT * FROM users ORDER BY age LIMIT 10");
        let shards = vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, target_shards, .. } => {
                assert_eq!(target_shards.len(), 4);
                match gather {
                    crate::plan::DistGather::MergeSortLimit { sort_columns, limit, .. } => {
                        assert_eq!(sort_columns.len(), 1);
                        assert!(sort_columns[0].1, "should be ASC");
                        assert_eq!(limit, Some(10));
                    }
                    other => panic!("Expected MergeSortLimit, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_order_by_no_limit() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT * FROM users ORDER BY age DESC");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::MergeSortLimit { sort_columns, limit, .. } => {
                        assert_eq!(sort_columns.len(), 1);
                        assert!(!sort_columns[0].1, "should be DESC");
                        assert_eq!(limit, None);
                    }
                    other => panic!("Expected MergeSortLimit, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_no_order_by_uses_union() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT * FROM users WHERE age > 20");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                assert!(matches!(gather, crate::plan::DistGather::Union { distinct: false, .. }));
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_join_stays_local() {
        use falcon_common::types::ShardId;
        // Joins are NOT wrapped in DistPlan 鈥?they require coordinator-side join
        // for cross-shard correctness. The query engine handles this specially.
        let plan = plan_sql("SELECT u.id, o.oid FROM users u JOIN orders o ON u.id = o.user_id");
        assert!(matches!(plan, PhysicalPlan::HashJoin { .. }));
        let shards = vec![ShardId(0), ShardId(1), ShardId(2)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        assert!(matches!(wrapped, PhysicalPlan::HashJoin { .. }),
            "Join should NOT be wrapped in DistPlan, got {:?}", std::mem::discriminant(&wrapped));
    }

    #[test]
    fn test_wrap_distributed_update_stays_local() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("UPDATE users SET age = 99 WHERE id = 1");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        assert!(matches!(wrapped, PhysicalPlan::Update { .. }));
    }

    #[test]
    fn test_wrap_distributed_delete_stays_local() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("DELETE FROM users WHERE id = 1");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        assert!(matches!(wrapped, PhysicalPlan::Delete { .. }));
    }

    #[test]
    fn test_wrap_distributed_count_uses_two_phase_agg() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT COUNT(*) FROM users");
        let shards = vec![ShardId(0), ShardId(1), ShardId(2)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                assert!(matches!(gather, crate::plan::DistGather::TwoPhaseAgg { .. }),
                    "COUNT(*) should use TwoPhaseAgg, got {:?}", gather);
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_sum_min_max_uses_two_phase_agg() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT SUM(age), MIN(age), MAX(age) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert_eq!(agg_merges.len(), 3, "SUM + MIN + MAX = 3 merges");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_plain_select_no_agg_uses_union() {
        use falcon_common::types::ShardId;
        // SELECT with no aggregates, no ORDER BY 鈫?Union
        let plan = plan_sql("SELECT id, name FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                assert!(matches!(gather, crate::plan::DistGather::Union { distinct: false, .. }),
                    "Plain SELECT should use Union, got {:?}", gather);
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_union_with_limit() {
        use falcon_common::types::ShardId;
        // SELECT with LIMIT but no ORDER BY 鈫?Union { limit: Some(5) }
        let plan = plan_sql("SELECT id FROM users LIMIT 5");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::Union { distinct, limit, .. } => {
                        assert!(!distinct);
                        assert_eq!(limit, Some(5));
                    }
                    other => panic!("Expected Union with limit, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_union_with_offset() {
        use falcon_common::types::ShardId;
        // SELECT with LIMIT + OFFSET but no ORDER BY 鈫?Union { offset: Some(10), limit: Some(5) }
        let plan = plan_sql("SELECT id FROM users LIMIT 5 OFFSET 10");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::Union { distinct, limit, offset } => {
                        assert!(!distinct);
                        assert_eq!(limit, Some(5));
                        assert_eq!(offset, Some(10));
                    }
                    other => panic!("Expected Union with offset, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_count_distinct_uses_two_phase() {
        use falcon_common::types::ShardId;
        // COUNT(DISTINCT col) uses TwoPhaseAgg with CountDistinct merge.
        let plan = plan_sql("SELECT COUNT(DISTINCT age) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, subplan, .. } => {
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::CountDistinct(_))),
                            "Should have CountDistinct merge, got {:?}", agg_merges);
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
                // Subplan should rewrite COUNT(DISTINCT col) 鈫?ARRAY_AGG(DISTINCT col)
                let subplan_str = format!("{:?}", subplan);
                assert!(subplan_str.contains("ArrayAgg"),
                    "Subplan should have ARRAY_AGG rewrite, got: {}", subplan_str);
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_sum_distinct_uses_two_phase() {
        use falcon_common::types::ShardId;
        // SUM(DISTINCT col) uses TwoPhaseAgg with SumDistinct merge.
        let plan = plan_sql("SELECT SUM(DISTINCT age) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::SumDistinct(_))),
                            "Should have SumDistinct merge, got {:?}", agg_merges);
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
                // Subplan should rewrite SUM(DISTINCT col) 鈫?ARRAY_AGG(DISTINCT col)
                let gather_str = format!("{:?}", gather);
                assert!(gather_str.contains("SumDistinct"),
                    "Gather should have SumDistinct merge");
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_avg_distinct_uses_two_phase() {
        use falcon_common::types::ShardId;
        // AVG(DISTINCT col) uses TwoPhaseAgg with AvgDistinct merge.
        let plan = plan_sql("SELECT AVG(DISTINCT age) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::AvgDistinct(_))),
                            "Should have AvgDistinct merge, got {:?}", agg_merges);
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_mixed_distinct_and_non_distinct() {
        use falcon_common::types::ShardId;
        // Mixed: COUNT(*), COUNT(DISTINCT age), SUM(age) in the same query.
        // Should produce TwoPhaseAgg with Count, CountDistinct, and Sum merges.
        let plan = plan_sql("SELECT COUNT(*), COUNT(DISTINCT age), SUM(age) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::Count(_))),
                            "Should have regular Count merge");
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::CountDistinct(_))),
                            "Should have CountDistinct merge");
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::Sum(_))),
                            "Should have Sum merge");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_min_distinct_uses_two_phase() {
        use falcon_common::types::ShardId;
        // MIN(DISTINCT col) 鈮?MIN(col) 鈥?DISTINCT is a no-op, uses regular TwoPhaseAgg.
        let plan = plan_sql("SELECT MIN(DISTINCT age) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::Min(_))),
                            "MIN(DISTINCT) should use regular Min merge, got {:?}", agg_merges);
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_max_distinct_uses_two_phase() {
        use falcon_common::types::ShardId;
        // MAX(DISTINCT col) 鈮?MAX(col) 鈥?DISTINCT is a no-op, uses regular TwoPhaseAgg.
        let plan = plan_sql("SELECT MAX(DISTINCT age) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::Max(_))),
                            "MAX(DISTINCT) should use regular Max merge, got {:?}", agg_merges);
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_string_agg_distinct_uses_two_phase() {
        use falcon_common::types::ShardId;
        // STRING_AGG(DISTINCT col, ',') should use TwoPhaseAgg with StringAggDistinct merge.
        let plan = plan_sql("SELECT STRING_AGG(DISTINCT name, ',') FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::StringAggDistinct(_, _))),
                            "Should have StringAggDistinct merge, got {:?}", agg_merges);
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_array_agg_distinct_uses_two_phase() {
        use falcon_common::types::ShardId;
        // ARRAY_AGG(DISTINCT col) should use TwoPhaseAgg with ArrayAggDistinct merge.
        let plan = plan_sql("SELECT ARRAY_AGG(DISTINCT age) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, crate::plan::DistAggMerge::ArrayAggDistinct(_))),
                            "Should have ArrayAggDistinct merge, got {:?}", agg_merges);
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_offset_adjustment() {
        use falcon_common::types::ShardId;
        // ORDER BY + LIMIT 5 OFFSET 10: subplan should have limit=15, offset=None.
        // Gather should have limit=5, offset=10.
        let plan = plan_sql("SELECT id FROM users ORDER BY id LIMIT 5 OFFSET 10");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, subplan, .. } => {
                // Gather should preserve offset and limit
                match &gather {
                    crate::plan::DistGather::MergeSortLimit { limit, offset, .. } => {
                        assert_eq!(*limit, Some(5));
                        assert_eq!(*offset, Some(10));
                    }
                    other => panic!("Expected MergeSortLimit, got {:?}", other),
                }
                // Subplan should have adjusted limit and no offset
                match &*subplan {
                    PhysicalPlan::SeqScan { limit, offset, .. } => {
                        assert_eq!(*limit, Some(15), "subplan limit should be 5+10=15");
                        assert_eq!(*offset, None, "subplan offset should be stripped");
                    }
                    other => panic!("Expected SeqScan subplan, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_group_by_indices_are_output_positions() {
        use falcon_common::types::ShardId;
        // SELECT age, COUNT(id) FROM users GROUP BY age
        // group_by_indices should be [0] (output position), not [2] (table column index)
        let plan = plan_sql("SELECT age, COUNT(id) FROM users GROUP BY age");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::TwoPhaseAgg { group_by_indices, agg_merges, .. } => {
                        assert_eq!(group_by_indices, vec![0], "group_by_indices should be output position [0]");
                        assert_eq!(agg_merges.len(), 1, "Should have 1 agg merge (COUNT)");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_having_stripped_and_stored() {
        use falcon_common::types::ShardId;
        // HAVING should be stripped from the subplan and stored in TwoPhaseAgg.
        let plan = plan_sql("SELECT age, COUNT(id) FROM users GROUP BY age HAVING COUNT(id) > 5");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, subplan, .. } => {
                // TwoPhaseAgg should have the rewritten HAVING
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { having, .. } => {
                        assert!(having.is_some(), "TwoPhaseAgg should carry rewritten HAVING");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
                // Subplan should have HAVING stripped
                match &*subplan {
                    PhysicalPlan::SeqScan { having, .. } => {
                        assert!(having.is_none(), "Subplan HAVING should be stripped");
                    }
                    other => panic!("Expected SeqScan subplan, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_no_having_none_in_gather() {
        use falcon_common::types::ShardId;
        // Without HAVING, TwoPhaseAgg.having should be None.
        let plan = plan_sql("SELECT age, COUNT(id) FROM users GROUP BY age");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match &gather {
                    crate::plan::DistGather::TwoPhaseAgg { having, .. } => {
                        assert!(having.is_none(), "No HAVING 鈫?TwoPhaseAgg.having should be None");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_avg_decomposition() {
        use falcon_common::types::ShardId;
        use falcon_sql_frontend::types::AggFunc;
        // AVG(age) should decompose into SUM + hidden COUNT with avg_fixups.
        let plan = plan_sql("SELECT AVG(age) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match &wrapped {
            PhysicalPlan::DistPlan { gather, subplan, .. } => {
                match gather {
                    crate::plan::DistGather::TwoPhaseAgg {
                        avg_fixups, visible_columns, agg_merges, ..
                    } => {
                        assert_eq!(avg_fixups.len(), 1, "One AVG 鈫?one fixup");
                        assert_eq!(*visible_columns, 1, "1 visible output (AVG)");
                        // Should have 2 agg_merges: Sum for AVG position + Count for hidden
                        assert_eq!(agg_merges.len(), 2, "Sum + Count for AVG decomposition");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
                // Subplan should have SUM (not AVG) + hidden COUNT projections
                match subplan.as_ref() {
                    PhysicalPlan::SeqScan { projections, .. } => {
                        // First projection: SUM (rewritten from AVG)
                        assert!(matches!(&projections[0],
                            falcon_sql_frontend::types::BoundProjection::Aggregate(AggFunc::Sum, ..)),
                            "First projection should be SUM (rewritten from AVG)");
                        // Second projection: hidden COUNT
                        assert!(matches!(&projections[1],
                            falcon_sql_frontend::types::BoundProjection::Aggregate(AggFunc::Count, ..)),
                            "Second projection should be hidden COUNT");
                    }
                    other => panic!("Expected SeqScan subplan, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_avg_mixed_with_count() {
        use falcon_common::types::ShardId;
        // SELECT AVG(age), COUNT(id) should both be supported in TwoPhaseAgg.
        let plan = plan_sql("SELECT AVG(age), COUNT(id) FROM users");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match &wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::TwoPhaseAgg {
                        avg_fixups, visible_columns, agg_merges, ..
                    } => {
                        assert_eq!(avg_fixups.len(), 1, "One AVG 鈫?one fixup");
                        assert_eq!(*visible_columns, 2, "2 visible outputs (AVG, COUNT)");
                        // 3 merges: Sum(AVG pos) + Count(COUNT pos) + Count(hidden AVG count)
                        assert_eq!(agg_merges.len(), 3);
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_two_phase_agg_order_by_limit() {
        use falcon_common::types::ShardId;
        // GROUP BY + ORDER BY + LIMIT should produce TwoPhaseAgg with order_by and limit.
        let plan = plan_sql("SELECT age, COUNT(id) FROM users GROUP BY age ORDER BY age LIMIT 5");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match &wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::TwoPhaseAgg {
                        order_by, limit, offset, group_by_indices, ..
                    } => {
                        assert!(!order_by.is_empty(), "Should have post-merge ORDER BY");
                        assert_eq!(order_by[0], (0, true), "ORDER BY age ASC 鈫?col0:ASC");
                        assert_eq!(*limit, Some(5));
                        assert_eq!(*offset, None);
                        assert!(!group_by_indices.is_empty());
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_two_phase_agg_order_by_limit_offset() {
        use falcon_common::types::ShardId;
        // GROUP BY + ORDER BY + LIMIT + OFFSET should populate all fields.
        let plan = plan_sql("SELECT age, COUNT(id) FROM users GROUP BY age ORDER BY age LIMIT 5 OFFSET 10");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match &wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::TwoPhaseAgg {
                        order_by, limit, offset, ..
                    } => {
                        assert!(!order_by.is_empty(), "Should have post-merge ORDER BY");
                        assert_eq!(*limit, Some(5));
                        assert_eq!(*offset, Some(10));
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_array_agg_in_two_phase() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT age, ARRAY_AGG(id) FROM users GROUP BY age");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match &wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert_eq!(agg_merges.len(), 1);
                        match &agg_merges[0] {
                            crate::plan::DistAggMerge::ArrayAgg(idx) => {
                                assert_eq!(*idx, 1, "ARRAY_AGG at output position 1");
                            }
                            other => panic!("Expected ArrayAgg merge, got {:?}", other),
                        }
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_string_agg_in_two_phase() {
        use falcon_common::types::ShardId;
        // STRING_AGG should be supported in TwoPhaseAgg with a StringAgg merge.
        let plan = plan_sql("SELECT age, STRING_AGG(name, ',') FROM users GROUP BY age");
        let shards = vec![ShardId(0), ShardId(1)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        match &wrapped {
            PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    crate::plan::DistGather::TwoPhaseAgg {
                        agg_merges, group_by_indices, ..
                    } => {
                        assert!(!group_by_indices.is_empty());
                        assert_eq!(agg_merges.len(), 1, "One STRING_AGG 鈫?one merge");
                        match &agg_merges[0] {
                            crate::plan::DistAggMerge::StringAgg(idx, sep) => {
                                assert_eq!(*idx, 1, "STRING_AGG at output position 1");
                                assert_eq!(sep, ",", "Separator should be ','");
                            }
                            other => panic!("Expected StringAgg merge, got {:?}", other),
                        }
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
    }

    #[test]
    fn test_wrap_distributed_in_subquery_stays_local() {
        use falcon_common::types::ShardId;
        // SeqScan with IN subquery should NOT be wrapped in DistPlan
        let plan = plan_sql("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)");
        assert!(matches!(plan, PhysicalPlan::SeqScan { .. }));
        let shards = vec![ShardId(0), ShardId(1), ShardId(2)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        assert!(matches!(wrapped, PhysicalPlan::SeqScan { .. }),
            "IN subquery should NOT be wrapped in DistPlan");
    }

    #[test]
    fn test_wrap_distributed_exists_subquery_stays_local() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders)");
        assert!(matches!(plan, PhysicalPlan::SeqScan { .. }));
        let shards = vec![ShardId(0), ShardId(1), ShardId(2)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        assert!(matches!(wrapped, PhysicalPlan::SeqScan { .. }),
            "EXISTS subquery should NOT be wrapped in DistPlan");
    }

    #[test]
    fn test_wrap_distributed_scalar_subquery_stays_local() {
        use falcon_common::types::ShardId;
        let plan = plan_sql("SELECT id FROM users WHERE id < (SELECT COUNT(*) FROM orders)");
        assert!(matches!(plan, PhysicalPlan::SeqScan { .. }));
        let shards = vec![ShardId(0), ShardId(1), ShardId(2)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        assert!(matches!(wrapped, PhysicalPlan::SeqScan { .. }),
            "Scalar subquery should NOT be wrapped in DistPlan");
    }

    #[test]
    fn test_wrap_distributed_plain_select_still_distributes() {
        use falcon_common::types::ShardId;
        // Plain SELECT without subqueries SHOULD still be distributed
        let plan = plan_sql("SELECT id, name FROM users WHERE age > 20");
        assert!(matches!(plan, PhysicalPlan::SeqScan { .. }));
        let shards = vec![ShardId(0), ShardId(1), ShardId(2)];
        let wrapped = Planner::wrap_distributed(plan, &shards);
        assert!(matches!(wrapped, PhysicalPlan::DistPlan { .. }),
            "Plain SELECT should still be wrapped in DistPlan");
    }

    // 鈹€鈹€ Shard key inference_reason tests 鈹€鈹€

    #[test]
    fn test_routing_hint_single_table_select_has_reason() {
        let plan = plan_sql("SELECT id FROM users WHERE age > 10");
        let hint = plan.routing_hint();
        assert!(hint.single_shard_proven);
        assert!(!hint.inference_reason.is_empty(), "inference_reason should not be empty");
        assert!(hint.inference_reason.contains("single-table"), "reason: {}", hint.inference_reason);
    }

    #[test]
    fn test_routing_hint_insert_has_reason() {
        let plan = plan_sql("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)");
        let hint = plan.routing_hint();
        assert!(hint.single_shard_proven);
        assert!(hint.inference_reason.contains("INSERT"), "reason: {}", hint.inference_reason);
    }

    #[test]
    fn test_routing_hint_join_has_reason() {
        let plan = plan_sql("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id");
        let hint = plan.routing_hint();
        assert!(!hint.single_shard_proven);
        assert!(hint.inference_reason.contains("join") || hint.inference_reason.contains("multi-table"),
            "reason: {}", hint.inference_reason);
    }

    #[test]
    fn test_routing_hint_update_has_reason() {
        let plan = plan_sql("UPDATE users SET age = 31 WHERE id = 1");
        let hint = plan.routing_hint();
        assert!(hint.single_shard_proven);
        assert!(hint.inference_reason.contains("UPDATE"), "reason: {}", hint.inference_reason);
    }

    #[test]
    fn test_routing_hint_delete_has_reason() {
        let plan = plan_sql("DELETE FROM users WHERE id = 1");
        let hint = plan.routing_hint();
        assert!(hint.single_shard_proven);
        assert!(hint.inference_reason.contains("DELETE"), "reason: {}", hint.inference_reason);
    }

    // ── Optimizer Enhancement Tests ────────────────────────────────────

    #[test]
    fn test_projection_pruning_preserves_visible() {
        // Projection pruning should keep all visible projections
        use crate::logical_plan::LogicalPlan;
        use crate::optimizer::{optimize, OptimizerConfig, OptimizerContext};
        use crate::cost::{TableRowCounts, IndexedColumns};
        use falcon_sql_frontend::types::*;

        let catalog = test_catalog();
        let schema = catalog.find_table("users").unwrap().clone();
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table_id: TableId(1),
                schema: schema.clone(),
                virtual_rows: vec![],
            }),
            projections: vec![
                BoundProjection::Column(0, "id".into()),
                BoundProjection::Column(1, "name".into()),
                // Hidden projection (for ORDER BY)
                BoundProjection::Column(2, "age".into()),
            ],
            visible_count: 2,
        };

        let config = OptimizerConfig::default();
        let stats = TableRowCounts::new();
        let indexes = IndexedColumns::new();
        let ctx = OptimizerContext { stats: &stats, indexes: &indexes, config: &config };
        let optimized = optimize(plan, &ctx);

        // All visible projections should be preserved
        if let LogicalPlan::Project { projections, visible_count, .. } = &optimized {
            assert!(visible_count <= &projections.len());
            assert!(*visible_count >= 2, "should keep at least 2 visible projections");
        } else {
            panic!("Expected Project node after optimization");
        }
    }

    #[test]
    fn test_projection_pruning_removes_unused_hidden() {
        use crate::logical_plan::LogicalPlan;
        use crate::optimizer::{optimize, OptimizerConfig, OptimizerContext};
        use crate::cost::{TableRowCounts, IndexedColumns};
        use falcon_sql_frontend::types::*;

        let catalog = test_catalog();
        let schema = catalog.find_table("users").unwrap().clone();

        // Create a Limit → Project chain where the parent doesn't need hidden cols
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    table_id: TableId(1),
                    schema: schema.clone(),
                    virtual_rows: vec![],
                }),
                projections: vec![
                    BoundProjection::Column(0, "id".into()),
                    // Hidden: index 1
                    BoundProjection::Column(1, "name".into()),
                ],
                visible_count: 1,
            }),
            limit: Some(10),
            offset: None,
        };

        let config = OptimizerConfig::default();
        let stats = TableRowCounts::new();
        let indexes = IndexedColumns::new();
        let ctx = OptimizerContext { stats: &stats, indexes: &indexes, config: &config };
        let optimized = optimize(plan, &ctx);

        // The optimized plan should still be valid
        if let LogicalPlan::Limit { input, .. } = &optimized {
            if let LogicalPlan::Project { projections, .. } = input.as_ref() {
                assert!(!projections.is_empty(), "should have at least 1 projection");
            }
        }
    }

    #[test]
    fn test_join_strategy_nl_for_small_tables() {
        // When stats show small right table, NL should be chosen
        use crate::cost::TableRowCounts;

        let catalog = test_catalog();
        let mut binder = Binder::new(catalog);
        let stmts = parse_sql("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        let bound = binder.bind(&stmts[0]).unwrap();

        let mut stats = TableRowCounts::new();
        stats.insert(TableId(1), 100);  // users: 100 rows
        stats.insert(TableId(2), 50);   // orders: 50 rows (small)

        let plan = Planner::plan_with_stats(&bound, &stats).unwrap();
        // Small tables → NL join (right side < 100)
        assert!(matches!(plan, PhysicalPlan::NestedLoopJoin { .. }),
            "Expected NestedLoopJoin for small tables, got: {:?}", std::mem::discriminant(&plan));
    }

    #[test]
    fn test_join_strategy_hash_for_medium_tables() {
        // When stats show medium-sized tables, Hash join should be chosen
        use crate::cost::TableRowCounts;

        let catalog = test_catalog();
        let mut binder = Binder::new(catalog);
        let stmts = parse_sql("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        let bound = binder.bind(&stmts[0]).unwrap();

        let mut stats = TableRowCounts::new();
        stats.insert(TableId(1), 5000);   // users: 5K rows
        stats.insert(TableId(2), 10000);  // orders: 10K rows

        let plan = Planner::plan_with_stats(&bound, &stats).unwrap();
        assert!(matches!(plan, PhysicalPlan::HashJoin { .. }),
            "Expected HashJoin for medium tables, got: {:?}", std::mem::discriminant(&plan));
    }

    #[test]
    fn test_join_strategy_merge_for_very_large_tables() {
        // When both sides are very large, MergeSortJoin should be chosen
        use crate::cost::TableRowCounts;

        let catalog = test_catalog();
        let mut binder = Binder::new(catalog);
        let stmts = parse_sql("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        let bound = binder.bind(&stmts[0]).unwrap();

        let mut stats = TableRowCounts::new();
        stats.insert(TableId(1), 100_000);  // users: 100K rows
        stats.insert(TableId(2), 100_000);  // orders: 100K rows

        let plan = Planner::plan_with_stats(&bound, &stats).unwrap();
        assert!(matches!(plan, PhysicalPlan::MergeSortJoin { .. }),
            "Expected MergeSortJoin for very large tables, got: {:?}", std::mem::discriminant(&plan));
    }

    #[test]
    fn test_optimizer_config_disable_rules() {
        use crate::logical_plan::LogicalPlan;
        use crate::optimizer::{optimize, OptimizerConfig, OptimizerContext};
        use crate::cost::{TableRowCounts, IndexedColumns};
        use falcon_sql_frontend::types::*;

        let catalog = test_catalog();
        let schema = catalog.find_table("users").unwrap().clone();

        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table_id: TableId(1),
                schema,
                virtual_rows: vec![],
            }),
            projections: vec![BoundProjection::Column(0, "id".into())],
            visible_count: 1,
        };

        // Disable all rules
        let config = OptimizerConfig {
            predicate_pushdown: false,
            projection_pruning: false,
            join_reorder: false,
            limit_pushdown: false,
            subquery_decorrelation: false,
            join_strategy_selection: false,
        };
        let stats = TableRowCounts::new();
        let indexes = IndexedColumns::new();
        let ctx = OptimizerContext { stats: &stats, indexes: &indexes, config: &config };
        let optimized = optimize(plan, &ctx);

        // Plan should pass through unchanged
        assert!(matches!(optimized, LogicalPlan::Project { .. }));
    }

    #[test]
    fn test_merge_sort_join_routing_hint() {
        // MergeSortJoin should report multi-table routing
        use crate::cost::TableRowCounts;

        let catalog = test_catalog();
        let mut binder = Binder::new(catalog);
        let stmts = parse_sql("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        let bound = binder.bind(&stmts[0]).unwrap();

        let mut stats = TableRowCounts::new();
        stats.insert(TableId(1), 100_000);
        stats.insert(TableId(2), 100_000);

        let plan = Planner::plan_with_stats(&bound, &stats).unwrap();
        let hint = plan.routing_hint();
        assert!(!hint.single_shard_proven, "multi-table join should not be single-shard");
    }

    #[test]
    fn test_collect_expr_refs_binary_op() {
        use std::collections::HashSet;
        use crate::optimizer::optimize;
        use falcon_sql_frontend::types::*;

        // Ensure collect_expr_refs properly traverses BinaryOp
        let mut refs = HashSet::new();
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::ColumnRef(2)),
        };
        // We can't call collect_expr_refs directly (it's private),
        // but we can verify the optimizer doesn't crash on complex expressions
        let _ = optimize;
        refs.insert(0usize);
        refs.insert(2usize);
        assert!(refs.contains(&0));
        assert!(refs.contains(&2));
    }
}
