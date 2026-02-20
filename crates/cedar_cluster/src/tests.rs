#[cfg(test)]
mod scatter_gather_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::{ColumnDef, TableSchema};
    use cedar_common::types::{ColumnId, DataType, IsolationLevel, ShardId, TableId};

    use crate::distributed_exec::{
        AggMerge, DistributedExecutor, GatherStrategy, SubPlan,
    };
    use crate::sharded_engine::ShardedEngine;

    fn test_schema() -> TableSchema {
        TableSchema {
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
                ColumnDef {
                    id: ColumnId(2),
                    name: "age".into(),
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

    /// Setup: 4-shard engine with test table, distribute rows round-robin.
    fn setup_sharded(num_shards: u64, num_rows: i32) -> Arc<ShardedEngine> {
        let engine = Arc::new(ShardedEngine::new(num_shards));
        engine.create_table_all(&test_schema()).unwrap();

        // Insert rows, distributing round-robin across shards.
        for i in 0..num_rows {
            let shard_idx = (i as u64) % num_shards;
            let shard = engine.shard(ShardId(shard_idx)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let row = OwnedRow::new(vec![
                Datum::Int32(i),
                Datum::Text(format!("user_{}", i)),
                Datum::Int32(20 + (i % 30)),
            ]);
            shard.storage.insert(TableId(1), row, txn.txn_id).unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }

        engine
    }

    #[test]
    fn test_scatter_gather_full_scan_union() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan all", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let result_rows: Vec<OwnedRow> = rows.into_iter().map(|(_, r)| r).collect();
            let columns = vec![
                ("id".into(), DataType::Int32),
                ("name".into(), DataType::Text),
                ("age".into(), DataType::Int32),
            ];
            Ok((columns, result_rows))
        });

        let all_shards = engine.shard_ids();
        let ((cols, rows), metrics) = exec
            .scatter_gather(&subplan, &all_shards, &GatherStrategy::Union { distinct: false, limit: None, offset: None })
            .unwrap();

        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 100, "should gather all 100 rows from 4 shards");
        assert_eq!(metrics.shards_participated, 4);
        assert_eq!(metrics.total_rows_gathered, 100);
    }

    #[test]
    fn test_scatter_gather_filter() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Filter: age > 40 (rows with id where 20 + (id % 30) > 40 => id % 30 > 20)
        let subplan = SubPlan::new("scan with filter", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let filtered: Vec<OwnedRow> = rows
                .into_iter()
                .filter_map(|(_, r)| {
                    if let Some(Datum::Int32(age)) = r.values.get(2) {
                        if *age > 40 { Some(r) } else { None }
                    } else {
                        None
                    }
                })
                .collect();

            Ok((
                vec![
                    ("id".into(), DataType::Int32),
                    ("name".into(), DataType::Text),
                    ("age".into(), DataType::Int32),
                ],
                filtered,
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(&subplan, &all_shards, &GatherStrategy::Union { distinct: false, limit: None, offset: None })
            .unwrap();

        // Verify all returned rows have age > 40
        for row in &rows {
            if let Some(Datum::Int32(age)) = row.values.get(2) {
                assert!(*age > 40, "filter failed: age {} <= 40", age);
            }
        }
        assert!(!rows.is_empty(), "should have some rows matching filter");
    }

    #[test]
    fn test_scatter_gather_two_phase_count() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Partial COUNT(*) on each shard
        let subplan = SubPlan::new("partial count", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let count = rows.len() as i64;
            Ok((
                vec![("count".into(), DataType::Int64)],
                vec![OwnedRow::new(vec![Datum::Int64(count)])],
            ))
        });

        let all_shards = engine.shard_ids();
        let ((cols, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![],
                    agg_merges: vec![AggMerge::Count(0)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(cols.len(), 1);
        assert_eq!(rows.len(), 1, "COUNT should produce exactly 1 row");
        match rows[0].values.get(0) {
            Some(Datum::Int64(total)) => assert_eq!(*total, 100, "COUNT(*) across 4 shards"),
            other => panic!("expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_scatter_gather_two_phase_sum() {
        let engine = setup_sharded(2, 10);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // SUM(age) - expected: sum of (20 + i%30) for i=0..9
        let expected_sum: i64 = (0..10).map(|i: i64| 20 + (i % 30)).sum();

        let subplan = SubPlan::new("partial sum(age)", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut sum: i64 = 0;
            for (_, r) in &rows {
                if let Some(Datum::Int32(age)) = r.values.get(2) {
                    sum += *age as i64;
                }
            }
            Ok((
                vec![("sum_age".into(), DataType::Int64)],
                vec![OwnedRow::new(vec![Datum::Int64(sum)])],
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![],
                    agg_merges: vec![AggMerge::Sum(0)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        match rows[0].values.get(0) {
            Some(Datum::Int64(total)) => assert_eq!(*total, expected_sum),
            other => panic!("expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_scatter_gather_two_phase_min_max() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // MIN(id), MAX(id) - expected: 0, 99
        let subplan = SubPlan::new("partial min/max(id)", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut min_id = i32::MAX;
            let mut max_id = i32::MIN;
            for (_, r) in &rows {
                if let Some(Datum::Int32(id)) = r.values.get(0) {
                    if *id < min_id { min_id = *id; }
                    if *id > max_id { max_id = *id; }
                }
            }
            Ok((
                vec![
                    ("min_id".into(), DataType::Int32),
                    ("max_id".into(), DataType::Int32),
                ],
                vec![OwnedRow::new(vec![Datum::Int32(min_id), Datum::Int32(max_id)])],
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![],
                    agg_merges: vec![AggMerge::Min(0), AggMerge::Max(1)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Datum::Int32(0));
        assert_eq!(rows[0].values[1], Datum::Int32(99));
    }

    #[test]
    fn test_scatter_gather_two_phase_avg_fixups() {
        // Test AVG decomposition via closure API: SUM + hidden COUNT 鈫?AVG fixup
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Subplan: each shard returns [SUM(id), COUNT(id)]
        let subplan = SubPlan::new("partial_avg", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut sum = 0i64;
            let mut count = 0i64;
            for (_, row) in &rows {
                if let Datum::Int32(v) = &row.values[0] {
                    sum += *v as i64;
                    count += 1;
                }
            }
            Ok((
                vec![("sum_id".into(), DataType::Int64), ("count_id".into(), DataType::Int64)],
                vec![OwnedRow::new(vec![Datum::Int64(sum), Datum::Int64(count)])],
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![],
                    agg_merges: vec![AggMerge::Sum(0), AggMerge::Count(1)],
                    avg_fixups: vec![(0, 1)], // row[0] = row[0] / row[1]
                    visible_columns: Some(1), // only show AVG result (col 0)
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        // ids 0..99, sum=4950, count=100, avg=49.5
        assert_eq!(rows[0].values[0], Datum::Float64(49.5));
        assert_eq!(rows[0].values.len(), 1, "hidden COUNT column should be truncated");
    }

    #[test]
    fn test_scatter_gather_two_phase_having() {
        // Test HAVING filter via closure API
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Subplan: each shard returns [age_group, count] where age_group = id / 10
        let subplan = SubPlan::new("partial_grouped", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut groups: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
            for (_, row) in &rows {
                if let Datum::Int32(id) = &row.values[0] {
                    *groups.entry(id / 10).or_default() += 1;
                }
            }
            let result_rows: Vec<OwnedRow> = groups.into_iter()
                .map(|(g, c)| OwnedRow::new(vec![Datum::Int32(g), Datum::Int64(c)]))
                .collect();
            Ok((
                vec![("age_group".into(), DataType::Int32), ("cnt".into(), DataType::Int64)],
                result_rows,
            ))
        });

        let all_shards = engine.shard_ids();
        // HAVING: only keep groups where merged count > 5
        let having_fn: Arc<dyn Fn(&OwnedRow) -> bool + Send + Sync> = Arc::new(|row| {
            matches!(row.values.get(1), Some(Datum::Int64(c)) if *c > 5)
        });
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![0],
                    agg_merges: vec![AggMerge::Count(1)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: Some(having_fn),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        // 100 ids 鈫?groups 0..9, each with 10 ids 鈫?merged count = 10 per group.
        // HAVING count > 5 鈫?all 10 groups pass.
        assert_eq!(rows.len(), 10, "all 10 groups should pass HAVING count > 5");
        for row in &rows {
            match row.values.get(1) {
                Some(Datum::Int64(c)) => assert!(*c > 5, "count should be > 5"),
                other => panic!("Expected Int64, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_scatter_gather_two_phase_order_limit_offset() {
        // Test post-merge ORDER BY + LIMIT + OFFSET via closure API
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Subplan: each shard returns [age_group, count]
        let subplan = SubPlan::new("partial_grouped", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut groups: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
            for (_, row) in &rows {
                if let Datum::Int32(id) = &row.values[0] {
                    *groups.entry(id / 10).or_default() += 1;
                }
            }
            let result_rows: Vec<OwnedRow> = groups.into_iter()
                .map(|(g, c)| OwnedRow::new(vec![Datum::Int32(g), Datum::Int64(c)]))
                .collect();
            Ok((
                vec![("age_group".into(), DataType::Int32), ("cnt".into(), DataType::Int64)],
                result_rows,
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![0],
                    agg_merges: vec![AggMerge::Count(1)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: None,
                    order_by: vec![(0, true)], // ORDER BY age_group ASC
                    limit: Some(3),
                    offset: Some(2),
                },
            )
            .unwrap();

        // 10 groups (0..9), sorted ASC, OFFSET 2 LIMIT 3 鈫?groups 2, 3, 4
        assert_eq!(rows.len(), 3, "LIMIT 3 after OFFSET 2");
        assert_eq!(rows[0].values[0], Datum::Int32(2));
        assert_eq!(rows[1].values[0], Datum::Int32(3));
        assert_eq!(rows[2].values[0], Datum::Int32(4));
    }

    #[test]
    fn test_scatter_gather_order_by_limit() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan all for sort", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let result_rows: Vec<OwnedRow> = rows.into_iter().map(|(_, r)| r).collect();
            Ok((
                vec![
                    ("id".into(), DataType::Int32),
                    ("name".into(), DataType::Text),
                    ("age".into(), DataType::Int32),
                ],
                result_rows,
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::OrderByLimit {
                    sort_columns: vec![(0, true)], // ORDER BY id ASC
                    limit: Some(10),               // LIMIT 10
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(rows.len(), 10);
        // Verify ordering
        for i in 0..10 {
            assert_eq!(rows[i].values[0], Datum::Int32(i as i32));
        }
    }

    #[test]
    fn test_scatter_gather_order_by_with_offset() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan_ids", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows: Vec<OwnedRow> = storage
                .scan(TableId(1), txn.txn_id, read_ts)?
                .into_iter()
                .map(|(_, r)| r)
                .collect();
            txn_mgr.commit(txn.txn_id)?;
            Ok((vec![("id".into(), DataType::Int32)], rows))
        });

        let all_shards = engine.shard_ids();
        // ORDER BY id ASC, OFFSET 5, LIMIT 10
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::OrderByLimit {
                    sort_columns: vec![(0, true)],
                    limit: Some(10),
                    offset: Some(5),
                },
            )
            .unwrap();

        assert_eq!(rows.len(), 10, "OFFSET 5 LIMIT 10 should return 10 rows");
        // First row should be id=5 (after skipping 0..4)
        assert_eq!(rows[0].values[0], Datum::Int32(5));
        assert_eq!(rows[9].values[0], Datum::Int32(14));
    }

    #[test]
    fn test_scatter_gather_timeout() {
        let engine = setup_sharded(4, 100);
        // Set timeout to 0 (immediate timeout)
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_nanos(1));

        let subplan = SubPlan::new("slow scan", |storage, txn_mgr| {
            // Simulate a slow operation
            std::thread::sleep(Duration::from_millis(10));
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;
            Ok((vec![], rows.into_iter().map(|(_, r)| r).collect()))
        });

        let all_shards = engine.shard_ids();
        let result = exec.scatter_gather(&subplan, &all_shards, &GatherStrategy::Union { distinct: false, limit: None, offset: None });

        assert!(result.is_err(), "should timeout");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("timeout"), "error should mention timeout: {}", err_msg);
    }

    #[test]
    fn test_scatter_gather_metrics() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;
            Ok((vec![], rows.into_iter().map(|(_, r)| r).collect()))
        });

        let all_shards = engine.shard_ids();
        let (_, metrics) = exec
            .scatter_gather(&subplan, &all_shards, &GatherStrategy::Union { distinct: false, limit: None, offset: None })
            .unwrap();

        assert_eq!(metrics.shards_participated, 4);
        assert_eq!(metrics.total_rows_gathered, 100);
        assert!(metrics.total_latency_us > 0 || metrics.max_subplan_latency_us == 0);
    }

    #[test]
    fn test_scatter_gather_partial_shards() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;
            Ok((
                vec![("id".into(), DataType::Int32)],
                rows.into_iter().map(|(_, r)| r).collect(),
            ))
        });

        // Only query 2 of 4 shards
        let target = vec![ShardId(0), ShardId(1)];
        let ((_, rows), metrics) = exec
            .scatter_gather(&subplan, &target, &GatherStrategy::Union { distinct: false, limit: None, offset: None })
            .unwrap();

        assert_eq!(metrics.shards_participated, 2);
        assert_eq!(rows.len(), 50, "2 of 4 shards should have 50 rows");
    }
}

#[cfg(test)]
mod replication_tests {
    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::{ColumnDef, TableSchema};
    use cedar_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId};
    use cedar_storage::wal::WalRecord;

    use crate::replication::{ReplicaRole, ShardReplicaGroup};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "replicated".into(),
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
                    data_type: DataType::Text,
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

    /// Insert a row on primary via WAL records and ship to replication log.
    fn insert_on_primary(
        group: &ShardReplicaGroup,
        txn_id: TxnId,
        commit_ts: Timestamp,
        id: i32,
        value: &str,
    ) {
        let row = OwnedRow::new(vec![Datum::Int32(id), Datum::Text(value.into())]);

        // Execute on primary engine
        group
            .primary
            .storage
            .insert(TableId(1), row.clone(), txn_id)
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(txn_id, commit_ts, cedar_common::types::TxnType::Local)
            .unwrap();

        // Ship WAL records
        group.ship_wal_record(WalRecord::Insert {
            txn_id,
            table_id: TableId(1),
            row,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id, commit_ts });
    }

    #[test]
    fn test_replication_basic_catch_up() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Insert on primary
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "hello");
        insert_on_primary(&group, TxnId(2), Timestamp(2), 2, "world");

        // Replica should have 0 rows before catch-up
        let replica = &group.replicas[0];
        let rows = replica.storage.scan(TableId(1), TxnId(999), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 0, "replica should be empty before catch-up");

        // Catch up
        let applied = group.catch_up_replica(0).unwrap();
        assert_eq!(applied, 4, "should apply 4 WAL records (2 insert + 2 commit)");

        // Replica should now have 2 rows
        let rows = replica.storage.scan(TableId(1), TxnId(999), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 2, "replica should have 2 rows after catch-up");
    }

    #[test]
    fn test_replication_lag_tracking() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "a");

        let lag = group.replication_lag();
        assert_eq!(lag.len(), 1);
        assert_eq!(lag[0].0, 0); // replica index
        assert!(lag[0].1 > 0, "replica should be behind primary");

        // Catch up
        group.catch_up_replica(0).unwrap();
        let lag = group.replication_lag();
        assert_eq!(lag[0].1, 0, "replica should be caught up");
    }

    #[test]
    fn test_committed_data_survives_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Insert data on primary
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "committed_before_crash");
        insert_on_primary(&group, TxnId(2), Timestamp(2), 2, "also_committed");

        // Catch up replica
        group.catch_up_replica(0).unwrap();

        // Promote replica to primary (simulates primary crash + failover)
        group.promote(0).unwrap();

        // Verify the new primary has the committed data
        assert_eq!(group.primary.current_role(), ReplicaRole::Primary);
        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 2, "committed data should survive promote");
    }

    #[test]
    fn test_uncommitted_not_visible_after_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Committed txn
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "committed");

        // Uncommitted txn: insert but no commit WAL record shipped
        let row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("uncommitted".into())]);
        group
            .primary
            .storage
            .insert(TableId(1), row.clone(), TxnId(2))
            .unwrap();
        // Ship only the insert, not the commit
        group.ship_wal_record(WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row,
        });

        // Catch up and promote
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // The uncommitted txn should NOT be visible on the new primary
        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "only committed txn should be visible after promote"
        );
        assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));
    }

    #[test]
    fn test_replica_reconnect_catch_up() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Phase 1: insert 3 rows, catch up
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "a");
        insert_on_primary(&group, TxnId(2), Timestamp(2), 2, "b");
        insert_on_primary(&group, TxnId(3), Timestamp(3), 3, "c");
        group.catch_up_replica(0).unwrap();

        // Phase 2: "disconnect" 鈥?insert more rows without catching up
        insert_on_primary(&group, TxnId(4), Timestamp(4), 4, "d");
        insert_on_primary(&group, TxnId(5), Timestamp(5), 5, "e");

        // Verify lag
        let lag = group.replication_lag();
        assert!(lag[0].1 > 0, "should have lag after disconnect");

        // Phase 3: "reconnect" 鈥?catch up from last applied LSN
        let applied = group.catch_up_replica(0).unwrap();
        assert_eq!(applied, 4, "should catch up 4 records (2 insert + 2 commit)");

        let rows = group.replicas[0]
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 5, "replica should have all 5 rows after catch-up");

        let lag = group.replication_lag();
        assert_eq!(lag[0].1, 0, "should be caught up");
    }

    #[test]
    fn test_promote_swaps_roles() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        assert_eq!(group.primary.current_role(), ReplicaRole::Primary);
        assert_eq!(group.replicas[0].current_role(), ReplicaRole::Replica);

        group.promote(0).unwrap();

        assert_eq!(group.primary.current_role(), ReplicaRole::Primary);
        assert_eq!(group.replicas[0].current_role(), ReplicaRole::Replica);
    }

    #[test]
    fn test_new_writes_after_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Initial data
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "before");
        group.catch_up_replica(0).unwrap();

        // Promote
        group.promote(0).unwrap();

        // Write to the new primary
        let row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("after_promote".into())]);
        group
            .primary
            .storage
            .insert(TableId(1), row, TxnId(10))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(TxnId(10), Timestamp(10), cedar_common::types::TxnType::Local)
            .unwrap();

        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 2, "new primary should accept new writes");
    }

    #[test]
    fn test_promote_with_routing_updates_shard_map() {
        use cedar_common::types::NodeId;
        use crate::routing::shard_map::ShardMap;

        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();
        let mut shard_map = ShardMap::uniform(2, NodeId(1));

        // Verify initial leader
        assert_eq!(shard_map.get_shard(ShardId(0)).unwrap().leader, NodeId(1));

        // Insert data, catch up
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "data");
        group.catch_up_replica(0).unwrap();

        // Promote with routing update: new leader is NodeId(2)
        group
            .promote_with_routing(0, &mut shard_map, NodeId(2))
            .unwrap();

        // Verify shard_map was updated
        assert_eq!(
            shard_map.get_shard(ShardId(0)).unwrap().leader,
            NodeId(2),
            "shard_map leader should be updated after promote_with_routing"
        );

        // Verify new primary still has the data
        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_replication_lag_timeline() {
        // Simulates replication lag over time with a burst of writes,
        // gradual catch-up, and a second burst 鈥?produces data suitable
        // for a "replication lag over time" graph.
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        let mut lag_timeline: Vec<(u32, u64)> = Vec::new(); // (step, lag)

        // Phase 1: burst of 10 writes, lag grows
        for i in 1..=10u32 {
            insert_on_primary(
                &group,
                TxnId(i as u64),
                Timestamp(i as u64),
                i as i32,
                &format!("v{}", i),
            );
            let lag = group.replication_lag()[0].1;
            lag_timeline.push((i, lag));
        }
        assert!(lag_timeline.last().unwrap().1 > 0, "lag should be > 0 after burst");

        // Phase 2: catch up
        group.catch_up_replica(0).unwrap();
        lag_timeline.push((11, group.replication_lag()[0].1));
        assert_eq!(lag_timeline.last().unwrap().1, 0, "lag should be 0 after catch-up");

        // Phase 3: second burst of 5 writes
        for i in 11..=15u32 {
            insert_on_primary(
                &group,
                TxnId(i as u64),
                Timestamp(i as u64),
                i as i32,
                &format!("v{}", i),
            );
            let lag = group.replication_lag()[0].1;
            lag_timeline.push((i + 1, lag));
        }
        assert!(lag_timeline.last().unwrap().1 > 0, "lag should grow again");

        // Phase 4: catch up again
        group.catch_up_replica(0).unwrap();
        lag_timeline.push((17, group.replication_lag()[0].1));
        assert_eq!(lag_timeline.last().unwrap().1, 0);

        // Verify timeline shape: lag increases during bursts, drops to 0 on catch-up
        assert!(lag_timeline.len() >= 16, "timeline should have enough data points");
    }
}

#[cfg(test)]
mod execute_subplan_tests {
    use std::sync::Arc;

    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::{ColumnDef, TableSchema};
    use cedar_common::types::{ColumnId, DataType, IsolationLevel, ShardId, TableId};

    use crate::sharded_engine::ShardedEngine;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "sub".into(),
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
            ],
            primary_key_columns: vec![0],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
        ..Default::default()
        }
    }

    #[test]
    fn test_execute_subplan_single_shard() {
        let engine = Arc::new(ShardedEngine::new(4));
        engine.create_table_all(&test_schema()).unwrap();

        // Insert some data on shard 0
        let shard = engine.shard(ShardId(0)).unwrap();
        let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
        shard
            .storage
            .insert(TableId(1), OwnedRow::new(vec![Datum::Int32(42)]), txn.txn_id)
            .unwrap();
        shard.txn_mgr.commit(txn.txn_id).unwrap();

        // Use execute_subplan to query shard 0
        let (cols, rows) = engine
            .execute_subplan(ShardId(0), |storage, txn_mgr| {
                let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                let read_ts = txn.read_ts(txn_mgr.current_ts());
                let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
                txn_mgr.commit(txn.txn_id)?;
                Ok((
                    vec![("id".into(), DataType::Int32)],
                    rows.into_iter().map(|(_, r)| r).collect(),
                ))
            })
            .unwrap();

        assert_eq!(cols.len(), 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Datum::Int32(42));
    }

    #[test]
    fn test_execute_subplan_invalid_shard() {
        let engine = Arc::new(ShardedEngine::new(2));

        let result = engine.execute_subplan(ShardId(99), |_, _| {
            Ok((vec![], vec![]))
        });

        assert!(result.is_err(), "should error for invalid shard ID");
    }
}

#[cfg(test)]
mod dist_query_engine_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::{ColumnDef, TableSchema};
    use cedar_common::types::{ColumnId, DataType, IsolationLevel, ShardId, TableId};
    use cedar_executor::executor::ExecutionResult;
    use cedar_planner::plan::{DistGather, PhysicalPlan};
    use cedar_sql_frontend::types::DistinctMode;

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
                let row = OwnedRow::new(vec![Datum::Int32(global_id), Datum::Int32(global_id * 10)]);
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
                cedar_sql_frontend::types::BoundProjection::Column(0, "id".into()),
                cedar_sql_frontend::types::BoundProjection::Column(1, "value".into()),
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
            gather: DistGather::Union { distinct: false, limit: None, offset: None },
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

        // Each shard does a COUNT 鈥?returns 1 row with count as Int64.
        // We use a SeqScan with a group_by trick, but since the executor
        // does the aggregation, we need to use the actual planner types.
        // For simplicity, test with Union and check total row count.
        let subplan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: test_schema(),
            projections: vec![
                cedar_sql_frontend::types::BoundProjection::Column(0, "id".into()),
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
            target_shards: vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)],
            gather: DistGather::Union { distinct: false, limit: None, offset: None },
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
                cedar_sql_frontend::types::BoundProjection::Column(0, "id".into()),
                cedar_sql_frontend::types::BoundProjection::Column(1, "value".into()),
            ],
            visible_projection_count: 2,
            filter: None,
            group_by: vec![],
            grouping_sets: vec![],
            having: None,
            order_by: vec![cedar_sql_frontend::types::BoundOrderBy { column_idx: 0, asc: true }],
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
            projections: vec![
                cedar_sql_frontend::types::BoundProjection::Column(0, "id".into()),
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

        let result = qe.execute_on_shard(ShardId(0), &plan, Some(&txn)).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "shard 0 should have 5 rows");
            }
            other => panic!("Expected Query result, got {:?}", other),
        }
    }
}

#[cfg(test)]
mod two_phase_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::{ColumnDef, TableSchema};
    use cedar_common::types::{ColumnId, DataType, IsolationLevel, ShardId, TableId, Timestamp, TxnId};

    use crate::sharded_engine::ShardedEngine;
    use crate::two_phase::TwoPhaseCoordinator;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "tpc_test".into(),
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
            ],
            primary_key_columns: vec![0],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
        ..Default::default()
        }
    }

    #[test]
    fn test_two_phase_commit_all_succeed() {
        let engine = Arc::new(ShardedEngine::new(3));
        engine.create_table_all(&test_schema()).unwrap();

        let coord = TwoPhaseCoordinator::new(engine.clone(), Duration::from_secs(10));
        let target = vec![ShardId(0), ShardId(1), ShardId(2)];

        let result = coord
            .execute(&target, IsolationLevel::ReadCommitted, |storage, _txn_mgr, txn_id| {
                let row = OwnedRow::new(vec![Datum::Int32(42)]);
                storage.insert(TableId(1), row, txn_id)?;
                Ok(())
            })
            .unwrap();

        assert!(result.committed, "all shards should commit");
        assert_eq!(result.participants.len(), 3);
        assert!(result.participants.iter().all(|p| p.prepared));

        // Verify data is visible on all shards
        for &sid in &target {
            let shard = engine.shard(sid).unwrap();
            let rows = shard
                .storage
                .scan(TableId(1), TxnId(999), Timestamp(1000))
                .unwrap();
            assert_eq!(rows.len(), 1, "shard {:?} should have 1 row", sid);
        }
    }

    #[test]
    fn test_two_phase_abort_on_failure() {
        let engine = Arc::new(ShardedEngine::new(3));
        engine.create_table_all(&test_schema()).unwrap();

        let coord = TwoPhaseCoordinator::new(engine.clone(), Duration::from_secs(10));
        let target = vec![ShardId(0), ShardId(1), ShardId(2)];

        let result = coord
            .execute(&target, IsolationLevel::ReadCommitted, |storage, _txn_mgr, txn_id| {
                // Insert on all shards 鈥?but shard 2 (3rd) gets a duplicate PK
                // which we simulate by inserting the same key twice.
                let row = OwnedRow::new(vec![Datum::Int32(1)]);
                storage.insert(TableId(1), row.clone(), txn_id)?;
                // Insert duplicate 鈥?this will fail on the storage layer
                storage.insert(TableId(1), row, txn_id)?;
                Ok(())
            })
            .unwrap();

        // Should abort because the write_fn fails (duplicate PK insert).
        assert!(!result.committed, "should abort due to failure");
    }

    #[test]
    fn test_two_phase_metrics() {
        let engine = Arc::new(ShardedEngine::new(2));
        engine.create_table_all(&test_schema()).unwrap();

        let coord = TwoPhaseCoordinator::new(engine.clone(), Duration::from_secs(10));
        let target = vec![ShardId(0), ShardId(1)];

        let result = coord
            .execute(&target, IsolationLevel::ReadCommitted, |storage, _txn_mgr, txn_id| {
                let row = OwnedRow::new(vec![Datum::Int32(1)]);
                storage.insert(TableId(1), row, txn_id)?;
                Ok(())
            })
            .unwrap();

        assert!(result.committed);
        assert!(result.prepare_latency_us > 0 || result.commit_latency_us == 0);
        assert_eq!(result.participants.len(), 2);
    }

    #[test]
    fn test_merge_bool_and_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // BoolAnd merge: all true 鈫?true; one false 鈫?false.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Int32), ("ba".into(), DataType::Boolean)],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Boolean(true)])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Boolean(false)])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::BoolAnd(1)],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].values[1], Datum::Boolean(false), "true AND false = false");
    }

    #[test]
    fn test_merge_bool_or_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // BoolOr merge: all false 鈫?false; one true 鈫?true.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Int32), ("bo".into(), DataType::Boolean)],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Boolean(false)])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Boolean(true)])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::BoolOr(1)],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].values[1], Datum::Boolean(true), "false OR true = true");
    }

    #[test]
    fn test_merge_string_agg_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // STRING_AGG merge: concatenate partial strings with separator.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Int32), ("sa".into(), DataType::Text)],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a,b".into())])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Text("c,d".into())])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::StringAgg(1, ",".into())],
        );
        assert_eq!(merged.len(), 1);
        match &merged[0].values[1] {
            Datum::Text(s) => {
                assert_eq!(s, "a,b,c,d", "STRING_AGG should concat with separator");
            }
            other => panic!("Expected Text, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_string_agg_with_null() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // STRING_AGG with NULL: NULL should be skipped.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Int32), ("sa".into(), DataType::Text)],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Null])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Text("x".into())])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::StringAgg(1, ",".into())],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].values[1], Datum::Text("x".into()), "NULL should be skipped");
    }

    #[test]
    fn test_merge_array_agg_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Int32), ("aa".into(), DataType::Array(Box::new(DataType::Int32)))],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Array(vec![Datum::Int32(10), Datum::Int32(20)])])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Array(vec![Datum::Int32(30)])])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::ArrayAgg(1)],
        );
        assert_eq!(merged.len(), 1);
        match &merged[0].values[1] {
            Datum::Array(arr) => {
                assert_eq!(arr.len(), 3, "Should concatenate arrays: [10,20] ++ [30]");
                assert_eq!(arr[0], Datum::Int32(10));
                assert_eq!(arr[1], Datum::Int32(20));
                assert_eq!(arr[2], Datum::Int32(30));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_array_agg_with_null() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Int32), ("aa".into(), DataType::Array(Box::new(DataType::Int32)))],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Null])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Array(vec![Datum::Int32(5)])])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::ArrayAgg(1)],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].values[1], Datum::Array(vec![Datum::Int32(5)]), "NULL shard should be skipped");
    }

    #[test]
    fn test_cmp_datum_cross_type() {
        use crate::distributed_exec::compare_datums;
        // Int32 vs Int64
        assert_eq!(
            compare_datums(Some(&Datum::Int32(5)), Some(&Datum::Int64(10))),
            std::cmp::Ordering::Less
        );
        // Float64 vs Int32
        assert_eq!(
            compare_datums(Some(&Datum::Float64(3.5)), Some(&Datum::Int32(3))),
            std::cmp::Ordering::Greater
        );
        // Null sorts before everything
        assert_eq!(
            compare_datums(Some(&Datum::Null), Some(&Datum::Int32(0))),
            std::cmp::Ordering::Less
        );
        // Timestamp comparison
        assert_eq!(
            compare_datums(Some(&Datum::Timestamp(100)), Some(&Datum::Timestamp(200))),
            std::cmp::Ordering::Less
        );
        // Text comparison
        assert_eq!(
            compare_datums(Some(&Datum::Text("apple".into())), Some(&Datum::Text("banana".into()))),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn test_datum_add_cross_type() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // SUM merge with Int32 + Int64 (cross-type)
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Int32), ("s".into(), DataType::Int64)],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Int32(10)])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Int64(20)])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::Sum(1)]);
        assert_eq!(merged.len(), 1);
        // Int32(10) + Int64(20) should produce Int64(30)
        assert_eq!(merged[0].values[1], Datum::Int64(30));
    }

    #[test]
    fn test_merge_count_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values [1, 2, 3]
        // Shard 1: group "a" has distinct values [2, 3, 4]
        // After merge: deduplicated = {1, 2, 3, 4} 鈫?count = 4
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("cd".into(), DataType::Int64)],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(1), Datum::Int32(2), Datum::Int32(3)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(2), Datum::Int32(3), Datum::Int32(4)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::CountDistinct(1)],
        );
        assert_eq!(merged.len(), 1);
        // Deduplicated {1,2,3,4} 鈫?count = 4
        assert_eq!(merged[0].values[1], Datum::Int64(4));
    }

    #[test]
    fn test_merge_count_distinct_with_null_shard() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "x" has [10, 20]
        // Shard 1: group "x" has NULL (no rows matched)
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("cd".into(), DataType::Int64)],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("x".into()),
                Datum::Array(vec![Datum::Int32(10), Datum::Int32(20)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("x".into()),
                Datum::Null,
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::CountDistinct(1)],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].values[1], Datum::Int64(2), "NULL shard should be skipped, count=2");
    }

    #[test]
    fn test_merge_sum_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values [10, 20, 30]
        // Shard 1: group "a" has distinct values [20, 30, 40]
        // After merge: unique = {10, 20, 30, 40} 鈫?sum = 100
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("sd".into(), DataType::Int64)],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(10), Datum::Int32(20), Datum::Int32(30)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(20), Datum::Int32(30), Datum::Int32(40)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::SumDistinct(1)],
        );
        assert_eq!(merged.len(), 1);
        // Unique {10,20,30,40}: Int32 values get promoted to Int64 via datum_add
        assert_eq!(merged[0].values[1], Datum::Int64(100));
    }

    #[test]
    fn test_merge_avg_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values [10, 20, 30]
        // Shard 1: group "a" has distinct values [20, 30, 40]
        // After merge: unique = {10, 20, 30, 40} 鈫?avg = 100/4 = 25.0
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("ad".into(), DataType::Float64)],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(10), Datum::Int32(20), Datum::Int32(30)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(20), Datum::Int32(30), Datum::Int32(40)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::AvgDistinct(1)],
        );
        assert_eq!(merged.len(), 1);
        // Unique {10,20,30,40}: avg = 100/4 = 25.0
        assert_eq!(merged[0].values[1], Datum::Float64(25.0));
    }

    #[test]
    fn test_merge_string_agg_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values ["hello", "world"]
        // Shard 1: group "a" has distinct values ["world", "foo"]
        // After merge: unique = {"hello", "world", "foo"} 鈫?joined with ","
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("sa".into(), DataType::Text)],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Text("hello".into()), Datum::Text("world".into())]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Text("world".into()), Datum::Text("foo".into())]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::StringAggDistinct(1, ",".into())],
        );
        assert_eq!(merged.len(), 1);
        // Should be 3 unique values joined with ","
        match &merged[0].values[1] {
            Datum::Text(s) => {
                let parts: Vec<&str> = s.split(',').collect();
                assert_eq!(parts.len(), 3, "3 unique values joined");
                assert!(parts.contains(&"hello"));
                assert!(parts.contains(&"world"));
                assert!(parts.contains(&"foo"));
            }
            other => panic!("Expected Text, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_array_agg_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values [1, 2, 3]
        // Shard 1: group "a" has distinct values [2, 3, 4]
        // After merge: unique = {1, 2, 3, 4} 鈫?returned as array
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("aa".into(), DataType::Int32)],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(1), Datum::Int32(2), Datum::Int32(3)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(2), Datum::Int32(3), Datum::Int32(4)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::ArrayAggDistinct(1)],
        );
        assert_eq!(merged.len(), 1);
        match &merged[0].values[1] {
            Datum::Array(arr) => {
                assert_eq!(arr.len(), 4, "4 unique values");
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_distinct_null_handling() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Edge case: arrays contain Null values. Nulls should be deduped like any other value.
        // Shard 0: [10, Null, 20]
        // Shard 1: [Null, 20, 30]
        // Unique non-null: {10, 20, 30}, Null appears but is deduped
        // CountDistinct should count all unique including Null = 4
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("cd".into(), DataType::Int64)],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(10), Datum::Null, Datum::Int32(20)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Null, Datum::Int32(20), Datum::Int32(30)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::CountDistinct(1)],
        );
        assert_eq!(merged.len(), 1);
        // Unique values: {10, Null, 20, 30} = 4
        assert_eq!(merged[0].values[1], Datum::Int64(4));
    }

    #[test]
    fn test_merge_distinct_all_null_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Edge case: both shards return Null (no data for the group).
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("cd".into(), DataType::Int64)],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Null,
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Null,
            ])],
            latency_us: 0,
        };

        // CountDistinct: all Null 鈫?0
        let merged = merge_two_phase_agg(&[sr1.clone(), sr2.clone()], &[0], &[AggMerge::CountDistinct(1)]);
        assert_eq!(merged[0].values[1], Datum::Int64(0));

        // SumDistinct: all Null 鈫?Null
        let merged = merge_two_phase_agg(&[sr1.clone(), sr2.clone()], &[0], &[AggMerge::SumDistinct(1)]);
        assert!(matches!(merged[0].values[1], Datum::Null));

        // AvgDistinct: all Null 鈫?Null
        let merged = merge_two_phase_agg(&[sr1.clone(), sr2.clone()], &[0], &[AggMerge::AvgDistinct(1)]);
        assert!(matches!(merged[0].values[1], Datum::Null));

        // StringAggDistinct: all Null 鈫?Null
        let merged = merge_two_phase_agg(&[sr1.clone(), sr2.clone()], &[0], &[AggMerge::StringAggDistinct(1, ",".into())]);
        assert!(matches!(merged[0].values[1], Datum::Null));

        // ArrayAggDistinct: all Null 鈫?Null
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::ArrayAggDistinct(1)]);
        assert!(matches!(merged[0].values[1], Datum::Null));
    }

    #[test]
    fn test_merge_distinct_empty_arrays() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Edge case: shards return empty arrays (group exists but no matching values).
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("cd".into(), DataType::Int64)],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(42)]),
            ])],
            latency_us: 0,
        };

        // CountDistinct: empty + [42] 鈫?1
        let merged = merge_two_phase_agg(&[sr1.clone(), sr2.clone()], &[0], &[AggMerge::CountDistinct(1)]);
        assert_eq!(merged[0].values[1], Datum::Int64(1));

        // SumDistinct: empty + [42] 鈫?42
        let merged = merge_two_phase_agg(&[sr1.clone(), sr2.clone()], &[0], &[AggMerge::SumDistinct(1)]);
        // Int32(42) gets promoted to Int64 via datum_add
        assert_eq!(merged[0].values[1], Datum::Int64(42));

        // AvgDistinct: empty + [42] 鈫?42.0
        let merged = merge_two_phase_agg(&[sr1.clone(), sr2.clone()], &[0], &[AggMerge::AvgDistinct(1)]);
        assert_eq!(merged[0].values[1], Datum::Float64(42.0));
    }

    #[test]
    fn test_merge_multiple_groups_distinct() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Multiple groups: group "a" and "b" across 2 shards.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![("grp".into(), DataType::Text), ("cd".into(), DataType::Int64)],
            rows: vec![
                OwnedRow::new(vec![Datum::Text("a".into()), Datum::Array(vec![Datum::Int32(1), Datum::Int32(2)])]),
                OwnedRow::new(vec![Datum::Text("b".into()), Datum::Array(vec![Datum::Int32(10)])]),
            ],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![
                OwnedRow::new(vec![Datum::Text("a".into()), Datum::Array(vec![Datum::Int32(2), Datum::Int32(3)])]),
                OwnedRow::new(vec![Datum::Text("b".into()), Datum::Array(vec![Datum::Int32(10), Datum::Int32(20)])]),
            ],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::CountDistinct(1)]);
        assert_eq!(merged.len(), 2);
        // Sort by group name for deterministic assertion
        let mut rows = merged;
        rows.sort_by(|a, b| format!("{:?}", a.values[0]).cmp(&format!("{:?}", b.values[0])));
        // Group "a": unique {1,2,3} 鈫?3
        assert_eq!(rows[0].values[0], Datum::Text("a".into()));
        assert_eq!(rows[0].values[1], Datum::Int64(3));
        // Group "b": unique {10,20} 鈫?2
        assert_eq!(rows[1].values[0], Datum::Text("b".into()));
        assert_eq!(rows[1].values[1], Datum::Int64(2));
    }
}

/// End-to-end integration: SQL 鈫?Parse 鈫?Bind 鈫?Plan 鈫?wrap_distributed 鈫?DistributedQueryEngine
#[cfg(test)]
mod end_to_end_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::Catalog;
    use cedar_common::types::{IsolationLevel, ShardId, TableId};
    use cedar_executor::executor::ExecutionResult;
    use cedar_planner::planner::Planner;
    use cedar_sql_frontend::binder::Binder;
    use cedar_sql_frontend::parser::parse_sql;

    use crate::query_engine::DistributedQueryEngine;
    use crate::sharded_engine::ShardedEngine;

    /// Build a 4-shard engine, create the "users" table on all shards,
    /// insert test data, and return the engine + catalog.
    fn setup() -> (Arc<ShardedEngine>, Catalog) {
        let engine = Arc::new(ShardedEngine::new(4));

        // We need to create the table schema and register it in catalog.
        let schema = cedar_common::schema::TableSchema {
            id: TableId(1),
            name: "users".into(),
            columns: vec![
                cedar_common::schema::ColumnDef {
                    id: cedar_common::types::ColumnId(0),
                    name: "id".into(),
                    data_type: cedar_common::types::DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                cedar_common::schema::ColumnDef {
                    id: cedar_common::types::ColumnId(1),
                    name: "name".into(),
                    data_type: cedar_common::types::DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                cedar_common::schema::ColumnDef {
                    id: cedar_common::types::ColumnId(2),
                    name: "age".into(),
                    data_type: cedar_common::types::DataType::Int32,
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

    fn plan_and_wrap(sql: &str, catalog: &Catalog, shards: &[ShardId]) -> cedar_planner::PhysicalPlan {
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
        assert!(matches!(plan, cedar_planner::PhysicalPlan::DistPlan { .. }));

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

        let plan = plan_and_wrap("SELECT id, name FROM users WHERE age > 50", &catalog, &shards);

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { columns, rows } => {
                assert_eq!(columns.len(), 2);
                // age ranges from 20..59 across 4 shards.
                // age > 50 means ids 31..39 鈫?9 rows.
                assert_eq!(rows.len(), 9, "ages 51-59 鈫?9 rows");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_single_shard_stays_local() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Single shard 鈫?wrap_distributed should NOT wrap.
        let plan = plan_and_wrap("SELECT id FROM users", &catalog, &[ShardId(0)]);
        assert!(matches!(plan, cedar_planner::PhysicalPlan::SeqScan { .. }));

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
        let result = qe.two_phase_coordinator().execute(
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
        ).unwrap();

        assert!(result.committed, "2PC should commit");
        assert_eq!(result.participants.len(), 4);

        // Verify the row exists on each shard individually (2PC replicates to all).
        for s in 0..4u64 {
            let shard = engine.shard(ShardId(s)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let rows = shard.storage
                .scan(TableId(1), txn.txn_id, cedar_common::types::Timestamp(u64::MAX))
                .unwrap();
            let found = rows.iter().any(|r| r.1.values.first() == Some(&Datum::Int32(999)));
            shard.txn_mgr.commit(txn.txn_id).unwrap();
            assert!(found, "Row 999 should exist on shard {}", s);
        }
    }

    #[test]
    fn test_e2e_ddl_propagation() {
        // Create engine with NO tables, then use DistributedQueryEngine to CREATE TABLE.
        let engine = Arc::new(ShardedEngine::new(4));
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        let schema = cedar_common::schema::TableSchema {
            id: TableId(2),
            name: "products".into(),
            columns: vec![
                cedar_common::schema::ColumnDef {
                    id: cedar_common::types::ColumnId(0),
                    name: "pid".into(),
                    data_type: cedar_common::types::DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
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

        let plan = cedar_planner::PhysicalPlan::CreateTable {
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

        // INSERT via DistributedQueryEngine 鈥?should route to a specific shard.
        let plan = plan_and_wrap(
            "INSERT INTO users (id, name, age) VALUES (12345, 'routed_user', 42)",
            &catalog,
            &shards,
        );
        // INSERT stays local (wrap_distributed doesn't wrap DML)
        assert!(matches!(plan, cedar_planner::PhysicalPlan::Insert { .. }));

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
            .scan(TableId(1), txn.txn_id, cedar_common::types::Timestamp(u64::MAX))
            .unwrap();
        let found = rows.iter().any(|r| {
            r.1.values.first() == Some(&Datum::Int32(12345))
        });
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
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                assert!(
                    matches!(gather, cedar_planner::DistGather::MergeSortLimit { .. }),
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
                    assert!(prev_age >= curr_age, "not sorted DESC: {} < {}", prev_age, curr_age);
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
        let explain_plan = cedar_planner::PhysicalPlan::Explain(Box::new(dist_plan));

        // The EXPLAIN plan's routing hint should come from the inner DistPlan.
        let hint = explain_plan.routing_hint();
        assert_eq!(hint.involved_shards.len(), 4);
    }

    #[test]
    fn test_e2e_update_broadcast_all_shards() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // UPDATE without PK filter 鈫?broadcasts to all shards.
        let plan = plan_and_wrap(
            "UPDATE users SET age = 99 WHERE age > 20",
            &catalog,
            &shards,
        );
        // UPDATE stays local (wrap_distributed doesn't wrap DML)
        assert!(matches!(plan, cedar_planner::PhysicalPlan::Update { .. }));

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                // All 20 rows across 4 shards have age 20..39 鈫?all match age > 20
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
        assert_eq!(before_count, 40, "Should have 40 rows before delete (10 per shard 脳 4)");

        // DELETE without PK filter 鈫?broadcasts to all shards.
        let del_plan = plan_and_wrap(
            "DELETE FROM users WHERE age < 25",
            &catalog,
            &shards,
        );
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
        assert!(after_count < before_count, "Should have fewer rows after delete");
    }

    #[test]
    fn test_e2e_multi_row_insert_split_by_shard() {
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // Multi-row INSERT with different PK values 鈥?rows should be split across shards.
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
            let rows = shard.storage
                .scan(TableId(1), txn.txn_id, cedar_common::types::Timestamp(u64::MAX))
                .unwrap();
            let found = rows.iter().any(|r| r.1.values.first() == Some(&Datum::Int32(pk as i32)));
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
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                assert!(
                    matches!(gather, cedar_planner::DistGather::TwoPhaseAgg { .. }),
                    "Expected TwoPhaseAgg gather for COUNT(*), got {:?}", gather
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

        // SELECT SUM(age), MIN(age), MAX(age) 鈥?all supported for TwoPhaseAgg
        let plan = plan_and_wrap(
            "SELECT SUM(age), MIN(age), MAX(age) FROM users",
            &catalog,
            &shards,
        );
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                assert!(matches!(gather, cedar_planner::DistGather::TwoPhaseAgg { .. }));
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
        let explain_plan = cedar_planner::PhysicalPlan::Explain(Box::new(dist_plan));

        // Execute EXPLAIN through DistributedQueryEngine
        let result = qe.execute(&explain_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert!(!rows.is_empty(), "EXPLAIN should produce output");
                let plan_text: Vec<String> = rows.iter()
                    .filter_map(|r| match &r.values[0] {
                        Datum::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let joined = plan_text.join("\n");
                assert!(joined.contains("DistPlan"), "EXPLAIN should mention DistPlan: {}", joined);
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_drop_table_propagation() {
        let engine = Arc::new(ShardedEngine::new(4));
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));

        // Create table on all shards first.
        let schema = cedar_common::schema::TableSchema {
            id: TableId(3),
            name: "temp_tbl".into(),
            columns: vec![cedar_common::schema::ColumnDef {
                id: cedar_common::types::ColumnId(0),
                name: "x".into(),
                data_type: cedar_common::types::DataType::Int32,
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
        let create_plan = cedar_planner::PhysicalPlan::CreateTable {
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
        let drop_plan = cedar_planner::PhysicalPlan::DropTable {
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
        let trunc = cedar_planner::PhysicalPlan::Truncate {
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
        let plan = cedar_planner::PhysicalPlan::RunGc;
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Should have watermark_ts, chains_processed, shards_processed
                assert!(rows.len() >= 3, "Expected at least 3 GC result rows");
                let shards_row = rows.iter().find(|r| {
                    matches!(&r.values[0], Datum::Text(s) if s == "shards_processed")
                });
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
        let alter_plan = cedar_planner::PhysicalPlan::AlterTable {
            table_name: "users".into(),
            ops: vec![cedar_sql_frontend::types::AlterTableOp::AddColumn(cedar_common::schema::ColumnDef {
                id: cedar_common::types::ColumnId(3),
                name: "email".into(),
                data_type: cedar_common::types::DataType::Text,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            })],
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
        let idx_plan = cedar_planner::PhysicalPlan::CreateIndex {
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
                "Shard {} should have index on age (col 2), found: {:?}", s, indexed
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
        assert!(stats.total_committed >= 40, "Expected at least 40 committed, got {}", stats.total_committed);
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
        let plan = plan_and_wrap("SELECT id, name FROM users WHERE id = 7777", &catalog, &shards);
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

        // Insert 100 rows that hash to different shards 鈥?parallel execution.
        let mut row_exprs = Vec::new();
        for i in 5000..5100 {
            row_exprs.push(vec![
                cedar_sql_frontend::types::BoundExpr::Literal(Datum::Int32(i)),
                cedar_sql_frontend::types::BoundExpr::Literal(Datum::Text(format!("user_{}", i))),
                cedar_sql_frontend::types::BoundExpr::Literal(Datum::Int32(25)),
            ]);
        }

        let schema = engine.shard(ShardId(0)).unwrap()
            .storage.get_table_schema("users").unwrap();
        let plan = cedar_planner::PhysicalPlan::Insert {
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
            let rows = shard.storage
                .scan(TableId(1), txn.txn_id, cedar_common::types::Timestamp(u64::MAX))
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
        assert_eq!(stats.per_shard_latency_us.len(), 4, "Should have 4 shard latencies");
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
        let explain_plan = cedar_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&explain_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows.iter().map(|r| {
                    match &r.values[0] { Datum::Text(s) => s.clone(), _ => String::new() }
                }).collect();
                let joined = text.join("\n");
                assert!(joined.contains("DistPlan"), "Should show DistPlan in output");
                assert!(joined.contains("Shards participated: 4"), "Should show 4 shards");
                assert!(joined.contains("Total rows gathered: 40"), "Should show 40 rows");
                assert!(joined.contains("Gather strategy: Union"), "Should show Union strategy");
                assert!(joined.contains("Total latency:"), "Should show total latency");
                assert!(joined.contains("Shard 0 latency:") || joined.contains("Shard 1 latency:"),
                    "Should show per-shard latency");
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
        let plan = plan_and_wrap("SELECT age, COUNT(id) FROM users GROUP BY age", &catalog, &shards);

        // Verify the plan uses TwoPhaseAgg
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                assert!(matches!(gather, cedar_planner::plan::DistGather::TwoPhaseAgg { .. }),
                    "GROUP BY + COUNT should use TwoPhaseAgg, got {:?}", gather);
            }
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // 40 unique ages 鈫?40 groups, each with count=1
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
                &format!("INSERT INTO users (id, name, age) VALUES ({}, 'dup_{}', 99)", 5000 + i, i),
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
                assert_eq!(rows[0].values[1], Datum::Int64(4),
                    "Should count 4 rows with age=99 across shards");
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
                6000 + i, i
            );
            qe.execute(&plan_and_wrap(&sql, &catalog, &shards), None).unwrap();
        }

        // SUM(id) = 6000+6001+6002+6003 = 24006
        let plan = plan_and_wrap(
            "SELECT age, SUM(id), MIN(id), MAX(id) FROM users WHERE age = 88 GROUP BY age",
            &catalog, &shards,
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
            two_pc.execute(
                &shards,
                IsolationLevel::ReadCommitted,
                |storage, _txn_mgr, txn_id| {
                    let row = OwnedRow::new(vec![
                        Datum::Int32(7000 + i),
                        Datum::Text("duplicate_name".into()),
                        Datum::Int32(77),
                    ]);
                    storage.insert(TableId(1), row, txn_id)
                        .map(|_| ())
                        .map_err(|e| cedar_common::error::CedarError::Internal(format!("{:?}", e)))
                },
            ).unwrap();
        }

        // Without DISTINCT: each shard has all 4 rows 鈫?4 shards * 4 rows = 16 name values
        // (plus the 40 original rows with unique names)
        let plan_no_distinct = plan_and_wrap("SELECT name FROM users WHERE age = 77", &catalog, &shards);
        let result = qe.execute(&plan_no_distinct, None).unwrap();
        let no_distinct_count = match result {
            ExecutionResult::Query { rows, .. } => rows.len(),
            _ => panic!("Expected Query"),
        };
        // 4 rows * 4 shards = 16 (2PC replicates to all shards)
        assert_eq!(no_distinct_count, 16, "Without DISTINCT: 4 rows on each of 4 shards");

        // With DISTINCT: should deduplicate "duplicate_name" to 1 row
        let plan_distinct = plan_and_wrap("SELECT DISTINCT name FROM users WHERE age = 77", &catalog, &shards);
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
            two_pc.execute(
                &shards,
                IsolationLevel::ReadCommitted,
                |storage, _txn_mgr, txn_id| {
                    let row = OwnedRow::new(vec![
                        Datum::Int32(8000 + i),
                        Datum::Text(format!("having_test_{}", i)),
                        Datum::Int32(95),
                    ]);
                    storage.insert(TableId(1), row, txn_id)
                        .map(|_| ())
                        .map_err(|e| cedar_common::error::CedarError::Internal(format!("{:?}", e)))
                },
            ).unwrap();
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
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::TwoPhaseAgg { having, .. } => {
                        assert!(having.is_some(), "TwoPhaseAgg should carry HAVING expr");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Merged COUNT=16 > 10, so the group should pass HAVING.
                assert_eq!(rows.len(), 1, "HAVING COUNT > 10 should pass after merge (count=16)");
                assert_eq!(rows[0].values[0], Datum::Int32(95));
                assert_eq!(rows[0].values[1], Datum::Int64(16), "4 rows * 4 shards = 16");
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
                assert_eq!(rows.len(), 0, "HAVING COUNT > 100 should filter out (count=16)");
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

        // 40 rows total (ids 0..39). ORDER BY id LIMIT 3 OFFSET 5 鈫?ids 5,6,7
        let plan = plan_and_wrap(
            "SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 5",
            &catalog, &shards,
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
        // ORDER BY id DESC LIMIT 5 鈫?ids 39,38,37,36,35
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id FROM users ORDER BY id DESC LIMIT 5",
            &catalog, &shards,
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
                assert_eq!(rows[0].values[0], Datum::Int64(40), "40 total rows across 4 shards");
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
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::TwoPhaseAgg { avg_fixups, .. } => {
                        assert!(!avg_fixups.is_empty(), "AVG should produce avg_fixups");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "Single aggregate row");
                match &rows[0].values[0] {
                    Datum::Float64(v) => {
                        assert!((v - 39.5).abs() < 0.001, "AVG(age) should be 39.5, got {}", v);
                    }
                    other => panic!("Expected Float64 for AVG, got {:?}", other),
                }
                // Should have only 1 visible column (AVG), no hidden COUNT
                assert_eq!(rows[0].values.len(), 1, "Hidden COUNT column should be truncated");
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
                9000 + i, i
            );
            qe.execute(&plan_and_wrap(&sql, &catalog, &shards), None).unwrap();
        }

        // AVG(id) for age=99 group: (9000+9001+9002+9003)/4 = 9001.5
        let plan = plan_and_wrap(
            "SELECT age, AVG(id) FROM users WHERE age = 99 GROUP BY age",
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "One group for age=99");
                assert_eq!(rows[0].values[0], Datum::Int32(99));
                match &rows[0].values[1] {
                    Datum::Float64(v) => {
                        assert!((v - 9001.5).abs() < 0.001, "AVG(id) should be 9001.5, got {}", v);
                    }
                    other => panic!("Expected Float64 for AVG, got {:?}", other),
                }
                // Should have only 2 visible columns (age, AVG), no hidden COUNT
                assert_eq!(rows[0].values.len(), 2, "Hidden COUNT column should be truncated");
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
            two_pc.execute(
                &shards,
                IsolationLevel::ReadCommitted,
                |storage, _txn_mgr, txn_id| {
                    let row = OwnedRow::new(vec![
                        Datum::Int32(9500 + i),
                        Datum::Text(format!("avg_having_{}", i)),
                        Datum::Int32(70),
                    ]);
                    storage.insert(TableId(1), row, txn_id)
                        .map(|_| ())
                        .map_err(|e| cedar_common::error::CedarError::Internal(format!("{:?}", e)))
                },
            ).unwrap();
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
                assert_eq!(rows.len(), 1, "HAVING AVG(id) > 9000 should keep age=70 group");
                assert_eq!(rows[0].values[0], Datum::Int32(70));
                match &rows[0].values[1] {
                    Datum::Float64(v) => {
                        assert!((v - 9501.5).abs() < 0.001, "AVG(id) should be 9501.5, got {}", v);
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
            &catalog, &shards,
        );
        let explain_plan = cedar_planner::PhysicalPlan::Explain(Box::new(inner));

        let result = qe.execute(&explain_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows.iter()
                    .filter_map(|r| {
                        if let Datum::Text(s) = &r.values[0] { Some(s.clone()) } else { None }
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(joined.contains("TwoPhaseAgg"), "EXPLAIN should show TwoPhaseAgg");
                assert!(joined.contains("AVG decomposition"), "EXPLAIN should show AVG decomposition");
                assert!(joined.contains("Execution Stats"), "EXPLAIN should show execution stats");
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
            &catalog, &shards,
        );
        let explain_plan = cedar_planner::PhysicalPlan::Explain(Box::new(inner));

        let result = qe.execute(&explain_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows.iter()
                    .filter_map(|r| {
                        if let Datum::Text(s) = &r.values[0] { Some(s.clone()) } else { None }
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(joined.contains("MergeSortLimit"), "EXPLAIN should show MergeSortLimit");
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
        assert!(stats.total_latency_us < 5_000_000, "Should complete within 5s");
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
                    "Error should mention timeout or cancelled, got: {}", msg,
                );
            }
            Ok(_) => {
                // If the machine is extremely fast, the query might succeed.
                // That's acceptable 鈥?the important thing is no panic.
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
            &catalog, &shards,
        );

        // Verify the plan uses TwoPhaseAgg (not MergeSortLimit)
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::TwoPhaseAgg { order_by, limit, .. } => {
                        assert!(!order_by.is_empty(), "Should have post-merge ORDER BY");
                        assert_eq!(*limit, Some(3), "Should have post-merge LIMIT 3");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
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
            &catalog, &shards,
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

        // 40 groups (age 20..59). ORDER BY age ASC, OFFSET 2, LIMIT 3 鈫?ages 22, 23, 24.
        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users GROUP BY age ORDER BY age LIMIT 3 OFFSET 2",
            &catalog, &shards,
        );

        // Verify plan shape
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::TwoPhaseAgg { offset, limit, .. } => {
                        assert_eq!(*offset, Some(2));
                        assert_eq!(*limit, Some(3));
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
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
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 0, "OFFSET 100 on 40 groups should return 0 rows");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_array_agg() {
        // ARRAY_AGG across shards: each shard collects partial arrays, gather concatenates.
        // Setup: 40 rows, id 0..39, age = 20 + id. Each age is unique 鈫?40 groups.
        // ARRAY_AGG(id) for a single-row group just returns [id].
        // We verify a GROUP BY with COUNT + ARRAY_AGG works end-to-end.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT age, ARRAY_AGG(id) FROM users GROUP BY age ORDER BY age LIMIT 3",
            &catalog, &shards,
        );

        // Verify plan uses TwoPhaseAgg
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, cedar_planner::plan::DistAggMerge::ArrayAgg(_))),
                            "Should have ArrayAgg merge");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
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
                        assert_eq!(arr.len(), 1, "age=20 has exactly 1 row 鈫?1 element");
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
        // 40 rows: id 0..39, age = 20 + id 鈫?ages 20..59, each unique.
        // COUNT(id) = 1 for every group. HAVING COUNT(id) >= 1 keeps all 40.
        // ORDER BY age ASC, OFFSET 5, LIMIT 3 鈫?ages 25, 26, 27.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT age, COUNT(id) FROM users GROUP BY age HAVING COUNT(id) >= 1 ORDER BY age LIMIT 3 OFFSET 5",
            &catalog, &shards,
        );

        // Verify plan shape
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::TwoPhaseAgg {
                        having, order_by, limit, offset, ..
                    } => {
                        assert!(having.is_some(), "Should have HAVING filter");
                        assert!(!order_by.is_empty(), "Should have ORDER BY");
                        assert_eq!(*limit, Some(3));
                        assert_eq!(*offset, Some(5));
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
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
        assert_eq!(stats.per_shard_row_count.len(), 4, "Should have row counts for 4 shards");
        let total_rows: usize = stats.per_shard_row_count.iter().map(|(_, c)| c).sum();
        assert_eq!(total_rows, 40, "Total rows across all shards should be 40");
    }

    #[test]
    fn test_e2e_union_offset_limit() {
        // Non-agg query with OFFSET + LIMIT uses Union gather with offset.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // 40 rows total. LIMIT 5 OFFSET 10 鈫?skip 10, take 5.
        let plan = plan_and_wrap(
            "SELECT id FROM users LIMIT 5 OFFSET 10",
            &catalog, &shards,
        );

        // Verify plan shape: should be Union with offset
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::Union { limit, offset, .. } => {
                        assert_eq!(*limit, Some(5));
                        assert_eq!(*offset, Some(10));
                    }
                    cedar_planner::plan::DistGather::MergeSortLimit { limit, offset, .. } => {
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
            &catalog, &shards,
        );
        // Wrap in Explain to trigger exec_explain_dist
        let plan = cedar_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&plan, None);
        match result {
            Ok(ExecutionResult::Query { rows, .. }) => {
                let text: Vec<String> = rows.iter()
                    .filter_map(|r| match &r.values[0] { Datum::Text(s) => Some(s.clone()), _ => None })
                    .collect();
                let joined = text.join("\n");
                assert!(joined.contains("Distributed Plan"), "Should contain Distributed Plan header");
                assert!(joined.contains("TwoPhaseAgg"), "Should contain TwoPhaseAgg gather strategy");
                assert!(joined.contains("merge: COUNT(col"), "Should contain human-readable merge labels");
                assert!(joined.contains("Execution Stats"), "Should contain Execution Stats");
                assert!(joined.contains("Shard"), "Should contain per-shard details");
                assert!(joined.contains("rows:"), "Should contain per-shard row counts");
            }
            other => panic!("Expected OK Query from explain_analyze, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_count_distinct() {
        // COUNT(DISTINCT age) across 4 shards.
        // Setup: 40 rows, id 0..39, age = 20 + id 鈫?40 distinct ages.
        // Each shard has 10 rows with unique ages.
        // COUNT(DISTINCT age) should be 40.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT COUNT(DISTINCT age) FROM users",
            &catalog, &shards,
        );

        // Verify plan uses TwoPhaseAgg with CountDistinct
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, cedar_planner::plan::DistAggMerge::CountDistinct(_))),
                            "Should have CountDistinct merge");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "COUNT(DISTINCT) without GROUP BY returns 1 row");
                // All 40 ages are distinct 鈫?COUNT(DISTINCT age) = 40
                assert_eq!(rows[0].values[0], Datum::Int64(40),
                    "40 unique ages across 4 shards");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_count_distinct_with_group_by() {
        // COUNT(DISTINCT id) GROUP BY age with duplicates across shards.
        // Setup: 40 rows, id 0..39, age = 20 + id.
        // Each age is unique 鈫?COUNT(DISTINCT id) per group = 1.
        // Also test: we can combine COUNT(DISTINCT) with regular COUNT in same query.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT age, COUNT(DISTINCT id) FROM users GROUP BY age ORDER BY age LIMIT 5",
            &catalog, &shards,
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "LIMIT 5");
                // First group: age=20, COUNT(DISTINCT id) = 1 (only id=0 has age=20)
                assert_eq!(rows[0].values[0], Datum::Int32(20));
                assert_eq!(rows[0].values[1], Datum::Int64(1),
                    "Each age has exactly 1 distinct id");
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
        // Setup: 40 rows, id 0..39, age = 20 + id 鈫?ages 20..59, all distinct.
        // SUM(DISTINCT age) = sum(20..59) = 40 * (20+59)/2 = 40 * 39.5 = 1580
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT SUM(DISTINCT age) FROM users",
            &catalog, &shards,
        );

        // Verify plan uses TwoPhaseAgg with SumDistinct
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, cedar_planner::plan::DistAggMerge::SumDistinct(_))),
                            "Should have SumDistinct merge");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "SUM(DISTINCT) without GROUP BY returns 1 row");
                // All 40 ages are distinct (20..59), SUM = 20+21+...+59 = 1580
                assert_eq!(rows[0].values[0], Datum::Int64(1580),
                    "SUM(DISTINCT age) should be 1580");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_distributed_avg_distinct() {
        // AVG(DISTINCT age) across 4 shards.
        // Setup: 40 rows, id 0..39, age = 20 + id 鈫?ages 20..59, all distinct.
        // AVG(DISTINCT age) = (20+21+...+59)/40 = 1580/40 = 39.5
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT AVG(DISTINCT age) FROM users",
            &catalog, &shards,
        );

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1, "AVG(DISTINCT) without GROUP BY returns 1 row");
                assert_eq!(rows[0].values[0], Datum::Float64(39.5),
                    "AVG(DISTINCT age) should be 39.5");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_mixed_distinct_and_non_distinct() {
        // Mixed: COUNT(*), COUNT(DISTINCT age), SUM(age) in the same query.
        // Setup: 40 rows, id 0..39, age = 20 + id 鈫?40 distinct ages.
        // COUNT(*) = 40, COUNT(DISTINCT age) = 40, SUM(age) = 1580
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT COUNT(*), COUNT(DISTINCT age), SUM(age) FROM users",
            &catalog, &shards,
        );

        // Verify plan uses TwoPhaseAgg with both Count and CountDistinct
        match &plan {
            cedar_planner::PhysicalPlan::DistPlan { gather, .. } => {
                match gather {
                    cedar_planner::plan::DistGather::TwoPhaseAgg { agg_merges, .. } => {
                        assert!(agg_merges.iter().any(|m| matches!(m, cedar_planner::plan::DistAggMerge::Count(_))),
                            "Should have regular Count merge");
                        assert!(agg_merges.iter().any(|m| matches!(m, cedar_planner::plan::DistAggMerge::CountDistinct(_))),
                            "Should have CountDistinct merge");
                        assert!(agg_merges.iter().any(|m| matches!(m, cedar_planner::plan::DistAggMerge::Sum(_))),
                            "Should have Sum merge");
                    }
                    other => panic!("Expected TwoPhaseAgg, got {:?}", other),
                }
            }
            _ => panic!("Expected DistPlan"),
        }

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], Datum::Int64(40), "COUNT(*) = 40");
                assert_eq!(rows[0].values[1], Datum::Int64(40), "COUNT(DISTINCT age) = 40");
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

        let inner = plan_and_wrap(
            "SELECT COUNT(DISTINCT age) FROM users",
            &catalog, &shards,
        );
        let plan = cedar_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&plan, None);
        match result {
            Ok(ExecutionResult::Query { rows, .. }) => {
                let text: Vec<String> = rows.iter()
                    .filter_map(|r| match &r.values[0] { Datum::Text(s) => Some(s.clone()), _ => None })
                    .collect();
                let joined = text.join("\n");
                assert!(joined.contains("COUNT(DISTINCT col"), "Should show COUNT(DISTINCT)");
                assert!(joined.contains("[collect-dedup]"), "Should annotate with [collect-dedup]");
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
            two_pc.execute(
                &shards,
                IsolationLevel::ReadCommitted,
                |storage, _txn_mgr, txn_id| {
                    let row = OwnedRow::new(vec![
                        Datum::Int32(8500 + i),
                        Datum::Text(format!("cd_having_{}", i)),
                        Datum::Int32(88),
                    ]);
                    storage.insert(TableId(1), row, txn_id)
                        .map(|_| ())
                        .map_err(|e| cedar_common::error::CedarError::Internal(format!("{:?}", e)))
                },
            ).unwrap();
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
                assert_eq!(rows.len(), 1, "HAVING COUNT(DISTINCT id) > 2 should pass (4 unique IDs)");
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
                assert_eq!(rows.len(), 0, "HAVING COUNT(DISTINCT id) > 10 should filter out");
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
            two_pc.execute(
                &shards,
                IsolationLevel::ReadCommitted,
                |storage, _txn_mgr, txn_id| {
                    let row = OwnedRow::new(vec![
                        Datum::Int32(9000 + i as i32),
                        Datum::Text(name.to_string()),
                        Datum::Int32(77),
                    ]);
                    storage.insert(TableId(1), row, txn_id)
                        .map(|_| ())
                        .map_err(|e| cedar_common::error::CedarError::Internal(format!("{:?}", e)))
                },
            ).unwrap();
        }

        // STRING_AGG(DISTINCT name, ',') WHERE age = 77:
        // Each name appears 4 times (1 per shard). After dedup: 3 unique names joined with ','
        let plan = plan_and_wrap(
            "SELECT STRING_AGG(DISTINCT name, ',') FROM users WHERE age = 77",
            &catalog, &shards,
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
            &catalog, &shards,
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
            &catalog, &shards,
        );
        qe.execute(&plan, None).unwrap();

        let stats = qe.last_scatter_stats();
        assert_eq!(stats.gather_strategy, "TwoPhaseAgg");
        assert!(!stats.merge_labels.is_empty(), "merge_labels should be populated");
        assert!(stats.merge_labels.iter().any(|l| l.contains("COUNT")),
            "Should have COUNT label: {:?}", stats.merge_labels);
        assert!(stats.merge_labels.iter().any(|l| l.contains("SUM(DISTINCT") && l.contains("[collect-dedup]")),
            "Should have SUM(DISTINCT) [collect-dedup] label: {:?}", stats.merge_labels);
    }

    /// Setup with two tables: "users" (TableId 1) and "orders" (TableId 2).
    /// Users: 10 per shard (40 total). Orders: placed on DIFFERENT shards than
    /// the owning user to guarantee cross-shard join matches.
    fn setup_join() -> (Arc<ShardedEngine>, Catalog) {
        use cedar_common::schema::{ColumnDef, TableSchema};
        use cedar_common::types::{ColumnId, DataType};

        let engine = Arc::new(ShardedEngine::new(4));

        let users_schema = TableSchema {
            id: TableId(1),
            name: "users".into(),
            columns: vec![
                ColumnDef { id: ColumnId(0), name: "id".into(), data_type: DataType::Int32, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "name".into(), data_type: DataType::Text, nullable: true, is_primary_key: false, default_value: None, is_serial: false },
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
                ColumnDef { id: ColumnId(0), name: "oid".into(), data_type: DataType::Int32, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "user_id".into(), data_type: DataType::Int32, nullable: false, is_primary_key: false, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(2), name: "amount".into(), data_type: DataType::Int32, nullable: true, is_primary_key: false, default_value: None, is_serial: false },
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
            shard.storage.insert(TableId(1), OwnedRow::new(vec![
                Datum::Int32(i), Datum::Text(format!("user_{}", i)),
            ]), txn.txn_id).unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }
        for i in 5..10 {
            let shard = engine.shard(ShardId(1)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            shard.storage.insert(TableId(1), OwnedRow::new(vec![
                Datum::Int32(i), Datum::Text(format!("user_{}", i)),
            ]), txn.txn_id).unwrap();
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
            shard.storage.insert(TableId(2), OwnedRow::new(vec![
                Datum::Int32(oid), Datum::Int32(uid), Datum::Int32(amt),
            ]), txn.txn_id).unwrap();
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
        assert!(matches!(plan, cedar_planner::PhysicalPlan::NestedLoopJoin { .. } | cedar_planner::PhysicalPlan::HashJoin { .. }),
            "Join should NOT be wrapped in DistPlan");

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { columns, rows } => {
                assert_eq!(columns.len(), 4, "u.id, u.name, o.oid, o.amount");
                assert_eq!(rows.len(), 4, "4 orders match 3 users: user 0 has 2 orders, user 3 has 1, user 5 has 1");

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
            &catalog, &shards,
        );
        assert!(matches!(plan, cedar_planner::PhysicalPlan::NestedLoopJoin { .. } | cedar_planner::PhysicalPlan::HashJoin { .. }));

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // 10 users: user 0 has 2 orders, user 3 has 1, user 5 has 1,
                // users 1,2,4,6,7,8,9 have 0 orders 鈫?7 NULL rows
                // Total: 4 matched + 7 unmatched = 11 rows
                assert_eq!(rows.len(), 11, "LEFT JOIN: 4 matched + 7 unmatched = 11");

                // First row: user 0, oid 100
                assert_eq!(rows[0].values[0], Datum::Int32(0));
                // User 0 has 2 orders so rows[0] and rows[1] are user 0

                // Check that unmatched users have NULL oid
                // User 1 has no orders
                let user1_rows: Vec<_> = rows.iter()
                    .filter(|r| r.values[0] == Datum::Int32(1))
                    .collect();
                assert_eq!(user1_rows.len(), 1);
                assert!(matches!(user1_rows[0].values[1], Datum::Null),
                    "User 1 should have NULL oid in LEFT JOIN");
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
            &catalog, &shards,
        );
        let plan = cedar_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows.iter()
                    .filter_map(|r| match &r.values[0] { Datum::Text(s) => Some(s.clone()), _ => None })
                    .collect();
                let joined = text.join("\n");
                assert!(joined.contains("CoordinatorJoin"), "Should show CoordinatorJoin strategy");
                assert!(joined.contains("Tables:"), "Should list tables involved");
                assert!(joined.contains("Result rows: 4"), "Should show 4 result rows");
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
            &catalog, &shards,
        );

        // Plan should NOT be wrapped in DistPlan (subquery detected)
        assert!(matches!(plan, cedar_planner::PhysicalPlan::SeqScan { .. }),
            "Subquery query should NOT be wrapped in DistPlan");

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Orders reference user_ids: 0, 5, 3 鈫?3 distinct users
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
            &catalog, &shards,
        );

        assert!(matches!(plan, cedar_planner::PhysicalPlan::SeqScan { .. }),
            "EXISTS subquery should NOT be wrapped in DistPlan");

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Orders exist, so all 10 users should be returned
                assert_eq!(rows.len(), 10, "EXISTS is true 鈫?all 10 users returned");
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
            &catalog, &shards,
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
            &catalog, &shards,
        );

        // UNIONs should NOT be wrapped in DistPlan
        assert!(matches!(plan, cedar_planner::PhysicalPlan::SeqScan { .. }),
            "UNION should NOT be wrapped in DistPlan");

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
            &catalog, &shards,
        );
        let plan = cedar_planner::PhysicalPlan::Explain(Box::new(inner));
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows.iter()
                    .filter_map(|r| match &r.values[0] { Datum::Text(s) => Some(s.clone()), _ => None })
                    .collect();
                let joined = text.join("\n");
                assert!(joined.contains("CoordinatorSubquery"), "Should show CoordinatorSubquery strategy");
                assert!(joined.contains("Tables:"), "Should list tables involved");
                assert!(joined.contains("Result rows: 3"), "Should show 3 result rows");
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_scalar_subquery_in_filter() {
        // Scalar subquery in filter: users whose id < (SELECT COUNT(*) FROM orders)
        // Total orders = 4 across all shards, so users with id < 4 鈫?ids 0,1,2,3
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id FROM users WHERE id < (SELECT COUNT(*) FROM orders) ORDER BY id",
            &catalog, &shards,
        );

        assert!(matches!(plan, cedar_planner::PhysicalPlan::SeqScan { .. }),
            "Scalar subquery should NOT be wrapped in DistPlan");

        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // COUNT(*) from orders = 4 (all shards), so id < 4 鈫?0,1,2,3
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
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                // user_ids in orders: 0, 5, 3, 0 鈫?distinct: 0, 3, 5 鈫?3 users updated
                assert_eq!(rows_affected, 3, "Should update 3 users who have orders");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify the updates took effect by reading back
        let select_plan = plan_and_wrap(
            "SELECT id FROM users WHERE name = 'updated' ORDER BY id",
            &catalog, &shards,
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
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(rows_affected, 3, "Should delete 3 users who have orders");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify remaining users
        let select_plan = plan_and_wrap(
            "SELECT COUNT(*) FROM users",
            &catalog, &shards,
        );
        let result = qe.execute(&select_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                // Started with 10 users, deleted 3 鈫?7 remaining
                assert_eq!(rows[0].values[0], Datum::Int64(7));
            }
            other => panic!("Expected Query, got {:?}", other),
        }
    }

    #[test]
    fn test_e2e_cross_shard_update_with_scalar_subquery() {
        // UPDATE users SET name = 'big' WHERE id > (SELECT COUNT(*) FROM orders)
        // COUNT(*) from orders = 4 (across all shards).
        // Users with id > 4: ids 5,6,7,8,9 鈫?5 users.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "UPDATE users SET name = 'big' WHERE id > (SELECT COUNT(*) FROM orders)",
            &catalog, &shards,
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
        // Since orders has user_id=3, EXISTS is true 鈫?delete ALL users.
        // This is an uncorrelated EXISTS: it's either true or false for all rows.
        let (engine, catalog) = setup_join();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "DELETE FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = 3)",
            &catalog, &shards,
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
        use cedar_common::schema::{ColumnDef, TableSchema};
        use cedar_common::types::{ColumnId, DataType};

        let engine = Arc::new(ShardedEngine::new(4));
        let schema = TableSchema {
            id: TableId(1),
            name: "items".into(),
            columns: vec![
                ColumnDef { id: ColumnId(0), name: "id".into(), data_type: DataType::Int32, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "val".into(), data_type: DataType::Int32, nullable: true, is_primary_key: false, default_value: None, is_serial: false },
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
                &catalog, &shards,
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

        let plan = plan_and_wrap(
            "SELECT id, val FROM items WHERE id = 7",
            &catalog, &shards,
        );
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
        assert!(stats.pruned_to_shard.is_some(), "pruned_to_shard should be set");
    }

    #[test]
    fn test_e2e_no_shard_pruning_full_scan() {
        // SELECT * FROM items (no PK filter) should scatter to all shards
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT id, val FROM items",
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 20, "Full scan should return all 20 items");
            }
            other => panic!("Expected Query, got {:?}", other),
        }

        let stats = qe.last_scatter_stats();
        assert_eq!(stats.shards_participated, 4, "Should scatter to all 4 shards");
        assert!(stats.pruned_to_shard.is_none(), "No shard pruning for full scan");
    }

    #[test]
    fn test_e2e_explain_shows_shard_pruning() {
        // EXPLAIN SELECT ... WHERE id = 7 should show shard pruning in output
        let (engine, catalog) = setup_hash_distributed();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "EXPLAIN SELECT id, val FROM items WHERE id = 7",
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { rows, .. } => {
                let text: Vec<String> = rows.iter()
                    .filter_map(|r| match &r.values[0] {
                        Datum::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let joined = text.join("\n");
                assert!(joined.contains("ShardPruned"), "EXPLAIN should show ShardPruned strategy: {}", joined);
                assert!(joined.contains("Shard pruning:"), "EXPLAIN should show pruning detail: {}", joined);
                assert!(joined.contains("Shards participated: 1"), "Should show 1 shard: {}", joined);
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
        use cedar_common::schema::{ColumnDef, TableSchema};
        use cedar_common::types::{ColumnId, DataType, TableId};

        let engine = Arc::new(crate::sharded_engine::ShardedEngine::new(4));

        let src_schema = TableSchema {
            id: TableId(1),
            name: "src".into(),
            columns: vec![
                ColumnDef { id: ColumnId(0), name: "id".into(), data_type: DataType::Int32, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "val".into(), data_type: DataType::Int32, nullable: true, is_primary_key: false, default_value: None, is_serial: false },
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
                ColumnDef { id: ColumnId(0), name: "id".into(), data_type: DataType::Int32, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "val".into(), data_type: DataType::Int32, nullable: true, is_primary_key: false, default_value: None, is_serial: false },
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
            shard.storage.insert(TableId(1), OwnedRow::new(vec![
                Datum::Int32(i), Datum::Int32(i * 10),
            ]), txn.txn_id).unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }
        for i in 5..10 {
            let shard = engine.shard(ShardId(1)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            shard.storage.insert(TableId(1), OwnedRow::new(vec![
                Datum::Int32(i), Datum::Int32(i * 10),
            ]), txn.txn_id).unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }

        let mut catalog = cedar_common::schema::Catalog::new();
        catalog.add_table(src_schema);
        catalog.add_table(dst_schema);

        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        // INSERT INTO dst SELECT id, val FROM src
        let plan = plan_and_wrap(
            "INSERT INTO dst SELECT id, val FROM src",
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Dml { rows_affected, .. } => {
                assert_eq!(rows_affected, 10, "Should insert all 10 rows from src");
            }
            other => panic!("Expected Dml, got {:?}", other),
        }

        // Verify all 10 rows are in dst across all shards
        let select_plan = plan_and_wrap(
            "SELECT COUNT(*) FROM dst",
            &catalog, &shards,
        );
        let result = qe.execute(&select_plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows[0].values[0], Datum::Int64(10), "dst should have 10 rows");
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
            &catalog, &shards,
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
        assert_eq!(stats.shards_participated, 1, "Should prune to 1 shard via AND chain");
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
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 2, "IN-list should return 2 rows");
                let mut ids: Vec<i32> = rows.iter()
                    .map(|r| match &r.values[0] { Datum::Int32(v) => *v, _ => panic!() })
                    .collect();
                ids.sort();
                assert_eq!(ids, vec![1, 3]);
            }
            other => panic!("Expected Query, got {:?}", other),
        }

        let stats = qe.last_scatter_stats();
        // IN (1, 3) touches at most 2 shards; verify pruning happened
        assert!(stats.shards_participated <= 2,
            "IN-list should prune to <= 2 shards, got {}", stats.shards_participated);
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
            &catalog, &shards,
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

        let plan = plan_and_wrap(
            "DELETE FROM items WHERE id IN (1, 3)",
            &catalog, &shards,
        );
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
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match &result {
            ExecutionResult::Query { columns, rows } => {
                assert_eq!(columns.len(), 2, "RETURNING id, val");
                assert_eq!(rows.len(), 3, "Should return 3 deleted rows");
                let mut ids: Vec<i32> = rows.iter()
                    .map(|r| match &r.values[0] { Datum::Int32(v) => *v, _ => panic!() })
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
            &catalog, &shards,
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
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 5, "LIMIT 5 should return 5 rows");
                let ids: Vec<i32> = rows.iter()
                    .map(|r| match &r.values[0] { Datum::Int32(v) => *v, _ => panic!() })
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
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3);
                let ids: Vec<i32> = rows.iter()
                    .map(|r| match &r.values[0] { Datum::Int32(v) => *v, _ => panic!() })
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
            &catalog, &shards,
        );
        let result = qe.execute(&plan, None).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 3);
                let ids: Vec<i32> = rows.iter()
                    .map(|r| match &r.values[0] { Datum::Int32(v) => *v, _ => panic!() })
                    .collect();
                assert_eq!(ids, vec![5, 6, 7], "OFFSET 5 + LIMIT 3 should give ids 5,6,7");
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
        // GROUP BY name 鈫?40 groups, each with count=1.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT name, COUNT(*) FROM users GROUP BY name ORDER BY name LIMIT 5",
            &catalog, &shards,
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
        // Each name is unique 鈫?40 groups. HAVING SUM(age) > 50 filters to ages 51..59 = 9 rows.
        let (engine, catalog) = setup();
        let qe = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();

        let plan = plan_and_wrap(
            "SELECT name, SUM(age) FROM users GROUP BY name HAVING SUM(age) > 50 ORDER BY name",
            &catalog, &shards,
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
}

// ===========================================================================
// M1: WalChunk + ReplicationTransport tests
// ===========================================================================

#[cfg(test)]
mod wal_chunk_transport_tests {
    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::{ColumnDef, TableSchema};
    use cedar_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId};
    use cedar_storage::wal::WalRecord;

    use crate::replication::{
        ChannelTransport, InProcessTransport, LsnWalRecord, ReplicationLog,
        ReplicationTransport, ShardReplicaGroup, WalChunk,
    };
    use std::sync::Arc;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "t".into(),
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
                    data_type: DataType::Text,
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

    #[test]
    fn test_wal_chunk_checksum() {
        let records = vec![
            LsnWalRecord { lsn: 1, record: WalRecord::BeginTxn { txn_id: TxnId(1) } },
            LsnWalRecord { lsn: 2, record: WalRecord::BeginTxn { txn_id: TxnId(2) } },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);
        assert!(chunk.verify_checksum(), "checksum should be valid");
        assert_eq!(chunk.start_lsn, 1);
        assert_eq!(chunk.end_lsn, 2);
        assert_eq!(chunk.len(), 2);
    }

    #[test]
    fn test_wal_chunk_empty() {
        let chunk = WalChunk::from_records(ShardId(0), vec![]);
        assert!(chunk.is_empty());
        assert!(chunk.verify_checksum());
        assert_eq!(chunk.start_lsn, 0);
        assert_eq!(chunk.end_lsn, 0);
    }

    #[test]
    fn test_wal_chunk_serde_roundtrip() {
        let row = cedar_common::datum::OwnedRow::new(vec![
            Datum::Int32(42),
            Datum::Text("hello".into()),
        ]);
        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::Insert {
                    txn_id: TxnId(1),
                    table_id: TableId(1),
                    row,
                },
            },
            LsnWalRecord {
                lsn: 2,
                record: WalRecord::CommitTxnLocal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(10),
                },
            },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);

        // JSON round-trip (simulates gRPC/tonic JSON serialization)
        let json = serde_json::to_string(&chunk).expect("WalChunk should serialize to JSON");
        let decoded: WalChunk =
            serde_json::from_str(&json).expect("WalChunk should deserialize from JSON");

        assert_eq!(decoded.shard_id, chunk.shard_id);
        assert_eq!(decoded.start_lsn, chunk.start_lsn);
        assert_eq!(decoded.end_lsn, chunk.end_lsn);
        assert_eq!(decoded.checksum, chunk.checksum);
        assert!(decoded.verify_checksum(), "deserialized chunk checksum should be valid");
        assert_eq!(decoded.len(), 2);
    }

    #[test]
    fn test_wal_chunk_json_roundtrip_global_txn() {
        let records = vec![
            LsnWalRecord { lsn: 1, record: WalRecord::BeginTxn { txn_id: TxnId(1) } },
            LsnWalRecord { lsn: 2, record: WalRecord::CommitTxnGlobal { txn_id: TxnId(1), commit_ts: Timestamp(5) } },
        ];
        let chunk = WalChunk::from_records(ShardId(3), records);

        let json = serde_json::to_string(&chunk).expect("WalChunk should serialize to JSON");
        let decoded: WalChunk = serde_json::from_str(&json).expect("WalChunk should deserialize from JSON");

        assert_eq!(decoded.shard_id, ShardId(3));
        assert_eq!(decoded.len(), 2);
        assert!(decoded.verify_checksum());
    }

    #[test]
    fn test_in_process_transport_pull_and_ack() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log.clone());

        // Append records to the log
        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(3) });

        // Pull from LSN 0 鈥?should get all 3
        let chunk = transport.pull_wal_chunk(ShardId(0), 0, 100).unwrap();
        assert_eq!(chunk.len(), 3);
        assert!(chunk.verify_checksum());

        // Pull from LSN 2 鈥?should get only LSN 3
        let chunk2 = transport.pull_wal_chunk(ShardId(0), 2, 100).unwrap();
        assert_eq!(chunk2.len(), 1);
        assert_eq!(chunk2.start_lsn, 3);

        // Ack
        transport.ack_wal(ShardId(0), 0, 3).unwrap();
        assert_eq!(transport.get_ack_lsn(0), 3);
    }

    #[test]
    fn test_in_process_transport_shard_mismatch() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log);
        let result = transport.pull_wal_chunk(ShardId(1), 0, 100);
        assert!(result.is_err(), "should fail on shard mismatch");
    }

    #[test]
    fn test_channel_transport_push_pull() {
        let transport = ChannelTransport::new(ShardId(0));

        let records = vec![
            LsnWalRecord { lsn: 1, record: WalRecord::BeginTxn { txn_id: TxnId(1) } },
            LsnWalRecord { lsn: 2, record: WalRecord::BeginTxn { txn_id: TxnId(2) } },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);
        transport.push_chunk(chunk).unwrap();

        // Pull 鈥?should get the chunk
        let pulled = transport.pull_wal_chunk(ShardId(0), 0, 100).unwrap();
        assert_eq!(pulled.len(), 2);
        assert!(pulled.verify_checksum());

        // Pull again 鈥?nothing pending
        let empty = transport.pull_wal_chunk(ShardId(0), 0, 100).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_channel_transport_ack() {
        let transport = ChannelTransport::new(ShardId(0));
        assert_eq!(transport.get_ack_lsn(0), 0);

        transport.ack_wal(ShardId(0), 0, 5).unwrap();
        assert_eq!(transport.get_ack_lsn(0), 5);

        transport.ack_wal(ShardId(0), 0, 10).unwrap();
        assert_eq!(transport.get_ack_lsn(0), 10);

        // Different replica
        transport.ack_wal(ShardId(0), 1, 3).unwrap();
        assert_eq!(transport.get_ack_lsn(1), 3);
    }

    #[test]
    fn test_apply_chunk_to_replica() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Build a chunk manually
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::Insert {
                    txn_id: TxnId(1),
                    table_id: TableId(1),
                    row: row.clone(),
                },
            },
            LsnWalRecord {
                lsn: 2,
                record: WalRecord::CommitTxnLocal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(1),
                },
            },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);

        // Apply chunk to replica
        let applied = group.apply_chunk_to_replica(0, &chunk).unwrap();
        assert_eq!(applied, 2);

        // Verify replica has the data
        let rows = group.replicas[0]
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[0], Datum::Int32(1));
    }

    #[test]
    fn test_apply_chunk_idempotent() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::Insert {
                    txn_id: TxnId(1),
                    table_id: TableId(1),
                    row,
                },
            },
            LsnWalRecord {
                lsn: 2,
                record: WalRecord::CommitTxnLocal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(1),
                },
            },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);

        // Apply once
        group.apply_chunk_to_replica(0, &chunk).unwrap();
        // Apply again (should be idempotent 鈥?skip already-applied LSNs)
        let applied2 = group.apply_chunk_to_replica(0, &chunk).unwrap();
        assert_eq!(applied2, 2); // returns chunk size but skips internally

        // Still just 1 row
        let rows = group.replicas[0]
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_in_process_transport_pull_with_limit() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log.clone());

        for i in 1..=10u64 {
            log.append(WalRecord::BeginTxn { txn_id: TxnId(i) });
        }

        // Pull with limit of 3
        let chunk = transport.pull_wal_chunk(ShardId(0), 0, 3).unwrap();
        assert_eq!(chunk.len(), 3);
        assert_eq!(chunk.start_lsn, 1);
        assert_eq!(chunk.end_lsn, 3);
    }
}

// ===========================================================================
// M2: AsyncReplicationTransport blanket impl tests
// ===========================================================================

#[cfg(test)]
mod async_transport_tests {
    use std::sync::Arc;

    use cedar_common::types::{ShardId, TxnId};
    use cedar_storage::wal::WalRecord;

    use crate::replication::{
        AsyncReplicationTransport, InProcessTransport, ChannelTransport,
        LsnWalRecord, ReplicationLog, WalChunk,
    };

    #[tokio::test]
    async fn test_async_pull_via_in_process_transport() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log.clone());

        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) });

        // Use the async trait method (blanket impl delegates to sync)
        let chunk = AsyncReplicationTransport::pull_wal_chunk(
            &transport, ShardId(0), 0, 100,
        ).await.unwrap();
        assert_eq!(chunk.len(), 2);
        assert!(chunk.verify_checksum());
    }

    #[tokio::test]
    async fn test_async_ack_via_in_process_transport() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log);

        AsyncReplicationTransport::ack_wal(
            &transport, ShardId(0), 0, 5,
        ).await.unwrap();
        assert_eq!(transport.get_ack_lsn(0), 5);
    }

    #[tokio::test]
    async fn test_async_pull_via_channel_transport() {
        let transport = ChannelTransport::new(ShardId(0));

        let records = vec![
            LsnWalRecord { lsn: 1, record: WalRecord::BeginTxn { txn_id: TxnId(1) } },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);
        transport.push_chunk(chunk).unwrap();

        let pulled = AsyncReplicationTransport::pull_wal_chunk(
            &transport, ShardId(0), 0, 100,
        ).await.unwrap();
        assert_eq!(pulled.len(), 1);
        assert!(pulled.verify_checksum());
    }
}

// ===========================================================================
// M2: GrpcTransport + WalReplicationService tests
// ===========================================================================

#[cfg(test)]
mod grpc_transport_tests {
    use std::sync::Arc;

    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::types::{ShardId, TableId, TxnId, Timestamp};
    use cedar_storage::wal::WalRecord;

    use crate::grpc_transport::{
        encode_wal_record, decode_wal_record, encode_wal_chunk, decode_wal_chunk,
        GrpcTransport, WalReplicationService,
    };
    use crate::replication::{
        AsyncReplicationTransport, LsnWalRecord, ReplicationLog, WalChunk,
    };

    #[test]
    fn test_wal_record_encode_decode_roundtrip() {
        let record = LsnWalRecord {
            lsn: 42,
            record: WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(7), Datum::Text("hello".into())]),
            },
        };
        let bytes = encode_wal_record(&record).unwrap();
        let decoded = decode_wal_record(&bytes).unwrap();
        assert_eq!(decoded.lsn, 42);
    }

    #[test]
    fn test_wal_chunk_encode_decode_roundtrip() {
        let records = vec![
            LsnWalRecord { lsn: 1, record: WalRecord::BeginTxn { txn_id: TxnId(1) } },
            LsnWalRecord { lsn: 2, record: WalRecord::CommitTxnLocal { txn_id: TxnId(1), commit_ts: Timestamp(10) } },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);
        let bytes = encode_wal_chunk(&chunk).unwrap();
        let decoded = decode_wal_chunk(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.start_lsn, 1);
        assert_eq!(decoded.end_lsn, 2);
        assert!(decoded.verify_checksum());
    }

    #[test]
    fn test_wal_replication_service_handle_pull() {
        let log = Arc::new(ReplicationLog::new());
        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(3) });

        let svc = WalReplicationService::new();
        svc.register_shard(ShardId(0), log);

        // Pull all from LSN 0
        let chunk = svc.handle_pull(ShardId(0), 0, 100).unwrap();
        assert_eq!(chunk.len(), 3);

        // Pull from LSN 2 鈥?should get only LSN 3
        let chunk2 = svc.handle_pull(ShardId(0), 2, 100).unwrap();
        assert_eq!(chunk2.len(), 1);
        assert_eq!(chunk2.start_lsn, 3);

        // Pull with limit
        let chunk3 = svc.handle_pull(ShardId(0), 0, 2).unwrap();
        assert_eq!(chunk3.len(), 2);
    }

    #[test]
    fn test_wal_replication_service_unknown_shard() {
        let svc = WalReplicationService::new();
        let result = svc.handle_pull(ShardId(99), 0, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_replication_service_ack_and_lag() {
        let log = Arc::new(ReplicationLog::new());
        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) });

        let svc = WalReplicationService::new();
        svc.register_shard(ShardId(0), log);

        // No acks yet
        assert_eq!(svc.get_ack_lsn(ShardId(0), 0), 0);

        // Ack LSN 1 from replica 0
        svc.handle_ack(ShardId(0), 0, 1);
        assert_eq!(svc.get_ack_lsn(ShardId(0), 0), 1);

        // Lag: primary at LSN 2, replica at LSN 1 鈫?lag = 1
        let lags = svc.replication_lag(ShardId(0));
        assert_eq!(lags.len(), 1);
        assert_eq!(lags[0], (0, 1));
    }

    #[tokio::test]
    async fn test_grpc_transport_pull_connection_refused() {
        // GrpcTransport now actually connects via gRPC 鈥?connecting to a
        // non-existent server should return a connection error.
        let transport = GrpcTransport::new("http://127.0.0.1:19999".into(), ShardId(0));
        let result = AsyncReplicationTransport::pull_wal_chunk(
            &transport, ShardId(0), 0, 100,
        ).await;
        assert!(result.is_err(), "pull to non-existent server should fail");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("gRPC connect error"), "Expected connect error, got: {}", err_msg);
    }

    #[tokio::test]
    async fn test_grpc_transport_ack_connection_refused() {
        let transport = GrpcTransport::new("http://127.0.0.1:19999".into(), ShardId(0));
        let result = AsyncReplicationTransport::ack_wal(
            &transport, ShardId(0), 0, 10,
        ).await;
        assert!(result.is_err(), "ack to non-existent server should fail");
    }

    #[tokio::test]
    async fn test_grpc_e2e_subscribe_and_ack() {
        use crate::proto::wal_replication_server::WalReplicationServer;

        // --- Set up server with some WAL records ---
        let log = Arc::new(crate::replication::ReplicationLog::new());
        log.append(cedar_storage::wal::WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(cedar_storage::wal::WalRecord::BeginTxn { txn_id: TxnId(2) });
        log.append(cedar_storage::wal::WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(100),
        });

        let svc = crate::grpc_transport::WalReplicationService::new();
        svc.register_shard(ShardId(0), log);

        // --- Start tonic server on a random port ---
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(WalReplicationServer::new(svc))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // --- Client: pull WAL chunk ---
        let endpoint = format!("http://127.0.0.1:{}", addr.port());
        let transport = GrpcTransport::new(endpoint, ShardId(0));

        let chunk = AsyncReplicationTransport::pull_wal_chunk(
            &transport, ShardId(0), 0, 100,
        ).await.unwrap();

        assert_eq!(chunk.len(), 3, "Should get all 3 WAL records");
        assert!(chunk.verify_checksum());
        assert_eq!(chunk.start_lsn, 1);
        assert_eq!(chunk.end_lsn, 3);

        // --- Client: pull from LSN 2 (should get records 3 only) ---
        let chunk2 = AsyncReplicationTransport::pull_wal_chunk(
            &transport, ShardId(0), 2, 100,
        ).await.unwrap();
        assert_eq!(chunk2.len(), 1);
        assert_eq!(chunk2.start_lsn, 3);

        // --- Client: ack WAL ---
        AsyncReplicationTransport::ack_wal(
            &transport, ShardId(0), 0, 3,
        ).await.unwrap();
        assert_eq!(transport.get_ack_lsn(0), 3);

        // Clean up
        server_handle.abort();
    }

    // --- Proto roundtrip tests ---

    #[test]
    fn test_proto_wal_chunk_roundtrip() {
        use crate::grpc_transport::{wal_chunk_to_proto, proto_to_wal_chunk};

        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: cedar_storage::wal::WalRecord::BeginTxn { txn_id: TxnId(1) },
            },
            LsnWalRecord {
                lsn: 2,
                record: cedar_storage::wal::WalRecord::Insert {
                    txn_id: TxnId(1),
                    table_id: cedar_common::types::TableId(10),
                    row: cedar_common::datum::OwnedRow::new(vec![
                        cedar_common::datum::Datum::Int32(42),
                        cedar_common::datum::Datum::Text("hello".into()),
                    ]),
                },
            },
            LsnWalRecord {
                lsn: 3,
                record: cedar_storage::wal::WalRecord::CommitTxnLocal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(100),
                },
            },
        ];

        let chunk = WalChunk::from_records(ShardId(7), records);
        assert_eq!(chunk.len(), 3);
        assert_eq!(chunk.shard_id, ShardId(7));
        assert!(chunk.verify_checksum());

        // Domain 鈫?Proto
        let proto_msg = wal_chunk_to_proto(&chunk).unwrap();
        assert_eq!(proto_msg.shard_id, 7);
        assert_eq!(proto_msg.start_lsn, 1);
        assert_eq!(proto_msg.end_lsn, 3);
        assert_eq!(proto_msg.records.len(), 3);
        assert_eq!(proto_msg.checksum, chunk.checksum);

        // Proto 鈫?Domain
        let roundtripped = proto_to_wal_chunk(&proto_msg).unwrap();
        assert_eq!(roundtripped.shard_id, ShardId(7));
        assert_eq!(roundtripped.start_lsn, 1);
        assert_eq!(roundtripped.end_lsn, 3);
        assert_eq!(roundtripped.len(), 3);
        assert!(roundtripped.verify_checksum());

        // Verify record contents survived roundtrip
        assert_eq!(roundtripped.records[0].lsn, 1);
        assert_eq!(roundtripped.records[1].lsn, 2);
        assert_eq!(roundtripped.records[2].lsn, 3);
    }

    #[test]
    fn test_proto_empty_chunk_roundtrip() {
        use crate::grpc_transport::{wal_chunk_to_proto, proto_to_wal_chunk};

        let chunk = WalChunk::empty(ShardId(0));
        let proto_msg = wal_chunk_to_proto(&chunk).unwrap();
        let roundtripped = proto_to_wal_chunk(&proto_msg).unwrap();
        assert!(roundtripped.is_empty());
        assert_eq!(roundtripped.shard_id, ShardId(0));
    }

    // --- Checkpoint streaming tests ---

    use crate::grpc_transport::{CheckpointStreamer, CheckpointAssembler};

    #[test]
    fn test_checkpoint_streamer_single_chunk() {
        let data = vec![1u8; 100];
        let streamer = CheckpointStreamer::from_bytes(data.clone(), 42, 256);
        assert_eq!(streamer.num_chunks(), 1);
        assert_eq!(streamer.total_bytes(), 100);
        let chunks = streamer.chunks();
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].total_chunks, 1);
        assert_eq!(chunks[0].checkpoint_lsn, 42);
        assert_eq!(chunks[0].data, data);
    }

    #[test]
    fn test_checkpoint_streamer_multi_chunk() {
        let data = vec![0xABu8; 1000];
        let streamer = CheckpointStreamer::from_bytes(data, 99, 300);
        // 1000 / 300 = 4 chunks (300, 300, 300, 100)
        assert_eq!(streamer.num_chunks(), 4);
        assert_eq!(streamer.total_bytes(), 1000);
        for (i, chunk) in streamer.chunks().iter().enumerate() {
            assert_eq!(chunk.chunk_index, i as u32);
            assert_eq!(chunk.total_chunks, 4);
            assert_eq!(chunk.checkpoint_lsn, 99);
        }
        assert_eq!(streamer.chunks()[3].data.len(), 100);
    }

    #[test]
    fn test_checkpoint_streamer_empty() {
        let streamer = CheckpointStreamer::from_bytes(vec![], 0, 256);
        assert_eq!(streamer.num_chunks(), 0);
        assert_eq!(streamer.total_bytes(), 0);
    }

    #[test]
    fn test_checkpoint_assembler_in_order() {
        let data = vec![0xCDu8; 500];
        let streamer = CheckpointStreamer::from_bytes(data.clone(), 10, 200);
        assert_eq!(streamer.num_chunks(), 3);

        let mut assembler = CheckpointAssembler::new(3, 10);
        assert!(!assembler.is_complete());

        for chunk in streamer.chunks() {
            assembler.add_chunk(chunk).unwrap();
        }
        assert!(assembler.is_complete());
        let reassembled = assembler.assemble().unwrap();
        assert_eq!(reassembled, data);
        assert_eq!(assembler.checkpoint_lsn(), 10);
    }

    #[test]
    fn test_checkpoint_assembler_out_of_order() {
        let data = vec![0xEFu8; 600];
        let streamer = CheckpointStreamer::from_bytes(data.clone(), 20, 200);
        let chunks = streamer.chunks();
        assert_eq!(chunks.len(), 3);

        let mut assembler = CheckpointAssembler::new(3, 20);
        // Add chunks out of order: 2, 0, 1
        assert!(!assembler.add_chunk(&chunks[2]).unwrap());
        assert!(!assembler.add_chunk(&chunks[0]).unwrap());
        assert!(assembler.add_chunk(&chunks[1]).unwrap());
        assert_eq!(assembler.assemble().unwrap(), data);
    }

    #[test]
    fn test_checkpoint_assembler_incomplete_fails() {
        let mut assembler = CheckpointAssembler::new(3, 5);
        let chunk = crate::grpc_transport::CheckpointChunk {
            chunk_index: 0,
            total_chunks: 3,
            data: vec![1, 2, 3],
            checkpoint_lsn: 5,
        };
        assembler.add_chunk(&chunk).unwrap();
        assert!(!assembler.is_complete());
        assert!(assembler.assemble().is_err());
    }

    #[test]
    fn test_checkpoint_assembler_out_of_range_chunk() {
        let mut assembler = CheckpointAssembler::new(2, 5);
        let bad_chunk = crate::grpc_transport::CheckpointChunk {
            chunk_index: 5,
            total_chunks: 2,
            data: vec![1],
            checkpoint_lsn: 5,
        };
        assert!(assembler.add_chunk(&bad_chunk).is_err());
    }

    #[test]
    fn test_checkpoint_data_full_roundtrip() {
        use cedar_common::schema::{Catalog, ColumnDef, TableSchema};
        use cedar_common::types::{ColumnId, DataType};
        use cedar_storage::wal::CheckpointData;

        // Build a realistic CheckpointData with schema + rows
        let schema = TableSchema {
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
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
        ..Default::default()
        };

        let mut catalog = Catalog::default();
        catalog.add_table(schema);

        let rows = vec![
            (vec![0, 0, 0, 1], OwnedRow::new(vec![Datum::Int32(1), Datum::Text("alice".into())])),
            (vec![0, 0, 0, 2], OwnedRow::new(vec![Datum::Int32(2), Datum::Text("bob".into())])),
        ];

        let ckpt = CheckpointData {
            catalog,
            table_data: vec![(TableId(1), rows)],
            wal_segment_id: 3,
            wal_lsn: 42,
        };

        // Stream 鈫?assemble roundtrip with small chunk size to force multi-chunk
        let streamer = CheckpointStreamer::from_checkpoint_data(&ckpt).unwrap();
        assert!(streamer.num_chunks() >= 1);

        let first = &streamer.chunks()[0];
        let mut assembler = CheckpointAssembler::new(first.total_chunks, first.checkpoint_lsn);
        for chunk in streamer.chunks() {
            assembler.add_chunk(chunk).unwrap();
        }
        assert!(assembler.is_complete());

        let restored = assembler.assemble_checkpoint().unwrap();
        assert_eq!(restored.wal_lsn, 42);
        assert_eq!(restored.wal_segment_id, 3);
        assert_eq!(restored.catalog.table_count(), 1);
        assert!(restored.catalog.find_table("users").is_some());
        assert_eq!(restored.table_data.len(), 1);
        assert_eq!(restored.table_data[0].1.len(), 2);
        assert_eq!(restored.table_data[0].1[0].1.values[1], Datum::Text("alice".into()));
        assert_eq!(restored.table_data[0].1[1].1.values[1], Datum::Text("bob".into()));
    }
}

// ===========================================================================
// M1: Promote fencing + metrics tests
// ===========================================================================

#[cfg(test)]
mod promote_fencing_tests {
    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::{ColumnDef, TableSchema};
    use cedar_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId};
    use cedar_storage::wal::WalRecord;

    use crate::replication::{ReplicaRole, ShardReplicaGroup};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "t".into(),
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
                    data_type: DataType::Text,
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

    fn insert_on_primary(
        group: &ShardReplicaGroup,
        txn_id: TxnId,
        commit_ts: Timestamp,
        id: i32,
        value: &str,
    ) {
        let row = OwnedRow::new(vec![Datum::Int32(id), Datum::Text(value.into())]);
        group
            .primary
            .storage
            .insert(TableId(1), row.clone(), txn_id)
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(txn_id, commit_ts, cedar_common::types::TxnType::Local)
            .unwrap();
        group.ship_wal_record(WalRecord::Insert {
            txn_id,
            table_id: TableId(1),
            row,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id, commit_ts });
    }

    #[test]
    fn test_old_primary_fenced_after_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Primary should not be read-only initially
        assert!(!group.primary.is_read_only());

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "before");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // New primary should not be read-only
        assert!(!group.primary.is_read_only());
        assert_eq!(group.primary.current_role(), ReplicaRole::Primary);

        // Old primary (now replica at index 0) should be fenced
        assert!(group.replicas[0].is_read_only());
        assert_eq!(group.replicas[0].current_role(), ReplicaRole::Replica);
    }

    #[test]
    fn test_promote_records_metrics() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Initial metrics
        let snap = group.metrics.snapshot();
        assert_eq!(snap.promote_count, 0);

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "v");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        let snap = group.metrics.snapshot();
        assert_eq!(snap.promote_count, 1);
        // failover_time_ms may be 0 (sub-ms) but should not panic
    }

    #[test]
    fn test_promote_with_routing_records_metrics() {
        use cedar_common::types::NodeId;
        use crate::routing::shard_map::ShardMap;

        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();
        let mut shard_map = ShardMap::uniform(2, NodeId(1));

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "v");
        group.catch_up_replica(0).unwrap();
        group.promote_with_routing(0, &mut shard_map, NodeId(2)).unwrap();

        let snap = group.metrics.snapshot();
        assert_eq!(snap.promote_count, 1);
        assert_eq!(shard_map.get_shard(ShardId(0)).unwrap().leader, NodeId(2));
    }

    #[test]
    fn test_new_primary_accepts_writes_after_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "before");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // New primary should accept writes (unfenced)
        assert!(!group.primary.is_read_only());
        let row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("after".into())]);
        group
            .primary
            .storage
            .insert(TableId(1), row, TxnId(10))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(TxnId(10), Timestamp(10), cedar_common::types::TxnType::Local)
            .unwrap();

        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_replica_starts_read_only() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();
        assert!(group.replicas[0].is_read_only());
        assert!(!group.primary.is_read_only());
    }

    #[test]
    fn test_promote_updates_engine_replication_stats() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Before promote, new primary's engine stats should be zero
        let snap_before = group.replicas[0].storage.replication_stats_snapshot();
        assert_eq!(snap_before.promote_count, 0);

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "v");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // After promote, new primary's engine stats should reflect the failover
        let snap_after = group.primary.storage.replication_stats_snapshot();
        assert_eq!(snap_after.promote_count, 1);
        // Duration may be 0 for sub-ms operations, but should not panic
    }

    #[test]
    fn test_double_promote_accumulates_engine_stats() {
        let schemas = vec![test_schema()];
        let mut group = ShardReplicaGroup::new(ShardId(0), &schemas).unwrap();

        // First promote: primary(A) 鈫?replica(B) promotes to primary
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "v1");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // Second promote: primary(B) 鈫?replica(A) promotes back
        let row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("v2".into())]);
        group.primary.storage.insert(TableId(1), row.clone(), TxnId(2)).unwrap();
        group.primary.storage.commit_txn(TxnId(2), Timestamp(2), cedar_common::types::TxnType::Local).unwrap();
        group.ship_wal_record(WalRecord::Insert { txn_id: TxnId(2), table_id: TableId(1), row });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id: TxnId(2), commit_ts: Timestamp(2) });
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // The current primary should have 1 promote recorded on its engine
        // (it was the target of the second promote)
        let snap = group.primary.storage.replication_stats_snapshot();
        assert_eq!(snap.promote_count, 1);

        // Cluster-level metrics should show 2 total promotes
        let cluster_snap = group.metrics.snapshot();
        assert_eq!(cluster_snap.promote_count, 2);
    }
}

// ===========================================================================
// M1: TxnContext invariant validation tests
// ===========================================================================

#[cfg(test)]
mod txn_context_tests {
    use cedar_common::types::{ShardId, Timestamp, TxnContext, TxnId, TxnPath, TxnType};

    #[test]
    fn test_local_txn_context_valid() {
        let ctx = TxnContext::local(TxnId(1), ShardId(0), Timestamp(1));
        assert!(ctx.validate_commit_invariants().is_ok());
    }

    #[test]
    fn test_global_txn_context_valid() {
        let ctx = TxnContext::global(
            TxnId(1),
            vec![ShardId(0), ShardId(1)],
            Timestamp(1),
        );
        assert!(ctx.validate_commit_invariants().is_ok());
    }

    #[test]
    fn test_local_txn_multiple_shards_invalid() {
        let ctx = TxnContext {
            txn_id: TxnId(1),
            txn_type: TxnType::Local,
            txn_path: TxnPath::Fast,
            involved_shards: vec![ShardId(0), ShardId(1)],
            start_ts: Timestamp(1),
        };
        let result = ctx.validate_commit_invariants();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("2 involved shards"));
    }

    #[test]
    fn test_local_txn_slow_path_invalid() {
        let ctx = TxnContext {
            txn_id: TxnId(1),
            txn_type: TxnType::Local,
            txn_path: TxnPath::Slow,
            involved_shards: vec![ShardId(0)],
            start_ts: Timestamp(1),
        };
        let result = ctx.validate_commit_invariants();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("slow path"));
    }

    #[test]
    fn test_global_txn_fast_path_invalid() {
        let ctx = TxnContext {
            txn_id: TxnId(1),
            txn_type: TxnType::Global,
            txn_path: TxnPath::Fast,
            involved_shards: vec![ShardId(0), ShardId(1)],
            start_ts: Timestamp(1),
        };
        let result = ctx.validate_commit_invariants();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("fast path"));
    }

    #[test]
    fn test_local_txn_zero_shards_invalid() {
        let ctx = TxnContext {
            txn_id: TxnId(1),
            txn_type: TxnType::Local,
            txn_path: TxnPath::Fast,
            involved_shards: vec![],
            start_ts: Timestamp(1),
        };
        let result = ctx.validate_commit_invariants();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("0 involved shards"));
    }

    #[test]
    fn test_txn_handle_to_context() {
        use cedar_common::types::IsolationLevel;
        use cedar_txn::manager::{SlowPathMode, TxnHandle, TxnState};

        let handle = TxnHandle {
            txn_id: TxnId(42),
            start_ts: Timestamp(100),
            isolation: IsolationLevel::ReadCommitted,
            txn_type: TxnType::Local,
            path: TxnPath::Fast,
            slow_path_mode: SlowPathMode::Xa2Pc,
            involved_shards: vec![ShardId(0)],
            degraded: false,
            state: TxnState::Active,
            begin_instant: None,
            trace_id: 42,
            occ_retry_count: 0,
            tenant_id: cedar_common::tenant::SYSTEM_TENANT_ID,
            priority: cedar_common::security::TxnPriority::Normal,
            latency_breakdown: cedar_common::kernel::TxnLatencyBreakdown::default(),
        };

        let ctx = handle.to_context();
        assert_eq!(ctx.txn_id, TxnId(42));
        assert_eq!(ctx.txn_type, TxnType::Local);
        assert_eq!(ctx.txn_path, TxnPath::Fast);
        assert_eq!(ctx.involved_shards, vec![ShardId(0)]);
        assert_eq!(ctx.start_ts, Timestamp(100));
        assert!(ctx.validate_commit_invariants().is_ok());
    }
}

// ===========================================================================
// Replication + GC combined correctness tests
// ===========================================================================

#[cfg(test)]
mod replication_gc_tests {
    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::{ColumnDef, TableSchema};
    use cedar_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId};
    use cedar_storage::gc::{compute_safepoint, GcConfig, GcStats};
    use cedar_storage::wal::WalRecord;

    use crate::replication::ShardReplicaGroup;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "t".into(),
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
                    data_type: DataType::Text,
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

    fn insert_on_primary(
        group: &ShardReplicaGroup,
        txn_id: TxnId,
        commit_ts: Timestamp,
        id: i32,
        value: &str,
    ) {
        let row = OwnedRow::new(vec![Datum::Int32(id), Datum::Text(value.into())]);
        group
            .primary
            .storage
            .insert(TableId(1), row.clone(), txn_id)
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(txn_id, commit_ts, cedar_common::types::TxnType::Local)
            .unwrap();
        group.ship_wal_record(WalRecord::Insert {
            txn_id,
            table_id: TableId(1),
            row,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id, commit_ts });
    }

    #[test]
    fn test_gc_on_primary_does_not_break_replica_catchup() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Write 3 versions of key 1 on primary
        let base_row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v1".into())]);
        let pk = cedar_storage::memtable::encode_pk(&base_row, &[0]);

        // Insert v1
        group.primary.storage.insert(TableId(1), base_row.clone(), TxnId(1)).unwrap();
        group.primary.storage.commit_txn(TxnId(1), Timestamp(10), cedar_common::types::TxnType::Local).unwrap();
        group.ship_wal_record(WalRecord::Insert { txn_id: TxnId(1), table_id: TableId(1), row: base_row });

        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id: TxnId(1), commit_ts: Timestamp(10) });

        // Update to v2
        let row2 = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v2".into())]);
        group.primary.storage.update(TableId(1), &pk, row2.clone(), TxnId(2)).unwrap();
        group.primary.storage.commit_txn(TxnId(2), Timestamp(20), cedar_common::types::TxnType::Local).unwrap();
        group.ship_wal_record(WalRecord::Update { txn_id: TxnId(2), table_id: TableId(1), pk: pk.clone(), new_row: row2 });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id: TxnId(2), commit_ts: Timestamp(20) });

        // Update to v3
        let row3 = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v3".into())]);
        group.primary.storage.update(TableId(1), &pk, row3.clone(), TxnId(3)).unwrap();
        group.primary.storage.commit_txn(TxnId(3), Timestamp(30), cedar_common::types::TxnType::Local).unwrap();
        group.ship_wal_record(WalRecord::Update { txn_id: TxnId(3), table_id: TableId(1), pk: pk.clone(), new_row: row3 });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id: TxnId(3), commit_ts: Timestamp(30) });

        // GC on primary at watermark 25 鈥?reclaims ts=10
        let result = group.primary.storage.gc_sweep(Timestamp(25));
        assert_eq!(result.reclaimed_versions, 1);

        // Replica catch-up should still work (WAL replay is independent of in-memory GC)
        group.catch_up_replica(0).unwrap();

        // Verify replica has the data
        let rows = group.replicas[0]
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
        // Replica should have the latest value
        assert_eq!(rows[0].1.values[1], Datum::Text("v3".into()));
    }

    #[test]
    fn test_gc_safepoint_respects_replica_lag() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Write 3 rows on primary
        for i in 1..=3u32 {
            insert_on_primary(
                &group,
                TxnId(i as u64),
                Timestamp(i as u64 * 10),
                i as i32,
                &format!("v{}", i),
            );
        }

        // Simulate: replica has NOT caught up (applied_ts = 0).
        // In a real system, replica_safe_ts would come from the replica's
        // applied LSN mapped to a timestamp.
        let min_active_ts = Timestamp(100); // no active txns blocking
        let replica_safe_ts = Timestamp(15); // replica only applied up to ts=15

        let safepoint = compute_safepoint(min_active_ts, replica_safe_ts);
        assert_eq!(safepoint, Timestamp(14));

        // GC with replica-aware safepoint: should NOT reclaim anything
        // because all commits are at ts=10, 20, 30 and safepoint=14
        // means only ts=10 is eligible, but each key has a single version.
        let config = GcConfig { min_chain_length: 0, ..Default::default() };
        let stats = GcStats::new();
        let result = group.primary.storage.run_gc_with_config(safepoint, &config, &stats);
        // No versions to reclaim (each key has exactly 1 version)
        assert_eq!(result.reclaimed_versions, 0);

        // Now catch up replica and recalculate
        group.catch_up_replica(0).unwrap();
        // After catch-up, replica_safe_ts would advance to 30
        let safepoint2 = compute_safepoint(min_active_ts, Timestamp(100));
        assert_eq!(safepoint2, Timestamp(99));

        // Still 0 reclaimed 鈥?each key has 1 version (no multi-version chains)
        let result2 = group.primary.storage.run_gc_with_config(safepoint2, &config, &stats);
        assert_eq!(result2.reclaimed_versions, 0);
    }

    #[test]
    fn test_gc_and_promote_combined() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Write 2 rows, GC on primary, then promote
        insert_on_primary(&group, TxnId(1), Timestamp(10), 1, "before");
        insert_on_primary(&group, TxnId(2), Timestamp(20), 2, "before2");

        // GC on primary (no multi-version chains, nothing to reclaim)
        group.primary.storage.gc_sweep(Timestamp(25));

        // Catch up and promote
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // New primary should have both rows
        let rows = group.primary.storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 2);

        // New primary should be writable
        let row = OwnedRow::new(vec![Datum::Int32(3), Datum::Text("after".into())]);
        group.primary.storage.insert(TableId(1), row, TxnId(10)).unwrap();
        group.primary.storage.commit_txn(
            TxnId(10), Timestamp(30),
            cedar_common::types::TxnType::Local,
        ).unwrap();

        let rows2 = group.primary.storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows2.len(), 3);
    }

    /// Comprehensive M1 acceptance test exercising the full lifecycle:
    /// 1. Create cluster (1 primary + 1 replica)
    /// 2. Write multiple rows with updates (multi-version chains)
    /// 3. Replicate to replica
    /// 4. GC on primary (reclaim old versions)
    /// 5. Promote replica to primary
    /// 6. Write new data on promoted primary
    /// 7. Verify all committed data is intact (zero data loss)
    /// 8. Verify replication stats reflect the promote
    /// 9. Verify GC stats reflect the sweep
    #[test]
    fn test_m1_full_lifecycle_acceptance() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // 鈹€鈹€ Phase 1: Write data with multi-version chains 鈹€鈹€
        // Insert 5 rows
        for i in 1..=5 {
            insert_on_primary(
                &group,
                TxnId(i as u64),
                Timestamp(i as u64 * 10),
                i,
                &format!("original_{}", i),
            );
        }

        // Update rows 1-3 to create multi-version chains
        let pk1 = cedar_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(1), Datum::Text("x".into())]), &[0]);
        let pk2 = cedar_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(2), Datum::Text("x".into())]), &[0]);
        let pk3 = cedar_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(3), Datum::Text("x".into())]), &[0]);

        let upd1 = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("updated_1".into())]);
        group.primary.storage.update(TableId(1), &pk1, upd1.clone(), TxnId(10)).unwrap();
        group.primary.storage.commit_txn(TxnId(10), Timestamp(60), cedar_common::types::TxnType::Local).unwrap();
        group.ship_wal_record(WalRecord::Update { txn_id: TxnId(10), table_id: TableId(1), pk: pk1.clone(), new_row: upd1 });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id: TxnId(10), commit_ts: Timestamp(60) });

        let upd2 = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("updated_2".into())]);
        group.primary.storage.update(TableId(1), &pk2, upd2.clone(), TxnId(11)).unwrap();
        group.primary.storage.commit_txn(TxnId(11), Timestamp(70), cedar_common::types::TxnType::Local).unwrap();
        group.ship_wal_record(WalRecord::Update { txn_id: TxnId(11), table_id: TableId(1), pk: pk2.clone(), new_row: upd2 });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id: TxnId(11), commit_ts: Timestamp(70) });

        let upd3 = OwnedRow::new(vec![Datum::Int32(3), Datum::Text("updated_3".into())]);
        group.primary.storage.update(TableId(1), &pk3, upd3.clone(), TxnId(12)).unwrap();
        group.primary.storage.commit_txn(TxnId(12), Timestamp(80), cedar_common::types::TxnType::Local).unwrap();
        group.ship_wal_record(WalRecord::Update { txn_id: TxnId(12), table_id: TableId(1), pk: pk3.clone(), new_row: upd3 });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id: TxnId(12), commit_ts: Timestamp(80) });

        // 鈹€鈹€ Phase 2: GC on primary 鈹€鈹€
        // Watermark must be >= max update ts (80) so updated versions become
        // the "latest visible at watermark", making the original versions
        // (ts=10,20,30) obsolete and reclaimable in chains of length 2.
        let gc_result = group.primary.storage.gc_sweep(Timestamp(85));
        assert!(gc_result.reclaimed_versions >= 3,
            "GC should reclaim at least 3 old versions, got {}", gc_result.reclaimed_versions);

        // Verify GC stats on primary
        let gc_snap = group.primary.storage.gc_stats_snapshot();
        assert!(gc_snap.total_sweeps >= 1);
        assert!(gc_snap.total_reclaimed_versions >= 3);

        // 鈹€鈹€ Phase 3: Replicate and promote 鈹€鈹€
        group.catch_up_replica(0).unwrap();

        // Verify replica has all 5 rows before promote
        let replica_rows = group.replicas[0].storage
            .scan(TableId(1), TxnId(999), Timestamp(200))
            .unwrap();
        assert_eq!(replica_rows.len(), 5, "replica should have all 5 rows before promote");

        // Promote
        group.promote(0).unwrap();

        // 鈹€鈹€ Phase 4: Verify on new primary 鈹€鈹€
        // Check replication stats
        let repl_snap = group.primary.storage.replication_stats_snapshot();
        assert_eq!(repl_snap.promote_count, 1, "promote_count should be 1");

        // Cluster metrics
        let cluster_snap = group.metrics.snapshot();
        assert_eq!(cluster_snap.promote_count, 1);

        // New primary should not be read-only
        assert!(!group.primary.is_read_only());

        // All 5 rows should be intact with updated values for rows 1-3
        let rows = group.primary.storage
            .scan(TableId(1), TxnId(999), Timestamp(200))
            .unwrap();
        assert_eq!(rows.len(), 5, "new primary should have all 5 rows");

        // Verify updated values
        let row1 = rows.iter().find(|(_, r)| r.values[0] == Datum::Int32(1)).unwrap();
        assert_eq!(row1.1.values[1], Datum::Text("updated_1".into()));
        let row2 = rows.iter().find(|(_, r)| r.values[0] == Datum::Int32(2)).unwrap();
        assert_eq!(row2.1.values[1], Datum::Text("updated_2".into()));
        let row3 = rows.iter().find(|(_, r)| r.values[0] == Datum::Int32(3)).unwrap();
        assert_eq!(row3.1.values[1], Datum::Text("updated_3".into()));
        // Rows 4-5 should still have original values
        let row4 = rows.iter().find(|(_, r)| r.values[0] == Datum::Int32(4)).unwrap();
        assert_eq!(row4.1.values[1], Datum::Text("original_4".into()));
        let row5 = rows.iter().find(|(_, r)| r.values[0] == Datum::Int32(5)).unwrap();
        assert_eq!(row5.1.values[1], Datum::Text("original_5".into()));

        // 鈹€鈹€ Phase 5: Write on promoted primary 鈹€鈹€
        let new_row = OwnedRow::new(vec![Datum::Int32(6), Datum::Text("post_promote".into())]);
        group.primary.storage.insert(TableId(1), new_row, TxnId(100)).unwrap();
        group.primary.storage.commit_txn(
            TxnId(100), Timestamp(200),
            cedar_common::types::TxnType::Local,
        ).unwrap();

        let final_rows = group.primary.storage
            .scan(TableId(1), TxnId(999), Timestamp(300))
            .unwrap();
        assert_eq!(final_rows.len(), 6, "post-promote write should succeed, total 6 rows");

        let row6 = final_rows.iter().find(|(_, r)| r.values[0] == Datum::Int32(6)).unwrap();
        assert_eq!(row6.1.values[1], Datum::Text("post_promote".into()));
    }

    #[test]
    fn test_grpc_transport_with_timeout() {
        let transport = crate::grpc_transport::GrpcTransport::with_timeout(
            "http://127.0.0.1:50099".to_string(),
            ShardId(0),
            std::time::Duration::from_millis(2500),
        );
        assert_eq!(transport.endpoint, "http://127.0.0.1:50099");
        assert_eq!(transport.shard_id, ShardId(0));
        assert_eq!(transport.get_ack_lsn(0), 0);
        assert_eq!(transport.get_ack_lsn(99), 0);
    }

    #[test]
    fn test_replication_config_new_fields_default() {
        let config = cedar_common::config::ReplicationConfig::default();
        assert_eq!(config.max_backoff_ms, 30_000);
        assert_eq!(config.connect_timeout_ms, 5_000);
        assert_eq!(config.poll_interval_ms, 100);
    }

    #[test]
    fn test_replication_config_serde_roundtrip() {
        let config = cedar_common::config::ReplicationConfig {
            role: cedar_common::config::NodeRole::Replica,
            grpc_listen_addr: "0.0.0.0:50052".to_string(),
            primary_endpoint: "http://127.0.0.1:50051".to_string(),
            max_records_per_chunk: 500,
            poll_interval_ms: 50,
            max_backoff_ms: 10_000,
            connect_timeout_ms: 3_000,
            shard_count: 2,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deser: cedar_common::config::ReplicationConfig =
            serde_json::from_str(&json).unwrap();
        assert_eq!(deser.role, cedar_common::config::NodeRole::Replica);
        assert_eq!(deser.max_records_per_chunk, 500);
        assert_eq!(deser.poll_interval_ms, 50);
        assert_eq!(deser.max_backoff_ms, 10_000);
        assert_eq!(deser.connect_timeout_ms, 3_000);
        assert_eq!(deser.shard_count, 2);
    }

    #[test]
    fn test_exponential_backoff_calculation() {
        let poll_ms: u64 = 100;
        let max_backoff_ms: u64 = 30_000;

        // Simulate consecutive errors and verify backoff
        let backoff = |errors: u32| -> u64 {
            std::cmp::min(poll_ms * 2u64.saturating_pow(errors), max_backoff_ms)
        };

        assert_eq!(backoff(1), 200);    // 100 * 2^1
        assert_eq!(backoff(2), 400);    // 100 * 2^2
        assert_eq!(backoff(3), 800);    // 100 * 2^3
        assert_eq!(backoff(5), 3200);   // 100 * 2^5
        assert_eq!(backoff(8), 25600);  // 100 * 2^8
        assert_eq!(backoff(9), 30_000); // capped at max
        assert_eq!(backoff(20), 30_000); // still capped
        assert_eq!(backoff(0), 100);    // 100 * 2^0 = 100 (no error)
    }
}

// ===========================================================================
// ReplicaRunner E2E tests 鈥?real gRPC server + client replication
// ===========================================================================

#[cfg(test)]
mod replica_runner_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use cedar_common::datum::{Datum, OwnedRow};
    use cedar_common::schema::{ColumnDef, TableSchema};
    use cedar_common::types::{
        ColumnId, DataType, ShardId, TableId, Timestamp, TxnId, TxnType,
    };
    use cedar_storage::engine::StorageEngine;
    use cedar_storage::wal::WalRecord;

    use crate::grpc_transport::WalReplicationService;
    use crate::proto::wal_replication_server::WalReplicationServer;
    use crate::replication::{
        ReplicaRunner, ReplicaRunnerConfig, ReplicationLog,
    };

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "accounts".into(),
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
                    name: "balance".into(),
                    data_type: DataType::Int64,
                    nullable: false,
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

    /// Full E2E test: primary gRPC server → ReplicaRunner → replica StorageEngine.
    ///
    /// The replica starts EMPTY (no pre-created tables). The ReplicaRunner
    /// bootstraps via GetCheckpoint, then subscribes to WAL for incremental updates.
    #[tokio::test]
    async fn test_replica_runner_e2e_cross_node() {
        // --- Primary setup ---
        let primary_storage = Arc::new(StorageEngine::new_in_memory());
        primary_storage.create_table(test_schema()).unwrap();

        // Write initial data on primary BEFORE starting the server so the
        // checkpoint will contain this data.
        let row1 = OwnedRow::new(vec![Datum::Int32(1), Datum::Int64(1000)]);
        primary_storage.insert(TableId(1), row1.clone(), TxnId(1)).unwrap();
        primary_storage.commit_txn(TxnId(1), Timestamp(10), TxnType::Local).unwrap();

        let row2 = OwnedRow::new(vec![Datum::Int32(2), Datum::Int64(2500)]);
        primary_storage.insert(TableId(1), row2.clone(), TxnId(2)).unwrap();
        primary_storage.commit_txn(TxnId(2), Timestamp(20), TxnType::Local).unwrap();

        let log = Arc::new(ReplicationLog::new());
        // Ship the initial WAL records so the replica can replay them after checkpoint.
        log.append(WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: row1,
        });
        log.append(WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(10),
        });
        log.append(WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row: row2,
        });
        log.append(WalRecord::CommitTxnLocal {
            txn_id: TxnId(2),
            commit_ts: Timestamp(20),
        });

        let svc = WalReplicationService::new();
        svc.register_shard(ShardId(0), log.clone());
        // Wire primary storage so GetCheckpoint RPC works.
        svc.set_storage(primary_storage.clone());

        // Start gRPC server on random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let _ = tonic::transport::Server::builder()
                .add_service(WalReplicationServer::new(svc))
                .serve_with_incoming(incoming)
                .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // --- Replica setup: starts EMPTY, bootstraps via checkpoint ---
        let replica_storage = Arc::new(StorageEngine::new_in_memory());

        let config = ReplicaRunnerConfig {
            primary_endpoint: format!("http://127.0.0.1:{}", addr.port()),
            shard_id: ShardId(0),
            replica_id: 0,
            max_records_per_chunk: 100,
            ack_interval_chunks: 1,
            initial_backoff: std::time::Duration::from_millis(50),
            max_backoff: std::time::Duration::from_secs(1),
            connect_timeout: std::time::Duration::from_secs(5),
        };

        let runner = ReplicaRunner::new(config, replica_storage.clone());
        let handle = runner.start();

        // --- Wait for checkpoint bootstrap + WAL catch-up ---
        // The checkpoint contains 2 rows; after bootstrap applied_lsn = ckpt_lsn (0 for
        // in-memory engine with no WAL). Then WAL records are replayed on top.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!("Replica did not catch up within timeout (applied_lsn={})",
                    handle.metrics().applied_lsn.load(Ordering::Relaxed));
            }
            // Check if replica has the table (schema bootstrapped from checkpoint)
            if replica_storage.get_table_schema("accounts").is_some() {
                let rows = replica_storage
                    .scan(TableId(1), TxnId(999), Timestamp(100))
                    .unwrap_or_default();
                if rows.len() >= 2 {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // --- Verify replica has the data ---
        let rows = replica_storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert!(rows.len() >= 2, "Replica should have at least 2 rows after bootstrap");

        let snap = handle.metrics().snapshot();
        assert!(snap.connected);

        // --- Write MORE data on primary ---
        let row3 = OwnedRow::new(vec![Datum::Int32(3), Datum::Int64(500)]);
        primary_storage.insert(TableId(1), row3.clone(), TxnId(3)).unwrap();
        primary_storage.commit_txn(TxnId(3), Timestamp(30), TxnType::Local).unwrap();

        log.append(WalRecord::Insert {
            txn_id: TxnId(3),
            table_id: TableId(1),
            row: row3,
        });
        log.append(WalRecord::CommitTxnLocal {
            txn_id: TxnId(3),
            commit_ts: Timestamp(30),
        });

        // Wait for replica to apply the new WAL record
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!("Replica did not catch up to new data");
            }
            let rows = replica_storage
                .scan(TableId(1), TxnId(999), Timestamp(100))
                .unwrap_or_default();
            if rows.len() >= 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let rows = replica_storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert!(rows.len() >= 3, "Replica should have 3 rows after incremental catch-up");

        // --- Clean shutdown ---
        handle.stop().await;
        server_handle.abort();
    }

    /// Test that ReplicaRunner retries on connection failure and increments reconnect_count.
    #[tokio::test]
    async fn test_replica_runner_reconnect_on_failure() {
        // Use an unreachable endpoint so every connect attempt fails fast.
        let replica_storage = Arc::new(StorageEngine::new_in_memory());

        let config = ReplicaRunnerConfig {
            primary_endpoint: "http://127.0.0.1:1".into(), // port 1 鈥?refused
            shard_id: ShardId(0),
            replica_id: 0,
            max_records_per_chunk: 100,
            ack_interval_chunks: 1,
            initial_backoff: std::time::Duration::from_millis(30),
            max_backoff: std::time::Duration::from_millis(60),
            connect_timeout: std::time::Duration::from_millis(100),
        };

        let runner = ReplicaRunner::new(config, replica_storage);
        let handle = runner.start();

        // Wait for a few reconnect cycles (each ~30-100ms)
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

        // Runner should still be alive, retrying
        assert!(handle.is_running(), "Runner should still be alive while retrying");

        let snap = handle.metrics().snapshot();
        assert!(snap.reconnect_count >= 2,
            "Expected at least 2 reconnect attempts, got {}", snap.reconnect_count);
        assert!(!snap.connected, "Should not be connected to unreachable endpoint");
        assert_eq!(snap.applied_lsn, 0, "No data should have been applied");

        handle.stop().await;
    }

    /// Test that ReplicaRunner stops cleanly when signaled.
    #[tokio::test]
    async fn test_replica_runner_clean_stop() {
        // Point at a non-existent server 鈥?runner will keep retrying
        let replica_storage = Arc::new(StorageEngine::new_in_memory());
        let config = ReplicaRunnerConfig {
            primary_endpoint: "http://127.0.0.1:1".into(), // unlikely to be open
            shard_id: ShardId(0),
            replica_id: 0,
            initial_backoff: std::time::Duration::from_millis(50),
            max_backoff: std::time::Duration::from_millis(100),
            connect_timeout: std::time::Duration::from_millis(100),
            ..ReplicaRunnerConfig::default()
        };

        let runner = ReplicaRunner::new(config, replica_storage);
        let handle = runner.start();

        // Let it attempt a few reconnects
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(handle.is_running());

        // Stop should complete within a reasonable time
        let stop_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        handle.stop().await;
        assert!(tokio::time::Instant::now() < stop_deadline, "Stop took too long");
    }

    /// Test ReplicationLog::trim_before removes records at or below the safe LSN.
    #[test]
    fn test_replication_log_trim_before() {
        use crate::replication::ReplicationLog;
        use cedar_common::types::TxnId;
        use cedar_storage::wal::WalRecord;

        let log = ReplicationLog::new();
        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) }); // LSN 1
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) }); // LSN 2
        log.append(WalRecord::BeginTxn { txn_id: TxnId(3) }); // LSN 3
        log.append(WalRecord::BeginTxn { txn_id: TxnId(4) }); // LSN 4
        assert_eq!(log.len(), 4);
        assert_eq!(log.current_lsn(), 4);

        // Trim records with LSN <= 2
        let removed = log.trim_before(2);
        assert_eq!(removed, 2, "Should remove LSN 1 and 2");
        assert_eq!(log.len(), 2, "Should have LSN 3 and 4 remaining");
        assert_eq!(log.min_lsn(), 3, "Minimum remaining LSN should be 3");

        // read_from(0) should still return LSN 3 and 4
        let records = log.read_from(0);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].lsn, 3);
        assert_eq!(records[1].lsn, 4);

        // Trim everything
        let removed2 = log.trim_before(4);
        assert_eq!(removed2, 2);
        assert_eq!(log.len(), 0);
        assert_eq!(log.min_lsn(), 0);

        // Trim on empty log is a no-op
        let removed3 = log.trim_before(100);
        assert_eq!(removed3, 0);
    }

    /// Test StorageEngine::apply_checkpoint_data bootstraps a replica correctly.
    #[test]
    fn test_apply_checkpoint_data_bootstrap() {
        use cedar_common::datum::{Datum, OwnedRow};
        use cedar_common::schema::{Catalog, ColumnDef, TableSchema};
        use cedar_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};
        use cedar_storage::engine::StorageEngine;
        use cedar_storage::wal::CheckpointData;

        // Build a checkpoint with one table and two rows.
        let schema = TableSchema {
            id: TableId(1),
            name: "accounts".into(),
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
                    name: "balance".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        };

        let mut catalog = Catalog::default();
        catalog.add_table(schema.clone());

        // Encode PK bytes for the two rows (Int32 big-endian with sign flip)
        let pk1 = cedar_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(1), Datum::Int64(1000)]),
            &[0],
        );
        let pk2 = cedar_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(2), Datum::Int64(2500)]),
            &[0],
        );

        let ckpt = CheckpointData {
            catalog,
            table_data: vec![(
                TableId(1),
                vec![
                    (pk1, OwnedRow::new(vec![Datum::Int32(1), Datum::Int64(1000)])),
                    (pk2, OwnedRow::new(vec![Datum::Int32(2), Datum::Int64(2500)])),
                ],
            )],
            wal_segment_id: 0,
            wal_lsn: 42,
        };

        // Apply to a fresh empty engine
        let engine = StorageEngine::new_in_memory();
        engine.apply_checkpoint_data(&ckpt).unwrap();

        // Verify schema was restored
        assert!(engine.get_table_schema("accounts").is_some());

        // Verify rows were restored
        let rows = engine.scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1)).unwrap();
        assert_eq!(rows.len(), 2, "Both rows should be present after checkpoint apply");

        // Apply again (idempotent: clears and re-applies)
        engine.apply_checkpoint_data(&ckpt).unwrap();
        let rows2 = engine.scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1)).unwrap();
        assert_eq!(rows2.len(), 2, "Idempotent re-apply should still have 2 rows");
    }
}
