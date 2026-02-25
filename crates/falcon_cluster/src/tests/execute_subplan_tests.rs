    use std::sync::Arc;

    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, IsolationLevel, ShardId, TableId};

    use crate::sharded_engine::ShardedEngine;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "sub".into(),
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
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(42)]),
                txn.txn_id,
            )
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

        let result = engine.execute_subplan(ShardId(99), |_, _| Ok((vec![], vec![])));

        assert!(result.is_err(), "should error for invalid shard ID");
    }
