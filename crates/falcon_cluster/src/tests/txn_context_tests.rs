    use falcon_common::types::{ShardId, Timestamp, TxnContext, TxnId, TxnPath, TxnType};

    #[test]
    fn test_local_txn_context_valid() {
        let ctx = TxnContext::local(TxnId(1), ShardId(0), Timestamp(1));
        assert!(ctx.validate_commit_invariants().is_ok());
    }

    #[test]
    fn test_global_txn_context_valid() {
        let ctx = TxnContext::global(TxnId(1), vec![ShardId(0), ShardId(1)], Timestamp(1));
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
        use falcon_common::types::IsolationLevel;
        use falcon_txn::manager::{SlowPathMode, TxnHandle, TxnState};

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
            tenant_id: falcon_common::tenant::SYSTEM_TENANT_ID,
            priority: falcon_common::security::TxnPriority::Normal,
            latency_breakdown: falcon_common::kernel::TxnLatencyBreakdown::default(),
            read_only: false,
            timeout_ms: 0,
            exec_summary: falcon_txn::manager::TxnExecSummary::default(),
        };

        let ctx = handle.to_context();
        assert_eq!(ctx.txn_id, TxnId(42));
        assert_eq!(ctx.txn_type, TxnType::Local);
        assert_eq!(ctx.txn_path, TxnPath::Fast);
        assert_eq!(ctx.involved_shards, vec![ShardId(0)]);
        assert_eq!(ctx.start_ts, Timestamp(100));
        assert!(ctx.validate_commit_invariants().is_ok());
    }
