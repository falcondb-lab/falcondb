    use crate::mvcc::VersionChain;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::types::{Timestamp, TxnId};

    fn row(vals: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(vals)
    }

    #[test]
    fn test_version_chain_basic_insert_commit_read() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let ts_commit = Timestamp(10);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(42)])));
        // Uncommitted  鈥?not visible to other txns
        assert!(chain.read_committed(Timestamp(100)).is_none());
        // Visible to own txn
        assert!(chain.read_for_txn(txn1, Timestamp(0)).is_some());

        chain.commit(txn1, ts_commit);
        // Now visible to reads at ts >= 10
        let r = chain.read_committed(Timestamp(10));
        assert!(r.is_some());
        assert_eq!(r.unwrap().values[0], Datum::Int32(42));

        // Not visible to reads before commit ts
        assert!(chain.read_committed(Timestamp(5)).is_none());
    }

    #[test]
    fn test_version_chain_abort() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);

        chain.prepend(txn1, Some(row(vec![Datum::Text("aborted".into())])));
        chain.abort(txn1);

        // Aborted version never visible
        assert!(chain.read_committed(Timestamp(1000)).is_none());
        assert!(chain.read_for_txn(txn1, Timestamp(1000)).is_none());
    }

    #[test]
    fn test_version_chain_multiple_versions() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        // txn1 inserts version at ts=10
        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        chain.commit(txn1, Timestamp(10));

        // txn2 updates at ts=20
        chain.prepend(txn2, Some(row(vec![Datum::Int32(2)])));
        chain.commit(txn2, Timestamp(20));

        // Read at ts=15 sees version from txn1
        let r = chain.read_committed(Timestamp(15));
        assert_eq!(r.unwrap().values[0], Datum::Int32(1));

        // Read at ts=25 sees version from txn2
        let r = chain.read_committed(Timestamp(25));
        assert_eq!(r.unwrap().values[0], Datum::Int32(2));
    }

    #[test]
    fn test_version_chain_delete_tombstone() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(99)])));
        chain.commit(txn1, Timestamp(10));

        // Delete = tombstone (None data)
        chain.prepend(txn2, None);
        chain.commit(txn2, Timestamp(20));

        // Before delete: visible
        let r = chain.read_committed(Timestamp(15));
        assert_eq!(r.unwrap().values[0], Datum::Int32(99));

        // After delete: not visible (tombstone returns None)
        assert!(chain.read_committed(Timestamp(25)).is_none());
    }

    #[test]
    fn test_write_conflict_detection() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        // txn1 has uncommitted write  鈥?txn2 should see conflict
        assert!(chain.has_write_conflict(txn2));
        // txn1 itself should NOT see conflict
        assert!(!chain.has_write_conflict(txn1));

        chain.commit(txn1, Timestamp(10));
        // After commit, no conflict
        assert!(!chain.has_write_conflict(txn2));
    }

    #[test]
    fn test_gc_truncates_old_versions() {
        let chain = VersionChain::new();

        for i in 1..=5u64 {
            let txn = TxnId(i);
            chain.prepend(txn, Some(row(vec![Datum::Int64(i as i64)])));
            chain.commit(txn, Timestamp(i * 10));
        }

        // GC with watermark=30 should keep version at ts=30 and drop older
        chain.gc(Timestamp(30));

        // Version at ts=50 still visible
        let r = chain.read_committed(Timestamp(50));
        assert_eq!(r.unwrap().values[0], Datum::Int64(5));

        // Version at ts=30 still visible
        let r = chain.read_committed(Timestamp(30));
        assert!(r.is_some());
    }
