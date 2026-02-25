    use crate::mvcc::VersionChain;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::types::{Timestamp, TxnId};

    fn row(vals: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(vals)
    }

    #[test]
    fn test_has_committed_write_after_detects_conflict() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        chain.commit(txn1, Timestamp(10));

        chain.prepend(txn2, Some(row(vec![Datum::Int32(2)])));
        chain.commit(txn2, Timestamp(20));

        // txn3 started at ts=15 should see conflict (txn2 committed at 20 > 15)
        assert!(chain.has_committed_write_after(TxnId(3), Timestamp(15)));
        // txn4 started at ts=25 should NOT see conflict
        assert!(!chain.has_committed_write_after(TxnId(4), Timestamp(25)));
    }

    #[test]
    fn test_has_committed_write_after_excludes_own_txn() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        chain.commit(txn1, Timestamp(10));

        // txn1's own commit should be excluded
        assert!(!chain.has_committed_write_after(txn1, Timestamp(5)));
    }

    #[test]
    fn test_has_committed_write_after_ignores_aborted() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        chain.commit(txn1, Timestamp(10));

        chain.prepend(txn2, Some(row(vec![Datum::Int32(2)])));
        chain.abort(txn2);

        // txn3 at ts=5 sees txn1 commit (10 > 5)
        assert!(chain.has_committed_write_after(TxnId(3), Timestamp(5)));
        // txn3 at ts=15: txn1 at 10 <= 15, txn2 aborted  鈥?no conflict
        assert!(!chain.has_committed_write_after(TxnId(3), Timestamp(15)));
    }

    #[test]
    fn test_has_committed_write_after_empty_chain() {
        let chain = VersionChain::new();
        assert!(!chain.has_committed_write_after(TxnId(1), Timestamp(0)));
    }
