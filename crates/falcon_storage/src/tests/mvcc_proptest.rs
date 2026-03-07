use crate::mvcc::VersionChain;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::types::{Timestamp, TxnId};
use proptest::prelude::*;

fn row_i32(v: i32) -> OwnedRow {
    OwnedRow::new(vec![Datum::Int32(v)])
}

// ── Property: committed version is visible at and after commit_ts ───

proptest! {
    #[test]
    fn committed_version_visible_at_and_after_commit_ts(
        txn_id in 1u64..10_000,
        commit_ts in 1u64..1_000_000,
        read_offset in 0u64..1_000_000,
    ) {
        let chain = VersionChain::new();
        let txn = TxnId(txn_id);
        let cts = Timestamp(commit_ts);

        chain.prepend(txn, Some(row_i32(42)));
        chain.commit(txn, cts);

        let read_ts = Timestamp(commit_ts + read_offset);
        // Must be visible at commit_ts and any later timestamp
        prop_assert!(
            chain.read_committed(read_ts).is_some(),
            "committed version must be visible at ts={} (commit_ts={})",
            read_ts.0, commit_ts,
        );
    }
}

// ── Property: committed version is NOT visible before commit_ts ─────

proptest! {
    #[test]
    fn committed_version_invisible_before_commit_ts(
        txn_id in 1u64..10_000,
        commit_ts in 2u64..1_000_000,
        read_before in 1u64..1_000_000,
    ) {
        let commit_ts_val = commit_ts;
        let read_ts_val = read_before % (commit_ts_val - 1) + 1; // 1..commit_ts-1

        let chain = VersionChain::new();
        let txn = TxnId(txn_id);
        let cts = Timestamp(commit_ts_val);

        chain.prepend(txn, Some(row_i32(42)));
        chain.commit(txn, cts);

        prop_assert!(
            chain.read_committed(Timestamp(read_ts_val)).is_none(),
            "committed version must NOT be visible at ts={} (commit_ts={})",
            read_ts_val, commit_ts_val,
        );
    }
}

// ── Property: uncommitted version invisible to other txns ────────────

proptest! {
    #[test]
    fn uncommitted_version_invisible_to_others(
        writer_id in 1u64..10_000,
        reader_id in 10_001u64..20_000,
        read_ts in 1u64..1_000_000,
    ) {
        let chain = VersionChain::new();
        let writer = TxnId(writer_id);
        let reader = TxnId(reader_id);

        chain.prepend(writer, Some(row_i32(99)));
        // NOT committed

        // Other txn must not see it via read_committed
        prop_assert!(chain.read_committed(Timestamp(read_ts)).is_none());
        // Other txn must not see it via read_for_txn
        prop_assert!(chain.read_for_txn(reader, Timestamp(read_ts)).is_none());
        // Own txn sees it
        prop_assert!(chain.read_for_txn(writer, Timestamp(0)).is_some());
    }
}

// ── Property: aborted version is never visible ──────────────────────

proptest! {
    #[test]
    fn aborted_version_never_visible(
        txn_id in 1u64..10_000,
        read_ts in 1u64..1_000_000,
    ) {
        let chain = VersionChain::new();
        let txn = TxnId(txn_id);

        chain.prepend(txn, Some(row_i32(7)));
        chain.abort(txn);

        prop_assert!(chain.read_committed(Timestamp(read_ts)).is_none());
        prop_assert!(chain.read_for_txn(txn, Timestamp(read_ts)).is_none());
        prop_assert!(!chain.is_visible(txn, Timestamp(read_ts)));
    }
}

// ── Property: read_for_txn and is_visible agree ─────────────────────

proptest! {
    #[test]
    fn read_for_txn_and_is_visible_consistent(
        writer_id in 1u64..10_000,
        reader_id in 1u64..10_000,
        commit_ts in 1u64..1_000_000,
        read_ts in 1u64..1_000_000,
    ) {
        let chain = VersionChain::new();
        let writer = TxnId(writer_id);
        let reader = TxnId(reader_id);

        chain.prepend(writer, Some(row_i32(42)));
        chain.commit(writer, Timestamp(commit_ts));

        let has_row = chain.read_for_txn(reader, Timestamp(read_ts)).is_some();
        let visible = chain.is_visible(reader, Timestamp(read_ts));

        prop_assert_eq!(
            has_row, visible,
            "read_for_txn and is_visible must agree: has_row={}, visible={}, reader={}, read_ts={}, commit_ts={}",
            has_row, visible, reader_id, read_ts, commit_ts,
        );
    }
}

// ── Property: multi-version chain returns correct version per ts ────

proptest! {
    #[test]
    fn multi_version_returns_correct_version(
        n in 2usize..8,
        base_ts in 1u64..100_000,
        gap in 1u64..1000,
        query_offset in 0u64..8000,
    ) {
        let chain = VersionChain::new();
        let mut timestamps = Vec::new();

        for i in 0..n {
            let txn = TxnId(i as u64 + 1);
            let cts = Timestamp(base_ts + (i as u64) * gap);
            chain.prepend(txn, Some(row_i32(i as i32)));
            chain.commit(txn, cts);
            timestamps.push(cts);
        }

        let read_ts = Timestamp(base_ts + query_offset);

        let result = chain.read_committed(read_ts);

        // Find the expected version: latest committed with cts <= read_ts
        let expected_idx = timestamps
            .iter()
            .rposition(|&ts| ts <= read_ts);

        match expected_idx {
            Some(idx) => {
                prop_assert!(result.is_some(), "expected version {} at read_ts={}", idx, read_ts.0);
                let row = result.unwrap();
                prop_assert_eq!(
                    row.values[0].clone(),
                    Datum::Int32(idx as i32),
                    "wrong version at read_ts={}: expected v{}",
                    read_ts.0, idx,
                );
            }
            None => {
                prop_assert!(result.is_none(), "expected no version at read_ts={}", read_ts.0);
            }
        }
    }
}

// ── Property: GC preserves visibility at watermark ──────────────────

proptest! {
    #[test]
    fn gc_preserves_visibility_at_watermark(
        n in 2usize..6,
        base_ts in 1u64..100_000,
        gap in 1u64..1000,
        watermark_idx in 0usize..6,
    ) {
        let chain = VersionChain::new();
        let mut timestamps = Vec::new();

        for i in 0..n {
            let txn = TxnId(i as u64 + 1);
            let cts = Timestamp(base_ts + (i as u64) * gap);
            chain.prepend(txn, Some(row_i32(i as i32)));
            chain.commit(txn, cts);
            timestamps.push(cts);
        }

        let wm_idx = watermark_idx % n;
        let watermark = timestamps[wm_idx];

        chain.gc(watermark);

        // The latest version must still be visible
        let latest_ts = *timestamps.last().unwrap();
        let result = chain.read_committed(latest_ts);
        prop_assert!(result.is_some(), "latest version must survive GC");

        // Version at watermark must still be visible
        let at_wm = chain.read_committed(watermark);
        prop_assert!(at_wm.is_some(), "version at watermark must survive GC");
    }
}

// ── Property: tombstone makes row invisible ─────────────────────────

proptest! {
    #[test]
    fn tombstone_makes_row_invisible(
        insert_ts in 1u64..500_000,
        delete_ts_offset in 1u64..500_000,
        read_ts_offset in 0u64..500_000,
    ) {
        let chain = VersionChain::new();
        let delete_ts = insert_ts + delete_ts_offset;

        chain.prepend(TxnId(1), Some(row_i32(1)));
        chain.commit(TxnId(1), Timestamp(insert_ts));

        chain.prepend(TxnId(2), None); // tombstone
        chain.commit(TxnId(2), Timestamp(delete_ts));

        let read_ts = Timestamp(delete_ts + read_ts_offset);
        // After delete, read_committed returns None (tombstone)
        prop_assert!(
            chain.read_committed(read_ts).is_none(),
            "tombstone must make row invisible at ts={} (delete_ts={})",
            read_ts.0, delete_ts,
        );

        // Before delete, row is still visible
        if insert_ts < delete_ts {
            let mid_ts = Timestamp(insert_ts + (delete_ts - insert_ts) / 2);
            if mid_ts >= Timestamp(insert_ts) && mid_ts < Timestamp(delete_ts) {
                let before_delete = chain.read_committed(mid_ts);
                prop_assert!(
                    before_delete.is_some(),
                    "row must be visible before tombstone at ts={}",
                    mid_ts.0,
                );
            }
        }
    }
}
