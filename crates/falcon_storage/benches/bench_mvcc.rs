use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::types::{Timestamp, TxnId};
use falcon_storage::mvcc::VersionChain;

fn make_row(id: i64) -> OwnedRow {
    OwnedRow::new(vec![
        Datum::Int64(id),
        Datum::Text(format!("user_{id}")),
        Datum::Float64(id as f64 * 1.5),
        Datum::Boolean(id % 2 == 0),
    ])
}

fn bench_version_chain_prepend(c: &mut Criterion) {
    c.bench_function("mvcc/prepend", |b| {
        b.iter_with_setup(
            || VersionChain::new(),
            |chain| {
                chain.prepend(TxnId(1), Some(make_row(1)));
                black_box(&chain);
            },
        )
    });
}

fn bench_version_chain_commit(c: &mut Criterion) {
    c.bench_function("mvcc/commit_single", |b| {
        b.iter_with_setup(
            || {
                let chain = VersionChain::new();
                chain.prepend(TxnId(1), Some(make_row(1)));
                chain
            },
            |chain| {
                chain.commit(TxnId(1), Timestamp(100));
                black_box(&chain);
            },
        )
    });
}

fn bench_read_committed_fast_path(c: &mut Criterion) {
    let chain = VersionChain::new();
    chain.prepend(TxnId(1), Some(make_row(1)));
    chain.commit(TxnId(1), Timestamp(100));

    c.bench_function("mvcc/read_committed_fast_path", |b| {
        b.iter(|| {
            let row = chain.read_committed(black_box(Timestamp(200)));
            black_box(row);
        })
    });
}

fn bench_read_committed_chain_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc/read_committed_chain_depth");
    for depth in [1, 5, 10, 20] {
        let chain = VersionChain::new();
        for i in 0..depth {
            let txn = TxnId(i as u64 + 1);
            chain.prepend(txn, Some(make_row(i as i64)));
            chain.commit(txn, Timestamp(100 + i as u64));
        }
        group.bench_with_input(BenchmarkId::from_parameter(depth), &chain, |b, chain| {
            b.iter(|| {
                let row = chain.read_committed(black_box(Timestamp(200)));
                black_box(row);
            })
        });
    }
    group.finish();
}

fn bench_read_for_txn_own_write(c: &mut Criterion) {
    let chain = VersionChain::new();
    chain.prepend(TxnId(1), Some(make_row(1)));

    c.bench_function("mvcc/read_for_txn_own_write", |b| {
        b.iter(|| {
            let row = chain.read_for_txn(black_box(TxnId(1)), black_box(Timestamp(200)));
            black_box(row);
        })
    });
}

fn bench_abort(c: &mut Criterion) {
    c.bench_function("mvcc/abort", |b| {
        b.iter_with_setup(
            || {
                let chain = VersionChain::new();
                chain.prepend(TxnId(1), Some(make_row(1)));
                chain.prepend(TxnId(2), Some(make_row(2)));
                chain
            },
            |chain| {
                chain.abort(TxnId(2));
                black_box(&chain);
            },
        )
    });
}

fn bench_gc(c: &mut Criterion) {
    c.bench_function("mvcc/gc_10_versions", |b| {
        b.iter_with_setup(
            || {
                let chain = VersionChain::new();
                for i in 0..10u64 {
                    let txn = TxnId(i + 1);
                    chain.prepend(txn, Some(make_row(i as i64)));
                    chain.commit(txn, Timestamp(100 + i));
                }
                chain
            },
            |chain| {
                let result = chain.gc(Timestamp(108));
                black_box(result);
            },
        )
    });
}

criterion_group!(
    benches,
    bench_version_chain_prepend,
    bench_version_chain_commit,
    bench_read_committed_fast_path,
    bench_read_committed_chain_depth,
    bench_read_for_txn_own_write,
    bench_abort,
    bench_gc,
);
criterion_main!(benches);
