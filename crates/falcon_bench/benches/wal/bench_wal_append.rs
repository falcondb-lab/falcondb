// benches/wal/bench_wal_append.rs
//
// WAL append + group-commit benchmarks.
// Uses a temp directory for real I/O; skips if no NVMe detected (uses tmpfs).
//
// Run: cargo bench --bench bench_wal_append

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;

fn bench_wal_append_single(c: &mut Criterion) {
    let dir = tempdir_or_skip();
    let wal = falcon_storage::wal::WalWriter::open(&dir).expect("open WAL");

    c.bench_function("wal/append_single", |b| {
        b.iter(|| {
            let record = falcon_storage::wal::WalRecord::Insert {
                table_id: falcon_common::types::TableId(1),
                txn_id:   falcon_common::types::TxnId(1),
                row:      bench_row(42),
            };
            black_box(wal.append(black_box(record)).expect("append"))
        })
    });
}

fn bench_wal_group_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal/group_commit");
    group.measurement_time(Duration::from_secs(5));

    for batch in [1usize, 8, 64] {
        let dir = tempdir_or_skip();
        let wal = falcon_storage::wal::WalWriter::open(&dir).expect("open WAL");

        group.bench_with_input(
            BenchmarkId::new("batch", batch),
            &batch,
            |b, &batch| {
                b.iter(|| {
                    for i in 0..batch {
                        let record = falcon_storage::wal::WalRecord::Insert {
                            table_id: falcon_common::types::TableId(1),
                            txn_id:   falcon_common::types::TxnId(i as u64),
                            row:      bench_row(i as i64),
                        };
                        wal.append(record).expect("append");
                    }
                    wal.flush().expect("flush");
                })
            },
        );
    }
    group.finish();
}

fn bench_row(id: i64) -> falcon_common::datum::OwnedRow {
    falcon_common::datum::OwnedRow::new(vec![
        falcon_common::datum::Datum::Int64(id),
        falcon_common::datum::Datum::Int64(id * 17 % 1_000_000),
        falcon_common::datum::Datum::Text(format!("name_{id}")),
    ])
}

fn tempdir_or_skip() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("falcon_wal_bench_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

criterion_group!(benches, bench_wal_append_single, bench_wal_group_commit);
criterion_main!(benches);
