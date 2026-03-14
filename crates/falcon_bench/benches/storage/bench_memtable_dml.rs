// benches/storage/bench_memtable_dml.rs
//
// Storage-layer benchmarks: MemTable INSERT / UPDATE / DELETE / point-GET.
// No full TxnManager; uses raw storage engine calls for isolated measurement.
//
// Run: cargo bench --bench bench_memtable_dml

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::{ColumnId, DataType, IsolationLevel, TableId};
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::TxnManager;

// ── Setup helpers ────────────────────────────────────────────────────────────

fn bench_schema() -> TableSchema {
    TableSchema {
        name: "bench_t".into(),
        columns: vec![
            ColumnDef { id: ColumnId(0), name: "id".into(),      data_type: DataType::Int64,  nullable: false, default: None },
            ColumnDef { id: ColumnId(1), name: "balance".into(),  data_type: DataType::Int64,  nullable: false, default: None },
            ColumnDef { id: ColumnId(2), name: "name".into(),     data_type: DataType::Text,   nullable: true,  default: None },
        ],
        primary_key: vec![ColumnId(0)],
        secondary_indexes: vec![],
        table_id: TableId(1),
    }
}

fn make_row(id: i64) -> OwnedRow {
    OwnedRow::new(vec![
        Datum::Int64(id),
        Datum::Int64(id % 1_000_000),
        Datum::Text(format!("user_{id:010}")),
    ])
}

fn setup_engine_with_rows(n: u64) -> (StorageEngine, TxnManager, TableId) {
    let engine  = StorageEngine::new_in_memory();
    let txn_mgr = TxnManager::new();
    let schema  = bench_schema();
    let table_id = engine.create_table(&schema);
    for i in 0..n {
        let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
        let _   = engine.insert(table_id, make_row(i as i64), &txn);
        txn_mgr.commit(txn.txn_id);
    }
    (engine, txn_mgr, table_id)
}

// ── INSERT benchmarks ─────────────────────────────────────────────────────────

fn bench_insert_sequential(c: &mut Criterion) {
    let (engine, txn_mgr, table_id) = setup_engine_with_rows(0);
    let mut next_id = 0i64;
    c.bench_function("storage/insert_seq", |b| {
        b.iter(|| {
            let row = make_row(next_id);
            next_id += 1;
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let r   = engine.insert(black_box(table_id), black_box(row), &txn);
            txn_mgr.commit(txn.txn_id);
            black_box(r)
        })
    });
}

fn bench_insert_random(c: &mut Criterion) {
    let (engine, txn_mgr, table_id) = setup_engine_with_rows(0);
    let mut rng = falcon_bench::dataset::XorShift64::new(42);
    c.bench_function("storage/insert_rand", |b| {
        b.iter(|| {
            let id  = (rng.next() % 10_000_000) as i64;
            let row = make_row(id);
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let r   = engine.insert(black_box(table_id), black_box(row), &txn);
            txn_mgr.commit(txn.txn_id);
            black_box(r)
        })
    });
}

// ── point lookup benchmarks ───────────────────────────────────────────────────

fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/point_lookup_hit");
    for n_rows in [1_000u64, 10_000, 100_000] {
        let (engine, txn_mgr, table_id) = setup_engine_with_rows(n_rows);
        let pk = Datum::Int64((n_rows / 2) as i64); // middle key
        group.bench_with_input(
            BenchmarkId::new("rows", n_rows),
            &n_rows,
            |b, _| {
                b.iter(|| {
                    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                    let r   = engine.get_by_pk(black_box(table_id), black_box(&[&pk]), &txn);
                    txn_mgr.commit(txn.txn_id);
                    black_box(r)
                })
            },
        );
    }
    group.finish();
}

fn bench_point_lookup_miss(c: &mut Criterion) {
    let (engine, txn_mgr, table_id) = setup_engine_with_rows(100_000);
    let pk = Datum::Int64(9_999_999); // definitely not in table
    c.bench_function("storage/point_lookup_miss", |b| {
        b.iter(|| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let r   = engine.get_by_pk(black_box(table_id), black_box(&[&pk]), &txn);
            txn_mgr.commit(txn.txn_id);
            black_box(r)
        })
    });
}

// ── UPDATE benchmarks ─────────────────────────────────────────────────────────

fn bench_update_same_row(c: &mut Criterion) {
    let (engine, txn_mgr, table_id) = setup_engine_with_rows(1_000);
    let pk = Datum::Int64(42);
    c.bench_function("storage/update_same_row", |b| {
        b.iter(|| {
            let txn     = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let new_row = OwnedRow::new(vec![
                Datum::Int64(42),
                Datum::Int64(black_box(txn.txn_id.0 as i64 % 1_000_000)),
                Datum::Text("updated".into()),
            ]);
            let r = engine.update(black_box(table_id), &[&pk], black_box(new_row), &txn);
            txn_mgr.commit(txn.txn_id);
            black_box(r)
        })
    });
}

fn bench_update_uniform(c: &mut Criterion) {
    let (engine, txn_mgr, table_id) = setup_engine_with_rows(100_000);
    let mut rng = falcon_bench::dataset::XorShift64::new(1337);
    c.bench_function("storage/update_uniform", |b| {
        b.iter(|| {
            let id      = (rng.next() % 100_000) as i64;
            let pk      = Datum::Int64(id);
            let txn     = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let new_row = OwnedRow::new(vec![
                Datum::Int64(id),
                Datum::Int64(rng.next() as i64 % 1_000_000),
                Datum::Text("upd".into()),
            ]);
            let r = engine.update(black_box(table_id), &[&pk], black_box(new_row), &txn);
            txn_mgr.commit(txn.txn_id);
            black_box(r)
        })
    });
}

// ── DELETE benchmark ──────────────────────────────────────────────────────────

fn bench_delete(c: &mut Criterion) {
    // Repopulate before each iter_with_setup
    c.bench_function("storage/delete", |b| {
        b.iter_with_setup(
            || {
                let (engine, txn_mgr, table_id) = setup_engine_with_rows(100_000);
                let id = 50_000i64;
                (engine, txn_mgr, table_id, id)
            },
            |(engine, txn_mgr, table_id, id)| {
                let pk  = Datum::Int64(id);
                let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                let r   = engine.delete(black_box(table_id), &[&pk], &txn);
                txn_mgr.commit(txn.txn_id);
                black_box(r)
            },
        )
    });
}

criterion_group!(
    benches,
    bench_insert_sequential,
    bench_insert_random,
    bench_point_lookup,
    bench_point_lookup_miss,
    bench_update_same_row,
    bench_update_uniform,
    bench_delete,
);
criterion_main!(benches);
