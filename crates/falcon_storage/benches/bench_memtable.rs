use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use falcon_common::datum::{Datum, OwnedRow};
use falcon_storage::memtable::{encode_column_value, encode_pk, encode_pk_from_datums};

fn make_row(id: i64) -> OwnedRow {
    OwnedRow::new(vec![
        Datum::Int64(id),
        Datum::Text(format!("user_{id}")),
        Datum::Float64(id as f64 * 1.5),
        Datum::Boolean(id % 2 == 0),
    ])
}

fn bench_encode_pk_int64(c: &mut Criterion) {
    let row = make_row(42);
    let pk_indices = vec![0usize];
    c.bench_function("memtable/encode_pk_int64", |b| {
        b.iter(|| encode_pk(black_box(&row), black_box(&pk_indices)))
    });
}

fn bench_encode_pk_composite(c: &mut Criterion) {
    let row = make_row(42);
    let pk_indices = vec![0usize, 1];
    c.bench_function("memtable/encode_pk_composite_int64_text", |b| {
        b.iter(|| encode_pk(black_box(&row), black_box(&pk_indices)))
    });
}

fn bench_encode_pk_from_datums(c: &mut Criterion) {
    let d1 = Datum::Int64(42);
    let d2 = Datum::Text("alice".into());
    let datums: Vec<&Datum> = vec![&d1, &d2];
    c.bench_function("memtable/encode_pk_from_datums", |b| {
        b.iter(|| encode_pk_from_datums(black_box(&datums)))
    });
}

fn bench_encode_column_value_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/encode_column_value");

    let cases: Vec<(&str, Datum)> = vec![
        ("int32", Datum::Int32(42)),
        ("int64", Datum::Int64(123456789)),
        ("float64", Datum::Float64(3.14159)),
        ("text_short", Datum::Text("hello".into())),
        ("text_long", Datum::Text("a]".repeat(100))),
        ("boolean", Datum::Boolean(true)),
        ("null", Datum::Null),
        ("timestamp", Datum::Timestamp(1700000000_000_000)),
        ("date", Datum::Date(19723)),
    ];

    for (name, datum) in &cases {
        group.bench_with_input(BenchmarkId::new("type", name), datum, |b, datum| {
            b.iter(|| encode_column_value(black_box(datum)))
        });
    }
    group.finish();
}

fn bench_encode_pk_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/encode_pk_columns_scaling");
    for ncols in [1, 2, 4, 8] {
        let row = OwnedRow::new((0..ncols).map(|i| Datum::Int64(i as i64)).collect());
        let pk_indices: Vec<usize> = (0..ncols).collect();
        group.bench_with_input(BenchmarkId::from_parameter(ncols), &ncols, |b, _| {
            b.iter(|| encode_pk(black_box(&row), black_box(&pk_indices)))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_pk_int64,
    bench_encode_pk_composite,
    bench_encode_pk_from_datums,
    bench_encode_column_value_types,
    bench_encode_pk_scaling,
);
criterion_main!(benches);
