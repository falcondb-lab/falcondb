// benches/micro/bench_codec.rs
//
// Micro benchmarks for protocol codec: encode_message_into / decode_message.
// Tests: DataRow (1/4/8/16 col), ErrorResponse, RowDescription, read_cstring.
//
// Run: cargo bench --bench bench_codec

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use falcon_protocol_pg::codec::{encode_message_into, BackendMessage, FieldDescription};

fn bench_encode_datarow(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro/encode_datarow");
    for ncols in [1usize, 4, 8, 16] {
        let values: Vec<Option<String>> = (0..ncols)
            .map(|i| Some(format!("value_{i:08}")))
            .collect();
        let msg = BackendMessage::DataRow { values };
        group.bench_with_input(
            BenchmarkId::new("cols", ncols),
            &msg,
            |b, msg| {
                let mut buf = BytesMut::with_capacity(512);
                b.iter(|| {
                    buf.clear();
                    encode_message_into(black_box(&mut buf), black_box(msg));
                });
            },
        );
    }
    group.finish();
}

fn bench_encode_datarow_null_values(c: &mut Criterion) {
    let values: Vec<Option<String>> = (0..8).map(|i| {
        if i % 2 == 0 { Some(format!("val_{i}")) } else { None }
    }).collect();
    let msg = BackendMessage::DataRow { values };
    c.bench_function("micro/encode_datarow_mixed_null", |b| {
        let mut buf = BytesMut::with_capacity(256);
        b.iter(|| {
            buf.clear();
            encode_message_into(black_box(&mut buf), black_box(&msg));
        });
    });
}

fn bench_encode_error_response(c: &mut Criterion) {
    let msg = BackendMessage::ErrorResponse {
        severity: "ERROR".into(),
        code: "23505".into(),
        message: "duplicate key value violates unique constraint \"bench_t_pkey\"".into(),
    };
    c.bench_function("micro/encode_error_response", |b| {
        let mut buf = BytesMut::with_capacity(256);
        b.iter(|| {
            buf.clear();
            encode_message_into(black_box(&mut buf), black_box(&msg));
        });
    });
}

fn bench_encode_row_description(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro/encode_row_description");
    for ncols in [1usize, 4, 8] {
        let fields: Vec<FieldDescription> = (0..ncols)
            .map(|i| FieldDescription {
                name:          format!("column_{i}"),
                table_oid:     0,
                column_attr:   i as i16,
                type_oid:      23, // INT4
                type_len:      4,
                type_modifier: -1,
                format_code:   0,
            })
            .collect();
        let msg = BackendMessage::RowDescription { fields };
        group.bench_with_input(
            BenchmarkId::new("cols", ncols),
            &msg,
            |b, msg| {
                let mut buf = BytesMut::with_capacity(512);
                b.iter(|| {
                    buf.clear();
                    encode_message_into(black_box(&mut buf), black_box(msg));
                });
            },
        );
    }
    group.finish();
}

fn bench_encode_ready_for_query(c: &mut Criterion) {
    let msg = BackendMessage::ReadyForQuery { txn_status: b'I' };
    c.bench_function("micro/encode_ready_for_query", |b| {
        let mut buf = BytesMut::with_capacity(8);
        b.iter(|| {
            buf.clear();
            encode_message_into(black_box(&mut buf), black_box(&msg));
        });
    });
}

fn bench_encode_command_complete(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro/encode_command_complete");
    for tag in ["SELECT 1", "INSERT 0 1", "UPDATE 42", "DELETE 1000000"] {
        let msg = BackendMessage::CommandComplete { tag: tag.to_string() };
        group.bench_with_input(
            BenchmarkId::new("tag", tag),
            &msg,
            |b, msg| {
                let mut buf = BytesMut::with_capacity(64);
                b.iter(|| {
                    buf.clear();
                    encode_message_into(black_box(&mut buf), black_box(msg));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_datarow,
    bench_encode_datarow_null_values,
    bench_encode_error_response,
    bench_encode_row_description,
    bench_encode_ready_for_query,
    bench_encode_command_complete,
);
criterion_main!(benches);
