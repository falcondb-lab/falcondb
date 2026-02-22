//! Encode/decode for the FalconDB Native Protocol.
//!
//! All multi-byte integers are little-endian. Every message is framed as:
//! `[msg_type: u8][length: u32 LE][payload: length bytes]`

use bytes::{BufMut, BytesMut};

use crate::error::NativeProtocolError;
use crate::types::*;

type Result<T> = std::result::Result<T, NativeProtocolError>;

// ── Helper: read/write primitives ────────────────────────────────────────

fn ensure(buf: &[u8], need: usize, ctx: &str) -> Result<()> {
    if buf.len() < need {
        return Err(NativeProtocolError::Truncated {
            expected: need,
            actual: buf.len(),
        });
    }
    let _ = ctx;
    Ok(())
}

fn read_u8(buf: &mut &[u8]) -> Result<u8> {
    ensure(buf, 1, "u8")?;
    let v = buf[0];
    *buf = &buf[1..];
    Ok(v)
}

fn read_u16(buf: &mut &[u8]) -> Result<u16> {
    ensure(buf, 2, "u16")?;
    let v = u16::from_le_bytes([buf[0], buf[1]]);
    *buf = &buf[2..];
    Ok(v)
}

fn read_u32(buf: &mut &[u8]) -> Result<u32> {
    ensure(buf, 4, "u32")?;
    let v = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    *buf = &buf[4..];
    Ok(v)
}

fn read_u64(buf: &mut &[u8]) -> Result<u64> {
    ensure(buf, 8, "u64")?;
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&buf[..8]);
    let v = u64::from_le_bytes(arr);
    *buf = &buf[8..];
    Ok(v)
}

fn read_i32(buf: &mut &[u8]) -> Result<i32> {
    ensure(buf, 4, "i32")?;
    let v = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    *buf = &buf[4..];
    Ok(v)
}

fn read_i64(buf: &mut &[u8]) -> Result<i64> {
    ensure(buf, 8, "i64")?;
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&buf[..8]);
    let v = i64::from_le_bytes(arr);
    *buf = &buf[8..];
    Ok(v)
}

fn read_i128(buf: &mut &[u8]) -> Result<i128> {
    ensure(buf, 16, "i128")?;
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&buf[..16]);
    let v = i128::from_le_bytes(arr);
    *buf = &buf[16..];
    Ok(v)
}

fn read_f64(buf: &mut &[u8]) -> Result<f64> {
    ensure(buf, 8, "f64")?;
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&buf[..8]);
    let v = f64::from_le_bytes(arr);
    *buf = &buf[8..];
    Ok(v)
}

fn read_bytes(buf: &mut &[u8], n: usize) -> Result<Vec<u8>> {
    ensure(buf, n, "bytes")?;
    let v = buf[..n].to_vec();
    *buf = &buf[n..];
    Ok(v)
}

fn read_fixed<const N: usize>(buf: &mut &[u8]) -> Result<[u8; N]> {
    ensure(buf, N, "fixed")?;
    let mut arr = [0u8; N];
    arr.copy_from_slice(&buf[..N]);
    *buf = &buf[N..];
    Ok(arr)
}

fn read_string_u16(buf: &mut &[u8], field: &str) -> Result<String> {
    let len = read_u16(buf)? as usize;
    let bytes = read_bytes(buf, len)?;
    String::from_utf8(bytes).map_err(|e| NativeProtocolError::InvalidUtf8 {
        field: field.to_string(),
        source: e,
    })
}

fn read_string_u32(buf: &mut &[u8], field: &str) -> Result<String> {
    let len = read_u32(buf)? as usize;
    let bytes = read_bytes(buf, len)?;
    String::from_utf8(bytes).map_err(|e| NativeProtocolError::InvalidUtf8 {
        field: field.to_string(),
        source: e,
    })
}

fn read_params(buf: &mut &[u8]) -> Result<Vec<Param>> {
    let n = read_u16(buf)? as usize;
    let mut params = Vec::with_capacity(n);
    for _ in 0..n {
        let key = read_string_u16(buf, "param_key")?;
        let value = read_string_u16(buf, "param_value")?;
        params.push(Param { key, value });
    }
    Ok(params)
}

fn write_string_u16(out: &mut BytesMut, s: &str) {
    out.put_u16_le(s.len() as u16);
    out.put_slice(s.as_bytes());
}

fn write_string_u32(out: &mut BytesMut, s: &str) {
    out.put_u32_le(s.len() as u32);
    out.put_slice(s.as_bytes());
}

fn write_params(out: &mut BytesMut, params: &[Param]) {
    out.put_u16_le(params.len() as u16);
    for p in params {
        write_string_u16(out, &p.key);
        write_string_u16(out, &p.value);
    }
}

// ── EncodedValue encode/decode ───────────────────────────────────────────

fn encode_value(out: &mut BytesMut, v: &EncodedValue) {
    match v {
        EncodedValue::Null => {}
        EncodedValue::Boolean(b) => out.put_u8(if *b { 1 } else { 0 }),
        EncodedValue::Int32(i) => out.put_i32_le(*i),
        EncodedValue::Int64(i) => out.put_i64_le(*i),
        EncodedValue::Float64(f) => out.put_f64_le(*f),
        EncodedValue::Text(s) => write_string_u32(out, s),
        EncodedValue::Timestamp(t) => out.put_i64_le(*t),
        EncodedValue::Date(d) => out.put_i32_le(*d),
        EncodedValue::Jsonb(s) => write_string_u32(out, s),
        EncodedValue::Decimal(mantissa, scale) => {
            out.put_u8(*scale);
            out.put_slice(&mantissa.to_le_bytes());
        }
        EncodedValue::Time(t) => out.put_i64_le(*t),
        EncodedValue::Interval(months, days, us) => {
            out.put_i32_le(*months);
            out.put_i32_le(*days);
            out.put_i64_le(*us);
        }
        EncodedValue::Uuid(u) => out.put_u128(*u),
        EncodedValue::Bytea(b) => {
            out.put_u32_le(b.len() as u32);
            out.put_slice(b);
        }
        EncodedValue::Array(elem_type, elems) => {
            out.put_u8(*elem_type);
            out.put_u32_le(elems.len() as u32);
            for e in elems {
                encode_value(out, e);
            }
        }
    }
}

fn decode_value(buf: &mut &[u8], type_id: u8) -> Result<EncodedValue> {
    match type_id {
        TYPE_NULL => Ok(EncodedValue::Null),
        TYPE_BOOLEAN => Ok(EncodedValue::Boolean(read_u8(buf)? != 0)),
        TYPE_INT32 => Ok(EncodedValue::Int32(read_i32(buf)?)),
        TYPE_INT64 => Ok(EncodedValue::Int64(read_i64(buf)?)),
        TYPE_FLOAT64 => Ok(EncodedValue::Float64(read_f64(buf)?)),
        TYPE_TEXT => Ok(EncodedValue::Text(read_string_u32(buf, "text_value")?)),
        TYPE_TIMESTAMP => Ok(EncodedValue::Timestamp(read_i64(buf)?)),
        TYPE_DATE => Ok(EncodedValue::Date(read_i32(buf)?)),
        TYPE_JSONB => Ok(EncodedValue::Jsonb(read_string_u32(buf, "jsonb_value")?)),
        TYPE_DECIMAL => {
            let scale = read_u8(buf)?;
            let mantissa = read_i128(buf)?;
            Ok(EncodedValue::Decimal(mantissa, scale))
        }
        TYPE_TIME => Ok(EncodedValue::Time(read_i64(buf)?)),
        TYPE_INTERVAL => {
            let months = read_i32(buf)?;
            let days = read_i32(buf)?;
            let us = read_i64(buf)?;
            Ok(EncodedValue::Interval(months, days, us))
        }
        TYPE_UUID => {
            let bytes = read_fixed::<16>(buf)?;
            Ok(EncodedValue::Uuid(u128::from_be_bytes(bytes)))
        }
        TYPE_BYTEA => {
            let len = read_u32(buf)? as usize;
            let data = read_bytes(buf, len)?;
            Ok(EncodedValue::Bytea(data))
        }
        TYPE_ARRAY => {
            let elem_type = read_u8(buf)?;
            let count = read_u32(buf)? as usize;
            let mut elems = Vec::with_capacity(count.min(65536));
            for _ in 0..count {
                elems.push(decode_value(buf, elem_type)?);
            }
            Ok(EncodedValue::Array(elem_type, elems))
        }
        _ => Err(NativeProtocolError::Corruption(format!(
            "unknown type_id: 0x{:02x}",
            type_id
        ))),
    }
}

fn value_type_id(v: &EncodedValue) -> u8 {
    match v {
        EncodedValue::Null => TYPE_NULL,
        EncodedValue::Boolean(_) => TYPE_BOOLEAN,
        EncodedValue::Int32(_) => TYPE_INT32,
        EncodedValue::Int64(_) => TYPE_INT64,
        EncodedValue::Float64(_) => TYPE_FLOAT64,
        EncodedValue::Text(_) => TYPE_TEXT,
        EncodedValue::Timestamp(_) => TYPE_TIMESTAMP,
        EncodedValue::Date(_) => TYPE_DATE,
        EncodedValue::Jsonb(_) => TYPE_JSONB,
        EncodedValue::Decimal(_, _) => TYPE_DECIMAL,
        EncodedValue::Time(_) => TYPE_TIME,
        EncodedValue::Interval(_, _, _) => TYPE_INTERVAL,
        EncodedValue::Uuid(_) => TYPE_UUID,
        EncodedValue::Bytea(_) => TYPE_BYTEA,
        EncodedValue::Array(_, _) => TYPE_ARRAY,
    }
}

// ── Null bitmap helpers ──────────────────────────────────────────────────

fn null_bitmap_size(num_cols: usize) -> usize {
    num_cols.div_ceil(8)
}

fn encode_null_bitmap(out: &mut BytesMut, values: &[EncodedValue]) {
    let nbytes = null_bitmap_size(values.len());
    let mut bitmap = vec![0u8; nbytes];
    for (i, v) in values.iter().enumerate() {
        if matches!(v, EncodedValue::Null) {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    out.put_slice(&bitmap);
}

fn decode_null_bitmap(buf: &mut &[u8], num_cols: usize) -> Result<Vec<bool>> {
    let nbytes = null_bitmap_size(num_cols);
    let bitmap = read_bytes(buf, nbytes)?;
    let mut is_null = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        is_null.push(bitmap[i / 8] & (1 << (i % 8)) != 0);
    }
    Ok(is_null)
}

// ── Row encode/decode ────────────────────────────────────────────────────

fn encode_row(out: &mut BytesMut, row: &EncodedRow, col_types: &[u8]) {
    encode_null_bitmap(out, &row.values);
    for (i, v) in row.values.iter().enumerate() {
        if !matches!(v, EncodedValue::Null) {
            encode_value(out, v);
        }
        let _ = i;
    }
    let _ = col_types;
}

fn decode_row(buf: &mut &[u8], col_types: &[u8]) -> Result<EncodedRow> {
    let num_cols = col_types.len();
    let is_null = decode_null_bitmap(buf, num_cols)?;
    let mut values = Vec::with_capacity(num_cols);
    for (i, &tid) in col_types.iter().enumerate() {
        if is_null[i] {
            values.push(EncodedValue::Null);
        } else {
            values.push(decode_value(buf, tid)?);
        }
    }
    Ok(EncodedRow { values })
}

// ── Message encode ───────────────────────────────────────────────────────

fn encode_payload(msg: &Message) -> BytesMut {
    let mut out = BytesMut::with_capacity(256);
    match msg {
        Message::ClientHello(h) => {
            out.put_u16_le(h.version_major);
            out.put_u16_le(h.version_minor);
            out.put_u64_le(h.feature_flags);
            write_string_u16(&mut out, &h.client_name);
            write_string_u16(&mut out, &h.database);
            write_string_u16(&mut out, &h.user);
            out.put_slice(&h.nonce);
            write_params(&mut out, &h.params);
        }
        Message::ServerHello(h) => {
            out.put_u16_le(h.version_major);
            out.put_u16_le(h.version_minor);
            out.put_u64_le(h.feature_flags);
            out.put_u64_le(h.server_epoch);
            out.put_u64_le(h.server_node_id);
            out.put_slice(&h.server_nonce);
            write_params(&mut out, &h.params);
        }
        Message::AuthRequest(a) => {
            out.put_u8(a.auth_method);
            out.put_slice(&a.challenge);
        }
        Message::AuthResponse(a) => {
            out.put_u8(a.auth_method);
            out.put_slice(&a.credential);
        }
        Message::AuthOk => {}
        Message::AuthFail(msg_str) => {
            write_string_u16(&mut out, msg_str);
        }
        Message::QueryRequest(q) => {
            out.put_u64_le(q.request_id);
            out.put_u64_le(q.epoch);
            write_string_u32(&mut out, &q.sql);
            out.put_u16_le(q.params.len() as u16);
            for p in &q.params {
                out.put_u8(value_type_id(p));
                encode_value(&mut out, p);
            }
            out.put_u32_le(q.session_flags);
        }
        Message::QueryResponse(r) => {
            out.put_u64_le(r.request_id);
            out.put_u16_le(r.columns.len() as u16);
            for c in &r.columns {
                write_string_u16(&mut out, &c.name);
                out.put_u8(c.type_id);
                out.put_u8(if c.nullable { 1 } else { 0 });
                out.put_u16_le(c.precision);
                out.put_u16_le(c.scale);
            }
            let col_types: Vec<u8> = r.columns.iter().map(|c| c.type_id).collect();
            out.put_u32_le(r.rows.len() as u32);
            for row in &r.rows {
                encode_row(&mut out, row, &col_types);
            }
            out.put_u64_le(r.rows_affected);
        }
        Message::ErrorResponse(e) => {
            encode_error_payload(&mut out, e);
        }
        Message::BatchRequest(b) => {
            out.put_u64_le(b.request_id);
            out.put_u64_le(b.epoch);
            write_string_u32(&mut out, &b.sql);
            out.put_u16_le(b.column_types.len() as u16);
            out.put_slice(&b.column_types);
            out.put_u32_le(b.rows.len() as u32);
            for row in &b.rows {
                encode_row(&mut out, row, &b.column_types);
            }
            out.put_u32_le(b.options);
        }
        Message::BatchResponse(b) => {
            out.put_u64_le(b.request_id);
            out.put_u32_le(b.counts.len() as u32);
            for &c in &b.counts {
                out.put_i64_le(c);
            }
            match &b.error {
                Some(e) => {
                    out.put_u8(1);
                    encode_error_payload(&mut out, e);
                }
                None => out.put_u8(0),
            }
        }
        Message::Ping
        | Message::Pong
        | Message::Disconnect
        | Message::DisconnectAck
        | Message::StartTls
        | Message::StartTlsAck => {}
    }
    out
}

fn encode_error_payload(out: &mut BytesMut, e: &ErrorResponse) {
    out.put_u64_le(e.request_id);
    out.put_u32_le(e.error_code);
    out.put_slice(&e.sqlstate);
    out.put_u8(if e.retryable { 1 } else { 0 });
    out.put_u64_le(e.server_epoch);
    write_string_u16(out, &e.message);
}

fn decode_error_payload(buf: &mut &[u8]) -> Result<ErrorResponse> {
    let request_id = read_u64(buf)?;
    let error_code = read_u32(buf)?;
    let sqlstate = read_fixed::<5>(buf)?;
    let retryable = read_u8(buf)? != 0;
    let server_epoch = read_u64(buf)?;
    let message = read_string_u16(buf, "error_message")?;
    Ok(ErrorResponse {
        request_id,
        error_code,
        sqlstate,
        retryable,
        server_epoch,
        message,
    })
}

/// Encode a `Message` into a framed byte buffer (header + payload).
pub fn encode_message(msg: &Message) -> BytesMut {
    let payload = encode_payload(msg);
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_SIZE + payload.len());
    frame.put_u8(msg.msg_type());
    frame.put_u32_le(payload.len() as u32);
    frame.put_slice(&payload);
    frame
}

/// Decode a `Message` from a framed byte buffer.
///
/// The input must contain the full frame (header + payload).
/// Returns `(message, bytes_consumed)`.
pub fn decode_message(input: &[u8]) -> Result<(Message, usize)> {
    if input.len() < FRAME_HEADER_SIZE {
        return Err(NativeProtocolError::Truncated {
            expected: FRAME_HEADER_SIZE,
            actual: input.len(),
        });
    }

    let msg_type = input[0];
    let length = u32::from_le_bytes([input[1], input[2], input[3], input[4]]);

    if length > MAX_FRAME_SIZE {
        return Err(NativeProtocolError::FrameTooLarge {
            size: length,
            max: MAX_FRAME_SIZE,
        });
    }

    let total = FRAME_HEADER_SIZE + length as usize;
    if input.len() < total {
        return Err(NativeProtocolError::Truncated {
            expected: total,
            actual: input.len(),
        });
    }

    let payload = &input[FRAME_HEADER_SIZE..total];
    let mut buf: &[u8] = payload;

    let msg = match msg_type {
        MSG_CLIENT_HELLO => {
            let version_major = read_u16(&mut buf)?;
            let version_minor = read_u16(&mut buf)?;
            let feature_flags = read_u64(&mut buf)?;
            let client_name = read_string_u16(&mut buf, "client_name")?;
            let database = read_string_u16(&mut buf, "database")?;
            let user = read_string_u16(&mut buf, "user")?;
            let nonce = read_fixed::<16>(&mut buf)?;
            let params = read_params(&mut buf)?;
            Message::ClientHello(ClientHello {
                version_major,
                version_minor,
                feature_flags,
                client_name,
                database,
                user,
                nonce,
                params,
            })
        }
        MSG_SERVER_HELLO => {
            let version_major = read_u16(&mut buf)?;
            let version_minor = read_u16(&mut buf)?;
            let feature_flags = read_u64(&mut buf)?;
            let server_epoch = read_u64(&mut buf)?;
            let server_node_id = read_u64(&mut buf)?;
            let server_nonce = read_fixed::<16>(&mut buf)?;
            let params = read_params(&mut buf)?;
            Message::ServerHello(ServerHello {
                version_major,
                version_minor,
                feature_flags,
                server_epoch,
                server_node_id,
                server_nonce,
                params,
            })
        }
        MSG_AUTH_REQUEST => {
            let auth_method = read_u8(&mut buf)?;
            let challenge = buf.to_vec();
            Message::AuthRequest(AuthRequest {
                auth_method,
                challenge,
            })
        }
        MSG_AUTH_RESPONSE => {
            let auth_method = read_u8(&mut buf)?;
            let credential = buf.to_vec();
            Message::AuthResponse(AuthResponse {
                auth_method,
                credential,
            })
        }
        MSG_AUTH_OK => Message::AuthOk,
        MSG_AUTH_FAIL => {
            let reason = if buf.is_empty() {
                String::new()
            } else {
                read_string_u16(&mut buf, "auth_fail_reason")?
            };
            Message::AuthFail(reason)
        }
        MSG_QUERY_REQUEST => {
            let request_id = read_u64(&mut buf)?;
            let epoch = read_u64(&mut buf)?;
            let sql = read_string_u32(&mut buf, "sql")?;
            let num_params = read_u16(&mut buf)? as usize;
            let mut params = Vec::with_capacity(num_params);
            for _ in 0..num_params {
                let tid = read_u8(&mut buf)?;
                params.push(decode_value(&mut buf, tid)?);
            }
            let session_flags = read_u32(&mut buf)?;
            Message::QueryRequest(QueryRequest {
                request_id,
                epoch,
                sql,
                params,
                session_flags,
            })
        }
        MSG_QUERY_RESPONSE => {
            let request_id = read_u64(&mut buf)?;
            let num_columns = read_u16(&mut buf)? as usize;
            let mut columns = Vec::with_capacity(num_columns);
            for _ in 0..num_columns {
                let name = read_string_u16(&mut buf, "column_name")?;
                let type_id = read_u8(&mut buf)?;
                let nullable = read_u8(&mut buf)? != 0;
                let precision = read_u16(&mut buf)?;
                let scale = read_u16(&mut buf)?;
                columns.push(ColumnMeta {
                    name,
                    type_id,
                    nullable,
                    precision,
                    scale,
                });
            }
            let col_types: Vec<u8> = columns.iter().map(|c| c.type_id).collect();
            let num_rows = read_u32(&mut buf)? as usize;
            let mut rows = Vec::with_capacity(num_rows.min(65536));
            for _ in 0..num_rows {
                rows.push(decode_row(&mut buf, &col_types)?);
            }
            let rows_affected = read_u64(&mut buf)?;
            Message::QueryResponse(QueryResponse {
                request_id,
                columns,
                rows,
                rows_affected,
            })
        }
        MSG_ERROR_RESPONSE => Message::ErrorResponse(decode_error_payload(&mut buf)?),
        MSG_BATCH_REQUEST => {
            let request_id = read_u64(&mut buf)?;
            let epoch = read_u64(&mut buf)?;
            let sql = read_string_u32(&mut buf, "batch_sql")?;
            let num_columns = read_u16(&mut buf)? as usize;
            let column_types = read_bytes(&mut buf, num_columns)?;
            let num_rows = read_u32(&mut buf)? as usize;
            let mut rows = Vec::with_capacity(num_rows.min(65536));
            for _ in 0..num_rows {
                rows.push(decode_row(&mut buf, &column_types)?);
            }
            let options = read_u32(&mut buf)?;
            Message::BatchRequest(BatchRequest {
                request_id,
                epoch,
                sql,
                column_types,
                rows,
                options,
            })
        }
        MSG_BATCH_RESPONSE => {
            let request_id = read_u64(&mut buf)?;
            let num_counts = read_u32(&mut buf)? as usize;
            let mut counts = Vec::with_capacity(num_counts.min(65536));
            for _ in 0..num_counts {
                counts.push(read_i64(&mut buf)?);
            }
            let has_error = read_u8(&mut buf)? != 0;
            let error = if has_error {
                Some(decode_error_payload(&mut buf)?)
            } else {
                None
            };
            Message::BatchResponse(BatchResponse {
                request_id,
                counts,
                error,
            })
        }
        MSG_PING => Message::Ping,
        MSG_PONG => Message::Pong,
        MSG_DISCONNECT => Message::Disconnect,
        MSG_DISCONNECT_ACK => Message::DisconnectAck,
        MSG_START_TLS => Message::StartTls,
        MSG_START_TLS_ACK => Message::StartTlsAck,
        _ => return Err(NativeProtocolError::UnknownMessageType(msg_type)),
    };

    Ok((msg, total))
}

// ── Datum conversion helpers ─────────────────────────────────────────────

use falcon_common::datum::Datum;
use falcon_common::types::DataType;

/// Convert a `Datum` to an `EncodedValue`.
pub fn datum_to_encoded(d: &Datum) -> EncodedValue {
    match d {
        Datum::Null => EncodedValue::Null,
        Datum::Boolean(b) => EncodedValue::Boolean(*b),
        Datum::Int32(i) => EncodedValue::Int32(*i),
        Datum::Int64(i) => EncodedValue::Int64(*i),
        Datum::Float64(f) => EncodedValue::Float64(*f),
        Datum::Text(s) => EncodedValue::Text(s.clone()),
        Datum::Timestamp(t) => EncodedValue::Timestamp(*t),
        Datum::Date(d) => EncodedValue::Date(*d),
        Datum::Jsonb(v) => EncodedValue::Jsonb(v.to_string()),
        Datum::Decimal(m, s) => EncodedValue::Decimal(*m, *s),
        Datum::Time(t) => EncodedValue::Time(*t),
        Datum::Interval(months, days, us) => EncodedValue::Interval(*months, *days, *us),
        Datum::Uuid(u) => EncodedValue::Uuid(*u),
        Datum::Bytea(b) => EncodedValue::Bytea(b.clone()),
        Datum::Array(elems) => {
            let encoded: Vec<EncodedValue> = elems.iter().map(datum_to_encoded).collect();
            let elem_type = encoded.first().map(value_type_id).unwrap_or(TYPE_NULL);
            EncodedValue::Array(elem_type, encoded)
        }
    }
}

/// Convert an `EncodedValue` to a `Datum`.
pub fn encoded_to_datum(v: &EncodedValue) -> Datum {
    match v {
        EncodedValue::Null => Datum::Null,
        EncodedValue::Boolean(b) => Datum::Boolean(*b),
        EncodedValue::Int32(i) => Datum::Int32(*i),
        EncodedValue::Int64(i) => Datum::Int64(*i),
        EncodedValue::Float64(f) => Datum::Float64(*f),
        EncodedValue::Text(s) => Datum::Text(s.clone()),
        EncodedValue::Timestamp(t) => Datum::Timestamp(*t),
        EncodedValue::Date(d) => Datum::Date(*d),
        EncodedValue::Jsonb(s) => {
            Datum::Jsonb(serde_json::from_str(s).unwrap_or(serde_json::Value::Null))
        }
        EncodedValue::Decimal(m, s) => Datum::Decimal(*m, *s),
        EncodedValue::Time(t) => Datum::Time(*t),
        EncodedValue::Interval(months, days, us) => Datum::Interval(*months, *days, *us),
        EncodedValue::Uuid(u) => Datum::Uuid(*u),
        EncodedValue::Bytea(b) => Datum::Bytea(b.clone()),
        EncodedValue::Array(_, elems) => Datum::Array(elems.iter().map(encoded_to_datum).collect()),
    }
}

/// Convert a `DataType` to a native protocol type_id.
pub fn datatype_to_type_id(dt: &DataType) -> u8 {
    match dt {
        DataType::Boolean => TYPE_BOOLEAN,
        DataType::Int32 => TYPE_INT32,
        DataType::Int64 => TYPE_INT64,
        DataType::Float64 => TYPE_FLOAT64,
        DataType::Text => TYPE_TEXT,
        DataType::Timestamp => TYPE_TIMESTAMP,
        DataType::Date => TYPE_DATE,
        DataType::Jsonb => TYPE_JSONB,
        DataType::Decimal(_, _) => TYPE_DECIMAL,
        DataType::Time => TYPE_TIME,
        DataType::Interval => TYPE_INTERVAL,
        DataType::Uuid => TYPE_UUID,
        DataType::Bytea => TYPE_BYTEA,
        DataType::Array(_) => TYPE_ARRAY,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &Message) -> Message {
        let encoded = encode_message(msg);
        let (decoded, consumed) = decode_message(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        decoded
    }

    #[test]
    fn test_ping_pong() {
        assert_eq!(roundtrip(&Message::Ping), Message::Ping);
        assert_eq!(roundtrip(&Message::Pong), Message::Pong);
    }

    #[test]
    fn test_disconnect() {
        assert_eq!(roundtrip(&Message::Disconnect), Message::Disconnect);
        assert_eq!(roundtrip(&Message::DisconnectAck), Message::DisconnectAck);
    }

    #[test]
    fn test_start_tls() {
        assert_eq!(roundtrip(&Message::StartTls), Message::StartTls);
        assert_eq!(roundtrip(&Message::StartTlsAck), Message::StartTlsAck);
    }

    #[test]
    fn test_auth_ok() {
        assert_eq!(roundtrip(&Message::AuthOk), Message::AuthOk);
    }

    #[test]
    fn test_auth_fail() {
        let msg = Message::AuthFail("bad password".into());
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_client_hello_roundtrip() {
        let msg = Message::ClientHello(ClientHello {
            version_major: PROTOCOL_VERSION_MAJOR,
            version_minor: PROTOCOL_VERSION_MINOR,
            feature_flags: FEATURE_BATCH_INGEST | FEATURE_PIPELINE,
            client_name: "falcondb-jdbc/0.1".into(),
            database: "testdb".into(),
            user: "admin".into(),
            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            params: vec![
                Param {
                    key: "app".into(),
                    value: "myapp".into(),
                },
                Param {
                    key: "timeout".into(),
                    value: "30000".into(),
                },
            ],
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_server_hello_roundtrip() {
        let msg = Message::ServerHello(ServerHello {
            version_major: 0,
            version_minor: 1,
            feature_flags: FEATURE_EPOCH_FENCING | FEATURE_TLS,
            server_epoch: 42,
            server_node_id: 7,
            server_nonce: [0xAA; 16],
            params: vec![Param {
                key: "server_version".into(),
                value: "1.0.0-rc.1".into(),
            }],
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_auth_request_roundtrip() {
        let msg = Message::AuthRequest(AuthRequest {
            auth_method: AUTH_PASSWORD,
            challenge: vec![0xDE, 0xAD, 0xBE, 0xEF],
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_auth_response_roundtrip() {
        let msg = Message::AuthResponse(AuthResponse {
            auth_method: AUTH_TOKEN,
            credential: b"my-secret-token".to_vec(),
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_query_request_no_params() {
        let msg = Message::QueryRequest(QueryRequest {
            request_id: 1,
            epoch: 10,
            sql: "SELECT 1".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_query_request_with_params() {
        let msg = Message::QueryRequest(QueryRequest {
            request_id: 99,
            epoch: 0,
            sql: "SELECT * FROM t WHERE id = ? AND name = ?".into(),
            params: vec![EncodedValue::Int64(42), EncodedValue::Text("hello".into())],
            session_flags: 0,
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_query_response_empty() {
        let msg = Message::QueryResponse(QueryResponse {
            request_id: 1,
            columns: vec![ColumnMeta {
                name: "id".into(),
                type_id: TYPE_INT64,
                nullable: false,
                precision: 0,
                scale: 0,
            }],
            rows: vec![],
            rows_affected: 0,
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_query_response_with_rows() {
        let msg = Message::QueryResponse(QueryResponse {
            request_id: 5,
            columns: vec![
                ColumnMeta {
                    name: "id".into(),
                    type_id: TYPE_INT64,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "name".into(),
                    type_id: TYPE_TEXT,
                    nullable: true,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "active".into(),
                    type_id: TYPE_BOOLEAN,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
            ],
            rows: vec![
                EncodedRow {
                    values: vec![
                        EncodedValue::Int64(1),
                        EncodedValue::Text("alice".into()),
                        EncodedValue::Boolean(true),
                    ],
                },
                EncodedRow {
                    values: vec![
                        EncodedValue::Int64(2),
                        EncodedValue::Null,
                        EncodedValue::Boolean(false),
                    ],
                },
            ],
            rows_affected: 0,
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_error_response_roundtrip() {
        let msg = Message::ErrorResponse(ErrorResponse {
            request_id: 7,
            error_code: ERR_FENCED_EPOCH,
            sqlstate: *b"42000",
            retryable: true,
            server_epoch: 99,
            message: "epoch mismatch: client=10, server=99".into(),
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_batch_request_roundtrip() {
        let msg = Message::BatchRequest(BatchRequest {
            request_id: 100,
            epoch: 5,
            sql: "INSERT INTO t (id, name) VALUES (?, ?)".into(),
            column_types: vec![TYPE_INT64, TYPE_TEXT],
            rows: vec![
                EncodedRow {
                    values: vec![EncodedValue::Int64(1), EncodedValue::Text("a".into())],
                },
                EncodedRow {
                    values: vec![EncodedValue::Int64(2), EncodedValue::Text("b".into())],
                },
                EncodedRow {
                    values: vec![EncodedValue::Int64(3), EncodedValue::Null],
                },
            ],
            options: 0,
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_batch_response_ok() {
        let msg = Message::BatchResponse(BatchResponse {
            request_id: 100,
            counts: vec![1, 1, 1],
            error: None,
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_batch_response_with_error() {
        let msg = Message::BatchResponse(BatchResponse {
            request_id: 100,
            counts: vec![1, -1],
            error: Some(ErrorResponse {
                request_id: 100,
                error_code: ERR_INTERNAL_ERROR,
                sqlstate: *b"XX000",
                retryable: false,
                server_epoch: 5,
                message: "constraint violation on row 2".into(),
            }),
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_all_value_types() {
        let msg = Message::QueryResponse(QueryResponse {
            request_id: 42,
            columns: vec![
                ColumnMeta {
                    name: "c_bool".into(),
                    type_id: TYPE_BOOLEAN,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_i32".into(),
                    type_id: TYPE_INT32,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_i64".into(),
                    type_id: TYPE_INT64,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_f64".into(),
                    type_id: TYPE_FLOAT64,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_text".into(),
                    type_id: TYPE_TEXT,
                    nullable: true,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_ts".into(),
                    type_id: TYPE_TIMESTAMP,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_date".into(),
                    type_id: TYPE_DATE,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_jsonb".into(),
                    type_id: TYPE_JSONB,
                    nullable: true,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_dec".into(),
                    type_id: TYPE_DECIMAL,
                    nullable: false,
                    precision: 10,
                    scale: 2,
                },
                ColumnMeta {
                    name: "c_time".into(),
                    type_id: TYPE_TIME,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_interval".into(),
                    type_id: TYPE_INTERVAL,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_uuid".into(),
                    type_id: TYPE_UUID,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                },
                ColumnMeta {
                    name: "c_bytea".into(),
                    type_id: TYPE_BYTEA,
                    nullable: true,
                    precision: 0,
                    scale: 0,
                },
            ],
            rows: vec![EncodedRow {
                values: vec![
                    EncodedValue::Boolean(true),
                    EncodedValue::Int32(42),
                    EncodedValue::Int64(1234567890),
                    EncodedValue::Float64(3.14159),
                    EncodedValue::Text("hello world".into()),
                    EncodedValue::Timestamp(1700000000_000_000),
                    EncodedValue::Date(19700),
                    EncodedValue::Jsonb(r#"{"key":"value"}"#.into()),
                    EncodedValue::Decimal(12345, 2),
                    EncodedValue::Time(43200_000_000),
                    EncodedValue::Interval(1, 15, 3600_000_000),
                    EncodedValue::Uuid(0x550e8400_e29b_41d4_a716_446655440000),
                    EncodedValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]),
                ],
            }],
            rows_affected: 0,
        });
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn test_frame_too_large() {
        let mut bad = BytesMut::new();
        bad.put_u8(MSG_PING);
        bad.put_u32_le(MAX_FRAME_SIZE + 1);
        let err = decode_message(&bad).unwrap_err();
        assert!(matches!(err, NativeProtocolError::FrameTooLarge { .. }));
    }

    #[test]
    fn test_truncated_header() {
        let err = decode_message(&[0x01, 0x00]).unwrap_err();
        assert!(matches!(err, NativeProtocolError::Truncated { .. }));
    }

    #[test]
    fn test_unknown_message_type() {
        let mut bad = BytesMut::new();
        bad.put_u8(0x99);
        bad.put_u32_le(0);
        let err = decode_message(&bad).unwrap_err();
        assert!(matches!(err, NativeProtocolError::UnknownMessageType(0x99)));
    }

    #[test]
    fn test_datum_conversion_roundtrip() {
        let datums = vec![
            Datum::Null,
            Datum::Boolean(true),
            Datum::Int32(-42),
            Datum::Int64(9999999),
            Datum::Float64(2.718),
            Datum::Text("test".into()),
            Datum::Timestamp(1700000000),
            Datum::Date(19000),
            Datum::Decimal(99999, 3),
            Datum::Time(43200_000_000),
            Datum::Interval(2, 10, 7200_000_000),
            Datum::Uuid(0x12345678_1234_1234_1234_123456789abc),
            Datum::Bytea(vec![1, 2, 3]),
        ];
        for d in &datums {
            let encoded = datum_to_encoded(d);
            let back = encoded_to_datum(&encoded);
            // Compare via debug since Datum doesn't impl PartialEq for Float64
            assert_eq!(format!("{:?}", d), format!("{:?}", back));
        }
    }

    #[test]
    fn test_golden_ping_bytes() {
        let encoded = encode_message(&Message::Ping);
        assert_eq!(encoded.as_ref(), &[MSG_PING, 0, 0, 0, 0]);
    }

    #[test]
    fn test_golden_pong_bytes() {
        let encoded = encode_message(&Message::Pong);
        assert_eq!(encoded.as_ref(), &[MSG_PONG, 0, 0, 0, 0]);
    }

    #[test]
    fn test_golden_auth_ok_bytes() {
        let encoded = encode_message(&Message::AuthOk);
        assert_eq!(encoded.as_ref(), &[MSG_AUTH_OK, 0, 0, 0, 0]);
    }

    #[test]
    fn test_golden_disconnect_bytes() {
        let encoded = encode_message(&Message::Disconnect);
        assert_eq!(encoded.as_ref(), &[MSG_DISCONNECT, 0, 0, 0, 0]);
    }

    #[test]
    fn test_golden_query_select_1() {
        let msg = Message::QueryRequest(QueryRequest {
            request_id: 1,
            epoch: 0,
            sql: "SELECT 1".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        });
        let encoded = encode_message(&msg);
        let (decoded, consumed) = decode_message(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        if let Message::QueryRequest(q) = decoded {
            assert_eq!(q.request_id, 1);
            assert_eq!(q.epoch, 0);
            assert_eq!(q.sql, "SELECT 1");
            assert!(q.params.is_empty());
            assert_eq!(q.session_flags, SESSION_AUTOCOMMIT);
        } else {
            panic!("expected QueryRequest");
        }
    }

    #[test]
    fn test_golden_error_fenced_epoch() {
        let msg = Message::ErrorResponse(ErrorResponse {
            request_id: 0,
            error_code: ERR_FENCED_EPOCH,
            sqlstate: *b"F0001",
            retryable: true,
            server_epoch: 100,
            message: "stale epoch".into(),
        });
        let encoded = encode_message(&msg);
        let (decoded, _) = decode_message(&encoded).unwrap();
        if let Message::ErrorResponse(e) = decoded {
            assert_eq!(e.error_code, ERR_FENCED_EPOCH);
            assert!(e.retryable);
            assert_eq!(e.server_epoch, 100);
        } else {
            panic!("expected ErrorResponse");
        }
    }
}
