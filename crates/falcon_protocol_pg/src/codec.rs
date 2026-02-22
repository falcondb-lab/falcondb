use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;

/// Raw PG frontend (client→server) message types.
#[derive(Debug)]
pub enum FrontendMessage {
    /// Initial startup message (no type byte).
    Startup {
        version: i32,
        params: HashMap<String, String>,
    },
    /// SSL request (special startup message).
    SslRequest,
    /// Password response during auth.
    PasswordMessage(String),
    /// Simple query ('Q').
    Query(String),
    /// Parse ('P') — extended query.
    Parse {
        name: String,
        query: String,
        param_types: Vec<i32>,
    },
    /// Bind ('B') — extended query.
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    /// Describe ('D').
    Describe { kind: u8, name: String },
    /// Execute ('E').
    Execute { portal: String, max_rows: i32 },
    /// Sync ('S').
    Sync,
    /// Close ('C').
    Close { kind: u8, name: String },
    /// Flush ('H').
    Flush,
    /// Terminate ('X').
    Terminate,
    /// CopyData ('d') — data row during COPY.
    CopyData(Vec<u8>),
    /// CopyDone ('c') — end of COPY data.
    CopyDone,
    /// CopyFail ('f') — client aborts COPY.
    CopyFail(String),
    /// Cancel request (special startup-phase message with process_id and secret_key).
    CancelRequest { process_id: i32, secret_key: i32 },
    /// SASL initial response ('p') — mechanism name + initial client data.
    SASLInitialResponse { mechanism: String, data: Vec<u8> },
    /// SASL response ('p') — subsequent client data.
    SASLResponse { data: Vec<u8> },
}

/// Raw PG backend (server→client) message types.
#[derive(Debug)]
pub enum BackendMessage {
    /// Authentication request.
    AuthenticationOk,
    AuthenticationMd5Password {
        salt: [u8; 4],
    },
    AuthenticationCleartextPassword,
    /// AuthenticationSASL (type 10) — list of SASL mechanisms.
    AuthenticationSASL {
        mechanisms: Vec<String>,
    },
    /// AuthenticationSASLContinue (type 11) — server challenge data.
    AuthenticationSASLContinue {
        data: Vec<u8>,
    },
    /// AuthenticationSASLFinal (type 12) — server proof.
    AuthenticationSASLFinal {
        data: Vec<u8>,
    },
    /// Asynchronous notification ('A') — from NOTIFY.
    NotificationResponse {
        process_id: i32,
        channel: String,
        payload: String,
    },
    /// Parameter status ('S').
    ParameterStatus {
        name: String,
        value: String,
    },
    /// Backend key data ('K').
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },
    /// Ready for query ('Z').
    ReadyForQuery {
        txn_status: u8,
    },
    /// Row description ('T').
    RowDescription {
        fields: Vec<FieldDescription>,
    },
    /// Data row ('D').
    DataRow {
        values: Vec<Option<String>>,
    },
    /// Command complete ('C').
    CommandComplete {
        tag: String,
    },
    /// Error response ('E').
    ErrorResponse {
        severity: String,
        code: String,
        message: String,
    },
    /// Notice response ('N').
    NoticeResponse {
        message: String,
    },
    /// Empty query response ('I').
    EmptyQueryResponse,
    /// Parse complete ('1').
    ParseComplete,
    /// Bind complete ('2').
    BindComplete,
    /// Close complete ('3').
    CloseComplete,
    /// No data ('n').
    NoData,
    /// Parameter description ('t').
    ParameterDescription {
        type_oids: Vec<i32>,
    },
    /// CopyInResponse ('G') — server ready to receive COPY data.
    CopyInResponse {
        format: u8, // 0 = text, 1 = binary
        column_formats: Vec<i16>,
    },
    /// CopyOutResponse ('H') — server will send COPY data.
    CopyOutResponse {
        format: u8,
        column_formats: Vec<i16>,
    },
    /// CopyData ('d') — a row of COPY data.
    CopyData(Vec<u8>),
    /// CopyDone ('c') — end of COPY output.
    CopyDone,
}

#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_attr: i16,
    pub type_oid: i32,
    pub type_len: i16,
    pub type_modifier: i32,
    pub format_code: i16, // 0 = text, 1 = binary
}

/// Decode a frontend message from raw bytes.
/// Returns Ok(Some(msg)) if a complete message was decoded,
/// Ok(None) if more data is needed.
pub fn decode_startup(buf: &mut BytesMut) -> Result<Option<FrontendMessage>, String> {
    if buf.len() < 4 {
        return Ok(None);
    }

    let len = (&buf[0..4]).get_i32() as usize;
    if buf.len() < len {
        return Ok(None);
    }

    let mut msg_buf = buf.split_to(len);
    msg_buf.advance(4); // skip length

    if msg_buf.remaining() < 4 {
        return Err("Startup message too short".into());
    }

    let version = msg_buf.get_i32();

    // SSL request: version = 80877103
    if version == 80877103 {
        return Ok(Some(FrontendMessage::SslRequest));
    }

    // Cancel request: version = 80877102
    if version == 80877102 {
        if msg_buf.remaining() < 8 {
            return Err("Cancel request too short".into());
        }
        let process_id = msg_buf.get_i32();
        let secret_key = msg_buf.get_i32();
        return Ok(Some(FrontendMessage::CancelRequest {
            process_id,
            secret_key,
        }));
    }

    // Normal startup: version 3.0 = 196608
    let mut params = HashMap::new();
    while msg_buf.has_remaining() {
        let key = read_cstring(&mut msg_buf)?;
        if key.is_empty() {
            break;
        }
        let value = read_cstring(&mut msg_buf)?;
        params.insert(key, value);
    }

    Ok(Some(FrontendMessage::Startup { version, params }))
}

/// Decode a regular frontend message (after startup).
pub fn decode_message(buf: &mut BytesMut) -> Result<Option<FrontendMessage>, String> {
    if buf.len() < 5 {
        return Ok(None);
    }

    let msg_type = buf[0];
    let len = (&buf[1..5]).get_i32() as usize;

    if buf.len() < 1 + len {
        return Ok(None);
    }

    buf.advance(1); // type byte
    let mut msg_buf = buf.split_to(len);
    msg_buf.advance(4); // length (already consumed conceptually)

    match msg_type {
        b'Q' => {
            let query = read_cstring(&mut msg_buf)?;
            Ok(Some(FrontendMessage::Query(query)))
        }
        b'X' => Ok(Some(FrontendMessage::Terminate)),
        b'p' => {
            let password = read_cstring(&mut msg_buf)?;
            Ok(Some(FrontendMessage::PasswordMessage(password)))
        }
        b'P' => {
            let name = read_cstring(&mut msg_buf)?;
            let query = read_cstring(&mut msg_buf)?;
            let num_params = if msg_buf.remaining() >= 2 {
                msg_buf.get_i16()
            } else {
                0
            };
            let mut param_types = Vec::new();
            for _ in 0..num_params {
                if msg_buf.remaining() >= 4 {
                    param_types.push(msg_buf.get_i32());
                }
            }
            Ok(Some(FrontendMessage::Parse {
                name,
                query,
                param_types,
            }))
        }
        b'B' => {
            let portal = read_cstring(&mut msg_buf)?;
            let statement = read_cstring(&mut msg_buf)?;
            let num_formats = if msg_buf.remaining() >= 2 {
                msg_buf.get_i16()
            } else {
                0
            };
            let mut param_formats = Vec::new();
            for _ in 0..num_formats {
                if msg_buf.remaining() >= 2 {
                    param_formats.push(msg_buf.get_i16());
                }
            }
            let num_values = if msg_buf.remaining() >= 2 {
                msg_buf.get_i16()
            } else {
                0
            };
            let mut param_values = Vec::new();
            for _ in 0..num_values {
                if msg_buf.remaining() >= 4 {
                    let vlen = msg_buf.get_i32();
                    if vlen < 0 {
                        param_values.push(None);
                    } else {
                        let mut val = vec![0u8; vlen as usize];
                        if msg_buf.remaining() >= vlen as usize {
                            msg_buf.copy_to_slice(&mut val);
                        }
                        param_values.push(Some(val));
                    }
                }
            }
            let num_result_formats = if msg_buf.remaining() >= 2 {
                msg_buf.get_i16()
            } else {
                0
            };
            let mut result_formats = Vec::new();
            for _ in 0..num_result_formats {
                if msg_buf.remaining() >= 2 {
                    result_formats.push(msg_buf.get_i16());
                }
            }
            Ok(Some(FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                param_values,
                result_formats,
            }))
        }
        b'D' => {
            let kind = if msg_buf.has_remaining() {
                msg_buf.get_u8()
            } else {
                b'S'
            };
            let name = read_cstring(&mut msg_buf)?;
            Ok(Some(FrontendMessage::Describe { kind, name }))
        }
        b'E' => {
            let portal = read_cstring(&mut msg_buf)?;
            let max_rows = if msg_buf.remaining() >= 4 {
                msg_buf.get_i32()
            } else {
                0
            };
            Ok(Some(FrontendMessage::Execute { portal, max_rows }))
        }
        b'S' => Ok(Some(FrontendMessage::Sync)),
        b'C' => {
            let kind = if msg_buf.has_remaining() {
                msg_buf.get_u8()
            } else {
                b'S'
            };
            let name = read_cstring(&mut msg_buf)?;
            Ok(Some(FrontendMessage::Close { kind, name }))
        }
        b'H' => Ok(Some(FrontendMessage::Flush)),
        b'd' => {
            // CopyData: raw bytes
            let data = msg_buf.to_vec();
            Ok(Some(FrontendMessage::CopyData(data)))
        }
        b'c' => Ok(Some(FrontendMessage::CopyDone)),
        b'f' => {
            let message = read_cstring(&mut msg_buf)?;
            Ok(Some(FrontendMessage::CopyFail(message)))
        }
        _ => {
            tracing::warn!(
                "Unknown frontend message type: {} (0x{:02x})",
                msg_type as char,
                msg_type
            );
            Ok(None)
        }
    }
}

/// Encode a backend message into bytes.
pub fn encode_message(msg: &BackendMessage) -> BytesMut {
    let mut buf = BytesMut::new();

    match msg {
        BackendMessage::AuthenticationOk => {
            buf.put_u8(b'R');
            buf.put_i32(8); // length
            buf.put_i32(0); // auth ok
        }
        BackendMessage::AuthenticationCleartextPassword => {
            buf.put_u8(b'R');
            buf.put_i32(8);
            buf.put_i32(3);
        }
        BackendMessage::AuthenticationMd5Password { salt } => {
            buf.put_u8(b'R');
            buf.put_i32(12);
            buf.put_i32(5);
            buf.put_slice(salt);
        }
        BackendMessage::AuthenticationSASL { mechanisms } => {
            // R message type 10: list mechanism names as C-strings, terminated by empty string
            let mut body = BytesMut::new();
            body.put_i32(10);
            for mech in mechanisms {
                write_cstring(&mut body, mech);
            }
            body.put_u8(0); // empty string terminator
            buf.put_u8(b'R');
            buf.put_i32(4 + body.len() as i32);
            buf.extend_from_slice(&body);
        }
        BackendMessage::AuthenticationSASLContinue { data } => {
            buf.put_u8(b'R');
            buf.put_i32(4 + 4 + data.len() as i32);
            buf.put_i32(11);
            buf.put_slice(data);
        }
        BackendMessage::AuthenticationSASLFinal { data } => {
            buf.put_u8(b'R');
            buf.put_i32(4 + 4 + data.len() as i32);
            buf.put_i32(12);
            buf.put_slice(data);
        }
        BackendMessage::NotificationResponse {
            process_id,
            channel,
            payload,
        } => {
            let body_len = 4 + channel.len() + 1 + payload.len() + 1;
            buf.put_u8(b'A');
            buf.put_i32(4 + body_len as i32);
            buf.put_i32(*process_id);
            write_cstring(&mut buf, channel);
            write_cstring(&mut buf, payload);
        }
        BackendMessage::ParameterStatus { name, value } => {
            let len = 4 + name.len() + 1 + value.len() + 1;
            buf.put_u8(b'S');
            buf.put_i32(len as i32);
            write_cstring(&mut buf, name);
            write_cstring(&mut buf, value);
        }
        BackendMessage::BackendKeyData {
            process_id,
            secret_key,
        } => {
            buf.put_u8(b'K');
            buf.put_i32(12);
            buf.put_i32(*process_id);
            buf.put_i32(*secret_key);
        }
        BackendMessage::ReadyForQuery { txn_status } => {
            buf.put_u8(b'Z');
            buf.put_i32(5);
            buf.put_u8(*txn_status);
        }
        BackendMessage::RowDescription { fields } => {
            let mut body = BytesMut::new();
            body.put_i16(fields.len() as i16);
            for field in fields {
                write_cstring(&mut body, &field.name);
                body.put_i32(field.table_oid);
                body.put_i16(field.column_attr);
                body.put_i32(field.type_oid);
                body.put_i16(field.type_len);
                body.put_i32(field.type_modifier);
                body.put_i16(field.format_code);
            }
            buf.put_u8(b'T');
            buf.put_i32(4 + body.len() as i32);
            buf.extend_from_slice(&body);
        }
        BackendMessage::DataRow { values } => {
            let mut body = BytesMut::new();
            body.put_i16(values.len() as i16);
            for val in values {
                match val {
                    Some(s) => {
                        let bytes = s.as_bytes();
                        body.put_i32(bytes.len() as i32);
                        body.put_slice(bytes);
                    }
                    None => {
                        body.put_i32(-1); // NULL
                    }
                }
            }
            buf.put_u8(b'D');
            buf.put_i32(4 + body.len() as i32);
            buf.extend_from_slice(&body);
        }
        BackendMessage::CommandComplete { tag } => {
            let len = 4 + tag.len() + 1;
            buf.put_u8(b'C');
            buf.put_i32(len as i32);
            write_cstring(&mut buf, tag);
        }
        BackendMessage::ErrorResponse {
            severity,
            code,
            message,
        } => {
            let mut body = BytesMut::new();
            body.put_u8(b'S');
            write_cstring(&mut body, severity);
            body.put_u8(b'V');
            write_cstring(&mut body, severity);
            body.put_u8(b'C');
            write_cstring(&mut body, code);
            body.put_u8(b'M');
            write_cstring(&mut body, message);
            body.put_u8(0); // terminator

            buf.put_u8(b'E');
            buf.put_i32(4 + body.len() as i32);
            buf.extend_from_slice(&body);
        }
        BackendMessage::NoticeResponse { message } => {
            let mut body = BytesMut::new();
            body.put_u8(b'S');
            write_cstring(&mut body, "NOTICE");
            body.put_u8(b'M');
            write_cstring(&mut body, message);
            body.put_u8(0);

            buf.put_u8(b'N');
            buf.put_i32(4 + body.len() as i32);
            buf.extend_from_slice(&body);
        }
        BackendMessage::EmptyQueryResponse => {
            buf.put_u8(b'I');
            buf.put_i32(4);
        }
        BackendMessage::ParseComplete => {
            buf.put_u8(b'1');
            buf.put_i32(4);
        }
        BackendMessage::BindComplete => {
            buf.put_u8(b'2');
            buf.put_i32(4);
        }
        BackendMessage::CloseComplete => {
            buf.put_u8(b'3');
            buf.put_i32(4);
        }
        BackendMessage::NoData => {
            buf.put_u8(b'n');
            buf.put_i32(4);
        }
        BackendMessage::ParameterDescription { type_oids } => {
            buf.put_u8(b't');
            buf.put_i32(4 + 2 + type_oids.len() as i32 * 4);
            buf.put_i16(type_oids.len() as i16);
            for oid in type_oids {
                buf.put_i32(*oid);
            }
        }
        BackendMessage::CopyInResponse {
            format,
            column_formats,
        } => {
            let len = 4 + 1 + 2 + column_formats.len() as i32 * 2;
            buf.put_u8(b'G');
            buf.put_i32(len);
            buf.put_u8(*format);
            buf.put_i16(column_formats.len() as i16);
            for cf in column_formats {
                buf.put_i16(*cf);
            }
        }
        BackendMessage::CopyOutResponse {
            format,
            column_formats,
        } => {
            let len = 4 + 1 + 2 + column_formats.len() as i32 * 2;
            buf.put_u8(b'H');
            buf.put_i32(len);
            buf.put_u8(*format);
            buf.put_i16(column_formats.len() as i16);
            for cf in column_formats {
                buf.put_i16(*cf);
            }
        }
        BackendMessage::CopyData(data) => {
            buf.put_u8(b'd');
            buf.put_i32(4 + data.len() as i32);
            buf.put_slice(data);
        }
        BackendMessage::CopyDone => {
            buf.put_u8(b'c');
            buf.put_i32(4);
        }
    }

    buf
}

fn read_cstring(buf: &mut BytesMut) -> Result<String, String> {
    if let Some(pos) = buf.iter().position(|&b| b == 0) {
        let s = String::from_utf8(buf[..pos].to_vec())
            .map_err(|e| format!("Invalid UTF-8 in cstring: {}", e))?;
        buf.advance(pos + 1);
        Ok(s)
    } else {
        Err("No null terminator in cstring".into())
    }
}

fn write_cstring(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}
