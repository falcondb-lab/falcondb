//! Native protocol message types and constants.

/// Protocol version.
pub const PROTOCOL_VERSION_MAJOR: u16 = 0;
pub const PROTOCOL_VERSION_MINOR: u16 = 1;

/// Maximum frame payload size: 64 MiB.
pub const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

/// Frame header size: 1 byte msg_type + 4 bytes length.
pub const FRAME_HEADER_SIZE: usize = 5;

// ── Message type tags ────────────────────────────────────────────────────

pub const MSG_CLIENT_HELLO: u8 = 0x01;
pub const MSG_SERVER_HELLO: u8 = 0x02;
pub const MSG_AUTH_REQUEST: u8 = 0x03;
pub const MSG_AUTH_RESPONSE: u8 = 0x04;
pub const MSG_AUTH_OK: u8 = 0x05;
pub const MSG_AUTH_FAIL: u8 = 0x06;
pub const MSG_QUERY_REQUEST: u8 = 0x10;
pub const MSG_QUERY_RESPONSE: u8 = 0x11;
pub const MSG_ERROR_RESPONSE: u8 = 0x12;
pub const MSG_BATCH_REQUEST: u8 = 0x13;
pub const MSG_BATCH_RESPONSE: u8 = 0x14;
pub const MSG_PING: u8 = 0x20;
pub const MSG_PONG: u8 = 0x21;
pub const MSG_DISCONNECT: u8 = 0x30;
pub const MSG_DISCONNECT_ACK: u8 = 0x31;
pub const MSG_START_TLS: u8 = 0xFE;
pub const MSG_START_TLS_ACK: u8 = 0xFF;

// ── Feature flags ────────────────────────────────────────────────────────

pub const FEATURE_COMPRESSION_LZ4: u64 = 1 << 0;
pub const FEATURE_COMPRESSION_ZSTD: u64 = 1 << 1;
pub const FEATURE_BATCH_INGEST: u64 = 1 << 2;
pub const FEATURE_PIPELINE: u64 = 1 << 3;
pub const FEATURE_EPOCH_FENCING: u64 = 1 << 4;
pub const FEATURE_TLS: u64 = 1 << 5;
pub const FEATURE_BINARY_PARAMS: u64 = 1 << 6;

// ── Auth methods ─────────────────────────────────────────────────────────

pub const AUTH_PASSWORD: u8 = 0;
pub const AUTH_TOKEN: u8 = 1;
pub const AUTH_SCRAM_SHA256: u8 = 2;

// ── Type IDs (binary column encoding) ────────────────────────────────────

pub const TYPE_NULL: u8 = 0x00;
pub const TYPE_BOOLEAN: u8 = 0x01;
pub const TYPE_INT32: u8 = 0x02;
pub const TYPE_INT64: u8 = 0x03;
pub const TYPE_FLOAT64: u8 = 0x04;
pub const TYPE_TEXT: u8 = 0x05;
pub const TYPE_TIMESTAMP: u8 = 0x06;
pub const TYPE_DATE: u8 = 0x07;
pub const TYPE_JSONB: u8 = 0x08;
pub const TYPE_DECIMAL: u8 = 0x09;
pub const TYPE_TIME: u8 = 0x0A;
pub const TYPE_INTERVAL: u8 = 0x0B;
pub const TYPE_UUID: u8 = 0x0C;
pub const TYPE_BYTEA: u8 = 0x0D;
pub const TYPE_ARRAY: u8 = 0x0E;

// ── Error codes ──────────────────────────────────────────────────────────

pub const ERR_SYNTAX_ERROR: u32 = 1000;
pub const ERR_INVALID_PARAM: u32 = 1001;
pub const ERR_NOT_LEADER: u32 = 2000;
pub const ERR_FENCED_EPOCH: u32 = 2001;
pub const ERR_READ_ONLY: u32 = 2002;
pub const ERR_SERIALIZATION_CONFLICT: u32 = 2003;
pub const ERR_INTERNAL_ERROR: u32 = 3000;
pub const ERR_TIMEOUT: u32 = 3001;
pub const ERR_OVERLOADED: u32 = 3002;
pub const ERR_AUTH_FAILED: u32 = 4000;
pub const ERR_PERMISSION_DENIED: u32 = 4001;

// ── Session flags ────────────────────────────────────────────────────────

pub const SESSION_AUTOCOMMIT: u32 = 1 << 0;
pub const SESSION_READ_ONLY: u32 = 1 << 1;

// ── Message definitions ──────────────────────────────────────────────────

/// Key-value parameter pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Param {
    pub key: String,
    pub value: String,
}

/// Client handshake initiation.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientHello {
    pub version_major: u16,
    pub version_minor: u16,
    pub feature_flags: u64,
    pub client_name: String,
    pub database: String,
    pub user: String,
    pub nonce: [u8; 16],
    pub params: Vec<Param>,
}

/// Server handshake response.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerHello {
    pub version_major: u16,
    pub version_minor: u16,
    pub feature_flags: u64,
    pub server_epoch: u64,
    pub server_node_id: u64,
    pub server_nonce: [u8; 16],
    pub params: Vec<Param>,
}

/// Authentication challenge from server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthRequest {
    pub auth_method: u8,
    pub challenge: Vec<u8>,
}

/// Authentication credentials from client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthResponse {
    pub auth_method: u8,
    pub credential: Vec<u8>,
}

/// Column metadata in a query response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: String,
    pub type_id: u8,
    pub nullable: bool,
    pub precision: u16,
    pub scale: u16,
}

/// A single encoded value (binary).
#[derive(Debug, Clone, PartialEq)]
pub enum EncodedValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    Timestamp(i64),
    Date(i32),
    Jsonb(String),
    Decimal(i128, u8),
    Time(i64),
    Interval(i32, i32, i64),
    Uuid(u128),
    Bytea(Vec<u8>),
    Array(u8, Vec<EncodedValue>),
}

/// A row of encoded values.
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedRow {
    pub values: Vec<EncodedValue>,
}

/// SQL query request.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequest {
    pub request_id: u64,
    pub epoch: u64,
    pub sql: String,
    pub params: Vec<EncodedValue>,
    pub session_flags: u32,
}

/// SQL query response with schema + rows.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResponse {
    pub request_id: u64,
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<EncodedRow>,
    pub rows_affected: u64,
}

/// Error response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponse {
    pub request_id: u64,
    pub error_code: u32,
    pub sqlstate: [u8; 5],
    pub retryable: bool,
    pub server_epoch: u64,
    pub message: String,
}

/// Batch insert request.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchRequest {
    pub request_id: u64,
    pub epoch: u64,
    pub sql: String,
    pub column_types: Vec<u8>,
    pub rows: Vec<EncodedRow>,
    pub options: u32,
}

/// Batch insert response.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchResponse {
    pub request_id: u64,
    pub counts: Vec<i64>,
    pub error: Option<ErrorResponse>,
}

/// Top-level protocol message.
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    ClientHello(ClientHello),
    ServerHello(ServerHello),
    AuthRequest(AuthRequest),
    AuthResponse(AuthResponse),
    AuthOk,
    AuthFail(String),
    QueryRequest(QueryRequest),
    QueryResponse(QueryResponse),
    ErrorResponse(ErrorResponse),
    BatchRequest(BatchRequest),
    BatchResponse(BatchResponse),
    Ping,
    Pong,
    Disconnect,
    DisconnectAck,
    StartTls,
    StartTlsAck,
}

impl Message {
    /// Return the message type tag byte.
    pub fn msg_type(&self) -> u8 {
        match self {
            Message::ClientHello(_) => MSG_CLIENT_HELLO,
            Message::ServerHello(_) => MSG_SERVER_HELLO,
            Message::AuthRequest(_) => MSG_AUTH_REQUEST,
            Message::AuthResponse(_) => MSG_AUTH_RESPONSE,
            Message::AuthOk => MSG_AUTH_OK,
            Message::AuthFail(_) => MSG_AUTH_FAIL,
            Message::QueryRequest(_) => MSG_QUERY_REQUEST,
            Message::QueryResponse(_) => MSG_QUERY_RESPONSE,
            Message::ErrorResponse(_) => MSG_ERROR_RESPONSE,
            Message::BatchRequest(_) => MSG_BATCH_REQUEST,
            Message::BatchResponse(_) => MSG_BATCH_RESPONSE,
            Message::Ping => MSG_PING,
            Message::Pong => MSG_PONG,
            Message::Disconnect => MSG_DISCONNECT,
            Message::DisconnectAck => MSG_DISCONNECT_ACK,
            Message::StartTls => MSG_START_TLS,
            Message::StartTlsAck => MSG_START_TLS_ACK,
        }
    }
}
