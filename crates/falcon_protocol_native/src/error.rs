//! Error types for the native protocol codec.

use thiserror::Error;

/// Errors that can occur during native protocol encode/decode.
#[derive(Error, Debug)]
pub enum NativeProtocolError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),

    #[error("Frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: u32, max: u32 },

    #[error("Unsupported protocol version: {major}.{minor}")]
    UnsupportedVersion { major: u16, minor: u16 },

    #[error("Unknown message type: 0x{0:02x}")]
    UnknownMessageType(u8),

    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    #[error("Truncated message: expected {expected} bytes, got {actual}")]
    Truncated { expected: usize, actual: usize },

    #[error("Invalid UTF-8 in field '{field}': {source}")]
    InvalidUtf8 {
        field: String,
        source: std::string::FromUtf8Error,
    },

    #[error("Corruption: {0}")]
    Corruption(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl NativeProtocolError {
    /// Whether this error indicates the connection should be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(self, NativeProtocolError::Io(_))
    }
}
