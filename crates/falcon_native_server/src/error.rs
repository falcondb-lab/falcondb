//! Error types for the native server.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NativeServerError {
    #[error("Protocol error: {0}")]
    Protocol(#[from] falcon_protocol_native::NativeProtocolError),

    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    #[error("Epoch mismatch: client={client_epoch}, server={server_epoch}")]
    EpochMismatch { client_epoch: u64, server_epoch: u64 },

    #[error("Session not authenticated")]
    NotAuthenticated,

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Server shutting down")]
    Shutdown,
}
