//! PG SSL/TLS support for FalconDB.
//!
//! Implements TLS negotiation per the PostgreSQL wire protocol:
//! 1. Client sends `SslRequest` (8-byte message with code 80877103).
//! 2. Server responds `S` (willing) or `N` (not willing).
//! 3. If `S`, the TCP stream is upgraded to TLS before the normal startup message.
//!
//! # Configuration
//!
//! - `pg.ssl.enabled` — whether to accept TLS connections (respond `S`).
//! - `pg.ssl.require` — whether to *require* TLS (reject plaintext after `N`).
//! - `pg.ssl.cert_path` — path to PEM-encoded server certificate.
//! - `pg.ssl.key_path` — path to PEM-encoded private key.
//!
//! # Invariants
//!
//! - **TLS-1**: If `require_ssl=true`, plaintext connections MUST be rejected with
//!   a FATAL error response ("08P01 — SSL required").
//! - **TLS-2**: Invalid/missing cert/key at startup is a fatal boot error (no silent fallback).
//! - **TLS-3**: If TLS is not configured, `SslRequest` gets `N` (existing behavior).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

// ═══════════════════════════════════════════════════════════════════════════
// TLS Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// TLS configuration for the PG server.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Whether TLS is enabled (server responds 'S' to SslRequest).
    pub enabled: bool,
    /// Whether TLS is required (plaintext rejected after SslRequest gets 'N').
    pub require: bool,
    /// Path to PEM-encoded server certificate chain.
    pub cert_path: PathBuf,
    /// Path to PEM-encoded private key.
    pub key_path: PathBuf,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            require: false,
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
        }
    }
}

impl TlsConfig {
    /// Create a TLS config for testing with explicit paths.
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            enabled: true,
            require: false,
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }

    /// Create a config that requires TLS.
    pub fn new_required(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            enabled: true,
            require: true,
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TLS Acceptor Builder
// ═══════════════════════════════════════════════════════════════════════════

/// Build a `TlsAcceptor` from a `TlsConfig`.
///
/// **TLS-2**: Returns an error if cert/key files are missing or invalid.
/// The caller (server startup) MUST treat this as a fatal boot error.
pub fn build_tls_acceptor(config: &TlsConfig) -> Result<TlsAcceptor, TlsSetupError> {
    if !config.enabled {
        return Err(TlsSetupError::NotEnabled);
    }

    let certs = load_certs(&config.cert_path)?;
    let key = load_private_key(&config.key_path)?;

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TlsSetupError::RustlsConfig(e.to_string()))?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// Load PEM-encoded certificates from a file.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsSetupError> {
    let file = std::fs::File::open(path).map_err(|e| TlsSetupError::CertLoad {
        path: path.to_path_buf(),
        error: e.to_string(),
    })?;
    let mut reader = io::BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .filter_map(std::result::Result::ok)
        .collect();
    if certs.is_empty() {
        return Err(TlsSetupError::CertLoad {
            path: path.to_path_buf(),
            error: "no certificates found in PEM file".into(),
        });
    }
    Ok(certs)
}

/// Load a PEM-encoded private key from a file.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsSetupError> {
    let file = std::fs::File::open(path).map_err(|e| TlsSetupError::KeyLoad {
        path: path.to_path_buf(),
        error: e.to_string(),
    })?;
    let mut reader = io::BufReader::new(file);

    // Try PKCS8 first, then RSA, then EC
    loop {
        match rustls_pemfile::read_one(&mut reader) {
            Ok(Some(rustls_pemfile::Item::Pkcs8Key(key))) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Ok(Some(rustls_pemfile::Item::Pkcs1Key(key))) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Ok(Some(rustls_pemfile::Item::Sec1Key(key))) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            Ok(Some(_)) => continue, // skip other PEM items (certs, etc.)
            Ok(None) => break,
            Err(e) => {
                return Err(TlsSetupError::KeyLoad {
                    path: path.to_path_buf(),
                    error: e.to_string(),
                });
            }
        }
    }

    Err(TlsSetupError::KeyLoad {
        path: path.to_path_buf(),
        error: "no private key found in PEM file".into(),
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

/// TLS setup errors (all are fatal at boot time).
#[derive(Debug)]
pub enum TlsSetupError {
    /// TLS is not enabled in config.
    NotEnabled,
    /// Certificate file could not be loaded.
    CertLoad { path: PathBuf, error: String },
    /// Private key file could not be loaded.
    KeyLoad { path: PathBuf, error: String },
    /// rustls configuration error.
    RustlsConfig(String),
}

impl std::fmt::Display for TlsSetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotEnabled => write!(f, "TLS is not enabled"),
            Self::CertLoad { path, error } => {
                write!(f, "failed to load TLS certificate from {path:?}: {error}")
            }
            Self::KeyLoad { path, error } => {
                write!(f, "failed to load TLS private key from {path:?}: {error}")
            }
            Self::RustlsConfig(e) => write!(f, "TLS configuration error: {e}"),
        }
    }
}

impl std::error::Error for TlsSetupError {}

// ═══════════════════════════════════════════════════════════════════════════
// Unified Stream — abstracts over plain TCP and TLS
// ═══════════════════════════════════════════════════════════════════════════

/// A connection stream that can be either plaintext TCP or TLS-wrapped.
pub enum PgStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl AsyncRead for PgStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PgStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl PgStream {
    /// Whether this stream is TLS-encrypted.
    pub const fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_disabled() {
        let cfg = TlsConfig::default();
        assert!(!cfg.enabled);
        assert!(!cfg.require);
    }

    #[test]
    fn test_new_config_enabled() {
        let cfg = TlsConfig::new("/tmp/cert.pem", "/tmp/key.pem");
        assert!(cfg.enabled);
        assert!(!cfg.require);
    }

    #[test]
    fn test_required_config() {
        let cfg = TlsConfig::new_required("/tmp/cert.pem", "/tmp/key.pem");
        assert!(cfg.enabled);
        assert!(cfg.require);
    }

    #[test]
    fn test_build_acceptor_not_enabled() {
        let cfg = TlsConfig::default();
        let result = build_tls_acceptor(&cfg);
        assert!(matches!(result, Err(TlsSetupError::NotEnabled)));
    }

    #[test]
    fn test_build_acceptor_missing_cert() {
        let cfg = TlsConfig::new("/nonexistent/cert.pem", "/nonexistent/key.pem");
        let result = build_tls_acceptor(&cfg);
        assert!(matches!(result, Err(TlsSetupError::CertLoad { .. })));
    }

    #[test]
    fn test_tls_setup_error_display() {
        let e = TlsSetupError::NotEnabled;
        assert_eq!(e.to_string(), "TLS is not enabled");
        let e = TlsSetupError::CertLoad {
            path: PathBuf::from("/tmp/cert.pem"),
            error: "file not found".into(),
        };
        assert!(e.to_string().contains("certificate"));
        assert!(e.to_string().contains("cert.pem"));
    }
}
