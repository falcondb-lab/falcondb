//! Configuration for the native protocol server.

/// Native server configuration.
#[derive(Debug, Clone)]
pub struct NativeServerConfig {
    /// Listen address for native protocol (e.g. "0.0.0.0:15433").
    pub listen_addr: String,
    /// Whether TLS is required for native connections.
    pub tls_enabled: bool,
    /// Maximum concurrent native connections.
    pub max_connections: usize,
    /// Default request timeout in milliseconds.
    pub default_timeout_ms: u64,
    /// Current cluster epoch (updated on leader change).
    pub epoch: u64,
    /// This node's ID.
    pub node_id: u64,
    /// Whether epoch fencing is enforced on write requests.
    pub epoch_fencing_enabled: bool,
    /// Authentication method: "password" or "token".
    pub auth_method: String,
}

impl Default for NativeServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:15433".into(),
            tls_enabled: false,
            max_connections: 1024,
            default_timeout_ms: 30_000,
            epoch: 1,
            node_id: 1,
            epoch_fencing_enabled: true,
            auth_method: "password".into(),
        }
    }
}
