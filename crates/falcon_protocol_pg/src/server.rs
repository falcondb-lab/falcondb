use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use falcon_cluster::{DistributedQueryEngine, ReplicaRunnerMetrics};
use falcon_common::config::{AuthConfig, AuthMethod};
use falcon_common::types::ShardId;
use falcon_executor::Executor;
use falcon_storage::engine::StorageEngine;
use falcon_txn::TxnManager;

use crate::codec::{self, BackendMessage, FrontendMessage};
use crate::connection_pool::{ConnectionPool, PoolConfig};
use crate::handler::QueryHandler;
use crate::logical_replication;
use crate::notify::NotificationHub;
use crate::session::PgSession;
use crate::tls::{self, TlsConfig};
use tokio_rustls::TlsAcceptor;

/// Entry in the cancellation registry for a session.
struct CancelEntry {
    secret_key: i32,
    cancelled: Arc<AtomicBool>,
}

/// Shared cancellation registry: maps session_id → CancelEntry.
/// Used to look up and signal cancellation for a running query.
type CancellationRegistry = Arc<DashMap<i32, CancelEntry>>;

/// PostgreSQL-compatible TCP server.
pub struct PgServer {
    listen_addr: String,
    storage: Arc<StorageEngine>,
    txn_mgr: Arc<TxnManager>,
    executor: Arc<Executor>,
    next_session_id: AtomicI32,
    /// Optional distributed engine for multi-shard mode.
    dist_engine: Option<Arc<DistributedQueryEngine>>,
    /// Shard IDs for multi-shard mode.
    cluster_shard_ids: Vec<ShardId>,
    /// Number of currently active connections.
    active_connections: Arc<AtomicUsize>,
    /// Maximum allowed concurrent connections (0 = unlimited).
    max_connections: usize,
    /// Default statement timeout in milliseconds (0 = no timeout).
    default_statement_timeout_ms: u64,
    /// Connection idle timeout in milliseconds (0 = no timeout).
    idle_timeout_ms: u64,
    /// Replica replication metrics (only set when running as replica).
    replica_metrics: Option<Arc<ReplicaRunnerMetrics>>,
    /// Authentication configuration.
    auth_config: AuthConfig,
    /// Shared cancellation registry for cancel request support.
    cancel_registry: CancellationRegistry,
    /// Optional server-side connection pool.
    connection_pool: Option<Arc<ConnectionPool>>,
    /// Shared LISTEN/NOTIFY hub — all sessions share this so NOTIFY reaches all listeners.
    notification_hub: Arc<NotificationHub>,
    /// Optional TLS acceptor (built from TlsConfig at startup).
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    /// TLS configuration.
    tls_config: TlsConfig,
}

impl PgServer {
    pub fn new(
        listen_addr: String,
        storage: Arc<StorageEngine>,
        txn_mgr: Arc<TxnManager>,
        executor: Arc<Executor>,
    ) -> Self {
        Self {
            listen_addr,
            storage,
            txn_mgr,
            executor,
            next_session_id: AtomicI32::new(1),
            dist_engine: None,
            cluster_shard_ids: vec![ShardId(0)],
            active_connections: Arc::new(AtomicUsize::new(0)),
            max_connections: 0,
            default_statement_timeout_ms: 0,
            idle_timeout_ms: 0,
            replica_metrics: None,
            auth_config: AuthConfig::default(),
            cancel_registry: Arc::new(DashMap::new()),
            connection_pool: None,
            notification_hub: Arc::new(NotificationHub::new()),
            tls_acceptor: None,
            tls_config: TlsConfig::default(),
        }
    }

    /// Create a server with distributed query engine for multi-shard mode.
    pub fn new_distributed(
        listen_addr: String,
        storage: Arc<StorageEngine>,
        txn_mgr: Arc<TxnManager>,
        executor: Arc<Executor>,
        shard_ids: Vec<ShardId>,
        dist_engine: Arc<DistributedQueryEngine>,
    ) -> Self {
        Self {
            listen_addr,
            storage,
            txn_mgr,
            executor,
            next_session_id: AtomicI32::new(1),
            dist_engine: Some(dist_engine),
            cluster_shard_ids: shard_ids,
            active_connections: Arc::new(AtomicUsize::new(0)),
            max_connections: 0,
            default_statement_timeout_ms: 0,
            idle_timeout_ms: 0,
            replica_metrics: None,
            auth_config: AuthConfig::default(),
            cancel_registry: Arc::new(DashMap::new()),
            connection_pool: None,
            notification_hub: Arc::new(NotificationHub::new()),
            tls_acceptor: None,
            tls_config: TlsConfig::default(),
        }
    }

    /// Configure TLS for SSL connections.
    ///
    /// **TLS-2**: If TLS is enabled but cert/key are invalid, this returns an error.
    /// The caller MUST treat this as a fatal boot error.
    pub fn set_tls_config(&mut self, config: TlsConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if config.enabled {
            let acceptor = tls::build_tls_acceptor(&config)?;
            self.tls_acceptor = Some(Arc::new(acceptor));
            tracing::info!(
                "TLS enabled (require={}), cert={:?}, key={:?}",
                config.require,
                config.cert_path,
                config.key_path,
            );
        }
        self.tls_config = config;
        Ok(())
    }

    /// Whether TLS is configured and available.
    pub const fn tls_enabled(&self) -> bool {
        self.tls_acceptor.is_some()
    }

    /// Whether TLS is required (plaintext rejected).
    pub const fn tls_required(&self) -> bool {
        self.tls_config.require
    }

    /// Enable the server-side connection pool with the given configuration.
    pub fn enable_connection_pool(&mut self, config: PoolConfig) {
        let pool = if let Some(ref dist) = self.dist_engine {
            ConnectionPool::new_distributed(
                self.storage.clone(),
                self.txn_mgr.clone(),
                self.executor.clone(),
                self.cluster_shard_ids.clone(),
                dist.clone(),
                config,
            )
        } else {
            ConnectionPool::new(
                self.storage.clone(),
                self.txn_mgr.clone(),
                self.executor.clone(),
                config,
            )
        };
        self.connection_pool = Some(Arc::new(pool));
    }

    /// Get pool statistics (if pool is enabled).
    pub fn pool_stats(&self) -> Option<crate::connection_pool::PoolStats> {
        self.connection_pool.as_ref().map(|p| p.stats())
    }

    /// Set the authentication configuration.
    pub fn set_auth_config(&mut self, config: AuthConfig) {
        self.auth_config = config;
    }

    /// Set the maximum number of concurrent connections (0 = unlimited).
    pub const fn set_max_connections(&mut self, max: usize) {
        self.max_connections = max;
    }

    /// Set the default statement timeout for new sessions (0 = no timeout).
    pub const fn set_default_statement_timeout_ms(&mut self, ms: u64) {
        self.default_statement_timeout_ms = ms;
    }

    /// Set the connection idle timeout (0 = no timeout).
    pub const fn set_idle_timeout_ms(&mut self, ms: u64) {
        self.idle_timeout_ms = ms;
    }

    /// Set replica runner metrics for SHOW falcon.replica_stats.
    pub fn set_replica_metrics(&mut self, metrics: Arc<ReplicaRunnerMetrics>) {
        self.replica_metrics = Some(metrics);
    }

    /// Replace the active connections counter with a shared one.
    /// Used to share the counter between PgServer and Executor for SHOW falcon.connections.
    pub fn set_active_connections(&mut self, counter: Arc<AtomicUsize>) {
        self.active_connections = counter;
    }

    /// Number of currently active connections.
    pub fn active_connection_count(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Get a shared reference to the active connections counter.
    /// Used by the health check server.
    pub fn active_connections_handle(&self) -> Arc<AtomicUsize> {
        self.active_connections.clone()
    }

    /// Start the PG server and listen for connections.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&self.listen_addr).await?;
        tracing::info!("FalconDB PG server listening on {}", self.listen_addr);

        loop {
            let (stream, addr) = listener.accept().await?;
            tracing::info!("New connection from {}", addr);
            self.spawn_connection(stream);
        }
    }

    /// Start the PG server with graceful shutdown support.
    ///
    /// The server stops accepting new connections when `shutdown` resolves,
    /// then waits up to `drain_timeout` for active connections to finish.
    ///
    /// **Shutdown contract**: The `TcpListener` is explicitly dropped inside
    /// this method before returning. This guarantees deterministic port release
    /// on both Windows and Linux — we never rely on runtime drop ordering.
    pub async fn run_with_shutdown(
        &self,
        shutdown: impl std::future::Future<Output = ()>,
        drain_timeout: std::time::Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&self.listen_addr).await?;
        let local_addr = listener
            .local_addr().map_or_else(|_| self.listen_addr.clone(), |a| a.to_string());
        let port = local_addr
            .rsplit(':')
            .next()
            .unwrap_or("unknown").to_owned();
        tracing::info!("FalconDB PG server listening on {}", local_addr);

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    tracing::info!("New connection from {}", addr);
                    self.spawn_connection(stream);
                }
                _ = &mut shutdown => {
                    break;
                }
            }
        }

        // ── Explicit listener drop — deterministic port release ──
        // This MUST happen before we return, not as a side-effect of
        // function exit or runtime teardown.
        drop(listener);
        tracing::info!(
            server = "pg_server",
            port = %port,
            "pg server shutdown: listener dropped (port={})",
            port,
        );

        // Drain active connections
        let active = self.active_connections.load(Ordering::Relaxed);
        if active > 0 {
            tracing::info!(
                "Draining {} active connection(s) (timeout: {:?})",
                active,
                drain_timeout
            );
            let deadline = tokio::time::Instant::now() + drain_timeout;
            loop {
                let remaining = self.active_connections.load(Ordering::Relaxed);
                if remaining == 0 {
                    tracing::info!("All connections drained");
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    tracing::warn!(
                        "Drain timeout reached with {} connection(s) still active",
                        remaining
                    );
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        // Flush WAL if enabled
        if self.storage.is_wal_enabled() {
            tracing::info!("Flushing WAL before exit");
            if let Err(e) = self.storage.flush_wal() {
                tracing::error!("WAL flush error during shutdown: {}", e);
            }
        }

        tracing::info!("PG server graceful shutdown complete");
        Ok(())
    }

    /// Spawn a new connection handler with active-connection tracking.
    /// The max_connections check is performed **after** the PG startup handshake
    /// so the client receives a properly-framed FATAL error it can display.
    fn spawn_connection(&self, stream: TcpStream) -> bool {
        let session_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);
        let active = self.active_connections.clone();
        let max_connections = self.max_connections;
        let timeout_ms = self.default_statement_timeout_ms;
        let idle_ms = self.idle_timeout_ms;
        let auth_config = self.auth_config.clone();
        let cancel_reg = self.cancel_registry.clone();
        let replica_metrics = self.replica_metrics.clone();
        let notification_hub = self.notification_hub.clone();
        let tls_acceptor = self.tls_acceptor.clone();
        let tls_required = self.tls_config.require;

        // Increment active connections now; the handler will decrement on exit.
        // The actual max_connections check happens after the startup handshake
        // so the client receives a properly-framed FATAL error.
        active.fetch_add(1, Ordering::Relaxed);

        if let Some(ref pool) = self.connection_pool {
            // Pool-based path: acquire handler from pool (bounded concurrency).
            let pool = pool.clone();

            tokio::spawn(async move {
                if let Some(pooled) = pool.acquire().await {
                    let mut handler = pooled.handler_clone();
                    if let Some(ref rm) = replica_metrics {
                        handler.set_replica_metrics(rm.clone());
                    }
                    if let Err(e) = handle_connection_with_timeout(
                        stream,
                        session_id,
                        handler,
                        timeout_ms,
                        idle_ms,
                        max_connections,
                        active.clone(),
                        auth_config,
                        cancel_reg.clone(),
                        notification_hub.clone(),
                        tls_acceptor,
                        tls_required,
                    )
                    .await
                    {
                        tracing::error!("Connection error (session {}): {}", session_id, e);
                    }
                    // pooled guard dropped here → permit returned to pool
                    drop(pooled);
                } else {
                    tracing::warn!(
                        "Connection pool exhausted, rejecting session {}",
                        session_id
                    );
                    let msg = BackendMessage::ErrorResponse {
                        severity: "FATAL".into(),
                        code: "53300".into(),
                        message: "connection pool exhausted".into(),
                    };
                    let buf = codec::encode_message(&msg);
                    let mut stream = stream;
                    let _ = stream.write_all(&buf).await;
                    let _ = stream.flush().await;
                }
                cancel_reg.remove(&session_id);
                active.fetch_sub(1, Ordering::Relaxed);
                tracing::info!("Connection closed (session {})", session_id);
            });
        } else {
            // Legacy path: create handler directly per connection.
            let mut handler = if let Some(ref dist) = self.dist_engine {
                QueryHandler::new_distributed(
                    self.storage.clone(),
                    self.txn_mgr.clone(),
                    self.executor.clone(),
                    self.cluster_shard_ids.clone(),
                    dist.clone(),
                )
            } else {
                QueryHandler::new(
                    self.storage.clone(),
                    self.txn_mgr.clone(),
                    self.executor.clone(),
                )
            };
            if let Some(ref rm) = replica_metrics {
                handler.set_replica_metrics(rm.clone());
            }

            tokio::spawn(async move {
                if let Err(e) = handle_connection_with_timeout(
                    stream,
                    session_id,
                    handler,
                    timeout_ms,
                    idle_ms,
                    max_connections,
                    active.clone(),
                    auth_config,
                    cancel_reg.clone(),
                    notification_hub,
                    tls_acceptor,
                    tls_required,
                )
                .await
                {
                    tracing::error!("Connection error (session {}): {}", session_id, e);
                }
                cancel_reg.remove(&session_id);
                active.fetch_sub(1, Ordering::Relaxed);
                tracing::info!("Connection closed (session {})", session_id);
            });
        }
        true
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection_with_timeout(
    mut raw_stream: TcpStream,
    session_id: i32,
    handler: QueryHandler,
    default_statement_timeout_ms: u64,
    idle_timeout_ms: u64,
    max_connections: usize,
    active_connections: Arc<AtomicUsize>,
    auth_config: AuthConfig,
    cancel_registry: CancellationRegistry,
    notification_hub: Arc<NotificationHub>,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    tls_required: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = BytesMut::with_capacity(8192);
    let mut session = PgSession::new_with_hub(session_id, notification_hub);
    session.statement_timeout_ms = default_statement_timeout_ms;
    #[allow(unused_assignments)]
    let mut is_replication = false;

    // Generate a random secret key for this session's cancel support
    let secret_key = {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        let s = RandomState::new();
        s.build_hasher().finish() as i32
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    cancel_registry.insert(
        session_id,
        CancelEntry {
            secret_key,
            cancelled: cancelled.clone(),
        },
    );

    // Phase 0: SSL/TLS negotiation (on raw TcpStream).
    // PG protocol: if the client wants TLS, SslRequest is the first message.
    // We handle it here, then wrap the stream as PgStream for all subsequent I/O.
    let mut stream: crate::tls::PgStream = loop {
        let n = raw_stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        // Peek at the message to check for SslRequest without consuming non-SSL messages
        if buf.len() >= 8 {
            let version = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            if version == 80877103 {
                // SslRequest — consume it
                let _ = codec::decode_startup(&mut buf)?;

                if let Some(ref acceptor) = tls_acceptor {
                    // Respond 'S' (willing to upgrade)
                    raw_stream.write_all(b"S").await?;
                    raw_stream.flush().await?;

                    match acceptor.accept(raw_stream).await {
                        Ok(tls_stream) => {
                            tracing::info!("TLS handshake completed (session {})", session_id);
                            break crate::tls::PgStream::Tls(Box::new(tls_stream));
                        }
                        Err(e) => {
                            tracing::error!(
                                "TLS handshake failed (session {}): {}", session_id, e
                            );
                            return Ok(());
                        }
                    }
                } else {
                    // TLS not configured — respond 'N'
                    raw_stream.write_all(b"N").await?;
                    raw_stream.flush().await?;

                    if tls_required {
                        // TLS-1: require_ssl=true but no cert configured → reject
                        tracing::warn!(
                            "SSL required but not configured, rejecting session {}",
                            session_id
                        );
                        return Ok(());
                    }
                    // Client will send Startup next on plaintext
                    continue;
                }
            } else if version == 80877102 {
                // CancelRequest — handle on raw stream
                if let Some(FrontendMessage::CancelRequest {
                    process_id,
                    secret_key: sk,
                }) = codec::decode_startup(&mut buf)?
                {
                    if let Some(entry) = cancel_registry.get(&process_id) {
                        if entry.secret_key == sk {
                            entry.cancelled.store(true, Ordering::SeqCst);
                            tracing::info!(
                                "Cancel request accepted for session {}", process_id
                            );
                        }
                    }
                }
                return Ok(());
            } else {
                // Normal Startup message — no SSL requested
                if tls_required {
                    // TLS-1: plaintext connection when require_ssl=true → FATAL
                    if let Some(FrontendMessage::Startup { .. }) =
                        codec::decode_startup(&mut buf)?
                    {
                        let msg = BackendMessage::ErrorResponse {
                            severity: "FATAL".into(),
                            code: "08P01".into(),
                            message: "no pg_hba.conf entry for host, SSL required".into(),
                        };
                        let err_buf = codec::encode_message(&msg);
                        raw_stream.write_all(&err_buf).await?;
                        raw_stream.flush().await?;
                    }
                    return Ok(());
                }
                // Wrap as plaintext PgStream — buf still has the Startup bytes
                break crate::tls::PgStream::Plain(raw_stream);
            }
        }
        // Not enough bytes yet, keep reading
    };

    // Phase 1: Startup handshake (on PgStream — either plain or TLS)
    loop {
        // If buf already has data from Phase 0, try to decode before reading more
        if buf.is_empty() {
            let n = stream.read_buf(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }
        }

        match codec::decode_startup(&mut buf)? {
            Some(FrontendMessage::SslRequest) => {
                // Late SslRequest after initial negotiation — not supported, respond N
                stream.write_all(b"N").await?;
                stream.flush().await?;
                continue;
            }
            Some(FrontendMessage::CancelRequest {
                process_id,
                secret_key: sk,
            }) => {
                if let Some(entry) = cancel_registry.get(&process_id) {
                    if entry.secret_key == sk {
                        entry.cancelled.store(true, Ordering::SeqCst);
                        tracing::info!("Cancel request accepted for session {}", process_id);
                    } else {
                        tracing::warn!(
                            "Cancel request rejected for session {}: wrong secret key",
                            process_id
                        );
                    }
                }
                return Ok(());
            }
            Some(FrontendMessage::Startup { version, params }) => {
                tracing::debug!("Startup: version={}, params={:?}", version, params);

                if let Some(user) = params.get("user") {
                    session.user = user.clone();
                }
                if let Some(db) = params.get("database") {
                    session.database = db.clone();
                }

                // Detect replication connection
                is_replication = logical_replication::is_replication_connection(&params);

                // Copy well-known startup params into session GUC so
                // pgjdbc's SET/SHOW round-trips work correctly.
                for (key, value) in &params {
                    match key.as_str() {
                        "user" | "database" => {} // already handled above
                        "client_encoding" | "application_name" | "DateStyle" | "TimeZone"
                        | "extra_float_digits" | "options" => {
                            session.set_guc(key, value);
                        }
                        _ => {
                            // Store any other driver-supplied params
                            session.set_guc(key, value);
                        }
                    }
                }

                // Catalog-based user validation: check if user exists and can login.
                // Look up the role in the catalog.
                let catalog = handler.storage.get_catalog();
                let catalog_role = catalog.find_role_by_name(&session.user);
                let catalog_password: Option<String> = catalog_role
                    .and_then(|r| r.password_hash.clone());
                let catalog_can_login = catalog_role.is_none_or(|r| r.can_login);
                let catalog_is_superuser = catalog_role.is_some_and(|r| r.is_superuser);

                // If a role exists in the catalog but cannot login, reject immediately.
                if catalog_role.is_some() && !catalog_can_login {
                    let msg = BackendMessage::ErrorResponse {
                        severity: "FATAL".into(),
                        code: "28000".into(),
                        message: format!(
                            "role \"{}\" is not permitted to log in",
                            session.user
                        ),
                    };
                    send_message(&mut stream, &msg).await?;
                    return Ok(());
                }

                // Username check (if configured via auth_config)
                if !auth_config.username.is_empty() && session.user != auth_config.username {
                    let msg = BackendMessage::ErrorResponse {
                        severity: "FATAL".into(),
                        code: "28P01".into(),
                        message: format!(
                            "password authentication failed for user \"{}\"",
                            session.user
                        ),
                    };
                    send_message(&mut stream, &msg).await?;
                    return Ok(());
                }

                // Determine the effective password for authentication.
                // Catalog password takes priority if set; otherwise fall back to auth_config.
                let effective_password = catalog_password
                    .unwrap_or_else(|| auth_config.password.clone());

                // Set is_superuser parameter based on catalog lookup
                drop(catalog);

                // Authentication handshake based on configured method
                match auth_config.method {
                    AuthMethod::Trust => {
                        send_message(&mut stream, &BackendMessage::AuthenticationOk).await?;
                    }
                    AuthMethod::Password => {
                        // Request cleartext password
                        send_message(
                            &mut stream,
                            &BackendMessage::AuthenticationCleartextPassword,
                        )
                        .await?;
                        stream.flush().await?;

                        // Read password response
                        let password = loop {
                            let pn = stream.read_buf(&mut buf).await?;
                            if pn == 0 {
                                return Ok(());
                            }
                            if let Some(FrontendMessage::PasswordMessage(pw)) =
                                codec::decode_message(&mut buf)?
                            {
                                break pw;
                            }
                        };

                        if password != effective_password {
                            let msg = BackendMessage::ErrorResponse {
                                severity: "FATAL".into(),
                                code: "28P01".into(),
                                message: format!(
                                    "password authentication failed for user \"{}\"",
                                    session.user
                                ),
                            };
                            send_message(&mut stream, &msg).await?;
                            return Ok(());
                        }
                        send_message(&mut stream, &BackendMessage::AuthenticationOk).await?;
                    }
                    AuthMethod::Md5 => {
                        // Generate random 4-byte salt
                        use std::collections::hash_map::RandomState;
                        use std::hash::{BuildHasher, Hasher};
                        let salt: [u8; 4] = {
                            let s = RandomState::new();
                            let h = s.build_hasher().finish();
                            (h as u32).to_le_bytes()
                        };
                        send_message(
                            &mut stream,
                            &BackendMessage::AuthenticationMd5Password { salt },
                        )
                        .await?;
                        stream.flush().await?;

                        // Read password response
                        let client_hash = loop {
                            let pn = stream.read_buf(&mut buf).await?;
                            if pn == 0 {
                                return Ok(());
                            }
                            if let Some(FrontendMessage::PasswordMessage(pw)) =
                                codec::decode_message(&mut buf)?
                            {
                                break pw;
                            }
                        };

                        // PG MD5 auth: md5(md5(password + user) + salt)
                        use md5::Digest;
                        let inner = {
                            let mut hasher = md5::Md5::new();
                            hasher.update(effective_password.as_bytes());
                            hasher.update(session.user.as_bytes());
                            format!("{:x}", hasher.finalize())
                        };
                        let expected = {
                            let mut hasher = md5::Md5::new();
                            hasher.update(inner.as_bytes());
                            hasher.update(salt);
                            format!("md5{:x}", hasher.finalize())
                        };

                        if client_hash != expected {
                            let msg = BackendMessage::ErrorResponse {
                                severity: "FATAL".into(),
                                code: "28P01".into(),
                                message: format!(
                                    "password authentication failed for user \"{}\"",
                                    session.user
                                ),
                            };
                            send_message(&mut stream, &msg).await?;
                            return Ok(());
                        }
                        send_message(&mut stream, &BackendMessage::AuthenticationOk).await?;
                    }
                    AuthMethod::ScramSha256 => {
                        // ── SCRAM-SHA-256 SASL authentication ──
                        use crate::auth::scram::{ScramVerifier, ScramServerSession, SCRAM_SHA_256};

                        // Look up SCRAM verifier for this user:
                        // 1. Check auth_config.users list for a matching entry
                        // 2. Fall back to generating an ephemeral verifier from effective_password
                        let verifier = auth_config
                            .users
                            .iter()
                            .find(|u| u.username == session.user)
                            .and_then(|u| {
                                if !u.scram.is_empty() {
                                    ScramVerifier::parse(&u.scram).ok()
                                } else if !u.password.is_empty() {
                                    Some(ScramVerifier::generate(&u.password, 4096))
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_else(|| {
                                // Fallback: generate ephemeral verifier from effective_password
                                ScramVerifier::generate(&effective_password, 4096)
                            });

                        let mut scram_session = ScramServerSession::new(verifier);

                        // Step 1: Send AuthenticationSASL with mechanism list
                        send_message(
                            &mut stream,
                            &BackendMessage::AuthenticationSASL {
                                mechanisms: vec![SCRAM_SHA_256.into()],
                            },
                        )
                        .await?;
                        stream.flush().await?;

                        // Step 2: Receive SASLInitialResponse (client-first-message)
                        let (mechanism, client_first_data) = loop {
                            let pn = stream.read_buf(&mut buf).await?;
                            if pn == 0 {
                                return Ok(());
                            }
                            if let Some(FrontendMessage::SASLInitialResponse {
                                mechanism,
                                data,
                            }) = codec::decode_sasl_initial_response(&mut buf)?
                            {
                                break (mechanism, data);
                            }
                        };

                        let client_first_msg = String::from_utf8_lossy(&client_first_data);

                        // Step 3: Process client-first, send server-first
                        let server_first_data = match scram_session
                            .handle_client_first(&mechanism, &client_first_msg)
                        {
                            Ok(data) => data,
                            Err(e) => {
                                tracing::warn!("SCRAM client-first failed for user \"{}\": {e}", session.user);
                                let msg = BackendMessage::ErrorResponse {
                                    severity: "FATAL".into(),
                                    code: "28000".into(),
                                    message: format!(
                                        "SASL authentication failed for user \"{}\"",
                                        session.user
                                    ),
                                };
                                send_message(&mut stream, &msg).await?;
                                return Ok(());
                            }
                        };
                        send_message(
                            &mut stream,
                            &BackendMessage::AuthenticationSASLContinue {
                                data: server_first_data,
                            },
                        )
                        .await?;
                        stream.flush().await?;

                        // Step 4: Receive SASLResponse (client-final-message)
                        let client_final_data = loop {
                            let pn = stream.read_buf(&mut buf).await?;
                            if pn == 0 {
                                return Ok(());
                            }
                            if let Some(FrontendMessage::SASLResponse { data }) =
                                codec::decode_sasl_response(&mut buf)?
                            {
                                break data;
                            }
                        };

                        let client_final_msg = String::from_utf8_lossy(&client_final_data);

                        // Step 5: Verify client proof and get server-final
                        let server_final_data =
                            match scram_session.handle_client_final(&client_final_msg) {
                                Ok(data) => data,
                                Err(_) => {
                                    tracing::warn!(
                                        "SCRAM authentication failed for user \"{}\"",
                                        session.user
                                    );
                                    let msg = BackendMessage::ErrorResponse {
                                        severity: "FATAL".into(),
                                        code: "28P01".into(),
                                        message: format!(
                                            "password authentication failed for user \"{}\"",
                                            session.user
                                        ),
                                    };
                                    send_message(&mut stream, &msg).await?;
                                    return Ok(());
                                }
                            };

                        // Step 6: Send AuthenticationSASLFinal + AuthenticationOk
                        send_message(
                            &mut stream,
                            &BackendMessage::AuthenticationSASLFinal {
                                data: server_final_data,
                            },
                        )
                        .await?;
                        send_message(&mut stream, &BackendMessage::AuthenticationOk).await?;
                    }
                }

                // Send initial parameter statuses (must match session GUC values).
                // pgjdbc parses server_version to determine feature support.
                // Use session GUC values so client-supplied startup params are reflected.
                let app_name = session
                    .get_guc("application_name")
                    .unwrap_or("").to_owned();
                let client_enc = session
                    .get_guc("client_encoding")
                    .unwrap_or("UTF8").to_owned();
                let datestyle = session
                    .get_guc("datestyle")
                    .unwrap_or("ISO, MDY").to_owned();
                let timezone = session.get_guc("timezone").unwrap_or("UTC").to_owned();
                let startup_params: Vec<(&str, String)> = vec![
                    ("server_version", "18.0.0".into()),
                    ("server_version_num", "180000".into()),
                    ("server_encoding", "UTF8".into()),
                    ("client_encoding", client_enc),
                    ("DateStyle", datestyle),
                    ("integer_datetimes", "on".into()),
                    ("standard_conforming_strings", "on".into()),
                    ("TimeZone", timezone),
                    ("is_superuser", if catalog_is_superuser { "on" } else { "off" }.into()),
                    ("session_authorization", session.user.clone()),
                    ("IntervalStyle", "postgres".into()),
                    ("application_name", app_name),
                ];
                for (name, value) in &startup_params {
                    send_message(
                        &mut stream,
                        &BackendMessage::ParameterStatus {
                            name: name.to_string(),
                            value: value.clone(),
                        },
                    )
                    .await?;
                }

                // Backend key data (used by client for cancel requests)
                send_message(
                    &mut stream,
                    &BackendMessage::BackendKeyData {
                        process_id: session_id,
                        secret_key,
                    },
                )
                .await?;

                // Ready for query
                send_message(
                    &mut stream,
                    &BackendMessage::ReadyForQuery {
                        txn_status: session.txn_status_byte(),
                    },
                )
                .await?;

                break;
            }
            None => continue,
            _ => {
                return Err("Unexpected message during startup".into());
            }
        }
    }

    // Post-startup connection limit check.
    // We check here (after the handshake) so the client receives a properly-framed
    // FATAL error message it can display, rather than raw bytes before startup.
    if max_connections > 0 {
        let current = active_connections.load(Ordering::Relaxed);
        if current > max_connections {
            tracing::warn!(
                "Connection rejected post-startup: {} active (max {}), session {}",
                current,
                max_connections,
                session_id
            );
            let msg = BackendMessage::ErrorResponse {
                severity: "FATAL".into(),
                code: "53300".into(),
                message: format!(
                    "sorry, too many clients already ({current} of {max_connections} connections used)"
                ),
            };
            let _ = send_message(&mut stream, &msg).await;
            return Ok(());
        }
    }

    // Phase 2a: Replication-mode query loop (if replication=database)
    if is_replication {
        tracing::info!("Entering replication mode for session {}", session_id);
        return handle_replication_session(
            &mut stream,
            &mut buf,
            &session,
            session_id,
            &handler,
            idle_timeout_ms,
        )
        .await;
    }

    // Phase 2: Query loop
    loop {
        let n = if idle_timeout_ms > 0 {
            let idle_dur = std::time::Duration::from_millis(idle_timeout_ms);
            if let Ok(result) = tokio::time::timeout(idle_dur, stream.read_buf(&mut buf)).await { result? } else {
                tracing::info!(
                    "Idle timeout ({}ms) for session {}",
                    idle_timeout_ms,
                    session_id
                );
                let msg = BackendMessage::ErrorResponse {
                    severity: "FATAL".into(),
                    code: "57P01".into(),
                    message: format!(
                        "terminating connection due to idle timeout ({idle_timeout_ms}ms)"
                    ),
                };
                let _ = send_message(&mut stream, &msg).await;
                return Ok(());
            }
        } else {
            stream.read_buf(&mut buf).await?
        };
        if n == 0 {
            // Connection closed by client
            if let Some(ref txn) = session.txn {
                // Abort any open transaction
                tracing::debug!("Aborting open transaction on disconnect: {}", txn.txn_id);
            }
            return Ok(());
        }

        // When true, discard all extended-query messages until Sync
        // (per PG protocol: error during extended query → skip to Sync).
        let mut extended_error = false;

        while let Some(msg) = codec::decode_message(&mut buf)? {
            // If in error state, discard everything except Sync
            if extended_error {
                if matches!(msg, FrontendMessage::Sync) {
                    extended_error = false;
                    send_message(
                        &mut stream,
                        &BackendMessage::ReadyForQuery {
                            txn_status: session.txn_status_byte(),
                        },
                    )
                    .await?;
                }
                // Discard Bind/Describe/Execute/Close/Flush until Sync
                continue;
            }

            match msg {
                FrontendMessage::Query(sql) => {
                    tracing::debug!("Query (session {}): {}", session_id, sql);

                    // Check for cancellation
                    if cancelled.swap(false, Ordering::SeqCst) {
                        tracing::info!("Query cancelled for session {}", session_id);
                        send_message(
                            &mut stream,
                            &BackendMessage::ErrorResponse {
                                severity: "ERROR".into(),
                                code: "57014".into(),
                                message: "canceling statement due to user request".into(),
                            },
                        )
                        .await?;
                        send_message(
                            &mut stream,
                            &BackendMessage::ReadyForQuery {
                                txn_status: session.txn_status_byte(),
                            },
                        )
                        .await?;
                        continue;
                    }

                    // Check for SET statement_timeout
                    if let Some(timeout_ms) = parse_set_statement_timeout(&sql) {
                        session.statement_timeout_ms = timeout_ms;
                        send_message(
                            &mut stream,
                            &BackendMessage::CommandComplete { tag: "SET".into() },
                        )
                        .await?;
                        send_message(
                            &mut stream,
                            &BackendMessage::ReadyForQuery {
                                txn_status: session.txn_status_byte(),
                            },
                        )
                        .await?;
                        continue;
                    }

                    let responses = if session.statement_timeout_ms > 0 {
                        let timeout_dur =
                            std::time::Duration::from_millis(session.statement_timeout_ms);
                        let handler_ref = &handler;
                        match tokio::time::timeout(
                            timeout_dur,
                            tokio::task::spawn_blocking({
                                let sql = sql.clone();
                                let handler = handler_ref.clone();
                                let mut sess = session.take_for_timeout();
                                move || {
                                    let responses = handler.handle_query(&sql, &mut sess);
                                    (responses, sess)
                                }
                            }),
                        )
                        .await
                        {
                            Ok(Ok((responses, returned_session))) => {
                                session.restore_from_timeout(returned_session);
                                responses
                            }
                            Ok(Err(e)) => {
                                vec![BackendMessage::ErrorResponse {
                                    severity: "ERROR".into(),
                                    code: "XX000".into(),
                                    message: format!("Internal error: {e}"),
                                }]
                            }
                            Err(_) => {
                                tracing::warn!(
                                    "Statement timeout ({}ms) for session {}",
                                    session.statement_timeout_ms,
                                    session_id
                                );
                                vec![BackendMessage::ErrorResponse {
                                    severity: "ERROR".into(),
                                    code: "57014".into(),
                                    message: format!(
                                        "canceling statement due to statement timeout ({}ms)",
                                        session.statement_timeout_ms
                                    ),
                                }]
                            }
                        }
                    } else {
                        // No statement_timeout: run in spawn_blocking so we can
                        // poll the cancellation flag concurrently.
                        let handler_ref = &handler;
                        let query_future = tokio::task::spawn_blocking({
                            let sql = sql.clone();
                            let handler = handler_ref.clone();
                            let mut sess = session.take_for_timeout();
                            move || {
                                let responses = handler.handle_query(&sql, &mut sess);
                                (responses, sess)
                            }
                        });

                        let cancel_flag = cancelled.clone();
                        let cancel_poll = async {
                            loop {
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                if cancel_flag.load(Ordering::SeqCst) {
                                    break;
                                }
                            }
                        };

                        tokio::select! {
                            result = query_future => {
                                match result {
                                    Ok((responses, returned_session)) => {
                                        session.restore_from_timeout(returned_session);
                                        // Check if cancel arrived just as query finished
                                        if cancelled.swap(false, Ordering::SeqCst) {
                                            vec![BackendMessage::ErrorResponse {
                                                severity: "ERROR".into(),
                                                code: "57014".into(),
                                                message: "canceling statement due to user request".into(),
                                            }]
                                        } else {
                                            responses
                                        }
                                    }
                                    Err(e) => {
                                        vec![BackendMessage::ErrorResponse {
                                            severity: "ERROR".into(),
                                            code: "XX000".into(),
                                            message: format!("Internal error: {e}"),
                                        }]
                                    }
                                }
                            }
                            _ = cancel_poll => {
                                cancelled.store(false, Ordering::SeqCst);
                                tracing::info!("Query cancelled mid-execution for session {}", session_id);
                                vec![BackendMessage::ErrorResponse {
                                    severity: "ERROR".into(),
                                    code: "57014".into(),
                                    message: "canceling statement due to user request".into(),
                                }]
                            }
                        }
                    };

                    for response in &responses {
                        send_message(&mut stream, response).await?;
                    }

                    // COPY FROM STDIN sub-protocol: if copy_state is set, the handler
                    // returned CopyInResponse. Enter receive mode for CopyData messages.
                    if session.copy_state.is_some() {
                        let mut copy_buf = Vec::new();
                        loop {
                            let cn = stream.read_buf(&mut buf).await?;
                            if cn == 0 {
                                // Connection closed during COPY
                                session.copy_state = None;
                                return Ok(());
                            }
                            while let Some(copy_msg) = codec::decode_message(&mut buf)? {
                                match copy_msg {
                                    FrontendMessage::CopyData(data) => {
                                        copy_buf.extend_from_slice(&data);
                                    }
                                    FrontendMessage::CopyDone => {
                                        // Process all collected data
                                        let result_msgs =
                                            handler.handle_copy_data(&copy_buf, &mut session);
                                        for msg in &result_msgs {
                                            send_message(&mut stream, msg).await?;
                                        }
                                        // Send ReadyForQuery after COPY completes
                                        send_message(
                                            &mut stream,
                                            &BackendMessage::ReadyForQuery {
                                                txn_status: session.txn_status_byte(),
                                            },
                                        )
                                        .await?;
                                        // Break out of COPY receive loop
                                        copy_buf.clear();
                                        // Use a flag to break the outer read loop too
                                        break;
                                    }
                                    FrontendMessage::CopyFail(reason) => {
                                        tracing::warn!(
                                            "COPY FROM STDIN failed (session {}): {}",
                                            session_id,
                                            reason
                                        );
                                        session.copy_state = None;
                                        send_message(
                                            &mut stream,
                                            &BackendMessage::ErrorResponse {
                                                severity: "ERROR".into(),
                                                code: "57014".into(),
                                                message: format!(
                                                    "COPY FROM STDIN failed: {reason}"
                                                ),
                                            },
                                        )
                                        .await?;
                                        send_message(
                                            &mut stream,
                                            &BackendMessage::ReadyForQuery {
                                                txn_status: session.txn_status_byte(),
                                            },
                                        )
                                        .await?;
                                        break;
                                    }
                                    _ => {
                                        // Unexpected message during COPY — abort
                                        tracing::warn!(
                                            "Unexpected message during COPY (session {})",
                                            session_id
                                        );
                                        session.copy_state = None;
                                        break;
                                    }
                                }
                            }
                            // If copy_state was cleared, we're done with COPY
                            if session.copy_state.is_none() {
                                break;
                            }
                        }
                        continue; // Skip the ReadyForQuery below — already sent
                    }

                    // Deliver pending LISTEN notifications before ReadyForQuery
                    for notif in session.notifications.drain_pending() {
                        send_message(
                            &mut stream,
                            &BackendMessage::NotificationResponse {
                                process_id: notif.sender_pid,
                                channel: notif.channel,
                                payload: notif.payload,
                            },
                        )
                        .await?;
                    }

                    // Always send ReadyForQuery after query processing
                    send_message(
                        &mut stream,
                        &BackendMessage::ReadyForQuery {
                            txn_status: session.txn_status_byte(),
                        },
                    )
                    .await?;
                }
                FrontendMessage::Parse {
                    name,
                    query,
                    param_types,
                } => {
                    tracing::debug!(
                        "Parse (session {}): name={}, query={}, params={}",
                        session_id,
                        name,
                        query,
                        param_types.len()
                    );
                    // Try to parse+bind+plan the query for the plan-based path.
                    let parse_start = std::time::Instant::now();
                    let (plan, inferred_param_types, row_desc) = match handler
                        .prepare_statement(&query)
                    {
                        Ok((p, ipt, rd)) => {
                            let dur = parse_start.elapsed().as_micros() as u64;
                            falcon_observability::record_prepared_stmt_parse_duration_us(dur, true);
                            falcon_observability::record_prepared_stmt_op("parse", "plan");
                            (Some(p), ipt, rd)
                        }
                        Err(_e) => {
                            let dur = parse_start.elapsed().as_micros() as u64;
                            falcon_observability::record_prepared_stmt_parse_duration_us(
                                dur, false,
                            );
                            falcon_observability::record_prepared_stmt_op("parse", "legacy");
                            // Fall back to legacy text-substitution path
                            (None, vec![], vec![])
                        }
                    };
                    // If client declared param type OIDs, use them for ParameterDescription;
                    // otherwise use the inferred types mapped to OIDs.
                    let effective_param_oids = if !param_types.is_empty() {
                        param_types.clone()
                    } else {
                        inferred_param_types
                            .iter()
                            .map(|t| handler.datatype_to_oid(t.as_ref()))
                            .collect()
                    };
                    session.prepared_statements.insert(
                        name.clone(),
                        crate::session::PreparedStatement {
                            query: query.clone(),
                            param_types: effective_param_oids,
                            plan,
                            inferred_param_types,
                            row_desc,
                        },
                    );
                    falcon_observability::record_prepared_stmt_active(
                        session.prepared_statements.len(),
                    );
                    send_message(&mut stream, &BackendMessage::ParseComplete).await?;
                }
                FrontendMessage::Bind {
                    portal,
                    statement,
                    param_formats,
                    param_values,
                    ..
                } => {
                    tracing::debug!(
                        "Bind (session {}): portal={}, stmt={}, params={}, formats={}",
                        session_id,
                        portal,
                        statement,
                        param_values.len(),
                        param_formats.len()
                    );
                    let bind_start = std::time::Instant::now();
                    falcon_observability::record_prepared_stmt_param_count(param_values.len());
                    let ps = session.prepared_statements.get(&statement);
                    let (plan, params_datum, bound_sql) = if let Some(ps) = ps {
                        let datum_params: Vec<falcon_common::datum::Datum> = param_values
                            .iter()
                            .enumerate()
                            .map(|(i, pv)| {
                                let fmt = resolve_param_format(&param_formats, i);
                                let type_hint =
                                    ps.inferred_param_types.get(i).and_then(|t| t.as_ref());
                                if fmt == 1 {
                                    decode_param_value_binary(pv, type_hint)
                                } else {
                                    decode_param_value(pv, type_hint)
                                }
                            })
                            .collect();
                        let bound_sql = bind_params(&ps.query, &param_values);
                        (ps.plan.clone(), datum_params, bound_sql)
                    } else {
                        (None, vec![], String::new())
                    };
                    let path = if plan.is_some() { "plan" } else { "legacy" };
                    session.portals.insert(
                        portal.clone(),
                        crate::session::Portal {
                            plan,
                            params: params_datum,
                            bound_sql,
                        },
                    );
                    let bind_dur = bind_start.elapsed().as_micros() as u64;
                    falcon_observability::record_prepared_stmt_bind_duration_us(bind_dur);
                    falcon_observability::record_prepared_stmt_op("bind", path);
                    falcon_observability::record_prepared_stmt_portals_active(
                        session.portals.len(),
                    );
                    send_message(&mut stream, &BackendMessage::BindComplete).await?;
                }
                FrontendMessage::Describe { kind, name } => {
                    tracing::debug!(
                        "Describe (session {}): kind={}, name={}",
                        session_id,
                        kind as char,
                        name
                    );
                    falcon_observability::record_prepared_stmt_op(
                        "describe",
                        if kind == b'S' { "statement" } else { "portal" },
                    );

                    if kind == b'S' {
                        // Statement describe: ParameterDescription + RowDescription/NoData
                        if let Some(ps) = session.prepared_statements.get(&name) {
                            send_message(
                                &mut stream,
                                &BackendMessage::ParameterDescription {
                                    type_oids: ps.param_types.clone(),
                                },
                            )
                            .await?;
                            // Use stored row_desc if available
                            if !ps.row_desc.is_empty() {
                                let fields = ps
                                    .row_desc
                                    .iter()
                                    .map(|fd| crate::codec::FieldDescription {
                                        name: fd.name.clone(),
                                        table_oid: 0,
                                        column_attr: 0,
                                        type_oid: fd.type_oid,
                                        type_len: fd.type_len,
                                        type_modifier: -1,
                                        format_code: 0,
                                    })
                                    .collect::<Vec<_>>();
                                send_message(
                                    &mut stream,
                                    &BackendMessage::RowDescription { fields },
                                )
                                .await?;
                            } else {
                                // Fallback: describe by re-parsing
                                match handler.describe_query(&ps.query) {
                                    Ok(fields) if !fields.is_empty() => {
                                        send_message(
                                            &mut stream,
                                            &BackendMessage::RowDescription { fields },
                                        )
                                        .await?;
                                    }
                                    _ => {
                                        send_message(&mut stream, &BackendMessage::NoData).await?;
                                    }
                                }
                            }
                        } else {
                            send_message(
                                &mut stream,
                                &BackendMessage::ParameterDescription { type_oids: vec![] },
                            )
                            .await?;
                            send_message(&mut stream, &BackendMessage::NoData).await?;
                        }
                    } else {
                        // Portal describe
                        let sql_for_describe =
                            session.portals.get(&name).map(|p| p.bound_sql.clone());
                        if let Some(ref sql) = sql_for_describe {
                            if !sql.is_empty() {
                                match handler.describe_query(sql) {
                                    Ok(fields) if !fields.is_empty() => {
                                        send_message(
                                            &mut stream,
                                            &BackendMessage::RowDescription { fields },
                                        )
                                        .await?;
                                    }
                                    _ => {
                                        send_message(&mut stream, &BackendMessage::NoData).await?;
                                    }
                                }
                            } else {
                                send_message(&mut stream, &BackendMessage::NoData).await?;
                            }
                        } else {
                            send_message(&mut stream, &BackendMessage::NoData).await?;
                        }
                    }
                }
                FrontendMessage::Execute { portal, .. } => {
                    tracing::debug!("Execute (session {}): portal={}", session_id, portal);

                    // Check for cancellation before starting execution
                    if cancelled.swap(false, Ordering::SeqCst) {
                        tracing::info!("Execute cancelled for session {}", session_id);
                        send_message(
                            &mut stream,
                            &BackendMessage::ErrorResponse {
                                severity: "ERROR".into(),
                                code: "57014".into(),
                                message: "canceling statement due to user request".into(),
                            },
                        )
                        .await?;
                        extended_error = true;
                        continue;
                    }

                    let exec_start = std::time::Instant::now();
                    let portal_data = session.portals.get(&portal).cloned();
                    if let Some(p) = portal_data {
                        // Helper: run the execution with optional statement timeout.
                        let responses: Vec<BackendMessage> = if session.statement_timeout_ms > 0 {
                            let timeout_dur =
                                std::time::Duration::from_millis(session.statement_timeout_ms);
                            let handler_ref = &handler;
                            match tokio::time::timeout(
                                timeout_dur,
                                tokio::task::spawn_blocking({
                                    let p = p.clone();
                                    let handler = handler_ref.clone();
                                    let mut sess = session.take_for_timeout();
                                    move || {
                                        let responses = if let Some(ref plan) = p.plan {
                                            falcon_observability::record_prepared_stmt_op(
                                                "execute", "plan",
                                            );
                                            handler.execute_plan(plan, &p.params, &mut sess)
                                        } else if !p.bound_sql.is_empty() {
                                            falcon_observability::record_prepared_stmt_op(
                                                "execute", "legacy",
                                            );
                                            handler.handle_query(&p.bound_sql, &mut sess)
                                        } else {
                                            falcon_observability::record_prepared_stmt_op(
                                                "execute", "empty",
                                            );
                                            vec![BackendMessage::EmptyQueryResponse]
                                        };
                                        (responses, sess)
                                    }
                                }),
                            )
                            .await
                            {
                                Ok(Ok((responses, returned_session))) => {
                                    session.restore_from_timeout(returned_session);
                                    responses
                                }
                                Ok(Err(e)) => vec![BackendMessage::ErrorResponse {
                                    severity: "ERROR".into(),
                                    code: "XX000".into(),
                                    message: format!("Internal error: {e}"),
                                }],
                                Err(_) => {
                                    tracing::warn!(
                                        "Execute timeout ({}ms) for session {}",
                                        session.statement_timeout_ms,
                                        session_id
                                    );
                                    vec![BackendMessage::ErrorResponse {
                                        severity: "ERROR".into(),
                                        code: "57014".into(),
                                        message: format!(
                                            "canceling statement due to statement timeout ({}ms)",
                                            session.statement_timeout_ms
                                        ),
                                    }]
                                }
                            }
                        } else {
                            // No statement_timeout: run in spawn_blocking so we can
                            // poll the cancellation flag concurrently.
                            let handler_ref = &handler;
                            let query_future = tokio::task::spawn_blocking({
                                let p = p.clone();
                                let handler = handler_ref.clone();
                                let mut sess = session.take_for_timeout();
                                move || {
                                    let responses = if let Some(ref plan) = p.plan {
                                        falcon_observability::record_prepared_stmt_op(
                                            "execute", "plan",
                                        );
                                        handler.execute_plan(plan, &p.params, &mut sess)
                                    } else if !p.bound_sql.is_empty() {
                                        falcon_observability::record_prepared_stmt_op(
                                            "execute", "legacy",
                                        );
                                        handler.handle_query(&p.bound_sql, &mut sess)
                                    } else {
                                        falcon_observability::record_prepared_stmt_op(
                                            "execute", "empty",
                                        );
                                        vec![BackendMessage::EmptyQueryResponse]
                                    };
                                    (responses, sess)
                                }
                            });

                            let cancel_flag = cancelled.clone();
                            let cancel_poll = async {
                                loop {
                                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                    if cancel_flag.load(Ordering::SeqCst) {
                                        break;
                                    }
                                }
                            };

                            tokio::select! {
                                result = query_future => {
                                    match result {
                                        Ok((responses, returned_session)) => {
                                            session.restore_from_timeout(returned_session);
                                            if cancelled.swap(false, Ordering::SeqCst) {
                                                vec![BackendMessage::ErrorResponse {
                                                    severity: "ERROR".into(),
                                                    code: "57014".into(),
                                                    message: "canceling statement due to user request".into(),
                                                }]
                                            } else {
                                                responses
                                            }
                                        }
                                        Err(e) => {
                                            vec![BackendMessage::ErrorResponse {
                                                severity: "ERROR".into(),
                                                code: "XX000".into(),
                                                message: format!("Internal error: {e}"),
                                            }]
                                        }
                                    }
                                }
                                _ = cancel_poll => {
                                    cancelled.store(false, Ordering::SeqCst);
                                    tracing::info!("Execute cancelled mid-execution for session {}", session_id);
                                    vec![BackendMessage::ErrorResponse {
                                        severity: "ERROR".into(),
                                        code: "57014".into(),
                                        message: "canceling statement due to user request".into(),
                                    }]
                                }
                            }
                        };

                        let exec_dur = exec_start.elapsed().as_micros() as u64;
                        let has_error = responses
                            .iter()
                            .any(|r| matches!(r, BackendMessage::ErrorResponse { .. }));
                        falcon_observability::record_prepared_stmt_execute_duration_us(
                            exec_dur, !has_error,
                        );
                        for response in &responses {
                            send_message(&mut stream, response).await?;
                        }
                        if has_error {
                            // Per PG protocol: error during extended query → discard until Sync
                            extended_error = true;
                        }
                    } else {
                        falcon_observability::record_prepared_stmt_op("execute", "empty");
                        send_message(&mut stream, &BackendMessage::EmptyQueryResponse).await?;
                    }
                }
                FrontendMessage::Sync => {
                    send_message(
                        &mut stream,
                        &BackendMessage::ReadyForQuery {
                            txn_status: session.txn_status_byte(),
                        },
                    )
                    .await?;
                }
                FrontendMessage::Close { kind, name } => {
                    if kind == b'S' {
                        session.prepared_statements.remove(&name);
                        falcon_observability::record_prepared_stmt_op("close", "statement");
                        falcon_observability::record_prepared_stmt_active(
                            session.prepared_statements.len(),
                        );
                    } else {
                        session.portals.remove(&name);
                        falcon_observability::record_prepared_stmt_op("close", "portal");
                        falcon_observability::record_prepared_stmt_portals_active(
                            session.portals.len(),
                        );
                    }
                    send_message(&mut stream, &BackendMessage::CloseComplete).await?;
                }
                FrontendMessage::Flush => {
                    stream.flush().await?;
                }
                FrontendMessage::Terminate => {
                    tracing::debug!("Client terminated (session {})", session_id);
                    return Ok(());
                }
                _ => {
                    tracing::warn!("Unexpected message in query phase (session {})", session_id);
                }
            }
        }
    }
}

/// Handle a replication-mode session (replication=database).
///
/// Replication commands arrive as simple Query messages. When START_REPLICATION
/// is received, we enter CopyBoth streaming mode and send XLogData messages
/// from the CDC buffer, with periodic keepalives.
async fn handle_replication_session<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    session: &PgSession,
    session_id: i32,
    handler: &QueryHandler,
    idle_timeout_ms: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let n = if idle_timeout_ms > 0 {
            let idle_dur = std::time::Duration::from_millis(idle_timeout_ms);
            if let Ok(result) = tokio::time::timeout(idle_dur, stream.read_buf(buf)).await { result? } else {
                let msg = BackendMessage::ErrorResponse {
                    severity: "FATAL".into(),
                    code: "57P01".into(),
                    message: format!(
                        "terminating replication connection due to idle timeout ({idle_timeout_ms}ms)"
                    ),
                };
                let _ = send_message(stream, &msg).await;
                return Ok(());
            }
        } else {
            stream.read_buf(buf).await?
        };
        if n == 0 {
            return Ok(());
        }

        while let Some(msg) = codec::decode_message(buf)? {
            match msg {
                FrontendMessage::Query(sql) => {
                    tracing::debug!("Replication query (session {}): {}", session_id, sql);

                    let result =
                        logical_replication::handle_replication_command(&sql, &handler.storage);

                    match result {
                        logical_replication::ReplicationResult::Messages(msgs) => {
                            for m in &msgs {
                                send_message(stream, m).await?;
                            }
                            send_message(
                                stream,
                                &BackendMessage::ReadyForQuery {
                                    txn_status: session.txn_status_byte(),
                                },
                            )
                            .await?;
                        }
                        logical_replication::ReplicationResult::StartStreaming {
                            slot_name,
                            start_lsn,
                        } => {
                            // Enter CopyBoth mode
                            send_message(
                                stream,
                                &BackendMessage::CopyBothResponse {
                                    format: 0,
                                    column_formats: vec![],
                                },
                            )
                            .await?;

                            // Stream CDC events from the slot
                            handle_replication_streaming(
                                stream,
                                buf,
                                &handler.storage,
                                &slot_name,
                                start_lsn,
                                session_id,
                            )
                            .await?;

                            // After streaming ends, send CopyDone + ReadyForQuery
                            send_message(stream, &BackendMessage::CopyDone).await?;
                            send_message(
                                stream,
                                &BackendMessage::ReadyForQuery {
                                    txn_status: session.txn_status_byte(),
                                },
                            )
                            .await?;
                        }
                        logical_replication::ReplicationResult::Error(e) => {
                            send_message(
                                stream,
                                &BackendMessage::ErrorResponse {
                                    severity: "ERROR".into(),
                                    code: "XX000".into(),
                                    message: e,
                                },
                            )
                            .await?;
                            send_message(
                                stream,
                                &BackendMessage::ReadyForQuery {
                                    txn_status: session.txn_status_byte(),
                                },
                            )
                            .await?;
                        }
                    }
                }
                FrontendMessage::Terminate => {
                    tracing::debug!("Replication client terminated (session {})", session_id);
                    return Ok(());
                }
                _ => {
                    tracing::warn!(
                        "Unexpected message in replication mode (session {})",
                        session_id
                    );
                }
            }
        }
    }
}

/// Handle the streaming phase of START_REPLICATION.
///
/// Polls CDC events from the slot, sends them as XLogData CopyData messages,
/// and sends periodic keepalives. Exits when the client sends CopyDone or
/// the connection closes.
async fn handle_replication_streaming<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    storage: &Arc<StorageEngine>,
    slot_name: &str,
    _start_lsn: u64,
    session_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let keepalive_interval = std::time::Duration::from_secs(10);
    let poll_interval = std::time::Duration::from_millis(100);
    let mut last_keepalive = std::time::Instant::now();
    let mut current_lsn: u64 = 0;

    loop {
        // Poll for new CDC events — collect into local vec to avoid holding
        // the RwLockReadGuard across .await points.
        let pending_messages: Vec<(u64, Vec<u8>)> = {
            let cdc = storage.cdc_manager.read();
            if let Some(slot) = cdc.get_slot_by_name(slot_name) {
                let slot_id = slot.id;
                cdc.poll_changes(slot_id, 100)
                    .iter()
                    .map(|event| {
                        let text = logical_replication::encode_change_event_text(event);
                        let xlog_data = logical_replication::build_xlog_data_message(
                            event.lsn.0,
                            text.as_bytes(),
                        );
                        (event.lsn.0, xlog_data)
                    })
                    .collect()
            } else {
                vec![]
            }
        }; // RwLockReadGuard dropped here

        // Send collected messages (no lock held)
        for (lsn, xlog_data) in &pending_messages {
            send_message(stream, &BackendMessage::CopyData(xlog_data.clone())).await?;
            current_lsn = *lsn;
        }

        // Advance the slot's confirmed flush position
        if current_lsn > 0 && !pending_messages.is_empty() {
            let cdc = storage.cdc_manager.read();
            if let Some(slot) = cdc.get_slot_by_name(slot_name) {
                let slot_id = slot.id;
                drop(cdc);
                let mut cdc_w = storage.cdc_manager.write();
                let _ = cdc_w.advance_slot(slot_id, falcon_storage::cdc::CdcLsn(current_lsn));
            }
        }

        // Send keepalive if interval elapsed
        if last_keepalive.elapsed() >= keepalive_interval {
            let keepalive = logical_replication::build_keepalive_message(current_lsn, false);
            send_message(stream, &BackendMessage::CopyData(keepalive)).await?;
            last_keepalive = std::time::Instant::now();
        }

        // Check for client messages (CopyDone, StandbyStatusUpdate, or disconnect)
        match tokio::time::timeout(poll_interval, stream.read_buf(buf)).await {
            Ok(Ok(0)) => {
                // Connection closed
                tracing::debug!("Replication client disconnected (session {})", session_id);
                // Deactivate slot
                let mut cdc = storage.cdc_manager.write();
                if let Some(slot) = cdc.get_slot_by_name(slot_name) {
                    let sid = slot.id;
                    cdc.deactivate_slot(sid);
                }
                return Ok(());
            }
            Ok(Ok(_)) => {
                // Process client messages
                while let Some(msg) = codec::decode_message(buf)? {
                    match msg {
                        FrontendMessage::CopyDone => {
                            tracing::info!(
                                "Replication streaming ended by client (session {})",
                                session_id
                            );
                            let mut cdc = storage.cdc_manager.write();
                            if let Some(slot) = cdc.get_slot_by_name(slot_name) {
                                let sid = slot.id;
                                cdc.deactivate_slot(sid);
                            }
                            return Ok(());
                        }
                        FrontendMessage::CopyData(data) => {
                            // StandbyStatusUpdate from client
                            let mut needs_reply = false;
                            if let Some(status) =
                                logical_replication::StandbyStatusUpdate::parse(&data)
                            {
                                let mut cdc = storage.cdc_manager.write();
                                if let Some(slot) = cdc.get_slot_by_name(slot_name) {
                                    let sid = slot.id;
                                    let _ = cdc.advance_slot(
                                        sid,
                                        falcon_storage::cdc::CdcLsn(status.flush_lsn),
                                    );
                                }
                                needs_reply = status.reply_requested;
                            }
                            // Send reply outside the lock
                            if needs_reply {
                                let keepalive = logical_replication::build_keepalive_message(
                                    current_lsn,
                                    false,
                                );
                                send_message(stream, &BackendMessage::CopyData(keepalive)).await?;
                            }
                        }
                        FrontendMessage::Terminate => {
                            let mut cdc = storage.cdc_manager.write();
                            if let Some(slot) = cdc.get_slot_by_name(slot_name) {
                                let sid = slot.id;
                                cdc.deactivate_slot(sid);
                            }
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "Replication stream read error (session {}): {}",
                    session_id,
                    e
                );
                let mut cdc = storage.cdc_manager.write();
                if let Some(slot) = cdc.get_slot_by_name(slot_name) {
                    let sid = slot.id;
                    cdc.deactivate_slot(sid);
                }
                return Err(e.into());
            }
            Err(_) => {
                // Timeout — no client data, continue polling CDC
            }
        }
    }
}

async fn send_message<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    msg: &BackendMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let buf = codec::encode_message(msg);
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}

/// Substitute `$1`, `$2`, ... placeholders in a SQL string with bound parameter values.
/// Text parameters are single-quoted with embedded quotes escaped.
/// NULL parameters are substituted as the literal `NULL`.
fn bind_params(sql: &str, param_values: &[Option<Vec<u8>>]) -> String {
    if param_values.is_empty() {
        return sql.to_owned();
    }
    let mut result = String::with_capacity(sql.len() + param_values.len() * 8);
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            // Parse the parameter index ($1, $2, ...)
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start {
                if let Ok(idx) = sql[start..end].parse::<usize>() {
                    if idx >= 1 && idx <= param_values.len() {
                        match &param_values[idx - 1] {
                            Some(val) => {
                                // Convert bytes to UTF-8 string, quote it
                                let s = String::from_utf8_lossy(val);
                                result.push('\'');
                                for ch in s.chars() {
                                    if ch == '\'' {
                                        result.push('\'');
                                    }
                                    result.push(ch);
                                }
                                result.push('\'');
                            }
                            None => {
                                result.push_str("NULL");
                            }
                        }
                        i = end;
                        continue;
                    }
                }
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

/// Decode a single parameter value from the PG wire format into a `Datum`.
/// `raw` is `None` for SQL NULL, `Some(bytes)` for a text-format value.
/// `type_hint` is the inferred DataType from the binder (if available).
fn decode_param_value(
    raw: &Option<Vec<u8>>,
    type_hint: Option<&falcon_common::types::DataType>,
) -> falcon_common::datum::Datum {
    use falcon_common::datum::Datum;
    use falcon_common::types::DataType;

    let bytes = match raw {
        Some(b) => b,
        None => return Datum::Null,
    };

    let text = String::from_utf8_lossy(bytes);
    let s = text.as_ref();

    match type_hint {
        Some(DataType::Int16) => s
            .parse::<i16>().map_or_else(|_| Datum::Text(s.to_owned()), |v| Datum::Int32(v as i32)),
        Some(DataType::Int32) => s
            .parse::<i32>().map_or_else(|_| Datum::Text(s.to_owned()), Datum::Int32),
        Some(DataType::Int64) => s
            .parse::<i64>().map_or_else(|_| Datum::Text(s.to_owned()), Datum::Int64),
        Some(DataType::Float32) => s
            .parse::<f32>().map_or_else(|_| Datum::Text(s.to_owned()), |v| Datum::Float64(v as f64)),
        Some(DataType::Float64) => s
            .parse::<f64>().map_or_else(|_| Datum::Text(s.to_owned()), Datum::Float64),
        Some(DataType::Boolean) => match s.to_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => Datum::Boolean(true),
            "f" | "false" | "0" | "no" | "off" => Datum::Boolean(false),
            _ => Datum::Text(s.to_owned()),
        },
        Some(DataType::Uuid) => {
            let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
            if hex.len() == 32 {
                u128::from_str_radix(&hex, 16)
                    .map_or_else(|_| Datum::Text(s.to_owned()), Datum::Uuid)
            } else {
                Datum::Text(s.to_owned())
            }
        }
        Some(DataType::Time) => {
            // Parse HH:MM:SS or HH:MM:SS.ffffff
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() >= 2 {
                let h: i64 = parts[0].parse().unwrap_or(0);
                let m: i64 = parts[1].parse().unwrap_or(0);
                let (sec, frac) = if parts.len() >= 3 {
                    let sp: Vec<&str> = parts[2].split('.').collect();
                    let sv: i64 = sp[0].parse().unwrap_or(0);
                    let fv: i64 = if sp.len() > 1 {
                        format!("{:0<6}", &sp[1][..sp[1].len().min(6)]).parse().unwrap_or(0)
                    } else { 0 };
                    (sv, fv)
                } else { (0, 0) };
                Datum::Time(h * 3_600_000_000 + m * 60_000_000 + sec * 1_000_000 + frac)
            } else {
                Datum::Text(s.to_owned())
            }
        }
        Some(DataType::Interval) => {
            // Simple interval text: "HH:MM:SS" or pass as text
            Datum::Text(s.to_owned())
        }
        Some(DataType::Text)
        | Some(DataType::Timestamp)
        | Some(DataType::Date)
        | Some(DataType::Array(_))
        | Some(DataType::Jsonb) => Datum::Text(s.to_owned()),
        Some(DataType::Decimal(_, _)) => {
            Datum::parse_decimal(s).unwrap_or_else(|| Datum::Text(s.to_owned()))
        }
        Some(DataType::Bytea) => {
            // Accept PG hex format: \x<hex> or raw bytes as text
            let hex_str = s.strip_prefix("\\x").unwrap_or(s);
            let bytes = (0..hex_str.len())
                .step_by(2)
                .filter_map(|i| {
                    hex_str
                        .get(i..i + 2)
                        .and_then(|h| u8::from_str_radix(h, 16).ok())
                })
                .collect();
            Datum::Bytea(bytes)
        }
        None => {
            // No type hint: try integer, then float, then text
            if let Ok(i) = s.parse::<i64>() {
                Datum::Int64(i)
            } else if let Ok(f) = s.parse::<f64>() {
                Datum::Float64(f)
            } else {
                Datum::Text(s.to_owned())
            }
        }
    }
}

/// Resolve the format code for parameter at index `i`.
///
/// PostgreSQL Bind message format semantics:
/// - 0 format codes → all parameters are text (format 0)
/// - 1 format code  → that code applies to ALL parameters
/// - N format codes → per-parameter format
fn resolve_param_format(formats: &[i16], i: usize) -> i16 {
    match formats.len() {
        0 => 0, // all text
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(0),
    }
}

/// Decode a single parameter value from PG **binary** wire format into a `Datum`.
///
/// Binary format (format code 1) is used by JDBC and other drivers.
/// Each type has a well-defined binary encoding:
/// - Int32 (OID 23):  4 bytes big-endian
/// - Int64 (OID 20):  8 bytes big-endian
/// - Float64 (OID 701): 8 bytes IEEE 754 big-endian
/// - Boolean (OID 16): 1 byte (0 = false, 1 = true)
/// - Text (OID 25):   raw UTF-8 bytes
/// - Int16 (OID 21):  2 bytes big-endian
/// - Float32 (OID 700): 4 bytes IEEE 754 big-endian
fn decode_param_value_binary(
    raw: &Option<Vec<u8>>,
    type_hint: Option<&falcon_common::types::DataType>,
) -> falcon_common::datum::Datum {
    use falcon_common::datum::Datum;
    use falcon_common::types::DataType;

    let bytes = match raw {
        Some(b) => b,
        None => return Datum::Null,
    };

    match type_hint {
        Some(DataType::Int16) => {
            if bytes.len() == 2 {
                let v = i16::from_be_bytes([bytes[0], bytes[1]]);
                Datum::Int32(v as i32)
            } else {
                let s = String::from_utf8_lossy(bytes);
                s.parse::<i16>().map_or_else(|_| Datum::Text(s.into_owned()), |v| Datum::Int32(v as i32))
            }
        }
        Some(DataType::Int32) => {
            if bytes.len() == 4 {
                let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Datum::Int32(v)
            } else {
                // Fallback: try text parse
                let s = String::from_utf8_lossy(bytes);
                s.parse::<i32>().map_or_else(|_| Datum::Text(s.into_owned()), Datum::Int32)
            }
        }
        Some(DataType::Int64) => {
            if bytes.len() == 8 {
                let v = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Datum::Int64(v)
            } else if bytes.len() == 4 {
                // Some drivers send int4 binary for int8 columns
                let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Datum::Int64(i64::from(v))
            } else {
                let s = String::from_utf8_lossy(bytes);
                s.parse::<i64>().map_or_else(|_| Datum::Text(s.into_owned()), Datum::Int64)
            }
        }
        Some(DataType::Float32) => {
            if bytes.len() == 4 {
                let v = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Datum::Float64(f64::from(v))
            } else {
                let s = String::from_utf8_lossy(bytes);
                s.parse::<f32>().map_or_else(|_| Datum::Text(s.into_owned()), |v| Datum::Float64(v as f64))
            }
        }
        Some(DataType::Float64) => {
            if bytes.len() == 8 {
                let v = f64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Datum::Float64(v)
            } else if bytes.len() == 4 {
                // float4 binary
                let v = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Datum::Float64(f64::from(v))
            } else {
                let s = String::from_utf8_lossy(bytes);
                s.parse::<f64>().map_or_else(|_| Datum::Text(s.into_owned()), Datum::Float64)
            }
        }
        Some(DataType::Boolean) => {
            if bytes.len() == 1 {
                Datum::Boolean(bytes[0] != 0)
            } else {
                let s = String::from_utf8_lossy(bytes);
                match s.as_ref() {
                    "t" | "true" | "1" => Datum::Boolean(true),
                    _ => Datum::Boolean(false),
                }
            }
        }
        Some(DataType::Bytea) => {
            // Binary format: raw bytes, no encoding needed
            Datum::Bytea(bytes.clone())
        }
        Some(DataType::Uuid) => {
            // PG binary UUID format: 16 bytes raw
            if bytes.len() == 16 {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                Datum::Uuid(u128::from_be_bytes(arr))
            } else {
                // Fallback: try text parse
                let s = String::from_utf8_lossy(bytes);
                let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
                if hex.len() == 32 {
                    u128::from_str_radix(&hex, 16)
                        .map_or_else(|_| Datum::Text(s.into_owned()), Datum::Uuid)
                } else {
                    Datum::Text(s.into_owned())
                }
            }
        }
        Some(DataType::Text)
        | Some(DataType::Timestamp)
        | Some(DataType::Date)
        | Some(DataType::Jsonb)
        | Some(DataType::Array(_))
        | Some(DataType::Time)
        | Some(DataType::Interval) => Datum::Text(String::from_utf8_lossy(bytes).into_owned()),
        Some(DataType::Decimal(_, _)) => {
            let s = String::from_utf8_lossy(bytes);
            Datum::parse_decimal(&s).unwrap_or_else(|| Datum::Text(s.into_owned()))
        }
        None => {
            // No type hint: try to infer from byte length
            match bytes.len() {
                4 => {
                    let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    Datum::Int32(v)
                }
                8 => {
                    let v = i64::from_be_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    Datum::Int64(v)
                }
                1 => Datum::Boolean(bytes[0] != 0),
                _ => Datum::Text(String::from_utf8_lossy(bytes).into_owned()),
            }
        }
    }
}

/// Parse `SET statement_timeout = <ms>` or `SET statement_timeout TO <ms>`.
/// Returns Some(ms) if matched, None otherwise.
fn parse_set_statement_timeout(sql: &str) -> Option<u64> {
    let sql = sql.trim().to_lowercase();
    let rest = sql.strip_prefix("set")?;
    let rest = rest.trim();
    let rest = rest.strip_prefix("statement_timeout")?;
    let rest = rest.trim();
    // Accept "= <value>" or "to <value>"
    let rest = if let Some(r) = rest.strip_prefix('=') {
        r.trim()
    } else if let Some(r) = rest.strip_prefix("to") {
        r.trim()
    } else {
        return None;
    };
    // Strip optional quotes and trailing semicolons
    let value = rest
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"');
    // "0" or "default" means no timeout
    if value == "default" || value == "0" {
        return Some(0);
    }
    value.parse::<u64>().ok()
}

// ---------------------------------------------------------------------------
// SCRAM-SHA-256 crypto helpers
// ---------------------------------------------------------------------------

/// PBKDF2-HMAC-SHA256 key derivation (RFC 7677).
#[allow(dead_code)]
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    // PBKDF2 with HMAC-SHA256: U1 = HMAC(password, salt || INT(1))
    // Ui = HMAC(password, U_{i-1}), result = U1 XOR U2 XOR ... XOR Ui
    let mut u_prev = {
        let mut input = Vec::with_capacity(salt.len() + 4);
        input.extend_from_slice(salt);
        input.extend_from_slice(&1u32.to_be_bytes());
        hmac_sha256(password, &input)
    };
    let mut result = u_prev;

    for _ in 1..iterations {
        let u_next = hmac_sha256(password, &u_prev);
        for (r, u) in result.iter_mut().zip(u_next.iter()) {
            *r ^= u;
        }
        u_prev = u_next;
    }
    result
}

/// HMAC-SHA-256 (RFC 2104).
#[allow(dead_code)]
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    const BLOCK_SIZE: usize = 64;

    let key_prime = if key.len() > BLOCK_SIZE {
        let mut h = sha2::Sha256::new();
        h.update(key);
        let hash: [u8; 32] = h.finalize().into();
        let mut padded = [0u8; BLOCK_SIZE];
        padded[..32].copy_from_slice(&hash);
        padded
    } else {
        let mut padded = [0u8; BLOCK_SIZE];
        padded[..key.len()].copy_from_slice(key);
        padded
    };

    let mut ipad = [0x36u8; BLOCK_SIZE];
    let mut opad = [0x5cu8; BLOCK_SIZE];
    for i in 0..BLOCK_SIZE {
        ipad[i] ^= key_prime[i];
        opad[i] ^= key_prime[i];
    }

    let inner_hash = {
        let mut h = sha2::Sha256::new();
        h.update(ipad);
        h.update(message);
        let result: [u8; 32] = h.finalize().into();
        result
    };

    let mut h = sha2::Sha256::new();
    h.update(opad);
    h.update(inner_hash);
    h.finalize().into()
}

/// Simple base64 encoder (standard alphabet, with padding).
#[allow(dead_code)]
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = if chunk.len() > 1 { u32::from(chunk[1]) } else { 0 };
        let b2 = if chunk.len() > 2 { u32::from(chunk[2]) } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(ALPHABET[((triple >> 18) & 0x3F) as usize] as char);
        result.push(ALPHABET[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(ALPHABET[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(ALPHABET[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_set_statement_timeout_equals() {
        assert_eq!(
            parse_set_statement_timeout("SET statement_timeout = 5000"),
            Some(5000)
        );
        assert_eq!(
            parse_set_statement_timeout("SET statement_timeout = 0"),
            Some(0)
        );
        assert_eq!(
            parse_set_statement_timeout("set statement_timeout = 100;"),
            Some(100)
        );
    }

    #[test]
    fn test_parse_set_statement_timeout_to() {
        assert_eq!(
            parse_set_statement_timeout("SET statement_timeout TO 3000"),
            Some(3000)
        );
        assert_eq!(
            parse_set_statement_timeout("SET statement_timeout TO '5000'"),
            Some(5000)
        );
    }

    #[test]
    fn test_parse_set_statement_timeout_default() {
        assert_eq!(
            parse_set_statement_timeout("SET statement_timeout = default"),
            Some(0)
        );
        assert_eq!(
            parse_set_statement_timeout("SET statement_timeout TO default"),
            Some(0)
        );
    }

    #[test]
    fn test_parse_set_statement_timeout_not_matching() {
        assert_eq!(parse_set_statement_timeout("SELECT 1"), None);
        assert_eq!(
            parse_set_statement_timeout("SET search_path = public"),
            None
        );
        assert_eq!(parse_set_statement_timeout("SET statement_timeout"), None);
    }

    #[test]
    fn test_pg_server_max_connections_setter() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let mut server = PgServer::new("127.0.0.1:0".into(), storage, txn_mgr, executor);
        assert_eq!(server.max_connections, 0);
        server.set_max_connections(50);
        assert_eq!(server.max_connections, 50);
    }

    #[test]
    fn test_pg_server_statement_timeout_setter() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let mut server = PgServer::new("127.0.0.1:0".into(), storage, txn_mgr, executor);
        assert_eq!(server.default_statement_timeout_ms, 0);
        server.set_default_statement_timeout_ms(5000);
        assert_eq!(server.default_statement_timeout_ms, 5000);
    }

    #[test]
    fn test_pg_server_shared_active_connections() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let mut server = PgServer::new("127.0.0.1:0".into(), storage, txn_mgr, executor);
        assert_eq!(server.active_connection_count(), 0);

        let shared = Arc::new(AtomicUsize::new(7));
        server.set_active_connections(shared.clone());
        assert_eq!(server.active_connection_count(), 7);

        // Mutating the shared counter is reflected
        shared.store(3, Ordering::Relaxed);
        assert_eq!(server.active_connection_count(), 3);
    }

    #[test]
    fn test_pg_server_idle_timeout_setter() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let mut server = PgServer::new("127.0.0.1:0".into(), storage, txn_mgr, executor);
        assert_eq!(server.idle_timeout_ms, 0);
        server.set_idle_timeout_ms(30000);
        assert_eq!(server.idle_timeout_ms, 30000);
    }

    // ── decode_param_value tests (Phase 2) ──

    #[test]
    fn test_decode_param_value_null() {
        use falcon_common::datum::Datum;
        let result = decode_param_value(&None, None);
        assert!(matches!(result, Datum::Null));
    }

    #[test]
    fn test_decode_param_value_int32_hint() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(b"42".to_vec());
        let result = decode_param_value(&raw, Some(&DataType::Int32));
        assert!(matches!(result, Datum::Int32(42)));
    }

    #[test]
    fn test_decode_param_value_int64_hint() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(b"9999999999".to_vec());
        let result = decode_param_value(&raw, Some(&DataType::Int64));
        assert!(matches!(result, Datum::Int64(9999999999)));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_decode_param_value_float64_hint() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(b"3.14".to_vec());
        match decode_param_value(&raw, Some(&DataType::Float64)) {
            Datum::Float64(v) => assert!((v - 3.14_f64).abs() < 1e-10),
            other => panic!("expected Float64, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_param_value_boolean_hint() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        assert!(matches!(
            decode_param_value(&Some(b"true".to_vec()), Some(&DataType::Boolean)),
            Datum::Boolean(true)
        ));
        assert!(matches!(
            decode_param_value(&Some(b"t".to_vec()), Some(&DataType::Boolean)),
            Datum::Boolean(true)
        ));
        assert!(matches!(
            decode_param_value(&Some(b"false".to_vec()), Some(&DataType::Boolean)),
            Datum::Boolean(false)
        ));
        assert!(matches!(
            decode_param_value(&Some(b"f".to_vec()), Some(&DataType::Boolean)),
            Datum::Boolean(false)
        ));
        assert!(matches!(
            decode_param_value(&Some(b"0".to_vec()), Some(&DataType::Boolean)),
            Datum::Boolean(false)
        ));
        assert!(matches!(
            decode_param_value(&Some(b"1".to_vec()), Some(&DataType::Boolean)),
            Datum::Boolean(true)
        ));
    }

    #[test]
    fn test_decode_param_value_text_hint() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(b"hello world".to_vec());
        match decode_param_value(&raw, Some(&DataType::Text)) {
            Datum::Text(s) => assert_eq!(s, "hello world"),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_param_value_no_hint_integer() {
        use falcon_common::datum::Datum;
        let raw = Some(b"123".to_vec());
        match decode_param_value(&raw, None) {
            Datum::Int64(v) => assert_eq!(v, 123),
            other => panic!("expected Int64, got {:?}", other),
        }
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_decode_param_value_no_hint_float() {
        use falcon_common::datum::Datum;
        let raw = Some(b"3.14".to_vec());
        match decode_param_value(&raw, None) {
            Datum::Float64(v) => assert!((v - 3.14_f64).abs() < 1e-10),
            other => panic!("expected Float64, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_param_value_no_hint_text() {
        use falcon_common::datum::Datum;
        let raw = Some(b"hello".to_vec());
        match decode_param_value(&raw, None) {
            Datum::Text(s) => assert_eq!(s, "hello"),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_param_value_invalid_int_falls_back_to_text() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(b"not_a_number".to_vec());
        match decode_param_value(&raw, Some(&DataType::Int32)) {
            Datum::Text(s) => assert_eq!(s, "not_a_number"),
            other => panic!("expected Text fallback, got {:?}", other),
        }
    }

    // ── resolve_param_format tests ──

    #[test]
    fn test_resolve_param_format_empty_means_text() {
        assert_eq!(resolve_param_format(&[], 0), 0);
        assert_eq!(resolve_param_format(&[], 5), 0);
    }

    #[test]
    fn test_resolve_param_format_single_applies_to_all() {
        assert_eq!(resolve_param_format(&[1], 0), 1);
        assert_eq!(resolve_param_format(&[1], 3), 1);
        assert_eq!(resolve_param_format(&[0], 0), 0);
    }

    #[test]
    fn test_resolve_param_format_per_param() {
        let formats = vec![0i16, 1, 0, 1];
        assert_eq!(resolve_param_format(&formats, 0), 0);
        assert_eq!(resolve_param_format(&formats, 1), 1);
        assert_eq!(resolve_param_format(&formats, 2), 0);
        assert_eq!(resolve_param_format(&formats, 3), 1);
        // Out of bounds defaults to text
        assert_eq!(resolve_param_format(&formats, 10), 0);
    }

    // ── decode_param_value_binary tests ──

    #[test]
    fn test_decode_binary_null() {
        use falcon_common::datum::Datum;
        let result = decode_param_value_binary(&None, None);
        assert!(matches!(result, Datum::Null));
    }

    #[test]
    fn test_decode_binary_int32() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(42i32.to_be_bytes().to_vec());
        match decode_param_value_binary(&raw, Some(&DataType::Int32)) {
            Datum::Int32(v) => assert_eq!(v, 42),
            other => panic!("expected Int32(42), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_binary_int32_negative() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some((-1i32).to_be_bytes().to_vec());
        match decode_param_value_binary(&raw, Some(&DataType::Int32)) {
            Datum::Int32(v) => assert_eq!(v, -1),
            other => panic!("expected Int32(-1), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_binary_int64() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(9999999999i64.to_be_bytes().to_vec());
        match decode_param_value_binary(&raw, Some(&DataType::Int64)) {
            Datum::Int64(v) => assert_eq!(v, 9999999999),
            other => panic!("expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_binary_int64_from_int4() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        // Some drivers send 4-byte binary for int8 columns
        let raw = Some(42i32.to_be_bytes().to_vec());
        match decode_param_value_binary(&raw, Some(&DataType::Int64)) {
            Datum::Int64(v) => assert_eq!(v, 42),
            other => panic!("expected Int64(42), got {:?}", other),
        }
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_decode_binary_float64() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(3.14_f64.to_be_bytes().to_vec());
        match decode_param_value_binary(&raw, Some(&DataType::Float64)) {
            Datum::Float64(v) => assert!((v - 3.14_f64).abs() < 1e-10),
            other => panic!("expected Float64, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_binary_float64_from_float4() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(1.5f32.to_be_bytes().to_vec());
        match decode_param_value_binary(&raw, Some(&DataType::Float64)) {
            Datum::Float64(v) => assert!((v - 1.5).abs() < 1e-6),
            other => panic!("expected Float64, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_binary_boolean() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        assert!(matches!(
            decode_param_value_binary(&Some(vec![1u8]), Some(&DataType::Boolean)),
            Datum::Boolean(true)
        ));
        assert!(matches!(
            decode_param_value_binary(&Some(vec![0u8]), Some(&DataType::Boolean)),
            Datum::Boolean(false)
        ));
    }

    #[test]
    fn test_decode_binary_text() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(b"hello binary".to_vec());
        match decode_param_value_binary(&raw, Some(&DataType::Text)) {
            Datum::Text(s) => assert_eq!(s, "hello binary"),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_binary_no_hint_4bytes() {
        use falcon_common::datum::Datum;
        let raw = Some(100i32.to_be_bytes().to_vec());
        match decode_param_value_binary(&raw, None) {
            Datum::Int32(v) => assert_eq!(v, 100),
            other => panic!("expected Int32(100), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_binary_no_hint_8bytes() {
        use falcon_common::datum::Datum;
        let raw = Some(123456789i64.to_be_bytes().to_vec());
        match decode_param_value_binary(&raw, None) {
            Datum::Int64(v) => assert_eq!(v, 123456789),
            other => panic!("expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_binary_no_hint_1byte() {
        use falcon_common::datum::Datum;
        assert!(matches!(
            decode_param_value_binary(&Some(vec![1u8]), None),
            Datum::Boolean(true)
        ));
    }

    #[test]
    fn test_decode_binary_uuid_16bytes() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        // UUID: 550e8400-e29b-41d4-a716-446655440000
        let raw = Some(vec![
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ]);
        match decode_param_value_binary(&raw, Some(&DataType::Uuid)) {
            Datum::Uuid(v) => {
                // Verify it formats back to the expected UUID string
                assert_eq!(format!("{}", Datum::Uuid(v)), "550e8400-e29b-41d4-a716-446655440000");
            }
            other => panic!("expected Datum::Uuid, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_binary_uuid_text_fallback() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(b"550e8400-e29b-41d4-a716-446655440000".to_vec());
        match decode_param_value_binary(&raw, Some(&DataType::Uuid)) {
            Datum::Uuid(v) => {
                assert_eq!(format!("{}", Datum::Uuid(v)), "550e8400-e29b-41d4-a716-446655440000");
            }
            other => panic!("expected Datum::Uuid, got {:?}", other),
        }
    }

    // ── SCRAM-SHA-256 crypto helper tests ──

    #[test]
    fn test_hmac_sha256_basic() {
        // RFC 4231 Test Case 2: "what do ya want for nothing?" with key "Jefe"
        let result = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex = result
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn test_hmac_sha256_empty_message() {
        let result = hmac_sha256(b"key", b"");
        assert_eq!(result.len(), 32);
        // Just verify it doesn't panic and returns 32 bytes
    }

    #[test]
    fn test_pbkdf2_sha256_deterministic() {
        let salt = b"salt1234salt1234";
        let r1 = pbkdf2_sha256(b"password", salt, 4096);
        let r2 = pbkdf2_sha256(b"password", salt, 4096);
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_pbkdf2_sha256_different_passwords() {
        let salt = b"salt1234salt1234";
        let r1 = pbkdf2_sha256(b"password1", salt, 100);
        let r2 = pbkdf2_sha256(b"password2", salt, 100);
        assert_ne!(r1, r2);
    }

    #[test]
    fn test_pbkdf2_sha256_different_salts() {
        let r1 = pbkdf2_sha256(b"password", b"salt1111salt1111", 100);
        let r2 = pbkdf2_sha256(b"password", b"salt2222salt2222", 100);
        assert_ne!(r1, r2);
    }

    #[test]
    fn test_base64_encode_empty() {
        assert_eq!(base64_encode(b""), "");
    }

    #[test]
    fn test_base64_encode_simple() {
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn test_base64_encode_binary() {
        assert_eq!(base64_encode(&[0, 1, 2, 3]), "AAECAw==");
    }

    // ── TLS config tests ──

    #[test]
    fn test_tls_config_disabled_by_default() {
        use falcon_common::config::TlsConfig;
        let tls = TlsConfig::default();
        assert!(!tls.is_enabled());
    }

    #[test]
    fn test_tls_config_enabled_with_paths() {
        use falcon_common::config::TlsConfig;
        let tls = TlsConfig {
            cert_path: "/etc/ssl/cert.pem".into(),
            key_path: "/etc/ssl/key.pem".into(),
        };
        assert!(tls.is_enabled());
    }

    #[test]
    fn test_tls_config_partial_not_enabled() {
        use falcon_common::config::TlsConfig;
        let tls = TlsConfig {
            cert_path: "/etc/ssl/cert.pem".into(),
            key_path: String::new(),
        };
        assert!(!tls.is_enabled());
    }

    // ── Production hardening tests ──

    /// Read-only executor rejects DML (INSERT/UPDATE/DELETE/CREATE TABLE).
    #[test]
    fn test_read_only_executor_rejects_dml() {
        use falcon_executor::Executor;
        use falcon_storage::engine::StorageEngine;
        use falcon_txn::TxnManager;

        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Executor::new_read_only(storage.clone(), txn_mgr.clone());

        assert!(
            executor.is_read_only(),
            "Executor should be in read-only mode"
        );

        // Writable executor should not be read-only
        let rw_executor = Executor::new(storage.clone(), txn_mgr.clone());
        assert!(!rw_executor.is_read_only());
    }

    /// NodeRole::is_writable() correctly classifies roles.
    #[test]
    fn test_node_role_writable_classification() {
        use falcon_common::config::NodeRole;
        assert!(NodeRole::Primary.is_writable());
        assert!(NodeRole::Standalone.is_writable());
        assert!(!NodeRole::Replica.is_writable());
        assert!(!NodeRole::Analytics.is_writable());
    }

    /// NodeRole::allows_columnstore() correctly classifies roles.
    #[test]
    fn test_node_role_columnstore_classification() {
        use falcon_common::config::NodeRole;
        assert!(!NodeRole::Primary.allows_columnstore());
        assert!(!NodeRole::Replica.allows_columnstore());
        assert!(NodeRole::Analytics.allows_columnstore());
        assert!(NodeRole::Standalone.allows_columnstore());
    }

    /// max_connections=0 means unlimited (no rejection).
    #[test]
    fn test_max_connections_zero_means_unlimited() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let server = PgServer::new("127.0.0.1:0".into(), storage, txn_mgr, executor);
        // max_connections=0 by default → unlimited
        assert_eq!(server.max_connections, 0);
    }

    /// Active connections counter is shared between server and executor.
    #[test]
    fn test_active_connections_shared_counter() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let mut server = PgServer::new("127.0.0.1:0".into(), storage, txn_mgr, executor);

        let shared = Arc::new(AtomicUsize::new(0));
        server.set_active_connections(shared.clone());

        // Simulate connections opening and closing
        shared.fetch_add(1, Ordering::Relaxed);
        assert_eq!(server.active_connection_count(), 1);
        shared.fetch_add(1, Ordering::Relaxed);
        assert_eq!(server.active_connection_count(), 2);
        shared.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(server.active_connection_count(), 1);
    }

    /// statement_timeout_ms defaults to 0 (no timeout) and can be set.
    #[test]
    fn test_statement_timeout_default_and_set() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let mut server = PgServer::new("127.0.0.1:0".into(), storage, txn_mgr, executor);

        assert_eq!(
            server.default_statement_timeout_ms, 0,
            "Default should be no timeout"
        );
        server.set_default_statement_timeout_ms(30_000);
        assert_eq!(server.default_statement_timeout_ms, 30_000);
        // Reset to 0 disables timeout
        server.set_default_statement_timeout_ms(0);
        assert_eq!(server.default_statement_timeout_ms, 0);
    }

    /// idle_timeout_ms defaults to 0 (no timeout) and can be set.
    #[test]
    fn test_idle_timeout_default_and_set() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let mut server = PgServer::new("127.0.0.1:0".into(), storage, txn_mgr, executor);

        assert_eq!(
            server.idle_timeout_ms, 0,
            "Default should be no idle timeout"
        );
        server.set_idle_timeout_ms(60_000);
        assert_eq!(server.idle_timeout_ms, 60_000);
    }

    /// parse_set_statement_timeout handles quoted values and semicolons.
    #[test]
    fn test_parse_set_statement_timeout_edge_cases() {
        assert_eq!(
            parse_set_statement_timeout("SET statement_timeout = '5000';"),
            Some(5000)
        );
        assert_eq!(
            parse_set_statement_timeout("SET statement_timeout = \"5000\""),
            Some(5000)
        );
        assert_eq!(
            parse_set_statement_timeout("  SET  statement_timeout  =  1000  "),
            Some(1000)
        );
        // Non-numeric value that isn't "default" → None
        assert_eq!(
            parse_set_statement_timeout("SET statement_timeout = 'abc'"),
            None
        );
    }

    /// shutdown_drain_timeout_secs config field has correct default.
    #[test]
    fn test_shutdown_drain_timeout_default() {
        use falcon_common::config::FalconConfig;
        let config = FalconConfig::default();
        assert_eq!(
            config.server.shutdown_drain_timeout_secs, 30,
            "Default drain timeout should be 30 seconds"
        );
    }

    /// shutdown_drain_timeout_secs can be set to 0 (min 1 enforced in main.rs).
    #[test]
    fn test_shutdown_drain_timeout_configurable() {
        use falcon_common::config::ServerConfig;
        use falcon_common::config::{AuthConfig, TlsConfig};
        let config = ServerConfig {
            pg_listen_addr: "0.0.0.0:5433".into(),
            admin_listen_addr: "0.0.0.0:8080".into(),
            node_id: 1,
            max_connections: 100,
            statement_timeout_ms: 5000,
            idle_timeout_ms: 60000,
            shutdown_drain_timeout_secs: 60,
            slow_txn_threshold_us: 100_000,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        };
        assert_eq!(config.shutdown_drain_timeout_secs, 60);
        assert_eq!(config.statement_timeout_ms, 5000);
        assert_eq!(config.idle_timeout_ms, 60000);
    }

    // ── Cancel request registry tests ──

    #[test]
    fn test_cancel_registry_insert_and_lookup() {
        let registry: CancellationRegistry = Arc::new(DashMap::new());
        let cancelled = Arc::new(AtomicBool::new(false));
        registry.insert(
            42,
            CancelEntry {
                secret_key: 12345,
                cancelled: cancelled.clone(),
            },
        );

        // Correct lookup
        let entry = registry.get(&42).unwrap();
        assert_eq!(entry.secret_key, 12345);
        assert!(!entry.cancelled.load(Ordering::SeqCst));

        // Signal cancellation
        entry.cancelled.store(true, Ordering::SeqCst);
        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[test]
    fn test_cancel_registry_wrong_secret_key() {
        let registry: CancellationRegistry = Arc::new(DashMap::new());
        let cancelled = Arc::new(AtomicBool::new(false));
        registry.insert(
            1,
            CancelEntry {
                secret_key: 99999,
                cancelled: cancelled.clone(),
            },
        );

        // Wrong secret key should not trigger cancellation
        if let Some(entry) = registry.get(&1) {
            if entry.secret_key == 11111 {
                entry.cancelled.store(true, Ordering::SeqCst);
            }
        }
        assert!(!cancelled.load(Ordering::SeqCst));
    }

    #[test]
    fn test_cancel_registry_missing_session() {
        let registry: CancellationRegistry = Arc::new(DashMap::new());
        // Looking up a non-existent session should return None
        assert!(registry.get(&999).is_none());
    }

    #[test]
    fn test_cancel_flag_swap_semantics() {
        let cancelled = Arc::new(AtomicBool::new(false));

        // First swap returns false (was not cancelled)
        assert!(!cancelled.swap(false, Ordering::SeqCst));

        // Set to true (simulating cancel request arrival)
        cancelled.store(true, Ordering::SeqCst);

        // Swap should return true and reset to false
        assert!(cancelled.swap(false, Ordering::SeqCst));

        // After swap, flag is false again
        assert!(!cancelled.load(Ordering::SeqCst));
    }

    #[test]
    fn test_cancel_registry_cleanup_on_disconnect() {
        let registry: CancellationRegistry = Arc::new(DashMap::new());
        let cancelled = Arc::new(AtomicBool::new(false));
        registry.insert(
            10,
            CancelEntry {
                secret_key: 555,
                cancelled: cancelled.clone(),
            },
        );
        assert!(registry.get(&10).is_some());

        // Simulate connection cleanup
        registry.remove(&10);
        assert!(registry.get(&10).is_none());
    }

    // ── SCRAM-SHA-256 SASL codec tests ──

    #[test]
    fn test_decode_sasl_initial_response() {
        use bytes::BufMut;
        // Build a SASLInitialResponse wire message:
        // 'p' | int32 len | C-string mechanism | int32 data_len | data bytes
        let mechanism = b"SCRAM-SHA-256\0";
        let sasl_data = b"n,,n=testuser,r=clientnonce123";
        let payload_len = mechanism.len() + 4 + sasl_data.len();
        let total_len = 4 + payload_len; // includes the 4-byte length field itself

        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_i32(total_len as i32);
        buf.put_slice(mechanism);
        buf.put_i32(sasl_data.len() as i32);
        buf.put_slice(sasl_data);

        match codec::decode_sasl_initial_response(&mut buf).unwrap() {
            Some(FrontendMessage::SASLInitialResponse { mechanism, data }) => {
                assert_eq!(mechanism, "SCRAM-SHA-256");
                assert_eq!(data, sasl_data.to_vec());
            }
            other => panic!("expected SASLInitialResponse, got: {other:?}"),
        }
        assert!(buf.is_empty(), "buffer should be fully consumed");
    }

    #[test]
    fn test_decode_sasl_response() {
        use bytes::BufMut;
        // Build a SASLResponse wire message:
        // 'p' | int32 len | data bytes
        let sasl_data = b"c=biws,r=clientnonce123servernonce456,p=dGVzdHByb29m";
        let total_len = 4 + sasl_data.len();

        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_i32(total_len as i32);
        buf.put_slice(sasl_data);

        match codec::decode_sasl_response(&mut buf).unwrap() {
            Some(FrontendMessage::SASLResponse { data }) => {
                assert_eq!(data, sasl_data.to_vec());
            }
            other => panic!("expected SASLResponse, got: {other:?}"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_sasl_initial_response_incomplete() {
        use bytes::BufMut;
        // Not enough bytes — should return None
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_i32(100); // claims 100 bytes but we don't provide them
        assert!(codec::decode_sasl_initial_response(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_decode_sasl_response_wrong_type() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q'); // wrong type
        buf.put_i32(4);
        assert!(codec::decode_sasl_response(&mut buf).is_err());
    }

    #[test]
    fn test_encode_authentication_sasl() {
        let msg = BackendMessage::AuthenticationSASL {
            mechanisms: vec!["SCRAM-SHA-256".into()],
        };
        let encoded = codec::encode_message(&msg);
        // R(1) + len(4) + auth_type=10(4) + "SCRAM-SHA-256\0" + "\0"
        assert_eq!(encoded[0], b'R');
        // Auth type at offset 5..9 should be 10
        let auth_type = i32::from_be_bytes([encoded[5], encoded[6], encoded[7], encoded[8]]);
        assert_eq!(auth_type, 10);
    }

    #[test]
    fn test_encode_authentication_sasl_continue() {
        let data = b"r=clientnonce123servernonce,s=c2FsdA==,i=4096";
        let msg = BackendMessage::AuthenticationSASLContinue {
            data: data.to_vec(),
        };
        let encoded = codec::encode_message(&msg);
        assert_eq!(encoded[0], b'R');
        let auth_type = i32::from_be_bytes([encoded[5], encoded[6], encoded[7], encoded[8]]);
        assert_eq!(auth_type, 11);
        // Data should follow after the auth type
        assert_eq!(&encoded[9..], data);
    }

    #[test]
    fn test_encode_authentication_sasl_final() {
        let data = b"v=dGVzdHNpZw==";
        let msg = BackendMessage::AuthenticationSASLFinal {
            data: data.to_vec(),
        };
        let encoded = codec::encode_message(&msg);
        assert_eq!(encoded[0], b'R');
        let auth_type = i32::from_be_bytes([encoded[5], encoded[6], encoded[7], encoded[8]]);
        assert_eq!(auth_type, 12);
        assert_eq!(&encoded[9..], data);
    }

    // ── SCRAM-SHA-256 full wire-level handshake test ──

    #[test]
    fn test_scram_wire_handshake_with_verifier() {
        use crate::auth::scram::{ScramVerifier, ScramServerSession, SCRAM_SHA_256, b64_encode, b64_decode};
        use sha2::{Sha256, Digest};

        let password = "s3cret";
        let salt = b"test_salt_16byte";
        let iterations = 4096u32;

        // Pre-generate a stored verifier (as would be in config)
        let verifier = ScramVerifier::generate_with_salt(password, iterations, salt);
        let pg_verifier = verifier.to_pg_verifier();

        // Parse it back (simulates loading from config)
        let loaded_verifier = ScramVerifier::parse(&pg_verifier).unwrap();
        assert_eq!(loaded_verifier.iterations, iterations);
        assert_eq!(loaded_verifier.salt, salt.to_vec());
        assert_eq!(loaded_verifier.stored_key, verifier.stored_key);
        assert_eq!(loaded_verifier.server_key, verifier.server_key);

        // Create server session
        let mut server = ScramServerSession::new(loaded_verifier);

        // Client-first-message
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let client_first_bare = format!("n=testuser,r={client_nonce}");
        let client_first_msg = format!("n,,{client_first_bare}");

        // Server processes client-first → server-first
        let server_first_bytes = server
            .handle_client_first(SCRAM_SHA_256, &client_first_msg)
            .unwrap();
        let server_first_msg = String::from_utf8(server_first_bytes).unwrap();

        // Parse server-first-message
        let combined_nonce = server_first_msg
            .split(',').find(|p| p.starts_with("r=")).unwrap()[2..].to_owned();
        let server_salt_b64 = server_first_msg
            .split(',').find(|p| p.starts_with("s=")).unwrap()[2..].to_owned();
        let server_iterations: u32 = server_first_msg
            .split(',').find(|p| p.starts_with("i=")).unwrap()[2..].parse().unwrap();

        assert!(combined_nonce.starts_with(client_nonce));
        assert_eq!(server_iterations, iterations);

        // Client derives keys using the salt and iterations from server
        let server_salt = b64_decode(&server_salt_b64).unwrap();
        let salted_password = {
            // PBKDF2 inline for test
            use hmac::{Hmac, Mac};
            type HmacSha256 = Hmac<Sha256>;
            let mut input = Vec::with_capacity(server_salt.len() + 4);
            input.extend_from_slice(&server_salt);
            input.extend_from_slice(&1u32.to_be_bytes());

            let mut mac = HmacSha256::new_from_slice(password.as_bytes()).unwrap();
            mac.update(&input);
            let mut u_prev: [u8; 32] = mac.finalize().into_bytes().into();
            let mut result = u_prev;

            for _ in 1..server_iterations {
                let mut mac = HmacSha256::new_from_slice(password.as_bytes()).unwrap();
                mac.update(&u_prev);
                let u_next: [u8; 32] = mac.finalize().into_bytes().into();
                for (r, u) in result.iter_mut().zip(u_next.iter()) {
                    *r ^= u;
                }
                u_prev = u_next;
            }
            result
        };

        let hmac_fn = |key: &[u8], msg: &[u8]| -> [u8; 32] {
            use hmac::{Hmac, Mac};
            type HmacSha256 = Hmac<Sha256>;
            let mut mac = HmacSha256::new_from_slice(key).unwrap();
            mac.update(msg);
            mac.finalize().into_bytes().into()
        };

        let client_key = hmac_fn(&salted_password, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();

        // Build auth message
        let client_final_without_proof = format!("c=biws,r={combined_nonce}");
        let auth_message = format!(
            "{client_first_bare},{server_first_msg},{client_final_without_proof}"
        );

        let client_signature = hmac_fn(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        let client_final_msg = format!(
            "{client_final_without_proof},p={}",
            b64_encode(&client_proof)
        );

        // Server verifies client-final → server-final
        let server_final_bytes = server.handle_client_final(&client_final_msg).unwrap();
        let server_final_msg = String::from_utf8(server_final_bytes).unwrap();
        assert!(server_final_msg.starts_with("v="));
        assert!(server.is_complete());

        // Verify server signature
        let server_key = hmac_fn(&salted_password, b"Server Key");
        let expected_server_sig = hmac_fn(&server_key, auth_message.as_bytes());
        let server_sig_b64 = &server_final_msg[2..];
        let actual_server_sig = b64_decode(server_sig_b64).unwrap();
        assert_eq!(actual_server_sig, expected_server_sig.to_vec());
    }

    #[test]
    fn test_auth_config_users_default_empty() {
        let config = AuthConfig::default();
        assert!(config.users.is_empty());
        assert!(config.allow_cidrs.is_empty());
        assert_eq!(config.method, AuthMethod::Trust);
    }

    #[test]
    fn test_auth_config_scram_user_lookup() {
        use crate::auth::scram::ScramVerifier;

        let verifier = ScramVerifier::generate("testpw", 4096);
        let pg_str = verifier.to_pg_verifier();

        let config = AuthConfig {
            method: AuthMethod::ScramSha256,
            password: String::new(),
            username: String::new(),
            users: vec![falcon_common::config::UserCredential {
                username: "alice".into(),
                scram: pg_str.clone(),
                password: String::new(),
            }],
            allow_cidrs: vec!["127.0.0.1/32".into()],
        };

        // Find user
        let found = config.users.iter().find(|u| u.username == "alice");
        assert!(found.is_some());
        let parsed = ScramVerifier::parse(&found.unwrap().scram).unwrap();
        assert_eq!(parsed.iterations, 4096);
        assert_eq!(parsed.stored_key, verifier.stored_key);

        // Unknown user
        let not_found = config.users.iter().find(|u| u.username == "bob");
        assert!(not_found.is_none());
    }
}
