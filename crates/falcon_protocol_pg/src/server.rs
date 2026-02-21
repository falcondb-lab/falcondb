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
use crate::notify::NotificationHub;
use crate::session::PgSession;

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
        }
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
    pub fn set_max_connections(&mut self, max: usize) {
        self.max_connections = max;
    }

    /// Set the default statement timeout for new sessions (0 = no timeout).
    pub fn set_default_statement_timeout_ms(&mut self, ms: u64) {
        self.default_statement_timeout_ms = ms;
    }

    /// Set the connection idle timeout (0 = no timeout).
    pub fn set_idle_timeout_ms(&mut self, ms: u64) {
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
    pub async fn run_with_shutdown(
        &self,
        shutdown: impl std::future::Future<Output = ()>,
        drain_timeout: std::time::Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&self.listen_addr).await?;
        tracing::info!("FalconDB PG server listening on {}", self.listen_addr);

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    tracing::info!("New connection from {}", addr);
                    self.spawn_connection(stream);
                }
                _ = &mut shutdown => {
                    tracing::info!("Shutdown signal received, stopping new connections");
                    break;
                }
            }
        }

        // Drain active connections
        let active = self.active_connections.load(Ordering::Relaxed);
        if active > 0 {
            tracing::info!("Draining {} active connection(s) (timeout: {:?})", active, drain_timeout);
            let deadline = tokio::time::Instant::now() + drain_timeout;
            loop {
                let remaining = self.active_connections.load(Ordering::Relaxed);
                if remaining == 0 {
                    tracing::info!("All connections drained");
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    tracing::warn!("Drain timeout reached with {} connection(s) still active", remaining);
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

        tracing::info!("Graceful shutdown complete");
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

        // Increment active connections now; the handler will decrement on exit.
        // The actual max_connections check happens after the startup handshake
        // so the client receives a properly-framed FATAL error.
        active.fetch_add(1, Ordering::Relaxed);

        if let Some(ref pool) = self.connection_pool {
            // Pool-based path: acquire handler from pool (bounded concurrency).
            let pool = pool.clone();

            tokio::spawn(async move {
                match pool.acquire().await {
                    Some(pooled) => {
                        let mut handler = pooled.handler_clone();
                        if let Some(ref rm) = replica_metrics {
                            handler.set_replica_metrics(rm.clone());
                        }
                        if let Err(e) = handle_connection_with_timeout(
                            stream, session_id, handler, timeout_ms, idle_ms,
                            max_connections, active.clone(),
                            auth_config, cancel_reg.clone(), notification_hub.clone(),
                        ).await {
                            tracing::error!("Connection error (session {}): {}", session_id, e);
                        }
                        // pooled guard dropped here → permit returned to pool
                        drop(pooled);
                    }
                    None => {
                        tracing::warn!("Connection pool exhausted, rejecting session {}", session_id);
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
                    stream, session_id, handler, timeout_ms, idle_ms,
                    max_connections, active.clone(),
                    auth_config, cancel_reg.clone(), notification_hub,
                ).await {
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

async fn handle_connection_with_timeout(
    mut stream: TcpStream,
    session_id: i32,
    handler: QueryHandler,
    default_statement_timeout_ms: u64,
    idle_timeout_ms: u64,
    max_connections: usize,
    active_connections: Arc<AtomicUsize>,
    auth_config: AuthConfig,
    cancel_registry: CancellationRegistry,
    notification_hub: Arc<NotificationHub>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = BytesMut::with_capacity(8192);
    let mut session = PgSession::new_with_hub(session_id, notification_hub);
    session.statement_timeout_ms = default_statement_timeout_ms;

    // Generate a random secret key for this session's cancel support
    let secret_key = {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        let s = RandomState::new();
        s.build_hasher().finish() as i32
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    cancel_registry.insert(session_id, CancelEntry {
        secret_key,
        cancelled: cancelled.clone(),
    });

    // Phase 1: Startup handshake
    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        match codec::decode_startup(&mut buf)? {
            Some(FrontendMessage::SslRequest) => {
                // SSL/TLS negotiation: respond 'S' if TLS is configured, 'N' otherwise.
                // A full TLS implementation would upgrade the stream here using
                // tokio_rustls or tokio_native_tls. For now we signal willingness
                // but fall back to 'N' since no TLS cert is configured at runtime.
                // When TLS certs are provided, replace this with stream upgrade.
                stream.write_all(b"N").await?;
                continue;
            }
            Some(FrontendMessage::CancelRequest { process_id, secret_key: sk }) => {
                // Cancel request arrives on a separate connection.
                // Look up the target session and signal cancellation.
                if let Some(entry) = cancel_registry.get(&process_id) {
                    if entry.secret_key == sk {
                        entry.cancelled.store(true, Ordering::SeqCst);
                        tracing::info!("Cancel request accepted for session {}", process_id);
                    } else {
                        tracing::warn!("Cancel request rejected for session {}: wrong secret key", process_id);
                    }
                }
                // Per PG protocol, the cancel connection is closed immediately
                return Ok(());
            }
            Some(FrontendMessage::Startup { version, params }) => {
                tracing::debug!(
                    "Startup: version={}, params={:?}",
                    version,
                    params
                );

                if let Some(user) = params.get("user") {
                    session.user = user.clone();
                }
                if let Some(db) = params.get("database") {
                    session.database = db.clone();
                }

                // Username check (if configured)
                if !auth_config.username.is_empty()
                    && session.user != auth_config.username
                {
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

                // Authentication handshake based on configured method
                match auth_config.method {
                    AuthMethod::Trust => {
                        send_message(&mut stream, &BackendMessage::AuthenticationOk).await?;
                    }
                    AuthMethod::Password => {
                        // Request cleartext password
                        send_message(&mut stream, &BackendMessage::AuthenticationCleartextPassword).await?;
                        stream.flush().await?;

                        // Read password response
                        let password = loop {
                            let pn = stream.read_buf(&mut buf).await?;
                            if pn == 0 {
                                return Ok(());
                            }
                            if let Some(FrontendMessage::PasswordMessage(pw)) = codec::decode_message(&mut buf)? {
                                break pw;
                            }
                        };

                        if password != auth_config.password {
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
                        send_message(&mut stream, &BackendMessage::AuthenticationMd5Password { salt }).await?;
                        stream.flush().await?;

                        // Read password response
                        let client_hash = loop {
                            let pn = stream.read_buf(&mut buf).await?;
                            if pn == 0 {
                                return Ok(());
                            }
                            if let Some(FrontendMessage::PasswordMessage(pw)) = codec::decode_message(&mut buf)? {
                                break pw;
                            }
                        };

                        // PG MD5 auth: md5(md5(password + user) + salt)
                        use md5::Digest;
                        let inner = {
                            let mut hasher = md5::Md5::new();
                            hasher.update(auth_config.password.as_bytes());
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
                        // Step 1: Send AuthenticationSASL with mechanism list
                        send_message(&mut stream, &BackendMessage::AuthenticationSASL {
                            mechanisms: vec!["SCRAM-SHA-256".into()],
                        }).await?;
                        stream.flush().await?;

                        // Step 2: Receive SASLInitialResponse (client-first-message)
                        let client_first_msg = loop {
                            let pn = stream.read_buf(&mut buf).await?;
                            if pn == 0 { return Ok(()); }
                            if let Some(FrontendMessage::PasswordMessage(pw)) = codec::decode_message(&mut buf)? {
                                break pw;
                            }
                        };

                        // Parse client-first-message: n,,n=<user>,r=<client-nonce>
                        let client_first_bare = if let Some(stripped) = client_first_msg.strip_prefix("n,,") {
                            stripped.to_string()
                        } else {
                            client_first_msg.clone()
                        };

                        let client_nonce = client_first_bare
                            .split(',')
                            .find(|p| p.starts_with("r="))
                            .map(|p| &p[2..])
                            .unwrap_or("")
                            .to_string();

                        // Generate server nonce and salt
                        use sha2::Digest as Sha2Digest;
                        let server_nonce_raw = {
                            use std::collections::hash_map::RandomState;
                            use std::hash::{BuildHasher, Hasher};
                            let s = RandomState::new();
                            format!("{:016x}", s.build_hasher().finish())
                        };
                        let combined_nonce = format!("{}{}", client_nonce, server_nonce_raw);
                        let iterations = 4096u32;
                        let salt_bytes: [u8; 16] = {
                            use std::collections::hash_map::RandomState;
                            use std::hash::{BuildHasher, Hasher};
                            let s1 = RandomState::new();
                            let s2 = RandomState::new();
                            let h1 = s1.build_hasher().finish().to_le_bytes();
                            let h2 = s2.build_hasher().finish().to_le_bytes();
                            let mut salt = [0u8; 16];
                            salt[..8].copy_from_slice(&h1);
                            salt[8..].copy_from_slice(&h2);
                            salt
                        };
                        let salt_b64 = base64_encode(&salt_bytes);

                        // Step 3: Send AuthenticationSASLContinue (server-first-message)
                        let server_first_msg = format!(
                            "r={},s={},i={}",
                            combined_nonce, salt_b64, iterations
                        );
                        send_message(&mut stream, &BackendMessage::AuthenticationSASLContinue {
                            data: server_first_msg.as_bytes().to_vec(),
                        }).await?;
                        stream.flush().await?;

                        // Step 4: Receive SASLResponse (client-final-message)
                        let client_final_msg = loop {
                            let pn = stream.read_buf(&mut buf).await?;
                            if pn == 0 { return Ok(()); }
                            if let Some(FrontendMessage::PasswordMessage(pw)) = codec::decode_message(&mut buf)? {
                                break pw;
                            }
                        };

                        // Parse client-final-message: c=<channel-binding>,r=<nonce>,p=<proof>
                        let client_proof_b64 = client_final_msg
                            .split(',')
                            .find(|p| p.starts_with("p="))
                            .map(|p| &p[2..])
                            .unwrap_or("");

                        // Derive keys using PBKDF2-HMAC-SHA256
                        let salted_password = pbkdf2_sha256(
                            auth_config.password.as_bytes(),
                            &salt_bytes,
                            iterations,
                        );

                        let client_key = hmac_sha256(&salted_password, b"Client Key");
                        let stored_key = {
                            let mut h = sha2::Sha256::new();
                            h.update(&client_key);
                            let result: [u8; 32] = h.finalize().into();
                            result
                        };
                        let server_key = hmac_sha256(&salted_password, b"Server Key");

                        // Build auth message for signature
                        let client_final_without_proof: String = client_final_msg
                            .split(',')
                            .filter(|p| !p.starts_with("p="))
                            .collect::<Vec<_>>()
                            .join(",");
                        let auth_message = format!(
                            "{},{},{}",
                            client_first_bare, server_first_msg, client_final_without_proof
                        );

                        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
                        let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());

                        // Verify client proof: ClientProof = ClientKey XOR ClientSignature
                        let expected_proof: Vec<u8> = client_key
                            .iter()
                            .zip(client_signature.iter())
                            .map(|(a, b)| a ^ b)
                            .collect();
                        let expected_proof_b64 = base64_encode(&expected_proof);

                        if client_proof_b64 != expected_proof_b64 {
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

                        // Step 5: Send AuthenticationSASLFinal (server signature)
                        let server_sig_b64 = base64_encode(&server_signature);
                        let server_final_msg = format!("v={}", server_sig_b64);
                        send_message(&mut stream, &BackendMessage::AuthenticationSASLFinal {
                            data: server_final_msg.as_bytes().to_vec(),
                        }).await?;
                        send_message(&mut stream, &BackendMessage::AuthenticationOk).await?;
                    }
                }

                // Send initial parameter statuses
                send_message(
                    &mut stream,
                    &BackendMessage::ParameterStatus {
                        name: "server_version".into(),
                        value: "15.0.0 FalconDB".into(),
                    },
                )
                .await?;
                send_message(
                    &mut stream,
                    &BackendMessage::ParameterStatus {
                        name: "server_encoding".into(),
                        value: "UTF8".into(),
                    },
                )
                .await?;
                send_message(
                    &mut stream,
                    &BackendMessage::ParameterStatus {
                        name: "client_encoding".into(),
                        value: "UTF8".into(),
                    },
                )
                .await?;
                send_message(
                    &mut stream,
                    &BackendMessage::ParameterStatus {
                        name: "DateStyle".into(),
                        value: "ISO, MDY".into(),
                    },
                )
                .await?;
                send_message(
                    &mut stream,
                    &BackendMessage::ParameterStatus {
                        name: "integer_datetimes".into(),
                        value: "on".into(),
                    },
                )
                .await?;
                send_message(
                    &mut stream,
                    &BackendMessage::ParameterStatus {
                        name: "standard_conforming_strings".into(),
                        value: "on".into(),
                    },
                )
                .await?;

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
                current, max_connections, session_id
            );
            let msg = BackendMessage::ErrorResponse {
                severity: "FATAL".into(),
                code: "53300".into(),
                message: format!(
                    "sorry, too many clients already ({} of {} connections used)",
                    current, max_connections
                ),
            };
            let _ = send_message(&mut stream, &msg).await;
            return Ok(());
        }
    }

    // Phase 2: Query loop
    loop {
        let n = if idle_timeout_ms > 0 {
            let idle_dur = std::time::Duration::from_millis(idle_timeout_ms);
            match tokio::time::timeout(idle_dur, stream.read_buf(&mut buf)).await {
                Ok(result) => result?,
                Err(_) => {
                    tracing::info!("Idle timeout ({}ms) for session {}", idle_timeout_ms, session_id);
                    let msg = BackendMessage::ErrorResponse {
                        severity: "FATAL".into(),
                        code: "57P01".into(),
                        message: format!("terminating connection due to idle timeout ({}ms)", idle_timeout_ms),
                    };
                    let _ = send_message(&mut stream, &msg).await;
                    return Ok(());
                }
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

        while let Some(msg) = codec::decode_message(&mut buf)? {
            match msg {
                FrontendMessage::Query(sql) => {
                    tracing::debug!("Query (session {}): {}", session_id, sql);

                    // Check for cancellation
                    if cancelled.swap(false, Ordering::SeqCst) {
                        tracing::info!("Query cancelled for session {}", session_id);
                        send_message(&mut stream, &BackendMessage::ErrorResponse {
                            severity: "ERROR".into(),
                            code: "57014".into(),
                            message: "canceling statement due to user request".into(),
                        }).await?;
                        send_message(
                            &mut stream,
                            &BackendMessage::ReadyForQuery {
                                txn_status: session.txn_status_byte(),
                            },
                        ).await?;
                        continue;
                    }

                    // Check for SET statement_timeout
                    if let Some(timeout_ms) = parse_set_statement_timeout(&sql) {
                        session.statement_timeout_ms = timeout_ms;
                        send_message(&mut stream, &BackendMessage::CommandComplete {
                            tag: "SET".into(),
                        }).await?;
                        send_message(
                            &mut stream,
                            &BackendMessage::ReadyForQuery {
                                txn_status: session.txn_status_byte(),
                            },
                        ).await?;
                        continue;
                    }

                    let responses = if session.statement_timeout_ms > 0 {
                        let timeout_dur = std::time::Duration::from_millis(session.statement_timeout_ms);
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
                        ).await {
                            Ok(Ok((responses, returned_session))) => {
                                session.restore_from_timeout(returned_session);
                                responses
                            }
                            Ok(Err(e)) => {
                                vec![BackendMessage::ErrorResponse {
                                    severity: "ERROR".into(),
                                    code: "XX000".into(),
                                    message: format!("Internal error: {}", e),
                                }]
                            }
                            Err(_) => {
                                tracing::warn!("Statement timeout ({}ms) for session {}", session.statement_timeout_ms, session_id);
                                vec![BackendMessage::ErrorResponse {
                                    severity: "ERROR".into(),
                                    code: "57014".into(),
                                    message: format!("canceling statement due to statement timeout ({}ms)", session.statement_timeout_ms),
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
                                            message: format!("Internal error: {}", e),
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
                                        let result_msgs = handler.handle_copy_data(&copy_buf, &mut session);
                                        for msg in &result_msgs {
                                            send_message(&mut stream, msg).await?;
                                        }
                                        // Send ReadyForQuery after COPY completes
                                        send_message(
                                            &mut stream,
                                            &BackendMessage::ReadyForQuery {
                                                txn_status: session.txn_status_byte(),
                                            },
                                        ).await?;
                                        // Break out of COPY receive loop
                                        copy_buf.clear();
                                        // Use a flag to break the outer read loop too
                                        break;
                                    }
                                    FrontendMessage::CopyFail(reason) => {
                                        tracing::warn!("COPY FROM STDIN failed (session {}): {}", session_id, reason);
                                        session.copy_state = None;
                                        send_message(&mut stream, &BackendMessage::ErrorResponse {
                                            severity: "ERROR".into(),
                                            code: "57014".into(),
                                            message: format!("COPY FROM STDIN failed: {}", reason),
                                        }).await?;
                                        send_message(
                                            &mut stream,
                                            &BackendMessage::ReadyForQuery {
                                                txn_status: session.txn_status_byte(),
                                            },
                                        ).await?;
                                        break;
                                    }
                                    _ => {
                                        // Unexpected message during COPY — abort
                                        tracing::warn!("Unexpected message during COPY (session {})", session_id);
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
                        send_message(&mut stream, &BackendMessage::NotificationResponse {
                            process_id: notif.sender_pid,
                            channel: notif.channel,
                            payload: notif.payload,
                        }).await?;
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
                FrontendMessage::Parse { name, query, param_types } => {
                    tracing::debug!("Parse (session {}): name={}, query={}, params={}", session_id, name, query, param_types.len());
                    // Try to parse+bind+plan the query for the plan-based path.
                    let parse_start = std::time::Instant::now();
                    let (plan, inferred_param_types, row_desc) = match handler.prepare_statement(&query) {
                        Ok((p, ipt, rd)) => {
                            let dur = parse_start.elapsed().as_micros() as u64;
                            falcon_observability::record_prepared_stmt_parse_duration_us(dur, true);
                            falcon_observability::record_prepared_stmt_op("parse", "plan");
                            (Some(p), ipt, rd)
                        }
                        Err(_e) => {
                            let dur = parse_start.elapsed().as_micros() as u64;
                            falcon_observability::record_prepared_stmt_parse_duration_us(dur, false);
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
                        inferred_param_types.iter().map(|t| handler.datatype_to_oid(t.as_ref())).collect()
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
                    falcon_observability::record_prepared_stmt_active(session.prepared_statements.len());
                    send_message(&mut stream, &BackendMessage::ParseComplete).await?;
                }
                FrontendMessage::Bind { portal, statement, param_formats, param_values, .. } => {
                    tracing::debug!("Bind (session {}): portal={}, stmt={}, params={}, formats={}", session_id, portal, statement, param_values.len(), param_formats.len());
                    let bind_start = std::time::Instant::now();
                    falcon_observability::record_prepared_stmt_param_count(param_values.len());
                    let ps = session.prepared_statements.get(&statement);
                    let (plan, params_datum, bound_sql) = if let Some(ps) = ps {
                        let datum_params: Vec<falcon_common::datum::Datum> = param_values.iter().enumerate().map(|(i, pv)| {
                            let fmt = resolve_param_format(&param_formats, i);
                            let type_hint = ps.inferred_param_types.get(i).and_then(|t| t.as_ref());
                            if fmt == 1 {
                                decode_param_value_binary(pv, type_hint)
                            } else {
                                decode_param_value(pv, type_hint)
                            }
                        }).collect();
                        let bound_sql = bind_params(&ps.query, &param_values);
                        (ps.plan.clone(), datum_params, bound_sql)
                    } else {
                        (None, vec![], String::new())
                    };
                    let path = if plan.is_some() { "plan" } else { "legacy" };
                    session.portals.insert(portal.clone(), crate::session::Portal {
                        plan,
                        params: params_datum,
                        bound_sql,
                    });
                    let bind_dur = bind_start.elapsed().as_micros() as u64;
                    falcon_observability::record_prepared_stmt_bind_duration_us(bind_dur);
                    falcon_observability::record_prepared_stmt_op("bind", path);
                    falcon_observability::record_prepared_stmt_portals_active(session.portals.len());
                    send_message(&mut stream, &BackendMessage::BindComplete).await?;
                }
                FrontendMessage::Describe { kind, name } => {
                    tracing::debug!("Describe (session {}): kind={}, name={}", session_id, kind as char, name);
                    falcon_observability::record_prepared_stmt_op("describe", if kind == b'S' { "statement" } else { "portal" });

                    if kind == b'S' {
                        // Statement describe: ParameterDescription + RowDescription/NoData
                        if let Some(ps) = session.prepared_statements.get(&name) {
                            send_message(&mut stream, &BackendMessage::ParameterDescription {
                                type_oids: ps.param_types.clone(),
                            }).await?;
                            // Use stored row_desc if available
                            if !ps.row_desc.is_empty() {
                                let fields = ps.row_desc.iter().map(|fd| crate::codec::FieldDescription {
                                    name: fd.name.clone(),
                                    table_oid: 0,
                                    column_attr: 0,
                                    type_oid: fd.type_oid,
                                    type_len: fd.type_len,
                                    type_modifier: -1,
                                    format_code: 0,
                                }).collect::<Vec<_>>();
                                send_message(&mut stream, &BackendMessage::RowDescription { fields }).await?;
                            } else {
                                // Fallback: describe by re-parsing
                                match handler.describe_query(&ps.query) {
                                    Ok(fields) if !fields.is_empty() => {
                                        send_message(&mut stream, &BackendMessage::RowDescription { fields }).await?;
                                    }
                                    _ => {
                                        send_message(&mut stream, &BackendMessage::NoData).await?;
                                    }
                                }
                            }
                        } else {
                            send_message(&mut stream, &BackendMessage::ParameterDescription { type_oids: vec![] }).await?;
                            send_message(&mut stream, &BackendMessage::NoData).await?;
                        }
                    } else {
                        // Portal describe
                        let sql_for_describe = session.portals.get(&name).map(|p| p.bound_sql.clone());
                        if let Some(ref sql) = sql_for_describe {
                            if !sql.is_empty() {
                                match handler.describe_query(sql) {
                                    Ok(fields) if !fields.is_empty() => {
                                        send_message(&mut stream, &BackendMessage::RowDescription { fields }).await?;
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
                    let exec_start = std::time::Instant::now();
                    let portal_data = session.portals.get(&portal).cloned();
                    if let Some(p) = portal_data {
                        // Helper: run the execution with optional statement timeout.
                        let responses: Vec<BackendMessage> = if session.statement_timeout_ms > 0 {
                            let timeout_dur = std::time::Duration::from_millis(session.statement_timeout_ms);
                            let handler_ref = &handler;
                            match tokio::time::timeout(
                                timeout_dur,
                                tokio::task::spawn_blocking({
                                    let p = p.clone();
                                    let handler = handler_ref.clone();
                                    let mut sess = session.take_for_timeout();
                                    move || {
                                        let responses = if let Some(ref plan) = p.plan {
                                            falcon_observability::record_prepared_stmt_op("execute", "plan");
                                            handler.execute_plan(plan, &p.params, &mut sess)
                                        } else if !p.bound_sql.is_empty() {
                                            falcon_observability::record_prepared_stmt_op("execute", "legacy");
                                            handler.handle_query(&p.bound_sql, &mut sess)
                                        } else {
                                            falcon_observability::record_prepared_stmt_op("execute", "empty");
                                            vec![BackendMessage::EmptyQueryResponse]
                                        };
                                        (responses, sess)
                                    }
                                }),
                            ).await {
                                Ok(Ok((responses, returned_session))) => {
                                    session.restore_from_timeout(returned_session);
                                    responses
                                }
                                Ok(Err(e)) => vec![BackendMessage::ErrorResponse {
                                    severity: "ERROR".into(),
                                    code: "XX000".into(),
                                    message: format!("Internal error: {}", e),
                                }],
                                Err(_) => {
                                    tracing::warn!(
                                        "Execute timeout ({}ms) for session {}",
                                        session.statement_timeout_ms, session_id
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
                        } else if let Some(ref plan) = p.plan {
                            falcon_observability::record_prepared_stmt_op("execute", "plan");
                            handler.execute_plan(plan, &p.params, &mut session)
                        } else if !p.bound_sql.is_empty() {
                            falcon_observability::record_prepared_stmt_op("execute", "legacy");
                            handler.handle_query(&p.bound_sql, &mut session)
                        } else {
                            falcon_observability::record_prepared_stmt_op("execute", "empty");
                            vec![BackendMessage::EmptyQueryResponse]
                        };

                        let exec_dur = exec_start.elapsed().as_micros() as u64;
                        let success = !responses.iter().any(|r| matches!(r, BackendMessage::ErrorResponse { .. }));
                        falcon_observability::record_prepared_stmt_execute_duration_us(exec_dur, success);
                        for response in &responses {
                            send_message(&mut stream, response).await?;
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
                        falcon_observability::record_prepared_stmt_active(session.prepared_statements.len());
                    } else {
                        session.portals.remove(&name);
                        falcon_observability::record_prepared_stmt_op("close", "portal");
                        falcon_observability::record_prepared_stmt_portals_active(session.portals.len());
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
                    tracing::warn!(
                        "Unexpected message in query phase (session {})",
                        session_id
                    );
                }
            }
        }
    }
}

async fn send_message(
    stream: &mut TcpStream,
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
        return sql.to_string();
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
        Some(DataType::Int32) => s.parse::<i32>().map(Datum::Int32).unwrap_or(Datum::Text(s.to_string())),
        Some(DataType::Int64) => s.parse::<i64>().map(Datum::Int64).unwrap_or(Datum::Text(s.to_string())),
        Some(DataType::Float64) => s.parse::<f64>().map(Datum::Float64).unwrap_or(Datum::Text(s.to_string())),
        Some(DataType::Boolean) => {
            match s.to_lowercase().as_str() {
                "t" | "true" | "1" | "yes" | "on" => Datum::Boolean(true),
                "f" | "false" | "0" | "no" | "off" => Datum::Boolean(false),
                _ => Datum::Text(s.to_string()),
            }
        }
        Some(DataType::Text) => Datum::Text(s.to_string()),
        Some(DataType::Timestamp) => Datum::Text(s.to_string()), // timestamp stored as text for now
        Some(DataType::Date) => Datum::Text(s.to_string()),
        Some(DataType::Array(_)) => Datum::Text(s.to_string()), // arrays as text for now
        Some(DataType::Jsonb) => Datum::Text(s.to_string()),    // jsonb as text for now
        Some(DataType::Decimal(_, _)) => Datum::parse_decimal(s).unwrap_or(Datum::Text(s.to_string())),
        None => {
            // No type hint: try integer, then float, then text
            if let Ok(i) = s.parse::<i64>() {
                Datum::Int64(i)
            } else if let Ok(f) = s.parse::<f64>() {
                Datum::Float64(f)
            } else {
                Datum::Text(s.to_string())
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
        Some(DataType::Int32) => {
            if bytes.len() == 4 {
                let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Datum::Int32(v)
            } else {
                // Fallback: try text parse
                let s = String::from_utf8_lossy(bytes);
                s.parse::<i32>().map(Datum::Int32).unwrap_or(Datum::Text(s.into_owned()))
            }
        }
        Some(DataType::Int64) => {
            if bytes.len() == 8 {
                let v = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Datum::Int64(v)
            } else if bytes.len() == 4 {
                // Some drivers send int4 binary for int8 columns
                let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Datum::Int64(v as i64)
            } else {
                let s = String::from_utf8_lossy(bytes);
                s.parse::<i64>().map(Datum::Int64).unwrap_or(Datum::Text(s.into_owned()))
            }
        }
        Some(DataType::Float64) => {
            if bytes.len() == 8 {
                let v = f64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Datum::Float64(v)
            } else if bytes.len() == 4 {
                // float4 binary
                let v = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Datum::Float64(v as f64)
            } else {
                let s = String::from_utf8_lossy(bytes);
                s.parse::<f64>().map(Datum::Float64).unwrap_or(Datum::Text(s.into_owned()))
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
        Some(DataType::Text) | Some(DataType::Timestamp) | Some(DataType::Date)
        | Some(DataType::Jsonb) | Some(DataType::Array(_)) => {
            Datum::Text(String::from_utf8_lossy(bytes).into_owned())
        }
        Some(DataType::Decimal(_, _)) => {
            let s = String::from_utf8_lossy(bytes);
            Datum::parse_decimal(&s).unwrap_or(Datum::Text(s.into_owned()))
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
                        bytes[0], bytes[1], bytes[2], bytes[3],
                        bytes[4], bytes[5], bytes[6], bytes[7],
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
    let value = rest.trim_end_matches(';').trim().trim_matches('\'').trim_matches('"');
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
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
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
        assert_eq!(parse_set_statement_timeout("SET statement_timeout = 5000"), Some(5000));
        assert_eq!(parse_set_statement_timeout("SET statement_timeout = 0"), Some(0));
        assert_eq!(parse_set_statement_timeout("set statement_timeout = 100;"), Some(100));
    }

    #[test]
    fn test_parse_set_statement_timeout_to() {
        assert_eq!(parse_set_statement_timeout("SET statement_timeout TO 3000"), Some(3000));
        assert_eq!(parse_set_statement_timeout("SET statement_timeout TO '5000'"), Some(5000));
    }

    #[test]
    fn test_parse_set_statement_timeout_default() {
        assert_eq!(parse_set_statement_timeout("SET statement_timeout = default"), Some(0));
        assert_eq!(parse_set_statement_timeout("SET statement_timeout TO default"), Some(0));
    }

    #[test]
    fn test_parse_set_statement_timeout_not_matching() {
        assert_eq!(parse_set_statement_timeout("SELECT 1"), None);
        assert_eq!(parse_set_statement_timeout("SET search_path = public"), None);
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
    fn test_decode_param_value_float64_hint() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(b"3.14".to_vec());
        match decode_param_value(&raw, Some(&DataType::Float64)) {
            Datum::Float64(v) => assert!((v - 3.14).abs() < 1e-10),
            other => panic!("expected Float64, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_param_value_boolean_hint() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        assert!(matches!(decode_param_value(&Some(b"true".to_vec()), Some(&DataType::Boolean)), Datum::Boolean(true)));
        assert!(matches!(decode_param_value(&Some(b"t".to_vec()), Some(&DataType::Boolean)), Datum::Boolean(true)));
        assert!(matches!(decode_param_value(&Some(b"false".to_vec()), Some(&DataType::Boolean)), Datum::Boolean(false)));
        assert!(matches!(decode_param_value(&Some(b"f".to_vec()), Some(&DataType::Boolean)), Datum::Boolean(false)));
        assert!(matches!(decode_param_value(&Some(b"0".to_vec()), Some(&DataType::Boolean)), Datum::Boolean(false)));
        assert!(matches!(decode_param_value(&Some(b"1".to_vec()), Some(&DataType::Boolean)), Datum::Boolean(true)));
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
    fn test_decode_param_value_no_hint_float() {
        use falcon_common::datum::Datum;
        let raw = Some(b"3.14".to_vec());
        match decode_param_value(&raw, None) {
            Datum::Float64(v) => assert!((v - 3.14).abs() < 1e-10),
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
    fn test_decode_binary_float64() {
        use falcon_common::datum::Datum;
        use falcon_common::types::DataType;
        let raw = Some(3.14f64.to_be_bytes().to_vec());
        match decode_param_value_binary(&raw, Some(&DataType::Float64)) {
            Datum::Float64(v) => assert!((v - 3.14).abs() < 1e-10),
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

    // ── SCRAM-SHA-256 crypto helper tests ──

    #[test]
    fn test_hmac_sha256_basic() {
        // RFC 4231 Test Case 2: "what do ya want for nothing?" with key "Jefe"
        let result = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex = result.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        assert_eq!(hex, "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
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
        use falcon_txn::TxnManager;
        use falcon_storage::engine::StorageEngine;

        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Executor::new_read_only(storage.clone(), txn_mgr.clone());

        assert!(executor.is_read_only(), "Executor should be in read-only mode");

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

        assert_eq!(server.default_statement_timeout_ms, 0, "Default should be no timeout");
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

        assert_eq!(server.idle_timeout_ms, 0, "Default should be no idle timeout");
        server.set_idle_timeout_ms(60_000);
        assert_eq!(server.idle_timeout_ms, 60_000);
    }

    /// parse_set_statement_timeout handles quoted values and semicolons.
    #[test]
    fn test_parse_set_statement_timeout_edge_cases() {
        assert_eq!(parse_set_statement_timeout("SET statement_timeout = '5000';"), Some(5000));
        assert_eq!(parse_set_statement_timeout("SET statement_timeout = \"5000\""), Some(5000));
        assert_eq!(parse_set_statement_timeout("  SET  statement_timeout  =  1000  "), Some(1000));
        // Non-numeric value that isn't "default" → None
        assert_eq!(parse_set_statement_timeout("SET statement_timeout = 'abc'"), None);
    }

    /// shutdown_drain_timeout_secs config field has correct default.
    #[test]
    fn test_shutdown_drain_timeout_default() {
        use falcon_common::config::FalconConfig;
        let config = FalconConfig::default();
        assert_eq!(config.server.shutdown_drain_timeout_secs, 30,
            "Default drain timeout should be 30 seconds");
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
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        };
        assert_eq!(config.shutdown_drain_timeout_secs, 60);
        assert_eq!(config.statement_timeout_ms, 5000);
        assert_eq!(config.idle_timeout_ms, 60000);
    }
}
