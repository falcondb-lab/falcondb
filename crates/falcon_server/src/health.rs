//! Minimal HTTP health check server for production readiness.
//!
//! Serves lightweight JSON responses on:
//! - `GET /health` — liveness probe (always 200 if process is up)
//! - `GET /ready`  — readiness probe (200 if node is ready to serve queries)
//! - `GET /status` — detailed status with metrics
//!
//! Uses raw TCP + tokio to avoid adding HTTP framework dependencies.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use falcon_common::config::NodeRole;
use falcon_storage::engine::StorageEngine;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_state(role: NodeRole) -> Arc<HealthState> {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let active = Arc::new(AtomicUsize::new(0));
        Arc::new(HealthState::new(role, active, storage))
    }

    // ── is_live / is_ready defaults ──

    #[test]
    fn test_health_state_defaults_live_and_ready() {
        let state = make_state(NodeRole::Standalone);
        assert!(state.is_live(), "Should be live by default");
        assert!(state.is_ready(), "Should be ready by default");
    }

    // ── set_ready transitions (shutdown drain) ──

    #[test]
    fn test_set_ready_false_marks_not_ready() {
        let state = make_state(NodeRole::Primary);
        assert!(state.is_ready());
        state.set_ready(false);
        assert!(!state.is_ready(), "Should be not-ready after set_ready(false)");
        // Restore
        state.set_ready(true);
        assert!(state.is_ready());
    }

    #[test]
    fn test_set_live_false_marks_not_live() {
        let state = make_state(NodeRole::Primary);
        assert!(state.is_live());
        state.set_live(false);
        assert!(!state.is_live(), "Should be not-live after set_live(false)");
        state.set_live(true);
        assert!(state.is_live());
    }

    // ── role_str ──

    #[test]
    fn test_role_str_all_variants() {
        assert_eq!(make_state(NodeRole::Primary).role_str(), "primary");
        assert_eq!(make_state(NodeRole::Replica).role_str(), "replica");
        assert_eq!(make_state(NodeRole::Analytics).role_str(), "analytics");
        assert_eq!(make_state(NodeRole::Standalone).role_str(), "standalone");
    }

    // ── uptime_secs ──

    #[test]
    fn test_uptime_secs_non_negative() {
        let state = make_state(NodeRole::Standalone);
        // Uptime should be 0 or very small immediately after creation
        assert!(state.uptime_secs() < 5, "Uptime should be < 5s just after creation");
    }

    // ── set_max_connections ──

    #[test]
    fn test_set_max_connections() {
        let state = make_state(NodeRole::Standalone);
        assert_eq!(state.max_connections.load(Ordering::Relaxed), 0);
        state.set_max_connections(1024);
        assert_eq!(state.max_connections.load(Ordering::Relaxed), 1024);
    }

    // ── active_connections reflected in state ──

    #[test]
    fn test_active_connections_reflected() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let active = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(HealthState::new(NodeRole::Standalone, active.clone(), storage));

        assert_eq!(state.active_connections.load(Ordering::Relaxed), 0);
        active.store(42, Ordering::Relaxed);
        assert_eq!(state.active_connections.load(Ordering::Relaxed), 42);
    }

    // ── HTTP response path tests (via handle_health_request) ──

    #[tokio::test]
    async fn test_http_live_endpoint_returns_200_when_live() {
        let state = make_state(NodeRole::Standalone);
        let response = make_http_request(&state, "GET /live HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "Expected 200, got: {}", &response[..50.min(response.len())]);
        assert!(response.contains("\"live\":true"));
    }

    #[tokio::test]
    async fn test_http_live_endpoint_returns_503_when_not_live() {
        let state = make_state(NodeRole::Standalone);
        state.set_live(false);
        let response = make_http_request(&state, "GET /live HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 503"), "Expected 503, got: {}", &response[..50.min(response.len())]);
        assert!(response.contains("\"live\":false"));
    }

    #[tokio::test]
    async fn test_http_healthz_alias_works() {
        let state = make_state(NodeRole::Standalone);
        let response = make_http_request(&state, "GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"live\":true"));
    }

    #[tokio::test]
    async fn test_http_health_legacy_returns_200_when_live() {
        let state = make_state(NodeRole::Replica);
        let response = make_http_request(&state, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"role\":\"replica\""));
    }

    #[tokio::test]
    async fn test_http_ready_returns_200_when_ready() {
        let state = make_state(NodeRole::Primary);
        let response = make_http_request(&state, "GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "Expected 200, got: {}", &response[..50.min(response.len())]);
        assert!(response.contains("\"ready\":true"));
        assert!(response.contains("\"role\":\"primary\""));
    }

    #[tokio::test]
    async fn test_http_ready_returns_503_during_shutdown() {
        let state = make_state(NodeRole::Primary);
        // Simulate graceful shutdown: mark not-ready
        state.set_ready(false);
        let response = make_http_request(&state, "GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 503"), "Expected 503 during shutdown, got: {}", &response[..50.min(response.len())]);
        assert!(response.contains("\"ready\":false"));
        assert!(response.contains("shutting down"));
    }

    #[tokio::test]
    async fn test_http_readyz_alias_works() {
        let state = make_state(NodeRole::Standalone);
        let response = make_http_request(&state, "GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"ready\":true"));
    }

    #[tokio::test]
    async fn test_http_status_returns_full_metrics() {
        let state = make_state(NodeRole::Standalone);
        state.set_max_connections(512);
        let response = make_http_request(&state, "GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"status\":\"ok\""));
        assert!(response.contains("\"max_connections\":512"));
        assert!(response.contains("\"live\":true"));
        assert!(response.contains("\"ready\":true"));
    }

    #[tokio::test]
    async fn test_http_unknown_path_returns_404() {
        let state = make_state(NodeRole::Standalone);
        let response = make_http_request(&state, "GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 404"), "Expected 404, got: {}", &response[..50.min(response.len())]);
        assert!(response.contains("\"error\":\"not found\""));
    }

    /// Helper: pipe an HTTP request through `handle_health_request` using a
    /// loopback TCP pair and return the full response as a String.
    async fn make_http_request(state: &Arc<HealthState>, request: &str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let state = state.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_health_request(stream, &state).await.unwrap();
        });

        // Connect and send the request
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(request.as_bytes()).await.unwrap();
        client.flush().await.unwrap();

        // Read the response
        let mut response = Vec::new();
        let _ = client.read_to_end(&mut response).await;

        let _ = server.await;
        String::from_utf8_lossy(&response).into_owned()
    }
}

/// Shared state for health check responses.
pub struct HealthState {
    /// When the server started.
    start_time: Instant,
    /// Node role (primary/replica/standalone).
    role: NodeRole,
    /// Whether the node is ready to serve queries.
    /// Set to false during graceful shutdown so load balancers stop routing traffic.
    ready: AtomicBool,
    /// Whether the process is alive (always true while the process is running).
    /// Set to false only in extreme failure scenarios.
    live: AtomicBool,
    /// Reference to active connection counter from PgServer.
    active_connections: Arc<AtomicUsize>,
    /// Max connections limit (0 = unlimited).
    max_connections: AtomicUsize,
    /// Reference to storage engine for WAL stats.
    storage: Arc<StorageEngine>,
}

impl HealthState {
    pub fn new(
        role: NodeRole,
        active_connections: Arc<AtomicUsize>,
        storage: Arc<StorageEngine>,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            role,
            ready: AtomicBool::new(true),
            live: AtomicBool::new(true),
            active_connections,
            max_connections: AtomicUsize::new(0),
            storage,
        }
    }

    /// Set the max connections limit for reporting in /status.
    pub fn set_max_connections(&self, max: usize) {
        self.max_connections.store(max, Ordering::Relaxed);
    }

    /// Mark the node as not ready (e.g. during graceful shutdown or bootstrap).
    /// Once not-ready, /ready returns 503 so load balancers drain connections.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    /// Mark the node as not live (extreme failure — triggers pod restart in Kubernetes).
    #[allow(dead_code)]
    pub fn set_live(&self, live: bool) {
        self.live.store(live, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::Relaxed)
    }

    fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    fn role_str(&self) -> &'static str {
        match self.role {
            NodeRole::Primary => "primary",
            NodeRole::Replica => "replica",
            NodeRole::Analytics => "analytics",
            NodeRole::Standalone => "standalone",
        }
    }
}

/// Run the health check HTTP server.
///
/// Listens on `addr` and serves health/readiness probes until
/// the `shutdown` future resolves.
pub async fn run_health_server(
    addr: &str,
    state: Arc<HealthState>,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            tracing::info!("Health check server listening on {}", addr);
            l
        }
        Err(e) => {
            tracing::error!("Failed to bind health check server on {}: {}", addr, e);
            return;
        }
    };

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let state = state.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_health_request(stream, &state).await {
                                tracing::debug!("Health check request error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::debug!("Health check accept error: {}", e);
                    }
                }
            }
            _ = &mut shutdown => {
                tracing::info!("Health check server shutting down");
                break;
            }
        }
    }
}

async fn handle_health_request(
    mut stream: tokio::net::TcpStream,
    state: &HealthState,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buf[..n]);

    // Extract path from "GET /path HTTP/1.x"
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, body) = match path {
        // Liveness probe: 200 while the process is alive, 503 if fatally broken.
        // Kubernetes restarts the pod on 503. This should almost never return 503.
        "/live" | "/healthz" => {
            if state.is_live() {
                let body = format!(
                    r#"{{"live":true,"uptime_secs":{},"role":"{}"}}"#,
                    state.uptime_secs(),
                    state.role_str()
                );
                ("200 OK", body)
            } else {
                let body = r#"{"live":false,"reason":"fatal error"}"#.to_string();
                ("503 Service Unavailable", body)
            }
        }
        // Legacy liveness alias (same as /live).
        "/health" => {
            let body = format!(
                r#"{{"status":"ok","live":{},"uptime_secs":{},"role":"{}"}}"#,
                state.is_live(),
                state.uptime_secs(),
                state.role_str()
            );
            if state.is_live() { ("200 OK", body) } else { ("503 Service Unavailable", body) }
        }
        // Readiness probe: 200 when ready to serve, 503 during shutdown/bootstrap.
        // Load balancers stop routing traffic on 503.
        "/ready" | "/readyz" => {
            let active = state.active_connections.load(Ordering::Relaxed);
            if state.is_ready() {
                let body = format!(
                    r#"{{"ready":true,"role":"{}","active_connections":{},"uptime_secs":{}}}"#,
                    state.role_str(),
                    active,
                    state.uptime_secs(),
                );
                ("200 OK", body)
            } else {
                let body = format!(
                    r#"{{"ready":false,"role":"{}","active_connections":{},"reason":"shutting down or bootstrapping"}}"#,
                    state.role_str(),
                    active,
                );
                ("503 Service Unavailable", body)
            }
        }
        // Detailed status: full metrics for monitoring dashboards.
        "/status" => {
            let wal_stats = state.storage.wal_stats_snapshot();
            let active = state.active_connections.load(Ordering::Relaxed);
            let max_conn = state.max_connections.load(Ordering::Relaxed);
            let body = format!(
                r#"{{"status":"ok","role":"{}","uptime_secs":{},"live":{},"ready":{},"active_connections":{},"max_connections":{},"wal_enabled":{},"wal_records_written":{},"wal_flushes":{},"wal_fsync_avg_us":{}}}"#,
                state.role_str(),
                state.uptime_secs(),
                state.is_live(),
                state.is_ready(),
                active,
                max_conn,
                state.storage.is_wal_enabled(),
                wal_stats.records_written,
                wal_stats.flushes,
                wal_stats.fsync_avg_us,
            );
            ("200 OK", body)
        }
        _ => {
            let body = r#"{"error":"not found"}"#.to_string();
            ("404 Not Found", body)
        }
    };

    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        body.len(),
        body
    );

    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}
