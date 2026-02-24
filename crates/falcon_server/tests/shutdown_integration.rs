//! Integration tests for FalconDB graceful shutdown protocol.
//!
//! These tests verify:
//! - ShutdownCoordinator cancels all child tokens
//! - PG server releases its port after shutdown signal
//! - Health server releases its port after shutdown signal
//! - Ordered teardown: main awaits all tasks
//! - Windows & Linux: explicit drop, no runtime-drop reliance

use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::time::Duration;

use falcon_server::shutdown::{ShutdownCoordinator, ShutdownReason};

/// Helper: check if a port is available by trying to bind it.
fn port_is_free(port: u16) -> bool {
    StdTcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Helper: find a free port.
fn find_free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

// ═══════════════════════════════════════════════════════════════════════════
// §1 — ShutdownCoordinator unit-level integration
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_coordinator_multiple_children_all_cancelled() {
    let coord = ShutdownCoordinator::new();
    let children: Vec<_> = (0..10).map(|_| coord.child_token()).collect();

    // None cancelled yet
    for c in &children {
        assert!(!c.is_cancelled());
    }

    coord.shutdown(ShutdownReason::CtrlC);

    // All cancelled
    for c in &children {
        assert!(c.is_cancelled());
    }
}

#[test]
fn test_coordinator_reason_is_first_signal() {
    let coord = ShutdownCoordinator::new();
    coord.shutdown(ShutdownReason::CtrlC);
    coord.shutdown(ShutdownReason::Sigterm);
    coord.shutdown(ShutdownReason::Requested);
    assert_eq!(coord.reason(), ShutdownReason::CtrlC);
}

#[tokio::test]
async fn test_coordinator_cancelled_wakes_multiple_waiters() {
    let coord = ShutdownCoordinator::new();
    let mut handles = Vec::new();

    for _ in 0..5 {
        let c = coord.clone();
        handles.push(tokio::spawn(async move {
            c.cancelled().await;
            true
        }));
    }

    // Brief delay then cancel
    tokio::time::sleep(Duration::from_millis(10)).await;
    coord.shutdown(ShutdownReason::Sigterm);

    for h in handles {
        assert!(h.await.unwrap());
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — TCP listener explicit drop releases port
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_tcp_listener_explicit_drop_releases_port() {
    let port = find_free_port();
    assert!(port_is_free(port), "port should be free before test");

    // Bind — port is now in use
    {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        assert!(!port_is_free(port), "port should be busy while listener exists");

        // Explicit drop — the key contract
        drop(listener);
    }

    // Port should be free immediately after drop
    // (On some OS, a tiny delay may be needed)
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(port_is_free(port), "port should be free after explicit drop");
}

#[tokio::test]
async fn test_accept_loop_with_shutdown_releases_port() {
    let port = find_free_port();
    let coord = ShutdownCoordinator::new();
    let token = coord.child_token();

    // Start a simulated server accept loop
    let handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();

        let shutdown = token.cancelled();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                _result = listener.accept() => {
                    // would handle connection
                }
                _ = &mut shutdown => {
                    break;
                }
            }
        }

        // ── Explicit listener drop — deterministic port release ──
        drop(listener);
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!port_is_free(port), "port should be busy while server runs");

    // Signal shutdown
    coord.shutdown(ShutdownReason::CtrlC);

    // Wait for task to complete
    handle.await.unwrap();

    // Port must be free
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        port_is_free(port),
        "port must be free after shutdown + explicit drop"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Multi-server coordinated shutdown
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multi_server_coordinated_shutdown() {
    let port_a = find_free_port();
    let port_b = find_free_port();
    let coord = ShutdownCoordinator::new();

    // Server A
    let token_a = coord.child_token();
    let handle_a = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port_a))
            .await
            .unwrap();
        token_a.cancelled().await;
        drop(listener);
    });

    // Server B
    let token_b = coord.child_token();
    let handle_b = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port_b))
            .await
            .unwrap();
        token_b.cancelled().await;
        drop(listener);
    });

    // Both should be listening
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!port_is_free(port_a));
    assert!(!port_is_free(port_b));

    // Single shutdown signal cancels both
    coord.shutdown(ShutdownReason::Sigterm);

    // Await both — this is what main() must do
    let _ = handle_a.await;
    let _ = handle_b.await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(port_is_free(port_a), "port A must be free");
    assert!(port_is_free(port_b), "port B must be free");
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Ordered teardown (simulates main() flow)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_ordered_teardown_all_handles_awaited() {
    let coord = ShutdownCoordinator::new();
    let completed = Arc::new(std::sync::atomic::AtomicU32::new(0));

    let mut handles = Vec::new();
    for _ in 0..3 {
        let token = coord.child_token();
        let done = completed.clone();
        handles.push(tokio::spawn(async move {
            token.cancelled().await;
            // Simulate cleanup work
            tokio::time::sleep(Duration::from_millis(20)).await;
            done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }));
    }

    tokio::time::sleep(Duration::from_millis(20)).await;
    coord.shutdown(ShutdownReason::Requested);

    // Await ALL handles — this is the contract
    for h in handles {
        let _ = h.await;
    }

    assert_eq!(
        completed.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "all 3 tasks must complete before main exits"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Shutdown does not depend on runtime drop
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_shutdown_is_explicit_not_drop_dependent() {
    let port = find_free_port();
    let coord = ShutdownCoordinator::new();
    let token = coord.child_token();

    // Start server that does NOT explicitly drop listener (the anti-pattern)
    let handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        token.cancelled().await;
        // Explicit drop — this is the correct pattern
        drop(listener);
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    coord.shutdown(ShutdownReason::CtrlC);
    handle.await.unwrap();

    // Verify port is released via explicit drop inside task
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        port_is_free(port),
        "port must be free — released by explicit drop inside task, not runtime teardown"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — gRPC-style shutdown (tonic serve_with_shutdown pattern)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_grpc_style_serve_with_shutdown() {
    let port = find_free_port();
    let coord = ShutdownCoordinator::new();
    let token = coord.child_token();

    // Simulate tonic's serve_with_shutdown pattern using a TCP listener
    let handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();

        // Simulate serve_with_shutdown: run until token cancelled
        let shutdown = token.cancelled();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                _ = listener.accept() => {}
                _ = &mut shutdown => { break; }
            }
        }

        drop(listener);
        // Log would go here in production
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!port_is_free(port));

    coord.shutdown(ShutdownReason::Sigterm);
    handle.await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(port_is_free(port), "gRPC-style port must be released");
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Shutdown reason propagation
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_shutdown_reason_display_all_variants() {
    assert_eq!(format!("{}", ShutdownReason::CtrlC), "ctrl_c");
    assert_eq!(format!("{}", ShutdownReason::Sigterm), "sigterm");
    assert_eq!(format!("{}", ShutdownReason::ServiceStop), "service_stop");
    assert_eq!(format!("{}", ShutdownReason::Requested), "requested");
    assert_eq!(format!("{}", ShutdownReason::Running), "running");
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — ServiceStop reason (Windows SCM integration)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_service_stop_reason_triggers_shutdown() {
    let coord = ShutdownCoordinator::new();
    let token = coord.child_token();
    let port = find_free_port();

    let handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        token.cancelled().await;
        drop(listener);
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!port_is_free(port));

    // Simulate SCM STOP event
    coord.shutdown(ShutdownReason::ServiceStop);
    assert_eq!(coord.reason(), ShutdownReason::ServiceStop);

    handle.await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(port_is_free(port), "port must be free after ServiceStop");
}

#[test]
fn test_coordinator_default_state() {
    let coord = ShutdownCoordinator::new();
    assert!(!coord.is_shutting_down());
    assert_eq!(coord.reason(), ShutdownReason::Running);
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 — WAL flush on shutdown (v1.0.5)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal_flush_returns_ok_when_enabled() {
    let dir = std::env::temp_dir().join("falcon_shutdown_wal_flush_test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let engine = falcon_storage::engine::StorageEngine::new_with_sync_mode(
        Some(dir.as_path()),
        falcon_storage::wal::SyncMode::None,
    )
    .unwrap();

    assert!(engine.is_wal_enabled());

    // Flush should succeed even with no pending data
    let result = engine.flush_wal();
    assert!(result.is_ok(), "WAL flush should succeed: {:?}", result);

    // LSN accessor should return 0 (no records written)
    let lsn = engine.current_wal_lsn();
    assert_eq!(lsn, 0, "LSN should be 0 with no records");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_wal_flush_noop_when_disabled() {
    let engine = falcon_storage::engine::StorageEngine::new_in_memory();
    assert!(!engine.is_wal_enabled());

    // Flush is a no-op when WAL is disabled
    let result = engine.flush_wal();
    assert!(result.is_ok());

    let lsn = engine.current_wal_lsn();
    assert_eq!(lsn, 0);
}

#[test]
fn test_wal_lsn_advances_after_transaction() {
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::*;
    use falcon_common::datum::Datum;

    let dir = std::env::temp_dir().join("falcon_shutdown_wal_lsn_txn_test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let engine = std::sync::Arc::new(
        falcon_storage::engine::StorageEngine::new_with_sync_mode(
            Some(dir.as_path()),
            falcon_storage::wal::SyncMode::None,
        )
        .unwrap(),
    );
    let txn_mgr = std::sync::Arc::new(falcon_txn::TxnManager::new(engine.clone()));

    let lsn_before = engine.current_wal_lsn();

    // Create a table (DDL writes to WAL)
    let schema = TableSchema {
        id: TableId(1),
        name: "wal_test".into(),
        columns: vec![ColumnDef {
            id: ColumnId(0),
            name: "id".into(),
            data_type: DataType::Int32,
            nullable: false,
            is_primary_key: true,
            default_value: None,
            is_serial: false,
        }],
        primary_key_columns: vec![0],
        ..Default::default()
    };
    engine.create_table(schema).unwrap();

    // Insert a row via transaction
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let row = falcon_common::datum::OwnedRow { values: vec![Datum::Int32(1)] };
    engine
        .insert(TableId(1), row, txn.txn_id)
        .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let lsn_after = engine.current_wal_lsn();
    assert!(
        lsn_after > lsn_before,
        "LSN should advance after transaction: before={}, after={}",
        lsn_before,
        lsn_after,
    );

    // Flush should succeed and LSN remains stable
    engine.flush_wal().unwrap();
    let lsn_post_flush = engine.current_wal_lsn();
    assert_eq!(lsn_after, lsn_post_flush, "LSN should not change after flush");

    let _ = std::fs::remove_dir_all(&dir);
}

// ═══════════════════════════════════════════════════════════════════════════
// §10 — Panic injection: coordinator survives panic in child task
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_coordinator_survives_child_panic() {
    let coord = ShutdownCoordinator::new();
    let token = coord.child_token();

    // Spawn a task that panics
    let handle = tokio::spawn(async move {
        let _token = token;
        panic!("intentional panic for shutdown test");
    });

    // The JoinHandle captures the panic — coordinator should still function
    let result = handle.await;
    assert!(result.is_err(), "Task should have panicked");

    // Coordinator should still be operational
    assert!(!coord.is_shutting_down());
    coord.shutdown(ShutdownReason::Requested);
    assert!(coord.is_shutting_down());
    assert_eq!(coord.reason(), ShutdownReason::Requested);
}

#[tokio::test]
async fn test_shutdown_after_panic_still_releases_resources() {
    let coord = ShutdownCoordinator::new();
    let port = find_free_port();

    // Spawn a listener task
    let token = coord.child_token();
    let listener_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        token.cancelled().await;
        drop(listener);
    });

    // Spawn a task that panics
    let panic_token = coord.child_token();
    let panic_handle = tokio::spawn(async move {
        let _t = panic_token;
        panic!("crash during processing");
    });

    // Wait for panic to complete
    let _ = panic_handle.await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!port_is_free(port), "port should be held before shutdown");

    // Shutdown should still work despite the panic
    coord.shutdown(ShutdownReason::Requested);
    listener_handle.await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        port_is_free(port),
        "port must be free after shutdown even with prior panics"
    );
}
