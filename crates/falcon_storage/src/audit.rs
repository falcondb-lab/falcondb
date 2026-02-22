//! P2-2c / DK §10: Audit log — off-hot-path, channel-backed audit event recorder.
//!
//! Records login/logout, DDL, privilege changes, role changes, config changes,
//! and authentication failures. Exposed via SHOW falcon.audit_log.
//!
//! ## Hot-path design
//! `record()` sends to a bounded `std::sync::mpsc` channel (non-blocking, O(1)).
//! A background thread drains the channel into the ring buffer under a Mutex.
//! This ensures the commit critical section is never blocked by audit I/O.
//! When the channel is full (backpressure), the event is dropped and
//! `dropped_events` is incremented for observability.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

use parking_lot::Mutex;

use falcon_common::security::{AuditEvent, AuditEventType};
use falcon_common::tenant::TenantId;

/// Maximum number of audit events kept in the ring buffer.
const AUDIT_LOG_CAPACITY: usize = 4096;

/// Capacity of the async channel between hot-path callers and the drain thread.
const AUDIT_CHANNEL_CAPACITY: usize = 8192;

/// Thread-safe audit log. `record()` is off the hot path — it sends to a
/// bounded channel; a background thread drains into the ring buffer.
pub struct AuditLog {
    /// Sender half of the async channel. Cloned per-caller; non-blocking send.
    tx: mpsc::SyncSender<AuditEvent>,
    /// Ring buffer of drained events (written by background thread only).
    events: Arc<Mutex<VecDeque<AuditEvent>>>,
    _capacity: usize,
    next_event_id: AtomicU64,
    /// Total events submitted (monotonic, includes dropped).
    total_events: AtomicU64,
    /// Events dropped because the channel was full (backpressure indicator).
    dropped_events: AtomicU64,
    /// Total events consumed by the drain thread (monotonic).
    drained_count: Arc<AtomicU64>,
    /// Background drain thread handle (kept alive for the lifetime of AuditLog).
    _drain_thread: std::thread::JoinHandle<()>,
}

impl AuditLog {
    pub fn new() -> Self {
        Self::with_capacity(AUDIT_LOG_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel::<AuditEvent>(AUDIT_CHANNEL_CAPACITY);
        let events = Arc::new(Mutex::new(VecDeque::with_capacity(capacity)));
        let events_clone = events.clone();
        let drained_count = Arc::new(AtomicU64::new(0));
        let drained_clone = drained_count.clone();

        let drain_thread = std::thread::Builder::new()
            .name("falcon-audit-drain".into())
            .spawn(move || {
                for event in rx {
                    let mut buf = events_clone.lock();
                    if buf.len() >= capacity {
                        buf.pop_front();
                    }
                    buf.push_back(event);
                    drained_clone.fetch_add(1, Ordering::Release);
                }
            })
            .unwrap_or_else(|e| {
                tracing::error!("failed to spawn audit drain thread: {}", e);
                // Spawn a no-op thread — audit will be non-functional but server won't crash
                std::thread::spawn(|| {})
            });

        Self {
            tx,
            events,
            _capacity: capacity,
            next_event_id: AtomicU64::new(1),
            total_events: AtomicU64::new(0),
            dropped_events: AtomicU64::new(0),
            drained_count,
            _drain_thread: drain_thread,
        }
    }

    /// Record an audit event. The event_id is auto-assigned.
    ///
    /// This method is **non-blocking**: it sends to the background channel.
    /// If the channel is full, the event is dropped and `dropped_events` is incremented.
    pub fn record(&self, mut event: AuditEvent) {
        event.event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        if event.timestamp_ms == 0 {
            event.timestamp_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
        }
        self.total_events.fetch_add(1, Ordering::Relaxed);

        // try_send is non-blocking: if channel full, drop the event (backpressure).
        if self.tx.try_send(event).is_err() {
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Convenience: record a login event.
    pub fn record_login(
        &self,
        tenant_id: TenantId,
        role_id: falcon_common::security::RoleId,
        role_name: &str,
        session_id: i32,
        source_ip: Option<String>,
        success: bool,
    ) {
        self.record(AuditEvent {
            event_id: 0,
            timestamp_ms: 0,
            event_type: if success {
                AuditEventType::Login
            } else {
                AuditEventType::AuthFailure
            },
            tenant_id,
            role_id,
            role_name: role_name.to_string(),
            session_id,
            source_ip,
            detail: if success {
                "Login successful".into()
            } else {
                "Authentication failed".into()
            },
            sql: None,
            success,
        });
    }

    /// Convenience: record a DDL event.
    pub fn record_ddl(
        &self,
        tenant_id: TenantId,
        role_id: falcon_common::security::RoleId,
        role_name: &str,
        session_id: i32,
        sql: &str,
        success: bool,
    ) {
        self.record(AuditEvent {
            event_id: 0,
            timestamp_ms: 0,
            event_type: AuditEventType::Ddl,
            tenant_id,
            role_id,
            role_name: role_name.to_string(),
            session_id,
            source_ip: None,
            detail: if success {
                format!("DDL executed: {}", truncate_sql(sql, 200))
            } else {
                format!("DDL failed: {}", truncate_sql(sql, 200))
            },
            sql: Some(truncate_sql(sql, 1000)),
            success,
        });
    }

    /// Convenience: record a privilege change (GRANT/REVOKE).
    pub fn record_privilege_change(
        &self,
        tenant_id: TenantId,
        role_id: falcon_common::security::RoleId,
        role_name: &str,
        session_id: i32,
        detail: &str,
    ) {
        self.record(AuditEvent {
            event_id: 0,
            timestamp_ms: 0,
            event_type: AuditEventType::PrivilegeChange,
            tenant_id,
            role_id,
            role_name: role_name.to_string(),
            session_id,
            source_ip: None,
            detail: detail.to_string(),
            sql: None,
            success: true,
        });
    }

    /// Snapshot of the most recent N events (newest first).
    pub fn snapshot(&self, limit: usize) -> Vec<AuditEvent> {
        let events = self.events.lock();
        events.iter().rev().take(limit).cloned().collect()
    }

    /// Snapshot filtered by event type.
    pub fn snapshot_by_type(&self, event_type: AuditEventType, limit: usize) -> Vec<AuditEvent> {
        let events = self.events.lock();
        events
            .iter()
            .rev()
            .filter(|e| e.event_type == event_type)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Snapshot filtered by tenant.
    pub fn snapshot_by_tenant(&self, tenant_id: TenantId, limit: usize) -> Vec<AuditEvent> {
        let events = self.events.lock();
        events
            .iter()
            .rev()
            .filter(|e| e.tenant_id == tenant_id)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Total number of events ever submitted (monotonic, includes dropped).
    pub fn total_events(&self) -> u64 {
        self.total_events.load(Ordering::Relaxed)
    }

    /// Events dropped due to channel backpressure (channel full).
    /// Non-zero values indicate the audit drain thread is falling behind.
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Current number of events in the ring buffer.
    pub fn buffered_count(&self) -> usize {
        self.events.lock().len()
    }

    /// Wait until the background drain thread has processed all pending events
    /// in the channel. Useful in tests to avoid race conditions.
    pub fn flush(&self) {
        // Wait until the drain thread has consumed all successfully-sent events.
        // drained_count is incremented by the drain thread after each event is
        // moved into the ring buffer, so when drained == sent we know the
        // channel is empty and the buffer is up-to-date.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let total = self.total_events.load(Ordering::SeqCst);
            let dropped = self.dropped_events.load(Ordering::SeqCst);
            let sent = total.saturating_sub(dropped);
            let drained = self.drained_count.load(Ordering::Acquire);
            if drained >= sent {
                break;
            }
            if std::time::Instant::now() > deadline {
                break; // safety valve
            }
            std::thread::yield_now();
        }
    }

    /// Clear all events from the buffer.
    pub fn clear(&self) {
        self.events.lock().clear();
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Truncate SQL for storage in audit events.
fn truncate_sql(sql: &str, max_len: usize) -> String {
    if sql.len() <= max_len {
        sql.to_string()
    } else {
        format!("{}...", &sql[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::security::{AuditEventType, RoleId};
    use falcon_common::tenant::{TenantId, SYSTEM_TENANT_ID};

    #[test]
    fn test_audit_log_record_and_snapshot() {
        let log = AuditLog::new();
        log.record_login(
            SYSTEM_TENANT_ID,
            RoleId(0),
            "falcon",
            1,
            Some("127.0.0.1".into()),
            true,
        );
        log.record_ddl(
            SYSTEM_TENANT_ID,
            RoleId(0),
            "falcon",
            1,
            "CREATE TABLE t(id INT)",
            true,
        );
        log.flush();

        assert_eq!(log.total_events(), 2);
        assert_eq!(log.buffered_count(), 2);

        let snap = log.snapshot(10);
        assert_eq!(snap.len(), 2);
        // Newest first
        assert_eq!(snap[0].event_type, AuditEventType::Ddl);
        assert_eq!(snap[1].event_type, AuditEventType::Login);
    }

    #[test]
    fn test_audit_log_capacity_eviction() {
        let log = AuditLog::with_capacity(3);
        for i in 0..5 {
            log.record_login(SYSTEM_TENANT_ID, RoleId(0), "falcon", i, None, true);
        }
        log.flush();
        assert_eq!(log.buffered_count(), 3);
        assert_eq!(log.total_events(), 5);

        let snap = log.snapshot(10);
        assert_eq!(snap.len(), 3);
        // Should have sessions 4, 3, 2 (newest first, oldest evicted)
        assert_eq!(snap[0].session_id, 4);
        assert_eq!(snap[2].session_id, 2);
    }

    #[test]
    fn test_snapshot_by_type() {
        let log = AuditLog::new();
        log.record_login(SYSTEM_TENANT_ID, RoleId(0), "falcon", 1, None, true);
        log.record_ddl(
            SYSTEM_TENANT_ID,
            RoleId(0),
            "falcon",
            1,
            "CREATE TABLE x(id INT)",
            true,
        );
        log.record_login(SYSTEM_TENANT_ID, RoleId(1), "alice", 2, None, false);
        log.flush();

        let logins = log.snapshot_by_type(AuditEventType::Login, 10);
        assert_eq!(logins.len(), 1);
        assert!(logins[0].success);

        let auth_failures = log.snapshot_by_type(AuditEventType::AuthFailure, 10);
        assert_eq!(auth_failures.len(), 1);
        assert!(!auth_failures[0].success);
    }

    #[test]
    fn test_snapshot_by_tenant() {
        let log = AuditLog::new();
        log.record_login(TenantId(1), RoleId(10), "alice", 1, None, true);
        log.record_login(TenantId(2), RoleId(20), "bob", 2, None, true);
        log.record_ddl(
            TenantId(1),
            RoleId(10),
            "alice",
            1,
            "CREATE TABLE t(x INT)",
            true,
        );
        log.flush();

        let t1_events = log.snapshot_by_tenant(TenantId(1), 10);
        assert_eq!(t1_events.len(), 2);
        assert!(t1_events.iter().all(|e| e.tenant_id == TenantId(1)));
    }

    #[test]
    fn test_clear() {
        let log = AuditLog::new();
        log.record_login(SYSTEM_TENANT_ID, RoleId(0), "falcon", 1, None, true);
        log.flush();
        assert_eq!(log.buffered_count(), 1);

        log.clear();
        assert_eq!(log.buffered_count(), 0);
        assert_eq!(log.total_events(), 1); // total is monotonic
    }

    #[test]
    fn test_auto_assigned_event_id_and_timestamp() {
        let log = AuditLog::new();
        log.record_login(SYSTEM_TENANT_ID, RoleId(0), "falcon", 1, None, true);
        log.record_login(SYSTEM_TENANT_ID, RoleId(0), "falcon", 2, None, true);
        log.flush();

        let snap = log.snapshot(10);
        assert_eq!(snap[0].event_id, 2);
        assert_eq!(snap[1].event_id, 1);
        assert!(snap[0].timestamp_ms > 0);
    }

    #[test]
    fn test_privilege_change_event() {
        let log = AuditLog::new();
        log.record_privilege_change(
            TenantId(1),
            RoleId(0),
            "falcon",
            1,
            "GRANT SELECT ON users TO alice",
        );
        log.flush();
        let snap = log.snapshot(1);
        assert_eq!(snap[0].event_type, AuditEventType::PrivilegeChange);
        assert!(snap[0].detail.contains("GRANT SELECT"));
    }
}
