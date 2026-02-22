//! Native protocol session state machine.
//!
//! Lifecycle: Connected → Handshake → Authenticated → Ready → Disconnected

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use falcon_protocol_native::types::*;

/// Session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Connected,
    Handshake,
    Authenticated,
    Ready,
    Disconnected,
}

/// A single native protocol session.
#[derive(Debug)]
pub struct NativeSession {
    pub id: u64,
    pub state: SessionState,
    pub client_name: String,
    pub database: String,
    pub user: String,
    pub feature_flags: u64,
    pub client_version_major: u16,
    pub client_version_minor: u16,
    pub negotiated_features: u64,
    pub client_epoch: u64,
    pub created_at: Instant,
    pub last_active: Instant,
    pub request_count: u64,
}

static SESSION_ID_SEQ: AtomicU64 = AtomicU64::new(1);

impl NativeSession {
    /// Create a new session in Connected state.
    pub fn new() -> Self {
        Self {
            id: SESSION_ID_SEQ.fetch_add(1, Ordering::Relaxed),
            state: SessionState::Connected,
            client_name: String::new(),
            database: String::new(),
            user: String::new(),
            feature_flags: 0,
            client_version_major: 0,
            client_version_minor: 0,
            negotiated_features: 0,
            client_epoch: 0,
            created_at: Instant::now(),
            last_active: Instant::now(),
            request_count: 0,
        }
    }

    /// Process a ClientHello, transition to Handshake state.
    pub fn on_client_hello(&mut self, hello: &ClientHello) {
        self.state = SessionState::Handshake;
        self.client_name = hello.client_name.clone();
        self.database = hello.database.clone();
        self.user = hello.user.clone();
        self.feature_flags = hello.feature_flags;
        self.client_version_major = hello.version_major;
        self.client_version_minor = hello.version_minor;
        self.last_active = Instant::now();
    }

    /// Mark session as authenticated.
    pub fn on_auth_ok(&mut self, negotiated_features: u64) {
        self.state = SessionState::Authenticated;
        self.negotiated_features = negotiated_features;
    }

    /// Transition to Ready state (after auth).
    pub fn set_ready(&mut self) {
        self.state = SessionState::Ready;
    }

    /// Mark session as disconnected.
    pub fn disconnect(&mut self) {
        self.state = SessionState::Disconnected;
    }

    /// Record a request.
    pub fn on_request(&mut self) {
        self.request_count += 1;
        self.last_active = Instant::now();
    }

    /// Whether this session is ready for queries.
    pub fn is_ready(&self) -> bool {
        self.state == SessionState::Ready
    }

    /// Session uptime.
    pub fn uptime_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }
}

impl Default for NativeSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe session registry.
#[derive(Debug)]
pub struct SessionRegistry {
    sessions: dashmap::DashMap<u64, Arc<parking_lot::Mutex<NativeSession>>>,
    max_sessions: usize,
}

impl SessionRegistry {
    pub fn new(max_sessions: usize) -> Self {
        Self {
            sessions: dashmap::DashMap::new(),
            max_sessions,
        }
    }

    /// Register a new session. Returns None if at capacity.
    pub fn register(
        &self,
        session: NativeSession,
    ) -> Option<Arc<parking_lot::Mutex<NativeSession>>> {
        if self.sessions.len() >= self.max_sessions {
            return None;
        }
        let id = session.id;
        let arc = Arc::new(parking_lot::Mutex::new(session));
        self.sessions.insert(id, arc.clone());
        Some(arc)
    }

    /// Remove a session by ID.
    pub fn remove(&self, id: u64) {
        self.sessions.remove(&id);
    }

    /// Current session count.
    pub fn count(&self) -> usize {
        self.sessions.len()
    }

    /// Get a snapshot of all session IDs.
    pub fn session_ids(&self) -> Vec<u64> {
        self.sessions.iter().map(|e| *e.key()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_lifecycle() {
        let mut s = NativeSession::new();
        assert_eq!(s.state, SessionState::Connected);

        s.on_client_hello(&ClientHello {
            version_major: 0,
            version_minor: 1,
            feature_flags: FEATURE_BATCH_INGEST,
            client_name: "test-driver".into(),
            database: "testdb".into(),
            user: "admin".into(),
            nonce: [0; 16],
            params: vec![],
        });
        assert_eq!(s.state, SessionState::Handshake);
        assert_eq!(s.client_name, "test-driver");

        s.on_auth_ok(FEATURE_BATCH_INGEST);
        assert_eq!(s.state, SessionState::Authenticated);

        s.set_ready();
        assert!(s.is_ready());

        s.on_request();
        assert_eq!(s.request_count, 1);

        s.disconnect();
        assert_eq!(s.state, SessionState::Disconnected);
        assert!(!s.is_ready());
    }

    #[test]
    fn test_session_registry() {
        let reg = SessionRegistry::new(2);
        assert_eq!(reg.count(), 0);

        let s1 = NativeSession::new();
        let s2 = NativeSession::new();
        let s3 = NativeSession::new();

        let id1 = s1.id;
        let id2 = s2.id;

        assert!(reg.register(s1).is_some());
        assert!(reg.register(s2).is_some());
        assert!(reg.register(s3).is_none()); // at capacity

        assert_eq!(reg.count(), 2);

        reg.remove(id1);
        assert_eq!(reg.count(), 1);

        // Now we can add another
        let s4 = NativeSession::new();
        assert!(reg.register(s4).is_some());
        assert_eq!(reg.count(), 2);

        let _ = id2;
    }
}
