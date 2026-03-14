use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;

/// Snapshot of a single backend's state, exposed via pg_stat_activity.
pub struct SessionInfo {
    pub pid: i32,
    pub user: String,
    pub database: String,
    pub client_addr: String,
    pub application_name: String,
    pub backend_start: Instant,
    pub state: SessionState,
    /// Current or last query text.
    pub query: String,
    pub query_start: Option<Instant>,
    pub xact_start: Option<Instant>,
    pub state_change: Instant,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Idle,
    Active,
    IdleInTransaction,
    IdleInTransactionAborted,
}

impl SessionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Active => "active",
            Self::IdleInTransaction => "idle in transaction",
            Self::IdleInTransactionAborted => "idle in transaction (aborted)",
        }
    }
}

/// Shared registry of all active sessions. Used by pg_stat_activity.
#[derive(Clone)]
pub struct SessionRegistry {
    inner: Arc<DashMap<i32, SessionInfo>>,
    start_time: Instant,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            start_time: Instant::now(),
        }
    }

    pub fn register(
        &self,
        pid: i32,
        user: &str,
        database: &str,
        client_addr: &str,
        app_name: &str,
    ) {
        let now = Instant::now();
        self.inner.insert(
            pid,
            SessionInfo {
                pid,
                user: user.to_owned(),
                database: database.to_owned(),
                client_addr: client_addr.to_owned(),
                application_name: app_name.to_owned(),
                backend_start: now,
                state: SessionState::Idle,
                query: String::new(),
                query_start: None,
                xact_start: None,
                state_change: now,
            },
        );
    }

    pub fn deregister(&self, pid: i32) {
        self.inner.remove(&pid);
    }

    pub fn set_query_start(&self, pid: i32, sql: &str) {
        if let Some(mut entry) = self.inner.get_mut(&pid) {
            let now = Instant::now();
            entry.state = SessionState::Active;
            entry.query = sql.to_owned();
            entry.query_start = Some(now);
            entry.state_change = now;
        }
    }

    pub fn set_idle(&self, pid: i32, in_txn: bool) {
        if let Some(mut entry) = self.inner.get_mut(&pid) {
            let now = Instant::now();
            entry.state = if in_txn {
                SessionState::IdleInTransaction
            } else {
                SessionState::Idle
            };
            entry.state_change = now;
            if !in_txn {
                entry.xact_start = None;
            }
        }
    }

    pub fn set_xact_start(&self, pid: i32) {
        if let Some(mut entry) = self.inner.get_mut(&pid) {
            if entry.xact_start.is_none() {
                entry.xact_start = Some(Instant::now());
            }
        }
    }

    /// Snapshot all sessions for pg_stat_activity.
    pub fn snapshot(&self) -> Vec<SessionSnapshot> {
        let base = self.start_time;
        self.inner
            .iter()
            .map(|e| {
                let s = e.value();
                SessionSnapshot {
                    pid: s.pid,
                    user: s.user.clone(),
                    database: s.database.clone(),
                    client_addr: s.client_addr.clone(),
                    application_name: s.application_name.clone(),
                    backend_start_secs: s.backend_start.duration_since(base).as_secs_f64(),
                    state: s.state.as_str(),
                    query: s.query.clone(),
                    query_start_secs: s.query_start.map(|t| t.duration_since(base).as_secs_f64()),
                    xact_start_secs: s.xact_start.map(|t| t.duration_since(base).as_secs_f64()),
                }
            })
            .collect()
    }
}

/// Serializable snapshot of one session (no Instant references).
pub struct SessionSnapshot {
    pub pid: i32,
    pub user: String,
    pub database: String,
    pub client_addr: String,
    pub application_name: String,
    pub backend_start_secs: f64,
    pub state: &'static str,
    pub query: String,
    pub query_start_secs: Option<f64>,
    pub xact_start_secs: Option<f64>,
}
