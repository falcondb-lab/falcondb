use std::collections::HashMap;
use std::sync::Arc;

use falcon_common::datum::Datum;
use falcon_common::types::{DataType, IsolationLevel, TableId};
use falcon_common::schema::TableSchema;
use falcon_common::tenant::{TenantId, SYSTEM_TENANT_ID};
use falcon_common::security::{RoleId, SUPERUSER_ROLE_ID, TxnPriority};
use falcon_planner::PhysicalPlan;
use falcon_txn::TxnHandle;

use crate::notify::{NotificationHub, SessionNotifications};

/// A prepared statement stored in the session.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Original SQL with $1, $2, ... placeholders.
    pub query: String,
    /// Parameter type OIDs declared at Parse time (0 = unspecified/infer).
    pub param_types: Vec<i32>,
    /// Physical plan produced by parse+bind+plan (None for legacy text-mode).
    pub plan: Option<PhysicalPlan>,
    /// Inferred parameter types from the binder's ParamEnv.
    pub inferred_param_types: Vec<Option<DataType>>,
    /// Output column descriptions (for Describe responses).
    pub row_desc: Vec<FieldDescriptionCompact>,
}

/// Compact column description stored in PreparedStatement.
/// Avoids coupling to the full BackendMessage FieldDescription.
#[derive(Debug, Clone)]
pub struct FieldDescriptionCompact {
    pub name: String,
    pub type_oid: i32,
    pub type_len: i16,
}

/// A portal: a prepared statement bound with concrete parameter values.
#[derive(Debug, Clone)]
pub struct Portal {
    /// The physical plan to execute.
    pub plan: Option<PhysicalPlan>,
    /// Concrete parameter values (1-indexed: $1 = params[0]).
    pub params: Vec<Datum>,
    /// Fallback: bound SQL string (for legacy text-substitution path).
    pub bound_sql: String,
}

/// Default session GUC variables.
fn default_guc_vars() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("server_version".into(), "15.0.0".into());
    m.insert("server_encoding".into(), "UTF8".into());
    m.insert("client_encoding".into(), "UTF8".into());
    m.insert("standard_conforming_strings".into(), "on".into());
    m.insert("search_path".into(), "\"$user\", public".into());
    m.insert("datestyle".into(), "ISO, MDY".into());
    m.insert("timezone".into(), "UTC".into());
    m.insert("integer_datetimes".into(), "on".into());
    m.insert("intervalstyle".into(), "postgres".into());
    m.insert("is_superuser".into(), "on".into());
    m.insert("default_transaction_isolation".into(), "read committed".into());
    m.insert("max_connections".into(), "100".into());
    m.insert("application_name".into(), String::new());
    m.insert("extra_float_digits".into(), "1".into());
    m.insert("lc_messages".into(), "en_US.UTF-8".into());
    m.insert("lc_monetary".into(), "en_US.UTF-8".into());
    m.insert("lc_numeric".into(), "en_US.UTF-8".into());
    m.insert("lc_time".into(), "en_US.UTF-8".into());
    m
}

/// COPY format options extracted from the COPY statement.
#[derive(Debug, Clone)]
pub struct CopyFormat {
    /// true = CSV, false = text (tab-delimited)
    pub csv: bool,
    /// Field delimiter character.
    pub delimiter: char,
    /// Whether the first line is a header row (skip on import, emit on export).
    pub header: bool,
    /// String that represents NULL.
    pub null_string: String,
    /// Quote character for CSV mode.
    pub quote: char,
    /// Escape character for CSV mode.
    pub escape: char,
}

impl Default for CopyFormat {
    fn default() -> Self {
        Self {
            csv: false,
            delimiter: '\t',
            header: false,
            null_string: "\\N".into(),
            quote: '"',
            escape: '"',
        }
    }
}

/// State for an in-progress COPY FROM STDIN operation.
#[derive(Debug, Clone)]
pub struct CopyState {
    pub table_name: String,
    pub table_id: TableId,
    pub schema: TableSchema,
    pub columns: Vec<usize>,
    pub format: CopyFormat,
}

/// State for a server-side cursor (DECLARE/FETCH/CLOSE).
#[derive(Debug, Clone)]
pub struct CursorState {
    /// Cursor name.
    pub name: String,
    /// Pre-computed result rows.
    pub rows: Vec<falcon_common::datum::OwnedRow>,
    /// Column descriptors.
    pub columns: Vec<(String, falcon_common::types::DataType)>,
    /// Current position (next row to fetch).
    pub position: usize,
    /// True if the cursor is holdable (survives transaction end).
    pub with_hold: bool,
}

/// Per-connection session state.
pub struct PgSession {
    /// Session ID (maps to PG backend process ID).
    pub id: i32,
    /// Current database name.
    pub database: String,
    /// Current user name.
    pub user: String,
    /// P2-1: Tenant this session belongs to.
    pub tenant_id: TenantId,
    /// P2-2: Role (user) for this session.
    pub role_id: RoleId,
    /// P2-3: Default transaction priority for this session.
    pub txn_priority: TxnPriority,
    /// Active transaction, if any.
    pub txn: Option<TxnHandle>,
    /// Whether we're in an implicit auto-commit transaction.
    pub autocommit: bool,
    /// Default isolation level for new transactions.
    pub default_isolation: IsolationLevel,
    /// Prepared statements: name → PreparedStatement (extended query protocol).
    pub prepared_statements: HashMap<String, PreparedStatement>,
    /// Portals: name → Portal (plan + param values).
    pub portals: HashMap<String, Portal>,
    /// Statement timeout in milliseconds (0 = no timeout).
    pub statement_timeout_ms: u64,
    /// Active COPY FROM STDIN state, if any.
    pub copy_state: Option<CopyState>,
    /// Session-level GUC variables (SET/SHOW/RESET).
    pub guc_vars: HashMap<String, String>,
    /// Savepoint stack for nested transaction control.
    pub savepoints: Vec<SavepointEntry>,
    /// Server-side cursors: name → CursorState (DECLARE/FETCH/CLOSE).
    pub cursors: HashMap<String, CursorState>,
    /// Per-session LISTEN/NOTIFY subscriptions.
    pub notifications: SessionNotifications,
    /// Shared notification hub (Arc so multiple sessions can share).
    pub notification_hub: Arc<NotificationHub>,
}

/// A savepoint entry captures the name and the write-set/read-set positions
/// at the time the savepoint was created, enabling partial rollback.
#[derive(Debug, Clone)]
pub struct SavepointEntry {
    pub name: String,
    /// Number of write-set entries at savepoint creation time.
    pub write_set_len: usize,
    /// Number of read-set entries at savepoint creation time.
    pub read_set_len: usize,
}

impl PgSession {
    pub fn new(id: i32) -> Self {
        Self {
            id,
            database: "falcon".into(),
            user: "falcon".into(),
            tenant_id: SYSTEM_TENANT_ID,
            role_id: SUPERUSER_ROLE_ID,
            txn_priority: TxnPriority::Normal,
            txn: None,
            autocommit: true,
            default_isolation: IsolationLevel::ReadCommitted,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            statement_timeout_ms: 0,
            copy_state: None,
            guc_vars: default_guc_vars(),
            savepoints: Vec::new(),
            cursors: HashMap::new(),
            notifications: SessionNotifications::new(),
            notification_hub: Arc::new(NotificationHub::new()),
        }
    }

    /// Create a session with a shared notification hub.
    pub fn new_with_hub(id: i32, hub: Arc<NotificationHub>) -> Self {
        let mut s = Self::new(id);
        s.notification_hub = hub;
        s
    }

    /// Find the position of the most recent savepoint with the given name.
    pub fn find_savepoint(&self, name: &str) -> Option<usize> {
        self.savepoints.iter().rposition(|sp| sp.name == name)
    }

    /// PG transaction status indicator byte for ReadyForQuery.
    pub fn txn_status_byte(&self) -> u8 {
        match &self.txn {
            None => b'I',    // idle
            Some(_) => b'T', // in transaction
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }

    /// Move session state out for use in a spawn_blocking closure.
    /// The original session retains its id and timeout but loses txn/prepared state.
    pub fn take_for_timeout(&mut self) -> PgSession {
        PgSession {
            id: self.id,
            database: self.database.clone(),
            user: self.user.clone(),
            tenant_id: self.tenant_id,
            role_id: self.role_id,
            txn_priority: self.txn_priority,
            txn: self.txn.take(),
            autocommit: self.autocommit,
            default_isolation: self.default_isolation,
            prepared_statements: std::mem::take(&mut self.prepared_statements),
            portals: std::mem::take(&mut self.portals),
            statement_timeout_ms: self.statement_timeout_ms,
            copy_state: self.copy_state.take(),
            guc_vars: self.guc_vars.clone(),
            savepoints: std::mem::take(&mut self.savepoints),
            cursors: std::mem::take(&mut self.cursors),
            notifications: std::mem::take(&mut self.notifications),
            notification_hub: self.notification_hub.clone(),
        }
    }

    /// Restore session state after a spawn_blocking closure returns.
    pub fn restore_from_timeout(&mut self, other: PgSession) {
        self.txn = other.txn;
        self.autocommit = other.autocommit;
        self.prepared_statements = other.prepared_statements;
        self.portals = other.portals;
        self.copy_state = other.copy_state;
        self.guc_vars = other.guc_vars;
        self.savepoints = other.savepoints;
        self.cursors = other.cursors;
        self.notifications = other.notifications;
    }

    /// Get a GUC variable value.
    pub fn get_guc(&self, name: &str) -> Option<&str> {
        self.guc_vars.get(&name.to_lowercase()).map(|s| s.as_str())
    }

    /// Set a GUC variable value.
    pub fn set_guc(&mut self, name: &str, value: &str) {
        self.guc_vars.insert(name.to_lowercase(), value.to_string());
    }

    /// Reset a GUC variable to its default value.
    pub fn reset_guc(&mut self, name: &str) {
        let defaults = default_guc_vars();
        let key = name.to_lowercase();
        if let Some(default_val) = defaults.get(&key) {
            self.guc_vars.insert(key, default_val.clone());
        } else {
            self.guc_vars.remove(&key);
        }
    }

    /// Reset all GUC variables to defaults.
    pub fn reset_all_gucs(&mut self) {
        self.guc_vars = default_guc_vars();
    }
}
