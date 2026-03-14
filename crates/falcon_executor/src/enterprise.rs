//! Enterprise runtime state for the Executor.
//!
//! Houses in-memory registries that back enterprise SQL commands:
//! - `RlsRegistry` — row level security policies per table
//! - `ReplicationSlotManager` — logical replication slot metadata

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::RwLock;

// ── Row Level Security ────────────────────────────────────────────────────────

/// A stored RLS policy (metadata only; expression evaluation is deferred).
#[derive(Debug, Clone)]
pub struct StoredPolicy {
    pub policy_name: String,
    pub table_name: String,
    pub command: String,    // "ALL" | "SELECT" | "INSERT" | "UPDATE" | "DELETE"
    pub permissive: bool,
    pub using_expr: Option<String>,
    pub check_expr: Option<String>,
}

/// Per-table RLS state: enabled flag + list of policies.
#[derive(Debug, Default)]
struct TableRlsState {
    enabled: bool,
    policies: Vec<StoredPolicy>,
}

/// In-memory registry of RLS policies. Thread-safe, shared via Arc.
#[derive(Debug, Default)]
pub struct RlsRegistry {
    tables: std::sync::RwLock<HashMap<String, TableRlsState>>,
}

impl RlsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_enabled(&self, table_name: &str, enabled: bool) {
        let mut g = self.tables.write().unwrap();
        g.entry(table_name.to_owned()).or_default().enabled = enabled;
    }

    pub fn is_enabled(&self, table_name: &str) -> bool {
        self.tables.read().unwrap().get(table_name).map_or(false, |s| s.enabled)
    }

    pub fn add_policy(
        &self,
        table_name: &str,
        policy_name: &str,
        command: &str,
        permissive: bool,
        using_expr: Option<String>,
        check_expr: Option<String>,
    ) {
        let mut g = self.tables.write().unwrap();
        let state = g.entry(table_name.to_owned()).or_default();
        // replace if same name already exists
        state.policies.retain(|p| p.policy_name != policy_name);
        state.policies.push(StoredPolicy {
            policy_name: policy_name.to_owned(),
            table_name: table_name.to_owned(),
            command: command.to_owned(),
            permissive,
            using_expr,
            check_expr,
        });
    }

    /// Collect active USING expressions for a table and command (SELECT/INSERT/UPDATE/DELETE/ALL).
    /// Returns raw SQL strings for all permissive policies whose USING clause is non-empty.
    pub fn get_using_exprs(&self, table_name: &str, command: &str) -> Vec<String> {
        let g = self.tables.read().unwrap();
        let Some(state) = g.get(table_name) else {
            return vec![];
        };
        if !state.enabled {
            return vec![];
        }
        state
            .policies
            .iter()
            .filter(|p| {
                p.permissive
                    && p.using_expr.is_some()
                    && (p.command == "ALL"
                        || p.command.eq_ignore_ascii_case(command)
                        || command == "ALL")
            })
            .filter_map(|p| p.using_expr.clone())
            .collect()
    }

    /// Returns true if the policy existed and was removed.
    pub fn drop_policy(&self, table_name: &str, policy_name: &str) -> bool {
        let mut g = self.tables.write().unwrap();
        if let Some(state) = g.get_mut(table_name) {
            let before = state.policies.len();
            state.policies.retain(|p| p.policy_name != policy_name);
            return state.policies.len() < before;
        }
        false
    }

    /// List policies. If `table_name` is Some, filter by table.
    /// Returns (table_name, policy_name, command, permissive, using_expr, check_expr, rls_enabled).
    pub fn list_policies(
        &self,
        table_name: Option<&str>,
    ) -> Vec<(String, String, String, bool, Option<String>, Option<String>, bool)> {
        type Row = (String, String, String, bool, Option<String>, Option<String>, bool);
        let g = self.tables.read().unwrap();
        let mut out: Vec<Row> = Vec::new();
        for (tbl, state) in g.iter() {
            if let Some(filter) = table_name {
                if tbl != filter {
                    continue;
                }
            }
            for p in &state.policies {
                out.push((
                    tbl.clone(),
                    p.policy_name.clone(),
                    p.command.clone(),
                    p.permissive,
                    p.using_expr.clone(),
                    p.check_expr.clone(),
                    state.enabled,
                ));
            }
            if state.policies.is_empty() && state.enabled {
                if table_name.is_none() || table_name == Some(tbl.as_str()) {
                    out.push((
                        tbl.clone(),
                        String::new(),
                        "ALL".to_owned(),
                        true,
                        None,
                        None,
                        true,
                    ));
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        out
    }
}

// ── Logical Replication Slots ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SlotRecord {
    pub name: String,
    pub plugin: String,
    pub confirmed_flush_lsn: u64,
    pub restart_lsn: u64,
    pub active: bool,
    pub created_at_unix_ms: u64,
}

/// In-memory replication slot manager.
#[derive(Debug)]
pub struct ReplicationSlotManager {
    slots: std::sync::RwLock<HashMap<String, SlotRecord>>,
    lsn: AtomicU64,
}

impl Default for ReplicationSlotManager {
    fn default() -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
            lsn: AtomicU64::new(0),
        }
    }
}

impl ReplicationSlotManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn current_lsn(&self) -> u64 {
        self.lsn.load(Ordering::Acquire)
    }

    pub fn advance_lsn(&self, new_lsn: u64) {
        self.lsn.fetch_max(new_lsn, Ordering::AcqRel);
    }

    pub fn create(&self, slot_name: &str, plugin: &str) {
        let lsn = self.lsn.load(Ordering::Acquire);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut g = self.slots.write().unwrap();
        g.entry(slot_name.to_owned()).or_insert_with(|| SlotRecord {
            name: slot_name.to_owned(),
            plugin: plugin.to_owned(),
            confirmed_flush_lsn: lsn,
            restart_lsn: lsn,
            active: true,
            created_at_unix_ms: now,
        });
    }

    pub fn remove(&self, slot_name: &str) {
        self.slots.write().unwrap().remove(slot_name);
    }

    pub fn list(&self) -> Vec<SlotRecord> {
        let g = self.slots.read().unwrap();
        let mut v: Vec<SlotRecord> = g.values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    pub fn advance_slot_lsn(&self, slot_name: &str, new_lsn: u64) {
        let mut g = self.slots.write().unwrap();
        if let Some(slot) = g.get_mut(slot_name) {
            slot.confirmed_flush_lsn = new_lsn;
        }
    }
}

// ── DML Audit Log ──────────────────────────────────────────────────

/// A single DML audit event recorded by the executor.
#[derive(Debug, Clone)]
pub struct AuditRecord {
    pub event_id: u64,
    pub timestamp_ms: u64,
    pub event_type: String,
    pub role_name: String,
    pub detail: String,
    pub sql: Option<String>,
    pub success: bool,
}

/// Ring-buffer DML audit log. Thread-safe, bounded, non-blocking.
pub struct DmlAuditLog {
    buf: std::sync::Mutex<std::collections::VecDeque<AuditRecord>>,
    capacity: usize,
    next_id: AtomicU64,
}

impl DmlAuditLog {
    pub fn new() -> Self {
        Self {
            buf: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(4096)),
            capacity: 4096,
            next_id: AtomicU64::new(1),
        }
    }

    pub fn record(&self, event_type: &str, role_name: &str, detail: &str, sql: Option<&str>, success: bool) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let rec = AuditRecord {
            event_id: id,
            timestamp_ms: now,
            event_type: event_type.to_owned(),
            role_name: role_name.to_owned(),
            detail: detail.to_owned(),
            sql: sql.map(|s| s.to_owned()),
            success,
        };
        let mut g = self.buf.lock().unwrap();
        if g.len() >= self.capacity {
            g.pop_front();
        }
        g.push_back(rec);
    }

    /// Return up to `limit` most recent events (newest first).
    pub fn snapshot(&self, limit: usize) -> Vec<AuditRecord> {
        let g = self.buf.lock().unwrap();
        g.iter().rev().take(limit).cloned().collect()
    }
}

impl Default for DmlAuditLog {
    fn default() -> Self { Self::new() }
}
