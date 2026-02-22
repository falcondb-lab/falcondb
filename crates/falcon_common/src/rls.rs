//! Row-Level Security (RLS) — policy-based row filtering per user/role.
//!
//! PostgreSQL-compatible RLS implementation:
//! - Per-table policies with USING (read filter) and WITH CHECK (write filter)
//! - Policies can target specific commands (SELECT, INSERT, UPDATE, DELETE)
//! - Policies can target specific roles (or all roles via `PUBLIC`)
//! - Multiple policies on the same table are OR-combined (permissive) or AND-combined (restrictive)
//! - Table owners and superusers bypass RLS unless `FORCE ROW LEVEL SECURITY` is set

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use crate::security::RoleId;
use crate::types::TableId;

/// Unique identifier for an RLS policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PolicyId(pub u64);

impl fmt::Display for PolicyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "policy:{}", self.0)
    }
}

/// The command types a policy applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

impl fmt::Display for PolicyCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "ALL"),
            Self::Select => write!(f, "SELECT"),
            Self::Insert => write!(f, "INSERT"),
            Self::Update => write!(f, "UPDATE"),
            Self::Delete => write!(f, "DELETE"),
        }
    }
}

impl PolicyCommand {
    pub fn matches(&self, cmd: PolicyCommand) -> bool {
        *self == PolicyCommand::All || *self == cmd
    }
}

/// Policy type: permissive (OR-combined) or restrictive (AND-combined).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum PolicyPermissiveness {
    /// Multiple permissive policies are OR-combined: any match allows access.
    #[default]
    Permissive,
    /// Restrictive policies are AND-combined: all must match.
    Restrictive,
}

/// A Row-Level Security policy on a table.
///
/// SQL syntax:
/// ```sql
/// CREATE POLICY name ON table_name
///   [AS { PERMISSIVE | RESTRICTIVE }]
///   [FOR { ALL | SELECT | INSERT | UPDATE | DELETE }]
///   [TO { role_name | PUBLIC }]
///   USING (using_expression)
///   [WITH CHECK (check_expression)];
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsPolicy {
    pub id: PolicyId,
    pub name: String,
    pub table_id: TableId,
    pub table_name: String,
    /// Which command this policy applies to.
    pub command: PolicyCommand,
    /// Permissive (OR) or restrictive (AND).
    pub permissiveness: PolicyPermissiveness,
    /// Roles this policy applies to. Empty = applies to all roles (PUBLIC).
    pub target_roles: Vec<RoleId>,
    /// USING expression (SQL text) — applied as a filter for SELECT/UPDATE/DELETE reads.
    /// e.g., "tenant_id = current_setting('app.tenant_id')::int"
    pub using_expr: Option<String>,
    /// WITH CHECK expression (SQL text) — applied for INSERT/UPDATE writes.
    /// If not specified, defaults to the USING expression.
    pub check_expr: Option<String>,
    /// Whether this policy is enabled.
    pub enabled: bool,
}

impl RlsPolicy {
    /// Check if this policy applies to the given role and command.
    pub fn applies_to(&self, role_id: RoleId, cmd: PolicyCommand) -> bool {
        if !self.enabled {
            return false;
        }
        if !self.command.matches(cmd) {
            return false;
        }
        // Empty target_roles = PUBLIC (applies to all)
        if self.target_roles.is_empty() {
            return true;
        }
        self.target_roles.contains(&role_id)
    }

    /// Get the effective check expression (falls back to USING if not specified).
    pub fn effective_check_expr(&self) -> Option<&str> {
        self.check_expr.as_deref().or(self.using_expr.as_deref())
    }
}

/// Per-table RLS configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableRlsConfig {
    /// Whether RLS is enabled on this table (ALTER TABLE ... ENABLE ROW LEVEL SECURITY).
    pub enabled: bool,
    /// Whether RLS is forced even for the table owner (FORCE ROW LEVEL SECURITY).
    pub force: bool,
}

/// Central RLS policy manager.
///
/// Stores policies per table and provides lookup methods for the executor
/// to apply row-level filtering at query time.
#[derive(Debug, Clone, Default)]
pub struct RlsPolicyManager {
    /// All policies, indexed by ID for fast lookup.
    policies: HashMap<PolicyId, RlsPolicy>,
    /// Table → list of policy IDs, for fast table-scoped lookup.
    table_policies: HashMap<TableId, Vec<PolicyId>>,
    /// Per-table RLS configuration (enabled/force).
    table_config: HashMap<TableId, TableRlsConfig>,
    /// Next policy ID for auto-increment.
    next_id: u64,
}

impl RlsPolicyManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new policy. Returns the assigned PolicyId.
    pub fn create_policy(&mut self, mut policy: RlsPolicy) -> PolicyId {
        let id = PolicyId(self.next_id);
        self.next_id += 1;
        policy.id = id;
        let table_id = policy.table_id;
        self.policies.insert(id, policy);
        self.table_policies.entry(table_id).or_default().push(id);
        id
    }

    /// Drop a policy by name on a specific table.
    pub fn drop_policy(&mut self, table_id: TableId, policy_name: &str) -> Option<RlsPolicy> {
        let ids = self.table_policies.get(&table_id)?;
        let target_id = ids
            .iter()
            .find(|id| {
                self.policies
                    .get(id)
                    .is_some_and(|p| p.name.eq_ignore_ascii_case(policy_name))
            })
            .copied()?;

        if let Some(ids) = self.table_policies.get_mut(&table_id) {
            ids.retain(|id| *id != target_id);
        }
        self.policies.remove(&target_id)
    }

    /// Enable RLS on a table.
    pub fn enable_rls(&mut self, table_id: TableId) {
        self.table_config.entry(table_id).or_default().enabled = true;
    }

    /// Disable RLS on a table.
    pub fn disable_rls(&mut self, table_id: TableId) {
        self.table_config.entry(table_id).or_default().enabled = false;
    }

    /// Force RLS even for the table owner.
    pub fn force_rls(&mut self, table_id: TableId) {
        let config = self.table_config.entry(table_id).or_default();
        config.enabled = true;
        config.force = true;
    }

    /// Check if RLS is enabled on a table.
    pub fn is_rls_enabled(&self, table_id: TableId) -> bool {
        self.table_config.get(&table_id).is_some_and(|c| c.enabled)
    }

    /// Check if RLS is forced for table owner.
    pub fn is_rls_forced(&self, table_id: TableId) -> bool {
        self.table_config.get(&table_id).is_some_and(|c| c.force)
    }

    /// Check if a role should bypass RLS on a table.
    /// Superusers and table owners bypass unless FORCE is set.
    pub fn should_bypass(&self, table_id: TableId, is_superuser: bool, is_owner: bool) -> bool {
        if !self.is_rls_enabled(table_id) {
            return true; // RLS not enabled — bypass
        }
        if self.is_rls_forced(table_id) {
            return false; // FORCE — nobody bypasses
        }
        is_superuser || is_owner
    }

    /// Get all applicable policies for a table, role, and command.
    /// Returns (permissive_policies, restrictive_policies).
    pub fn applicable_policies(
        &self,
        table_id: TableId,
        role_id: RoleId,
        cmd: PolicyCommand,
    ) -> (Vec<&RlsPolicy>, Vec<&RlsPolicy>) {
        let mut permissive = Vec::new();
        let mut restrictive = Vec::new();

        if let Some(ids) = self.table_policies.get(&table_id) {
            for id in ids {
                if let Some(policy) = self.policies.get(id) {
                    if policy.applies_to(role_id, cmd) {
                        match policy.permissiveness {
                            PolicyPermissiveness::Permissive => permissive.push(policy),
                            PolicyPermissiveness::Restrictive => restrictive.push(policy),
                        }
                    }
                }
            }
        }
        (permissive, restrictive)
    }

    /// Collect the combined USING expressions for SELECT filtering.
    ///
    /// Per PG semantics:
    /// - Permissive policies are OR-combined
    /// - Restrictive policies are AND-combined
    /// - Final = (permissive1 OR permissive2 OR ...) AND restrictive1 AND restrictive2 AND ...
    /// - If no permissive policies exist, no rows are visible
    pub fn combined_using_expr(
        &self,
        table_id: TableId,
        role_id: RoleId,
        cmd: PolicyCommand,
    ) -> Option<String> {
        let (permissive, restrictive) = self.applicable_policies(table_id, role_id, cmd);

        let perm_exprs: Vec<&str> = permissive
            .iter()
            .filter_map(|p| p.using_expr.as_deref())
            .collect();

        if perm_exprs.is_empty() && permissive.is_empty() {
            // No policies at all — depends on whether RLS is enabled
            if self.is_rls_enabled(table_id) {
                return Some("false".to_string()); // RLS on but no policies → block all
            }
            return None; // RLS off → no filter
        }

        if perm_exprs.is_empty() {
            // Permissive policies exist but none have USING → block all
            return Some("false".to_string());
        }

        let mut parts = Vec::new();

        // OR-combine permissive
        if perm_exprs.len() == 1 {
            parts.push(perm_exprs[0].to_string());
        } else {
            let combined = perm_exprs
                .iter()
                .map(|e| format!("({})", e))
                .collect::<Vec<_>>()
                .join(" OR ");
            parts.push(format!("({})", combined));
        }

        // AND-combine restrictive
        for policy in &restrictive {
            if let Some(expr) = &policy.using_expr {
                parts.push(format!("({})", expr));
            }
        }

        Some(parts.join(" AND "))
    }

    /// Collect the combined WITH CHECK expression for INSERT/UPDATE filtering.
    pub fn combined_check_expr(
        &self,
        table_id: TableId,
        role_id: RoleId,
        cmd: PolicyCommand,
    ) -> Option<String> {
        let (permissive, restrictive) = self.applicable_policies(table_id, role_id, cmd);

        let perm_exprs: Vec<&str> = permissive
            .iter()
            .filter_map(|p| p.effective_check_expr())
            .collect();

        if perm_exprs.is_empty() && permissive.is_empty() {
            if self.is_rls_enabled(table_id) {
                return Some("false".to_string());
            }
            return None;
        }

        if perm_exprs.is_empty() {
            return Some("false".to_string());
        }

        let mut parts = Vec::new();

        if perm_exprs.len() == 1 {
            parts.push(perm_exprs[0].to_string());
        } else {
            let combined = perm_exprs
                .iter()
                .map(|e| format!("({})", e))
                .collect::<Vec<_>>()
                .join(" OR ");
            parts.push(format!("({})", combined));
        }

        for policy in &restrictive {
            if let Some(expr) = policy.effective_check_expr() {
                parts.push(format!("({})", expr));
            }
        }

        Some(parts.join(" AND "))
    }

    /// Get a policy by ID.
    pub fn get_policy(&self, id: PolicyId) -> Option<&RlsPolicy> {
        self.policies.get(&id)
    }

    /// List all policies on a table.
    pub fn policies_on_table(&self, table_id: TableId) -> Vec<&RlsPolicy> {
        self.table_policies
            .get(&table_id)
            .map(|ids| ids.iter().filter_map(|id| self.policies.get(id)).collect())
            .unwrap_or_default()
    }

    /// Total number of policies.
    pub fn policy_count(&self) -> usize {
        self.policies.len()
    }

    /// Drop all policies on a table (used when dropping the table).
    pub fn drop_all_policies(&mut self, table_id: TableId) {
        if let Some(ids) = self.table_policies.remove(&table_id) {
            for id in ids {
                self.policies.remove(&id);
            }
        }
        self.table_config.remove(&table_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_policy(name: &str, table_id: TableId, cmd: PolicyCommand, using: &str) -> RlsPolicy {
        RlsPolicy {
            id: PolicyId(0), // will be assigned
            name: name.to_string(),
            table_id,
            table_name: "test_table".to_string(),
            command: cmd,
            permissiveness: PolicyPermissiveness::Permissive,
            target_roles: vec![],
            using_expr: Some(using.to_string()),
            check_expr: None,
            enabled: true,
        }
    }

    #[test]
    fn test_create_and_drop_policy() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        let policy = make_policy("tenant_iso", tid, PolicyCommand::All, "tenant_id = 42");
        let id = mgr.create_policy(policy);
        assert_eq!(mgr.policy_count(), 1);
        assert!(mgr.get_policy(id).is_some());

        let dropped = mgr.drop_policy(tid, "tenant_iso");
        assert!(dropped.is_some());
        assert_eq!(mgr.policy_count(), 0);
    }

    #[test]
    fn test_enable_disable_rls() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        assert!(!mgr.is_rls_enabled(tid));

        mgr.enable_rls(tid);
        assert!(mgr.is_rls_enabled(tid));

        mgr.disable_rls(tid);
        assert!(!mgr.is_rls_enabled(tid));
    }

    #[test]
    fn test_force_rls() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        mgr.force_rls(tid);
        assert!(mgr.is_rls_enabled(tid));
        assert!(mgr.is_rls_forced(tid));
        // Even owner cannot bypass
        assert!(!mgr.should_bypass(tid, false, true));
        assert!(!mgr.should_bypass(tid, true, false));
    }

    #[test]
    fn test_bypass_when_rls_disabled() {
        let mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        assert!(mgr.should_bypass(tid, false, false));
    }

    #[test]
    fn test_bypass_for_owner_and_superuser() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        mgr.enable_rls(tid);
        assert!(mgr.should_bypass(tid, true, false)); // superuser
        assert!(mgr.should_bypass(tid, false, true)); // owner
        assert!(!mgr.should_bypass(tid, false, false)); // regular user
    }

    #[test]
    fn test_applicable_policies_by_role() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        let mut policy = make_policy("admin_only", tid, PolicyCommand::Select, "true");
        policy.target_roles = vec![RoleId(10)]; // only for role 10
        mgr.create_policy(policy);

        let (perm, _) = mgr.applicable_policies(tid, RoleId(10), PolicyCommand::Select);
        assert_eq!(perm.len(), 1);

        let (perm, _) = mgr.applicable_policies(tid, RoleId(99), PolicyCommand::Select);
        assert_eq!(perm.len(), 0); // different role — not applicable
    }

    #[test]
    fn test_applicable_policies_public() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        let policy = make_policy("all_users", tid, PolicyCommand::All, "active = true");
        mgr.create_policy(policy);

        // Empty target_roles = PUBLIC
        let (perm, _) = mgr.applicable_policies(tid, RoleId(999), PolicyCommand::Select);
        assert_eq!(perm.len(), 1);
    }

    #[test]
    fn test_combined_using_expr_single_permissive() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        mgr.enable_rls(tid);
        mgr.create_policy(make_policy("p1", tid, PolicyCommand::All, "tenant_id = 1"));

        let expr = mgr.combined_using_expr(tid, RoleId(1), PolicyCommand::Select);
        assert_eq!(expr.as_deref(), Some("tenant_id = 1"));
    }

    #[test]
    fn test_combined_using_expr_multiple_permissive() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        mgr.enable_rls(tid);
        mgr.create_policy(make_policy("p1", tid, PolicyCommand::All, "tenant_id = 1"));
        mgr.create_policy(make_policy(
            "p2",
            tid,
            PolicyCommand::All,
            "is_public = true",
        ));

        let expr = mgr
            .combined_using_expr(tid, RoleId(1), PolicyCommand::Select)
            .unwrap();
        assert!(expr.contains("OR"));
        assert!(expr.contains("tenant_id = 1"));
        assert!(expr.contains("is_public = true"));
    }

    #[test]
    fn test_combined_using_expr_with_restrictive() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        mgr.enable_rls(tid);
        mgr.create_policy(make_policy("p1", tid, PolicyCommand::All, "tenant_id = 1"));

        let mut restrictive = make_policy("r1", tid, PolicyCommand::All, "deleted_at IS NULL");
        restrictive.permissiveness = PolicyPermissiveness::Restrictive;
        mgr.create_policy(restrictive);

        let expr = mgr
            .combined_using_expr(tid, RoleId(1), PolicyCommand::Select)
            .unwrap();
        assert!(expr.contains("tenant_id = 1"));
        assert!(expr.contains("AND"));
        assert!(expr.contains("deleted_at IS NULL"));
    }

    #[test]
    fn test_rls_enabled_no_policies_blocks_all() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        mgr.enable_rls(tid);
        // No policies created
        let expr = mgr.combined_using_expr(tid, RoleId(1), PolicyCommand::Select);
        assert_eq!(expr.as_deref(), Some("false"));
    }

    #[test]
    fn test_check_expr_falls_back_to_using() {
        let policy = make_policy("p1", TableId(1), PolicyCommand::Insert, "tenant_id = 1");
        // check_expr is None, so effective_check_expr should use using_expr
        assert_eq!(policy.effective_check_expr(), Some("tenant_id = 1"));
    }

    #[test]
    fn test_drop_all_policies_on_table() {
        let mut mgr = RlsPolicyManager::new();
        let tid = TableId(1);
        mgr.enable_rls(tid);
        mgr.create_policy(make_policy("p1", tid, PolicyCommand::All, "true"));
        mgr.create_policy(make_policy("p2", tid, PolicyCommand::All, "false"));
        assert_eq!(mgr.policy_count(), 2);

        mgr.drop_all_policies(tid);
        assert_eq!(mgr.policy_count(), 0);
        assert!(!mgr.is_rls_enabled(tid));
    }

    #[test]
    fn test_policy_command_matching() {
        assert!(PolicyCommand::All.matches(PolicyCommand::Select));
        assert!(PolicyCommand::All.matches(PolicyCommand::Insert));
        assert!(PolicyCommand::Select.matches(PolicyCommand::Select));
        assert!(!PolicyCommand::Select.matches(PolicyCommand::Insert));
    }
}
