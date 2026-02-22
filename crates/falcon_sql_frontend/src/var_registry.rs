//! Variable registry for SHOW command dispatch.
//!
//! Instead of hardcoding variable names in the binder's match arms, this
//! registry maps variable names to factory functions that produce the
//! appropriate `BoundStatement`. New variables can be added via
//! `register()` without modifying the binder.
//!
//! # Usage
//! ```ignore
//! let reg = VarRegistry::with_defaults();
//! // In the binder:
//! match reg.resolve("falcon_txn_stats") {
//!     Some(stmt) => Ok(stmt),
//!     None => Err(SqlError::Unsupported(...)),
//! }
//! ```

use std::collections::HashMap;

use crate::types::BoundStatement;

/// A function that produces a `BoundStatement` for a SHOW variable.
///
/// For variables that need a parameter (e.g. `SHOW falcon_table_stats_<name>`),
/// the `Option<String>` carries the suffix.
pub type VarHandler = Box<dyn Fn(Option<&str>) -> BoundStatement + Send + Sync>;

/// Registry of SHOW variable handlers.
///
/// Thread-safe (immutable after construction in typical use).
pub struct VarRegistry {
    /// Exact-match handlers: variable name → handler.
    exact: HashMap<String, VarHandler>,
    /// Prefix-match handlers: prefix → handler (receives the suffix).
    prefixes: Vec<(String, VarHandler)>,
}

impl VarRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            exact: HashMap::new(),
            prefixes: Vec::new(),
        }
    }

    /// Register an exact-match variable handler.
    pub fn register(&mut self, name: &str, handler: VarHandler) {
        self.exact.insert(name.to_string(), handler);
    }

    /// Register a prefix-match variable handler.
    /// When a variable name starts with `prefix`, the handler is called with
    /// the remaining suffix.
    pub fn register_prefix(&mut self, prefix: &str, handler: VarHandler) {
        self.prefixes.push((prefix.to_string(), handler));
    }

    /// Resolve a variable name to a `BoundStatement`, if registered.
    pub fn resolve(&self, var_name: &str) -> Option<BoundStatement> {
        // Try exact match first
        if let Some(handler) = self.exact.get(var_name) {
            return Some(handler(None));
        }
        // Try prefix match
        for (prefix, handler) in &self.prefixes {
            if let Some(suffix) = var_name.strip_prefix(prefix.as_str()) {
                return Some(handler(Some(suffix)));
            }
        }
        None
    }

    /// List all registered exact-match variable names (sorted).
    pub fn list_variables(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.exact.keys().map(|s| s.as_str()).collect();
        names.sort();
        names
    }

    /// Number of registered exact handlers.
    pub fn exact_count(&self) -> usize {
        self.exact.len()
    }

    /// Number of registered prefix handlers.
    pub fn prefix_count(&self) -> usize {
        self.prefixes.len()
    }

    /// Create a registry pre-populated with all built-in FalconDB variables.
    pub fn with_defaults() -> Self {
        let mut reg = Self::new();

        reg.register("falcon_txn_stats", Box::new(|_| BoundStatement::ShowTxnStats));
        reg.register("falcon_node_role", Box::new(|_| BoundStatement::ShowNodeRole));
        reg.register("falcon_wal_stats", Box::new(|_| BoundStatement::ShowWalStats));
        reg.register("falcon_connections", Box::new(|_| BoundStatement::ShowConnections));
        reg.register("falcon_gc", Box::new(|_| BoundStatement::RunGc));
        reg.register("falcon_table_stats", Box::new(|_| {
            BoundStatement::ShowTableStats { table_name: None }
        }));
        reg.register("falcon_sequences", Box::new(|_| BoundStatement::ShowSequences));
        reg.register("falcon_tenants", Box::new(|_| BoundStatement::ShowTenants));
        reg.register("falcon_tenant_usage", Box::new(|_| BoundStatement::ShowTenantUsage));

        // Prefix handler: falcon_table_stats_<table_name>
        reg.register_prefix("falcon_table_stats_", Box::new(|suffix| {
            let tname = suffix.unwrap_or("").to_string();
            BoundStatement::ShowTableStats { table_name: Some(tname) }
        }));

        reg
    }
}

impl Default for VarRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let reg = VarRegistry::with_defaults();
        let stmt = reg.resolve("falcon_txn_stats");
        assert!(stmt.is_some());
        assert!(matches!(stmt.unwrap(), BoundStatement::ShowTxnStats));
    }

    #[test]
    fn test_prefix_match() {
        let reg = VarRegistry::with_defaults();
        let stmt = reg.resolve("falcon_table_stats_users");
        assert!(stmt.is_some());
        match stmt.unwrap() {
            BoundStatement::ShowTableStats { table_name } => {
                assert_eq!(table_name, Some("users".to_string()));
            }
            _ => panic!("expected ShowTableStats"),
        }
    }

    #[test]
    fn test_unknown_variable() {
        let reg = VarRegistry::with_defaults();
        assert!(reg.resolve("unknown_var").is_none());
    }

    #[test]
    fn test_exact_takes_priority_over_prefix() {
        let reg = VarRegistry::with_defaults();
        // "falcon_table_stats" is exact → table_name: None
        let stmt = reg.resolve("falcon_table_stats").unwrap();
        match stmt {
            BoundStatement::ShowTableStats { table_name } => {
                assert_eq!(table_name, None);
            }
            _ => panic!("expected ShowTableStats with None"),
        }
    }

    #[test]
    fn test_list_variables() {
        let reg = VarRegistry::with_defaults();
        let vars = reg.list_variables();
        assert!(vars.contains(&"falcon_txn_stats"));
        assert!(vars.contains(&"falcon_gc"));
        assert!(vars.len() >= 9);
    }

    #[test]
    fn test_custom_variable() {
        let mut reg = VarRegistry::with_defaults();
        reg.register("my_custom_var", Box::new(|_| BoundStatement::ShowTxnStats));
        assert!(reg.resolve("my_custom_var").is_some());
    }

    #[test]
    fn test_counts() {
        let reg = VarRegistry::with_defaults();
        assert_eq!(reg.exact_count(), 9);
        assert_eq!(reg.prefix_count(), 1);
    }
}
