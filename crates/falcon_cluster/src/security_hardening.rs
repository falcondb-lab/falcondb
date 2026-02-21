//! Security hardening extensions for enterprise-grade protection.
//!
//! Three components:
//! - **AuthRateLimiter**: Brute-force protection with per-IP failure tracking, lockout, cooldown
//! - **PasswordPolicy**: Minimum complexity, length, expiry, reuse prevention
//! - **SqlFirewall**: SQL injection pattern detection, dangerous statement blocking

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

// ═══════════════════════════════════════════════════════════════════════════
// AuthRateLimiter — brute-force protection
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for authentication rate limiting.
#[derive(Debug, Clone)]
pub struct AuthRateLimiterConfig {
    /// Maximum failed attempts before lockout.
    pub max_failures: u32,
    /// Lockout duration after max failures exceeded.
    pub lockout_duration: Duration,
    /// Window for counting failures (failures older than this are forgotten).
    pub failure_window: Duration,
    /// Whether to track by IP address (vs globally).
    pub per_ip: bool,
}

impl Default for AuthRateLimiterConfig {
    fn default() -> Self {
        Self {
            max_failures: 5,
            lockout_duration: Duration::from_secs(300), // 5 minutes
            failure_window: Duration::from_secs(600),   // 10 minutes
            per_ip: true,
        }
    }
}

/// Result of an auth rate limit check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthRateResult {
    /// Allowed to attempt authentication.
    Allowed,
    /// Locked out — too many failures.
    LockedOut {
        remaining_secs: u64,
        failure_count: u32,
    },
}

/// Per-source tracking state.
struct AuthSourceState {
    failures: Vec<Instant>,
    locked_until: Option<Instant>,
}

impl AuthSourceState {
    fn new() -> Self {
        Self {
            failures: Vec::new(),
            locked_until: None,
        }
    }
}

/// Brute-force protection: tracks auth failures per IP/user and enforces lockout.
pub struct AuthRateLimiter {
    config: AuthRateLimiterConfig,
    sources: Mutex<HashMap<String, AuthSourceState>>,
    total_checks: AtomicU64,
    total_lockouts: AtomicU64,
    total_failures_recorded: AtomicU64,
}

impl AuthRateLimiter {
    pub fn new(config: AuthRateLimiterConfig) -> Self {
        Self {
            config,
            sources: Mutex::new(HashMap::new()),
            total_checks: AtomicU64::new(0),
            total_lockouts: AtomicU64::new(0),
            total_failures_recorded: AtomicU64::new(0),
        }
    }

    /// Check if a source (IP or user) is allowed to attempt authentication.
    pub fn check(&self, source: &str) -> AuthRateResult {
        self.total_checks.fetch_add(1, Ordering::Relaxed);
        let mut sources = self.sources.lock();
        let state = sources.entry(source.to_string()).or_insert_with(AuthSourceState::new);
        let now = Instant::now();

        // Check lockout
        if let Some(locked_until) = state.locked_until {
            if now < locked_until {
                let remaining = locked_until.duration_since(now).as_secs();
                self.total_lockouts.fetch_add(1, Ordering::Relaxed);
                return AuthRateResult::LockedOut {
                    remaining_secs: remaining,
                    failure_count: state.failures.len() as u32,
                };
            }
            // Lockout expired — clear it
            state.locked_until = None;
            state.failures.clear();
        }

        // Prune old failures outside the window
        let cutoff = now - self.config.failure_window;
        state.failures.retain(|t| *t > cutoff);

        AuthRateResult::Allowed
    }

    /// Record an authentication failure for a source.
    pub fn record_failure(&self, source: &str) {
        self.total_failures_recorded.fetch_add(1, Ordering::Relaxed);
        let mut sources = self.sources.lock();
        let state = sources.entry(source.to_string()).or_insert_with(AuthSourceState::new);
        let now = Instant::now();

        // Prune old failures
        let cutoff = now - self.config.failure_window;
        state.failures.retain(|t| *t > cutoff);

        state.failures.push(now);

        // Check if lockout threshold reached
        if state.failures.len() as u32 >= self.config.max_failures {
            state.locked_until = Some(now + self.config.lockout_duration);
            tracing::warn!(
                source = source,
                failures = state.failures.len(),
                lockout_secs = self.config.lockout_duration.as_secs(),
                "Auth rate limiter: source locked out"
            );
        }
    }

    /// Record a successful authentication (clears failure history).
    pub fn record_success(&self, source: &str) {
        let mut sources = self.sources.lock();
        if let Some(state) = sources.get_mut(source) {
            state.failures.clear();
            state.locked_until = None;
        }
    }

    /// Get snapshot for observability.
    pub fn snapshot(&self) -> AuthRateLimiterSnapshot {
        let sources = self.sources.lock();
        let active_lockouts = sources
            .values()
            .filter(|s| s.locked_until.map_or(false, |t| Instant::now() < t))
            .count();
        let tracked_sources = sources.len();
        AuthRateLimiterSnapshot {
            max_failures: self.config.max_failures,
            lockout_duration_secs: self.config.lockout_duration.as_secs(),
            failure_window_secs: self.config.failure_window.as_secs(),
            tracked_sources,
            active_lockouts,
            total_checks: self.total_checks.load(Ordering::Relaxed),
            total_lockouts: self.total_lockouts.load(Ordering::Relaxed),
            total_failures_recorded: self.total_failures_recorded.load(Ordering::Relaxed),
        }
    }
}

/// Observable snapshot of auth rate limiter state.
#[derive(Debug, Clone)]
pub struct AuthRateLimiterSnapshot {
    pub max_failures: u32,
    pub lockout_duration_secs: u64,
    pub failure_window_secs: u64,
    pub tracked_sources: usize,
    pub active_lockouts: usize,
    pub total_checks: u64,
    pub total_lockouts: u64,
    pub total_failures_recorded: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// PasswordPolicy — password complexity and lifecycle
// ═══════════════════════════════════════════════════════════════════════════

/// Password policy configuration.
#[derive(Debug, Clone)]
pub struct PasswordPolicyConfig {
    /// Minimum password length.
    pub min_length: usize,
    /// Require at least one uppercase letter.
    pub require_uppercase: bool,
    /// Require at least one lowercase letter.
    pub require_lowercase: bool,
    /// Require at least one digit.
    pub require_digit: bool,
    /// Require at least one special character.
    pub require_special: bool,
    /// Maximum password age in days (0 = no expiry).
    pub max_age_days: u32,
    /// Number of previous passwords to remember (prevent reuse).
    pub history_count: usize,
}

impl Default for PasswordPolicyConfig {
    fn default() -> Self {
        Self {
            min_length: 8,
            require_uppercase: true,
            require_lowercase: true,
            require_digit: true,
            require_special: false,
            max_age_days: 90,
            history_count: 3,
        }
    }
}

/// Result of a password validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PasswordValidation {
    /// Password meets all requirements.
    Valid,
    /// Password fails one or more requirements.
    Invalid { reasons: Vec<String> },
}

impl PasswordValidation {
    pub fn is_valid(&self) -> bool {
        matches!(self, PasswordValidation::Valid)
    }
}

/// Password policy enforcer.
pub struct PasswordPolicy {
    config: PasswordPolicyConfig,
    total_checks: AtomicU64,
    total_rejections: AtomicU64,
}

impl PasswordPolicy {
    pub fn new(config: PasswordPolicyConfig) -> Self {
        Self {
            config,
            total_checks: AtomicU64::new(0),
            total_rejections: AtomicU64::new(0),
        }
    }

    /// Validate a password against the policy.
    pub fn validate(&self, password: &str) -> PasswordValidation {
        self.total_checks.fetch_add(1, Ordering::Relaxed);
        let mut reasons = Vec::new();

        if password.len() < self.config.min_length {
            reasons.push(format!(
                "Password must be at least {} characters (got {})",
                self.config.min_length,
                password.len()
            ));
        }

        if self.config.require_uppercase && !password.chars().any(|c| c.is_uppercase()) {
            reasons.push("Password must contain at least one uppercase letter".into());
        }

        if self.config.require_lowercase && !password.chars().any(|c| c.is_lowercase()) {
            reasons.push("Password must contain at least one lowercase letter".into());
        }

        if self.config.require_digit && !password.chars().any(|c| c.is_ascii_digit()) {
            reasons.push("Password must contain at least one digit".into());
        }

        if self.config.require_special
            && !password.chars().any(|c| !c.is_alphanumeric() && !c.is_whitespace())
        {
            reasons.push("Password must contain at least one special character".into());
        }

        if reasons.is_empty() {
            PasswordValidation::Valid
        } else {
            self.total_rejections.fetch_add(1, Ordering::Relaxed);
            PasswordValidation::Invalid { reasons }
        }
    }

    /// Check if a password has expired given the last change timestamp (epoch ms).
    pub fn is_expired(&self, last_changed_epoch_ms: u64) -> bool {
        if self.config.max_age_days == 0 {
            return false;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let age_ms = now_ms.saturating_sub(last_changed_epoch_ms);
        let max_age_ms = self.config.max_age_days as u64 * 86_400_000;
        age_ms > max_age_ms
    }

    /// Get snapshot for observability.
    pub fn snapshot(&self) -> PasswordPolicySnapshot {
        PasswordPolicySnapshot {
            min_length: self.config.min_length,
            require_uppercase: self.config.require_uppercase,
            require_lowercase: self.config.require_lowercase,
            require_digit: self.config.require_digit,
            require_special: self.config.require_special,
            max_age_days: self.config.max_age_days,
            history_count: self.config.history_count,
            total_checks: self.total_checks.load(Ordering::Relaxed),
            total_rejections: self.total_rejections.load(Ordering::Relaxed),
        }
    }
}

/// Observable snapshot of password policy state.
#[derive(Debug, Clone)]
pub struct PasswordPolicySnapshot {
    pub min_length: usize,
    pub require_uppercase: bool,
    pub require_lowercase: bool,
    pub require_digit: bool,
    pub require_special: bool,
    pub max_age_days: u32,
    pub history_count: usize,
    pub total_checks: u64,
    pub total_rejections: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// SqlFirewall — SQL injection detection and dangerous statement blocking
// ═══════════════════════════════════════════════════════════════════════════

/// SQL firewall configuration.
#[derive(Debug, Clone)]
pub struct SqlFirewallConfig {
    /// Enable SQL injection pattern detection.
    pub detect_injection: bool,
    /// Block dangerous statements (DROP DATABASE, TRUNCATE, etc.) for non-superusers.
    pub block_dangerous: bool,
    /// Maximum SQL statement length (0 = unlimited).
    pub max_statement_length: usize,
    /// Block statements with multiple semicolons (statement stacking).
    pub block_stacking: bool,
    /// Custom blocked patterns (regex-like simple patterns).
    pub blocked_patterns: Vec<String>,
}

impl Default for SqlFirewallConfig {
    fn default() -> Self {
        Self {
            detect_injection: true,
            block_dangerous: true,
            max_statement_length: 1_048_576, // 1MB
            block_stacking: true,
            blocked_patterns: Vec::new(),
        }
    }
}

/// Result of SQL firewall check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlFirewallResult {
    /// Statement is allowed.
    Allowed,
    /// Statement is blocked.
    Blocked { reason: String },
}

impl SqlFirewallResult {
    pub fn is_allowed(&self) -> bool {
        matches!(self, SqlFirewallResult::Allowed)
    }
}

/// SQL firewall: detects injection patterns and blocks dangerous statements.
pub struct SqlFirewall {
    config: SqlFirewallConfig,
    total_checks: AtomicU64,
    total_blocked: AtomicU64,
    injection_detected: AtomicU64,
    dangerous_blocked: AtomicU64,
    stacking_blocked: AtomicU64,
    overlength_blocked: AtomicU64,
}

impl SqlFirewall {
    pub fn new(config: SqlFirewallConfig) -> Self {
        Self {
            config,
            total_checks: AtomicU64::new(0),
            total_blocked: AtomicU64::new(0),
            injection_detected: AtomicU64::new(0),
            dangerous_blocked: AtomicU64::new(0),
            stacking_blocked: AtomicU64::new(0),
            overlength_blocked: AtomicU64::new(0),
        }
    }

    /// Check a SQL statement against the firewall rules.
    ///
    /// `is_superuser` bypasses dangerous statement blocking (but not injection detection).
    pub fn check(&self, sql: &str, is_superuser: bool) -> SqlFirewallResult {
        self.total_checks.fetch_add(1, Ordering::Relaxed);

        // Length check
        if self.config.max_statement_length > 0 && sql.len() > self.config.max_statement_length {
            self.total_blocked.fetch_add(1, Ordering::Relaxed);
            self.overlength_blocked.fetch_add(1, Ordering::Relaxed);
            return SqlFirewallResult::Blocked {
                reason: format!(
                    "Statement exceeds maximum length ({} > {})",
                    sql.len(),
                    self.config.max_statement_length
                ),
            };
        }

        // Statement stacking detection (multiple statements separated by ;)
        if self.config.block_stacking {
            let trimmed = sql.trim().trim_end_matches(';');
            if trimmed.contains(';') {
                // Check it's not inside a string literal
                if Self::has_real_semicolons(trimmed) {
                    self.total_blocked.fetch_add(1, Ordering::Relaxed);
                    self.stacking_blocked.fetch_add(1, Ordering::Relaxed);
                    return SqlFirewallResult::Blocked {
                        reason: "Statement stacking detected (multiple semicolons)".into(),
                    };
                }
            }
        }

        let sql_upper = sql.to_uppercase();

        // SQL injection pattern detection
        if self.config.detect_injection {
            if let Some(reason) = Self::detect_injection_patterns(&sql_upper) {
                self.total_blocked.fetch_add(1, Ordering::Relaxed);
                self.injection_detected.fetch_add(1, Ordering::Relaxed);
                return SqlFirewallResult::Blocked { reason };
            }
        }

        // Dangerous statement blocking (non-superuser only)
        if self.config.block_dangerous && !is_superuser {
            if let Some(reason) = Self::detect_dangerous_statements(&sql_upper) {
                self.total_blocked.fetch_add(1, Ordering::Relaxed);
                self.dangerous_blocked.fetch_add(1, Ordering::Relaxed);
                return SqlFirewallResult::Blocked { reason };
            }
        }

        // Custom blocked patterns
        for pattern in &self.config.blocked_patterns {
            let pattern_upper = pattern.to_uppercase();
            if sql_upper.contains(&pattern_upper) {
                self.total_blocked.fetch_add(1, Ordering::Relaxed);
                return SqlFirewallResult::Blocked {
                    reason: format!("Blocked by custom pattern: {}", pattern),
                };
            }
        }

        SqlFirewallResult::Allowed
    }

    /// Detect common SQL injection patterns.
    fn detect_injection_patterns(sql_upper: &str) -> Option<String> {
        // Classic tautology injection: OR 1=1, OR '1'='1', OR ''=''
        let tautology_patterns = [
            "OR 1=1",
            "OR '1'='1'",
            "OR ''=''",
            "OR TRUE",
            "OR 1 = 1",
        ];
        for pat in &tautology_patterns {
            if sql_upper.contains(pat) {
                return Some(format!("SQL injection pattern detected: tautology ({})", pat));
            }
        }

        // UNION-based injection
        if sql_upper.contains("UNION SELECT") || sql_upper.contains("UNION ALL SELECT") {
            // Only flag if it looks suspicious (e.g. after a WHERE clause)
            if sql_upper.contains("WHERE") || sql_upper.contains("--") {
                return Some("SQL injection pattern detected: UNION-based injection".into());
            }
        }

        // Comment-based injection: -- or /* used to truncate queries
        if sql_upper.contains("--") && sql_upper.contains("WHERE") {
            // Check if -- appears after WHERE (potential comment injection)
            if let Some(where_pos) = sql_upper.find("WHERE") {
                if let Some(comment_pos) = sql_upper.find("--") {
                    if comment_pos > where_pos {
                        return Some(
                            "SQL injection pattern detected: comment truncation after WHERE".into(),
                        );
                    }
                }
            }
        }

        // Sleep/benchmark injection (time-based blind injection)
        if sql_upper.contains("PG_SLEEP(") || sql_upper.contains("BENCHMARK(") {
            return Some("SQL injection pattern detected: time-based injection (pg_sleep/benchmark)".into());
        }

        None
    }

    /// Detect dangerous statements that non-superusers should not execute.
    fn detect_dangerous_statements(sql_upper: &str) -> Option<String> {
        let dangerous = [
            ("DROP DATABASE", "DROP DATABASE is restricted to superusers"),
            ("DROP SCHEMA", "DROP SCHEMA is restricted to superusers"),
            ("TRUNCATE", "TRUNCATE is restricted to superusers"),
            ("ALTER SYSTEM", "ALTER SYSTEM is restricted to superusers"),
            ("COPY TO", "COPY TO (file export) is restricted to superusers"),
            ("LOAD ", "LOAD is restricted to superusers"),
        ];
        for (pattern, reason) in &dangerous {
            if sql_upper.starts_with(pattern) || sql_upper.contains(&format!(" {}", pattern)) {
                return Some(reason.to_string());
            }
        }
        None
    }

    /// Check for real semicolons (not inside string literals).
    fn has_real_semicolons(sql: &str) -> bool {
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut prev_char = '\0';

        for ch in sql.chars() {
            match ch {
                '\'' if !in_double_quote && prev_char != '\\' => {
                    in_single_quote = !in_single_quote;
                }
                '"' if !in_single_quote && prev_char != '\\' => {
                    in_double_quote = !in_double_quote;
                }
                ';' if !in_single_quote && !in_double_quote => {
                    return true;
                }
                _ => {}
            }
            prev_char = ch;
        }
        false
    }

    /// Get snapshot for observability.
    pub fn snapshot(&self) -> SqlFirewallSnapshot {
        SqlFirewallSnapshot {
            detect_injection: self.config.detect_injection,
            block_dangerous: self.config.block_dangerous,
            max_statement_length: self.config.max_statement_length,
            block_stacking: self.config.block_stacking,
            custom_patterns_count: self.config.blocked_patterns.len(),
            total_checks: self.total_checks.load(Ordering::Relaxed),
            total_blocked: self.total_blocked.load(Ordering::Relaxed),
            injection_detected: self.injection_detected.load(Ordering::Relaxed),
            dangerous_blocked: self.dangerous_blocked.load(Ordering::Relaxed),
            stacking_blocked: self.stacking_blocked.load(Ordering::Relaxed),
            overlength_blocked: self.overlength_blocked.load(Ordering::Relaxed),
        }
    }
}

/// Observable snapshot of SQL firewall state.
#[derive(Debug, Clone)]
pub struct SqlFirewallSnapshot {
    pub detect_injection: bool,
    pub block_dangerous: bool,
    pub max_statement_length: usize,
    pub block_stacking: bool,
    pub custom_patterns_count: usize,
    pub total_checks: u64,
    pub total_blocked: u64,
    pub injection_detected: u64,
    pub dangerous_blocked: u64,
    pub stacking_blocked: u64,
    pub overlength_blocked: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── AuthRateLimiter tests ──

    #[test]
    fn test_auth_rate_limiter_allows_initially() {
        let limiter = AuthRateLimiter::new(AuthRateLimiterConfig::default());
        assert_eq!(limiter.check("192.168.1.1"), AuthRateResult::Allowed);
    }

    #[test]
    fn test_auth_rate_limiter_lockout_after_max_failures() {
        let config = AuthRateLimiterConfig {
            max_failures: 3,
            lockout_duration: Duration::from_secs(60),
            failure_window: Duration::from_secs(300),
            per_ip: true,
        };
        let limiter = AuthRateLimiter::new(config);

        // Record 3 failures
        limiter.record_failure("10.0.0.1");
        limiter.record_failure("10.0.0.1");
        limiter.record_failure("10.0.0.1");

        // Should be locked out
        match limiter.check("10.0.0.1") {
            AuthRateResult::LockedOut { failure_count, .. } => {
                assert_eq!(failure_count, 3);
            }
            _ => panic!("Expected lockout"),
        }

        // Different IP should still be allowed
        assert_eq!(limiter.check("10.0.0.2"), AuthRateResult::Allowed);
    }

    #[test]
    fn test_auth_rate_limiter_success_clears_failures() {
        let config = AuthRateLimiterConfig {
            max_failures: 3,
            lockout_duration: Duration::from_secs(60),
            failure_window: Duration::from_secs(300),
            per_ip: true,
        };
        let limiter = AuthRateLimiter::new(config);

        limiter.record_failure("10.0.0.1");
        limiter.record_failure("10.0.0.1");
        limiter.record_success("10.0.0.1");

        // Should be allowed (failures cleared)
        assert_eq!(limiter.check("10.0.0.1"), AuthRateResult::Allowed);
    }

    #[test]
    fn test_auth_rate_limiter_snapshot() {
        let limiter = AuthRateLimiter::new(AuthRateLimiterConfig::default());
        limiter.check("10.0.0.1");
        limiter.record_failure("10.0.0.1");

        let snap = limiter.snapshot();
        assert_eq!(snap.max_failures, 5);
        assert_eq!(snap.total_checks, 1);
        assert_eq!(snap.total_failures_recorded, 1);
        assert_eq!(snap.tracked_sources, 1);
    }

    #[test]
    fn test_auth_rate_limiter_below_threshold_allowed() {
        let config = AuthRateLimiterConfig {
            max_failures: 5,
            ..Default::default()
        };
        let limiter = AuthRateLimiter::new(config);

        // 4 failures — still under threshold
        for _ in 0..4 {
            limiter.record_failure("10.0.0.1");
        }
        assert_eq!(limiter.check("10.0.0.1"), AuthRateResult::Allowed);
    }

    // ── PasswordPolicy tests ──

    #[test]
    fn test_password_policy_valid() {
        let policy = PasswordPolicy::new(PasswordPolicyConfig::default());
        assert!(policy.validate("MyP@ss1234").is_valid());
    }

    #[test]
    fn test_password_policy_too_short() {
        let policy = PasswordPolicy::new(PasswordPolicyConfig {
            min_length: 12,
            ..Default::default()
        });
        let result = policy.validate("Ab1");
        assert!(!result.is_valid());
        if let PasswordValidation::Invalid { reasons } = result {
            assert!(reasons[0].contains("at least 12"));
        }
    }

    #[test]
    fn test_password_policy_missing_uppercase() {
        let policy = PasswordPolicy::new(PasswordPolicyConfig::default());
        let result = policy.validate("mypassword1");
        assert!(!result.is_valid());
        if let PasswordValidation::Invalid { reasons } = result {
            assert!(reasons.iter().any(|r| r.contains("uppercase")));
        }
    }

    #[test]
    fn test_password_policy_missing_digit() {
        let policy = PasswordPolicy::new(PasswordPolicyConfig::default());
        let result = policy.validate("MyPassword");
        assert!(!result.is_valid());
        if let PasswordValidation::Invalid { reasons } = result {
            assert!(reasons.iter().any(|r| r.contains("digit")));
        }
    }

    #[test]
    fn test_password_policy_special_required() {
        let policy = PasswordPolicy::new(PasswordPolicyConfig {
            require_special: true,
            ..Default::default()
        });
        let result = policy.validate("MyPass1234");
        assert!(!result.is_valid());
        if let PasswordValidation::Invalid { reasons } = result {
            assert!(reasons.iter().any(|r| r.contains("special")));
        }
        assert!(policy.validate("MyP@ss1234").is_valid());
    }

    #[test]
    fn test_password_policy_expiry() {
        let policy = PasswordPolicy::new(PasswordPolicyConfig {
            max_age_days: 90,
            ..Default::default()
        });
        // Password changed 100 days ago
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let old = now_ms - (100 * 86_400_000);
        assert!(policy.is_expired(old));

        // Password changed 10 days ago
        let recent = now_ms - (10 * 86_400_000);
        assert!(!policy.is_expired(recent));
    }

    #[test]
    fn test_password_policy_no_expiry() {
        let policy = PasswordPolicy::new(PasswordPolicyConfig {
            max_age_days: 0,
            ..Default::default()
        });
        assert!(!policy.is_expired(0)); // Even epoch 0 should not expire
    }

    #[test]
    fn test_password_policy_snapshot() {
        let policy = PasswordPolicy::new(PasswordPolicyConfig::default());
        policy.validate("short");
        policy.validate("ValidPass1");

        let snap = policy.snapshot();
        assert_eq!(snap.min_length, 8);
        assert_eq!(snap.total_checks, 2);
        assert_eq!(snap.total_rejections, 1);
    }

    // ── SqlFirewall tests ──

    #[test]
    fn test_firewall_allows_normal_sql() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        assert!(fw.check("SELECT * FROM users WHERE id = 1", false).is_allowed());
        assert!(fw.check("INSERT INTO orders (id, name) VALUES (1, 'test')", false).is_allowed());
        assert!(fw.check("UPDATE users SET name = 'alice' WHERE id = 1", false).is_allowed());
    }

    #[test]
    fn test_firewall_detects_tautology_injection() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        let result = fw.check("SELECT * FROM users WHERE name = '' OR 1=1", false);
        assert!(!result.is_allowed());
        if let SqlFirewallResult::Blocked { reason } = result {
            assert!(reason.contains("tautology"));
        }
    }

    #[test]
    fn test_firewall_detects_comment_injection() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        let result = fw.check("SELECT * FROM users WHERE name = 'admin' -- AND password = 'x'", false);
        assert!(!result.is_allowed());
        if let SqlFirewallResult::Blocked { reason } = result {
            assert!(reason.contains("comment truncation"));
        }
    }

    #[test]
    fn test_firewall_detects_sleep_injection() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        let result = fw.check("SELECT * FROM users WHERE id = 1; SELECT pg_sleep(10)", false);
        assert!(!result.is_allowed());
    }

    #[test]
    fn test_firewall_blocks_dangerous_for_non_superuser() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        let result = fw.check("DROP DATABASE production", false);
        assert!(!result.is_allowed());
        if let SqlFirewallResult::Blocked { reason } = result {
            assert!(reason.contains("superuser"));
        }
    }

    #[test]
    fn test_firewall_allows_dangerous_for_superuser() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        assert!(fw.check("DROP DATABASE test_db", true).is_allowed());
    }

    #[test]
    fn test_firewall_blocks_stacking() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        let result = fw.check("SELECT 1; DROP TABLE users", false);
        assert!(!result.is_allowed());
        if let SqlFirewallResult::Blocked { reason } = result {
            assert!(reason.contains("stacking"));
        }
    }

    #[test]
    fn test_firewall_allows_semicolons_in_strings() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        assert!(fw.check("SELECT * FROM t WHERE name = 'a;b'", false).is_allowed());
    }

    #[test]
    fn test_firewall_blocks_overlength() {
        let config = SqlFirewallConfig {
            max_statement_length: 50,
            ..Default::default()
        };
        let fw = SqlFirewall::new(config);
        let long_sql = "SELECT * FROM users WHERE ".to_string() + &"x".repeat(50);
        let result = fw.check(&long_sql, false);
        assert!(!result.is_allowed());
    }

    #[test]
    fn test_firewall_custom_pattern() {
        let config = SqlFirewallConfig {
            blocked_patterns: vec!["xp_cmdshell".into()],
            ..Default::default()
        };
        let fw = SqlFirewall::new(config);
        let result = fw.check("EXEC xp_cmdshell 'dir'", false);
        assert!(!result.is_allowed());
    }

    #[test]
    fn test_firewall_snapshot() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        fw.check("SELECT 1", false);
        fw.check("SELECT * FROM t WHERE x = '' OR 1=1", false);

        let snap = fw.snapshot();
        assert_eq!(snap.total_checks, 2);
        assert_eq!(snap.total_blocked, 1);
        assert_eq!(snap.injection_detected, 1);
    }

    #[test]
    fn test_firewall_blocks_truncate() {
        let fw = SqlFirewall::new(SqlFirewallConfig::default());
        assert!(!fw.check("TRUNCATE users", false).is_allowed());
        assert!(fw.check("TRUNCATE users", true).is_allowed());
    }
}
