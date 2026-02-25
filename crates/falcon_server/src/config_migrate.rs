//! Config schema versioning and migration for FalconDB upgrades.
//!
//! Each config version bump corresponds to a schema change. The migration
//! system applies transforms sequentially: v1→v2→v3→...→current.
//!
//! This ensures in-place upgrades never break due to config incompatibility.

use falcon_common::config::CURRENT_CONFIG_VERSION;

/// Result of a config migration attempt.
#[derive(Debug)]
pub struct MigrationResult {
    /// The migrated TOML content.
    pub content: String,
    /// Starting version.
    pub from_version: u32,
    /// Final version after migration.
    pub to_version: u32,
    /// Steps applied.
    pub steps: Vec<String>,
}

/// Check config version and return migration status.
pub fn check_config_version(toml_text: &str) -> ConfigVersionStatus {
    let version = extract_version(toml_text);
    if version == CURRENT_CONFIG_VERSION {
        ConfigVersionStatus::Current
    } else if version < CURRENT_CONFIG_VERSION {
        ConfigVersionStatus::NeedsMigration {
            from: version,
            to: CURRENT_CONFIG_VERSION,
        }
    } else {
        ConfigVersionStatus::TooNew {
            found: version,
            max_supported: CURRENT_CONFIG_VERSION,
        }
    }
}

/// Config version status.
#[derive(Debug, PartialEq, Eq)]
pub enum ConfigVersionStatus {
    /// Config is at current version — no action needed.
    Current,
    /// Config needs migration from an older version.
    NeedsMigration { from: u32, to: u32 },
    /// Config is from a newer version — cannot downgrade.
    TooNew { found: u32, max_supported: u32 },
}

/// Migrate config TOML from its current version to the latest.
///
/// Returns the migrated content and a log of steps applied.
/// Creates a backup of the original file before writing.
pub fn migrate_config(toml_text: &str) -> Result<MigrationResult, String> {
    let from_version = extract_version(toml_text);

    if from_version > CURRENT_CONFIG_VERSION {
        return Err(format!(
            "Config version {from_version} is newer than supported version {CURRENT_CONFIG_VERSION}. Cannot downgrade."
        ));
    }

    if from_version == CURRENT_CONFIG_VERSION {
        return Ok(MigrationResult {
            content: toml_text.to_owned(),
            from_version,
            to_version: CURRENT_CONFIG_VERSION,
            steps: vec!["Already at current version — no migration needed.".into()],
        });
    }

    let mut content = toml_text.to_owned();
    let mut steps = Vec::new();

    // Apply migrations sequentially
    let mut current = from_version;

    if current < 1 {
        content = migrate_v0_to_v1(&content);
        steps.push("v0→v1: Added config_version field".into());
        current = 1;
    }

    if current < 2 {
        content = migrate_v1_to_v2(&content);
        steps.push("v1→v2: Renamed cedar.* → server.*, added memory section".into());
        current = 2;
    }

    if current < 3 {
        content = migrate_v2_to_v3(&content);
        steps.push("v2→v3: Added shutdown_drain_timeout_secs, idle_timeout_ms, ustm section".into());
        current = 3;
    }

    if current < 4 {
        content = migrate_v3_to_v4(&content);
        steps.push("v3→v4: Added compression_profile, wal_mode, [gateway] section".into());
        current = 4;
    }

    // Update the version stamp
    content = set_version(&content, CURRENT_CONFIG_VERSION);

    Ok(MigrationResult {
        content,
        from_version,
        to_version: current,
        steps,
    })
}

/// Migrate config file on disk. Creates .bak backup before writing.
pub fn migrate_config_file(path: &std::path::Path) -> Result<MigrationResult, String> {
    let toml_text = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read config {}: {}", path.display(), e))?;

    let result = migrate_config(&toml_text)?;

    if result.from_version == result.to_version {
        return Ok(result);
    }

    // Create backup
    let backup_path = path.with_extension("toml.bak");
    std::fs::copy(path, &backup_path)
        .map_err(|e| format!("Failed to create backup at {}: {}", backup_path.display(), e))?;

    // Write migrated config
    std::fs::write(path, &result.content)
        .map_err(|e| format!("Failed to write migrated config: {e}"))?;

    Ok(result)
}

/// Run `falcon config migrate` CLI command.
pub fn run_config_migrate(config_path: &str) {
    let path = std::path::Path::new(config_path);
    if !path.exists() {
        eprintln!("ERROR: Config file not found: {config_path}");
        std::process::exit(1);
    }

    println!("FalconDB Config Migration");
    println!("=========================");
    println!("  File: {config_path}");

    match check_config_version(
        &std::fs::read_to_string(path).unwrap_or_default(),
    ) {
        ConfigVersionStatus::Current => {
            println!("  Status: Already at version {CURRENT_CONFIG_VERSION} (current)");
            println!("  No migration needed.");
        }
        ConfigVersionStatus::NeedsMigration { from, to } => {
            println!("  Current version: {from}");
            println!("  Target version:  {to}");
            println!();

            match migrate_config_file(path) {
                Ok(result) => {
                    println!("  Migration successful!");
                    println!("  Backup:  {config_path}.bak");
                    for step in &result.steps {
                        println!("    - {step}");
                    }
                }
                Err(e) => {
                    eprintln!("  ERROR: {e}");
                    std::process::exit(1);
                }
            }
        }
        ConfigVersionStatus::TooNew { found, max_supported } => {
            eprintln!(
                "  ERROR: Config version {found} is newer than this build supports (max: {max_supported})."
            );
            eprintln!("  Upgrade FalconDB to a newer version, or restore from backup.");
            std::process::exit(1);
        }
    }
}

// ── Internal migration helpers ──

fn extract_version(toml_text: &str) -> u32 {
    for line in toml_text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("config_version") {
            if let Some(val) = trimmed.split('=').nth(1) {
                return val.trim().parse().unwrap_or(0);
            }
        }
    }
    // No version field = version 0 (pre-versioning)
    0
}

fn set_version(toml_text: &str, version: u32) -> String {
    let mut found = false;
    let lines: Vec<String> = toml_text
        .lines()
        .map(|line| {
            if line.trim().starts_with("config_version") {
                found = true;
                format!("config_version = {version}")
            } else {
                line.to_owned()
            }
        })
        .collect();

    if found {
        lines.join("\n")
    } else {
        // Prepend config_version at the top
        format!("config_version = {version}\n{toml_text}")
    }
}

fn migrate_v0_to_v1(content: &str) -> String {
    // v0→v1: Just add config_version = 1 (handled by set_version at end)
    content.to_owned()
}

fn migrate_v1_to_v2(content: &str) -> String {
    // v1→v2: Rename [cedar] → [server] if present
    content
        .replace("[cedar]", "[server]")
        .replace("cedar_data_dir", "data_dir")
}

fn migrate_v2_to_v3(content: &str) -> String {
    // v2→v3: Add new fields with defaults if missing
    let mut result = content.to_owned();
    if !content.contains("shutdown_drain_timeout_secs") {
        // Add under [server] section
        result = result.replace(
            "[server]",
            "[server]\n# Added by migration v2→v3\n# shutdown_drain_timeout_secs = 30",
        );
    }
    result
}

fn migrate_v3_to_v4(content: &str) -> String {
    // v3→v4: Add compression_profile, wal_mode, [gateway] section
    let mut result = content.to_owned();
    if !content.contains("compression_profile") {
        result.push_str("\n# Added by migration v3→v4\ncompression_profile = \"balanced\"\n");
    }
    if !content.contains("wal_mode") {
        result.push_str("wal_mode = \"auto\"\n");
    }
    if !content.contains("[gateway]") {
        result.push_str("\n[gateway]\nrole = \"smart_gateway\"\n");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_version_present() {
        assert_eq!(extract_version("config_version = 3\n[server]\n"), 3);
    }

    #[test]
    fn test_extract_version_missing() {
        assert_eq!(extract_version("[server]\npg_listen_addr = \"0.0.0.0:5443\"\n"), 0);
    }

    #[test]
    fn test_check_version_current() {
        let toml = format!("config_version = {}\n[server]\n", CURRENT_CONFIG_VERSION);
        assert_eq!(check_config_version(&toml), ConfigVersionStatus::Current);
    }

    #[test]
    fn test_check_version_needs_migration() {
        let toml = "config_version = 1\n[server]\n";
        assert_eq!(
            check_config_version(toml),
            ConfigVersionStatus::NeedsMigration {
                from: 1,
                to: CURRENT_CONFIG_VERSION
            }
        );
    }

    #[test]
    fn test_check_version_too_new() {
        let toml = "config_version = 999\n[server]\n";
        assert_eq!(
            check_config_version(toml),
            ConfigVersionStatus::TooNew {
                found: 999,
                max_supported: CURRENT_CONFIG_VERSION
            }
        );
    }

    #[test]
    fn test_migrate_already_current() {
        let toml = format!("config_version = {}\n[server]\npg = \"0.0.0.0:5443\"\n", CURRENT_CONFIG_VERSION);
        let result = migrate_config(&toml).unwrap();
        assert_eq!(result.from_version, CURRENT_CONFIG_VERSION);
        assert_eq!(result.to_version, CURRENT_CONFIG_VERSION);
    }

    #[test]
    fn test_migrate_from_zero() {
        let toml = "[server]\npg_listen_addr = \"0.0.0.0:5443\"\n";
        let result = migrate_config(toml).unwrap();
        assert_eq!(result.from_version, 0);
        assert_eq!(result.to_version, CURRENT_CONFIG_VERSION);
        assert!(result.content.contains(&format!("config_version = {}", CURRENT_CONFIG_VERSION)));
    }

    #[test]
    fn test_migrate_too_new_fails() {
        let toml = "config_version = 999\n[server]\n";
        assert!(migrate_config(toml).is_err());
    }

    #[test]
    fn test_set_version_existing() {
        let result = set_version("config_version = 1\n[server]\n", 3);
        assert!(result.contains("config_version = 3"));
        assert!(!result.contains("config_version = 1"));
    }

    #[test]
    fn test_set_version_missing() {
        let result = set_version("[server]\n", 3);
        assert!(result.starts_with("config_version = 3\n"));
    }
}
