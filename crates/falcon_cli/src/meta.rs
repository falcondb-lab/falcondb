use crate::client::DbClient;
use anyhow::Result;

/// Result of parsing a meta-command line.
#[derive(Debug, PartialEq)]
pub enum MetaCommand {
    Quit,
    Help,
    ConnInfo,
    ListTables,
    DescribeTable(String),
    ListDatabases,
    Connect(String),
    ToggleExpanded,
    ToggleTiming,
    Export(String),
    Import(String),
    Cluster(String),
    Txn(String),
    Consistency(String),
    Node(String),
    Failover(String),
    Shard(String),
    Audit(String),
    Policy(String),
    Automation(String),
    Integration(String),
    Events(String),
    Unknown(String),
}

/// Try to parse a line as a meta-command. Returns None if it's not a meta-command.
pub fn parse_meta(line: &str) -> Option<MetaCommand> {
    let trimmed = line.trim();
    if !trimmed.starts_with('\\') {
        return None;
    }
    let rest = trimmed[1..].trim();
    let (cmd, arg) = rest
        .split_once(char::is_whitespace)
        .map(|(c, a)| (c, a.trim()))
        .unwrap_or((rest, ""));

    Some(match cmd {
        "q" | "quit" => MetaCommand::Quit,
        "?" | "help" => MetaCommand::Help,
        "conninfo" => MetaCommand::ConnInfo,
        "dt" => MetaCommand::ListTables,
        "d" if !arg.is_empty() => MetaCommand::DescribeTable(arg.to_string()),
        "d" => MetaCommand::ListTables,
        "l" => MetaCommand::ListDatabases,
        "c" if !arg.is_empty() => MetaCommand::Connect(arg.to_string()),
        "x" => MetaCommand::ToggleExpanded,
        "timing" => MetaCommand::ToggleTiming,
        "export" => MetaCommand::Export(trimmed.to_string()),
        "import" => MetaCommand::Import(trimmed.to_string()),
        "cluster" => MetaCommand::Cluster(arg.to_string()),
        "txn" => MetaCommand::Txn(arg.to_string()),
        "consistency" => MetaCommand::Consistency(arg.to_string()),
        "node" => MetaCommand::Node(arg.to_string()),
        "failover" => MetaCommand::Failover(arg.to_string()),
        "shard" => MetaCommand::Shard(arg.to_string()),
        "audit" => MetaCommand::Audit(arg.to_string()),
        "policy" => MetaCommand::Policy(arg.to_string()),
        "automation" => MetaCommand::Automation(arg.to_string()),
        "integration" => MetaCommand::Integration(arg.to_string()),
        "events" => MetaCommand::Events(arg.to_string()),
        other => MetaCommand::Unknown(format!("\\{}", other)),
    })
}

/// Execute a meta-command. Returns true if the CLI should quit.
pub async fn execute_meta(cmd: &MetaCommand, client: &DbClient) -> Result<MetaResult> {
    match cmd {
        MetaCommand::Quit => Ok(MetaResult::Quit),

        MetaCommand::Help => {
            let help = "\
General
  \\q                    quit fsql
  \\?                    show this help (fsql v0.3)

Connection
  \\conninfo             display connection information
  \\c DBNAME             reconnect to a different database

Informational
  \\l                    list databases
  \\dt                   list tables
  \\d TABLE              describe table schema

Formatting
  \\x                    toggle expanded output mode
  \\timing               toggle execution timing

Import / Export
  \\export <query> TO <file> [OPTIONS]   export query results to CSV
  \\import <file> INTO <table> [OPTIONS] import CSV into table

Operational (v0.4 — read-only)
  \\cluster [nodes|leaders|shards]       cluster topology
  \\txn [active|prepared|<id>]           transaction visibility
  \\consistency [status|in-doubt]        consistency & WAL state
  \\node [stats|memory|wal]              node health & resources
  \\failover [status|history|simulate <node>]  failover events / simulation

Policy & Automation (v0.6)
  \\policy [list|show|create|enable|disable|delete|simulate|audit]
  \\automation [status|events|pause|resume]

Integration & Ecosystem (v0.7)
  \\integration [list|add webhook <url>|remove|disable|enable]
  \\events [list|tail|replay <event_id>]

Management (v0.5 — plan/apply, requires --apply to execute)
  \\node drain <node_id>                 drain node (stop new txn routing)
  \\node resume <node_id>               resume node routing
  \\shard move <shard_id> to <node_id>  move shard leader
  \\cluster mode readonly|readwrite     set cluster access mode
  \\txn resolve <txn_id> [commit|abort] resolve in-doubt transaction
  \\failover simulate <node_id>         dry-run failover simulation
  \\audit [recent|<event_id>]           view audit log
";
            Ok(MetaResult::Output(help.to_string()))
        }

        MetaCommand::ConnInfo => Ok(MetaResult::Output(client.conninfo())),

        MetaCommand::ListTables => {
            let sql = "SELECT table_schema, table_name, table_type \
                       FROM information_schema.tables \
                       WHERE table_schema NOT IN ('pg_catalog','information_schema') \
                       ORDER BY table_schema, table_name";
            let (rows, _tag) = client.query_simple(sql).await?;
            if rows.is_empty() {
                return Ok(MetaResult::Output("No relations found.".to_string()));
            }
            let mut out = format!("{:<20} {:<30} {}\n", "Schema", "Name", "Type");
            out.push_str(&"-".repeat(60));
            out.push('\n');
            for row in &rows {
                let schema = row.get(0).unwrap_or("");
                let name = row.get(1).unwrap_or("");
                let typ = row.get(2).unwrap_or("");
                out.push_str(&format!("{:<20} {:<30} {}\n", schema, name, typ));
            }
            Ok(MetaResult::Output(out))
        }

        MetaCommand::DescribeTable(table) => {
            let sql = format!(
                "SELECT column_name, data_type, is_nullable, column_default \
                 FROM information_schema.columns \
                 WHERE table_name = '{}' \
                 ORDER BY ordinal_position",
                table.replace('\'', "''")
            );
            let (rows, _tag) = client.query_simple(&sql).await?;
            if rows.is_empty() {
                return Ok(MetaResult::Output(format!(
                    "Did not find any relation named \"{}\".",
                    table
                )));
            }
            let mut out = format!("Table \"{}\":\n", table);
            out.push_str(&format!(
                "  {:<25} {:<20} {:<10} {}\n",
                "Column", "Type", "Nullable", "Default"
            ));
            out.push_str(&format!("  {}\n", "-".repeat(70)));
            for row in &rows {
                let col = row.get(0).unwrap_or("");
                let typ = row.get(1).unwrap_or("");
                let nullable = row.get(2).unwrap_or("");
                let default = row.get(3).unwrap_or("");
                out.push_str(&format!(
                    "  {:<25} {:<20} {:<10} {}\n",
                    col, typ, nullable, default
                ));
            }
            Ok(MetaResult::Output(out))
        }

        MetaCommand::ListDatabases => {
            let sql =
                "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname";
            let (rows, _tag) = client.query_simple(sql).await?;
            if rows.is_empty() {
                return Ok(MetaResult::Output("No databases found.".to_string()));
            }
            let mut out = "List of databases:\n".to_string();
            for row in &rows {
                out.push_str(&format!("  {}\n", row.get(0).unwrap_or("")));
            }
            Ok(MetaResult::Output(out))
        }

        MetaCommand::Connect(dbname) => Ok(MetaResult::Reconnect(dbname.clone())),

        MetaCommand::ToggleExpanded => Ok(MetaResult::ToggleExpanded),

        MetaCommand::ToggleTiming => Ok(MetaResult::ToggleTiming),

        MetaCommand::Export(line) => Ok(MetaResult::Export(line.clone())),

        MetaCommand::Import(line) => Ok(MetaResult::Import(line.clone())),

        MetaCommand::Cluster(arg) => Ok(MetaResult::Cluster(arg.clone())),

        MetaCommand::Txn(arg) => Ok(MetaResult::Txn(arg.clone())),

        MetaCommand::Consistency(arg) => Ok(MetaResult::Consistency(arg.clone())),

        MetaCommand::Node(arg) => Ok(MetaResult::Node(arg.clone())),

        MetaCommand::Failover(arg) => Ok(MetaResult::Failover(arg.clone())),

        MetaCommand::Shard(arg) => Ok(MetaResult::Shard(arg.clone())),

        MetaCommand::Audit(arg) => Ok(MetaResult::Audit(arg.clone())),

        MetaCommand::Policy(arg) => Ok(MetaResult::Policy(arg.clone())),

        MetaCommand::Automation(arg) => Ok(MetaResult::Automation(arg.clone())),

        MetaCommand::Integration(arg) => Ok(MetaResult::Integration(arg.clone())),

        MetaCommand::Events(arg) => Ok(MetaResult::Events(arg.clone())),

        MetaCommand::Unknown(cmd) => Ok(MetaResult::Output(format!(
            "Invalid command \"{}\". Try \\? for help.",
            cmd
        ))),
    }
}

#[derive(Debug)]
pub enum MetaResult {
    Quit,
    Output(String),
    Reconnect(String),
    ToggleExpanded,
    ToggleTiming,
    Export(String),
    Import(String),
    Cluster(String),
    Txn(String),
    Consistency(String),
    Node(String),
    Failover(String),
    Shard(String),
    Audit(String),
    Policy(String),
    Automation(String),
    Integration(String),
    Events(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quit() {
        assert_eq!(parse_meta("\\q"), Some(MetaCommand::Quit));
        assert_eq!(parse_meta("  \\q  "), Some(MetaCommand::Quit));
    }

    #[test]
    fn test_parse_help() {
        assert_eq!(parse_meta("\\?"), Some(MetaCommand::Help));
    }

    #[test]
    fn test_parse_conninfo() {
        assert_eq!(parse_meta("\\conninfo"), Some(MetaCommand::ConnInfo));
    }

    #[test]
    fn test_parse_list_tables() {
        assert_eq!(parse_meta("\\dt"), Some(MetaCommand::ListTables));
    }

    #[test]
    fn test_parse_describe_table() {
        assert_eq!(
            parse_meta("\\d users"),
            Some(MetaCommand::DescribeTable("users".to_string()))
        );
    }

    #[test]
    fn test_parse_list_databases() {
        assert_eq!(parse_meta("\\l"), Some(MetaCommand::ListDatabases));
    }

    #[test]
    fn test_parse_connect() {
        assert_eq!(
            parse_meta("\\c mydb"),
            Some(MetaCommand::Connect("mydb".to_string()))
        );
    }

    #[test]
    fn test_parse_toggle_expanded() {
        assert_eq!(parse_meta("\\x"), Some(MetaCommand::ToggleExpanded));
    }

    #[test]
    fn test_parse_toggle_timing() {
        assert_eq!(parse_meta("\\timing"), Some(MetaCommand::ToggleTiming));
    }

    #[test]
    fn test_parse_export() {
        assert_eq!(
            parse_meta("\\export SELECT 1 TO /tmp/out.csv"),
            Some(MetaCommand::Export(
                "\\export SELECT 1 TO /tmp/out.csv".to_string()
            ))
        );
    }

    #[test]
    fn test_parse_import() {
        assert_eq!(
            parse_meta("\\import /tmp/data.csv INTO mytable"),
            Some(MetaCommand::Import(
                "\\import /tmp/data.csv INTO mytable".to_string()
            ))
        );
    }

    #[test]
    fn test_parse_integration() {
        assert_eq!(
            parse_meta("\\integration"),
            Some(MetaCommand::Integration("".to_string()))
        );
        assert_eq!(
            parse_meta("\\integration list"),
            Some(MetaCommand::Integration("list".to_string()))
        );
        assert_eq!(
            parse_meta("\\integration add webhook https://example.com/hook"),
            Some(MetaCommand::Integration(
                "add webhook https://example.com/hook".to_string()
            ))
        );
        assert_eq!(
            parse_meta("\\integration disable int-1"),
            Some(MetaCommand::Integration("disable int-1".to_string()))
        );
    }

    #[test]
    fn test_parse_events() {
        assert_eq!(
            parse_meta("\\events"),
            Some(MetaCommand::Events("".to_string()))
        );
        assert_eq!(
            parse_meta("\\events list"),
            Some(MetaCommand::Events("list".to_string()))
        );
        assert_eq!(
            parse_meta("\\events tail"),
            Some(MetaCommand::Events("tail".to_string()))
        );
        assert_eq!(
            parse_meta("\\events replay evt-42"),
            Some(MetaCommand::Events("replay evt-42".to_string()))
        );
    }

    #[test]
    fn test_parse_policy() {
        assert_eq!(
            parse_meta("\\policy"),
            Some(MetaCommand::Policy("".to_string()))
        );
        assert_eq!(
            parse_meta("\\policy list"),
            Some(MetaCommand::Policy("list".to_string()))
        );
        assert_eq!(
            parse_meta("\\policy simulate pol1"),
            Some(MetaCommand::Policy("simulate pol1".to_string()))
        );
        assert_eq!(
            parse_meta(
                "\\policy create id=pol1 condition=cluster_readonly action=require_human_approval"
            ),
            Some(MetaCommand::Policy(
                "create id=pol1 condition=cluster_readonly action=require_human_approval"
                    .to_string()
            ))
        );
    }

    #[test]
    fn test_parse_automation() {
        assert_eq!(
            parse_meta("\\automation"),
            Some(MetaCommand::Automation("".to_string()))
        );
        assert_eq!(
            parse_meta("\\automation status"),
            Some(MetaCommand::Automation("status".to_string()))
        );
        assert_eq!(
            parse_meta("\\automation events"),
            Some(MetaCommand::Automation("events".to_string()))
        );
        assert_eq!(
            parse_meta("\\automation pause"),
            Some(MetaCommand::Automation("pause".to_string()))
        );
    }

    #[test]
    fn test_parse_shard() {
        assert_eq!(
            parse_meta("\\shard move 1 to node2"),
            Some(MetaCommand::Shard("move 1 to node2".to_string()))
        );
        assert_eq!(
            parse_meta("\\shard"),
            Some(MetaCommand::Shard("".to_string()))
        );
    }

    #[test]
    fn test_parse_audit() {
        assert_eq!(
            parse_meta("\\audit"),
            Some(MetaCommand::Audit("".to_string()))
        );
        assert_eq!(
            parse_meta("\\audit recent"),
            Some(MetaCommand::Audit("recent".to_string()))
        );
        assert_eq!(
            parse_meta("\\audit evt-42"),
            Some(MetaCommand::Audit("evt-42".to_string()))
        );
    }

    #[test]
    fn test_parse_cluster() {
        assert_eq!(
            parse_meta("\\cluster"),
            Some(MetaCommand::Cluster("".to_string()))
        );
        assert_eq!(
            parse_meta("\\cluster nodes"),
            Some(MetaCommand::Cluster("nodes".to_string()))
        );
        assert_eq!(
            parse_meta("\\cluster shards"),
            Some(MetaCommand::Cluster("shards".to_string()))
        );
    }

    #[test]
    fn test_parse_txn() {
        assert_eq!(parse_meta("\\txn"), Some(MetaCommand::Txn("".to_string())));
        assert_eq!(
            parse_meta("\\txn active"),
            Some(MetaCommand::Txn("active".to_string()))
        );
        assert_eq!(
            parse_meta("\\txn prepared"),
            Some(MetaCommand::Txn("prepared".to_string()))
        );
    }

    #[test]
    fn test_parse_consistency() {
        assert_eq!(
            parse_meta("\\consistency"),
            Some(MetaCommand::Consistency("".to_string()))
        );
        assert_eq!(
            parse_meta("\\consistency status"),
            Some(MetaCommand::Consistency("status".to_string()))
        );
        assert_eq!(
            parse_meta("\\consistency in-doubt"),
            Some(MetaCommand::Consistency("in-doubt".to_string()))
        );
    }

    #[test]
    fn test_parse_node() {
        assert_eq!(
            parse_meta("\\node"),
            Some(MetaCommand::Node("".to_string()))
        );
        assert_eq!(
            parse_meta("\\node stats"),
            Some(MetaCommand::Node("stats".to_string()))
        );
        assert_eq!(
            parse_meta("\\node wal"),
            Some(MetaCommand::Node("wal".to_string()))
        );
    }

    #[test]
    fn test_parse_failover() {
        assert_eq!(
            parse_meta("\\failover"),
            Some(MetaCommand::Failover("".to_string()))
        );
        assert_eq!(
            parse_meta("\\failover history"),
            Some(MetaCommand::Failover("history".to_string()))
        );
        assert_eq!(
            parse_meta("\\failover status"),
            Some(MetaCommand::Failover("status".to_string()))
        );
    }

    #[test]
    fn test_parse_unknown() {
        assert_eq!(
            parse_meta("\\zzz"),
            Some(MetaCommand::Unknown("\\zzz".to_string()))
        );
    }

    #[test]
    fn test_not_meta_command() {
        assert_eq!(parse_meta("SELECT 1"), None);
        assert_eq!(parse_meta(""), None);
    }
}
