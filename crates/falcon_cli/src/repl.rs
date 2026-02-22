use crate::args::Args;
use crate::client::DbClient;
use crate::cluster::{run_cluster, ClusterCmd};
use crate::complete::FsqlHelper;
use crate::consistency::{run_consistency, ConsistencyCmd};
use crate::export::{parse_export, run_export};
use crate::failover::{run_failover, FailoverCmd};
use crate::format::OutputMode;
use crate::history::{load_history, save_history};
use crate::import::{parse_import, run_import};
use crate::integration::machine::{wrap_machine_error, wrap_machine_output, CliMode, Identity};
use crate::integration::{run_events, run_integration, EventsCmd, IntegrationCmd};
use crate::manage::audit::{run_audit, AuditCmd};
use crate::manage::cluster_mgmt::{
    apply_cluster_mode, is_cluster_mode_cmd, parse_cluster_mode_arg, plan_cluster_mode,
};
use crate::manage::node_mgmt::{
    apply_node_drain, apply_node_resume, plan_node_drain, plan_node_resume,
};
use crate::manage::shard_mgmt::{apply_shard_move, plan_shard_move};
use crate::manage::txn_mgmt::{
    apply_txn_resolve, is_txn_resolve_cmd, parse_txn_resolve_arg, plan_txn_resolve,
};
use crate::meta::{execute_meta, parse_meta, MetaResult};
use crate::node::{run_node, NodeCmd};
use crate::pager::print_with_pager;
use crate::policy::automation::{run_automation, AutomationCmd};
use crate::policy::{run_policy, PolicyCmd};
use crate::runner::run_statement;
use crate::timing::TimingState;
use crate::txn::{run_txn, TxnCmd};
use anyhow::Result;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tracing::debug;

pub async fn run_repl(client: &mut DbClient, args: &Args) -> Result<()> {
    let mut rl = Editor::new()?;
    rl.set_helper(Some(FsqlHelper));

    // Load persistent history
    let _ = load_history(&mut rl);

    let mut expanded = args.expanded;
    let mut timing = TimingState::default();
    let mut buffer = String::new();

    println!(
        "fsql (FalconDB) v{} — connected to \"{}\" on {}:{}",
        env!("CARGO_PKG_VERSION"),
        client.dbname,
        client.host,
        client.port
    );
    println!("Type \\? for help, \\q to quit.");

    loop {
        let prompt = if buffer.is_empty() {
            format!(
                "fsql ({}@{}:{}/{})> ",
                client.user, client.host, client.port, client.dbname
            )
        } else {
            "fsql ...> ".to_string()
        };

        let line = match rl.readline(&prompt) {
            Ok(l) => l,
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C: clear buffer
                if !buffer.is_empty() {
                    println!("(buffer cleared)");
                    buffer.clear();
                }
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D: quit
                break;
            }
            Err(e) => {
                eprintln!("Input error: {}", e);
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Add to history (skip duplicates handled by rustyline)
        let _ = rl.add_history_entry(trimmed);

        // Meta-command (only when buffer is empty)
        if buffer.is_empty() {
            if let Some(cmd) = parse_meta(trimmed) {
                match execute_meta(&cmd, client).await? {
                    MetaResult::Quit => break,
                    MetaResult::Output(s) => print!("{}", s),
                    MetaResult::ToggleExpanded => {
                        expanded = !expanded;
                        println!(
                            "Expanded display is {}.",
                            if expanded { "on" } else { "off" }
                        );
                    }
                    MetaResult::ToggleTiming => {
                        let on = timing.toggle();
                        println!("Timing is {}.", if on { "on" } else { "off" });
                    }
                    MetaResult::Export(line) => match parse_export(&line) {
                        Ok(cmd) => {
                            let timer = timing.maybe_start();
                            match run_export(client, &cmd).await {
                                Ok(n) => println!("Exported {} rows to '{}'.", n, cmd.file),
                                Err(e) => eprintln!("ERROR (EXPORT): {:#}", e),
                            }
                            timing.maybe_print(timer);
                        }
                        Err(e) => eprintln!("ERROR (EXPORT): {:#}", e),
                    },
                    MetaResult::Import(line) => match parse_import(&line) {
                        Ok(cmd) => {
                            let timer = timing.maybe_start();
                            match run_import(client, &cmd).await {
                                Ok(s) => println!(
                                    "Import complete: {} total, {} inserted, {} failed.",
                                    s.total, s.inserted, s.failed
                                ),
                                Err(e) => eprintln!("ERROR (IMPORT): {:#}", e),
                            }
                            timing.maybe_print(timer);
                        }
                        Err(e) => eprintln!("ERROR (IMPORT): {:#}", e),
                    },
                    MetaResult::Cluster(arg) => {
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        let out = if is_cluster_mode_cmd(&arg) {
                            match parse_cluster_mode_arg(&arg) {
                                Ok(target) => {
                                    if args.apply {
                                        apply_cluster_mode(client, &target, true)
                                            .await
                                            .map(|r| r.render())
                                    } else {
                                        plan_cluster_mode(client, &target, mode).await
                                    }
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            let sub = ClusterCmd::parse(&arg);
                            run_cluster(client, &sub, mode).await
                        };
                        match out {
                            Ok(s) => print_with_pager(&s, false),
                            Err(e) => eprintln!("ERROR (\\cluster): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Txn(arg) => {
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        let out = if is_txn_resolve_cmd(&arg) {
                            match parse_txn_resolve_arg(&arg) {
                                Ok((txn_id, resolution)) => {
                                    if args.apply {
                                        match resolution {
                                            Some(res) => apply_txn_resolve(client, &txn_id, &res, true).await
                                                .map(|r| r.render()),
                                            None => Ok("Specify resolution: \\txn resolve <id> commit|abort\n".to_string()),
                                        }
                                    } else {
                                        plan_txn_resolve(client, &txn_id, mode).await
                                    }
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            let sub = TxnCmd::parse(&arg);
                            run_txn(client, &sub, mode).await
                        };
                        match out {
                            Ok(s) => print_with_pager(&s, false),
                            Err(e) => eprintln!("ERROR (\\txn): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Consistency(arg) => {
                        let sub = ConsistencyCmd::parse(&arg);
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        match run_consistency(client, &sub, mode).await {
                            Ok(out) => print_with_pager(&out, false),
                            Err(e) => eprintln!("ERROR (\\consistency): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Node(arg) => {
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        let lower = arg.to_lowercase();
                        let out = if lower.starts_with("drain ") {
                            let node_id = arg[6..].trim();
                            if args.apply {
                                apply_node_drain(client, node_id, true)
                                    .await
                                    .map(|r| r.render())
                            } else {
                                plan_node_drain(client, node_id, mode).await
                            }
                        } else if lower.starts_with("resume ") {
                            let node_id = arg[7..].trim();
                            if args.apply {
                                apply_node_resume(client, node_id, true)
                                    .await
                                    .map(|r| r.render())
                            } else {
                                plan_node_resume(client, node_id, mode).await
                            }
                        } else {
                            let sub = NodeCmd::parse(&arg);
                            run_node(client, &sub, mode).await
                        };
                        match out {
                            Ok(s) => print_with_pager(&s, false),
                            Err(e) => eprintln!("ERROR (\\node): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Failover(arg) => {
                        let sub = FailoverCmd::parse(&arg);
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        match run_failover(client, &sub, mode).await {
                            Ok(out) => print_with_pager(&out, false),
                            Err(e) => eprintln!("ERROR (\\failover): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Shard(arg) => {
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        let out = parse_shard_move_arg(&arg).and_then(|(shard_id, target_node)| {
                            // Return a future — we need to block on it
                            // Use a local async block via Box::pin
                            Ok((shard_id, target_node))
                        });
                        let result = match out {
                            Ok((shard_id, target_node)) => {
                                if args.apply {
                                    apply_shard_move(client, &shard_id, &target_node, true)
                                        .await
                                        .map(|r| r.render())
                                } else {
                                    plan_shard_move(client, &shard_id, &target_node, mode).await
                                }
                            }
                            Err(e) => Err(e),
                        };
                        match result {
                            Ok(s) => print_with_pager(&s, false),
                            Err(e) => eprintln!("ERROR (\\shard): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Audit(arg) => {
                        let sub = AuditCmd::parse(&arg);
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        match run_audit(client, &sub, mode).await {
                            Ok(out) => print_with_pager(&out, false),
                            Err(e) => eprintln!("ERROR (\\audit): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Policy(arg) => {
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        match PolicyCmd::parse(&arg) {
                            Ok(cmd) => match run_policy(client, &cmd, mode, args.apply).await {
                                Ok(out) => print_with_pager(&out, false),
                                Err(e) => eprintln!("ERROR (\\policy): {:#}", e),
                            },
                            Err(e) => eprintln!("ERROR (\\policy): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Automation(arg) => {
                        let sub = AutomationCmd::parse(&arg);
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        match run_automation(client, &sub, mode).await {
                            Ok(out) => print_with_pager(&out, false),
                            Err(e) => eprintln!("ERROR (\\automation): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Integration(arg) => {
                        let sub = IntegrationCmd::parse(&arg);
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        match run_integration(client, &sub, mode).await {
                            Ok(out) => {
                                let final_out =
                                    maybe_wrap_machine(&out, r"\integration", true, args);
                                print_with_pager(&final_out, false);
                            }
                            Err(e) => eprintln!("ERROR (\\integration): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Events(arg) => {
                        let sub = EventsCmd::parse(&arg);
                        let mode = effective_mode(args, expanded);
                        let timer = timing.maybe_start();
                        match run_events(client, &sub, mode).await {
                            Ok(out) => {
                                let final_out = maybe_wrap_machine(&out, r"\events", true, args);
                                print_with_pager(&final_out, false);
                            }
                            Err(e) => eprintln!("ERROR (\\events): {:#}", e),
                        }
                        timing.maybe_print(timer);
                    }
                    MetaResult::Reconnect(dbname) => {
                        match DbClient::connect_to(
                            &client.host,
                            client.port,
                            &client.user,
                            &dbname,
                            None,
                        )
                        .await
                        {
                            Ok(new_client) => {
                                *client = new_client;
                                println!("You are now connected to database \"{}\".", dbname);
                            }
                            Err(e) => eprintln!("ERROR: {}", e),
                        }
                    }
                }
                continue;
            }
        }

        // Accumulate into buffer
        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(trimmed);

        // Execute when we see a terminating semicolon
        if trimmed.ends_with(';') {
            let sql = buffer.trim().to_string();
            buffer.clear();

            let mode = effective_mode(args, expanded);
            debug!("REPL execute: {}", sql);

            let timer = timing.maybe_start();
            if let Err(e) = run_statement(client, &sql, mode, args.tuples_only).await {
                eprintln!("ERROR: {}", e);
            }
            timing.maybe_print(timer);
        }
    }

    // Save history on exit
    let _ = save_history(&mut rl);

    Ok(())
}

/// Wrap output in machine-mode JSON envelope if --mode machine is set.
fn maybe_wrap_machine(output: &str, command: &str, success: bool, args: &Args) -> String {
    let cli_mode = CliMode::parse(&args.mode).unwrap_or_default();
    if cli_mode.is_machine() {
        let identity = Identity::resolve(args.operator.as_deref(), args.source.as_deref());
        wrap_machine_output(
            command,
            output,
            success,
            &identity.operator,
            &identity.source,
        )
    } else {
        output.to_string()
    }
}

/// Parse "move <shard_id> to <node_id>" from a \shard argument.
fn parse_shard_move_arg(arg: &str) -> anyhow::Result<(String, String)> {
    let lower = arg.trim().to_lowercase();
    if !lower.starts_with("move ") {
        anyhow::bail!("Usage: \\shard move <shard_id> to <node_id>");
    }
    let rest = arg.trim()[5..].trim(); // strip "move "
                                       // Find " to " (case-insensitive)
    let lower_rest = rest.to_lowercase();
    let to_pos = lower_rest
        .find(" to ")
        .ok_or_else(|| anyhow::anyhow!("Usage: \\shard move <shard_id> to <node_id>"))?;
    let shard_id = rest[..to_pos].trim().to_string();
    let target_node = rest[to_pos + 4..].trim().to_string();
    if shard_id.is_empty() || target_node.is_empty() {
        anyhow::bail!("Usage: \\shard move <shard_id> to <node_id>");
    }
    Ok((shard_id, target_node))
}

pub fn effective_mode(args: &Args, expanded: bool) -> OutputMode {
    if args.tuples_only {
        OutputMode::TuplesOnly
    } else if args.csv {
        OutputMode::Csv
    } else if args.json {
        OutputMode::Json
    } else if expanded || args.expanded {
        OutputMode::Expanded
    } else {
        OutputMode::Table
    }
}
