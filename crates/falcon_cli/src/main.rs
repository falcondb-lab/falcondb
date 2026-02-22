mod args;
mod client;
mod cluster;
mod complete;
mod consistency;
mod csv;
mod export;
mod failover;
mod format;
mod history;
mod import;
mod integration;
mod manage;
mod meta;
mod node;
mod pager;
mod policy;
mod repl;
mod runner;
mod splitter;
mod timing;
mod txn;

use anyhow::{Context, Result};
use args::Args;
use clap::Parser;
use client::DbClient;
use cluster::{run_cluster, ClusterCmd};
use consistency::{run_consistency, ConsistencyCmd};
use export::{parse_export, run_export};
use failover::{run_failover, FailoverCmd};
use format::OutputMode;
use import::{parse_import, run_import};
use integration::machine::{wrap_machine_error, wrap_machine_output, CliMode, Identity};
use integration::{run_events, run_integration, EventsCmd, IntegrationCmd};
use manage::audit::{run_audit, AuditCmd};
use manage::cluster_mgmt::{
    apply_cluster_mode, is_cluster_mode_cmd, parse_cluster_mode_arg, plan_cluster_mode,
};
use manage::node_mgmt::{apply_node_drain, apply_node_resume, plan_node_drain, plan_node_resume};
use manage::shard_mgmt::{apply_shard_move, plan_shard_move};
use manage::txn_mgmt::{
    apply_txn_resolve, is_txn_resolve_cmd, parse_txn_resolve_arg, plan_txn_resolve,
};
use meta::parse_meta;
use node::{run_node, NodeCmd};
use pager::print_with_pager;
use policy::automation::{run_automation, AutomationCmd};
use policy::{run_policy, PolicyCmd};
use runner::run_statement;
use splitter::split_statements;
use std::process;
use tracing::debug;
use txn::{run_txn, TxnCmd};

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("fsql: error: {:#}", e);
        process::exit(1);
    }
}

async fn run() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();

    // Connect
    let mut client = DbClient::connect(&args)
        .await
        .context("Could not connect to FalconDB")?;

    let mode = effective_mode(&args);

    // Dispatch execution mode
    let cli_mode = CliMode::parse(&args.mode).unwrap_or_default();
    let identity = Identity::resolve(args.operator.as_deref(), args.source.as_deref());

    if let Some(cmd) = args.command.as_deref() {
        debug!("Mode: -c");
        let stmts = split_statements(cmd);
        run_statements(
            &client,
            &stmts,
            mode,
            args.tuples_only,
            args.on_error_stop(),
            args.apply,
            &cli_mode,
            &identity,
        )
        .await?;
    } else if let Some(path) = args.file.as_deref() {
        debug!("Mode: -f {}", path);
        let sql =
            std::fs::read_to_string(path).with_context(|| format!("Cannot read file: {}", path))?;
        let stmts = split_statements(&sql);
        run_statements(
            &client,
            &stmts,
            mode,
            args.tuples_only,
            args.on_error_stop(),
            args.apply,
            &cli_mode,
            &identity,
        )
        .await?;
    } else {
        // Interactive REPL
        debug!("Mode: REPL");
        repl::run_repl(&mut client, &args).await?;
    }

    Ok(())
}

fn script_wrap_machine(
    output: &str,
    command: &str,
    success: bool,
    cli_mode: &CliMode,
    identity: &Identity,
) -> String {
    if cli_mode.is_machine() {
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

fn parse_shard_move_arg_script(arg: &str) -> anyhow::Result<(String, String)> {
    let lower = arg.trim().to_lowercase();
    if !lower.starts_with("move ") {
        anyhow::bail!("Usage: \\shard move <shard_id> to <node_id>");
    }
    let rest = arg.trim()[5..].trim();
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

async fn run_statements(
    client: &DbClient,
    stmts: &[String],
    mode: OutputMode,
    tuples_only: bool,
    on_error_stop: bool,
    apply: bool,
    cli_mode: &CliMode,
    identity: &Identity,
) -> Result<()> {
    for stmt in stmts {
        let trimmed = stmt.trim();
        // Handle meta-commands in script mode
        if let Some(cmd) = parse_meta(trimmed) {
            use meta::MetaCommand;
            match cmd {
                MetaCommand::Export(_) => match parse_export(trimmed) {
                    Ok(exp_cmd) => match run_export(client, &exp_cmd).await {
                        Ok(n) => println!("Exported {} rows to '{}'.", n, exp_cmd.file),
                        Err(e) => {
                            eprintln!("ERROR (EXPORT): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("ERROR (EXPORT): {:#}", e);
                        if on_error_stop {
                            process::exit(3);
                        }
                    }
                },
                MetaCommand::Import(_) => match parse_import(trimmed) {
                    Ok(imp_cmd) => match run_import(client, &imp_cmd).await {
                        Ok(s) => println!(
                            "Import complete: {} total, {} inserted, {} failed.",
                            s.total, s.inserted, s.failed
                        ),
                        Err(e) => {
                            eprintln!("ERROR (IMPORT): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("ERROR (IMPORT): {:#}", e);
                        if on_error_stop {
                            process::exit(3);
                        }
                    }
                },
                MetaCommand::Cluster(arg) => {
                    let out = if is_cluster_mode_cmd(&arg) {
                        match parse_cluster_mode_arg(&arg) {
                            Ok(target) => {
                                if apply {
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
                        Err(e) => {
                            eprintln!("ERROR (\\cluster): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                MetaCommand::Txn(arg) => {
                    let out = if is_txn_resolve_cmd(&arg) {
                        match parse_txn_resolve_arg(&arg) {
                            Ok((txn_id, resolution)) => {
                                if apply {
                                    match resolution {
                                        Some(res) => apply_txn_resolve(client, &txn_id, &res, true)
                                            .await
                                            .map(|r| r.render()),
                                        None => Ok(
                                            "Specify resolution: \\txn resolve <id> commit|abort\n"
                                                .to_string(),
                                        ),
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
                        Err(e) => {
                            eprintln!("ERROR (\\txn): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                MetaCommand::Consistency(arg) => {
                    let sub = ConsistencyCmd::parse(&arg);
                    match run_consistency(client, &sub, mode).await {
                        Ok(out) => print_with_pager(&out, false),
                        Err(e) => {
                            eprintln!("ERROR (\\consistency): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                MetaCommand::Node(arg) => {
                    let lower = arg.to_lowercase();
                    let out = if lower.starts_with("drain ") {
                        let node_id = arg[6..].trim();
                        if apply {
                            apply_node_drain(client, node_id, true)
                                .await
                                .map(|r| r.render())
                        } else {
                            plan_node_drain(client, node_id, mode).await
                        }
                    } else if lower.starts_with("resume ") {
                        let node_id = arg[7..].trim();
                        if apply {
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
                        Err(e) => {
                            eprintln!("ERROR (\\node): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                MetaCommand::Failover(arg) => {
                    let sub = FailoverCmd::parse(&arg);
                    match run_failover(client, &sub, mode).await {
                        Ok(out) => print_with_pager(&out, false),
                        Err(e) => {
                            eprintln!("ERROR (\\failover): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                MetaCommand::Shard(arg) => {
                    let out = parse_shard_move_arg_script(&arg)
                        .and_then(|(shard_id, target_node)| Ok((shard_id, target_node)));
                    let result = match out {
                        Ok((shard_id, target_node)) => {
                            if apply {
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
                        Err(e) => {
                            eprintln!("ERROR (\\shard): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                MetaCommand::Audit(arg) => {
                    let sub = AuditCmd::parse(&arg);
                    match run_audit(client, &sub, mode).await {
                        Ok(out) => print_with_pager(&out, false),
                        Err(e) => {
                            eprintln!("ERROR (\\audit): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                MetaCommand::Integration(arg) => {
                    let sub = IntegrationCmd::parse(&arg);
                    match run_integration(client, &sub, mode).await {
                        Ok(out) => {
                            let final_out = script_wrap_machine(
                                &out,
                                r"\integration",
                                true,
                                cli_mode,
                                identity,
                            );
                            print_with_pager(&final_out, false);
                        }
                        Err(e) => {
                            let msg = format!("{:#}", e);
                            if cli_mode.is_machine() {
                                print!(
                                    "{}",
                                    wrap_machine_error(
                                        r"\integration",
                                        &msg,
                                        &identity.operator,
                                        &identity.source
                                    )
                                );
                            } else {
                                eprintln!("ERROR (\\integration): {}", msg);
                            }
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                MetaCommand::Events(arg) => {
                    let sub = EventsCmd::parse(&arg);
                    match run_events(client, &sub, mode).await {
                        Ok(out) => {
                            let final_out =
                                script_wrap_machine(&out, r"\events", true, cli_mode, identity);
                            print_with_pager(&final_out, false);
                        }
                        Err(e) => {
                            let msg = format!("{:#}", e);
                            if cli_mode.is_machine() {
                                print!(
                                    "{}",
                                    wrap_machine_error(
                                        r"\events",
                                        &msg,
                                        &identity.operator,
                                        &identity.source
                                    )
                                );
                            } else {
                                eprintln!("ERROR (\\events): {}", msg);
                            }
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                MetaCommand::Policy(arg) => match PolicyCmd::parse(&arg) {
                    Ok(cmd) => match run_policy(client, &cmd, mode, apply).await {
                        Ok(out) => print_with_pager(&out, false),
                        Err(e) => {
                            eprintln!("ERROR (\\policy): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("ERROR (\\policy): {:#}", e);
                        if on_error_stop {
                            process::exit(3);
                        }
                    }
                },
                MetaCommand::Automation(arg) => {
                    let sub = AutomationCmd::parse(&arg);
                    match run_automation(client, &sub, mode).await {
                        Ok(out) => print_with_pager(&out, false),
                        Err(e) => {
                            eprintln!("ERROR (\\automation): {:#}", e);
                            if on_error_stop {
                                process::exit(3);
                            }
                        }
                    }
                }
                // Other meta-commands are silently skipped in script mode
                // (\q, \x, \timing etc. don't make sense in -c/-f)
                _ => {}
            }
            continue;
        }
        if let Err(e) = run_statement(client, stmt, mode, tuples_only).await {
            eprintln!("ERROR: {:#}", e);
            if on_error_stop {
                process::exit(3);
            }
        }
    }
    Ok(())
}

fn effective_mode(args: &Args) -> OutputMode {
    if args.tuples_only {
        OutputMode::TuplesOnly
    } else if args.csv {
        OutputMode::Csv
    } else if args.json {
        OutputMode::Json
    } else if args.expanded {
        OutputMode::Expanded
    } else {
        OutputMode::Table
    }
}
