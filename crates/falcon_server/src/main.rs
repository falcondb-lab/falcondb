mod health;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};

use falcon_cluster::{DistributedQueryEngine, ShardedEngine};
use falcon_common::config::{FalconConfig, NodeRole};
use falcon_common::types::ShardId;
use falcon_executor::Executor;
use falcon_protocol_pg::server::PgServer;
use falcon_server::shutdown::{self, ShutdownCoordinator, ShutdownReason};
use falcon_server::service;
use falcon_storage::engine::StorageEngine;
use falcon_storage::gc::{GcConfig, GcRunner};
use falcon_storage::memory::MemoryBudget;
use falcon_storage::wal::SyncMode;
use falcon_txn::TxnManager;

// ═══════════════════════════════════════════════════════════════════════════
// CLI Definition
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Parser, Debug)]
#[command(name = "falcon", about = "FalconDB — PG-Compatible In-Memory OLTP")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Config file path.
    #[arg(short, long, default_value = "falcon.toml", global = true)]
    config: String,

    /// PG listen address (overrides config).
    #[arg(long)]
    pg_addr: Option<String>,

    /// Data directory (overrides config).
    #[arg(long)]
    data_dir: Option<String>,

    /// Disable WAL (pure in-memory mode).
    #[arg(long)]
    no_wal: bool,

    /// Number of in-process shards (>1 enables distributed mode).
    #[arg(long, default_value = "1")]
    shards: u64,

    /// Metrics listen address.
    #[arg(long, default_value = "0.0.0.0:9090")]
    metrics_addr: String,

    /// Node role: standalone, primary, or replica (overrides config).
    #[arg(long, value_parser = parse_node_role)]
    role: Option<NodeRole>,

    /// Primary's gRPC endpoint for replica to connect (overrides config).
    #[arg(long)]
    primary_endpoint: Option<String>,

    /// gRPC listen address for replication service (overrides config).
    #[arg(long)]
    grpc_addr: Option<String>,

    /// Print the default configuration as TOML and exit.
    #[arg(long)]
    print_default_config: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Manage FalconDB as a Windows Service.
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
    /// Run diagnostic checks.
    Doctor,
    /// Show version and build information.
    Version,
    /// Show server and service status.
    Status,
    /// Config management (migrate, check).
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Purge all FalconDB data (ProgramData). Requires confirmation.
    Purge {
        /// Skip confirmation prompt.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigAction {
    /// Migrate config file to the current schema version.
    Migrate,
    /// Check config version without modifying.
    Check,
}

#[derive(Subcommand, Debug)]
enum ServiceAction {
    /// Install FalconDB as a Windows Service.
    Install,
    /// Uninstall the FalconDB Windows Service.
    Uninstall,
    /// Start the FalconDB Windows Service.
    Start,
    /// Stop the FalconDB Windows Service.
    Stop,
    /// Restart the FalconDB Windows Service.
    Restart,
    /// Show status of the FalconDB Windows Service.
    Status,
    /// Internal: run as Windows Service (called by SCM, not by users).
    #[command(hide = true)]
    Dispatch,
}

// ═══════════════════════════════════════════════════════════════════════════
// main() — dispatch to console run, service commands, or doctor
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        // ── Service subcommands ──
        Some(Command::Service { action }) => {
            match action {
                ServiceAction::Install => {
                    service::commands::install(&cli.config)
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                ServiceAction::Uninstall => {
                    service::commands::uninstall()
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                ServiceAction::Start => {
                    service::commands::start()
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                ServiceAction::Stop => {
                    service::commands::stop()
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                ServiceAction::Restart => {
                    service::commands::restart()
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                ServiceAction::Status => {
                    service::commands::status()
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                ServiceAction::Dispatch => {
                    // Service mode: initialize file logger, then dispatch to SCM
                    let log_dir = service::paths::service_log_dir();
                    let _ = std::fs::create_dir_all(&log_dir);
                    let _guard = service::logger::init_file_logger(&log_dir, "falcon.log");
                    tracing::info!(mode = "service", "FalconDB starting in service mode");

                    // Register the server runner so the SCM callback can invoke it
                    service::windows::set_server_runner(Box::new(|config_path, coord| {
                        Box::pin(run_server(config_path, coord))
                    }));

                    service::windows::scm::set_config_path(&cli.config);
                    #[cfg(windows)]
                    {
                        service::windows::scm::dispatch()
                            .map_err(|e| anyhow::anyhow!("Service dispatch failed: {e}"))?;
                    }
                    #[cfg(not(windows))]
                    {
                        anyhow::bail!("Service dispatch is only available on Windows");
                    }
                }
            }
            Ok(())
        }

        // ── Doctor ──
        Some(Command::Doctor) => {
            falcon_server::doctor::run_doctor(&cli.config);
            Ok(())
        }

        // ── Version ──
        Some(Command::Version) => {
            print_version();
            Ok(())
        }

        // ── Status ──
        Some(Command::Status) => {
            service::commands::status()
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(())
        }

        // ── Config ──
        Some(Command::Config { action }) => {
            match action {
                ConfigAction::Migrate => {
                    falcon_server::config_migrate::run_config_migrate(&cli.config);
                }
                ConfigAction::Check => {
                    let text = std::fs::read_to_string(&cli.config)
                        .unwrap_or_default();
                    match falcon_server::config_migrate::check_config_version(&text) {
                        falcon_server::config_migrate::ConfigVersionStatus::Current => {
                            println!("Config version: {} (current)", falcon_common::config::CURRENT_CONFIG_VERSION);
                        }
                        falcon_server::config_migrate::ConfigVersionStatus::NeedsMigration { from, to } => {
                            println!("Config version: {from} (needs migration to {to})");
                            println!("Run: falcon config migrate --config {}", cli.config);
                        }
                        falcon_server::config_migrate::ConfigVersionStatus::TooNew { found, max_supported } => {
                            eprintln!("Config version {found} is newer than supported (max: {max_supported})");
                        }
                    }
                }
            }
            Ok(())
        }

        // ── Purge ──
        Some(Command::Purge { yes }) => {
            run_purge(yes);
            Ok(())
        }

        // ── Default: console mode (backward-compatible) ──
        None => {
            run_console(cli).await
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Console mode — the original main() flow
// ═══════════════════════════════════════════════════════════════════════════

async fn run_console(cli: Cli) -> Result<()> {
    // --print-default-config: dump default TOML and exit
    if cli.print_default_config {
        let default_config = FalconConfig::default();
        let toml_str = toml::to_string_pretty(&default_config)
            .unwrap_or_else(|e| format!("# failed to serialize default config: {e}"));
        println!("{toml_str}");
        return Ok(());
    }

    // Install crash domain panic hook (must be before any other initialization)
    falcon_common::crash_domain::install_panic_hook();

    // Initialize observability (stderr for console mode)
    falcon_observability::init_tracing();
    tracing::info!(
        mode = "console",
        version = env!("CARGO_PKG_VERSION"),
        git = env!("FALCONDB_GIT_HASH"),
        built = env!("FALCONDB_BUILD_TIME"),
        "Starting FalconDB..."
    );

    run_server_inner(&cli.config, &cli, None).await
}

/// Core server logic — called from both console mode and service mode.
///
/// When `external_coordinator` is `Some`, the server uses it (service mode).
/// When `None`, the server creates its own coordinator and waits for OS signals.
pub async fn run_server(config_path: String, external_coordinator: Option<ShutdownCoordinator>) -> Result<()> {
    let cli = Cli {
        command: None,
        config: config_path.clone(),
        pg_addr: None,
        data_dir: None,
        no_wal: false,
        shards: 1,
        metrics_addr: "0.0.0.0:9090".to_owned(),
        role: None,
        primary_endpoint: None,
        grpc_addr: None,
        print_default_config: false,
    };
    run_server_inner(&config_path, &cli, external_coordinator).await
}

async fn run_server_inner(
    config_path: &str,
    cli: &Cli,
    external_coordinator: Option<ShutdownCoordinator>,
) -> Result<()> {
    // Load or create config
    let mut config = load_config(config_path);

    // CLI overrides
    if let Some(ref addr) = cli.pg_addr {
        config.server.pg_listen_addr = addr.clone();
    }
    if let Some(ref dir) = cli.data_dir {
        config.storage.data_dir = dir.clone();
    }
    if cli.no_wal {
        config.storage.wal_enabled = false;
    }
    if let Some(role) = cli.role {
        config.replication.role = role;
    }
    if let Some(ref ep) = cli.primary_endpoint {
        config.replication.primary_endpoint = ep.clone();
    }
    if let Some(ref addr) = cli.grpc_addr {
        config.replication.grpc_listen_addr = addr.clone();
    }

    tracing::info!("Config: {:?}", config);

    // ── Production Safety Mode ──
    let safety_violations = falcon_common::config::validate_production_safety(&config);
    if !safety_violations.is_empty() {
        let critical_count = safety_violations.iter().filter(|v| v.severity == "CRITICAL").count();
        for v in &safety_violations {
            if v.severity == "CRITICAL" {
                tracing::error!(id = v.id, "[Production Safety] {}: {}", v.id, v.message);
            } else {
                tracing::warn!(id = v.id, "[Production Safety] {}: {}", v.id, v.message);
            }
        }
        if config.production_safety.enforce && critical_count > 0 {
            anyhow::bail!(
                "Production safety mode is enabled and {} CRITICAL violation(s) detected. \
                 Fix the configuration or set production_safety.enforce = false to proceed. \
                 See docs/production_safety.md for details.",
                critical_count
            );
        } else if critical_count > 0 {
            tracing::warn!(
                "[Production Safety] {} CRITICAL violation(s) detected but enforce=false — proceeding anyway. \
                 Set [production_safety] enforce = true to block unsafe startups.",
                critical_count
            );
        }
    } else {
        tracing::info!("[Production Safety] All checks passed");
    }

    // ── Linux Platform Detection ──
    #[cfg(target_os = "linux")]
    {
        let data_path = std::path::Path::new(&config.storage.data_dir);
        let report = falcon_storage::io::linux_platform::detect_platform(data_path);
        tracing::info!(
            kernel = %report.kernel.release,
            filesystem = %report.filesystem,
            block_device = %report.block_device,
            numa_nodes = report.numa.node_count,
            cpus = report.numa.cpu_count,
            "[Platform] Linux environment detected"
        );
        for adv in &report.advisories {
            match adv.severity {
                falcon_storage::io::linux_platform::AdvisorySeverity::Critical => {
                    tracing::error!(id = %adv.id, "[Platform] {}: {} → {}", adv.id, adv.message, adv.recommendation);
                }
                falcon_storage::io::linux_platform::AdvisorySeverity::Warn => {
                    tracing::warn!(id = %adv.id, "[Platform] {}: {} → {}", adv.id, adv.message, adv.recommendation);
                }
                falcon_storage::io::linux_platform::AdvisorySeverity::Info => {
                    tracing::info!(id = %adv.id, "[Platform] {}: {}", adv.id, adv.message);
                }
            }
        }
    }

    // Expose node role to SHOW falcon.node_role via env var
    std::env::set_var(
        "FALCON_NODE_ROLE",
        format!("{:?}", config.replication.role).to_lowercase(),
    );

    // Initialize metrics
    if let Err(e) = falcon_observability::init_metrics(&cli.metrics_addr) {
        tracing::warn!("Failed to initialize metrics: {}", e);
    }

    // Create replication log for Primary mode (before StorageEngine is wrapped in Arc)
    let replication_log = if config.replication.role == NodeRole::Primary {
        Some(Arc::new(falcon_cluster::ReplicationLog::new()))
    } else {
        None
    };

    // Initialize storage engine (shard 0 is also the "local" engine for single-shard mode)
    let wal_sync_mode = match config.wal.sync_mode.as_str() {
        "none" => SyncMode::None,
        "fsync" => SyncMode::FSync,
        _ => SyncMode::FDataSync,
    };
    tracing::info!("WAL sync mode: {:?}", wal_sync_mode);

    let storage = if config.storage.wal_enabled {
        let data_dir = Path::new(&config.storage.data_dir);
        tracing::info!(
            "WAL mode: '{}' (no_buffering={})",
            config.wal_mode,
            config.wal.no_buffering,
        );
        let has_wal = data_dir.join("falcon.wal").exists()
            || std::fs::read_dir(data_dir)
                .map(|entries| {
                    entries.flatten().any(|e| {
                        let name = e.file_name();
                        let s = name.to_string_lossy();
                        s.starts_with("falcon_") && s.ends_with(".wal")
                    })
                })
                .unwrap_or(false);
        let mut engine = if has_wal {
            tracing::info!("Recovering from WAL at {:?}", data_dir);
            StorageEngine::recover(data_dir)?
        } else {
            StorageEngine::new_with_wal_mode(
                Some(data_dir),
                wal_sync_mode,
                &config.wal_mode,
                config.wal.no_buffering,
            )?
        };

        // Apply memory budget from config
        let budget = MemoryBudget::new(
            config.memory.shard_soft_limit_bytes,
            config.memory.shard_hard_limit_bytes,
        );
        if budget.enabled {
            engine.set_memory_budget(budget);
            tracing::info!(
                "Memory backpressure enabled: soft={}B, hard={}B",
                config.memory.shard_soft_limit_bytes,
                config.memory.shard_hard_limit_bytes,
            );
        }

        // Apply USTM configuration
        if config.ustm.enabled {
            engine.set_ustm_config(&config.ustm);
        }

        // Apply LSM sync_writes from storage config
        engine.set_lsm_sync_writes(config.storage.lsm_sync_writes);
        tracing::info!("LSM sync_writes: {}", config.storage.lsm_sync_writes);

        // Hook WAL observer for primary replication
        if let Some(ref log) = replication_log {
            let log_clone = log.clone();
            engine.set_wal_observer(Box::new(move |record| {
                log_clone.append(record.clone());
            }));
            tracing::info!("WAL observer attached for primary replication");
        }

        Arc::new(engine)
    } else {
        tracing::info!("Running in pure in-memory mode (no WAL)");
        let mut engine = StorageEngine::new_in_memory();

        // Apply memory budget from config
        let budget = MemoryBudget::new(
            config.memory.shard_soft_limit_bytes,
            config.memory.shard_hard_limit_bytes,
        );
        if budget.enabled {
            engine.set_memory_budget(budget);
            tracing::info!(
                "Memory backpressure enabled: soft={}B, hard={}B",
                config.memory.shard_soft_limit_bytes,
                config.memory.shard_hard_limit_bytes,
            );
        }

        // Apply USTM configuration
        if config.ustm.enabled {
            engine.set_ustm_config(&config.ustm);
        }

        // Apply LSM sync_writes from storage config
        engine.set_lsm_sync_writes(config.storage.lsm_sync_writes);
        tracing::info!("LSM sync_writes: {}", config.storage.lsm_sync_writes);

        // Even in-memory mode can replicate if role is Primary
        if let Some(ref log) = replication_log {
            let log_clone = log.clone();
            engine.set_wal_observer(Box::new(move |record| {
                log_clone.append(record.clone());
            }));
            tracing::info!("WAL observer attached for primary replication (in-memory mode)");
        }

        Arc::new(engine)
    };

    // Initialize transaction manager
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    txn_mgr.set_slow_txn_threshold_us(config.server.slow_txn_threshold_us);
    tracing::info!("Slow txn threshold: {}us (0=disabled)", config.server.slow_txn_threshold_us);

    // Initialize executor (read-only on replicas/analytics to reject writes at SQL level)
    let mut executor = if matches!(
        config.replication.role,
        NodeRole::Replica | NodeRole::Analytics
    ) {
        Executor::new_read_only(storage.clone(), txn_mgr.clone())
    } else {
        Executor::new(storage.clone(), txn_mgr.clone())
    };
    // Shared active connections counter — wired to both executor and PgServer
    let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    executor.set_connection_info(active_connections.clone(), config.server.max_connections);
    let executor = Arc::new(executor);

    // Start PG server — distributed or single-shard
    let num_shards = cli.shards.max(1);
    let mut pg_server = if num_shards > 1 {
        tracing::info!("Starting in distributed mode with {} shards", num_shards);
        let sharded = Arc::new(ShardedEngine::new(num_shards));
        let shard_ids: Vec<ShardId> = (0..num_shards).map(ShardId).collect();
        let dist_engine = Arc::new(DistributedQueryEngine::new(
            sharded,
            std::time::Duration::from_secs(30),
        ));
        PgServer::new_distributed(
            config.server.pg_listen_addr.clone(),
            storage.clone(),
            txn_mgr.clone(),
            executor.clone(),
            shard_ids,
            dist_engine,
        )
    } else {
        PgServer::new(
            config.server.pg_listen_addr.clone(),
            storage.clone(),
            txn_mgr.clone(),
            executor.clone(),
        )
    };

    // Set authentication configuration
    pg_server.set_auth_config(config.server.auth.clone());
    if config.server.auth.method != falcon_common::config::AuthMethod::Trust {
        tracing::info!("Authentication method: {:?}", config.server.auth.method);
    }

    // Share active connections counter between PgServer, Executor, and health server
    pg_server.set_active_connections(active_connections.clone());
    if config.server.max_connections > 0 {
        pg_server.set_max_connections(config.server.max_connections);
        tracing::info!("Max connections: {}", config.server.max_connections);
    }
    if config.server.statement_timeout_ms > 0 {
        pg_server.set_default_statement_timeout_ms(config.server.statement_timeout_ms);
        tracing::info!(
            "Default statement timeout: {}ms",
            config.server.statement_timeout_ms
        );
    }
    if config.server.idle_timeout_ms > 0 {
        pg_server.set_idle_timeout_ms(config.server.idle_timeout_ms);
        tracing::info!(
            "Connection idle timeout: {}ms",
            config.server.idle_timeout_ms
        );
    }

    // Start background GC runner (if enabled in config)
    let _gc_runner = if config.gc.enabled {
        let gc_config = GcConfig {
            enabled: true,
            interval_ms: config.gc.interval_ms,
            batch_size: config.gc.batch_size,
            min_chain_length: config.gc.min_chain_length,
            max_chain_length: 0,
            min_sweep_interval_ms: 0,
        };
        tracing::info!(
            "Background GC runner started (interval={}ms, batch_size={}, min_chain_length={})",
            gc_config.interval_ms,
            gc_config.batch_size,
            gc_config.min_chain_length,
        );
        match GcRunner::start(storage.clone(), txn_mgr.clone(), gc_config) {
            Ok(runner) => Some(runner),
            Err(e) => {
                tracing::error!(
                    "Failed to start GC runner: {} — running without background GC",
                    e
                );
                None
            }
        }
    } else {
        tracing::info!("Background GC runner disabled (gc.enabled=false)");
        None
    };

    // ═══════════════════════════════════════════════════════════════════
    // Shutdown Protocol — single CancellationToken as root primitive
    // ═══════════════════════════════════════════════════════════════════
    let coordinator = external_coordinator.unwrap_or_default();

    // Role-based replication startup
    let mut replica_runner_handle: Option<falcon_cluster::ReplicaRunnerHandle> = None;
    let mut grpc_join_handle: Option<tokio::task::JoinHandle<()>> = None;
    match config.replication.role {
        NodeRole::Primary => {
            let grpc_addr = config.replication.grpc_listen_addr.clone();
            tracing::info!("Role: PRIMARY — gRPC replication service on {}", grpc_addr);

            // Use the replication_log that is already wired to the WAL observer
            let log = replication_log
                .clone()
                .ok_or_else(|| anyhow::anyhow!("replication_log must exist for Primary role"))?;
            let svc = falcon_cluster::grpc_transport::WalReplicationService::new();
            svc.set_storage(storage.clone());
            for i in 0..num_shards {
                svc.register_shard(ShardId(i), log.clone());
            }

            let grpc_addr_parsed: std::net::SocketAddr = grpc_addr
                .parse()
                .map_err(|e| anyhow::anyhow!("Invalid grpc_listen_addr '{grpc_addr}': {e}"))?;

            // Wire gRPC server with graceful shutdown via CancellationToken.
            // The JoinHandle is captured and awaited during ordered teardown.
            let grpc_token = coordinator.child_token();
            let grpc_port = grpc_addr_parsed.port().to_string();
            grpc_join_handle = Some(tokio::spawn(async move {
                use falcon_cluster::proto::wal_replication_server::WalReplicationServer;
                tracing::info!("gRPC replication server starting on {}", grpc_addr_parsed);
                if let Err(e) = tonic::transport::Server::builder()
                    .add_service(WalReplicationServer::new(svc))
                    .serve_with_shutdown(grpc_addr_parsed, async move {
                        grpc_token.cancelled().await;
                    })
                    .await
                {
                    tracing::error!("gRPC replication server error: {}", e);
                }
                tracing::info!(
                    server = "grpc_replication",
                    port = %grpc_port,
                    "grpc server shutdown (port={})",
                    grpc_port,
                );
            }));
        }
        NodeRole::Replica => {
            let runner_config = falcon_cluster::ReplicaRunnerConfig {
                primary_endpoint: config.replication.primary_endpoint.clone(),
                shard_id: ShardId(0),
                replica_id: 0,
                max_records_per_chunk: config.replication.max_records_per_chunk,
                ack_interval_chunks: 10,
                initial_backoff: std::time::Duration::from_millis(
                    config.replication.poll_interval_ms,
                ),
                max_backoff: std::time::Duration::from_millis(config.replication.max_backoff_ms),
                connect_timeout: std::time::Duration::from_millis(
                    config.replication.connect_timeout_ms,
                ),
            };
            tracing::info!(
                "Role: REPLICA — connecting to primary at {} via ReplicaRunner",
                runner_config.primary_endpoint,
            );

            let runner = falcon_cluster::ReplicaRunner::new(runner_config, storage.clone());
            let handle = runner.start();
            // Share metrics with PgServer for SHOW falcon.replica_stats
            pg_server.set_replica_metrics(handle.metrics_arc());
            // Store handle for graceful shutdown
            replica_runner_handle = Some(handle);
        }
        NodeRole::Analytics => {
            // Analytics nodes behave like replicas (receive WAL, read-only)
            // but are allowed to use columnstore / vectorised paths.
            let runner_config = falcon_cluster::ReplicaRunnerConfig {
                primary_endpoint: config.replication.primary_endpoint.clone(),
                shard_id: ShardId(0),
                replica_id: 0,
                max_records_per_chunk: config.replication.max_records_per_chunk,
                ack_interval_chunks: 10,
                initial_backoff: std::time::Duration::from_millis(
                    config.replication.poll_interval_ms,
                ),
                max_backoff: std::time::Duration::from_millis(config.replication.max_backoff_ms),
                connect_timeout: std::time::Duration::from_millis(
                    config.replication.connect_timeout_ms,
                ),
            };
            tracing::info!(
                "Role: ANALYTICS — connecting to primary at {} (read-only, columnstore enabled)",
                runner_config.primary_endpoint,
            );

            let runner = falcon_cluster::ReplicaRunner::new(runner_config, storage.clone());
            let handle = runner.start();
            pg_server.set_replica_metrics(handle.metrics_arc());
            replica_runner_handle = Some(handle);
        }
        NodeRole::Standalone => {
            tracing::info!("Role: STANDALONE — no replication");
        }
    }

    let pg_port = config
        .server
        .pg_listen_addr
        .split(':')
        .next_back()
        .unwrap_or("5433");
    tracing::info!(
        "FalconDB ready (role={:?}, {} shard{}). Connect with: psql -h 127.0.0.1 -p {}",
        config.replication.role,
        num_shards,
        if num_shards > 1 { "s" } else { "" },
        pg_port
    );

    // ── Health check HTTP server (JoinHandle tracked) ──
    let health_state = Arc::new(health::HealthState::new(
        config.replication.role,
        pg_server.active_connections_handle(),
        storage.clone(),
    ));
    health_state.set_max_connections(config.server.max_connections);
    let health_addr = config.server.admin_listen_addr.clone();
    let health_token = coordinator.child_token();
    let health_state_for_server = health_state.clone();
    let health_handle = tokio::spawn(async move {
        health::run_health_server(&health_addr, health_state_for_server, async move {
            health_token.cancelled().await;
        })
        .await;
    });

    // ── PG server — blocks until shutdown signal or external coordinator, then drains ──
    let health_state_for_shutdown = health_state.clone();
    let coord_for_signal = coordinator.clone();
    let drain_timeout =
        std::time::Duration::from_secs(config.server.shutdown_drain_timeout_secs.max(1));
    pg_server
        .run_with_shutdown(
            async move {
                // In service mode, the coordinator is cancelled by SCM event handler.
                // In console mode, we wait for OS signals.
                tokio::select! {
                    reason = shutdown::wait_for_os_signal() => {
                        tracing::info!(reason = %reason, "OS signal received — initiating graceful shutdown");
                        health_state_for_shutdown.set_ready(false);
                        coord_for_signal.shutdown(reason);
                    }
                    _ = coord_for_signal.cancelled() => {
                        tracing::info!(reason = %coord_for_signal.reason(), "Shutdown coordinator triggered — initiating graceful shutdown");
                        health_state_for_shutdown.set_ready(false);
                    }
                }
            },
            drain_timeout,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // ═══════════════════════════════════════════════════════════════════
    // Ordered teardown — await ALL server JoinHandles, flush WAL
    // ═══════════════════════════════════════════════════════════════════
    tracing::info!(reason = %coordinator.reason(), "Ordered teardown starting");

    // 1. Stop replica runner gracefully if running
    if let Some(handle) = replica_runner_handle {
        tracing::info!("Stopping ReplicaRunner...");
        handle.stop().await;
        tracing::info!("ReplicaRunner stopped");
    }

    // 2. Ensure global shutdown is signalled (in case PG server exited without signal)
    if !coordinator.is_shutting_down() {
        coordinator.shutdown(ShutdownReason::Requested);
    }

    // 3. Await gRPC server shutdown
    if let Some(h) = grpc_join_handle {
        tracing::info!("Awaiting gRPC replication server shutdown...");
        let _ = h.await;
        tracing::info!("gRPC replication server stopped");
    }

    // 4. Await health server shutdown
    tracing::info!("Awaiting health server shutdown...");
    let _ = health_handle.await;
    tracing::info!("Health server stopped");

    // 5. Final WAL flush — ensure all committed data is durable before exit
    if storage.is_wal_enabled() {
        match storage.flush_wal() {
            Ok(()) => {
                let lsn = storage.current_wal_lsn();
                tracing::info!(flushed_lsn = lsn, "WAL final flush complete");
            }
            Err(e) => {
                tracing::error!(error = %e, "WAL final flush FAILED — data may be lost");
            }
        }
    }

    tracing::info!(
        reason = %coordinator.reason(),
        "FalconDB shutdown complete — all ports released, WAL flushed"
    );
    Ok(())
}

fn parse_node_role(s: &str) -> Result<NodeRole, String> {
    match s.to_lowercase().as_str() {
        "standalone" => Ok(NodeRole::Standalone),
        "primary" => Ok(NodeRole::Primary),
        "replica" => Ok(NodeRole::Replica),
        "analytics" => Ok(NodeRole::Analytics),
        _ => Err(format!(
            "Invalid role '{s}': expected standalone, primary, replica, or analytics"
        )),
    }
}

fn print_version() {
    println!("FalconDB v{}", env!("CARGO_PKG_VERSION"));
    println!("  Git commit:    {}", env!("FALCONDB_GIT_HASH"));
    println!("  Build time:    {}", env!("FALCONDB_BUILD_TIME"));
    println!("  Config schema: v{}", falcon_common::config::CURRENT_CONFIG_VERSION);
    println!("  Build target:  {}", std::env::consts::ARCH);
    println!("  OS:            {}", std::env::consts::OS);
    println!("  Exe:           {}", std::env::current_exe().map_or_else(|_| "unknown".into(), |p| p.display().to_string()));
}

/// Returns the version string for use by other modules (e.g., startup banner).
pub fn falcondb_version_string() -> String {
    format!(
        "FalconDB v{} (git:{} built:{})",
        env!("CARGO_PKG_VERSION"),
        env!("FALCONDB_GIT_HASH"),
        env!("FALCONDB_BUILD_TIME"),
    )
}

fn run_purge(skip_confirm: bool) {
    let root = service::paths::program_data_root();
    println!("FalconDB Purge");
    println!("==============");
    println!();
    println!("This will PERMANENTLY delete ALL FalconDB data:");
    println!("  {}", root.display());
    println!();

    if !root.exists() {
        println!("Nothing to purge — directory does not exist.");
        return;
    }

    if !skip_confirm {
        println!("Type 'YES' to confirm:");
        let mut input = String::new();
        if std::io::stdin().read_line(&mut input).is_err() {
            eprintln!("Failed to read input.");
            std::process::exit(1);
        }
        if input.trim() != "YES" {
            println!("Aborted.");
            return;
        }
    }

    // Check if service is running
    #[cfg(windows)]
    {
        let output = std::process::Command::new("sc.exe")
            .args(["query", service::paths::SERVICE_NAME])
            .output();
        if let Ok(o) = output {
            let stdout = String::from_utf8_lossy(&o.stdout);
            if stdout.contains("RUNNING") {
                eprintln!("ERROR: Service is running. Stop it first: falcon service stop");
                std::process::exit(1);
            }
        }
    }

    match std::fs::remove_dir_all(&root) {
        Ok(_) => {
            println!("Purged: {}", root.display());
            println!("All data, config, logs, and certificates have been removed.");
        }
        Err(e) => {
            eprintln!("ERROR: Failed to purge {}: {}", root.display(), e);
            std::process::exit(1);
        }
    }
}

fn load_config(path: &str) -> FalconConfig {
    if let Ok(content) = std::fs::read_to_string(path) { match toml::from_str(&content) {
        Ok(config) => {
            tracing::info!("Loaded config from {}", path);
            config
        }
        Err(e) => {
            tracing::warn!("Failed to parse config {}: {}, using defaults", path, e);
            FalconConfig::default()
        }
    } } else {
        tracing::info!("Config file {} not found, using defaults", path);
        FalconConfig::default()
    }
}
