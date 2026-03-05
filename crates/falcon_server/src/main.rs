mod health;

use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};

use falcon_cluster::{
    ClusterFailureDetector, ClusterLifecycleConfig, ClusterLifecycleCoordinator,
    DistributedQueryEngine, FailureDetectorConfig, RaftShardCoordinator, ShardedEngine,
    SmartGateway, SmartGatewayConfig, SmartRebalanceConfig, SmartRebalanceRunner,
    ParticipantIdempotencyRegistry, TwoPcRecoveryCoordinator,
    deterministic_2pc::{CoordinatorDecisionLog, DecisionLogConfig},
    indoubt_resolver::InDoubtResolver,
};
use falcon_common::config::{FalconConfig, NodeRole};
use falcon_common::types::ShardId;
use falcon_executor::Executor;
use falcon_protocol_pg::server::PgServer;
use falcon_raft::server::{LocalRaftHandle, start_raft_transport_server};
use falcon_raft::network::GrpcNetworkFactory;
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

    // Initialize observability based on config (early load for logging setup)
    let early_cfg = load_config(&cli.config);
    let _log_guards = falcon_observability::init_tracing_from_config(&early_cfg.logging);
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

    // Default table engine for CREATE TABLE without ENGINE=...
    falcon_common::globals::set_default_table_engine(
        config.storage.default_engine.to_lowercase(),
    );

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

    falcon_common::globals::set_node_role(
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
        if !data_dir.exists() {
            std::fs::create_dir_all(data_dir)
                .map_err(|e| anyhow::anyhow!("failed to create data_dir {:?}: {}", data_dir, e))?;
            tracing::info!("Created data directory: {:?}", data_dir);
        }
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

        apply_engine_config(&mut engine, &config, &replication_log);

        // Wire up group commit syncer if enabled and WAL is present.
        if config.wal.group_commit {
            use falcon_storage::group_commit::GroupCommitConfig;
            let gc_config = GroupCommitConfig {
                flush_interval_us: config.wal.flush_interval_us,
                group_commit_window_us: config.wal.group_commit_window_us,
                ..GroupCommitConfig::default()
            };
            engine.setup_group_commit(gc_config)?;
            tracing::info!(
                "Group commit enabled (window={}µs, flush_interval={}µs)",
                config.wal.group_commit_window_us,
                config.wal.flush_interval_us,
            );
        }

        Arc::new(engine)
    } else {
        tracing::info!("Running in pure in-memory mode (no WAL)");
        let mut engine = StorageEngine::new_in_memory();
        apply_engine_config(&mut engine, &config, &replication_log);
        Arc::new(engine)
    };

    // Initialize transaction manager
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    {
        use std::sync::atomic::Ordering;
        let max_ts = storage.recovered_max_ts.load(Ordering::Relaxed);
        let max_txn = storage.recovered_max_txn_id.load(Ordering::Relaxed);
        if max_ts > 0 || max_txn > 0 {
            txn_mgr.advance_counters_past(max_ts, max_txn);
            tracing::info!("TxnManager counters advanced past recovery: ts={}, txn_id={}", max_ts, max_txn);
        }
    }
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
    let sharded_engine: Option<Arc<ShardedEngine>> = if num_shards > 1 {
        Some(if config.storage.wal_enabled {
            let base_dir = Path::new(&config.storage.data_dir);
            Arc::new(ShardedEngine::new_with_wal(
                num_shards,
                base_dir,
                wal_sync_mode,
                &config.wal_mode,
                config.wal.no_buffering,
            )?)
        } else {
            Arc::new(ShardedEngine::new(num_shards))
        })
    } else {
        None
    };
    let mut pg_server = if let Some(ref sharded) = sharded_engine {
        tracing::info!("Starting in distributed mode with {} shards", num_shards);
        let shard_ids: Vec<ShardId> = (0..num_shards).map(ShardId).collect();
        let dist_engine = Arc::new(DistributedQueryEngine::new(
            sharded.clone(),
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
    if config.logging.slow_query_ms > 0 {
        use falcon_protocol_pg::slow_query_log::SlowQueryLog;
        let threshold = std::time::Duration::from_millis(config.logging.slow_query_ms);
        let log = Arc::new(SlowQueryLog::new(threshold, 1000));
        pg_server.set_slow_query_log(log);
        tracing::info!("Slow query logging enabled (threshold: {}ms)", config.logging.slow_query_ms);
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

    // Shared config store — injected into gRPC service (SyncConfig handler) and lifecycle coordinator
    let shared_config_store = Arc::new(falcon_enterprise::control_plane::ConfigStore::new());

    // Role-based replication startup
    let mut replica_runner_handle: Option<falcon_cluster::ReplicaRunnerHandle> = None;
    let mut grpc_join_handle: Option<tokio::task::JoinHandle<()>> = None;
    // Raft coordinator and transport handle (only set for RaftMember role)
    let mut raft_coordinator: Option<std::sync::Arc<RaftShardCoordinator>> = None;
    let mut raft_transport_handle: Option<tokio::task::JoinHandle<()>> = None;
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
            svc.set_txn_manager(txn_mgr.clone());
            svc.set_executor(executor.clone());
            svc.set_config_store(shared_config_store.clone());
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
                replica_id: config.replication.replica_id as usize,
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
            pg_server.set_replica_metrics(handle.metrics_arc());
            replica_runner_handle = Some(handle);

            // Replica also needs a gRPC service to respond to ForwardQuery and SyncConfig
            let grpc_addr = config.replication.grpc_listen_addr.clone();
            if !grpc_addr.is_empty() {
                let svc = falcon_cluster::grpc_transport::WalReplicationService::new();
                svc.set_storage(storage.clone());
                svc.set_txn_manager(txn_mgr.clone());
                svc.set_executor(executor.clone());
                svc.set_config_store(shared_config_store.clone());
                let grpc_addr_parsed: std::net::SocketAddr = grpc_addr
                    .parse()
                    .map_err(|e| anyhow::anyhow!("Invalid grpc_listen_addr '{grpc_addr}': {e}"))?;
                let grpc_token = coordinator.child_token();
                let grpc_port = grpc_addr_parsed.port().to_string();
                grpc_join_handle = Some(tokio::spawn(async move {
                    use falcon_cluster::proto::wal_replication_server::WalReplicationServer;
                    tracing::info!("gRPC service (replica) starting on {}", grpc_addr_parsed);
                    if let Err(e) = tonic::transport::Server::builder()
                        .add_service(WalReplicationServer::new(svc))
                        .serve_with_shutdown(grpc_addr_parsed, async move {
                            grpc_token.cancelled().await;
                        })
                        .await
                    {
                        tracing::error!("gRPC replica service error: {}", e);
                    }
                    tracing::info!(server = "grpc_replica", port = %grpc_port, "grpc replica server shutdown");
                }));
            }
        }
        NodeRole::Analytics => {
            // Analytics nodes behave like replicas (receive WAL, read-only)
            // but are allowed to use columnstore / vectorised paths.
            let runner_config = falcon_cluster::ReplicaRunnerConfig {
                primary_endpoint: config.replication.primary_endpoint.clone(),
                shard_id: ShardId(0),
                replica_id: config.replication.replica_id as usize,
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
        NodeRole::RaftMember => {
            // ── Raft consensus replication path ──────────────────────────────
            // Validate Raft config before starting.
            if let Err(e) = config.raft.validate() {
                anyhow::bail!("Invalid [raft] configuration: {e}");
            }

            tracing::info!(
                node_id = config.raft.node_id,
                node_ids = ?config.raft.node_ids,
                raft_addr = %config.raft.raft_listen_addr,
                "Role: RAFT_MEMBER — starting Raft consensus"
            );

            // Build the shard routing map for the coordinator.
            let local_node_id = falcon_common::types::NodeId(config.raft.node_id);
            let shard_map = Arc::new(parking_lot::RwLock::new(
                falcon_cluster::ShardMap::uniform(num_shards, local_node_id),
            ));

            // Create one RaftWalGroup per shard.
            let coordinator = RaftShardCoordinator::new(shard_map.clone());

            for shard_idx in 0..num_shards {
                let shard_id = ShardId(shard_idx);
                let group = falcon_cluster::RaftWalGroup::new(
                    shard_id,
                    config.raft.node_ids.clone(),
                    storage.clone(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("RaftWalGroup init shard {shard_idx}: {e}"))?;
                coordinator.register_shard(group);
            }

            // Wait for leader election on all shards.
            coordinator
                .wait_all_leaders_elected(std::time::Duration::from_secs(10))
                .await
                .map_err(|e| anyhow::anyhow!("Raft leader election timeout: {e}"))?;

            // Start automatic failover watcher.
            let poll_interval =
                std::time::Duration::from_millis(config.raft.failover_poll_interval_ms);
            coordinator.start_failover_watcher(poll_interval);

            tracing::info!("Raft leader elected; failover watcher running");

            // Wire the Raft gRPC transport server so remote peers can reach this node.
            // Register shard-0's local Raft node handle for the gRPC transport.
            let local_handle = LocalRaftHandle::new();
            if let Some(group) = coordinator.get_shard_group(ShardId(0)) {
                if let Some(raft_node) = group.raft_group.get_node(config.raft.node_id) {
                    local_handle.set(raft_node).await;
                }
            }

            let raft_listen = config.raft.raft_listen_addr.clone();
            match start_raft_transport_server(raft_listen.clone(), local_handle).await {
                Ok(h) => {
                    tracing::info!("Raft transport server started on {}", raft_listen);
                    raft_transport_handle = Some(h);
                }
                Err(e) => {
                    anyhow::bail!("Raft transport server failed to start on {raft_listen}: {e}");
                }
            }

            // Wire GrpcNetworkFactory peers so remote-node RPCs can be sent.
            // Peers are specified as "<node_id>=<addr>" in raft.peers.
            let peers = config.raft.parsed_peers();
            if !peers.is_empty() {
                let factory = GrpcNetworkFactory::with_peers(peers.clone());
                tracing::info!("Raft gRPC peers configured: {:?}", peers);
                // Store the factory on the coordinator so peer connections
                // persist for the lifetime of the server.
                coordinator.set_grpc_network(factory);
            }

            // Share coordinator with shutdown path.
            raft_coordinator = Some(coordinator);
        }
    }

    // Share Raft coordinator with PgServer for SHOW falcon.raft_stats
    if let Some(ref coord) = raft_coordinator {
        pg_server.set_raft_coordinator(coord.clone());
    }

    // ═══════════════════════════════════════════════════════════════════
    // Cluster Subsystems — lifecycle, failure detection, rebalancing, gateway
    // Only active in distributed (shards>1) or raft mode.
    // ═══════════════════════════════════════════════════════════════════
    let is_distributed = num_shards > 1
        || config.replication.role == NodeRole::RaftMember
        || config.replication.role == NodeRole::Primary
        || config.replication.role == NodeRole::Replica;

    let mut lifecycle_coordinator: Option<Arc<ClusterLifecycleCoordinator>> = None;
    let mut failure_detector: Option<Arc<ClusterFailureDetector>> = None;
    let mut _failure_detector_handle: Option<falcon_cluster::FailureDetectorHandle> = None;
    let mut _rebalance_handle: Option<falcon_cluster::SmartRebalanceRunnerHandle> = None;
    let smart_gateway: Option<Arc<SmartGateway>>;

    if is_distributed && config.cluster.lifecycle_enabled {
        let local_node_id = falcon_common::types::NodeId(config.server.node_id);

        // ── Cluster Lifecycle Coordinator ──
        let lc_config = ClusterLifecycleConfig {
            node_id: local_node_id,
            advertise_addr: config.server.pg_listen_addr.clone(),
            heartbeat_interval: std::time::Duration::from_millis(
                config.cluster.heartbeat_interval_ms,
            ),
            suspect_threshold: std::time::Duration::from_millis(
                config.cluster.suspect_threshold_ms,
            ),
            offline_threshold: std::time::Duration::from_millis(
                config.cluster.offline_threshold_ms,
            ),
            config_sync_interval: std::time::Duration::from_millis(
                config.cluster.config_sync_interval_ms,
            ),
            slo_eval_interval: std::time::Duration::from_millis(
                config.cluster.slo_eval_interval_ms,
            ),
            max_shards: config.cluster.max_shards,
            is_controller: config.replication.role == NodeRole::Primary
                || config.replication.role == NodeRole::RaftMember,
        };
        let lc = Arc::new(ClusterLifecycleCoordinator::new_with_config_store(lc_config, shared_config_store.clone()));
        lc.register_default_slos();
        lc.start();
        tracing::info!("Cluster lifecycle coordinator started");
        lifecycle_coordinator = Some(lc);

        // ── Failure Detector ──
        if config.cluster.failure_detector_enabled {
            let fd_config = FailureDetectorConfig {
                heartbeat_interval: std::time::Duration::from_millis(
                    config.cluster.heartbeat_interval_ms,
                ),
                suspect_threshold: std::time::Duration::from_millis(
                    config.cluster.suspect_threshold_ms,
                ),
                failure_timeout: std::time::Duration::from_millis(
                    config.cluster.offline_threshold_ms,
                ),
                max_consecutive_misses: config.cluster.max_consecutive_misses,
                auto_declare_dead: true,
            };
            let fd = Arc::new(ClusterFailureDetector::new(fd_config));
            fd.register_node(
                local_node_id,
                env!("CARGO_PKG_VERSION").to_owned(),
            );
            // Evaluator is spawned after SmartGateway so the failover callback can capture it

            // Active probing — register peers and start gRPC prober
            if !config.cluster.peers.is_empty() {
                let mut peer_addrs: Vec<(falcon_common::types::NodeId, String)> = vec![];
                for peer_str in &config.cluster.peers {
                    let parts: Vec<&str> = peer_str.splitn(2, ':').collect();
                    if parts.len() == 2 {
                        if let Ok(nid) = parts[0].parse::<u64>() {
                            let addr = if peer_str[parts[0].len() + 1..].starts_with("http") {
                                peer_str[parts[0].len() + 1..].to_owned()
                            } else {
                                format!("http://{}", &peer_str[parts[0].len() + 1..])
                            };
                            let peer_node = falcon_common::types::NodeId(nid);
                            fd.register_node(peer_node, String::new());
                            peer_addrs.push((peer_node, addr));
                        }
                    }
                }
                if !peer_addrs.is_empty() {
                    let prober_token = coordinator.child_token();
                    fd.spawn_prober(prober_token, peer_addrs);
                    tracing::info!("Cluster failure detector prober started");
                }
            }

            tracing::info!("Cluster failure detector started");
            failure_detector = Some(fd);
        }

        // ── Smart Gateway ──
        let gw_role = match config.gateway.role.as_str() {
            "dedicated_gateway" => falcon_cluster::GatewayRole::DedicatedGateway,
            "compute_only" => falcon_cluster::GatewayRole::ComputeOnly,
            _ => falcon_cluster::GatewayRole::SmartGateway,
        };
        let mut gw = Arc::new(SmartGateway::new(SmartGatewayConfig {
            node_id: local_node_id,
            role: gw_role,
            max_inflight: config.gateway.max_inflight,
            max_forwarded: config.gateway.max_forwarded,
            forward_timeout: std::time::Duration::from_millis(config.gateway.forward_timeout_ms),
            topology_staleness: std::time::Duration::from_secs(
                config.gateway.topology_staleness_secs,
            ),
            max_queue_depth: 0,
        }));
        // Seed topology with local shards
        for shard_idx in 0..num_shards {
            gw.topology.update_leader(
                falcon_common::types::ShardId(shard_idx),
                local_node_id,
                config.server.pg_listen_addr.clone(),
            );
        }

        // Parse peers from config and build shard map for cross-node routing
        // Format: "node_id:grpc_addr" e.g. "2:http://10.0.0.2:6543"
        if !config.cluster.peers.is_empty() {
            let mut shard_map = falcon_cluster::routing::ShardMap::uniform(
                num_shards as u64,
                local_node_id,
            );
            for peer_str in &config.cluster.peers {
                let parts: Vec<&str> = peer_str.splitn(2, ':').collect();
                if parts.len() == 2 {
                    if let Ok(nid) = parts[0].parse::<u64>() {
                        let addr = if peer_str[parts[0].len() + 1..].starts_with("http") {
                            peer_str[parts[0].len() + 1..].to_owned()
                        } else {
                            format!("http://{}", &peer_str[parts[0].len() + 1..])
                        };
                        let peer_node_id = falcon_common::types::NodeId(nid);
                        gw.add_node_endpoint(nid, addr.clone());
                        gw.topology.update_leader(
                            falcon_common::types::ShardId(nid % num_shards as u64),
                            peer_node_id,
                            addr.clone(),
                        );
                        shard_map.update_leader(
                            falcon_common::types::ShardId(nid % num_shards as u64),
                            peer_node_id,
                        );
                        // F3: register peer gRPC addr for config sync pushes
                        if let Some(ref lc) = lifecycle_coordinator {
                            lc.register_node_addr(nid, addr.clone());
                        }
                        tracing::info!(node_id = nid, addr = %addr, "Registered peer node");
                    }
                }
            }
            if let Some(g) = Arc::get_mut(&mut gw) {
                g.set_shard_map(shard_map);
            }
        }

        tracing::info!(role = %gw_role, peers = config.cluster.peers.len(), "Smart gateway initialized");
        smart_gateway = Some(gw);
    } else {
        smart_gateway = None;
    }

    // ── Spawn failure detector evaluator (deferred until after SmartGateway) ──
    if let Some(ref fd) = failure_detector {
        let fd_token = coordinator.child_token();
        let gw_for_failover = smart_gateway.clone();
        let exec_for_failover = executor.clone();
        let role = config.replication.role;
        let on_dead: Option<Arc<dyn Fn(falcon_common::types::NodeId) + Send + Sync>> =
            Some(Arc::new(move |dead_node: falcon_common::types::NodeId| {
                tracing::error!(node = dead_node.0, "auto-failover: node declared Dead");
                if let Some(ref gw) = gw_for_failover {
                    gw.topology.mark_node_dead(dead_node);
                    tracing::info!(node = dead_node.0, "marked dead node in gateway topology");
                }
                // Self-promote if we are a Replica and the dead node was our Primary
                if role == NodeRole::Replica {
                    exec_for_failover.set_read_only(false);
                    tracing::warn!("self-promoted to writable after primary failure");
                }
            }));
        _failure_detector_handle = Some(fd.spawn_evaluator_with_failover(fd_token, on_dead));
        tracing::info!("Failure detector evaluator started with auto-failover");
    }

    // ── Smart Rebalancer (needs shards > 1) ──
    if num_shards > 1 && config.rebalance.enabled {
        if let Err(e) = config.rebalance.validate() {
            tracing::error!("Invalid rebalance config: {e} — rebalancer disabled");
        } else {
            let rb_config = SmartRebalanceConfig {
                check_interval: std::time::Duration::from_millis(
                    config.rebalance.check_interval_ms,
                ),
                rebalancer: falcon_cluster::RebalancerConfig {
                    imbalance_threshold: config.rebalance.imbalance_threshold,
                    batch_size: config.rebalance.batch_size,
                    min_donor_rows: config.rebalance.min_donor_rows,
                    cooldown_ms: config.rebalance.cooldown_ms,
                },
                policy: falcon_cluster::RebalanceTriggerPolicy {
                    imbalance_threshold: config.rebalance.imbalance_threshold,
                    min_rows_to_trigger: config.rebalance.min_donor_rows * 10,
                    min_shards: 2,
                },
                start_paused: config.rebalance.start_paused,
                ..Default::default()
            };
            let runner = SmartRebalanceRunner::new(rb_config);
            let rb_engine = sharded_engine.clone().unwrap_or_else(|| Arc::new(ShardedEngine::new(num_shards)));
            match runner.start(rb_engine) {
                Ok(handle) => {
                    tracing::info!(
                        check_interval_ms = config.rebalance.check_interval_ms,
                        threshold = config.rebalance.imbalance_threshold,
                        "Smart rebalancer started"
                    );
                    _rebalance_handle = Some(handle);
                }
                Err(e) => {
                    tracing::error!("Failed to start smart rebalancer: {e}");
                }
            }
        }
    }

    // ── 2PC Decision Log + InDoubt Resolver (distributed mode) ──
    let mut _indoubt_resolver_handle: Option<falcon_cluster::indoubt_resolver::InDoubtResolverHandle> = None;
    let mut _recovery_coord: Option<TwoPcRecoveryCoordinator> = None;
    if let Some(ref engine) = sharded_engine {
        let data_dir = Path::new(&config.storage.data_dir);
        let journal_path = data_dir.join("2pc_decision.journal");
        let decision_log = CoordinatorDecisionLog::open(DecisionLogConfig::default(), &journal_path);
        tracing::info!(path = ?journal_path, "2PC decision journal opened");

        let outcome_cache = falcon_cluster::indoubt_resolver::TxnOutcomeCache::new(
            std::time::Duration::from_secs(3600),
            10_000,
        );
        let resolver = InDoubtResolver::with_engine(outcome_cache, engine.clone());

        // Re-register any unapplied decisions from the journal as in-doubt txns
        for rec in decision_log.unapplied_decisions() {
            use falcon_cluster::indoubt_resolver::TxnOutcome;
            use falcon_cluster::deterministic_2pc::CoordinatorDecision;
            let participant_pairs: Vec<_> = rec.participant_shards.iter()
                .map(|&s| (s, falcon_common::types::TxnId(rec.txn_id.0)))
                .collect();
            resolver.register_indoubt(rec.txn_id, participant_pairs);
            let outcome = match rec.decision {
                CoordinatorDecision::Commit => TxnOutcome::Committed,
                CoordinatorDecision::Abort => TxnOutcome::Aborted,
            };
            resolver.record_decision(rec.txn_id, outcome);
        }

        match resolver.start() {
            Ok(handle) => {
                tracing::info!("InDoubt resolver started");
                _indoubt_resolver_handle = Some(handle);
            }
            Err(e) => tracing::error!("Failed to start indoubt resolver: {e}"),
        }

        // Wire TwoPcRecoveryCoordinator — hoisted to outer scope so it lives until shutdown
        let idempotency = Arc::new(ParticipantIdempotencyRegistry::new(10_000, std::time::Duration::from_secs(3600)));
        _recovery_coord = Some(TwoPcRecoveryCoordinator::new(idempotency)
            .with_engine(engine.clone()));
        tracing::info!("TwoPc recovery coordinator wired");
    }

    // Pass cluster subsystems to PgServer for SHOW commands
    if let Some(ref lc) = lifecycle_coordinator {
        pg_server.set_lifecycle_coordinator(lc.clone());
    }
    if let Some(ref fd) = failure_detector {
        pg_server.set_failure_detector(fd.clone());
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

    // ── Background metrics reporter (publishes to Prometheus every 5s) ──
    {
        falcon_observability::init_txn_counters();
        let stor = storage.clone();
        let txn = txn_mgr.clone();
        let conns = pg_server.active_connections_handle();
        let metrics_token = coordinator.child_token();
        let node_count = if config.replication.role == NodeRole::Standalone { 1u64 } else { 2 };
        let lc_ref = lifecycle_coordinator.clone();
        let fd_ref = failure_detector.clone();
        let gw_ref = smart_gateway.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = metrics_token.cancelled() => break,
                }
                falcon_observability::record_active_connections(
                    conns.load(std::sync::atomic::Ordering::Relaxed),
                );
                falcon_observability::record_txn_active_count(txn.active_count() as u64);
                let ws = stor.wal_stats_snapshot();
                let backlog = stor.wal_backlog_bytes();
                falcon_observability::record_wal_stability_metrics(
                    ws.fsync_total_us, ws.fsync_max_us, ws.fsync_avg_us,
                    ws.group_commit_avg_size, backlog,
                );
                let hot = stor.memory_hot_bytes();
                let cold = stor.memory_cold_bytes();
                falcon_observability::record_memory_metrics(
                    0, 0, 0, hot + cold, 0, 0, "normal", 0,
                );

                // Cluster health from real subsystems
                if let Some(ref lc) = lc_ref {
                    let m = lc.metrics();
                    let total = m.nodes_online + m.nodes_suspect + m.nodes_offline;
                    let health = if m.nodes_offline > 0 { "degraded" } else { "healthy" };
                    falcon_observability::record_cluster_health_metrics(
                        health,
                        total as u64,
                        m.nodes_online as u64,
                        1,
                        if m.nodes_offline == 0 { 1 } else { 0 },
                        m.nodes_suspect as u64,
                    );
                } else {
                    falcon_observability::record_cluster_health_metrics(
                        "healthy", node_count, node_count, 1, 1, 0,
                    );
                }

                // Failure detector metrics
                if let Some(ref fd) = fd_ref {
                    let alive = fd.alive_count() as u64;
                    let dead = fd.dead_nodes().len() as u64;
                    falcon_observability::record_replication_metrics(
                        fd.epoch(), alive, dead,
                        fd.metrics.heartbeats_received.load(std::sync::atomic::Ordering::Relaxed),
                    );
                } else {
                    falcon_observability::record_replication_metrics(0, 0, 0, 0);
                }

                // Gateway metrics
                if let Some(ref gw) = gw_ref {
                    let snap = gw.metrics.snapshot();
                    falcon_observability::record_rebalancer_metrics(
                        snap.local_exec_total, snap.forward_total,
                        false, false, 0.0, snap.reject_no_route_total,
                        snap.reject_overloaded_total, snap.reject_timeout_total, 0.0,
                    );
                } else {
                    falcon_observability::record_rebalancer_metrics(0, 0, false, false, 0.0, 0, 0, 0, 0.0);
                }

                falcon_observability::record_segment_streaming_metrics(0, 0, 0, 0, 0, 0, 0);
                falcon_observability::record_shard_migration_metrics(0, 0, 0, 0, 0);
            }
        });
        tracing::info!("Background metrics reporter started (interval=5s)");
    }

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

    // 1b. Drop Raft coordinator (stops failover watcher and Raft groups)
    if let Some(coord) = raft_coordinator.take() {
        tracing::info!("Stopping Raft coordinator...");
        drop(coord);
        tracing::info!("Raft coordinator stopped");
    }

    // 1c. Stop Raft transport server
    if let Some(h) = raft_transport_handle.take() {
        h.abort();
    }

    // 1d. Stop cluster subsystems
    if let Some(ref lc) = lifecycle_coordinator {
        tracing::info!("Stopping cluster lifecycle coordinator...");
        lc.shutdown();
    }
    if let Some(h) = _rebalance_handle.take() {
        tracing::info!("Stopping smart rebalancer...");
        h.stop_and_join();
        tracing::info!("Smart rebalancer stopped");
    }
    // Failure detector handle drops automatically (cancel token propagated from coordinator)

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

    // 5b. Flush per-shard WALs (distributed mode)
    if let Some(ref se) = sharded_engine {
        match se.flush_wal_all() {
            Ok(()) => tracing::info!("Shard WAL final flush complete ({} shards)", se.num_shards()),
            Err(e) => tracing::error!(error = %e, "Shard WAL flush FAILED"),
        }
        se.shutdown_wal_all();
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
        "raft_member" | "raft" => Ok(NodeRole::RaftMember),
        _ => Err(format!(
            "Invalid role '{s}': expected standalone, primary, replica, analytics, or raft_member"
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

fn apply_engine_config(
    engine: &mut StorageEngine,
    config: &FalconConfig,
    replication_log: &Option<Arc<falcon_cluster::ReplicationLog>>,
) {
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

    if config.ustm.enabled {
        engine.set_ustm_config(&config.ustm);
    }

    engine.set_lsm_sync_writes(config.storage.lsm_sync_writes);
    tracing::info!("LSM sync_writes: {}", config.storage.lsm_sync_writes);

    engine.configure_cdc(config.cdc.enabled, config.cdc.buffer_size);

    engine.configure_pitr(
        config.pitr.enabled,
        &config.pitr.archive_dir,
        config.pitr.retention_hours,
    );

    engine.configure_multi_tenant(
        config.multi_tenant.enabled,
        config.multi_tenant.metering_enabled,
        config.multi_tenant.default_max_qps,
        config.multi_tenant.default_max_concurrent_txns,
        config.multi_tenant.default_max_memory_bytes,
        config.multi_tenant.default_max_storage_bytes,
    );

    engine.configure_tde(
        config.tde.enabled,
        &config.tde.key_env_var,
        config.tde.encrypt_wal,
        config.tde.encrypt_data,
    );

    engine.configure_resource_isolation(
        config.resource_isolation.enabled,
        config.resource_isolation.io_rate_bytes_per_sec,
        config.resource_isolation.io_burst_bytes,
        config.resource_isolation.max_bg_threads,
        config.resource_isolation.fg_pressure_threshold,
    );

    if let Some(ref log) = replication_log {
        let log_clone = log.clone();
        engine.set_wal_observer(Box::new(move |record| {
            log_clone.append(record.clone());
        }));
        tracing::info!("WAL observer attached for primary replication");
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
