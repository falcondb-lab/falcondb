mod health;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;

use falcon_cluster::{DistributedQueryEngine, ShardedEngine};
use falcon_common::config::{FalconConfig, NodeRole};
use falcon_common::types::ShardId;
use falcon_executor::Executor;
use falcon_protocol_pg::server::PgServer;
use falcon_storage::engine::StorageEngine;
use falcon_storage::gc::{GcConfig, GcRunner};
use falcon_storage::memory::MemoryBudget;
use falcon_txn::TxnManager;

#[derive(Parser, Debug)]
#[command(name = "falcon", about = "FalconDB — PG-Compatible In-Memory OLTP")]
struct Cli {
    /// Config file path.
    #[arg(short, long, default_value = "falcon.toml")]
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // --print-default-config: dump default TOML and exit
    if cli.print_default_config {
        let default_config = FalconConfig::default();
        let toml_str = toml::to_string_pretty(&default_config)
            .unwrap_or_else(|e| format!("# failed to serialize default config: {}", e));
        println!("{}", toml_str);
        return Ok(());
    }

    // Install crash domain panic hook (must be before any other initialization)
    falcon_common::crash_domain::install_panic_hook();

    // Initialize observability
    falcon_observability::init_tracing();
    tracing::info!("Starting FalconDB...");

    // Load or create config
    let mut config = load_config(&cli.config);

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

    // Expose node role to SHOW falcon.node_role via env var
    std::env::set_var("FALCON_NODE_ROLE", format!("{:?}", config.replication.role).to_lowercase());

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
    let storage = if config.storage.wal_enabled {
        let data_dir = Path::new(&config.storage.data_dir);
        let mut engine = if data_dir.join("falcon.wal").exists() {
            tracing::info!("Recovering from WAL at {:?}", data_dir);
            StorageEngine::recover(data_dir)?
        } else {
            StorageEngine::new(Some(data_dir))?
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

    // Initialize executor (read-only on replicas/analytics to reject writes at SQL level)
    let mut executor = if matches!(config.replication.role, NodeRole::Replica | NodeRole::Analytics) {
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
        tracing::info!("Default statement timeout: {}ms", config.server.statement_timeout_ms);
    }
    if config.server.idle_timeout_ms > 0 {
        pg_server.set_idle_timeout_ms(config.server.idle_timeout_ms);
        tracing::info!("Connection idle timeout: {}ms", config.server.idle_timeout_ms);
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
            gc_config.interval_ms, gc_config.batch_size, gc_config.min_chain_length,
        );
        Some(GcRunner::start(storage.clone(), txn_mgr.clone(), gc_config))
    } else {
        tracing::info!("Background GC runner disabled (gc.enabled=false)");
        None
    };

    // Role-based replication startup
    let mut replica_runner_handle: Option<falcon_cluster::ReplicaRunnerHandle> = None;
    match config.replication.role {
        NodeRole::Primary => {
            let grpc_addr = config.replication.grpc_listen_addr.clone();
            tracing::info!("Role: PRIMARY — gRPC replication service on {}", grpc_addr);

            // Use the replication_log that is already wired to the WAL observer
            let log = replication_log.clone().ok_or_else(|| {
                anyhow::anyhow!("replication_log must exist for Primary role")
            })?;
            let svc = falcon_cluster::grpc_transport::WalReplicationService::new();
            svc.set_storage(storage.clone());
            for i in 0..num_shards {
                svc.register_shard(ShardId(i), log.clone());
            }

            let grpc_addr_parsed: std::net::SocketAddr = grpc_addr
                .parse()
                .map_err(|e| anyhow::anyhow!("Invalid grpc_listen_addr '{}': {}", grpc_addr, e))?;

            tokio::spawn(async move {
                use falcon_cluster::proto::wal_replication_server::WalReplicationServer;
                tracing::info!("gRPC replication server starting on {}", grpc_addr_parsed);
                if let Err(e) = tonic::transport::Server::builder()
                    .add_service(WalReplicationServer::new(svc))
                    .serve(grpc_addr_parsed)
                    .await
                {
                    tracing::error!("gRPC replication server error: {}", e);
                }
            });
        }
        NodeRole::Replica => {
            let runner_config = falcon_cluster::ReplicaRunnerConfig {
                primary_endpoint: config.replication.primary_endpoint.clone(),
                shard_id: ShardId(0),
                replica_id: 0,
                max_records_per_chunk: config.replication.max_records_per_chunk,
                ack_interval_chunks: 10,
                initial_backoff: std::time::Duration::from_millis(config.replication.poll_interval_ms),
                max_backoff: std::time::Duration::from_millis(config.replication.max_backoff_ms),
                connect_timeout: std::time::Duration::from_millis(config.replication.connect_timeout_ms),
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
                initial_backoff: std::time::Duration::from_millis(config.replication.poll_interval_ms),
                max_backoff: std::time::Duration::from_millis(config.replication.max_backoff_ms),
                connect_timeout: std::time::Duration::from_millis(config.replication.connect_timeout_ms),
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

    // Shared shutdown signal for PG server and health check server
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);

    // Start health check HTTP server
    let health_state = Arc::new(health::HealthState::new(
        config.replication.role,
        pg_server.active_connections_handle(),
        storage.clone(),
    ));
    health_state.set_max_connections(config.server.max_connections);
    let health_addr = config.server.admin_listen_addr.clone();
    let mut health_rx = shutdown_tx.subscribe();
    let health_state_for_server = health_state.clone();
    tokio::spawn(async move {
        health::run_health_server(
            &health_addr,
            health_state_for_server,
            async move { let _ = health_rx.changed().await; },
        ).await;
    });

    // Run PG server with graceful shutdown on SIGINT (Ctrl+C) or SIGTERM.
    // Mark health state as not-ready immediately so load balancers stop
    // routing traffic before the drain begins.
    let health_state_for_shutdown = health_state.clone();
    let drain_timeout = std::time::Duration::from_secs(
        config.server.shutdown_drain_timeout_secs.max(1),
    );
    pg_server
        .run_with_shutdown(
            async move {
                let shutdown_reason = wait_for_shutdown_signal().await;
                tracing::info!("{} — initiating graceful shutdown", shutdown_reason);
                // Mark not-ready immediately so health probes return 503
                health_state_for_shutdown.set_ready(false);
            },
            drain_timeout,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    // Stop replica runner gracefully if running
    if let Some(handle) = replica_runner_handle {
        tracing::info!("Stopping ReplicaRunner...");
        handle.stop().await;
        tracing::info!("ReplicaRunner stopped");
    }

    // Signal health server to shut down too
    let _ = shutdown_tx.send(true);

    Ok(())
}

/// Wait for SIGINT (Ctrl+C) or SIGTERM, returning a description of which signal fired.
async fn wait_for_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())
            .unwrap_or_else(|e| panic!("Failed to register SIGTERM handler: {}", e));
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT (Ctrl+C) received",
            _ = sigterm.recv() => "SIGTERM received",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT (Ctrl+C) received"
    }
}

fn parse_node_role(s: &str) -> Result<NodeRole, String> {
    match s.to_lowercase().as_str() {
        "standalone" => Ok(NodeRole::Standalone),
        "primary" => Ok(NodeRole::Primary),
        "replica" => Ok(NodeRole::Replica),
        "analytics" => Ok(NodeRole::Analytics),
        _ => Err(format!("Invalid role '{}': expected standalone, primary, replica, or analytics", s)),
    }
}

fn load_config(path: &str) -> FalconConfig {
    match std::fs::read_to_string(path) {
        Ok(content) => match toml::from_str(&content) {
            Ok(config) => {
                tracing::info!("Loaded config from {}", path);
                config
            }
            Err(e) => {
                tracing::warn!("Failed to parse config {}: {}, using defaults", path, e);
                FalconConfig::default()
            }
        },
        Err(_) => {
            tracing::info!("Config file {} not found, using defaults", path);
            FalconConfig::default()
        }
    }
}
