# FalconDB Documentation Index

> Organized reference for all documentation. Files grouped by topic.

## Getting Started

| Document | Description |
|----------|-------------|
| [INSTALL.md](INSTALL.md) | Installation guide (MSI, ZIP, Linux) |
| [UPGRADE.md](UPGRADE.md) | Upgrade procedures and rollback |
| [UNINSTALL.md](UNINSTALL.md) | Uninstall and cleanup |
| [OPERATIONS.md](OPERATIONS.md) | CLI reference, monitoring, diagnostics |
| [cli.md](cli.md) | Full CLI command reference |

## Architecture & Design

| Document | Description |
|----------|-------------|
| [../ARCHITECTURE.md](../ARCHITECTURE.md) | Top-level architecture overview |
| [bootstrap_flow.md](bootstrap_flow.md) | Server startup and bootstrap sequence |
| [config_management.md](config_management.md) | Configuration schema and management |
| [connection_model.md](connection_model.md) | Client connection lifecycle |
| [shutdown_model.md](shutdown_model.md) | Graceful shutdown sequence |
| [error_model.md](error_model.md) | Error taxonomy and SQLSTATE codes |
| [versioning.md](versioning.md) | Version numbering and compatibility |
| [design/](design/) | Design documents (prepared statements, routing, SQL subset) |
| [adr/](adr/) | Architecture Decision Records |

## Storage & WAL

| Document | Description |
|----------|-------------|
| [wal_backend_matrix.md](wal_backend_matrix.md) | WAL backend comparison (Posix, IOCP, raw) |
| [wal_lsn_structured.md](wal_lsn_structured.md) | LSN structure and WAL record format |
| [windows_wal_modes.md](windows_wal_modes.md) | Windows-specific WAL modes (IOCP) |
| [cold_segment_format.md](cold_segment_format.md) | Cold storage segment format |
| [design_ustm.md](design_ustm.md) | Unified Storage & Transaction Manager |
| [unified_data_plane_full.md](unified_data_plane_full.md) | Unified data plane design (supersedes `unified_data_plane.md`) |
| [dictionary_in_segment_store.md](dictionary_in_segment_store.md) | Dictionary encoding in segments |
| [dictionary_lifecycle.md](dictionary_lifecycle.md) | Dictionary lifecycle management |

## Compression & Codecs

| Document | Description |
|----------|-------------|
| [compression_profiles.md](compression_profiles.md) | Compression algorithm profiles |
| [segment_codec_matrix.md](segment_codec_matrix.md) | Segment codec feature matrix |
| [segment_codec_zstd.md](segment_codec_zstd.md) | Zstd codec implementation |
| [zstd_in_unified_data_plane.md](zstd_in_unified_data_plane.md) | Zstd integration in data plane |
| [streaming_codec_negotiation.md](streaming_codec_negotiation.md) | Codec negotiation for streaming |

## Replication & Failover

| Document | Description |
|----------|-------------|
| [replication_delta_lsn.md](replication_delta_lsn.md) | Delta-LSN replication protocol |
| [replication_integrity.md](replication_integrity.md) | Replication integrity invariants |
| [replication_segment_streaming.md](replication_segment_streaming.md) | Segment-level streaming replication |
| [failover_behavior.md](failover_behavior.md) | Failover behavior and guarantees |
| [failover_determinism_report.md](failover_determinism_report.md) | Failover determinism analysis |
| [failover_partition_sla.md](failover_partition_sla.md) | Partition tolerance and SLA |
| [rpo_rto.md](rpo_rto.md) | RPO/RTO targets and evidence |
| [self_healing.md](self_healing.md) | Self-healing mechanisms |
| [rolling_upgrade.md](rolling_upgrade.md) | Rolling upgrade procedures |

## Consistency & Transactions

| Document | Description |
|----------|-------------|
| [CONSISTENCY.md](CONSISTENCY.md) | Consistency model and invariants |
| [csn_lsn_decoupling.md](csn_lsn_decoupling.md) | CSN/LSN decoupling design |
| [gc_safety_model.md](gc_safety_model.md) | GC safety model and safepoints |
| [crash_point_matrix.md](crash_point_matrix.md) | Crash-point analysis matrix |

## Memory Management

| Document | Description |
|----------|-------------|
| [memory_backpressure.md](memory_backpressure.md) | Memory backpressure mechanisms |
| [memory_compression.md](memory_compression.md) | In-memory compression |
| [memory_pressure_spill.md](memory_pressure_spill.md) | Memory pressure and spill-to-disk |

## Protocol & Compatibility

| Document | Description |
|----------|-------------|
| [sql_compatibility.md](sql_compatibility.md) | SQL dialect compatibility matrix |
| [protocol_compatibility.md](protocol_compatibility.md) | PostgreSQL wire protocol compatibility |
| [wire_compatibility.md](wire_compatibility.md) | Wire-level compatibility details |
| [compatibility_contract.md](compatibility_contract.md) | Compatibility contract and guarantees |
| [native_protocol.md](native_protocol.md) | FalconDB native binary protocol |
| [native_protocol_compat.md](native_protocol_compat.md) | Native protocol compatibility |
| [jdbc_connection.md](jdbc_connection.md) | JDBC driver connection guide |
| [orm_compat_smoke.md](orm_compat_smoke.md) | ORM compatibility smoke tests |
| [extended_scalar_functions.md](extended_scalar_functions.md) | Extended scalar function reference |
| [show_commands_schema.md](show_commands_schema.md) | SHOW command schema reference |
| [ga_sql_boundary.md](ga_sql_boundary.md) | GA SQL feature boundary |

## Security

| Document | Description |
|----------|-------------|
| [security.md](security.md) | Security architecture (AuthN, TLS, audit) |
| [security_model.md](security_model.md) | Enterprise security model (AuthN/AuthZ/TLS/Audit) |
| [rbac.md](rbac.md) | RBAC permission model |
| [cluster_access_model.md](cluster_access_model.md) | Cluster access control model |

## Operations & Runbooks

| Document | Description |
|----------|-------------|
| [ops_playbook.md](ops_playbook.md) | Comprehensive operations playbook |
| [ops_runbook.md](ops_runbook.md) | Operational runbook (common procedures) |
| [ops_runbook_enterprise.md](ops_runbook_enterprise.md) | Enterprise operations runbook |
| [falconctl_ops.md](falconctl_ops.md) | falconctl operations reference |
| [observability.md](observability.md) | Observability, metrics, and tracing |
| [postmortem.md](postmortem.md) | Postmortem template and process |
| [backup_restore.md](backup_restore.md) | Backup and restore with PITR |
| [sla_slo.md](sla_slo.md) | SLA/SLO definitions |

## Cluster & Enterprise

| Document | Description |
|----------|-------------|
| [enterprise_architecture.md](enterprise_architecture.md) | Enterprise architecture overview |
| [control_plane.md](control_plane.md) | Control plane operations |
| [distributed_hardening.md](distributed_hardening.md) | Distributed systems hardening |
| [gateway_behavior.md](gateway_behavior.md) | Query gateway/router behavior |
| [chaos_matrix.md](chaos_matrix.md) | Chaos testing matrix |

## Performance

| Document | Description |
|----------|-------------|
| [performance_baseline.md](performance_baseline.md) | Performance baseline measurements |
| [performance_guardrails.md](performance_guardrails.md) | Performance guardrails and limits |
| [perf_testing.md](perf_testing.md) | Performance testing methodology |
| [perf_prepared_v1x.md](perf_prepared_v1x.md) | Prepared statement performance |
| [latency_evidence_pack.md](latency_evidence_pack.md) | Latency evidence and benchmarks |
| [degradation_curve.md](degradation_curve.md) | Degradation curve analysis |
| [cost_capacity.md](cost_capacity.md) | Cost and capacity planning |

## Release & Readiness

| Document | Description |
|----------|-------------|
| [v1.0_scope.md](v1.0_scope.md) | v1.0 scope and feature checklist |
| [ga_release_checklist.md](ga_release_checklist.md) | GA release checklist |
| [ga_hardening.md](ga_hardening.md) | GA hardening items |
| [production_readiness.md](production_readiness.md) | Production readiness checklist |
| [production_readiness_report.md](production_readiness_report.md) | Production readiness report |
| [stability_report_v104.md](stability_report_v104.md) | Stability report v1.0.4 |
| [stability_report_v1x.md](stability_report_v1x.md) | Stability report v1.x |
| [feature_gap_analysis.md](feature_gap_analysis.md) | Feature gap analysis |
| [crate_audit_report.md](crate_audit_report.md) | Crate dependency audit |
| [operability_baseline.md](operability_baseline.md) | Operability baseline |

## Commercial & Industry

| Document | Description |
|----------|-------------|
| [commercial_model.md](commercial_model.md) | Commercial licensing model |
| [industry_edition_overview.md](industry_edition_overview.md) | Industry edition overview |
| [industry_focus.md](industry_focus.md) | Industry-specific features |
| [industry_poc_playbook.md](industry_poc_playbook.md) | Industry POC playbook |
| [industry_sla.md](industry_sla.md) | Industry SLA requirements |
| [roadmap.md](roadmap.md) | Product roadmap |
| [phase1_plan.md](phase1_plan.md) | Phase 1 implementation plan |

## Superseded Documents

These files are superseded by newer, more complete versions:

| Superseded | Replaced by |
|------------|-------------|
| `unified_data_plane.md` | `unified_data_plane_full.md` |
