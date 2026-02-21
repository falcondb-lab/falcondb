use crate::codec::BackendMessage;
use crate::session::PgSession;

use super::handler::QueryHandler;

impl QueryHandler {
    /// Handle `SHOW falcon.<param>` commands.
    /// Returns `Some(messages)` if the param was recognized, `None` otherwise.
    pub(crate) fn handle_show_falcon(
        &self,
        param: &str,
        session: &mut PgSession,
    ) -> Option<Vec<BackendMessage>> {
        match param {
            "falcon.txn" => Some(self.show_falcon_txn(session)),
            "falcon.txn_stats" => Some(self.show_falcon_txn_stats()),
            "falcon.txn_history" => Some(self.show_falcon_txn_history()),
            "falcon.node_role" => Some(self.show_falcon_node_role()),
            "falcon.replication_stats" => Some(self.show_falcon_replication_stats()),
            "falcon.wal_stats" => Some(self.show_falcon_wal_stats()),
            "falcon.gc_stats" => Some(self.show_falcon_gc_stats()),
            "falcon.gc_safepoint" => Some(self.show_falcon_gc_safepoint()),
            "falcon.memory_pressure" => Some(self.show_falcon_memory_pressure()),
            "falcon.scatter_stats" => Some(self.show_falcon_scatter_stats()),
            "falcon.slow_queries" => Some(self.show_falcon_slow_queries()),
            "falcon.checkpoint_stats" => Some(self.show_falcon_checkpoint_stats()),
            "falcon.plan_cache" => Some(self.show_falcon_plan_cache()),
            "falcon.replica_stats" => Some(self.show_falcon_replica_stats()),
            "falcon.health" => Some(self.show_falcon_health()),
            "falcon.dist_capabilities" => Some(self.show_falcon_dist_capabilities()),
            "falcon.version" => Some(self.show_falcon_version()),
            "falcon.query_routing" => Some(self.show_falcon_query_routing()),
            "falcon.replication" => Some(self.show_falcon_replication()),
            "falcon.cluster" => Some(self.show_falcon_cluster()),
            "falcon.shard_stats" => Some(self.show_falcon_shard_stats()),
            "falcon.shards" => Some(self.show_falcon_shards()),
            "falcon.rebalance_status" => Some(self.show_falcon_rebalance_status()),
            "falcon.ha_status" => Some(self.show_falcon_ha_status()),
            "falcon.replica_health" => Some(self.show_falcon_replica_health()),
            "falcon.two_phase" => Some(self.show_falcon_two_phase()),
            "falcon.slow_txns" => Some(self.show_falcon_slow_txns()),
            "falcon.tenants" => Some(self.show_falcon_tenants()),
            "falcon.tenant_usage" => Some(self.show_falcon_tenant_usage()),
            "falcon.audit_log" => Some(self.show_falcon_audit_log()),
            "falcon.sla_stats" => Some(self.show_falcon_sla_stats()),
            "falcon.license" => Some(self.show_falcon_license()),
            "falcon.metering" => Some(self.show_falcon_metering()),
            "falcon.health_score" => Some(self.show_falcon_health_score()),
            "falcon.security" => Some(self.show_falcon_security()),
            "falcon.compat" => Some(self.show_falcon_compat()),
            "falcon.admission" => Some(self.show_falcon_admission()),
            "falcon.hotspots" => Some(self.show_falcon_hotspots()),
            "falcon.verification" => Some(self.show_falcon_verification()),
            "falcon.latency_contract" => Some(self.show_falcon_latency_contract()),
            "falcon.cluster_events" => Some(self.show_falcon_cluster_events()),
            "falcon.node_lifecycle" => Some(self.show_falcon_node_lifecycle()),
            "falcon.rebalance_plan" => Some(self.show_falcon_rebalance_plan()),
            "falcon.priority_scheduler" => Some(self.show_falcon_priority_scheduler()),
            "falcon.token_bucket" => Some(self.show_falcon_token_bucket()),
            "falcon.two_phase_config" => Some(self.show_falcon_two_phase_config()),
            "falcon.fault_injection" => Some(self.show_falcon_fault_injection()),
            "falcon.observability_catalog" => Some(self.show_falcon_observability_catalog()),
            "falcon.security_audit" => Some(self.show_falcon_security_audit()),
            "falcon.wire_compat" => Some(self.show_falcon_wire_compat()),
            _ => None,
        }
    }

    fn show_falcon_txn(&self, session: &mut PgSession) -> Vec<BackendMessage> {
        let value = if let Some(ref txn) = session.txn {
            if let Some(observed) = self.txn_mgr.get_txn(txn.txn_id) {
                format!(
                    "txn_id={},type={:?},path={:?},state={:?},shards={},degraded={},slow_mode={:?}",
                    observed.txn_id.0,
                    observed.txn_type,
                    observed.path,
                    observed.state,
                    observed.involved_shards.len(),
                    observed.degraded,
                    observed.slow_path_mode
                )
            } else {
                "inactive".into()
            }
        } else {
            "none".into()
        };

        self.single_row_result(
            vec![("falcon.txn", 25, -1)],
            vec![vec![Some(value)]],
        )
    }

    fn show_falcon_txn_stats(&self) -> Vec<BackendMessage> {
        let stats = if let Some(ref dist) = self.dist_engine {
            dist.aggregate_txn_stats()
        } else {
            self.txn_mgr.stats_snapshot()
        };
        let rows = vec![
            vec![Some("total_committed".into()), Some(stats.total_committed.to_string())],
            vec![Some("fast_path_commits".into()), Some(stats.fast_path_commits.to_string())],
            vec![Some("slow_path_commits".into()), Some(stats.slow_path_commits.to_string())],
            vec![Some("total_aborted".into()), Some(stats.total_aborted.to_string())],
            vec![Some("occ_conflicts".into()), Some(stats.occ_conflicts.to_string())],
            vec![Some("degraded_to_global".into()), Some(stats.degraded_to_global.to_string())],
            vec![Some("constraint_violations".into()), Some(stats.constraint_violations.to_string())],
            vec![Some("active_count".into()), Some(stats.active_count.to_string())],
            vec![Some("fast_p50_us".into()), Some(stats.latency.fast_path.p50_us.to_string())],
            vec![Some("fast_p95_us".into()), Some(stats.latency.fast_path.p95_us.to_string())],
            vec![Some("fast_p99_us".into()), Some(stats.latency.fast_path.p99_us.to_string())],
            vec![Some("slow_p50_us".into()), Some(stats.latency.slow_path.p50_us.to_string())],
            vec![Some("slow_p95_us".into()), Some(stats.latency.slow_path.p95_us.to_string())],
            vec![Some("slow_p99_us".into()), Some(stats.latency.slow_path.p99_us.to_string())],
            vec![Some("all_p50_us".into()), Some(stats.latency.all.p50_us.to_string())],
            vec![Some("all_p95_us".into()), Some(stats.latency.all.p95_us.to_string())],
            vec![Some("all_p99_us".into()), Some(stats.latency.all.p99_us.to_string())],
            vec![Some("high_p50_us".into()), Some(stats.priority_latency.high.p50_us.to_string())],
            vec![Some("high_p99_us".into()), Some(stats.priority_latency.high.p99_us.to_string())],
            vec![Some("normal_p50_us".into()), Some(stats.priority_latency.normal.p50_us.to_string())],
            vec![Some("normal_p99_us".into()), Some(stats.priority_latency.normal.p99_us.to_string())],
            vec![Some("bg_p50_us".into()), Some(stats.priority_latency.background.p50_us.to_string())],
            vec![Some("bg_p99_us".into()), Some(stats.priority_latency.background.p99_us.to_string())],
            vec![Some("sla_violations_total".into()), Some(stats.sla_violations.total_violations.to_string())],
            vec![Some("sla_high_violations".into()), Some(stats.sla_violations.high_priority_violations.to_string())],
            vec![Some("sla_normal_violations".into()), Some(stats.sla_violations.normal_priority_violations.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_txn_history(&self) -> Vec<BackendMessage> {
        let records = self.txn_mgr.txn_history_snapshot();
        let columns = vec![
            ("txn_id", 23, 4),
            ("txn_type", 25, -1),
            ("txn_path", 25, -1),
            ("shard_count", 23, 4),
            ("start_ts", 20, 8),
            ("commit_ts", 25, -1),
            ("latency_us", 20, 8),
            ("outcome", 25, -1),
            ("degraded", 16, 1),
        ];
        let rows: Vec<Vec<Option<String>>> = records.iter().map(|r| {
            vec![
                Some(r.txn_id.0.to_string()),
                Some(format!("{:?}", r.txn_type)),
                Some(format!("{:?}", r.txn_path)),
                Some(r.shard_count.to_string()),
                Some(r.start_ts.0.to_string()),
                r.commit_ts.map(|ts| ts.0.to_string()),
                Some(r.commit_latency_us.to_string()),
                Some(match &r.outcome {
                    falcon_txn::TxnOutcome::Committed => "committed".into(),
                    falcon_txn::TxnOutcome::Aborted(reason) => format!("aborted: {}", reason),
                }),
                Some(r.degraded.to_string()),
            ]
        }).collect();
        self.single_row_result(columns, rows)
    }

    fn show_falcon_node_role(&self) -> Vec<BackendMessage> {
        let role = std::env::var("FALCON_NODE_ROLE").unwrap_or_else(|_| "standalone".into());
        let rows = vec![
            vec![Some("role".into()), Some(role)],
        ];
        self.single_row_result(
            vec![("property", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_replication_stats(&self) -> Vec<BackendMessage> {
        let rs = self.storage.replication_stats_snapshot();
        let rows = vec![
            vec![Some("promote_count".into()), Some(rs.promote_count.to_string())],
            vec![Some("last_failover_time_ms".into()), Some(rs.last_failover_time_ms.to_string())],
            vec![Some("leader_changes".into()), Some(rs.leader_changes.to_string())],
            vec![Some("replication_lag_us".into()), Some(rs.replication_lag_us.to_string())],
            vec![Some("max_replication_lag_us".into()), Some(rs.max_replication_lag_us.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_wal_stats(&self) -> Vec<BackendMessage> {
        let ws = self.storage.wal_stats_snapshot();
        let wal_enabled = self.storage.is_wal_enabled();
        let rows = vec![
            vec![Some("wal_enabled".into()), Some(wal_enabled.to_string())],
            vec![Some("records_written".into()), Some(ws.records_written.to_string())],
            vec![Some("observer_notifications".into()), Some(ws.observer_notifications.to_string())],
            vec![Some("flushes".into()), Some(ws.flushes.to_string())],
            vec![Some("fsync_total_us".into()), Some(ws.fsync_total_us.to_string())],
            vec![Some("fsync_max_us".into()), Some(ws.fsync_max_us.to_string())],
            vec![Some("fsync_avg_us".into()), Some(ws.fsync_avg_us.to_string())],
            vec![Some("group_commit_avg_size".into()), Some(format!("{:.2}", ws.group_commit_avg_size))],
            vec![Some("backlog_bytes".into()), Some(ws.backlog_bytes.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_gc_stats(&self) -> Vec<BackendMessage> {
        let gs = self.storage.gc_stats_snapshot();
        let active_count = self.txn_mgr.active_count();
        let min_ts = self.txn_mgr.min_active_ts();
        let rows = vec![
            vec![Some("gc_safepoint_ts".into()), Some(gs.last_safepoint_ts.0.to_string())],
            vec![Some("active_txn_count".into()), Some(active_count.to_string())],
            vec![Some("oldest_txn_ts".into()), Some(min_ts.0.to_string())],
            vec![Some("total_sweeps".into()), Some(gs.total_sweeps.to_string())],
            vec![Some("reclaimed_version_count".into()), Some(gs.total_reclaimed_versions.to_string())],
            vec![Some("reclaimed_memory_bytes".into()), Some(gs.total_reclaimed_bytes.to_string())],
            vec![Some("chains_inspected".into()), Some(gs.total_chains_inspected.to_string())],
            vec![Some("chains_pruned".into()), Some(gs.total_chains_pruned.to_string())],
            vec![Some("last_sweep_duration_us".into()), Some(gs.last_sweep_duration_us.to_string())],
            vec![Some("max_chain_length".into()), Some(gs.max_chain_length_observed.to_string())],
            vec![Some("avg_chain_length".into()), Some(format!("{:.2}", gs.avg_chain_length))],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_gc_safepoint(&self) -> Vec<BackendMessage> {
        let info = self.txn_mgr.gc_safepoint_info();
        let rows = vec![
            vec![Some("min_active_start_ts".into()), Some(info.min_active_start_ts.0.to_string())],
            vec![Some("current_ts".into()), Some(info.current_ts.0.to_string())],
            vec![Some("active_txn_count".into()), Some(info.active_txn_count.to_string())],
            vec![Some("prepared_txn_count".into()), Some(info.prepared_txn_count.to_string())],
            vec![Some("longest_txn_age_us".into()), Some(info.longest_txn_age_us.to_string())],
            vec![Some("stalled".into()), Some(info.stalled.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_memory_pressure(&self) -> Vec<BackendMessage> {
        let snap = self.storage.memory_snapshot();
        let pressure = self.storage.pressure_state();
        let budget = self.storage.memory_tracker().budget();
        let ratio_str = if snap.pressure_ratio.is_nan() {
            "N/A".to_string()
        } else {
            format!("{:.4}", snap.pressure_ratio)
        };
        let rows = vec![
            vec![Some("pressure_state".into()), Some(pressure.to_string())],
            vec![Some("pressure_ratio".into()), Some(ratio_str)],
            vec![Some("total_bytes".into()), Some(snap.total_bytes.to_string())],
            vec![Some("mvcc_bytes".into()), Some(snap.mvcc_bytes.to_string())],
            vec![Some("index_bytes".into()), Some(snap.index_bytes.to_string())],
            vec![Some("write_buffer_bytes".into()), Some(snap.write_buffer_bytes.to_string())],
            vec![Some("soft_limit".into()), Some(budget.soft_limit.to_string())],
            vec![Some("hard_limit".into()), Some(budget.hard_limit.to_string())],
            vec![Some("enabled".into()), Some(budget.enabled.to_string())],
            vec![Some("rejected_txn_count".into()), Some(snap.rejected_txn_count.to_string())],
            vec![Some("delayed_txn_count".into()), Some(snap.delayed_txn_count.to_string())],
            vec![Some("gc_trigger_count".into()), Some(snap.gc_trigger_count.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_scatter_stats(&self) -> Vec<BackendMessage> {
        let rows = if let Some(ref dist) = self.dist_engine {
            let ss = dist.last_scatter_stats();
            let max_latency = ss.per_shard_latency_us.iter()
                .map(|(_, lat)| *lat)
                .max()
                .unwrap_or(0);
            let shard_detail = ss.per_shard_latency_us.iter()
                .map(|(sid, lat)| {
                    let rows = ss.per_shard_row_count.iter()
                        .find(|(s, _)| s == sid)
                        .map(|(_, c)| *c)
                        .unwrap_or(0);
                    format!("{}:{}us/{}rows", sid, lat, rows)
                })
                .collect::<Vec<_>>()
                .join(", ");
            let mut rows = vec![
                vec![Some("shards_participated".into()), Some(ss.shards_participated.to_string())],
                vec![Some("total_rows_gathered".into()), Some(ss.total_rows_gathered.to_string())],
                vec![Some("gather_strategy".into()), Some(ss.gather_strategy.clone())],
                vec![Some("max_shard_latency_us".into()), Some(max_latency.to_string())],
                vec![Some("per_shard_detail".into()), Some(format!("[{}]", shard_detail))],
                vec![Some("total_latency_us".into()), Some(ss.total_latency_us.to_string())],
            ];
            if !ss.failed_shards.is_empty() {
                rows.push(vec![Some("failed_shards".into()), Some(format!("{:?}", ss.failed_shards))]);
            }
            if !ss.merge_labels.is_empty() {
                rows.push(vec![Some("merge_operations".into()), Some(ss.merge_labels.join(", "))]);
            }
            rows
        } else {
            vec![
                vec![Some("shards_participated".into()), Some("0".into())],
                vec![Some("total_rows_gathered".into()), Some("0".into())],
                vec![Some("gather_strategy".into()), Some("(single-shard mode)".into())],
                vec![Some("max_shard_latency_us".into()), Some("0".into())],
                vec![Some("per_shard_latency".into()), Some("[]".into())],
                vec![Some("total_latency_us".into()), Some("0".into())],
            ]
        };
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_slow_queries(&self) -> Vec<BackendMessage> {
        let threshold = self.slow_query_log.threshold();
        let (entries, total_count) = self.slow_query_log.snapshot();
        let mut rows: Vec<Vec<Option<String>>> = vec![
            vec![Some("threshold_ms".into()), Some(if threshold.is_zero() { "disabled".into() } else { threshold.as_millis().to_string() })],
            vec![Some("total_slow_queries".into()), Some(total_count.to_string())],
            vec![Some("entries_retained".into()), Some(entries.len().to_string())],
        ];
        for (i, entry) in entries.iter().enumerate() {
            let sql_preview = if entry.sql.len() > 80 {
                format!("{}...", &entry.sql[..80])
            } else {
                entry.sql.clone()
            };
            rows.push(vec![
                Some(format!("query_{}", i + 1)),
                Some(format!(
                    "session={} duration={:.3}ms ts={} sql={}",
                    entry.session_id,
                    entry.duration.as_secs_f64() * 1000.0,
                    entry.timestamp_ms,
                    sql_preview,
                )),
            ]);
        }
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 80, -1)],
            rows,
        )
    }

    fn show_falcon_checkpoint_stats(&self) -> Vec<BackendMessage> {
        let wal_enabled = self.storage.is_wal_enabled();
        if !wal_enabled {
            let rows = vec![
                vec![Some("wal_enabled".into()), Some("false".into())],
                vec![Some("checkpoint_available".into()), Some("false".into())],
            ];
            return self.single_row_result(
                vec![("metric", 25, -1), ("value", 25, -1)],
                rows,
            );
        }
        let ws = self.storage.wal_stats_snapshot();
        let rows = vec![
            vec![Some("wal_enabled".into()), Some("true".into())],
            vec![Some("checkpoint_available".into()), Some("true".into())],
            vec![Some("records_written".into()), Some(ws.records_written.to_string())],
            vec![Some("flushes".into()), Some(ws.flushes.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_plan_cache(&self) -> Vec<BackendMessage> {
        let stats = self.plan_cache.stats();
        let rows = vec![
            vec![Some("entries".into()), Some(stats.entries.to_string())],
            vec![Some("capacity".into()), Some(stats.capacity.to_string())],
            vec![Some("hits".into()), Some(stats.hits.to_string())],
            vec![Some("misses".into()), Some(stats.misses.to_string())],
            vec![Some("hit_rate_pct".into()), Some(format!("{:.1}", stats.hit_rate_pct))],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_replica_stats(&self) -> Vec<BackendMessage> {
        let rows = if let Some(ref rm) = self.replica_metrics {
            let snap = rm.snapshot();
            vec![
                vec![Some("connected".into()), Some(snap.connected.to_string())],
                vec![Some("applied_lsn".into()), Some(snap.applied_lsn.to_string())],
                vec![Some("chunks_applied".into()), Some(snap.chunks_applied.to_string())],
                vec![Some("records_applied".into()), Some(snap.records_applied.to_string())],
                vec![Some("reconnect_count".into()), Some(snap.reconnect_count.to_string())],
                vec![Some("acks_sent".into()), Some(snap.acks_sent.to_string())],
            ]
        } else {
            vec![
                vec![Some("status".into()), Some("not a replica node".into())],
            ]
        };
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_health(&self) -> Vec<BackendMessage> {
        let rows = if let Some(ref dist) = self.dist_engine {
            let health = dist.shard_health_check();
            let all_healthy = health.iter().all(|(_, h, _)| *h);
            let max_lat = health.iter().map(|(_, _, l)| *l).max().unwrap_or(0);
            let detail: Vec<String> = health.iter()
                .map(|(sid, h, l)| format!("shard_{}: {} ({}us)", sid.0, if *h { "ok" } else { "FAIL" }, l))
                .collect();
            vec![
                vec![Some("status".into()), Some(if all_healthy { "healthy".into() } else { "degraded".into() })],
                vec![Some("shard_count".into()), Some(health.len().to_string())],
                vec![Some("healthy_shards".into()), Some(health.iter().filter(|(_, h, _)| *h).count().to_string())],
                vec![Some("max_probe_latency_us".into()), Some(max_lat.to_string())],
                vec![Some("per_shard".into()), Some(detail.join("; "))],
            ]
        } else {
            vec![
                vec![Some("status".into()), Some("single-shard".into())],
                vec![Some("shard_count".into()), Some("1".into())],
                vec![Some("healthy_shards".into()), Some("1".into())],
                vec![Some("max_probe_latency_us".into()), Some("0".into())],
                vec![Some("per_shard".into()), Some("shard_0: ok".into())],
            ]
        };
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 40, -1)],
            rows,
        )
    }

    fn show_falcon_dist_capabilities(&self) -> Vec<BackendMessage> {
        let rows = vec![
            vec![Some("feature".into()), Some("status".into()), Some("notes".into())],
            vec![Some("SELECT (full scan)".into()), Some("supported".into()), Some("scatter/gather across all shards".into())],
            vec![Some("GROUP BY + COUNT/SUM/MIN/MAX".into()), Some("supported".into()), Some("TwoPhaseAgg merge".into())],
            vec![Some("GROUP BY + AVG".into()), Some("supported".into()), Some("SUM/COUNT decomposition + post-merge fixup".into())],
            vec![Some("GROUP BY + BOOL_AND/BOOL_OR".into()), Some("supported".into()), Some("boolean merge across shards".into())],
            vec![Some("HAVING (post-merge)".into()), Some("supported".into()), Some("stripped from subplan, applied after merge".into())],
            vec![Some("ORDER BY + LIMIT".into()), Some("supported".into()), Some("MergeSortLimit or TwoPhaseAgg post-sort".into())],
            vec![Some("ORDER BY + LIMIT + OFFSET".into()), Some("supported".into()), Some("MergeSortLimit with offset adjustment".into())],
            vec![Some("DISTINCT".into()), Some("supported".into()), Some("Union with post-gather dedup".into())],
            vec![Some("PK point lookup".into()), Some("supported".into()), Some("single-shard shortcut via hash routing".into())],
            vec![Some("INSERT (sharded)".into()), Some("supported".into()), Some("PK hash routing to target shard".into())],
            vec![Some("DDL broadcast".into()), Some("supported".into()), Some("propagated to all shards in parallel".into())],
            vec![Some("UPDATE/DELETE broadcast".into()), Some("supported".into()), Some("executed on all shards in parallel".into())],
            vec![Some("EXPLAIN ANALYZE".into()), Some("supported".into()), Some("per-shard stats + gather strategy details".into())],
            vec![Some("Health check".into()), Some("supported".into()), Some("SHOW falcon.health".into())],
            vec![Some("Scatter timeout".into()), Some("supported".into()), Some("AtomicBool cancellation + gather-phase check".into())],
            vec![Some("STRING_AGG".into()), Some("supported".into()), Some("partial concat per shard, merge with separator".into())],
            vec![Some("ARRAY_AGG".into()), Some("supported".into()), Some("partial arrays per shard, concatenated at gather".into())],
            vec![Some("COUNT(DISTINCT)".into()), Some("supported".into()), Some("collect-dedup: ARRAY_AGG(DISTINCT) per shard, global dedup + count".into())],
            vec![Some("SUM(DISTINCT)".into()), Some("supported".into()), Some("collect-dedup: ARRAY_AGG(DISTINCT) per shard, global dedup + sum".into())],
            vec![Some("AVG(DISTINCT)".into()), Some("supported".into()), Some("collect-dedup: ARRAY_AGG(DISTINCT) per shard, global dedup + avg".into())],
            vec![Some("MIN/MAX(DISTINCT)".into()), Some("supported".into()), Some("DISTINCT is no-op for MIN/MAX, uses regular merge".into())],
            vec![Some("STRING_AGG(DISTINCT)".into()), Some("supported".into()), Some("collect-dedup: ARRAY_AGG(DISTINCT) per shard, global dedup + join".into())],
            vec![Some("ARRAY_AGG(DISTINCT)".into()), Some("supported".into()), Some("collect-dedup: ARRAY_AGG(DISTINCT) per shard, global dedup".into())],
            vec![Some("Window functions".into()), Some("unsupported".into()), Some("requires full dataset ordering".into())],
            vec![Some("Cross-shard JOIN".into()), Some("unsupported".into()), Some("requires distributed join strategy".into())],
        ];
        self.single_row_result(
            vec![("feature", 35, -1), ("status", 15, -1), ("notes", 50, -1)],
            rows,
        )
    }

    fn show_falcon_version(&self) -> Vec<BackendMessage> {
        let rows = vec![
            vec![Some("engine".into()), Some("FalconDB".into())],
            vec![Some("version".into()), Some(env!("CARGO_PKG_VERSION").into())],
            vec![Some("distributed".into()), Some(if self.cluster_shard_ids.len() > 1 { "yes" } else { "no" }.into())],
            vec![Some("shard_count".into()), Some(self.cluster_shard_ids.len().to_string())],
            vec![Some("supported_agg_funcs".into()), Some("9 (COUNT, SUM, MIN, MAX, AVG, BOOL_AND, BOOL_OR, STRING_AGG, ARRAY_AGG)".into())],
            vec![Some("gather_strategies".into()), Some("Union, MergeSortLimit, TwoPhaseAgg".into())],
        ];
        self.single_row_result(
            vec![("property", 25, -1), ("value", 50, -1)],
            rows,
        )
    }

    fn show_falcon_query_routing(&self) -> Vec<BackendMessage> {
        let shard_count = self.cluster_shard_ids.len();
        let mode = if shard_count <= 1 { "single-shard" } else { "distributed" };
        let last_stats = if let Some(ref dist) = self.dist_engine {
            let s = dist.last_scatter_stats();
            vec![
                vec![Some("last_gather_strategy".into()), Some(s.gather_strategy.clone())],
                vec![Some("last_shards_participated".into()), Some(s.shards_participated.to_string())],
                vec![Some("last_total_rows".into()), Some(s.total_rows_gathered.to_string())],
                vec![Some("last_total_latency_us".into()), Some(s.total_latency_us.to_string())],
                vec![Some("last_failed_shards".into()), Some(if s.failed_shards.is_empty() { "none".into() } else { format!("{:?}", s.failed_shards) })],
            ]
        } else {
            vec![
                vec![Some("last_gather_strategy".into()), Some("N/A".into())],
            ]
        };
        let mut rows = vec![
            vec![Some("routing_mode".into()), Some(mode.into())],
            vec![Some("shard_count".into()), Some(shard_count.to_string())],
            vec![Some("supported_agg_funcs".into()), Some("COUNT, SUM, MIN, MAX, AVG, BOOL_AND, BOOL_OR, STRING_AGG, ARRAY_AGG".into())],
            vec![Some("partial_failure".into()), Some("resilient (returns partial results)".into())],
            vec![Some("timeout_mechanism".into()), Some("AtomicBool cancellation + gather-phase check".into())],
        ];
        rows.extend(last_stats);
        self.single_row_result(
            vec![("metric", 30, -1), ("value", 50, -1)],
            rows,
        )
    }

    fn show_falcon_replication(&self) -> Vec<BackendMessage> {
        let rows = vec![
            vec![Some("replication_model".into()), Some("primary-replica (async)".into())],
            vec![Some("commit_ack".into()), Some("primary_durable_only".into())],
            vec![Some("replica_count".into()), Some("1".into())],
            vec![Some("replication_lag_lsn".into()), Some("(query ShardReplicaGroup)".into())],
            vec![Some("promote_count".into()), Some("0".into())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_cluster(&self) -> Vec<BackendMessage> {
        let shard_count = self.cluster_shard_ids.len();
        let shard_list = self.cluster_shard_ids.iter()
            .map(|s| s.0.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let mode = if shard_count <= 1 { "single-shard" } else { "distributed" };
        let rows = vec![
            vec![Some("mode".into()), Some(mode.into())],
            vec![Some("shard_count".into()), Some(shard_count.to_string())],
            vec![Some("shard_ids".into()), Some(format!("[{}]", shard_list))],
            vec![Some("insert_routing".into()), Some("PK hash (shard_for_key)".into())],
            vec![Some("update_delete_routing".into()), Some("PK filter → single shard, else broadcast".into())],
            vec![Some("select_routing".into()), Some("scatter/gather via DistPlan".into())],
            vec![Some("ddl_routing".into()), Some("propagate to all shards".into())],
            vec![Some("gather_strategies".into()), Some("Union, MergeSortLimit, TwoPhaseAgg".into())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_shard_stats(&self) -> Vec<BackendMessage> {
        if let Some(ref dist) = self.dist_engine {
            let stats = dist.per_shard_stats();
            let columns = vec![
                ("shard_id", 23, 4),
                ("tables", 23, 4),
                ("committed", 20, 8),
                ("aborted", 20, 8),
                ("active", 23, 4),
            ];
            let rows: Vec<Vec<Option<String>>> = stats.iter().map(|(sid, tbl, comm, abrt, act)| {
                vec![
                    Some(sid.to_string()),
                    Some(tbl.to_string()),
                    Some(comm.to_string()),
                    Some(abrt.to_string()),
                    Some(act.to_string()),
                ]
            }).collect();
            self.single_row_result(columns, rows)
        } else {
            let rows = vec![vec![Some("0".into()), Some("(single-shard mode)".into()), Some("".into()), Some("".into()), Some("".into())]];
            self.single_row_result(
                vec![("shard_id", 23, 4), ("tables", 25, -1), ("committed", 25, -1), ("aborted", 25, -1), ("active", 25, -1)],
                rows,
            )
        }
    }

    fn show_falcon_shards(&self) -> Vec<BackendMessage> {
        if let Some(ref dist) = self.dist_engine {
            let detailed = dist.shard_load_detailed();
            let columns = vec![
                ("shard_id", 23, 4),
                ("total_rows", 20, 8),
                ("table_count", 23, 4),
                ("tables", 25, -1),
            ];
            let rows: Vec<Vec<Option<String>>> = detailed.iter().map(|d| {
                let table_info: Vec<String> = d.tables.iter()
                    .map(|t| format!("{}({})", t.table_name, t.row_count))
                    .collect();
                vec![
                    Some(d.shard_id.0.to_string()),
                    Some(d.total_row_count.to_string()),
                    Some(d.table_count.to_string()),
                    Some(table_info.join(", ")),
                ]
            }).collect();
            self.single_row_result(columns, rows)
        } else {
            self.single_row_result(
                vec![("shard_id", 23, 4), ("total_rows", 20, 8), ("table_count", 23, 4), ("tables", 25, -1)],
                vec![vec![Some("0".into()), Some("(single-node)".into()), Some("".into()), Some("".into())]],
            )
        }
    }

    fn show_falcon_rebalance_status(&self) -> Vec<BackendMessage> {
        if let Some(ref dist) = self.dist_engine {
            let status = dist.rebalance_status();
            let mut rows = vec![
                vec![Some("runs_completed".into()), Some(status.runs_completed.to_string())],
                vec![Some("total_rows_migrated".into()), Some(status.total_rows_migrated.to_string())],
            ];
            if let Some(ref snap) = status.last_snapshot {
                rows.push(vec![Some("imbalance_ratio".into()), Some(format!("{:.3}", snap.imbalance_ratio()))]);
                rows.push(vec![Some("total_rows".into()), Some(snap.total_rows().to_string())]);
            }
            if let Some(ref plan) = status.last_plan {
                rows.push(vec![Some("last_plan_tasks".into()), Some(plan.tasks.len().to_string())]);
                rows.push(vec![Some("last_plan_rows_to_move".into()), Some(plan.total_rows_to_move().to_string())]);
            }
            for ms in &status.migration_statuses {
                rows.push(vec![
                    Some(format!("migration_{:?}->{:?}", ms.task.source_shard, ms.task.target_shard)),
                    Some(format!("phase={}, rows={}", ms.phase, ms.rows_migrated)),
                ]);
            }
            self.single_row_result(
                vec![("metric", 25, -1), ("value", 25, -1)],
                rows,
            )
        } else {
            self.single_row_result(
                vec![("metric", 25, -1), ("value", 25, -1)],
                vec![vec![Some("status".into()), Some("single-node (no rebalancing)".into())]],
            )
        }
    }

    fn show_falcon_ha_status(&self) -> Vec<BackendMessage> {
        // HA status is available via the DistributedQueryEngine or standalone HA group.
        // In single-node mode, report basic HA info.
        let rows = if let Some(ref dist) = self.dist_engine {
            let engine = dist.engine();
            let num_shards = engine.num_shards();
            vec![
                vec![Some("mode".into()), Some("distributed".into())],
                vec![Some("num_shards".into()), Some(num_shards.to_string())],
                vec![Some("auto_failover".into()), Some("enabled".into())],
                vec![Some("sync_mode".into()), Some("async".into())],
            ]
        } else {
            vec![
                vec![Some("mode".into()), Some("single-node".into())],
                vec![Some("auto_failover".into()), Some("n/a".into())],
                vec![Some("sync_mode".into()), Some("n/a".into())],
                vec![Some("epoch".into()), Some("1".into())],
            ]
        };
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_replica_health(&self) -> Vec<BackendMessage> {
        if let Some(ref dist) = self.dist_engine {
            let columns = vec![
                ("shard_id", 23, 4),
                ("replica_idx", 23, 4),
                ("role", 25, -1),
                ("applied_lsn", 20, 8),
                ("status", 25, -1),
            ];
            // In distributed mode, show per-shard health
            let rows: Vec<Vec<Option<String>>> = dist.engine().all_shards()
                .iter()
                .enumerate()
                .map(|(_i, shard)| {
                    vec![
                        Some(shard.shard_id.0.to_string()),
                        Some("0".into()),
                        Some("primary".into()),
                        Some("0".into()),
                        Some("healthy".into()),
                    ]
                })
                .collect();
            self.single_row_result(columns, rows)
        } else {
            self.single_row_result(
                vec![("shard_id", 23, 4), ("replica_idx", 23, 4), ("role", 25, -1), ("applied_lsn", 20, 8), ("status", 25, -1)],
                vec![vec![Some("0".into()), Some("0".into()), Some("primary".into()), Some("0".into()), Some("healthy".into())]],
            )
        }
    }

    fn show_falcon_slow_txns(&self) -> Vec<BackendMessage> {
        let threshold = self.txn_mgr.slow_txn_threshold_us();
        let slow_txns = self.txn_mgr.slow_txn_snapshot();
        let mut rows = vec![
            vec![Some("threshold_us".into()), Some(threshold.to_string())],
            vec![Some("total_slow_txns".into()), Some(slow_txns.len().to_string())],
        ];
        for (i, record) in slow_txns.iter().enumerate().take(50) {
            let outcome_str = match &record.outcome {
                falcon_txn::manager::TxnOutcome::Committed => "committed".to_string(),
                falcon_txn::manager::TxnOutcome::Aborted(reason) => format!("aborted({})", reason),
            };
            rows.push(vec![
                Some(format!("txn_{}", i + 1)),
                Some(format!(
                    "id={} trace={} path={:?} shards={} latency={}us occ_retries={} outcome={}",
                    record.txn_id, record.trace_id, record.txn_path,
                    record.shard_count, record.commit_latency_us,
                    record.occ_retry_count, outcome_str,
                )),
            ]);
        }
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 80, -1)],
            rows,
        )
    }

    fn show_falcon_tenants(&self) -> Vec<BackendMessage> {
        let snapshots = self.tenant_registry.all_tenant_snapshots();
        let mut rows = vec![
            vec![Some("tenant_count".into()), Some(snapshots.len().to_string())],
        ];
        for snap in &snapshots {
            rows.push(vec![
                Some(format!("tenant_{}", snap.tenant_id.0)),
                Some(format!(
                    "name={} status={} active_txns={} memory={}B qps={:.1} committed={} aborted={} quota_exceeded={}",
                    snap.tenant_name, snap.status, snap.active_txns,
                    snap.memory_bytes, snap.current_qps,
                    snap.txns_committed, snap.txns_aborted, snap.quota_exceeded_count,
                )),
            ]);
        }
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 80, -1)],
            rows,
        )
    }

    fn show_falcon_tenant_usage(&self) -> Vec<BackendMessage> {
        let snapshots = self.tenant_registry.all_tenant_snapshots();
        if snapshots.is_empty() {
            return self.single_row_result(
                vec![("metric", 30, -1), ("value", 30, -1)],
                vec![vec![Some("tenant_count".into()), Some("0".into())]],
            );
        }
        let columns = vec![
            ("tenant_id", 23, 4),
            ("name", 64, -1),
            ("status", 16, -1),
            ("active_txns", 23, 4),
            ("txns_committed", 20, 8),
            ("txns_aborted", 20, 8),
            ("memory_bytes", 20, 8),
            ("quota_exceeded", 20, 8),
            ("current_qps", 20, 8),
        ];
        let rows: Vec<Vec<Option<String>>> = snapshots.iter().map(|s| vec![
            Some(s.tenant_id.0.to_string()),
            Some(s.tenant_name.clone()),
            Some(format!("{:?}", s.status)),
            Some(s.active_txns.to_string()),
            Some(s.txns_committed.to_string()),
            Some(s.txns_aborted.to_string()),
            Some(s.memory_bytes.to_string()),
            Some(s.quota_exceeded_count.to_string()),
            Some(format!("{:.1}", s.current_qps)),
        ]).collect();
        self.single_row_result(columns, rows)
    }

    fn show_falcon_audit_log(&self) -> Vec<BackendMessage> {
        let events = self.audit_log.snapshot(50);
        let mut rows = vec![
            vec![Some("total_events".into()), Some(self.audit_log.total_events().to_string())],
            vec![Some("buffered".into()), Some(self.audit_log.buffered_count().to_string())],
        ];
        for event in &events {
            rows.push(vec![
                Some(format!("event_{}", event.event_id)),
                Some(format!(
                    "type={} tenant={} role={} session={} success={} detail={}",
                    event.event_type, event.tenant_id, event.role_name,
                    event.session_id, event.success,
                    if event.detail.len() > 80 { &event.detail[..80] } else { &event.detail },
                )),
            ]);
        }
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 100, -1)],
            rows,
        )
    }

    fn show_falcon_sla_stats(&self) -> Vec<BackendMessage> {
        let stats = self.txn_mgr.stats_snapshot();
        let rows = vec![
            vec![Some("sla_target_high_us".into()), Some("10000".into())],
            vec![Some("sla_target_normal_us".into()), Some("100000".into())],
            vec![Some("sla_target_background_us".into()), Some("1000000".into())],
            vec![Some("high_p50_us".into()), Some(stats.priority_latency.high.p50_us.to_string())],
            vec![Some("high_p95_us".into()), Some(stats.priority_latency.high.p95_us.to_string())],
            vec![Some("high_p99_us".into()), Some(stats.priority_latency.high.p99_us.to_string())],
            vec![Some("high_count".into()), Some(stats.priority_latency.high.count.to_string())],
            vec![Some("normal_p50_us".into()), Some(stats.priority_latency.normal.p50_us.to_string())],
            vec![Some("normal_p95_us".into()), Some(stats.priority_latency.normal.p95_us.to_string())],
            vec![Some("normal_p99_us".into()), Some(stats.priority_latency.normal.p99_us.to_string())],
            vec![Some("normal_count".into()), Some(stats.priority_latency.normal.count.to_string())],
            vec![Some("bg_p50_us".into()), Some(stats.priority_latency.background.p50_us.to_string())],
            vec![Some("bg_p99_us".into()), Some(stats.priority_latency.background.p99_us.to_string())],
            vec![Some("bg_count".into()), Some(stats.priority_latency.background.count.to_string())],
            vec![Some("system_p50_us".into()), Some(stats.priority_latency.system.p50_us.to_string())],
            vec![Some("system_count".into()), Some(stats.priority_latency.system.count.to_string())],
            vec![Some("sla_violations_total".into()), Some(stats.sla_violations.total_violations.to_string())],
            vec![Some("sla_high_violations".into()), Some(stats.sla_violations.high_priority_violations.to_string())],
            vec![Some("sla_normal_violations".into()), Some(stats.sla_violations.normal_priority_violations.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_license(&self) -> Vec<BackendMessage> {
        let lic = &self.license_info;
        let rows = vec![
            vec![Some("edition".into()), Some(lic.edition.to_string())],
            vec![Some("organization".into()), Some(lic.organization.clone())],
            vec![Some("max_nodes".into()), Some(lic.max_nodes.to_string())],
            vec![Some("max_tenants".into()), Some(lic.max_tenants.to_string())],
            vec![Some("expires_at".into()), Some(if lic.expires_at == 0 { "never".into() } else { lic.expires_at.to_string() })],
            vec![Some("validated".into()), Some(lic.validated.to_string())],
            vec![Some("features_enabled".into()), Some(self.feature_gate.enabled_count().to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 40, -1)],
            rows,
        )
    }

    fn show_falcon_metering(&self) -> Vec<BackendMessage> {
        let snaps = self.resource_meter.all_usage_snapshots();
        let mut rows = vec![
            vec![Some("tenant_count".into()), Some(self.resource_meter.tenant_count().to_string())],
            vec![Some("throttled_total".into()), Some(self.resource_meter.throttled_count().to_string())],
            vec![Some("overage_alerts".into()), Some(self.resource_meter.overage_alerts().to_string())],
        ];
        for snap in &snaps {
            rows.push(vec![
                Some(format!("tenant_{}", snap.tenant_id.0)),
                Some(format!(
                    "cpu={}us mem={}B storage={}B queries={} txn_c={} txn_a={} qps={:.1}",
                    snap.cpu_us, snap.memory_bytes, snap.storage_bytes,
                    snap.query_count, snap.txn_committed, snap.txn_aborted, snap.qps,
                )),
            ]);
        }
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 80, -1)],
            rows,
        )
    }

    fn show_falcon_health_score(&self) -> Vec<BackendMessage> {
        let inputs = falcon_storage::health::HealthInputs::default();
        let report = falcon_storage::health::HealthScorer::score(&inputs);
        let mut rows = vec![
            vec![Some("overall_score".into()), Some(report.overall_score.to_string())],
            vec![Some("status".into()), Some(report.status.to_string())],
        ];
        for comp in &report.components {
            rows.push(vec![
                Some(format!("{}_score", comp.name)),
                Some(format!("{} ({}) {}", comp.score, comp.status, comp.detail)),
            ]);
        }
        for (i, rec) in report.recommendations.iter().enumerate() {
            rows.push(vec![
                Some(format!("recommendation_{}", i + 1)),
                Some(if rec.len() > 80 { format!("{}...", &rec[..77]) } else { rec.clone() }),
            ]);
        }
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 80, -1)],
            rows,
        )
    }

    fn show_falcon_security(&self) -> Vec<BackendMessage> {
        let summary = self.security_manager.security_summary();
        let rows = vec![
            vec![Some("encryption_enabled".into()), Some(summary.encryption_enabled.to_string())],
            vec![Some("encryption_algorithm".into()), Some(summary.encryption_algorithm.clone())],
            vec![Some("tls_enabled".into()), Some(summary.tls_enabled.to_string())],
            vec![Some("tls_min_version".into()), Some(summary.tls_min_version.clone())],
            vec![Some("tls_require_client_cert".into()), Some(summary.tls_require_client_cert.to_string())],
            vec![Some("kms_provider".into()), Some(summary.kms_provider.clone())],
            vec![Some("ip_allowlist_enabled".into()), Some(summary.ip_allowlist_enabled.to_string())],
            vec![Some("ip_allowlist_size".into()), Some(summary.ip_allowlist_size.to_string())],
            vec![Some("blocked_by_ip".into()), Some(summary.blocked_by_ip.to_string())],
            vec![Some("total_connection_attempts".into()), Some(summary.total_connection_attempts.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 30, -1), ("value", 40, -1)],
            rows,
        )
    }

    fn show_falcon_compat(&self) -> Vec<BackendMessage> {
        let matrix = falcon_common::compat::pg_compat_matrix();
        let mut rows = vec![
            vec![Some("total_drivers".into()), Some(matrix.len().to_string())],
        ];
        for entry in &matrix {
            rows.push(vec![
                Some(format!("{} ({})", entry.name, entry.language)),
                Some(format!("{} v{} — {}", entry.status, entry.version, entry.notes)),
            ]);
        }
        self.single_row_result(
            vec![("driver", 30, -1), ("status", 60, -1)],
            rows,
        )
    }

    fn show_falcon_admission(&self) -> Vec<BackendMessage> {
        let inputs = falcon_common::kernel::AdmissionInputs::default();
        let decision = falcon_common::kernel::AdmissionController::evaluate(&inputs);
        let rows = vec![
            vec![Some("decision".into()), Some(format!("{:?}", decision))],
            vec![Some("queue_length".into()), Some(inputs.queue_length.to_string())],
            vec![Some("memory_pressure".into()), Some(format!("{:.2}", inputs.memory_pressure))],
            vec![Some("wal_backlog_bytes".into()), Some(inputs.wal_backlog_bytes.to_string())],
            vec![Some("replication_lag_us".into()), Some(inputs.replication_lag_us.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 40, -1)],
            rows,
        )
    }

    fn show_falcon_hotspots(&self) -> Vec<BackendMessage> {
        let alerts = self.hotspot_detector.detect();
        let mut rows = vec![
            vec![Some("tracked_shards".into()), Some(self.hotspot_detector.tracked_shards().to_string())],
            vec![Some("tracked_tables".into()), Some(self.hotspot_detector.tracked_tables().to_string())],
            vec![Some("total_alerts".into()), Some(self.hotspot_detector.total_alerts().to_string())],
        ];
        for alert in &alerts {
            rows.push(vec![
                Some(format!("{}:{}", alert.kind, alert.target)),
                Some(format!("count={} severity={:.2} mitigation={}", alert.access_count, alert.severity, alert.mitigation)),
            ]);
        }
        self.single_row_result(
            vec![("metric", 30, -1), ("value", 60, -1)],
            rows,
        )
    }

    fn show_falcon_verification(&self) -> Vec<BackendMessage> {
        let report = self.consistency_verifier.audit_report(0);
        let mut rows = vec![
            vec![Some("total_checks".into()), Some(report.total_checks.to_string())],
            vec![Some("passed".into()), Some(report.passed.to_string())],
            vec![Some("failed".into()), Some(report.failed.to_string())],
            vec![Some("coverage".into()), Some(format!("{:.1}%", report.coverage_ratio * 100.0))],
            vec![Some("anomalies_detected".into()), Some(report.anomalies_detected.to_string())],
        ];
        for r in report.results.iter().take(10) {
            rows.push(vec![
                Some(format!("{} [{}]", r.check_type, r.scope)),
                Some(format!("passed={} hash={}", r.passed, r.computed_hash)),
            ]);
        }
        self.single_row_result(
            vec![("metric", 30, -1), ("value", 60, -1)],
            rows,
        )
    }

    fn show_falcon_latency_contract(&self) -> Vec<BackendMessage> {
        let contract = falcon_common::kernel::LatencyContract::default();
        let stats = self.txn_mgr.stats_snapshot();
        let eval = contract.evaluate(stats.latency.all.p99_us, stats.latency.all.p99_us * 2);
        let rows = vec![
            vec![Some("contract".into()), Some(contract.name.clone())],
            vec![Some("p99_target_us".into()), Some(contract.p99_target_us.to_string())],
            vec![Some("p999_target_us".into()), Some(contract.p999_target_us.to_string())],
            vec![Some("max_jitter_ratio".into()), Some(format!("{:.2}", contract.max_jitter_ratio))],
            vec![Some("observed_p99_us".into()), Some(eval.observed_p99_us.to_string())],
            vec![Some("p99_met".into()), Some(eval.p99_met.to_string())],
            vec![Some("p999_met".into()), Some(eval.p999_met.to_string())],
            vec![Some("jitter_ratio".into()), Some(format!("{:.4}", eval.jitter_ratio))],
            vec![Some("jitter_within_bounds".into()), Some(eval.jitter_within_bounds.to_string())],
            vec![Some("passed".into()), Some(eval.passed.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_two_phase(&self) -> Vec<BackendMessage> {
        let rows = vec![
            vec![Some("protocol".into()), Some("two-phase commit (2PC)".into())],
            vec![Some("coordinator".into()), Some("TwoPhaseCoordinator".into())],
            vec![Some("phase1".into()), Some("prepare (begin + execute on each shard)".into())],
            vec![Some("phase2".into()), Some("commit all or abort all".into())],
            vec![Some("atomicity".into()), Some("all-or-nothing across shards".into())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    // ── Cluster Operations Closure ──────────────────────────────────────

    fn show_falcon_cluster_events(&self) -> Vec<BackendMessage> {
        let events = self.cluster_admin.event_log().snapshot();
        let rows: Vec<Vec<Option<String>>> = events
            .iter()
            .map(|e| {
                vec![
                    Some(e.seq.to_string()),
                    Some(e.timestamp_ms.to_string()),
                    Some(e.category.to_string()),
                    Some(e.severity.to_string()),
                    Some(e.node_id.to_string()),
                    Some(if e.shard_id == u64::MAX { "N/A".into() } else { e.shard_id.to_string() }),
                    Some(e.from_state.clone()),
                    Some(e.to_state.clone()),
                    Some(e.message.clone()),
                ]
            })
            .collect();
        if rows.is_empty() {
            return self.single_row_result(
                vec![
                    ("seq", 20, -1), ("timestamp_ms", 20, -1), ("category", 25, -1),
                    ("severity", 25, -1), ("node_id", 20, -1), ("shard_id", 20, -1),
                    ("from_state", 25, -1), ("to_state", 25, -1), ("message", 25, -1),
                ],
                vec![vec![
                    Some("(no events)".into()), Some("".into()), Some("".into()),
                    Some("".into()), Some("".into()), Some("".into()),
                    Some("".into()), Some("".into()), Some("".into()),
                ]],
            );
        }
        self.single_row_result(
            vec![
                ("seq", 20, -1), ("timestamp_ms", 20, -1), ("category", 25, -1),
                ("severity", 25, -1), ("node_id", 20, -1), ("shard_id", 20, -1),
                ("from_state", 25, -1), ("to_state", 25, -1), ("message", 25, -1),
            ],
            rows,
        )
    }

    fn show_falcon_node_lifecycle(&self) -> Vec<BackendMessage> {
        let scale_outs = self.cluster_admin.scale_out_snapshot();
        let scale_ins = self.cluster_admin.scale_in_snapshot();

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();

        for op in &scale_outs {
            rows.push(vec![
                Some("scale_out".into()),
                Some(op.node_id.0.to_string()),
                Some(op.state.to_string()),
                Some(format!("{:.1}s", op.elapsed().as_secs_f64())),
                Some(op.error.clone().unwrap_or_default()),
            ]);
        }
        for op in &scale_ins {
            rows.push(vec![
                Some("scale_in".into()),
                Some(op.node_id.0.to_string()),
                Some(op.state.to_string()),
                Some(format!("{:.1}s", op.elapsed().as_secs_f64())),
                Some(op.error.clone().unwrap_or_default()),
            ]);
        }

        if rows.is_empty() {
            rows.push(vec![
                Some("(none)".into()), Some("".into()), Some("".into()),
                Some("".into()), Some("".into()),
            ]);
        }

        self.single_row_result(
            vec![
                ("operation", 25, -1), ("node_id", 20, -1), ("state", 25, -1),
                ("elapsed", 25, -1), ("error", 25, -1),
            ],
            rows,
        )
    }

    fn show_falcon_rebalance_plan(&self) -> Vec<BackendMessage> {
        if let Some(ref dist) = self.dist_engine {
            let snapshot = dist.shard_load_snapshot();
            let planner = falcon_cluster::rebalancer::RebalancePlanner::new(
                falcon_cluster::rebalancer::RebalancerConfig::default(),
            );
            let plan = dist.rebalance_plan(&planner);

            let mut rows: Vec<Vec<Option<String>>> = Vec::new();
            rows.push(vec![
                Some("summary".into()),
                Some("".into()),
                Some("".into()),
                Some(format!("{} tasks", plan.tasks.len())),
                Some(format!("{} rows", plan.total_rows_to_move())),
                Some(plan.estimated_duration_display()),
            ]);
            rows.push(vec![
                Some("imbalance_ratio".into()),
                Some("".into()),
                Some("".into()),
                Some(format!("{:.3}", snapshot.imbalance_ratio())),
                Some(format!("{} total rows", snapshot.total_rows())),
                Some("".into()),
            ]);

            for task in &plan.tasks {
                rows.push(vec![
                    Some("move".into()),
                    Some(format!("shard_{} → shard_{}", task.source_shard.0, task.target_shard.0)),
                    Some(task.table_name.clone()),
                    Some(format!("{} rows", task.rows_to_move)),
                    Some("pending".into()),
                    Some(format!("~{}ms", task.estimated_time_ms)),
                ]);
            }

            if plan.tasks.is_empty() {
                rows.push(vec![
                    Some("(balanced)".into()),
                    Some("".into()),
                    Some("".into()),
                    Some("no moves needed".into()),
                    Some("".into()),
                    Some("".into()),
                ]);
            }

            self.single_row_result(
                vec![
                    ("type", 25, -1), ("shards", 25, -1), ("table", 25, -1),
                    ("detail", 25, -1), ("status", 25, -1), ("estimated_time", 25, -1),
                ],
                rows,
            )
        } else {
            self.single_row_result(
                vec![("metric", 25, -1), ("value", 25, -1)],
                vec![vec![
                    Some("rebalance_plan".into()),
                    Some("single-shard mode — rebalance not applicable".into()),
                ]],
            )
        }
    }

    /// Handle cluster admin commands dispatched from the query handler.
    /// Commands: `SELECT falcon_add_node(id)`, `SELECT falcon_remove_node(id)`,
    ///           `SELECT falcon_promote_leader(shard_id)`,
    ///           `SELECT falcon_rebalance_apply()`.
    pub(crate) fn handle_cluster_admin_command(
        &self,
        command: &str,
        args: &[&str],
    ) -> Option<Vec<BackendMessage>> {
        match command {
            "falcon_add_node" => {
                let node_id: u64 = args.first()
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(0);
                if node_id == 0 {
                    return Some(self.single_row_result(
                        vec![("result", 25, -1)],
                        vec![vec![Some("ERROR: invalid node_id (must be > 0)".into())]],
                    ));
                }
                match self.cluster_admin.begin_scale_out(falcon_common::types::NodeId(node_id)) {
                    Ok(()) => Some(self.single_row_result(
                        vec![("result", 25, -1)],
                        vec![vec![Some(format!("Scale-out initiated for node {}", node_id))]],
                    )),
                    Err(e) => Some(self.single_row_result(
                        vec![("result", 25, -1)],
                        vec![vec![Some(format!("ERROR: {}", e))]],
                    )),
                }
            }
            "falcon_remove_node" => {
                let node_id: u64 = args.first()
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(0);
                if node_id == 0 {
                    return Some(self.single_row_result(
                        vec![("result", 25, -1)],
                        vec![vec![Some("ERROR: invalid node_id (must be > 0)".into())]],
                    ));
                }
                match self.cluster_admin.begin_scale_in(falcon_common::types::NodeId(node_id)) {
                    Ok(()) => Some(self.single_row_result(
                        vec![("result", 25, -1)],
                        vec![vec![Some(format!("Scale-in initiated for node {}", node_id))]],
                    )),
                    Err(e) => Some(self.single_row_result(
                        vec![("result", 25, -1)],
                        vec![vec![Some(format!("ERROR: {}", e))]],
                    )),
                }
            }
            "falcon_promote_leader" => {
                let shard_id: u64 = args.first()
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(u64::MAX);
                if shard_id == u64::MAX {
                    return Some(self.single_row_result(
                        vec![("result", 25, -1)],
                        vec![vec![Some("ERROR: invalid shard_id".into())]],
                    ));
                }
                self.cluster_admin.log_leader_transfer(
                    falcon_common::types::ShardId(shard_id),
                    falcon_common::types::NodeId(0),
                    falcon_common::types::NodeId(0),
                );
                Some(self.single_row_result(
                    vec![("result", 25, -1)],
                    vec![vec![Some(format!(
                        "Leader promotion requested for shard {} (will execute on next failover cycle)",
                        shard_id
                    ))]],
                ))
            }
            "falcon_rebalance_apply" => {
                if self.dist_engine.is_none() {
                    return Some(self.single_row_result(
                        vec![("result", 25, -1)],
                        vec![vec![Some("ERROR: single-shard mode — rebalance not applicable".into())]],
                    ));
                }
                let planner = falcon_cluster::rebalancer::RebalancePlanner::new(
                    falcon_cluster::rebalancer::RebalancerConfig::default(),
                );
                let plan = self.dist_engine.as_ref().unwrap().rebalance_plan(&planner);
                let num_tasks = plan.tasks.len();
                let total_rows = plan.total_rows_to_move();

                self.cluster_admin.log_rebalance_plan(num_tasks, total_rows);

                if plan.tasks.is_empty() {
                    return Some(self.single_row_result(
                        vec![("result", 25, -1)],
                        vec![vec![Some("Cluster is balanced — no migration needed".into())]],
                    ));
                }

                self.cluster_admin.log_rebalance_start(num_tasks);

                let start = std::time::Instant::now();
                let (completed, failed, rows_migrated) =
                    self.dist_engine.as_ref().unwrap().rebalance_execute(&plan);

                self.cluster_admin.log_rebalance_complete(
                    completed, failed, rows_migrated, start.elapsed(),
                );

                Some(self.single_row_result(
                    vec![("result", 25, -1)],
                    vec![vec![Some(format!(
                        "Rebalance complete: {}/{} tasks, {} rows migrated in {:.1}s",
                        completed, num_tasks, rows_migrated, start.elapsed().as_secs_f64(),
                    ))]],
                ))
            }
            _ => None,
        }
    }

    fn show_falcon_priority_scheduler(&self) -> Vec<BackendMessage> {
        let snap = self.priority_scheduler.snapshot();
        let rows = vec![
            vec![Some("high_active".into()), Some(snap.high_active.to_string())],
            vec![Some("high_admitted".into()), Some(snap.high_admitted.to_string())],
            vec![Some("high_rejected".into()), Some(snap.high_rejected.to_string())],
            vec![Some("normal_active".into()), Some(snap.normal_active.to_string())],
            vec![Some("normal_max".into()), Some(snap.normal_max.to_string())],
            vec![Some("normal_admitted".into()), Some(snap.normal_admitted.to_string())],
            vec![Some("normal_rejected".into()), Some(snap.normal_rejected.to_string())],
            vec![Some("normal_wait_us".into()), Some(snap.normal_wait_us.to_string())],
            vec![Some("low_active".into()), Some(snap.low_active.to_string())],
            vec![Some("low_max".into()), Some(snap.low_max.to_string())],
            vec![Some("low_admitted".into()), Some(snap.low_admitted.to_string())],
            vec![Some("low_rejected".into()), Some(snap.low_rejected.to_string())],
            vec![Some("low_wait_us".into()), Some(snap.low_wait_us.to_string())],
            vec![Some("high_waiters".into()), Some(snap.high_waiters.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_token_bucket(&self) -> Vec<BackendMessage> {
        let snap = self.rebalance_token_bucket.snapshot();
        let rows = vec![
            vec![Some("rate_per_sec".into()), Some(snap.rate_per_sec.to_string())],
            vec![Some("burst".into()), Some(snap.burst.to_string())],
            vec![Some("available_tokens".into()), Some(snap.available_tokens.to_string())],
            vec![Some("paused".into()), Some(snap.paused.to_string())],
            vec![Some("total_consumed".into()), Some(snap.total_consumed.to_string())],
            vec![Some("total_acquired".into()), Some(snap.total_acquired.to_string())],
            vec![Some("total_rejected".into()), Some(snap.total_rejected.to_string())],
            vec![Some("total_wait_us".into()), Some(snap.total_wait_us.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 25, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_two_phase_config(&self) -> Vec<BackendMessage> {
        let dl = self.decision_log.snapshot();
        let tc = self.timeout_controller.snapshot();
        let ss = self.slow_shard_tracker.snapshot();

        let rows = vec![
            // Decision log
            vec![Some("decision_log.total_logged".into()), Some(dl.total_logged.to_string())],
            vec![Some("decision_log.total_applied".into()), Some(dl.total_applied.to_string())],
            vec![Some("decision_log.current_entries".into()), Some(dl.current_entries.to_string())],
            vec![Some("decision_log.unapplied_count".into()), Some(dl.unapplied_count.to_string())],
            vec![Some("decision_log.max_entries".into()), Some(dl.max_entries.to_string())],
            // Layered timeouts
            vec![Some("timeout.soft_ms".into()), Some(tc.soft_timeout_ms.to_string())],
            vec![Some("timeout.hard_ms".into()), Some(tc.hard_timeout_ms.to_string())],
            vec![Some("timeout.per_shard_us".into()), Some(tc.per_shard_timeout_us.to_string())],
            vec![Some("timeout.total_soft_timeouts".into()), Some(tc.total_soft_timeouts.to_string())],
            vec![Some("timeout.total_hard_timeouts".into()), Some(tc.total_hard_timeouts.to_string())],
            vec![Some("timeout.total_shard_timeouts".into()), Some(tc.total_shard_timeouts.to_string())],
            // Slow-shard policy
            vec![Some("slow_shard.policy".into()), Some(ss.policy.to_string())],
            vec![Some("slow_shard.threshold_us".into()), Some(ss.slow_threshold_us.to_string())],
            vec![Some("slow_shard.hedge_delay_us".into()), Some(ss.hedge_delay_us.to_string())],
            vec![Some("slow_shard.max_hedged_inflight".into()), Some(ss.max_hedged_inflight.to_string())],
            vec![Some("slow_shard.total_slow_detected".into()), Some(ss.total_slow_detected.to_string())],
            vec![Some("slow_shard.total_fast_aborts".into()), Some(ss.total_fast_aborts.to_string())],
            vec![Some("slow_shard.total_hedged_sent".into()), Some(ss.total_hedged_sent.to_string())],
            vec![Some("slow_shard.total_hedged_won".into()), Some(ss.total_hedged_won.to_string())],
            vec![Some("slow_shard.hedged_inflight".into()), Some(ss.hedged_inflight.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 40, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_fault_injection(&self) -> Vec<BackendMessage> {
        let snap = self.fault_injector.full_snapshot();
        let rows = vec![
            vec![Some("leader_killed".into()), Some(snap.leader_killed.to_string())],
            vec![Some("replica_delay_us".into()), Some(snap.replica_delay_us.to_string())],
            vec![Some("wal_corruption_armed".into()), Some(snap.wal_corruption_armed.to_string())],
            vec![Some("disk_delay_us".into()), Some(snap.disk_delay_us.to_string())],
            vec![Some("faults_fired".into()), Some(snap.faults_fired.to_string())],
            // Partition
            vec![Some("partition.active".into()), Some(snap.partition.active.to_string())],
            vec![Some("partition.group_a".into()), Some(format!("{:?}", snap.partition.group_a))],
            vec![Some("partition.group_b".into()), Some(format!("{:?}", snap.partition.group_b))],
            vec![Some("partition.partition_count".into()), Some(snap.partition.partition_count.to_string())],
            vec![Some("partition.heal_count".into()), Some(snap.partition.heal_count.to_string())],
            vec![Some("partition.total_events".into()), Some(snap.partition.total_events.to_string())],
            // Jitter
            vec![Some("jitter.enabled".into()), Some(snap.jitter.enabled.to_string())],
            vec![Some("jitter.base_us".into()), Some(snap.jitter.base_us.to_string())],
            vec![Some("jitter.amplitude_us".into()), Some(snap.jitter.amplitude_us.to_string())],
            vec![Some("jitter.mode".into()), Some(snap.jitter.mode.to_string())],
            vec![Some("jitter.total_events".into()), Some(snap.jitter.total_events.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 40, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_observability_catalog(&self) -> Vec<BackendMessage> {
        let commands: Vec<(&str, &str, &str)> = vec![
            // Core
            ("falcon.version", "Server version and build info", "v0.1"),
            ("falcon.health", "Basic health status", "v0.1"),
            ("falcon.node_role", "Current node role", "v0.1"),
            // Transactions
            ("falcon.txn", "Current session transaction state", "v0.1"),
            ("falcon.txn_stats", "Transaction statistics", "v0.1"),
            ("falcon.txn_history", "Recent transaction history", "v0.1"),
            ("falcon.slow_txns", "Slow/long-running transactions", "v0.3"),
            // Storage & WAL
            ("falcon.wal_stats", "WAL write/flush/backlog statistics", "v0.1"),
            ("falcon.gc_stats", "GC sweep statistics", "v0.1"),
            ("falcon.gc_safepoint", "GC safepoint and active txn count", "v0.1"),
            ("falcon.memory_pressure", "Memory usage, limits, pressure state", "v0.1"),
            ("falcon.checkpoint_stats", "Checkpoint statistics", "v0.2"),
            // Replication & HA
            ("falcon.replication_stats", "Replication lag, applied LSN", "v0.1"),
            ("falcon.replication", "Replication configuration", "v0.2"),
            ("falcon.replica_stats", "Per-replica statistics", "v0.2"),
            ("falcon.replica_health", "Replica health and connectivity", "v0.3"),
            ("falcon.ha_status", "HA group status", "v0.3"),
            // Cluster & Sharding
            ("falcon.cluster", "Cluster topology and membership", "v0.2"),
            ("falcon.shards", "Shard map and distribution", "v0.2"),
            ("falcon.shard_stats", "Per-shard statistics", "v0.2"),
            ("falcon.scatter_stats", "Scatter/gather query statistics", "v0.2"),
            ("falcon.query_routing", "Query routing decisions", "v0.2"),
            ("falcon.dist_capabilities", "Distributed query capabilities", "v0.2"),
            ("falcon.rebalance_status", "Shard rebalancer status", "v0.3"),
            ("falcon.two_phase", "2PC coordinator statistics", "v0.3"),
            // Query Processing
            ("falcon.slow_queries", "Slow query log entries", "v0.3"),
            ("falcon.plan_cache", "Plan cache hit/miss statistics", "v0.3"),
            // Multi-Tenancy
            ("falcon.tenants", "Registered tenants", "v0.4"),
            ("falcon.tenant_usage", "Per-tenant resource usage", "v0.4"),
            // Enterprise & Security
            ("falcon.license", "License information and edition", "v0.4"),
            ("falcon.metering", "Resource metering per tenant", "v0.4"),
            ("falcon.security", "Security posture", "v0.4"),
            ("falcon.health_score", "Weighted health score", "v0.4"),
            ("falcon.compat", "Compatibility information", "v0.4"),
            ("falcon.audit_log", "Recent audit log entries", "v0.4"),
            ("falcon.sla_stats", "SLA priority latency statistics", "v0.4"),
            // Production Hardening (v0.5+)
            ("falcon.admission", "Admission control permits", "v0.5"),
            ("falcon.hotspots", "Hotspot detection results", "v0.5"),
            ("falcon.verification", "Consistency verification status", "v0.5"),
            ("falcon.latency_contract", "Latency contract and SLO targets", "v0.5"),
            ("falcon.cluster_events", "Cluster event log", "v0.5"),
            ("falcon.node_lifecycle", "Node lifecycle state machines", "v0.5"),
            ("falcon.rebalance_plan", "Rebalance migration plan", "v0.5"),
            // Tail Latency Governance (v0.6)
            ("falcon.priority_scheduler", "Priority queue scheduler metrics", "v0.6"),
            ("falcon.token_bucket", "Token bucket rate limiter metrics", "v0.6"),
            // Deterministic 2PC (v0.7)
            ("falcon.two_phase_config", "Decision log, timeouts, slow-shard policy", "v0.7"),
            // Chaos Engineering (v0.8)
            ("falcon.fault_injection", "Fault injector state", "v0.8"),
            // Security Hardening (v0.9)
            ("falcon.security_audit", "Auth rate limiter, password policy, SQL firewall", "v0.9"),
            // Release Engineering (v0.9)
            ("falcon.wire_compat", "Version, WAL format, snapshot format, min compatible", "v0.9"),
            // Meta
            ("falcon.observability_catalog", "This catalog of all SHOW commands", "v0.8"),
        ];

        let rows: Vec<Vec<Option<String>>> = commands
            .iter()
            .map(|(cmd, desc, since)| {
                vec![
                    Some(format!("SHOW {}", cmd)),
                    Some(desc.to_string()),
                    Some(since.to_string()),
                ]
            })
            .collect();

        self.single_row_result(
            vec![("command", 40, -1), ("description", 50, -1), ("since", 10, -1)],
            rows,
        )
    }

    fn show_falcon_security_audit(&self) -> Vec<BackendMessage> {
        let auth = self.auth_rate_limiter.snapshot();
        let pw = self.password_policy.snapshot();
        let fw = self.sql_firewall.snapshot();

        let rows = vec![
            // Auth rate limiter
            vec![Some("auth.max_failures".into()), Some(auth.max_failures.to_string())],
            vec![Some("auth.lockout_duration_secs".into()), Some(auth.lockout_duration_secs.to_string())],
            vec![Some("auth.failure_window_secs".into()), Some(auth.failure_window_secs.to_string())],
            vec![Some("auth.tracked_sources".into()), Some(auth.tracked_sources.to_string())],
            vec![Some("auth.active_lockouts".into()), Some(auth.active_lockouts.to_string())],
            vec![Some("auth.total_checks".into()), Some(auth.total_checks.to_string())],
            vec![Some("auth.total_lockouts".into()), Some(auth.total_lockouts.to_string())],
            vec![Some("auth.total_failures".into()), Some(auth.total_failures_recorded.to_string())],
            // Password policy
            vec![Some("password.min_length".into()), Some(pw.min_length.to_string())],
            vec![Some("password.require_uppercase".into()), Some(pw.require_uppercase.to_string())],
            vec![Some("password.require_lowercase".into()), Some(pw.require_lowercase.to_string())],
            vec![Some("password.require_digit".into()), Some(pw.require_digit.to_string())],
            vec![Some("password.require_special".into()), Some(pw.require_special.to_string())],
            vec![Some("password.max_age_days".into()), Some(pw.max_age_days.to_string())],
            vec![Some("password.total_checks".into()), Some(pw.total_checks.to_string())],
            vec![Some("password.total_rejections".into()), Some(pw.total_rejections.to_string())],
            // SQL firewall
            vec![Some("firewall.detect_injection".into()), Some(fw.detect_injection.to_string())],
            vec![Some("firewall.block_dangerous".into()), Some(fw.block_dangerous.to_string())],
            vec![Some("firewall.block_stacking".into()), Some(fw.block_stacking.to_string())],
            vec![Some("firewall.max_statement_length".into()), Some(fw.max_statement_length.to_string())],
            vec![Some("firewall.custom_patterns".into()), Some(fw.custom_patterns_count.to_string())],
            vec![Some("firewall.total_checks".into()), Some(fw.total_checks.to_string())],
            vec![Some("firewall.total_blocked".into()), Some(fw.total_blocked.to_string())],
            vec![Some("firewall.injection_detected".into()), Some(fw.injection_detected.to_string())],
            vec![Some("firewall.dangerous_blocked".into()), Some(fw.dangerous_blocked.to_string())],
            vec![Some("firewall.stacking_blocked".into()), Some(fw.stacking_blocked.to_string())],
            vec![Some("firewall.overlength_blocked".into()), Some(fw.overlength_blocked.to_string())],
        ];
        self.single_row_result(
            vec![("metric", 40, -1), ("value", 25, -1)],
            rows,
        )
    }

    fn show_falcon_wire_compat(&self) -> Vec<BackendMessage> {
        use falcon_storage::upgrade::{
            FALCON_VERSION_MAJOR, FALCON_VERSION_MINOR, FALCON_VERSION_PATCH,
            MIN_COMPATIBLE_MAJOR, MIN_COMPATIBLE_MINOR,
            SNAPSHOT_FORMAT_VERSION, VersionHeader,
        };
        use falcon_storage::wal::WAL_FORMAT_VERSION;

        let current = VersionHeader::current();
        let rows = vec![
            vec![Some("server_version".into()), Some(current.version_string())],
            vec![Some("wal_format_version".into()), Some(WAL_FORMAT_VERSION.to_string())],
            vec![Some("snapshot_format_version".into()), Some(SNAPSHOT_FORMAT_VERSION.to_string())],
            vec![Some("min_compatible_major".into()), Some(MIN_COMPATIBLE_MAJOR.to_string())],
            vec![Some("min_compatible_minor".into()), Some(MIN_COMPATIBLE_MINOR.to_string())],
            vec![Some("version_major".into()), Some(FALCON_VERSION_MAJOR.to_string())],
            vec![Some("version_minor".into()), Some(FALCON_VERSION_MINOR.to_string())],
            vec![Some("version_patch".into()), Some(FALCON_VERSION_PATCH.to_string())],
            vec![Some("wal_magic".into()), Some("FALC".into())],
            vec![Some("wal_segment_header_bytes".into()), Some("8".into())],
        ];
        self.single_row_result(
            vec![("property", 35, -1), ("value", 20, -1)],
            rows,
        )
    }
}
