//! v1.1.0 Enterprise Compliance & Chaos Tests
//!
//! Tests cover:
//! - Control plane HA failover
//! - Metadata consistency under concurrent ops
//! - AuthN lockout and credential rotation
//! - RBAC permission misuse and least privilege
//! - TLS certificate rotation without disruption
//! - Backup/restore lifecycle with PITR
//! - Audit log tamper detection and SIEM export
//! - Auto-rebalance on node join/leave
//! - SLO engine accuracy and breach detection
//! - Incident auto-correlation
//! - Full enterprise lifecycle orchestration

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use falcon_common::types::NodeId;
use falcon_cluster::control_plane::*;
use falcon_cluster::enterprise_security::*;
use falcon_cluster::enterprise_ops::*;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Control Plane HA Failover
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_controller_failover_metadata_survives() {
    // Setup: 3-node controller group, node 1 is leader
    let ha = ControllerHAGroup::new(1, vec![
        (1, "ctrl1:9000".into()),
        (2, "ctrl2:9000".into()),
        (3, "ctrl3:9000".into()),
    ]);
    ha.elect_self();
    assert!(ha.is_leader());

    // Write metadata while node 1 is leader
    let store = ConsistentMetadataStore::new(true);
    store.put(MetadataDomain::ClusterConfig, "cluster.name", "prod-enterprise", "admin");
    store.put(MetadataDomain::ShardPlacement, "shard-0", "node-1", "system");

    // Simulate failover: node 1 goes down, node 2 becomes leader
    ha.set_leader(2, ha.term() + 1);
    assert!(!ha.is_leader());

    // Metadata still readable (data plane continues)
    let name = store.get(&MetadataDomain::ClusterConfig, "cluster.name", ReadConsistency::Any);
    assert_eq!(name, Some("prod-enterprise".to_string()));
}

#[test]
fn test_concurrent_metadata_writes_no_drift() {
    let store = Arc::new(ConsistentMetadataStore::new(true));
    let mut handles = Vec::new();

    // 4 threads writing to different domains concurrently
    for t in 0..4 {
        let s = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            let domain = match t {
                0 => MetadataDomain::ClusterConfig,
                1 => MetadataDomain::ShardPlacement,
                2 => MetadataDomain::VersionEpoch,
                _ => MetadataDomain::SecurityPolicy,
            };
            for i in 0..100 {
                s.put(domain.clone(), &format!("key-{}", i), &format!("val-{}-{}", t, i), "test");
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    // Verify all writes landed
    assert_eq!(store.metrics.writes.load(std::sync::atomic::Ordering::Relaxed), 400);
    // Each domain has 100 keys
    assert_eq!(store.list_keys(&MetadataDomain::ClusterConfig).len(), 100);
    assert_eq!(store.list_keys(&MetadataDomain::ShardPlacement).len(), 100);
}

#[test]
fn test_metadata_cas_prevents_race() {
    let store = ConsistentMetadataStore::new(true);
    store.put(MetadataDomain::ClusterConfig, "leader", "node-1", "system");

    // CAS with correct expected value succeeds
    let r1 = store.cas(MetadataDomain::ClusterConfig, "leader", Some("node-1"), "node-2", "failover");
    assert!(matches!(r1, MetadataWriteResult::Ok { .. }));

    // CAS with stale expected value fails
    let r2 = store.cas(MetadataDomain::ClusterConfig, "leader", Some("node-1"), "node-3", "failover");
    assert!(matches!(r2, MetadataWriteResult::CasFailed { current } if current == Some("node-2".into())));
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Node Registry & Config Distribution
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_node_registry_lifecycle() {
    let reg = NodeRegistry::new(Duration::from_millis(50), Duration::from_millis(150));
    let config = ConfigStore::new();

    // Register 3 data nodes
    for i in 1..=3 {
        reg.register(NodeId(i), format!("data{}:5000", i), NodeCapabilities::default());
    }
    assert_eq!(reg.node_count(), 3);
    assert_eq!(reg.online_nodes().len(), 3);

    // Distribute config
    let v = config.set("replication.factor", "3", "admin");
    let needing = reg.nodes_needing_config(v);
    assert_eq!(needing.len(), 3); // All nodes need update

    // Nodes ack config via heartbeat
    reg.heartbeat(NodeId(1), v);
    reg.heartbeat(NodeId(2), v);
    let needing = reg.nodes_needing_config(v);
    assert_eq!(needing.len(), 1); // Only node 3 still needs update

    // Node goes offline
    thread::sleep(Duration::from_millis(200));
    reg.heartbeat(NodeId(1), v);
    reg.heartbeat(NodeId(2), v);
    reg.evaluate();

    let n3 = reg.get(NodeId(3)).unwrap();
    assert_eq!(n3.state, DataNodeState::Offline);
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — AuthN: Lockout, Credential Rotation, Multi-Method
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_auth_lockout_and_recovery() {
    let mgr = AuthnManager::new(3, 1); // lockout after 3 fails, 1 second lockout
    let uid = mgr.create_user("admin", "correct_hash");

    // 3 failed attempts → lockout
    for _ in 0..3 {
        let r = mgr.authenticate(&AuthnRequest {
            username: "admin".into(),
            credential_type: CredentialType::Password,
            credential: "wrong".into(),
            source_ip: "10.0.0.1".into(),
        });
        assert!(matches!(r, AuthnResult::Failed { .. }));
    }

    let r = mgr.authenticate(&AuthnRequest {
        username: "admin".into(),
        credential_type: CredentialType::Password,
        credential: "correct_hash".into(),
        source_ip: "10.0.0.1".into(),
    });
    assert!(matches!(r, AuthnResult::Locked { .. }));

    // Wait for lockout to expire
    thread::sleep(Duration::from_secs(2));

    let r = mgr.authenticate(&AuthnRequest {
        username: "admin".into(),
        credential_type: CredentialType::Password,
        credential: "correct_hash".into(),
        source_ip: "10.0.0.1".into(),
    });
    assert!(matches!(r, AuthnResult::Success { .. }));
}

#[test]
fn test_credential_rotation_without_downtime() {
    let mgr = AuthnManager::new(5, 300);
    let uid = mgr.create_user("svc-account", "old_token_hash");
    mgr.add_token(uid, "new_token_hash", None);

    // Old token still works
    let r = mgr.authenticate(&AuthnRequest {
        username: "svc-account".into(),
        credential_type: CredentialType::Password,
        credential: "old_token_hash".into(),
        source_ip: "10.0.0.1".into(),
    });
    assert!(matches!(r, AuthnResult::Success { .. }));

    // Revoke old token
    mgr.revoke_credential(uid, CredentialType::Password, "old_token_hash");

    // Old token fails
    let r = mgr.authenticate(&AuthnRequest {
        username: "svc-account".into(),
        credential_type: CredentialType::Password,
        credential: "old_token_hash".into(),
        source_ip: "10.0.0.1".into(),
    });
    assert!(matches!(r, AuthnResult::Failed { .. }));

    // New token works
    let r = mgr.authenticate(&AuthnRequest {
        username: "svc-account".into(),
        credential_type: CredentialType::Token,
        credential: "new_token_hash".into(),
        source_ip: "10.0.0.1".into(),
    });
    assert!(matches!(r, AuthnResult::Success { .. }));
}

#[test]
fn test_auth_does_not_leak_topology() {
    let mgr = AuthnManager::new(5, 300);
    // Attempting auth with non-existent user should return generic "user not found"
    let r = mgr.authenticate(&AuthnRequest {
        username: "unknown_user".into(),
        credential_type: CredentialType::Password,
        credential: "anything".into(),
        source_ip: "evil.attacker.com".into(),
    });
    match r {
        AuthnResult::Failed { reason } => {
            assert!(!reason.contains("node"));
            assert!(!reason.contains("shard"));
            assert!(!reason.contains("controller"));
        }
        _ => panic!("expected failed"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — RBAC: Permission Misuse & Least Privilege
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_rbac_least_privilege() {
    let rbac = EnterpriseRbac::new();

    // Grant: role 10 can SELECT on 'users' table only
    rbac.grant(10, RbacScope::Table, "users", EnterprisePermission::Select, "admin");

    let mut roles = HashSet::new();
    roles.insert(10u64);

    // Allowed: SELECT on users
    assert_eq!(
        rbac.check(10, &roles, RbacScope::Table, "users", &EnterprisePermission::Select),
        RbacCheckResult::Allowed
    );

    // Denied: INSERT on users
    assert!(matches!(
        rbac.check(10, &roles, RbacScope::Table, "users", &EnterprisePermission::Insert),
        RbacCheckResult::Denied { .. }
    ));

    // Denied: SELECT on different table
    assert!(matches!(
        rbac.check(10, &roles, RbacScope::Table, "orders", &EnterprisePermission::Select),
        RbacCheckResult::Denied { .. }
    ));

    // Denied: Cluster-level operation
    assert!(matches!(
        rbac.check(10, &roles, RbacScope::Cluster, "*", &EnterprisePermission::ManageCluster),
        RbacCheckResult::Denied { .. }
    ));
}

#[test]
fn test_rbac_role_hierarchy() {
    let rbac = EnterpriseRbac::new();

    // Grant to parent role (id=100)
    rbac.grant(100, RbacScope::Database, "analytics", EnterprisePermission::Connect, "admin");
    rbac.grant(100, RbacScope::Table, "events", EnterprisePermission::Select, "admin");

    // Child role (id=200) inherits from parent (id=100)
    let mut effective = HashSet::new();
    effective.insert(200u64);
    effective.insert(100u64); // inherited

    // Child can use parent's grants
    assert_eq!(
        rbac.check(200, &effective, RbacScope::Database, "analytics", &EnterprisePermission::Connect),
        RbacCheckResult::Allowed
    );
    assert_eq!(
        rbac.check(200, &effective, RbacScope::Table, "events", &EnterprisePermission::Select),
        RbacCheckResult::Allowed
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — TLS Certificate Rotation
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_tls_rotation_all_links() {
    let cm = CertificateManager::new(true);
    let links = vec![
        TlsLinkType::ClientToGateway,
        TlsLinkType::GatewayToDataNode,
        TlsLinkType::DataNodeToReplica,
        TlsLinkType::DataNodeToController,
    ];

    // Load initial certs
    for link in &links {
        cm.load_cert(CertificateRecord {
            link_type: *link,
            cert_path: format!("/certs/{:?}.pem", link),
            key_path: format!("/certs/{:?}.key", link),
            ca_path: Some("/certs/ca.pem".into()),
            fingerprint: format!("old-fp-{:?}", link),
            not_before: 1000,
            not_after: 2000,
            loaded_at: 1500,
            is_active: true,
        });
    }
    assert!(cm.all_links_covered().is_empty()); // All covered

    // Rotate all certs
    for link in &links {
        let ok = cm.rotate_cert(*link, "/new.pem", "/new.key", &format!("new-fp-{:?}", link), 2000, 3000);
        assert!(ok);
    }

    // Verify all rotated
    for link in &links {
        let cert = cm.get_cert(*link).unwrap();
        assert!(cert.fingerprint.starts_with("new-fp-"));
    }
    assert_eq!(cm.metrics.rotations_succeeded.load(std::sync::atomic::Ordering::Relaxed), 4);
    assert_eq!(cm.rotation_history(10).len(), 4);
}

#[test]
fn test_cert_expiry_detection() {
    let cm = CertificateManager::new(true);
    cm.load_cert(CertificateRecord {
        link_type: TlsLinkType::ClientToGateway,
        cert_path: "a".into(),
        key_path: "b".into(),
        ca_path: None,
        fingerprint: "f".into(),
        not_before: 0,
        not_after: 100, // expires very soon (timestamp 100)
        loaded_at: 0,
        is_active: true,
    });
    let expiring = cm.expiring_certs(u64::MAX); // within "infinite" window
    assert_eq!(expiring.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Backup/Restore with PITR
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_full_backup_restore_cycle() {
    let orch = BackupOrchestrator::new(30);
    let audit = EnterpriseAuditLog::new(100, "backup-key");

    // Schedule and complete full backup
    let bid = orch.schedule_backup(
        EnterpriseBackupType::Full,
        BackupTarget::S3 { bucket: "prod-backups".into(), prefix: "daily/".into(), region: "us-east-1".into() },
        "cron",
    );
    orch.start_backup(bid, 10_000);
    orch.complete_backup(bid, 5_000_000_000, 50, 20_000, 0xDEADBEEF);
    audit.record(AuditCategory::BackupRestore, AuditSeverity::Info, "cron", "system",
        "FULL_BACKUP", &format!("backup_id={}", bid), "SUCCESS", "5GB, 50 tables");

    // Schedule incremental
    let iid = orch.schedule_backup(
        EnterpriseBackupType::Incremental,
        BackupTarget::S3 { bucket: "prod-backups".into(), prefix: "hourly/".into(), region: "us-east-1".into() },
        "cron",
    );
    orch.start_backup(iid, 20_000);
    orch.complete_backup(iid, 100_000_000, 50, 25_000, 0xCAFE);

    // Restore to point-in-time
    let rid = orch.schedule_restore(
        BackupTarget::S3 { bucket: "prod-backups".into(), prefix: "daily/".into(), region: "us-east-1".into() },
        bid,
        RestoreType::ToTimestamp,
        None,
        Some(1718000000),
    );
    orch.complete_restore(rid, 5_000_000_000, 50);
    audit.record(AuditCategory::BackupRestore, AuditSeverity::Info, "ops", "system",
        "PITR_RESTORE", &format!("restore_id={}", rid), "SUCCESS", "target_ts=1718000000");

    let restore = orch.get_restore_job(rid).unwrap();
    assert_eq!(restore.status, BackupJobStatus::Completed);
    assert_eq!(restore.restore_type, RestoreType::ToTimestamp);
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Audit Log Integrity & SIEM Export
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_audit_chain_integrity_under_load() {
    let log = Arc::new(EnterpriseAuditLog::new(5000, "enterprise-hmac-key"));
    let mut handles = Vec::new();

    // 4 threads recording events concurrently
    for t in 0..4 {
        let l = Arc::clone(&log);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                l.record(
                    AuditCategory::DataAccess,
                    AuditSeverity::Info,
                    &format!("user-t{}", t),
                    "10.0.0.1",
                    "SELECT",
                    &format!("table_{}", i),
                    "OK",
                    "",
                );
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    assert_eq!(log.total(), 400);
    // Note: chain integrity may not hold under concurrent writes because
    // the hash chain is serialized via mutex, so it should still be valid.
    assert!(log.verify_integrity());
}

#[test]
fn test_audit_siem_export_all_categories() {
    let log = EnterpriseAuditLog::new(1000, "siem-key");

    log.record(AuditCategory::Authentication, AuditSeverity::Info,
        "alice", "10.0.0.1", "LOGIN", "system", "SUCCESS", "");
    log.record(AuditCategory::Authorization, AuditSeverity::Warning,
        "bob", "10.0.0.2", "ACCESS_DENIED", "orders", "DENIED", "");
    log.record(AuditCategory::SchemaChange, AuditSeverity::Info,
        "admin", "10.0.0.3", "CREATE TABLE", "users", "OK", "");
    log.record(AuditCategory::OpsOperation, AuditSeverity::Info,
        "ops", "10.0.0.4", "DRAIN", "node-3", "OK", "");
    log.record(AuditCategory::TopologyChange, AuditSeverity::Critical,
        "system", "10.0.0.5", "LEADER_CHANGE", "shard-0", "OK", "");

    let lines = log.export_jsonl(1);
    assert_eq!(lines.len(), 5);

    // Each line is valid JSON-like structure with required fields
    for line in &lines {
        assert!(line.contains("\"id\":"));
        assert!(line.contains("\"ts\":"));
        assert!(line.contains("\"cat\":"));
        assert!(line.contains("\"sev\":"));
        assert!(line.contains("\"hash\":"));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — Auto-Rebalance on Node Join/Leave
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_rebalance_on_node_join() {
    let rb = AutoRebalancer::new(RebalanceConfig::default());

    // Initial: 2 nodes, 6 shards unevenly distributed
    let mut assignments = HashMap::new();
    assignments.insert(NodeId(1), vec![0, 1, 2, 3, 4, 5]);
    assignments.insert(NodeId(2), vec![]);

    // New node joins
    let nodes = vec![NodeId(1), NodeId(2), NodeId(3)];
    assignments.insert(NodeId(3), vec![]);

    let plan = rb.compute_plan(&assignments, &nodes);
    // Should move shards from node 1 to nodes 2 and 3
    assert!(plan.len() >= 2);
    for (_, from, to) in &plan {
        assert_eq!(*from, NodeId(1)); // all from overloaded node
        assert!(to.0 == 2 || to.0 == 3);
    }

    // Execute migrations
    for (shard, from, to) in &plan {
        let tid = rb.schedule_migration(*shard, *from, *to).unwrap();
        rb.advance(tid, MigrationState::Preparing);
        rb.advance(tid, MigrationState::Copying);
        rb.advance(tid, MigrationState::CatchingUp);
        rb.advance(tid, MigrationState::Cutover);
        rb.advance(tid, MigrationState::Completed);
    }

    assert_eq!(rb.active_migrations(), 0);
    assert!(rb.metrics.migrations_completed.load(std::sync::atomic::Ordering::Relaxed) >= 2);
}

#[test]
fn test_rebalance_sla_pause() {
    let rb = AutoRebalancer::new(RebalanceConfig { max_concurrent: 3, ..Default::default() });

    let t1 = rb.schedule_migration(0, NodeId(1), NodeId(2)).unwrap();
    rb.advance(t1, MigrationState::Preparing);

    // SLA impact detected — pause
    rb.pause();
    assert!(rb.is_paused());
    assert!(rb.schedule_migration(1, NodeId(1), NodeId(3)).is_none());

    // SLA recovers — resume
    rb.resume();
    assert!(rb.schedule_migration(1, NodeId(1), NodeId(3)).is_some());
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 — SLO Engine Accuracy
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_slo_engine_multi_slo_evaluation() {
    let engine = SloEngine::new(100_000);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    engine.define(SloDefinition {
        id: "write-avail".into(),
        metric: SloMetricType::Availability,
        target: 0.999,
        window_secs: 3600,
        description: "Write availability".into(),
    });
    engine.define(SloDefinition {
        id: "read-p99".into(),
        metric: SloMetricType::LatencyP99,
        target: 20.0,
        window_secs: 3600,
        description: "Read p99 latency (ms)".into(),
    });
    engine.define(SloDefinition {
        id: "repl-lag".into(),
        metric: SloMetricType::ReplicationLag,
        target: 1000.0,
        window_secs: 3600,
        description: "Max replication lag (LSN)".into(),
    });

    // Good availability
    for i in 0..10000u64 {
        engine.record_sample_at(SloMetricType::Availability, if i == 5000 { 0.0 } else { 1.0 }, now - 1800 + i);
    }
    // Good latency
    for i in 0..1000u64 {
        engine.record_sample_at(SloMetricType::LatencyP99, 5.0 + (i as f64) * 0.01, now - 1800 + i);
    }
    // Good replication lag
    for i in 0..100u64 {
        engine.record_sample_at(SloMetricType::ReplicationLag, 50.0 + i as f64, now - 1800 + i * 36);
    }

    let evals = engine.evaluate_all();
    assert_eq!(evals.len(), 3);
    assert!(evals.iter().all(|e| e.met)); // All SLOs met
}

#[test]
fn test_slo_error_budget_depletion() {
    let engine = SloEngine::new(100_000);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    engine.define(SloDefinition {
        id: "avail".into(),
        metric: SloMetricType::Availability,
        target: 0.99, // 99% target → 1% error budget
        window_secs: 3600,
        description: "".into(),
    });

    // 95% availability → 5% errors, well over 1% budget
    for i in 0..100u64 {
        engine.record_sample_at(
            SloMetricType::Availability,
            if i % 20 == 0 { 0.0 } else { 1.0 },
            now - 1800 + i,
        );
    }

    let evals = engine.evaluate_all();
    assert!(!evals[0].met);
    assert!(evals[0].error_budget_remaining <= 0.0);
}

// ═══════════════════════════════════════════════════════════════════════════
// §10 — Incident Auto-Correlation
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_incident_full_lifecycle() {
    let timeline = IncidentTimeline::new(1000, 300);

    // Phase 1: Normal operation
    timeline.record_event(
        TimelineEventType::OpsAction("routine check".into()),
        IncidentSeverity::Low,
        "Routine health check passed",
    );

    // Phase 2: Failure cascade
    let e1 = timeline.record_event(
        TimelineEventType::NodeFailure(NodeId(2)),
        IncidentSeverity::Critical,
        "Node 2 crashed — disk failure",
    );
    let e2 = timeline.record_event(
        TimelineEventType::LeaderChange { shard_id: 1, old: Some(NodeId(2)), new: NodeId(3) },
        IncidentSeverity::High,
        "Leader election triggered for shard 1",
    );
    let e3 = timeline.record_event(
        TimelineEventType::SloBreached("write-avail".into()),
        IncidentSeverity::High,
        "Write availability dropped below 99.9%",
    );

    // Auto-correlate
    let inc_id = timeline.auto_correlate().unwrap();
    let events = timeline.events_for_incident(inc_id);
    assert!(events.len() >= 2);

    // Resolve
    timeline.resolve_incident(
        inc_id,
        "Node 2 disk failure triggered leader election cascade",
        "~10s write unavailability on shard 1",
    );

    let incidents = timeline.all_incidents();
    assert_eq!(incidents.len(), 1);
    assert!(incidents[0].resolved_at.is_some());
    assert!(timeline.open_incidents().is_empty());
}

// ═══════════════════════════════════════════════════════════════════════════
// §11 — Full Enterprise Lifecycle Orchestration
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_full_enterprise_lifecycle() {
    // Setup: Control plane, registry, security, audit
    let ha = ControllerHAGroup::new(1, vec![(1, "ctrl:9000".into())]);
    ha.elect_self();

    let store = ConsistentMetadataStore::new(true);
    let registry = NodeRegistry::new(Duration::from_millis(100), Duration::from_millis(300));
    let placement = ShardPlacementManager::new();
    let authn = AuthnManager::new(5, 300);
    let rbac = EnterpriseRbac::new();
    let cert_mgr = CertificateManager::new(true);
    let backup = BackupOrchestrator::new(30);
    let audit = EnterpriseAuditLog::new(1000, "enterprise-key");
    let slo_engine = SloEngine::new(10000);
    let timeline = IncidentTimeline::new(1000, 300);

    // Step 1: Create admin user
    let admin_id = authn.create_user("admin", "admin_hash");
    rbac.set_superuser(admin_id);
    audit.record(AuditCategory::Authentication, AuditSeverity::Info,
        "system", "localhost", "CREATE_USER", "admin", "OK", "superuser");

    // Step 2: Register data nodes
    for i in 1..=3 {
        registry.register(NodeId(i), format!("data{}:5000", i), NodeCapabilities::default());
        store.put(MetadataDomain::NodeRegistry, &format!("node-{}", i), "online", "system");
    }

    // Step 3: Setup shard placements
    for sid in 0..4u64 {
        placement.set_placement(ShardPlacement {
            shard_id: sid,
            leader: Some(NodeId((sid % 3 + 1) as u64)),
            replicas: vec![NodeId((sid % 3 + 2) as u64 % 3 + 1), NodeId((sid + 2) % 3 + 1)],
            target_replica_count: 2,
            state: ShardPlacementState::Healthy,
        });
    }

    // Step 4: Load TLS certs
    for link in &[TlsLinkType::ClientToGateway, TlsLinkType::GatewayToDataNode,
                  TlsLinkType::DataNodeToReplica, TlsLinkType::DataNodeToController] {
        cert_mgr.load_cert(CertificateRecord {
            link_type: *link,
            cert_path: format!("/certs/{:?}.pem", link),
            key_path: format!("/certs/{:?}.key", link),
            ca_path: Some("/certs/ca.pem".into()),
            fingerprint: format!("fp-{:?}", link),
            not_before: 0, not_after: u64::MAX, loaded_at: 0, is_active: true,
        });
    }
    assert!(cert_mgr.all_links_covered().is_empty());

    // Step 5: Define SLOs
    slo_engine.define(SloDefinition {
        id: "write-avail".into(),
        metric: SloMetricType::Availability,
        target: 0.999,
        window_secs: 3600,
        description: "Write availability".into(),
    });

    // Step 6: Simulate normal operations
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    for i in 0..100u64 {
        slo_engine.record_sample_at(SloMetricType::Availability, 1.0, now - 1800 + i);
    }

    // Step 7: Take a backup
    let bid = backup.schedule_backup(
        EnterpriseBackupType::Full,
        BackupTarget::Local { path: "/backup/enterprise".into() },
        "admin",
    );
    backup.start_backup(bid, 10_000);
    backup.complete_backup(bid, 2_000_000_000, 20, 15_000, 0xBEEF);
    audit.record(AuditCategory::BackupRestore, AuditSeverity::Info,
        "admin", "localhost", "FULL_BACKUP", "all", "SUCCESS", "2GB");

    // Step 8: Verify SLOs
    let evals = slo_engine.evaluate_all();
    assert!(evals.iter().all(|e| e.met));

    // Step 9: Verify audit integrity
    assert!(audit.verify_integrity());
    assert!(audit.total() >= 2);

    // Step 10: Record timeline success
    timeline.record_event(
        TimelineEventType::OpsAction("enterprise lifecycle test complete".into()),
        IncidentSeverity::Low,
        "All systems operational",
    );

    // Final assertions
    assert!(ha.is_leader());
    assert_eq!(registry.node_count(), 3);
    assert_eq!(placement.all_placements().len(), 4);
    assert_eq!(slo_engine.definition_count(), 1);
}
