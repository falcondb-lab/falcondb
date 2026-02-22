#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# FalconDB v1.0.2 — Failover × Transaction Test Matrix CI Gate
# ═══════════════════════════════════════════════════════════════════════
#
# Two modes:
#   PR Gate (fast):    SS-01, XS-02, ID-01
#   Nightly (full):    All 20 tests in the failover_txn_matrix
#
# Usage:
#   bash scripts/ci_v102_test_matrix.sh          # Full nightly
#   bash scripts/ci_v102_test_matrix.sh --pr     # PR gate only
#
# Failing any single invariant blocks release.
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

MODE="${1:-nightly}"
PASS=0
FAIL=0

echo "════════════════════════════════════════════════════════════════"
echo "  FalconDB v1.0.2 — Failover × Transaction Test Matrix"
echo "  Mode: $MODE"
echo "════════════════════════════════════════════════════════════════"
echo ""

run_test() {
    local test_id="$1"
    local test_name="$2"

    echo -n "  $test_id: $test_name ... "
    if cargo test -p falcon_cluster "failover_txn_matrix::$test_name" -- --exact 2>&1 | grep -q "1 passed"; then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        FAIL=$((FAIL + 1))
    fi
}

if [ "$MODE" = "--pr" ]; then
    echo "── PR Gate (fast): 3 critical-path tests ──"
    echo ""
    run_test "SS-01" "test_ss01_kill_leader_during_execution"
    run_test "XS-02" "test_xs02_kill_coordinator_during_prepare"
    run_test "ID-01" "test_id01_client_commit_retry"
else
    echo "── Nightly / Release Gate: Full Test Matrix ──"
    echo ""

    echo "─ SS: Single-Shard × Failover ─"
    run_test "SS-01" "test_ss01_kill_leader_during_execution"
    run_test "SS-02" "test_ss02_kill_leader_before_commit"
    run_test "SS-03" "test_ss03_client_disconnect_during_commit_response"
    run_test "SS-04" "test_ss04_replica_lag_plus_leader_switch"
    echo ""

    echo "─ XS: Cross-Shard × Failover ─"
    run_test "XS-01" "test_xs01_kill_coordinator_before_prepare"
    run_test "XS-02" "test_xs02_kill_coordinator_during_prepare"
    run_test "XS-03" "test_xs03_kill_coordinator_after_prepare_before_decision"
    run_test "XS-04" "test_xs04_kill_coordinator_during_commit_propagation"
    run_test "XS-05" "test_xs05_kill_participant_leader_during_prepare"
    run_test "XS-06" "test_xs06_kill_participant_leader_after_prepare"
    run_test "XS-07" "test_xs07_network_partition_coordinator_shard"
    run_test "XS-08" "test_xs08_network_partition_between_shards"
    echo ""

    echo "─ CH: Churn & Stability ─"
    run_test "CH-01" "test_ch01_repeated_leader_failover_ss_workload"
    run_test "CH-02" "test_ch02_repeated_coordinator_restart_xs_workload"
    echo ""

    echo "─ ID: Idempotency & Replay ─"
    run_test "ID-01" "test_id01_client_commit_retry"
    run_test "ID-02" "test_id02_protocol_message_replay"
    echo ""

    echo "─ Extended: TTL, Timeout, Observability ─"
    run_test "TTL-01"  "test_indoubt_ttl_enforcement_force_abort"
    run_test "BLK-01"  "test_blocked_txn_timeout_during_failover"
    run_test "OBS-01"  "test_observability_and_admin_tooling"
    run_test "MET-01"  "test_failover_coordinator_metrics_complete"
fi

# ── Summary ────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  TEST MATRIX SUMMARY ($MODE)"
echo "════════════════════════════════════════════════════════════════"
echo "  Passed: $PASS / $((PASS + FAIL))"
echo "  Failed: $FAIL"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "  ❌ TEST MATRIX FAILED — v1.0.2 release blocked"
    exit 1
else
    echo ""
    echo "  ✅ TEST MATRIX PASSED — v1.0.2 LTS qualified"
    exit 0
fi
