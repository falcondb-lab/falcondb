#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# FalconDB v1.0.4 — Determinism & Failure Safety CI Gate
# ═══════════════════════════════════════════════════════════════════════════
#
# Run: bash scripts/ci_v104_determinism_gate.sh
#
# Gates:
#   1. Determinism hardening unit tests (§1–§6)
#   2. Stability hardening tests (v1.0.3 regression)
#   3. Failover txn hardening tests (v1.0.2 regression)
#   4. Full workspace test suite
#   5. Clippy clean
#   6. Resource contract validation (compile-time)
#   7. Documentation consistency check

set -euo pipefail

PASS=0
FAIL=0
TOTAL=0

gate() {
    local name="$1"
    shift
    TOTAL=$((TOTAL + 1))
    echo ""
    echo "════════════════════════════════════════════"
    echo "GATE $TOTAL: $name"
    echo "════════════════════════════════════════════"
    if "$@"; then
        echo "✅ PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "❌ FAIL: $name"
        FAIL=$((FAIL + 1))
    fi
}

# ── Gate 1: Determinism hardening unit tests ──
gate "v1.0.4 determinism hardening tests" \
    cargo test -p falcon_cluster determinism_hardening -- --nocapture

# ── Gate 2: Stability hardening tests (v1.0.3 regression) ──
gate "v1.0.3 stability hardening tests" \
    cargo test -p falcon_cluster stability_hardening -- --nocapture

# ── Gate 3: Failover txn hardening tests (v1.0.2 regression) ──
gate "v1.0.2 failover txn hardening tests" \
    cargo test -p falcon_cluster failover_txn -- --nocapture

# ── Gate 4: Full workspace test suite ──
gate "Full workspace tests" \
    cargo test --workspace

# ── Gate 5: Clippy clean ──
gate "Clippy clean (no warnings)" \
    cargo clippy --workspace --all-targets -- -D warnings

# ── Gate 6: Resource contract validation ──
gate "Resource contract self-consistency" \
    cargo test -p falcon_cluster test_all_contracts_are_valid -- --nocapture

# ── Gate 7: Terminal state coverage ──
gate "Terminal state SQLSTATE coverage" \
    cargo test -p falcon_cluster test_all_reject_reasons_have_sqlstate -- --nocapture

echo ""
echo "════════════════════════════════════════════"
echo "RESULTS: $PASS/$TOTAL gates passed, $FAIL failed"
echo "════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
    echo "❌ v1.0.4 DETERMINISM GATE: FAIL"
    exit 1
fi

echo "✅ v1.0.4 DETERMINISM GATE: PASS"
exit 0
