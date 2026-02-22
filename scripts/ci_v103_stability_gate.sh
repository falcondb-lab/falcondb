#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# FalconDB v1.0.3 — Stability, Determinism & Trust Hardening CI Gate
# ═══════════════════════════════════════════════════════════════════════
#
# Gates:
#   1. §1-§8 stability hardening unit tests pass (37)
#   2. §9 stress & regression tests pass (8)
#   3. v1.0.2 failover×txn hardening tests still pass (35)
#   4. v1.0.2 failover×txn test matrix still passes (20)
#   5. Transaction state machine tests pass
#   6. Full workspace test suite passes
#   7. v1.0.1 zero-panic baseline maintained
#
# Usage: bash scripts/ci_v103_stability_gate.sh
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

PASS=0
FAIL=0
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "════════════════════════════════════════════════════════════════"
echo "  FalconDB v1.0.3 — Stability, Determinism & Trust Gate"
echo "════════════════════════════════════════════════════════════════"
echo ""

run_gate() {
    local gate_num="$1"
    local gate_name="$2"
    local test_cmd="$3"

    echo -n "Gate $gate_num: $gate_name ... "
    if eval "$test_cmd" 2>&1 | grep -q "test result: ok"; then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        FAIL=$((FAIL + 1))
    fi
}

# ── Gate 1: §1-§8 stability hardening unit tests ──────────────────────
run_gate 1 "Stability hardening unit tests (§1-§8)" \
    "cargo test -p falcon_cluster stability_hardening::tests"

# ── Gate 2: §9 stress & regression tests ──────────────────────────────
run_gate 2 "Stress & regression tests (§9)" \
    "cargo test -p falcon_cluster stability_stress"

# ── Gate 3: v1.0.2 failover×txn hardening tests ──────────────────────
run_gate 3 "v1.0.2 failover×txn hardening tests" \
    "cargo test -p falcon_cluster failover_txn_hardening"

# ── Gate 4: v1.0.2 failover×txn test matrix ──────────────────────────
run_gate 4 "v1.0.2 failover×txn test matrix" \
    "cargo test -p falcon_cluster failover_txn_matrix"

# ── Gate 5: Transaction state machine tests ───────────────────────────
run_gate 5 "Transaction state machine tests" \
    "cargo test -p falcon_txn txn_state_machine"

# ── Gate 6: Full workspace test suite ─────────────────────────────────
echo -n "Gate 6: Full workspace test suite ... "
RESULT=$(cargo test --workspace 2>&1)
FAILURES=$(echo "$RESULT" | grep "FAILED" | wc -l)
if [ "$FAILURES" -eq 0 ]; then
    TOTAL=$(echo "$RESULT" | grep "test result: ok" | awk '{sum += $4} END {print sum}')
    echo "PASS ($TOTAL tests)"
    PASS=$((PASS + 1))
else
    echo "FAIL: $FAILURES test failure(s)"
    FAIL=$((FAIL + 1))
fi

# ── Gate 7: v1.0.1 zero-panic baseline maintained ────────────────────
echo -n "Gate 7: v1.0.1 zero-panic baseline maintained ... "
VIOLATIONS=0
for crate_src in \
    "$ROOT_DIR/crates/falcon_protocol_pg/src" \
    "$ROOT_DIR/crates/falcon_server/src" \
    "$ROOT_DIR/crates/falcon_txn/src" \
    "$ROOT_DIR/crates/falcon_storage/src" \
    "$ROOT_DIR/crates/falcon_cluster/src" \
    "$ROOT_DIR/crates/falcon_common/src" \
    "$ROOT_DIR/crates/falcon_executor/src" \
    "$ROOT_DIR/crates/falcon_planner/src" \
    "$ROOT_DIR/crates/falcon_sql_frontend/src" \
    "$ROOT_DIR/crates/falcon_observability/src"; do
    if [ -d "$crate_src" ]; then
        while IFS= read -r file; do
            case "$file" in
                *tests.rs|*test_*|*/tests/*|*build.rs) continue ;;
            esac
            in_test=0
            while IFS= read -r line; do
                echo "$line" | grep -qE '#\[cfg\(test\)\]|mod tests' && in_test=1
                if [ "$in_test" -eq 0 ]; then
                    echo "$line" | grep -qE '\.unwrap\(|\.expect\(|panic!\(' && VIOLATIONS=$((VIOLATIONS + 1))
                fi
            done < "$file"
        done < <(find "$crate_src" -name "*.rs" -type f 2>/dev/null)
    fi
done

if [ "$VIOLATIONS" -eq 0 ]; then
    echo "PASS (0 violations)"
    PASS=$((PASS + 1))
else
    echo "FAIL ($VIOLATIONS violations)"
    FAIL=$((FAIL + 1))
fi

# ── Summary ────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  v1.0.3 STABILITY & DETERMINISM GATE SUMMARY"
echo "════════════════════════════════════════════════════════════════"
echo "  Passed: $PASS / $((PASS + FAIL))"
echo "  Failed: $FAIL"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "  ❌ GATE FAILED — v1.0.3 release blocked"
    exit 1
else
    echo ""
    echo "  ✅ GATE PASSED — v1.0.3 release cleared"
    exit 0
fi
