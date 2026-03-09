#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# FalconDB v1.0.2 — Transaction & Failover Hardening CI Gate
# ═══════════════════════════════════════════════════════════════════════
#
# Gates:
#   1. Failover×Txn hardening tests pass
#   2. Transaction state machine tests pass
#   3. In-doubt resolver tests pass
#   4. Cross-shard retry framework tests pass
#   5. HA/failover tests pass
#   6. Full workspace test suite passes
#   7. No new unwrap()/expect()/panic!() in production code (v1.0.1 baseline)
#
# Usage: bash scripts/ci_v102_failover_gate.sh
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

PASS=0
FAIL=0
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "════════════════════════════════════════════════════════════════"
echo "  FalconDB v1.0.2 — Transaction & Failover Hardening Gate"
echo "════════════════════════════════════════════════════════════════"
echo ""

run_gate() {
    local gate_num="$1"
    local gate_name="$2"
    local test_cmd="$3"

    echo "Gate $gate_num: $gate_name"
    if eval "$test_cmd" 2>&1 | grep -q "test result: ok"; then
        echo "  ✅ PASS"
        PASS=$((PASS + 1))
    else
        echo "  ❌ FAIL"
        FAIL=$((FAIL + 1))
    fi
}

# ── Gate 1: Failover×Txn hardening tests ──────────────────────────────
run_gate 1 "Failover×Txn hardening tests" \
    "cargo test -p falcon_cluster failover_txn_hardening"

# ── Gate 2: Transaction state machine tests ────────────────────────────
run_gate 2 "Transaction state machine tests" \
    "cargo test -p falcon_txn txn_state_machine"

# ── Gate 3: In-doubt resolver tests ───────────────────────────────────
run_gate 3 "In-doubt resolver tests" \
    "cargo test -p falcon_cluster indoubt_resolver"

# ── Gate 4: Cross-shard retry framework tests ──────────────────────────
run_gate 4 "Cross-shard retry framework tests" \
    "cargo test -p falcon_cluster cross_shard"

# ── Gate 5: HA/failover tests ─────────────────────────────────────────
run_gate 5 "HA/failover tests" \
    "cargo test -p falcon_cluster ha::"

# ── Gate 6: Full workspace test suite ──────────────────────────────────
echo "Gate 6: Full workspace test suite"
RESULT=$(cargo test --workspace 2>&1)
FAILURES=$(echo "$RESULT" | grep "FAILED" | wc -l)
if [ "$FAILURES" -eq 0 ]; then
    TOTAL=$(echo "$RESULT" | grep "test result: ok" | awk '{sum += $4} END {print sum}')
    echo "  ✅ PASS ($TOTAL tests)"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: $FAILURES test failure(s)"
    FAIL=$((FAIL + 1))
fi

# ── Gate 7: v1.0.1 zero-panic baseline maintained ─────────────────────
echo "Gate 7: v1.0.1 zero-panic baseline maintained"
# Reuse the v1.0.1 static scan logic (simplified)
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
        # Count .unwrap() / .expect( / panic!( in non-test .rs files
        while IFS= read -r file; do
            case "$file" in
                *tests.rs|*test_*|*/tests/*|*build.rs) continue ;;
            esac
            in_test=0
            while IFS= read -r line; do
                echo "$line" | grep -qE '#\[cfg\(test\)\]|mod tests' && in_test=1
                if [ "$in_test" -eq 0 ]; then
                    echo "$line" | grep -qE '\.unwrap\(\)|\.expect\(|panic!\(' && VIOLATIONS=$((VIOLATIONS + 1))
                fi
            done < "$file"
        done < <(find "$crate_src" -name "*.rs" -type f 2>/dev/null)
    fi
done

if [ "$VIOLATIONS" -eq 0 ]; then
    echo "  ✅ PASS: 0 unwrap/expect/panic in production code"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: $VIOLATIONS violations found"
    FAIL=$((FAIL + 1))
fi

# ── Summary ────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  v1.0.2 TRANSACTION & FAILOVER HARDENING GATE SUMMARY"
echo "════════════════════════════════════════════════════════════════"
echo "  Passed: $PASS / $((PASS + FAIL))"
echo "  Failed: $FAIL"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "  ❌ GATE FAILED — v1.0.2 release blocked"
    exit 1
else
    echo ""
    echo "  ✅ GATE PASSED — v1.0.2 release cleared"
    exit 0
fi
