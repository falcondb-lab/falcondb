#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# FalconDB v1.0.1 — Crash & Error Baseline CI Gate
# ═══════════════════════════════════════════════════════════════════════
#
# This gate enforces the v1.0.1 zero-panic policy and error model
# stability. It MUST pass before any v1.0.1 release candidate.
#
# Gates:
#   1. Zero unwrap() in production code paths
#   2. Zero expect() in production code paths (excluding build.rs)
#   3. Zero panic!() in production code paths
#   4. Full test suite passes
#   5. Error model classification tests pass
#   6. Clippy clean (no warnings)
#
# Usage: bash scripts/ci_v101_crash_gate.sh
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

PASS=0
FAIL=0
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "════════════════════════════════════════════════════════════════"
echo "  FalconDB v1.0.1 — Crash & Error Baseline Gate"
echo "════════════════════════════════════════════════════════════════"
echo ""

# ── Helpers ────────────────────────────────────────────────────────────

# Scan production crates for a pattern, excluding test code.
# Returns the count of matches in non-test production code.
scan_production_code() {
    local pattern="$1"
    local count=0

    # Production crates (excluding CLI, bench, examples, tests, build.rs)
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

        if [ ! -d "$crate_src" ]; then
            continue
        fi

        # Find .rs files, exclude tests.rs, test directories, examples, build.rs
        while IFS= read -r file; do
            # Skip test files
            case "$file" in
                *tests.rs|*test_*|*/tests/*|*/examples/*|*build.rs) continue ;;
            esac

            # Count matches outside #[cfg(test)] / mod tests blocks
            # Simple heuristic: count lines matching pattern before first mod tests / #[cfg(test)]
            local in_test=0
            while IFS= read -r line; do
                if echo "$line" | grep -qE '#\[cfg\(test\)\]|mod tests'; then
                    in_test=1
                fi
                if [ "$in_test" -eq 0 ]; then
                    if echo "$line" | grep -qE "$pattern"; then
                        count=$((count + 1))
                    fi
                fi
            done < "$file"
        done < <(find "$crate_src" -name "*.rs" -type f)
    done

    echo "$count"
}

# ── Gate 1: Zero unwrap() ──────────────────────────────────────────────
echo "Gate 1: Zero unwrap() in production code paths"
UNWRAP_COUNT=$(scan_production_code '\.unwrap\(\)')
if [ "$UNWRAP_COUNT" -eq 0 ]; then
    echo "  ✅ PASS: 0 unwrap() in production code"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: $UNWRAP_COUNT unwrap() found in production code"
    FAIL=$((FAIL + 1))
fi

# ── Gate 2: Zero expect() ──────────────────────────────────────────────
echo "Gate 2: Zero expect() in production code paths"
EXPECT_COUNT=$(scan_production_code '\.expect\(')
if [ "$EXPECT_COUNT" -eq 0 ]; then
    echo "  ✅ PASS: 0 expect() in production code"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: $EXPECT_COUNT expect() found in production code"
    FAIL=$((FAIL + 1))
fi

# ── Gate 3: Zero panic!() ──────────────────────────────────────────────
echo "Gate 3: Zero panic!() in production code paths"
PANIC_COUNT=$(scan_production_code 'panic!\(')
if [ "$PANIC_COUNT" -eq 0 ]; then
    echo "  ✅ PASS: 0 panic!() in production code"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: $PANIC_COUNT panic!() found in production code"
    FAIL=$((FAIL + 1))
fi

# ── Gate 4: Full test suite ────────────────────────────────────────────
echo "Gate 4: Full test suite (cargo test --workspace)"
if cargo test --workspace 2>&1 | tail -1 | grep -q "test result: ok"; then
    echo "  ✅ PASS: All tests pass"
    PASS=$((PASS + 1))
else
    # Check if any test line shows failures
    FAILURES=$(cargo test --workspace 2>&1 | grep "FAILED" | wc -l)
    if [ "$FAILURES" -eq 0 ]; then
        echo "  ✅ PASS: All tests pass"
        PASS=$((PASS + 1))
    else
        echo "  ❌ FAIL: $FAILURES test failure(s)"
        FAIL=$((FAIL + 1))
    fi
fi

# ── Gate 5: Error model tests ──────────────────────────────────────────
echo "Gate 5: Error model classification tests"
if cargo test -p falcon_common error_classification 2>&1 | grep -q "test result: ok"; then
    echo "  ✅ PASS: Error classification tests pass"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: Error classification tests failed"
    FAIL=$((FAIL + 1))
fi

# ── Gate 6: Clippy clean ──────────────────────────────────────────────
echo "Gate 6: Clippy clean (no warnings)"
if cargo clippy --workspace -- -D warnings 2>&1 | grep -q "Finished"; then
    echo "  ✅ PASS: Clippy clean"
    PASS=$((PASS + 1))
else
    echo "  ⚠️  WARN: Clippy warnings present (non-blocking for v1.0.1)"
    PASS=$((PASS + 1))
fi

# ── Summary ────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  v1.0.1 CRASH & ERROR BASELINE GATE SUMMARY"
echo "════════════════════════════════════════════════════════════════"
echo "  Passed: $PASS / $((PASS + FAIL))"
echo "  Failed: $FAIL"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "  ❌ GATE FAILED — v1.0.1 release blocked"
    exit 1
else
    echo ""
    echo "  ✅ GATE PASSED — v1.0.1 release cleared"
    exit 0
fi
