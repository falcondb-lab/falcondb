#!/usr/bin/env bash
# CI gate for FalconDB v1.x P0 compatibility contract.
# Every merge to main must pass all gates below.
# See docs/compatibility_contract.md for the normative contract.

set -euo pipefail

PASS=0
FAIL=0

gate() {
    local name="$1"
    shift
    echo "══════════════════════════════════════════════════"
    echo "GATE: $name"
    echo "══════════════════════════════════════════════════"
    if "$@"; then
        echo "✅ PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "❌ FAIL: $name"
        FAIL=$((FAIL + 1))
    fi
    echo ""
}

# ── Gate 1: Membership tests ──
gate "P0-1 Membership unit tests" \
    cargo test -p falcon_cluster -- "membership"

# ── Gate 2: TLS tests ──
gate "P0-2 TLS unit tests" \
    cargo test -p falcon_protocol_pg -- "tls"

# ── Gate 3: Streaming tests ──
gate "P0-3 Streaming unit tests" \
    cargo test -p falcon_executor -- "row_stream"

# ── Gate 4: Doc contract IDs present in code ──
gate "P0-4 Contract ID consistency" bash -c '
    MISSING=0
    # Membership invariants
    for id in M-1 M-2 M-3 M-4 M-5; do
        if ! grep -rq "$id" crates/falcon_cluster/src/cluster/membership.rs; then
            echo "  MISSING: $id not found in membership.rs"
            MISSING=$((MISSING + 1))
        fi
    done
    # TLS invariants
    for id in TLS-1 TLS-2 TLS-3; do
        if ! grep -rq "$id" crates/falcon_protocol_pg/src/tls.rs crates/falcon_protocol_pg/src/server.rs; then
            echo "  MISSING: $id not found in tls.rs/server.rs"
            MISSING=$((MISSING + 1))
        fi
    done
    # Streaming invariants
    for id in STREAM-1 STREAM-2; do
        if ! grep -rq "$id" crates/falcon_executor/src/row_stream.rs; then
            echo "  MISSING: $id not found in row_stream.rs"
            MISSING=$((MISSING + 1))
        fi
    done
    if [ "$MISSING" -gt 0 ]; then
        echo "  $MISSING contract IDs missing from source code"
        exit 1
    fi
    echo "  All contract IDs found in source"
'

# ── Gate 5: Full workspace tests ──
gate "Full workspace tests" \
    cargo test --workspace --no-fail-fast

# ── Gate 6: Clippy ──
gate "Clippy (warnings as errors)" \
    cargo clippy --workspace -- -D warnings 2>/dev/null || true

# ── Summary ──
echo "══════════════════════════════════════════════════"
echo "RESULTS: $PASS passed, $FAIL failed"
echo "══════════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
    echo "❌ P0 CONTRACT GATE FAILED"
    exit 1
fi

echo "✅ P0 CONTRACT GATE PASSED"
exit 0
