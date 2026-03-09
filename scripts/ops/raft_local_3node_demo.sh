#!/usr/bin/env bash
# FalconDB — Raft 3-Node Local Demo / CI Gate
#
# Validates:
#   1. falcon_raft lib tests (unit + in-process multi-node)
#   2. Integration tests (gRPC transport, fault injection, snapshot)
#   3. Proto compilation
#
# Usage:
#   ./scripts/raft_local_3node_demo.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "═══════════════════════════════════════════════════════════"
echo "  FalconDB — Raft Transport CI Gate"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ── Gate 1: Proto compilation ──
echo "[Gate 1] Checking falcon_proto compilation (incl. raft proto)..."
if cargo check -p falcon_proto --quiet 2>/dev/null; then
    echo "  ✓ PASS: falcon_proto compiles (raft + replication protos)"
else
    echo "  ✗ FAIL: falcon_proto compilation"
    exit 1
fi

# ── Gate 2: falcon_raft compilation ──
echo "[Gate 2] Checking falcon_raft compilation..."
if cargo check -p falcon_raft --quiet 2>/dev/null; then
    echo "  ✓ PASS: falcon_raft compiles"
else
    echo "  ✗ FAIL: falcon_raft compilation"
    exit 1
fi

# ── Gate 3: Unit tests ──
echo "[Gate 3] Running falcon_raft lib tests..."
LIB_OUTPUT=$(cargo test -p falcon_raft --lib 2>&1)
LIB_COUNT=$(echo "$LIB_OUTPUT" | grep "test result" | head -1 | grep -oP '\d+ passed' | grep -oP '\d+')
if echo "$LIB_OUTPUT" | grep -q "0 failed"; then
    echo "  ✓ PASS: $LIB_COUNT lib tests passed"
else
    echo "  ✗ FAIL: lib tests"
    echo "$LIB_OUTPUT" | tail -5
    exit 1
fi

# ── Gate 4: Integration tests ──
echo "[Gate 4] Running integration tests (gRPC + fault injection)..."
INT_OUTPUT=$(cargo test -p falcon_raft --test transport_grpc 2>&1)
INT_COUNT=$(echo "$INT_OUTPUT" | grep "test result" | head -1 | grep -oP '\d+ passed' | grep -oP '\d+')
if echo "$INT_OUTPUT" | grep -q "0 failed"; then
    echo "  ✓ PASS: $INT_COUNT integration tests passed"
else
    echo "  ✗ FAIL: integration tests"
    echo "$INT_OUTPUT" | tail -10
    exit 1
fi

# ── Gate 5: Verify key modules exist ──
echo "[Gate 5] Verifying module structure..."
MODULES=(
    "crates/falcon_raft/src/transport/mod.rs"
    "crates/falcon_raft/src/transport/grpc.rs"
    "crates/falcon_raft/src/transport/fault.rs"
    "crates/falcon_raft/src/server.rs"
    "crates/falcon_raft/src/network.rs"
    "crates/falcon_proto/proto/falcon_raft.proto"
)
for mod in "${MODULES[@]}"; do
    if [ -f "$ROOT_DIR/$mod" ]; then
        echo "  ✓ $mod"
    else
        echo "  ✗ MISSING: $mod"
        exit 1
    fi
done

echo ""
echo "═══════════════════════════════════════════════════════════"
TOTAL=$((LIB_COUNT + INT_COUNT))
echo "  Raft Transport CI Gate: ALL GATES PASSED"
echo "  Total tests: $TOTAL ($LIB_COUNT lib + $INT_COUNT integration)"
echo "═══════════════════════════════════════════════════════════"
