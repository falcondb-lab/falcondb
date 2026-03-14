#!/usr/bin/env bash
# ci_clippy_gate.sh — Clippy lint gate for CI pipelines.
# Fails the build if any Clippy warnings are present.
#
# Usage:
#   ./scripts/ci_clippy_gate.sh
#
# Exit codes:
#   0 — no warnings
#   1 — Clippy found warnings or errors

set -euo pipefail

echo "=== FalconDB Clippy Gate ==="
echo ""

# Run clippy on the entire workspace with warnings as errors.
# --all-targets includes tests, benches, and examples.
if cargo clippy --workspace --all-targets -- -D warnings 2>&1; then
    echo ""
    echo "✅ Clippy gate PASSED — zero warnings."
    exit 0
else
    echo ""
    echo "❌ Clippy gate FAILED — fix warnings above before merging."
    exit 1
fi
