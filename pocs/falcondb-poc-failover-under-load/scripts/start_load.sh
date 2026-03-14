#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #3 — Failover Under Load: Start Write Load
# ============================================================================
# Launches the tx_marker_writer in the background against the primary.
# Supports Rust binary, Python fallback, and psql fallback.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

export FALCON_HOST="${FALCON_HOST:-127.0.0.1}"
export FALCON_PORT="${FALCON_PORT:-5433}"
export FALCON_DB="${FALCON_DB:-falcon}"
export FALCON_USER="${FALCON_USER:-falcon}"
export FAILOVER_HOST="${FAILOVER_HOST:-127.0.0.1}"
export FAILOVER_PORT="${FAILOVER_PORT:-5434}"
export MARKER_COUNT="${MARKER_COUNT:-50000}"
export OUTPUT_DIR="${OUTPUT_DIR}"
export START_MARKER_ID="${START_MARKER_ID:-1}"
export STOP_FILE="${OUTPUT_DIR}/stop_writer"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

mkdir -p "${OUTPUT_DIR}"

# Clear previous output
> "${OUTPUT_DIR}/committed_markers.log"
rm -f "${OUTPUT_DIR}/stop_writer"

RUST_BIN="${POC_ROOT}/workload/target/release/tx_marker_writer"

if [ -f "${RUST_BIN}" ]; then
  info "Launching Rust workload generator (background)..."
  "${RUST_BIN}" > "${OUTPUT_DIR}/writer_stdout.log" 2>"${OUTPUT_DIR}/writer_stderr.log" &
elif command -v python3 &>/dev/null; then
  info "Launching Python fallback workload generator (background)..."
  python3 "${POC_ROOT}/workload/tx_marker_writer.py" > "${OUTPUT_DIR}/writer_stdout.log" 2>"${OUTPUT_DIR}/writer_stderr.log" &
else
  info "No Rust binary or Python found. Using psql fallback (background)..."
  bash "${SCRIPT_DIR}/_psql_writer.sh" > "${OUTPUT_DIR}/writer_stdout.log" 2>"${OUTPUT_DIR}/writer_stderr.log" &
fi

WRITER_PID=$!
echo "${WRITER_PID}" > "${OUTPUT_DIR}/writer.pid"

ok "Writer started (pid ${WRITER_PID}, ${MARKER_COUNT} markers)"
ok "Committed markers log: ${OUTPUT_DIR}/committed_markers.log"

# Wait a moment and verify it's running
sleep 2
if kill -0 "${WRITER_PID}" 2>/dev/null; then
  EARLY_COUNT=$(wc -l < "${OUTPUT_DIR}/committed_markers.log" 2>/dev/null | tr -d ' ')
  ok "Writer is running (${EARLY_COUNT} markers committed so far)"
else
  echo -e "  \033[0;31m✗\033[0m Writer exited unexpectedly. Check ${OUTPUT_DIR}/writer_stderr.log"
  exit 1
fi
