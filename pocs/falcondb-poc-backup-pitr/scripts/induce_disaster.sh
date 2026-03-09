#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #7 — Backup & PITR: Induce Disaster
# ============================================================================
# Deliberately destroys the live database. This is irreversible.
# The ONLY way to recover is from the backup + WAL replay.
#
# Anti-cheating: the data directory is completely wiped.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

DATA_DIR="${POC_ROOT}/pitr_data"
PID_FILE="${OUTPUT_DIR}/falcon.pid"

RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

banner "DISASTER SIMULATION"

echo -e "  ${RED}${BOLD}╔════════════════════════════════════════════════╗${NC}"
echo -e "  ${RED}${BOLD}║  WARNING: This will destroy the live database  ║${NC}"
echo -e "  ${RED}${BOLD}║  Data directory will be WIPED COMPLETELY.      ║${NC}"
echo -e "  ${RED}${BOLD}║  The only recovery is from backup + WAL.      ║${NC}"
echo -e "  ${RED}${BOLD}╚════════════════════════════════════════════════╝${NC}"
echo ""

# Record what we're about to destroy
if [ -d "${DATA_DIR}" ]; then
  DATA_SIZE=$(du -sb "${DATA_DIR}" 2>/dev/null | cut -f1 || echo "unknown")
  info "Data directory: ${DATA_DIR} (${DATA_SIZE} bytes)"
fi

# Step 1: Stop FalconDB process
info "Stopping FalconDB..."
if [ -f "${PID_FILE}" ]; then
  FPID=$(cat "${PID_FILE}" | tr -d '[:space:]')
  if kill -0 "${FPID}" 2>/dev/null; then
    kill -9 "${FPID}" 2>/dev/null || true
    sleep 1
    ok "FalconDB killed (pid ${FPID})"
  else
    info "FalconDB (pid ${FPID}) already stopped"
  fi
  rm -f "${PID_FILE}"
else
  info "No PID file found — FalconDB may already be stopped"
fi

# Step 2: DESTROY the data directory (irreversible)
info "Destroying data directory..."
rm -rf "${DATA_DIR}"
ok "Data directory DESTROYED: ${DATA_DIR}"

# Verify destruction
if [ -d "${DATA_DIR}" ]; then
  echo -e "  ${RED}ERROR: Data directory still exists!${NC}"
  exit 1
fi

# Step 3: Log the disaster event
DISASTER_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "${DISASTER_TS} | DISASTER | data_dir=${DATA_DIR} destroyed" >> "${OUTPUT_DIR}/event_timeline.txt" 2>/dev/null || true

echo ""
echo -e "  ${RED}${BOLD}Database DESTROYED at ${DISASTER_TS}${NC}"
echo ""
echo "  The database is gone. The data directory no longer exists."
echo "  The ONLY way to recover is:"
echo "    1. Restore from backup  (restore_from_backup.sh)"
echo "    2. Replay WAL to T1     (replay_wal_until.sh)"
echo ""
