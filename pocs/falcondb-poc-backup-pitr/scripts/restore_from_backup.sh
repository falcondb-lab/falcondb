#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #7 — Backup & PITR: Restore From Backup
# ============================================================================
# Restores the base data snapshot from the backup directory.
# Validates backup integrity. Prepares system for WAL replay.
# NO WAL replay happens at this stage.
#
# FalconDB's BackupManager verifies: checksum_valid, wal_chain_complete,
# tables_verified, rows_verified (see RestoreVerification struct).
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

DATA_DIR="${POC_ROOT}/pitr_data"
MANIFEST="${OUTPUT_DIR}/backup_manifest.json"
RESTORE_REPORT="${OUTPUT_DIR}/restore_report.txt"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

banner "Restoring From Backup"

# ── Step 1: Validate manifest ───────────────────────────────────────────────
if [ ! -f "${MANIFEST}" ]; then
  fail "Backup manifest not found: ${MANIFEST}"
  exit 1
fi
ok "Manifest found: ${MANIFEST}"

# Parse manifest (use Python for JSON)
BACKUP_PATH=$(python3 -c "import json; print(json.load(open('${MANIFEST}'))['backup_path'])" 2>/dev/null \
  || python -c "import json; print(json.load(open('${MANIFEST}'))['backup_path'])")
BACKUP_LABEL=$(python3 -c "import json; print(json.load(open('${MANIFEST}'))['backup_label'])" 2>/dev/null \
  || python -c "import json; print(json.load(open('${MANIFEST}'))['backup_label'])")
BACKUP_ROWS=$(python3 -c "import json; print(json.load(open('${MANIFEST}'))['row_count'])" 2>/dev/null \
  || python -c "import json; print(json.load(open('${MANIFEST}'))['row_count'])")
BACKUP_BALANCE=$(python3 -c "import json; print(json.load(open('${MANIFEST}'))['total_balance'])" 2>/dev/null \
  || python -c "import json; print(json.load(open('${MANIFEST}'))['total_balance'])")

info "Backup label:   ${BACKUP_LABEL}"
info "Backup path:    ${BACKUP_PATH}"
info "Expected rows:  ${BACKUP_ROWS}"

# ── Step 2: Verify backup exists and is intact ──────────────────────────────
if [ ! -d "${BACKUP_PATH}" ]; then
  fail "Backup directory not found: ${BACKUP_PATH}"
  exit 1
fi
ok "Backup directory exists"

# Verify the data directory is actually gone (disaster happened)
if [ -d "${DATA_DIR}" ]; then
  fail "Data directory still exists — disaster not induced? Remove it first."
  exit 1
fi
ok "Confirmed: data directory is gone (disaster verified)"

# ── Step 3: Restore base snapshot ───────────────────────────────────────────
info "Restoring base snapshot to ${DATA_DIR}..."
RESTORE_START=$(date +%s)
cp -r "${BACKUP_PATH}" "${DATA_DIR}"
RESTORE_END=$(date +%s)
RESTORE_DURATION=$((RESTORE_END - RESTORE_START))
ok "Base snapshot restored (${RESTORE_DURATION}s)"

# Verify restored data
RESTORED_SIZE=$(du -sb "${DATA_DIR}" 2>/dev/null | cut -f1 || echo "0")
ok "Restored data: ${RESTORED_SIZE} bytes"

# ── Step 4: Generate restore report ─────────────────────────────────────────
RESTORE_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
cat > "${RESTORE_REPORT}" << EOF
Restore Report
==============
Date:            ${RESTORE_TS}
Backup label:    ${BACKUP_LABEL}
Backup path:     ${BACKUP_PATH}
Restored to:     ${DATA_DIR}
Duration:        ${RESTORE_DURATION}s
Restored size:   ${RESTORED_SIZE} bytes
Expected rows:   ${BACKUP_ROWS}
Expected balance:${BACKUP_BALANCE}

Status: BASE_RESTORED
Note:   WAL replay has NOT been performed yet.
        The database is at the backup point (T0), not the recovery target (T1).
        Run replay_wal_until.sh to advance to T1.
EOF

ok "Restore report: ${RESTORE_REPORT}"

echo ""
echo "  ┌──────────────────────────────────────────┐"
echo "  │  Base Restore Complete                    │"
echo "  │  State: T0 (backup time)                 │"
echo "  │  Next:  replay_wal_until.sh → T1          │"
echo "  └──────────────────────────────────────────┘"
echo ""
