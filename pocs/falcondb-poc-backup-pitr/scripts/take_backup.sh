#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #7 — Backup & PITR: Take Full Backup
# ============================================================================
# Creates a consistent full backup of the FalconDB data directory + WAL state.
# Produces a machine-readable backup manifest (JSON).
#
# FalconDB's BackupManager tracks: backup_id, snapshot_ts, start_lsn, end_lsn,
# total_bytes, table_count, checksum. The WalArchiver records the WAL position
# so that incremental replay can resume from the backup point.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
FALCON_PORT=5433
FALCON_USER="falcon"
FALCON_DB="pitr_demo"
DATA_DIR="${POC_ROOT}/pitr_data"
BACKUP_DIR="${POC_ROOT}/backups"
WAL_ARCHIVE="${POC_ROOT}/wal_archive"
MANIFEST="${OUTPUT_DIR}/backup_manifest.json"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

mkdir -p "${BACKUP_DIR}" "${WAL_ARCHIVE}" "${OUTPUT_DIR}"

banner "Taking Full Backup (T0)"

BACKUP_TS=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
BACKUP_LABEL="full_backup_$(date -u +%Y%m%d_%H%M%S)"
BACKUP_PATH="${BACKUP_DIR}/${BACKUP_LABEL}"

# Record row counts and balances before backup
info "Recording pre-backup state..."
ROW_COUNT=$(psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -t -A -c "SELECT COUNT(*) FROM accounts;" 2>/dev/null)
TOTAL_BALANCE=$(psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -t -A -c "SELECT SUM(balance) FROM accounts;" 2>/dev/null)
ok "Pre-backup: ${ROW_COUNT} accounts, total balance: ${TOTAL_BALANCE}"

# Create consistent snapshot copy of data directory
info "Creating data snapshot..."
cp -r "${DATA_DIR}" "${BACKUP_PATH}"
BACKUP_SIZE=$(du -sb "${BACKUP_PATH}" 2>/dev/null | cut -f1 || echo "0")
ok "Snapshot created: ${BACKUP_PATH} (${BACKUP_SIZE} bytes)"

# Archive current WAL segments
info "Archiving WAL segments..."
WAL_DIR="${DATA_DIR}/wal"
WAL_SEGMENT_COUNT=0
if [ -d "${WAL_DIR}" ]; then
  for seg in "${WAL_DIR}"/*.wal "${WAL_DIR}"/*.log; do
    [ -f "${seg}" ] || continue
    cp "${seg}" "${WAL_ARCHIVE}/"
    WAL_SEGMENT_COUNT=$((WAL_SEGMENT_COUNT + 1))
  done
fi
ok "Archived ${WAL_SEGMENT_COUNT} WAL segments to ${WAL_ARCHIVE}"

# Record WAL position (LSN proxy: count WAL files × assumed segment size)
WAL_FILES=$(find "${WAL_ARCHIVE}" -type f 2>/dev/null | wc -l | tr -d ' ')
BASE_LSN="${WAL_FILES}"

# Compute simple checksum of the backup
CHECKSUM=$(find "${BACKUP_PATH}" -type f -exec cat {} + 2>/dev/null | cksum | cut -d' ' -f1 || echo "0")

# Write backup manifest
info "Writing manifest..."
cat > "${MANIFEST}" << EOF
{
  "backup_label": "${BACKUP_LABEL}",
  "backup_time": "${BACKUP_TS}",
  "base_lsn": "${BASE_LSN}",
  "backup_path": "${BACKUP_PATH}",
  "wal_archive": "${WAL_ARCHIVE}",
  "data_dir": "${DATA_DIR}",
  "database": "${FALCON_DB}",
  "row_count": ${ROW_COUNT},
  "total_balance": ${TOTAL_BALANCE},
  "backup_size_bytes": ${BACKUP_SIZE},
  "wal_segments_archived": ${WAL_SEGMENT_COUNT},
  "checksum": ${CHECKSUM},
  "status": "completed"
}
EOF
ok "Manifest written: ${MANIFEST}"

# Log WAL segment inventory
info "WAL segment inventory..."
ls -la "${WAL_ARCHIVE}/" 2>/dev/null > "${OUTPUT_DIR}/wal_segments.log" || true
ok "WAL inventory: ${OUTPUT_DIR}/wal_segments.log"

echo ""
echo "  ┌──────────────────────────────────────┐"
echo "  │  Backup Complete (T0)                 │"
echo "  │  Label:    ${BACKUP_LABEL}            │"
echo "  │  Time:     ${BACKUP_TS}               │"
echo "  │  Accounts: ${ROW_COUNT}               │"
echo "  │  Balance:  ${TOTAL_BALANCE}            │"
echo "  │  Size:     ${BACKUP_SIZE} bytes        │"
echo "  └──────────────────────────────────────┘"
echo ""
