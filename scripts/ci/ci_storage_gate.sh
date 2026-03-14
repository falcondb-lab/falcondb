#!/usr/bin/env bash
# S6-2: Storage Long-Run / Perf Gate
# Validates storage engine correctness and performance under stress.
# Exit 0 = all checks pass, non-zero = failure.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
PASS=0
FAIL=0

log() { echo "[$(date +%H:%M:%S)] $*"; }

check() {
    local name="$1"
    local result="$2"
    if [ "$result" -eq 0 ]; then
        log "  ✓ $name"
        PASS=$((PASS + 1))
    else
        log "  ✗ $name"
        FAIL=$((FAIL + 1))
    fi
}

log "=== Storage Engine CI Gate ==="

# ====================================================================
# Gate 1: Storage module tests (all new hardening modules)
# ====================================================================
log "[1/6] Storage hardening module tests"
(cd "$ROOT_DIR" && cargo test -p falcon_storage -- \
    storage_error recovery compaction_scheduler memory_budget \
    gc_safepoint storage_tools storage_fault_injection \
    2>&1 | tail -5)
RESULT=$?
check "Storage hardening tests" "$RESULT"

# ====================================================================
# Gate 2: Full storage crate tests
# ====================================================================
log "[2/6] Full falcon_storage test suite"
(cd "$ROOT_DIR" && cargo test -p falcon_storage 2>&1 | tail -5)
RESULT=$?
check "Full storage tests" "$RESULT"

# ====================================================================
# Gate 3: SST integrity — write + verify cycle
# ====================================================================
log "[3/6] SST write-verify cycle"
(cd "$ROOT_DIR" && cargo test -p falcon_storage storage_tools::tests::test_sst_verify_valid_file -- --nocapture 2>&1 | tail -3)
RESULT=$?
check "SST verify cycle" "$RESULT"

# ====================================================================
# Gate 4: WAL corruption resilience — no panic on corrupt data
# ====================================================================
log "[4/6] WAL corruption resilience"
(cd "$ROOT_DIR" && cargo test -p falcon_storage -- \
    test_scan_corrupt_checksum test_scan_truncated test_wal_inspect_corrupt \
    2>&1 | tail -3)
RESULT=$?
check "WAL corruption resilience" "$RESULT"

# ====================================================================
# Gate 5: Memory budget — reject writes under pressure, no OOM
# ====================================================================
log "[5/6] Memory budget enforcement"
(cd "$ROOT_DIR" && cargo test -p falcon_storage memory_budget -- --nocapture 2>&1 | tail -3)
RESULT=$?
check "Memory budget tests" "$RESULT"

# ====================================================================
# Gate 6: Clippy clean (storage crate)
# ====================================================================
log "[6/6] Clippy clean"
(cd "$ROOT_DIR" && cargo clippy -p falcon_storage -- -D warnings 2>&1 | tail -5)
RESULT=$?
check "Clippy clean" "$RESULT"

# ====================================================================
# Summary
# ====================================================================
TOTAL=$((PASS + FAIL))
log ""
log "=== Storage Gate Results: $PASS/$TOTAL passed ==="
if [ "$FAIL" -gt 0 ]; then
    log "FAILED — $FAIL checks did not pass"
    exit 1
fi
log "ALL PASSED"
exit 0
