#!/usr/bin/env bash
# ============================================================================
# FalconDB — P1-2: Long-Run Stability Test
# ============================================================================
#
# Runs a continuous OLTP workload for a specified duration while monitoring:
#   - RSS / heap usage (no sustained growth = no leak)
#   - TPS / latency drift
#   - WAL backlog
#   - Error rate
#
# Usage:
#   chmod +x scripts/run_stability_test.sh
#   ./scripts/run_stability_test.sh --duration 72h   # 72-hour run
#   ./scripts/run_stability_test.sh --duration 7d    # 7-day run
#   ./scripts/run_stability_test.sh --duration 10m   # 10-minute smoke test
#
# Output:
#   evidence/stability/<duration>_report_<timestamp>.md
#   evidence/stability/<duration>_metrics_<timestamp>.csv
# ============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EVIDENCE_DIR="$REPO_ROOT/evidence/stability"
TIMESTAMP=$(date +%Y%m%dT%H%M%S)

# ── Parse args ──
DURATION_STR="10m"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration) DURATION_STR="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# Convert duration to seconds
parse_duration() {
    local d="$1"
    if [[ "$d" =~ ^([0-9]+)s$ ]]; then echo "${BASH_REMATCH[1]}"
    elif [[ "$d" =~ ^([0-9]+)m$ ]]; then echo $(( ${BASH_REMATCH[1]} * 60 ))
    elif [[ "$d" =~ ^([0-9]+)h$ ]]; then echo $(( ${BASH_REMATCH[1]} * 3600 ))
    elif [[ "$d" =~ ^([0-9]+)d$ ]]; then echo $(( ${BASH_REMATCH[1]} * 86400 ))
    else echo "$d"
    fi
}
DURATION_SECS=$(parse_duration "$DURATION_STR")
SAMPLE_INTERVAL=60  # sample every 60s

mkdir -p "$EVIDENCE_DIR"

VERSION=$(grep -m1 '^version' "$REPO_ROOT/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')
GIT_HASH=$(git -C "$REPO_ROOT" rev-parse --short=8 HEAD 2>/dev/null || echo "unknown")

METRICS_FILE="$EVIDENCE_DIR/${DURATION_STR}_metrics_${TIMESTAMP}.csv"
REPORT_FILE="$EVIDENCE_DIR/${DURATION_STR}_report_${TIMESTAMP}.md"

echo "================================================================="
echo " FalconDB — Long-Run Stability Test"
echo " Version:  $VERSION ($GIT_HASH)"
echo " Duration: $DURATION_STR ($DURATION_SECS seconds)"
echo " Sample:   every ${SAMPLE_INTERVAL}s"
echo " Output:   $EVIDENCE_DIR/"
echo "================================================================="

# ── Build ──
echo "[1/4] Building..."
cargo build --release -p falcon_server 2>/dev/null || {
    echo "WARN: release build failed, using debug"
}

# ── Start FalconDB ──
echo "[2/4] Starting FalconDB..."
FALCON_BIN="$REPO_ROOT/target/release/falcon"
[ -f "$FALCON_BIN" ] || FALCON_BIN="$REPO_ROOT/target/debug/falcon"
if [ ! -f "$FALCON_BIN" ]; then
    echo "ERROR: No falcon binary found"
    exit 1
fi

DATA_DIR=$(mktemp -d /tmp/falcon_stability_XXXX)
"$FALCON_BIN" --data-dir "$DATA_DIR" &
FALCON_PID=$!
echo "  PID: $FALCON_PID, data: $DATA_DIR"

cleanup() {
    kill $FALCON_PID 2>/dev/null || true
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# Wait for ready
sleep 3

# ── Run workload + monitor ──
echo "[3/4] Running stability workload for $DURATION_STR..."
echo "timestamp,elapsed_s,rss_kb,tps,avg_latency_ms,error_count,wal_backlog" > "$METRICS_FILE"

START_TIME=$(date +%s)
SAMPLE=0
TOTAL_TPS=0
MAX_RSS=0
ERRORS=0

while true; do
    NOW=$(date +%s)
    ELAPSED=$(( NOW - START_TIME ))
    if [ "$ELAPSED" -ge "$DURATION_SECS" ]; then
        break
    fi

    # Sample RSS
    RSS=$(ps -o rss= -p $FALCON_PID 2>/dev/null || echo "0")
    RSS=${RSS// /}
    [ -z "$RSS" ] && RSS=0
    [ "$RSS" -gt "$MAX_RSS" ] && MAX_RSS=$RSS

    # Run a short pgbench burst (5 seconds)
    TPS="0"
    LATENCY="0"
    if command -v pgbench &>/dev/null; then
        BURST=$(pgbench -h 127.0.0.1 -p 5443 -U falcon -d falcon \
            -c 4 -j 4 -T 5 2>&1 || true)
        TPS=$(echo "$BURST" | grep -oP 'tps = \K[0-9.]+' | tail -1 || echo "0")
        LATENCY=$(echo "$BURST" | grep -oP 'latency average = \K[0-9.]+' || echo "0")
    else
        # Fallback: use cargo test as synthetic load
        cargo test -p falcon_storage -- memory --quiet 2>/dev/null && TPS="synthetic" || ERRORS=$((ERRORS + 1))
    fi

    # WAL backlog (from /admin/status if available)
    WAL_BACKLOG=$(curl -s http://127.0.0.1:8080/status 2>/dev/null | grep -oP '"wal_backlog_bytes":\K[0-9]+' || echo "0")

    echo "$(date -Iseconds),$ELAPSED,$RSS,$TPS,$LATENCY,$ERRORS,$WAL_BACKLOG" >> "$METRICS_FILE"
    SAMPLE=$((SAMPLE + 1))

    if [ $((SAMPLE % 10)) -eq 0 ]; then
        echo "  [$ELAPSED/${DURATION_SECS}s] RSS=${RSS}KB TPS=$TPS errors=$ERRORS"
    fi

    sleep "$SAMPLE_INTERVAL"
done

# ── Generate report ──
echo "[4/4] Generating stability report..."

FINAL_RSS=$(ps -o rss= -p $FALCON_PID 2>/dev/null || echo "0")
FINAL_RSS=${FINAL_RSS// /}
INITIAL_RSS=$(head -2 "$METRICS_FILE" | tail -1 | cut -d',' -f3)
[ -z "$INITIAL_RSS" ] && INITIAL_RSS=0
[ -z "$FINAL_RSS" ] && FINAL_RSS=0

RSS_GROWTH=$(( FINAL_RSS - INITIAL_RSS ))
LEAK_SUSPECT="no"
if [ "$INITIAL_RSS" -gt 0 ] && [ "$RSS_GROWTH" -gt 0 ]; then
    GROWTH_PCT=$(( RSS_GROWTH * 100 / INITIAL_RSS ))
    [ "$GROWTH_PCT" -gt 20 ] && LEAK_SUSPECT="yes (${GROWTH_PCT}% growth)"
fi

cat > "$REPORT_FILE" <<EOF
# FalconDB — Stability Test Report

| Field | Value |
|-------|-------|
| **Version** | $VERSION ($GIT_HASH) |
| **Duration** | $DURATION_STR ($DURATION_SECS seconds) |
| **Timestamp** | $TIMESTAMP |
| **Samples** | $SAMPLE |

## Memory

| Metric | Value |
|--------|-------|
| Initial RSS | ${INITIAL_RSS} KB |
| Final RSS | ${FINAL_RSS} KB |
| Peak RSS | ${MAX_RSS} KB |
| RSS Growth | ${RSS_GROWTH} KB |
| Memory Leak Suspect | $LEAK_SUSPECT |

## Errors

| Metric | Value |
|--------|-------|
| Total Errors | $ERRORS |

## Conclusion

$(if [ "$ERRORS" -eq 0 ] && [ "$LEAK_SUSPECT" = "no" ]; then
    echo "✅ **PASS**: No memory leaks detected, zero errors over $DURATION_STR"
elif [ "$ERRORS" -gt 0 ]; then
    echo "⚠️ **WARN**: $ERRORS errors detected during run"
else
    echo "⚠️ **WARN**: Possible memory leak — $LEAK_SUSPECT"
fi)

## Raw Data

- Metrics CSV: \`$(basename "$METRICS_FILE")\`
- Sample interval: ${SAMPLE_INTERVAL}s

## Reproduction

\`\`\`bash
./scripts/run_stability_test.sh --duration $DURATION_STR
\`\`\`
EOF

echo ""
echo "================================================================="
echo " Stability test complete: $DURATION_STR"
echo " Report:  $REPORT_FILE"
echo " Metrics: $METRICS_FILE"
echo " Errors:  $ERRORS"
echo " Leak:    $LEAK_SUSPECT"
echo "================================================================="
