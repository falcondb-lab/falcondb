#!/usr/bin/env bash
# ============================================================================
# FalconDB Benchmark — Run a single workload against a target database
# ============================================================================
#
# Usage:
#   ./run_workload.sh <target> <workload> <threads> <duration_sec>
#
# Examples:
#   ./run_workload.sh falcondb w1 8 60
#   ./run_workload.sh postgresql w2 16 60
#
# Output: results/<target>_<workload>_t<threads>.json
# ============================================================================

set -euo pipefail

TARGET="${1:?Usage: run_workload.sh <falcondb|postgresql> <w1|w2> <threads> <duration_sec>}"
WORKLOAD="${2:?Missing workload (w1 or w2)}"
THREADS="${3:?Missing thread count}"
DURATION="${4:?Missing duration in seconds}"

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_DIR="$BENCH_DIR/results"
mkdir -p "$RESULTS_DIR"

# Target connection params
case "$TARGET" in
    falcondb)
        HOST="127.0.0.1"
        PORT="5443"
        USER="falcon"
        DB="falcon"
        ;;
    postgresql)
        HOST="127.0.0.1"
        PORT="5432"
        USER="$(whoami)"
        DB="falconbench"
        ;;
    *)
        echo "ERROR: Unknown target '$TARGET'. Use 'falcondb' or 'postgresql'."
        exit 1
        ;;
esac

OUTFILE="$RESULTS_DIR/${TARGET}_${WORKLOAD}_t${THREADS}.txt"

echo "================================================================="
echo "  Benchmark: $TARGET / $WORKLOAD / ${THREADS} threads / ${DURATION}s"
echo "  Target:    $HOST:$PORT ($DB)"
echo "  Output:    $OUTFILE"
echo "================================================================="

# ── Setup schema ──
echo "[1/3] Loading schema..."
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -f "$BENCH_DIR/workloads/setup.sql" > /dev/null 2>&1
echo "  Schema loaded (100K accounts)"

# ── Run workload ──
echo "[2/3] Running workload ($WORKLOAD, ${THREADS} threads, ${DURATION}s)..."

# Use pgbench for standardized measurement
case "$WORKLOAD" in
    w1)
        # W1: Single-table OLTP (70% SELECT, 20% UPDATE, 10% INSERT)
        # pgbench custom script
        PGBENCH_SCRIPT=$(mktemp)
        cat > "$PGBENCH_SCRIPT" <<'EOF'
\set random_id random(1, 100000)
\set delta random(-100, 100)
\set op random(1, 100)
\if :op <= 70
SELECT id, balance, name FROM accounts WHERE id = :random_id;
\elif :op <= 90
UPDATE accounts SET balance = balance + :delta WHERE id = :random_id;
\else
\set new_id random(200001, 999999)
INSERT INTO accounts (id, balance, name) VALUES (:new_id, :random_id, 'bench') ON CONFLICT DO NOTHING;
\endif
EOF
        pgbench -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
            -f "$PGBENCH_SCRIPT" \
            -c "$THREADS" -j "$THREADS" -T "$DURATION" \
            --progress=10 2>&1 | tee "$OUTFILE"
        rm -f "$PGBENCH_SCRIPT"
        ;;
    w2)
        # W2: Multi-table transaction
        PGBENCH_SCRIPT=$(mktemp)
        cat > "$PGBENCH_SCRIPT" <<'EOF'
\set from_id random(1, 100000)
\set to_id random(1, 100000)
\set amount random(1, 100)
BEGIN;
SELECT balance FROM accounts WHERE id = :from_id;
INSERT INTO transfers (from_id, to_id, amount) VALUES (:from_id, :to_id, :amount);
UPDATE accounts SET balance = balance - :amount WHERE id = :from_id;
UPDATE accounts SET balance = balance + :amount WHERE id = :to_id;
COMMIT;
EOF
        pgbench -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
            -f "$PGBENCH_SCRIPT" \
            -c "$THREADS" -j "$THREADS" -T "$DURATION" \
            --progress=10 2>&1 | tee "$OUTFILE"
        rm -f "$PGBENCH_SCRIPT"
        ;;
    *)
        echo "ERROR: Unknown workload '$WORKLOAD'. Use 'w1' or 'w2'."
        exit 1
        ;;
esac

echo ""
echo "[3/3] Results saved to: $OUTFILE"
