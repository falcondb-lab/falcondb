#!/bin/bash
# tune_linux_threads.sh — Apply thread priorities for FalconDB production
# Usage: sudo bash scripts/tune_linux_threads.sh
# See: docs/os/linux_ubuntu_24_04.md §4 Thread Scheduling & CPU Affinity
set -euo pipefail

PID=$(pidof falcon 2>/dev/null || true)
if [ -z "$PID" ]; then
    echo "ERROR: FalconDB is not running." >&2
    exit 1
fi

echo "FalconDB PID: $PID"
echo ""

TUNED=0
for tid in $(ls /proc/$PID/task/ 2>/dev/null); do
    name=$(cat /proc/$PID/task/$tid/comm 2>/dev/null || continue)
    case "$name" in
        falcon-wal-writ*|falcon-async-wa*)
            renice -n -10 -p "$tid" 2>/dev/null && \
                echo "  [HIGH]   $name (tid=$tid) → nice -10" && TUNED=$((TUNED+1))
            ;;
        falcon-repl-sen*)
            renice -n -5 -p "$tid" 2>/dev/null && \
                echo "  [MEDIUM] $name (tid=$tid) → nice -5" && TUNED=$((TUNED+1))
            ;;
        falcon-gc-*)
            renice -n 10 -p "$tid" 2>/dev/null && \
                echo "  [LOW]    $name (tid=$tid) → nice 10" && TUNED=$((TUNED+1))
            ;;
    esac
done

echo ""
echo "Tuned $TUNED threads."
if [ "$TUNED" -eq 0 ]; then
    echo "NOTE: No FalconDB-specific threads found. Threads may use different names."
    echo "      Check: ls /proc/$PID/task/*/comm"
fi
