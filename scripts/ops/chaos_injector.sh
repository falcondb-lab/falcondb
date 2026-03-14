#!/usr/bin/env bash
# chaos_injector.sh — Minimal chaos injection for FalconDB CI smoke tests.
#
# Supports:
#   kill      — kill -9 a node process and optionally restart it
#   restart   — graceful stop + restart
#   delay     — add network delay via tc/netem (Linux only)
#   loss      — add packet loss via tc/netem (Linux only)
#   clear     — remove all tc/netem rules
#   run       — run a full chaos scenario (kill+restart cycle)
#
# Usage:
#   ./scripts/chaos_injector.sh kill   <pid_file> [--no-restart]
#   ./scripts/chaos_injector.sh restart <pid_file> <start_cmd>
#   ./scripts/chaos_injector.sh delay  <iface> <delay_ms> [<jitter_ms>]
#   ./scripts/chaos_injector.sh loss   <iface> <loss_pct>
#   ./scripts/chaos_injector.sh clear  <iface>
#   ./scripts/chaos_injector.sh run    <scenario> <pid_dir> [options]

set -euo pipefail

CMD="${1:-help}"
shift || true

log()  { echo "[chaos $(date '+%H:%M:%S')] $*"; }
warn() { echo "[chaos WARN $(date '+%H:%M:%S')] $*" >&2; }

# ── kill ─────────────────────────────────────────────────────────────────────
do_kill() {
  local pid_file="${1:?pid_file required}"
  local no_restart="${2:-}"

  [[ -f "$pid_file" ]] || { warn "PID file not found: $pid_file"; return 1; }
  local pid
  pid=$(cat "$pid_file")
  if kill -0 "$pid" 2>/dev/null; then
    log "kill -9 PID $pid (from $pid_file)"
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$pid_file"
    log "killed PID $pid"
  else
    warn "PID $pid not running"
    rm -f "$pid_file"
  fi
}

# ── restart ───────────────────────────────────────────────────────────────────
do_restart() {
  local pid_file="${1:?pid_file required}"
  local start_cmd="${2:?start_cmd required}"
  local log_file="${3:-/tmp/chaos_restart.log}"

  do_kill "$pid_file" || true
  sleep 0.5
  log "restarting: $start_cmd"
  eval "$start_cmd" >> "$log_file" 2>&1 &
  echo $! > "$pid_file"
  log "restarted as PID $!"
}

# ── network delay (Linux tc/netem) ───────────────────────────────────────────
do_delay() {
  local iface="${1:?iface required}"
  local delay_ms="${2:?delay_ms required}"
  local jitter_ms="${3:-0}"

  if ! command -v tc &>/dev/null; then
    warn "tc not available — skipping network delay injection"
    return 0
  fi

  # Remove existing qdisc if present
  tc qdisc del dev "$iface" root 2>/dev/null || true

  if [[ "$jitter_ms" -gt 0 ]]; then
    tc qdisc add dev "$iface" root netem delay "${delay_ms}ms" "${jitter_ms}ms" distribution normal
    log "added delay ${delay_ms}ms ±${jitter_ms}ms on $iface"
  else
    tc qdisc add dev "$iface" root netem delay "${delay_ms}ms"
    log "added delay ${delay_ms}ms on $iface"
  fi
}

# ── packet loss ───────────────────────────────────────────────────────────────
do_loss() {
  local iface="${1:?iface required}"
  local loss_pct="${2:?loss_pct required}"

  if ! command -v tc &>/dev/null; then
    warn "tc not available — skipping packet loss injection"
    return 0
  fi

  tc qdisc del dev "$iface" root 2>/dev/null || true
  tc qdisc add dev "$iface" root netem loss "${loss_pct}%"
  log "added ${loss_pct}% packet loss on $iface"
}

# ── clear tc rules ────────────────────────────────────────────────────────────
do_clear() {
  local iface="${1:?iface required}"
  if command -v tc &>/dev/null; then
    tc qdisc del dev "$iface" root 2>/dev/null || true
    log "cleared tc rules on $iface"
  fi
}

# ── run scenario ──────────────────────────────────────────────────────────────
do_run() {
  local scenario="${1:?scenario required}"
  local pid_dir="${2:?pid_dir required}"
  local duration="${3:-30}"

  log "=== Chaos scenario: $scenario (${duration}s) ==="

  case "$scenario" in
    kill_restart)
      # Repeatedly kill and restart a random node
      local end_time=$(( $(date +%s) + duration ))
      local cycle=0
      while [[ $(date +%s) -lt $end_time ]]; do
        cycle=$(( cycle + 1 ))
        # Pick a random pid file
        local pid_files=("$pid_dir"/*.pid)
        if [[ ${#pid_files[@]} -eq 0 || ! -f "${pid_files[0]}" ]]; then
          log "No PID files found in $pid_dir, waiting..."
          sleep 2
          continue
        fi
        local idx=$(( RANDOM % ${#pid_files[@]} ))
        local pid_file="${pid_files[$idx]}"
        local node_name
        node_name=$(basename "$pid_file" .pid)
        log "Cycle $cycle: killing $node_name"
        do_kill "$pid_file" || true
        sleep 1
        log "Cycle $cycle: $node_name killed, sleeping 3s before next action"
        sleep 3
      done
      log "kill_restart scenario complete ($cycle cycles)"
      ;;

    network_delay)
      local iface="${4:-lo}"
      local delay_ms="${5:-50}"
      local jitter_ms="${6:-10}"
      do_delay "$iface" "$delay_ms" "$jitter_ms"
      log "Network delay active for ${duration}s..."
      sleep "$duration"
      do_clear "$iface"
      log "Network delay cleared"
      ;;

    packet_loss)
      local iface="${4:-lo}"
      local loss_pct="${5:-5}"
      do_loss "$iface" "$loss_pct"
      log "Packet loss active for ${duration}s..."
      sleep "$duration"
      do_clear "$iface"
      log "Packet loss cleared"
      ;;

    combined)
      # Kill + network delay combined
      local iface="${4:-lo}"
      do_delay "$iface" "30" "10" || true
      local end_time=$(( $(date +%s) + duration ))
      local cycle=0
      while [[ $(date +%s) -lt $end_time ]]; do
        cycle=$(( cycle + 1 ))
        local pid_files=("$pid_dir"/*.pid)
        if [[ ${#pid_files[@]} -gt 0 && -f "${pid_files[0]}" ]]; then
          local idx=$(( RANDOM % ${#pid_files[@]} ))
          do_kill "${pid_files[$idx]}" || true
          sleep 2
        fi
        sleep 5
      done
      do_clear "$iface" || true
      log "combined scenario complete ($cycle cycles)"
      ;;

    *)
      warn "Unknown scenario: $scenario"
      echo "Available scenarios: kill_restart, network_delay, packet_loss, combined"
      exit 1
      ;;
  esac

  log "=== Chaos scenario '$scenario' finished ==="
}

# ── report ────────────────────────────────────────────────────────────────────
do_report() {
  local log_dir="${1:-.}"
  local out="${2:-chaos_report.txt}"

  {
    echo "=== FalconDB Chaos Report ==="
    echo "Generated: $(date)"
    echo ""
    echo "--- Log Summary ---"
    if ls "$log_dir"/*.log 2>/dev/null | head -1 | grep -q .; then
      for f in "$log_dir"/*.log; do
        echo "File: $f"
        echo "  Lines: $(wc -l < "$f")"
        echo "  ERRORs: $(grep -c "ERROR\|PANIC\|panic" "$f" 2>/dev/null || echo 0)"
        echo "  WARNs:  $(grep -c "WARN" "$f" 2>/dev/null || echo 0)"
        echo "  Last 5 lines:"
        tail -5 "$f" | sed 's/^/    /'
        echo ""
      done
    else
      echo "  No log files found in $log_dir"
    fi
    echo "--- End Report ---"
  } > "$out"

  log "Chaos report written to: $out"
  cat "$out"
}

# ── dispatch ──────────────────────────────────────────────────────────────────
case "$CMD" in
  kill)     do_kill "$@" ;;
  restart)  do_restart "$@" ;;
  delay)    do_delay "$@" ;;
  loss)     do_loss "$@" ;;
  clear)    do_clear "$@" ;;
  run)      do_run "$@" ;;
  report)   do_report "$@" ;;
  help|--help|-h)
    echo "Usage: chaos_injector.sh <command> [args]"
    echo "Commands: kill, restart, delay, loss, clear, run, report"
    ;;
  *)
    warn "Unknown command: $CMD"
    exit 1
    ;;
esac
