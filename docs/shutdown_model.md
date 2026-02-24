# FalconDB Graceful Shutdown Model

## Overview

FalconDB treats shutdown as a **first-class runtime protocol**, not a side-effect
of process exit or Tokio runtime drop. Every listening port is explicitly released
inside its owning task before that task returns.

## Invariants

1. **Port release ≠ process exit.** Every `TcpListener` is explicitly `drop()`-ed
   inside its server task. We never rely on Tokio runtime teardown or OS socket
   cleanup.

2. **Single root signal.** A `ShutdownCoordinator` wrapping a `CancellationToken`
   is the sole shutdown primitive. All servers observe child tokens derived from
   the same root.

3. **No fire-and-forget server tasks.** Every `tokio::spawn` that holds a listener
   produces a `JoinHandle` that `main()` awaits before returning.

4. **Windows = correctness oracle.** Windows socket handle release is more
   conservative than Linux. If shutdown works on Windows, it works everywhere.

## Shutdown Sequence

```
OS Signal (Ctrl+C / SIGTERM)
   │
   ▼
wait_for_os_signal() returns ShutdownReason
   │
   ▼
coordinator.shutdown(reason)     ← cancels root CancellationToken
   │
   ├─► PG server:  select! sees cancelled → break → drop(listener) → drain → WAL flush
   ├─► HTTP health: select! sees cancelled → break → drop(listener)
   ├─► gRPC replication: serve_with_shutdown future completes → port released
   └─► Replica runner: stop() called
   │
   ▼
main() awaits ALL JoinHandles in order:
   1. ReplicaRunner.stop()
   2. gRPC JoinHandle
   3. Health JoinHandle
   │
   ▼
"FalconDB shutdown complete — all ports released"
Process exits
```

## Components

### ShutdownCoordinator (`shutdown.rs`)

- Wraps `tokio_util::sync::CancellationToken`
- `ShutdownReason` enum: `Running | CtrlC | Sigterm | Requested`
- Reason is sticky — first signal wins
- `child_token()` creates derived tokens for each server
- `wait_for_os_signal()` listens for Ctrl+C (all platforms) + SIGTERM (Unix)

### PG Server (`falcon_protocol_pg/src/server.rs`)

- `run_with_shutdown(shutdown_future, drain_timeout)`
- `tokio::select!` on `listener.accept()` vs `shutdown`
- On shutdown: `drop(listener)` → drain active connections → WAL flush
- Structured log: `[INFO] pg server shutdown: listener dropped (port=5443)`

### HTTP Health Server (`falcon_server/src/health.rs`)

- `run_health_server(addr, state, shutdown_future)`
- `tokio::select!` on `listener.accept()` vs `shutdown`
- On shutdown: `drop(listener)`
- Structured log: `[INFO] http server shutdown: listener dropped (port=8080)`

### gRPC Replication Server (`main.rs`, Primary role)

- `tonic::Server::serve_with_shutdown(addr, token.cancelled())`
- Port released when tonic's serve future completes
- JoinHandle captured and awaited during ordered teardown
- Structured log: `[INFO] grpc server shutdown (port=50051)`

## Verification

### Automated Scripts

| Platform | Script | Usage |
|----------|--------|-------|
| Windows  | `scripts/shutdown_port_check.ps1` | `.\scripts\shutdown_port_check.ps1` |
| Linux    | `scripts/shutdown_port_check.sh`  | `./scripts/shutdown_port_check.sh`  |

Both scripts: start falcon → verify ports listening → send signal → verify ports released.

### Manual Verification

```bash
# Start
falcon.exe --pg-addr 127.0.0.1:5443 --no-wal

# Stop
Ctrl+C

# Verify (Windows)
netstat -ano | findstr 5443    # must be empty
netstat -ano | findstr 8080    # must be empty

# Verify (Linux)
ss -tln | grep 5443            # must be empty
ss -tln | grep 8080            # must be empty
```

### Test Suite

```bash
cargo test -p falcon_server --lib shutdown        # 8 unit tests
cargo test -p falcon_server --test shutdown_integration  # 11 integration tests
```

## Anti-Patterns (Forbidden)

| Anti-Pattern | Why It's Wrong |
|---|---|
| Relying on `Drop` for port release | Drop order is unspecified in async runtime teardown |
| `tokio::spawn` without capturing `JoinHandle` | Task can outlive `main()` |
| `accept()` loop without `select!` on shutdown | Loop never exits, listener never drops |
| Using `SO_REUSEADDR` as correctness fix | Masks the bug; correct fix is explicit shutdown |
| Sending shutdown signal without awaiting tasks | Race: main exits before listener drops |
