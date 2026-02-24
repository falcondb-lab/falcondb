# FalconDB Logs Directory

This directory is reserved for FalconDB log output.

## Current Behavior

FalconDB logs to **stderr** by default. To capture logs to a file:

```powershell
# Foreground mode with log capture
.\bin\falcon.exe --config .\conf\falcon.toml 2> .\logs\falcon.log

# Or use PowerShell redirection
.\bin\falcon.exe --config .\conf\falcon.toml *> .\logs\falcon.log
```

When running as a Windows Service, configure the service to redirect output
to this directory (see `scripts/install_service.ps1`).

## Log Levels

Control log verbosity via the `RUST_LOG` environment variable:

```powershell
$env:RUST_LOG = "info"     # Default — startup, shutdown, connections
$env:RUST_LOG = "debug"    # Verbose — query execution, WAL ops
$env:RUST_LOG = "warn"     # Quiet — only warnings and errors
```
