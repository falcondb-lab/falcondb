#Requires -RunAsAdministrator
# ============================================================================
# FalconDB — Uninstall Windows Service
# ============================================================================
#
# This script stops and removes the FalconDB Windows Service.
#
# Usage (run as Administrator):
#   .\scripts\uninstall_service.ps1

param(
    [string]$ServiceName = "FalconDB"
)

$ErrorActionPreference = "Stop"

Write-Host "================================================================="
Write-Host "  FalconDB — Windows Service Uninstallation"
Write-Host "================================================================="
Write-Host ""

# ── Check if service exists ──
sc.exe query $ServiceName 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Service '$ServiceName' does not exist. Nothing to do." -ForegroundColor Yellow
    exit 0
}

# ── Stop the service if running ──
$status = (sc.exe query $ServiceName | Select-String "STATE") -replace '.*STATE\s+:\s+\d+\s+', ''
if ($status -and $status.Trim() -eq "RUNNING") {
    Write-Host "Stopping service '$ServiceName'..."
    sc.exe stop $ServiceName | Out-Null
    # Wait for stop (up to 30 seconds)
    $waited = 0
    while ($waited -lt 30) {
        Start-Sleep -Seconds 1
        $waited++
        $check = (sc.exe query $ServiceName | Select-String "STATE") -replace '.*STATE\s+:\s+\d+\s+', ''
        if ($check -and $check.Trim() -eq "STOPPED") {
            Write-Host "  Service stopped."
            break
        }
    }
    if ($waited -ge 30) {
        Write-Host "WARNING: Service did not stop within 30 seconds." -ForegroundColor Yellow
    }
}

# ── Delete the service ──
Write-Host "Deleting service '$ServiceName'..."
sc.exe delete $ServiceName | Out-Null

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to delete service (exit code: $LASTEXITCODE)" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "================================================================="
Write-Host "  Service '$ServiceName' removed successfully."
Write-Host ""
Write-Host "  Verify:"
Write-Host "    sc query $ServiceName"
Write-Host "    (should return 'service does not exist')"
Write-Host "================================================================="
