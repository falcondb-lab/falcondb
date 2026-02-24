# ============================================================================
# FalconDB — In-Place Upgrade Script
# ============================================================================
#
# Performs a safe in-place upgrade of FalconDB:
#   1. Validates new MSI / exe
#   2. Backs up current binary + config
#   3. Stops the service
#   4. Replaces binary
#   5. Migrates config if needed
#   6. Starts the service
#   7. Verifies health
#   8. On failure: rolls back to previous version
#
# Usage (run as Administrator):
#   .\scripts\upgrade_falcondb.ps1 -NewMsi ".\FalconDB-1.1.0-x64.msi"
#   .\scripts\upgrade_falcondb.ps1 -NewExe ".\falcon.exe"  # manual binary upgrade
#
# ============================================================================

#Requires -RunAsAdministrator

param(
    [string]$NewMsi,
    [string]$NewExe,
    [int]$HealthCheckTimeoutSec = 30,
    [int]$PgPort = 5443,
    [int]$HttpPort = 8080
)

$ErrorActionPreference = "Stop"
$ServiceName = "FalconDB"

$InstallDir = Join-Path $env:ProgramFiles "FalconDB"
$BinDir = Join-Path $InstallDir "bin"
$CurrentExe = Join-Path $BinDir "falcon.exe"
$DataRoot = Join-Path $env:ProgramData "FalconDB"
$ConfDir = Join-Path $DataRoot "conf"
$ConfigFile = Join-Path $ConfDir "falcon.toml"
$BackupDir = Join-Path $DataRoot "backup"

function Write-Step([string]$msg) {
    Write-Host "  [$script:step] $msg" -ForegroundColor Cyan
    $script:step++
}

function Test-ServiceRunning {
    $q = sc.exe query $ServiceName 2>&1
    return ($q | Out-String).Contains("RUNNING")
}

function Test-PortListening([int]$port) {
    $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    return ($null -ne $conn -and $conn.Count -gt 0)
}

$step = 1

Write-Host "================================================================="
Write-Host "  FalconDB — In-Place Upgrade"
Write-Host "================================================================="
Write-Host ""

# ── Validate inputs ──
if (-not $NewMsi -and -not $NewExe) {
    Write-Host "ERROR: Provide -NewMsi or -NewExe" -ForegroundColor Red
    Write-Host "  .\scripts\upgrade_falcondb.ps1 -NewMsi '.\FalconDB-1.1.0-x64.msi'"
    Write-Host "  .\scripts\upgrade_falcondb.ps1 -NewExe '.\falcon.exe'"
    exit 1
}

# ── MSI upgrade path ──
if ($NewMsi) {
    if (-not (Test-Path $NewMsi)) {
        Write-Host "ERROR: MSI not found: $NewMsi" -ForegroundColor Red
        exit 1
    }

    Write-Step "MSI upgrade: $NewMsi"

    # Get current version
    if (Test-Path $CurrentExe) {
        $oldVersion = & $CurrentExe version 2>&1 | Select-Object -First 1
        Write-Host "    Current: $oldVersion"
    }

    # MSI handles stop → replace → start automatically via MajorUpgrade
    Write-Step "Running msiexec (this may take a moment)..."
    $logFile = Join-Path $DataRoot "logs\upgrade_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
    $msiArgs = "/i `"$NewMsi`" /qn /l*v `"$logFile`""
    Write-Host "    Log: $logFile"

    $proc = Start-Process -FilePath "msiexec.exe" -ArgumentList $msiArgs -Wait -PassThru
    if ($proc.ExitCode -ne 0) {
        Write-Host "ERROR: MSI install failed (exit code: $($proc.ExitCode))" -ForegroundColor Red
        Write-Host "  Check log: $logFile"
        exit 1
    }

    Write-Step "MSI upgrade complete"

    # Get new version
    if (Test-Path $CurrentExe) {
        $newVersion = & $CurrentExe version 2>&1 | Select-Object -First 1
        Write-Host "    New: $newVersion"
    }

    # Verify service is running
    Write-Step "Verifying service..."
    Start-Sleep -Seconds 3
    if (Test-ServiceRunning) {
        Write-Host "    Service: RUNNING" -ForegroundColor Green
    } else {
        Write-Host "    Service: NOT RUNNING — attempting start..." -ForegroundColor Yellow
        sc.exe start $ServiceName | Out-Null
        Start-Sleep -Seconds 5
    }

    Write-Host ""
    Write-Host "  Upgrade complete!" -ForegroundColor Green
    exit 0
}

# ── Manual binary upgrade path ──
if ($NewExe) {
    if (-not (Test-Path $NewExe)) {
        Write-Host "ERROR: Exe not found: $NewExe" -ForegroundColor Red
        exit 1
    }

    # Step 1: Create backup
    Write-Step "Creating backup..."
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $backupSubDir = Join-Path $BackupDir $timestamp
    New-Item -ItemType Directory -Path $backupSubDir -Force | Out-Null

    if (Test-Path $CurrentExe) {
        Copy-Item $CurrentExe (Join-Path $backupSubDir "falcon.exe") -Force
        $oldVersion = & $CurrentExe version 2>&1 | Select-Object -First 1
        Write-Host "    Backed up: falcon.exe ($oldVersion)"
    }
    if (Test-Path $ConfigFile) {
        Copy-Item $ConfigFile (Join-Path $backupSubDir "falcon.toml") -Force
        Write-Host "    Backed up: falcon.toml"
    }

    # Step 2: Stop service
    Write-Step "Stopping service..."
    if (Test-ServiceRunning) {
        sc.exe stop $ServiceName | Out-Null
        for ($i = 0; $i -lt 30; $i++) {
            Start-Sleep -Seconds 1
            if (-not (Test-ServiceRunning)) { break }
        }
        if (Test-ServiceRunning) {
            Write-Host "    WARNING: Service did not stop in 30s" -ForegroundColor Yellow
        } else {
            Write-Host "    Service stopped"
        }
    } else {
        Write-Host "    Service was not running"
    }

    # Step 3: Replace binary
    Write-Step "Replacing binary..."
    New-Item -ItemType Directory -Path $BinDir -Force | Out-Null
    Copy-Item $NewExe $CurrentExe -Force
    $newVersion = & $CurrentExe version 2>&1 | Select-Object -First 1
    Write-Host "    Installed: $newVersion"

    # Step 4: Migrate config
    Write-Step "Checking config migration..."
    $migrateResult = & $CurrentExe config check --config $ConfigFile 2>&1
    Write-Host "    $migrateResult"
    if ($migrateResult -match "needs migration") {
        Write-Host "    Running config migration..."
        & $CurrentExe config migrate --config $ConfigFile
    }

    # Step 5: Start service
    Write-Step "Starting service..."
    sc.exe start $ServiceName | Out-Null

    # Step 6: Health check
    Write-Step ("Verifying health (timeout: " + $HealthCheckTimeoutSec + "s)...")
    $healthy = $false
    for ($i = 0; $i -lt $HealthCheckTimeoutSec; $i++) {
        Start-Sleep -Seconds 1
        if (Test-PortListening $PgPort) {
            $healthy = $true
            break
        }
    }

    if ($healthy) {
        Write-Host ("    PG port " + $PgPort + ": LISTENING") -ForegroundColor Green
        Write-Host ""
        Write-Host "  Upgrade successful: $oldVersion -> $newVersion" -ForegroundColor Green
        Write-Host "  Backup at: $backupSubDir"
    } else {
        # ── ROLLBACK ──
        Write-Host ""
        Write-Host "  UPGRADE FAILED — initiating rollback..." -ForegroundColor Red
        Write-Host ""

        # Stop the failed service
        sc.exe stop $ServiceName 2>&1 | Out-Null
        Start-Sleep -Seconds 3

        # Restore backup
        $backupExe = Join-Path $backupSubDir "falcon.exe"
        $backupConf = Join-Path $backupSubDir "falcon.toml"

        if (Test-Path $backupExe) {
            Copy-Item $backupExe $CurrentExe -Force
            Write-Host "    Restored: falcon.exe"
        }
        if (Test-Path $backupConf) {
            Copy-Item $backupConf $ConfigFile -Force
            Write-Host "    Restored: falcon.toml"
        }

        # Restart with old version
        sc.exe start $ServiceName | Out-Null
        Start-Sleep -Seconds 5

        if (Test-ServiceRunning) {
            Write-Host "    Rollback complete — service running on previous version" -ForegroundColor Yellow
        } else {
            Write-Host "    CRITICAL: Rollback failed — manual intervention required" -ForegroundColor Red
        }

        exit 1
    }
}
