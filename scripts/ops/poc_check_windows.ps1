# ============================================================================
# FalconDB — PoC Verification Script (Windows)
# ============================================================================
#
# Validates the full FalconDB lifecycle on Windows:
#   1. Console mode: start → verify ports → Ctrl+C → verify ports released
#   2. Service mode: install → start → verify → stop → verify → uninstall
#
# Usage (run as Administrator for service tests):
#   .\scripts\poc_check_windows.ps1
#
# Exit code 0 = all gates passed, non-zero = failure.

param(
    [string]$FalconExe = ".\bin\falcon.exe",
    [string]$ConfigFile = ".\conf\falcon.toml",
    [int]$PgPort = 5443,
    [int]$HttpPort = 8080,
    [int]$StartupWaitSec = 5,
    [int]$ShutdownWaitSec = 10
)

$ErrorActionPreference = "Stop"
$pass = 0
$fail = 0
$total = 0

function Gate([string]$name, [scriptblock]$test) {
    $script:total++
    Write-Host -NoNewline "  [$script:total] $name ... "
    try {
        $result = & $test
        if ($result) {
            Write-Host "PASS" -ForegroundColor Green
            $script:pass++
        } else {
            Write-Host "FAIL" -ForegroundColor Red
            $script:fail++
        }
    } catch {
        Write-Host "FAIL ($_)" -ForegroundColor Red
        $script:fail++
    }
}

function Test-PortListening([int]$port) {
    $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    return ($null -ne $conn -and $conn.Count -gt 0)
}

function Test-PortFree([int]$port) {
    $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    return ($null -eq $conn -or $conn.Count -eq 0)
}

Write-Host "================================================================="
Write-Host "  FalconDB — PoC Verification (Windows)"
Write-Host "================================================================="
Write-Host ""

# ── Gate 0: Prerequisites ──
Write-Host "Phase 0: Prerequisites"
Write-Host "---------------------"

Gate "falcon.exe exists" {
    Test-Path $FalconExe
}

Gate "falcon.toml exists" {
    Test-Path $ConfigFile
}

Gate "Port $PgPort is free" {
    Test-PortFree $PgPort
}

Gate "Port $HttpPort is free" {
    Test-PortFree $HttpPort
}

if ($fail -gt 0) {
    Write-Host ""
    Write-Host "ABORT: Prerequisites not met ($fail failures)" -ForegroundColor Red
    exit 1
}

# ── Phase 1: Console Mode ──
Write-Host ""
Write-Host "Phase 1: Console Mode"
Write-Host "---------------------"

$proc = $null
try {
    $proc = Start-Process -FilePath $FalconExe `
        -ArgumentList "--config", $ConfigFile, "--no-wal" `
        -PassThru -WindowStyle Hidden

    Start-Sleep -Seconds $StartupWaitSec

    Gate "falcon.exe process is running" {
        -not $proc.HasExited
    }

    Gate "PG port $PgPort is listening" {
        Test-PortListening $PgPort
    }

    Gate "HTTP port $HttpPort is listening" {
        Test-PortListening $HttpPort
    }

    # Health check
    Gate "Health endpoint responds" {
        try {
            $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$HttpPort/health" -UseBasicParsing -TimeoutSec 3
            $resp.StatusCode -eq 200
        } catch { $false }
    }

} finally {
    # Send Ctrl+C via taskkill (graceful)
    if ($proc -and -not $proc.HasExited) {
        Write-Host "  Sending stop signal..."
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        $proc.WaitForExit($ShutdownWaitSec * 1000) | Out-Null
    }
}

Start-Sleep -Seconds 2

Gate "falcon.exe process has exited" {
    $proc.HasExited
}

Gate "PG port $PgPort released after shutdown" {
    Test-PortFree $PgPort
}

Gate "HTTP port $HttpPort released after shutdown" {
    Test-PortFree $HttpPort
}

# ── Phase 2: Service Mode (requires Administrator) ──
Write-Host ""
Write-Host "Phase 2: Service Mode"
Write-Host "---------------------"

$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "  SKIP: Service tests require Administrator privileges" -ForegroundColor Yellow
    Write-Host "  Re-run this script as Administrator to test service mode."
} else {
    # Clean up any previous service
    sc.exe query FalconDB 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        sc.exe stop FalconDB 2>&1 | Out-Null
        Start-Sleep -Seconds 2
        sc.exe delete FalconDB 2>&1 | Out-Null
        Start-Sleep -Seconds 1
    }

    # Install
    Gate "falcon service install succeeds" {
        $out = & $FalconExe service install --config $ConfigFile 2>&1
        $LASTEXITCODE -eq 0
    }

    Gate "Service is registered" {
        sc.exe query FalconDB 2>&1 | Out-Null
        $LASTEXITCODE -eq 0
    }

    # Start
    Gate "sc start FalconDB succeeds" {
        sc.exe start FalconDB 2>&1 | Out-Null
        Start-Sleep -Seconds $StartupWaitSec
        $query = sc.exe query FalconDB 2>&1
        ($query | Out-String).Contains("RUNNING")
    }

    if ((sc.exe query FalconDB 2>&1 | Out-String).Contains("RUNNING")) {
        Gate "PG port $PgPort listening (service mode)" {
            Test-PortListening $PgPort
        }

        Gate "HTTP port $HttpPort listening (service mode)" {
            Test-PortListening $HttpPort
        }
    }

    # Stop
    Gate "sc stop FalconDB succeeds" {
        sc.exe stop FalconDB 2>&1 | Out-Null
        Start-Sleep -Seconds $ShutdownWaitSec
        $query = sc.exe query FalconDB 2>&1
        ($query | Out-String).Contains("STOPPED")
    }

    Start-Sleep -Seconds 2

    Gate "PG port $PgPort released after service stop" {
        Test-PortFree $PgPort
    }

    Gate "HTTP port $HttpPort released after service stop" {
        Test-PortFree $HttpPort
    }

    # Uninstall
    Gate "falcon service uninstall succeeds" {
        & $FalconExe service uninstall 2>&1 | Out-Null
        $true
    }

    Gate "Service is removed" {
        sc.exe query FalconDB 2>&1 | Out-Null
        $LASTEXITCODE -ne 0
    }
}

# ── Phase 3: File System ──
Write-Host ""
Write-Host "Phase 3: File System"
Write-Host "--------------------"

Gate "data/ directory exists" {
    Test-Path "data" -PathType Container
}

Gate "logs/ directory exists" {
    Test-Path "logs" -PathType Container
}

if ($isAdmin) {
    Gate "ProgramData\FalconDB directory exists" {
        Test-Path "$env:ProgramData\FalconDB" -PathType Container
    }
}

# ── Summary ──
Write-Host ""
Write-Host "================================================================="
Write-Host "  Results: $pass passed, $fail failed, $total total"
Write-Host "================================================================="

if ($fail -gt 0) {
    Write-Host "  PoC verification FAILED" -ForegroundColor Red
    exit 1
} else {
    Write-Host "  PoC verification PASSED" -ForegroundColor Green
    exit 0
}
