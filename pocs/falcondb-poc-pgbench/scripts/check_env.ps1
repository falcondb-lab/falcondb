#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB pgbench PoC — Environment Check (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$ResultsDir = Join-Path $PocRoot "results"
$FalconBin  = if ($env:FALCON_BIN) { $env:FALCON_BIN } else { "target\release\falcon_server.exe" }

function Ok($msg)     { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg)   { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg)   { Write-Host "  > $msg" -ForegroundColor Yellow }
function Banner($msg) { Write-Host "`n  $msg`n" -ForegroundColor Cyan }

$Errors = 0

Banner "FalconDB pgbench PoC - Environment Check"

# ── Required tools ────────────────────────────────────────────────────────
Banner "1. Required Tools"

foreach ($tool in @("pgbench", "psql")) {
    if (Get-Command $tool -ErrorAction SilentlyContinue) {
        $ver = & $tool --version 2>$null | Select-Object -First 1
        Ok "$tool`: $ver"
    } else {
        Fail "$tool`: NOT FOUND"
        $Errors++
    }
}

if (-not (Test-Path $FalconBin)) {
    $RepoBin = Join-Path (Split-Path -Parent $PocRoot) $FalconBin
    if (Test-Path $RepoBin) {
        $FalconBin = $RepoBin
        Ok "FalconDB: $FalconBin"
    } else {
        Fail "FalconDB binary not found at '$FalconBin'"
        Info "Build it: cargo build -p falcon_server --release"
        $Errors++
    }
} else {
    Ok "FalconDB: $FalconBin"
}

if (Get-Command pg_isready -ErrorAction SilentlyContinue) {
    Ok "pg_isready available"
} elseif (Get-Command pg_ctl -ErrorAction SilentlyContinue) {
    Ok "pg_ctl available"
} else {
    Fail "PostgreSQL server tools not found"
    $Errors++
}

# ── Hardware info ─────────────────────────────────────────────────────────
Banner "2. Hardware"

New-Item -ItemType Directory -Path (Join-Path $ResultsDir "parsed") -Force | Out-Null

$cpuInfo = Get-CimInstance Win32_Processor | Select-Object -First 1
$osInfo  = Get-CimInstance Win32_OperatingSystem
$ramGB   = [math]::Round($osInfo.TotalVisibleMemorySize / 1MB, 1)

Info "OS:   $($osInfo.Caption) $($osInfo.Version)"
Info "CPU:  $($cpuInfo.Name) ($($cpuInfo.NumberOfLogicalProcessors) cores)"
Info "RAM:  $ramGB GB"

$pgbenchVer = try { & pgbench --version 2>$null | Select-Object -First 1 } catch { "unknown" }
$psqlVer    = try { & psql --version 2>$null | Select-Object -First 1 } catch { "unknown" }
if (-not $pgbenchVer) { $pgbenchVer = "unknown" }
if (-not $psqlVer)    { $psqlVer = "unknown" }

$envData = @{
    os              = "$($osInfo.Caption) $($osInfo.Version)"
    cpu_model       = $cpuInfo.Name
    cpu_cores       = $cpuInfo.NumberOfLogicalProcessors
    ram_gb          = "$ramGB"
    pgbench_version = $pgbenchVer
    psql_version    = $psqlVer
    falcon_bin      = $FalconBin
    timestamp       = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
} | ConvertTo-Json -Depth 2

$envData | Set-Content (Join-Path $ResultsDir "parsed\environment.json")
Ok "Environment saved"

# ── Verdict ───────────────────────────────────────────────────────────────
Banner "Result"

if ($Errors -eq 0) {
    Ok "All prerequisites met. Ready to benchmark."
    exit 0
} else {
    Fail "$Errors prerequisite(s) missing."
    exit 1
}
