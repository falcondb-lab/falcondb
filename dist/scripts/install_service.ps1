#Requires -RunAsAdministrator
# ============================================================================
# FalconDB — Install as Windows Service
# ============================================================================
#
# This script registers FalconDB as a Windows Service using sc.exe.
# No third-party tools (nssm, etc.) are required.
#
# Usage (run as Administrator):
#   .\scripts\install_service.ps1
#
# The service is created but NOT started. Start it manually:
#   sc start FalconDB

param(
    [string]$ServiceName = "FalconDB",
    [string]$DisplayName = "FalconDB Database",
    [string]$Description = "FalconDB — PG-Compatible In-Memory OLTP Database"
)

$ErrorActionPreference = "Stop"

Write-Host "================================================================="
Write-Host "  FalconDB — Windows Service Installation"
Write-Host "================================================================="
Write-Host ""

# ── Resolve paths ──
# The script assumes it lives in FalconDB\scripts\
# and the binary is at FalconDB\bin\falcon.exe
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir
$FalconExe = Join-Path $RootDir "bin\falcon.exe"
$ConfigFile = Join-Path $RootDir "conf\falcon.toml"

# ── Validate files exist ──
if (-not (Test-Path $FalconExe)) {
    Write-Host "ERROR: falcon.exe not found at: $FalconExe" -ForegroundColor Red
    Write-Host "  Make sure this script is in the 'scripts' folder of your FalconDB installation."
    exit 1
}
if (-not (Test-Path $ConfigFile)) {
    Write-Host "ERROR: falcon.toml not found at: $ConfigFile" -ForegroundColor Red
    exit 1
}

Write-Host "  Root directory : $RootDir"
Write-Host "  Binary         : $FalconExe"
Write-Host "  Config         : $ConfigFile"
Write-Host ""

# ── Check if service already exists ──
sc.exe query $ServiceName 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "WARNING: Service '$ServiceName' already exists." -ForegroundColor Yellow
    Write-Host "  Use 'sc delete $ServiceName' to remove it first,"
    Write-Host "  or run scripts\uninstall_service.ps1."
    exit 1
}

# ── Build binPath ──
# sc.exe requires the full path in quotes, with arguments outside.
# The working directory for the service will be the root directory.
$BinPath = "`"$FalconExe`" --config `"$ConfigFile`""

Write-Host "  binPath: $BinPath"
Write-Host ""

# ── Create the service ──
Write-Host "Creating service '$ServiceName'..."
sc.exe create $ServiceName `
    binPath= $BinPath `
    DisplayName= $DisplayName `
    start= auto | Out-Null

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to create service (exit code: $LASTEXITCODE)" -ForegroundColor Red
    exit 1
}

# ── Set description ──
sc.exe description $ServiceName $Description | Out-Null

# ── Set failure recovery (restart on failure) ──
# reset= 86400 (reset failure count after 24h)
# actions= restart/5000 (restart after 5s on first failure)
sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/10000/restart/30000 | Out-Null

Write-Host ""
Write-Host "================================================================="
Write-Host "  Service '$ServiceName' created successfully."
Write-Host ""
Write-Host "  Start the service:"
Write-Host "    sc start $ServiceName"
Write-Host ""
Write-Host "  Check status:"
Write-Host "    sc query $ServiceName"
Write-Host ""
Write-Host "  Stop the service:"
Write-Host "    sc stop $ServiceName"
Write-Host ""
Write-Host "  Uninstall:"
Write-Host "    .\scripts\uninstall_service.ps1"
Write-Host "================================================================="
