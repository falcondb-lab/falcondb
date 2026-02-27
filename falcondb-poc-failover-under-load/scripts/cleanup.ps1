#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #3 — Failover Under Load: Cleanup (Windows)
.PARAMETER All
    If specified, also removes data directories and output files.
#>
param([switch]$All)

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

# Stop writer
$writerPidFile = Join-Path $OutputDir "writer.pid"
if (Test-Path $writerPidFile) {
    $wpid = [int](Get-Content $writerPidFile).Trim()
    try {
        $null = Get-Process -Id $wpid -ErrorAction Stop
        Info "Stopping writer (pid $wpid)..."
        "" | Set-Content (Join-Path $OutputDir "stop_writer") -NoNewline
        Start-Sleep -Seconds 2
        Stop-Process -Id $wpid -Force -ErrorAction SilentlyContinue
        Ok "Writer stopped"
    } catch {
        Info "Writer (pid $wpid) already stopped"
    }
    Remove-Item $writerPidFile -Force
}

# Stop FalconDB nodes
foreach ($role in @("primary", "replica")) {
    $pidFile = Join-Path $OutputDir "$role.pid"
    if (Test-Path $pidFile) {
        $procId = [int](Get-Content $pidFile).Trim()
        try {
            $null = Get-Process -Id $procId -ErrorAction Stop
            Info "Stopping $role (pid $procId)..."
            Stop-Process -Id $procId -Force
            Ok "$role stopped"
        } catch {
            Info "$role (pid $procId) already stopped"
        }
        Remove-Item $pidFile -Force
    }
}

if ($All) {
    Info "Removing data directories..."
    Remove-Item ".\poc_data_primary" -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item ".\poc_data_replica" -Recurse -Force -ErrorAction SilentlyContinue
    Ok "Data directories removed"

    Info "Removing output files..."
    Get-ChildItem $OutputDir -Include "*.log","*.txt","*.json","*.pid" -Recurse |
        Remove-Item -Force
    Remove-Item (Join-Path $OutputDir "stop_writer") -Force -ErrorAction SilentlyContinue
    Ok "Output files removed"
}

Ok "Cleanup complete"
