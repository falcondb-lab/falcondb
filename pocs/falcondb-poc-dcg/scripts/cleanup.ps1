#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB DCG PoC — Cleanup (Windows)
.DESCRIPTION
    Stops all FalconDB processes and optionally removes data directories.
.PARAMETER All
    If specified, also removes data directories and output files.
.EXAMPLE
    .\scripts\cleanup.ps1
    .\scripts\cleanup.ps1 -All
#>
param([switch]$All)

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

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
    Ok "Output files removed"
}

Ok "Cleanup complete"
