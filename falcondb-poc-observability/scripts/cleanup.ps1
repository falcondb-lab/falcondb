#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #4 — Observability: Cleanup (Windows)
.PARAMETER All
    Also remove data directories, output files, and Docker volumes.
#>
param([switch]$All)

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

foreach ($node in @("node1","node2")) {
    $pidFile = Join-Path $OutputDir "$node.pid"
    if (Test-Path $pidFile) {
        $procId = [int](Get-Content $pidFile).Trim()
        try {
            Stop-Process -Id $procId -Force -ErrorAction Stop
            Ok "Stopped $node (pid $procId)"
        } catch {
            Info "$node (pid $procId) already stopped"
        }
        Remove-Item $pidFile -Force
    }
}

Info "Stopping monitoring containers..."
& docker compose -f (Join-Path $PocRoot "docker\docker-compose.yml") down 2>$null
if ($LASTEXITCODE -ne 0) { Info "Docker Compose not available or containers already stopped" }

if ($All) {
    Info "Removing data directories..."
    Remove-Item -Recurse -Force ".\obs_data_node1" -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force ".\obs_data_node2" -ErrorAction SilentlyContinue
    Ok "Data directories removed"

    Info "Removing output files..."
    Get-ChildItem $OutputDir -Include "*.log","*.pid","*.txt","*.json" -Recurse | Remove-Item -Force
    Ok "Output files removed"

    Info "Removing Docker volumes..."
    & docker compose -f (Join-Path $PocRoot "docker\docker-compose.yml") down -v 2>$null
    Ok "Docker volumes removed"
}

Write-Host ""
Ok "Cleanup complete"
