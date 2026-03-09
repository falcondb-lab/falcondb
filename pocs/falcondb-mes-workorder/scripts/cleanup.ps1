#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB MES — Cleanup (Windows)
.PARAMETER All
    Also remove data and output files.
#>
param([switch]$All)

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$backendPidFile = Join-Path $OutputDir "backend.pid"
if (Test-Path $backendPidFile) {
    $procId = [int](Get-Content $backendPidFile).Trim()
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    Remove-Item $backendPidFile -Force
    Ok "Backend stopped"
}

$falconPidFile = Join-Path $OutputDir "falcon.pid"
if (Test-Path $falconPidFile) {
    $procId = [int](Get-Content $falconPidFile).Trim()
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    Remove-Item $falconPidFile -Force
    Ok "FalconDB stopped"
}

if ($All) {
    Info "Removing data..."
    Remove-Item -Recurse -Force (Join-Path $PocRoot "mes_data") -ErrorAction SilentlyContinue
    Ok "Data removed"

    Info "Removing output files..."
    Get-ChildItem $OutputDir -Include "*.log","*.txt","*.json","*.csv" -Recurse | Remove-Item -Force
    Ok "Output files removed"
}

Write-Host ""
Ok "Cleanup complete"
