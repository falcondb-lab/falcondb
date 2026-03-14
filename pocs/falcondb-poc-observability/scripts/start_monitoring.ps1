#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #4 — Observability: Start Monitoring Stack (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

Write-Host "`n  Starting Monitoring Stack (Prometheus + Grafana)`n" -ForegroundColor Cyan

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Fail "Docker not found. Install Docker to use the monitoring stack."
    Write-Host "  Alternative: run Prometheus and Grafana manually."
    exit 1
}
Ok "Docker available"

Info "Starting containers..."
& docker compose -f (Join-Path $PocRoot "docker\docker-compose.yml") up -d

Info "Waiting for Prometheus..."
for ($i = 1; $i -le 20; $i++) {
    try {
        $null = Invoke-RestMethod -Uri "http://127.0.0.1:9090/-/ready" -TimeoutSec 2 -ErrorAction SilentlyContinue
        Ok "Prometheus ready: http://127.0.0.1:9090"
        break
    } catch {}
    Start-Sleep -Seconds 1
}

Info "Waiting for Grafana..."
for ($i = 1; $i -le 30; $i++) {
    try {
        $null = Invoke-RestMethod -Uri "http://127.0.0.1:3000/api/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
        Ok "Grafana ready: http://127.0.0.1:3000 (admin/admin)"
        break
    } catch {}
    Start-Sleep -Seconds 1
}

Write-Host ""
Write-Host "  Monitoring Stack"
Write-Host "  Prometheus: http://127.0.0.1:9090"
Write-Host "  Grafana:    http://127.0.0.1:3000 (admin/admin)"
Write-Host "  Dashboard:  'FalconDB Cluster Overview' (auto-provisioned)"
Write-Host ""
