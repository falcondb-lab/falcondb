#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB DCG PoC — Run Workload (Windows)
.DESCRIPTION
    Starts the order writer against the primary node.
    Supports Rust binary, Python, and psql fallback.
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$env:FALCON_HOST    = if ($env:FALCON_HOST)    { $env:FALCON_HOST }    else { "127.0.0.1" }
$env:FALCON_PORT    = if ($env:FALCON_PORT)    { $env:FALCON_PORT }    else { "5433" }
$env:FALCON_DB      = if ($env:FALCON_DB)      { $env:FALCON_DB }      else { "falcon" }
$env:FALCON_USER    = if ($env:FALCON_USER)    { $env:FALCON_USER }    else { "falcon" }
$env:ORDER_COUNT    = if ($env:ORDER_COUNT)    { $env:ORDER_COUNT }    else { "1000" }
$env:OUTPUT_DIR     = $OutputDir
$env:START_ORDER_ID = if ($env:START_ORDER_ID) { $env:START_ORDER_ID } else { "1" }

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

# Clear previous committed log
"" | Set-Content (Join-Path $OutputDir "committed_orders.log") -NoNewline

$RustBin = Join-Path $PocRoot "workload\target\release\order_writer.exe"

if (Test-Path $RustBin) {
    Info "Using Rust workload generator"
    & $RustBin
} elseif ((Get-Command python3 -ErrorAction SilentlyContinue) -or (Get-Command python -ErrorAction SilentlyContinue)) {
    $py = if (Get-Command python3 -ErrorAction SilentlyContinue) { "python3" } else { "python" }
    & $py -c "import psycopg2" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Info "Using Python workload generator"
        & $py (Join-Path $PocRoot "workload\order_writer.py")
    } else {
        Info "psycopg2 not found, falling back to psql"
        # Fall through to psql below
        $usePsql = $true
    }
} else {
    $usePsql = $true
}

if ($usePsql) {
    Info "Using psql fallback"

    $OrderCount  = [int]$env:ORDER_COUNT
    $StartId     = [int]$env:START_ORDER_ID
    $Committed   = 0
    $Failed      = 0
    $logPath     = Join-Path $OutputDir "committed_orders.log"

    for ($i = 0; $i -lt $OrderCount; $i++) {
        $orderId = $StartId + $i
        $payload = "order-{0:D8}" -f $orderId

        $sql = "BEGIN; INSERT INTO orders (order_id, payload) VALUES ($orderId, '$payload'); COMMIT; SELECT $orderId;"
        try {
            $result = & psql -h $env:FALCON_HOST -p $env:FALCON_PORT -U $env:FALCON_USER -d $env:FALCON_DB -t -A -c $sql 2>$null
            if ($result -match "^$orderId$") {
                Add-Content -Path $logPath -Value "$orderId"
                $Committed++
            } else {
                $Failed++
            }
        } catch {
            $Failed++
        }

        if ((($i + 1) % 500 -eq 0) -or ($i + 1 -eq $OrderCount)) {
            Info "Progress: $($i+1)/$OrderCount (committed=$Committed, failed=$Failed)"
        }
    }

    $summary = @{
        orders_attempted       = $OrderCount
        orders_committed       = $Committed
        orders_failed          = $Failed
        first_order_id         = $StartId
        last_committed_order_id = $StartId + $OrderCount - 1
    } | ConvertTo-Json
    $summary | Set-Content (Join-Path $OutputDir "workload_summary.json")
}

$CommittedCount = (Get-Content (Join-Path $OutputDir "committed_orders.log") | Where-Object { $_ -match '^\d+$' }).Count
Ok "Workload complete: $CommittedCount orders committed"
Ok "Log: $(Join-Path $OutputDir 'committed_orders.log')"
