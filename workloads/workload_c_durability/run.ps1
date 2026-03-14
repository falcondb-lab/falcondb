# Workload C: Durability Write Throughput — Windows Runner
# Usage:
#   .\run.ps1
#   .\run.ps1 -Target postgres -Password Hy12345678 -Clients 16 -Duration 60
param(
    [string]$Target   = "falcondb",
    [int]   $Clients  = 16,
    [int]   $Duration = 60,
    [string]$DbHost   = "127.0.0.1",
    [string]$Port     = "",
    [string]$Db       = "falcon",
    [string]$User     = "falcon",
    [string]$Password = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir  = $PSScriptRoot
$ResultsDir = Join-Path $ScriptDir "results"
New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null

if (-not $Port) {
    $Port = if ($Target -eq "postgres") { "5432" } else { "5433" }
}
if ($User -eq "falcon" -and $Target -eq "postgres") {
    $User = "postgres"
    $Db   = if ($Db -eq "falcon") { "postgres" } else { $Db }
}
if ($Target -eq "postgres") {
    if (-not $Password) {
        $ss = Read-Host -Prompt "Password for user $User" -AsSecureString
        $Password = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($ss))
    }
    $env:PGPASSWORD = $Password
}

$pgBin   = "C:\Program Files\PostgreSQL\16\bin"
$psql    = Join-Path $pgBin "psql.exe"
$pgbench = Join-Path $pgBin "pgbench.exe"

$Timestamp  = (Get-Date -Format "yyyyMMddTHHmmssZ")
$ResultFile = Join-Path $ResultsDir "workload_c_${Target}_${Timestamp}.json"
$RawLog     = Join-Path $ResultsDir "workload_c_${Target}_${Timestamp}.log"

Write-Host ""
Write-Host "=== Workload C: Durability Write Throughput ==="
Write-Host "  Target:   $Target ($DbHost`:$Port)"
Write-Host "  Clients:  $Clients"
Write-Host "  Duration: ${Duration}s"
Write-Host "  Mix:      100% INSERT (durable writes)"
Write-Host ""

# Enforce durability on PG
if ($Target -eq "postgres") {
    & $psql -h $DbHost -p $Port -U $User -d $Db -c "ALTER SYSTEM SET synchronous_commit = 'on';" 2>$null | Out-Null
    & $psql -h $DbHost -p $Port -U $User -d $Db -c "ALTER SYSTEM SET fsync = 'on';" 2>$null | Out-Null
    & $psql -h $DbHost -p $Port -U $User -d $Db -c "SELECT pg_reload_conf();" 2>$null | Out-Null
}

# Schema
Write-Host "  Setting up schema..."
$ErrorActionPreference = "Continue"
& $psql -h $DbHost -p $Port -U $User -d $Db -f (Join-Path $ScriptDir "schema.sql") 2>&1 | Out-Null
$ErrorActionPreference = "Stop"
Write-Host "  Schema ready"

# Run pgbench
Write-Host "  Running pgbench (${Duration}s, $Clients clients)..."
$pgbenchArgs = @(
    "-h", $DbHost, "-p", $Port, "-U", $User,
    "-f", (Join-Path $ScriptDir "workload.sql"),
    "-c", $Clients, "-j", $Clients, "-T", $Duration,
    "-P", "5", "--no-vacuum", "-r", $Db
)
Start-Process -FilePath $pgbench -ArgumentList $pgbenchArgs `
    -RedirectStandardOutput $RawLog -RedirectStandardError "$RawLog.err" `
    -NoNewWindow -Wait
Get-Content "$RawLog.err" | Write-Host
Get-Content "$RawLog.err" | Add-Content $RawLog
Remove-Item "$RawLog.err" -ErrorAction SilentlyContinue

# Parse results
$rawContent = Get-Content $RawLog -Raw
$tpsMatch   = [regex]::Match($rawContent, 'tps = ([0-9.]+)')
$latMatch   = [regex]::Match($rawContent, 'latency average = ([0-9.]+)')
$Tps        = if ($tpsMatch.Success) { $tpsMatch.Groups[1].Value } else { "0" }
$LatencyAvg = if ($latMatch.Success) { $latMatch.Groups[1].Value } else { "0" }

# Post-run state
$RowCount = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM commit_log;").Trim()
$DupPks   = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c `
    "SELECT COUNT(*) FROM (SELECT seq_id FROM commit_log GROUP BY seq_id HAVING COUNT(*) > 1) d;").Trim()

# Verification
Write-Host ""
Write-Host "  Running verification..."
Write-Host "    row_count|$RowCount"
Write-Host "    dup_pks|$DupPks"

$PkUnique = if ([int]$DupPks -eq 0) { "true" } else { "false" }

# JSON output
@"
{
  "workload": "C",
  "name": "durability_write_throughput",
  "target": "$Target",
  "timestamp": "$Timestamp",
  "config": {
    "clients": $Clients,
    "duration_sec": $Duration,
    "host": "$DbHost",
    "port": $Port,
    "mix": "100% INSERT (durable writes)"
  },
  "results": {
    "tps": $Tps,
    "latency_avg_ms": $LatencyAvg,
    "rows": $RowCount,
    "pk_unique": $PkUnique
  }
}
"@ | Set-Content -Path $ResultFile -Encoding UTF8

Write-Host ""
Write-Host "  === Results ==="
Write-Host "  TPS:         $Tps"
Write-Host "  Avg Latency: $LatencyAvg ms"
Write-Host "  Rows:        $RowCount"
Write-Host "  PK Unique:   $PkUnique"
Write-Host "  Output:      $ResultFile"
Write-Host ""
