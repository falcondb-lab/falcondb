# Workload A: Write-Heavy — Windows Runner
# Usage:
#   .\run.ps1
#   .\run.ps1 -Target postgres -Clients 32 -Duration 120
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

$pgBin  = "C:\Program Files\PostgreSQL\16\bin"
$psql   = Join-Path $pgBin "psql.exe"
$pgbench = Join-Path $pgBin "pgbench.exe"

$Timestamp  = (Get-Date -Format "yyyyMMddTHHmmssZ")
$ResultFile = Join-Path $ResultsDir "workload_a_${Target}_${Timestamp}.json"
$RawLog     = Join-Path $ResultsDir "workload_a_${Target}_${Timestamp}.log"

Write-Host ""
Write-Host "=== Workload A: Write-Heavy ==="
Write-Host "  Target:   $Target ($DbHost`:$Port)"
Write-Host "  Clients:  $Clients"
Write-Host "  Duration: ${Duration}s"
Write-Host "  Mix:      90% INSERT / 10% UPDATE"
Write-Host ""

# Fairness: enforce durability on PG
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
Write-Host "  Schema ready (10K seed rows)"

# Pre-run count
$PreCount = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM orders;").Trim()

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
$rawContent  = Get-Content $RawLog -Raw
$tpsMatch    = [regex]::Match($rawContent, 'tps = ([0-9.]+)')
$latMatch    = [regex]::Match($rawContent, 'latency average = ([0-9.]+)')
$Tps         = if ($tpsMatch.Success)  { $tpsMatch.Groups[1].Value }  else { "0" }
$LatencyAvg  = if ($latMatch.Success)  { $latMatch.Groups[1].Value }  else { "0" }

# Post-run count
$PostCount = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM orders;").Trim()
$Inserted  = [int]$PostCount - [int]$PreCount

# Verification
Write-Host ""
Write-Host "  Running verification..."
$env:PAGER = ""
& $psql -h $DbHost -p $Port -U $User -d $Db -f (Join-Path $ScriptDir "verify.sql") -t -A 2>&1

$DupCount = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c `
    "SELECT COUNT(*) FROM (SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*) > 1) d;").Trim()
$VerifyPass = if ([int]$DupCount -eq 0) { "true" } else { "false" }

# JSON output
@"
{
  "workload": "A",
  "name": "write_heavy",
  "target": "$Target",
  "timestamp": "$Timestamp",
  "config": {
    "clients": $Clients,
    "duration_sec": $Duration,
    "host": "$DbHost",
    "port": $Port,
    "mix": "90% INSERT / 10% UPDATE",
    "durability": "fsync + synchronous_commit"
  },
  "results": {
    "tps": $Tps,
    "latency_avg_ms": $LatencyAvg,
    "rows_before": $PreCount,
    "rows_after": $PostCount,
    "rows_inserted": $Inserted
  },
  "verification": {
    "pk_unique": $VerifyPass,
    "duplicate_pks": $DupCount
  }
}
"@ | Set-Content -Path $ResultFile -Encoding UTF8

Write-Host ""
Write-Host "  === Results ==="
Write-Host "  TPS:           $Tps"
Write-Host "  Avg Latency:   $LatencyAvg ms"
Write-Host "  Rows Inserted: $Inserted"
Write-Host "  PK Unique:     $VerifyPass"
Write-Host "  Output:        $ResultFile"
Write-Host ""
