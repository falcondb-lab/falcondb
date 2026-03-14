# Workload B: Read-Write OLTP — Windows Runner
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

$pgBin   = "C:\Program Files\PostgreSQL\16\bin"
$psql    = Join-Path $pgBin "psql.exe"
$pgbench = Join-Path $pgBin "pgbench.exe"

$Timestamp  = (Get-Date -Format "yyyyMMddTHHmmssZ")
$ResultFile = Join-Path $ResultsDir "workload_b_${Target}_${Timestamp}.json"
$RawLog     = Join-Path $ResultsDir "workload_b_${Target}_${Timestamp}.log"

Write-Host ""
Write-Host "=== Workload B: Read-Write OLTP ==="
Write-Host "  Target:   $Target ($DbHost`:$Port)"
Write-Host "  Clients:  $Clients"
Write-Host "  Duration: ${Duration}s"
Write-Host "  Mix:      50% SELECT / 25% INSERT / 25% UPDATE"
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

# Seed data via batch INSERT VALUES (FalconDB doesn't support INSERT...SELECT)
$BatchSize = 500
Write-Host "  Seeding 50K users..."
for ($base = 1; $base -le 50000; $base += $BatchSize) {
    $end = [Math]::Min($base + $BatchSize - 1, 50000)
    $vals = ($base..$end | ForEach-Object {
        $bal = 10000 + ($_ % 90000)
        $st  = if ($_ % 20 -eq 0) { 'inactive' } else { 'active' }
        "($_,'u_$_',$bal,'$st')"
    }) -join ','
    & $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "INSERT INTO users (user_id,username,balance,status) VALUES $vals" 2>$null | Out-Null
}
Write-Host "  Seeding 100K orders..."
for ($base = 1; $base -le 100000; $base += $BatchSize) {
    $end = [Math]::Min($base + $BatchSize - 1, 100000)
    $vals = ($base..$end | ForEach-Object {
        $uid = ($_ % 50000) + 1
        $amt = 50 + ($_ % 5000)
        $ot  = if ($_ % 3 -eq 0) { 'refund' } else { 'purchase' }
        "($_,$uid,$amt,'$ot')"
    }) -join ','
    & $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "INSERT INTO orders (order_id,user_id,amount,order_type) VALUES $vals" 2>$null | Out-Null
}
$seedUsers  = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM users").Trim()
$seedOrders = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM orders").Trim()
Write-Host "  Schema ready ($seedUsers users, $seedOrders orders)"

# Pre-run state
$PreUsers   = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM users;").Trim()
$PreOrders  = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM orders;").Trim()
$PreBalance = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT SUM(balance) FROM users;").Trim()

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
$PostUsers   = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM users;").Trim()
$PostOrders  = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM orders;").Trim()
$PostBalance = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT SUM(balance) FROM users;").Trim()
$NewOrders   = [int]$PostOrders - [int]$PreOrders

# Verification
Write-Host ""
Write-Host "  Running verification..."
$env:PAGER = ""
& $psql -h $DbHost -p $Port -U $User -d $Db -f (Join-Path $ScriptDir "verify.sql") -t -A 2>&1 | Select-Object -First 20

$DupUsers  = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c `
    "SELECT COUNT(*) FROM (SELECT user_id FROM users GROUP BY user_id HAVING COUNT(*) > 1) d;").Trim()
$DupOrders = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c `
    "SELECT COUNT(*) FROM (SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*) > 1) d;").Trim()

$PkUniqueUsers  = if ([int]$DupUsers  -eq 0) { "true" } else { "false" }
$PkUniqueOrders = if ([int]$DupOrders -eq 0) { "true" } else { "false" }
$VerifyPass     = if ([int]$DupUsers -eq 0 -and [int]$DupOrders -eq 0) { "true" } else { "false" }

# JSON output
@"
{
  "workload": "B",
  "name": "read_write_oltp",
  "target": "$Target",
  "timestamp": "$Timestamp",
  "config": {
    "clients": $Clients,
    "duration_sec": $Duration,
    "host": "$DbHost",
    "port": $Port,
    "mix": "50% SELECT / 25% INSERT / 25% UPDATE",
    "durability": "fsync + synchronous_commit"
  },
  "results": {
    "tps": $Tps,
    "latency_avg_ms": $LatencyAvg,
    "users_before": $PreUsers,
    "users_after": $PostUsers,
    "orders_before": $PreOrders,
    "orders_after": $PostOrders,
    "new_orders": $NewOrders,
    "balance_before": $PreBalance,
    "balance_after": $PostBalance
  },
  "verification": {
    "pk_unique_users": $PkUniqueUsers,
    "pk_unique_orders": $PkUniqueOrders,
    "pass": $VerifyPass
  }
}
"@ | Set-Content -Path $ResultFile -Encoding UTF8

Write-Host ""
Write-Host "  === Results ==="
Write-Host "  TPS:         $Tps"
Write-Host "  Avg Latency: $LatencyAvg ms"
Write-Host "  New Orders:  $NewOrders"
Write-Host "  PK Unique:   $VerifyPass"
Write-Host "  Output:      $ResultFile"
Write-Host ""
