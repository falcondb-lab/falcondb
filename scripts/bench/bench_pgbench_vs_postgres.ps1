#Requires -Version 7.0
<#
.SYNOPSIS
  FalconDB vs PostgreSQL — pgbench Comparison Benchmark (Windows)

.DESCRIPTION
  Runs pgbench workload against FalconDB and PostgreSQL using identical settings.
  Produces comparable TPS and latency (avg + p95 if available).
  Output goes to bench_out\<timestamp>\

.EXAMPLE
  .\scripts\bench_pgbench_vs_postgres.ps1
  $env:SCALE=100; $env:CONCURRENCY=64; .\scripts\bench_pgbench_vs_postgres.ps1
  $env:MODE="read_only"; $env:RUNS=5; .\scripts\bench_pgbench_vs_postgres.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

# ── Defaults ────────────────────────────────────────────────────────────────
$FALCON_HOST   = if ($env:FALCON_HOST)  { $env:FALCON_HOST }  else { "127.0.0.1" }
$FALCON_PORT   = if ($env:FALCON_PORT)  { $env:FALCON_PORT }  else { "5433" }
$FALCON_DB     = if ($env:FALCON_DB)    { $env:FALCON_DB }    else { "falcon" }
$FALCON_USER   = if ($env:FALCON_USER)  { $env:FALCON_USER }  else { "falcon" }
$FALCON_BIN    = if ($env:FALCON_BIN)   { $env:FALCON_BIN }   else { "target\release\falcon_server.exe" }
$FALCON_CONF   = if ($env:FALCON_CONF)  { $env:FALCON_CONF }  else { "bench_configs\falcon.bench.toml" }

$PG_HOST       = if ($env:PG_HOST)      { $env:PG_HOST }      else { "127.0.0.1" }
$PG_PORT       = if ($env:PG_PORT)      { $env:PG_PORT }      else { "5432" }
$PG_DB         = if ($env:PG_DB)        { $env:PG_DB }        else { "pgbench" }
$PG_USER       = if ($env:PG_USER)      { $env:PG_USER }      else { "postgres" }

$CONCURRENCY   = if ($env:CONCURRENCY)   { [int]$env:CONCURRENCY }   else { 32 }
$JOBS          = if ($env:JOBS)           { [int]$env:JOBS }           else { $CONCURRENCY }
$DURATION_SEC  = if ($env:DURATION_SEC)   { [int]$env:DURATION_SEC }   else { 60 }
$SCALE         = if ($env:SCALE)          { [int]$env:SCALE }          else { 50 }
$WARMUP_SEC    = if ($env:WARMUP_SEC)     { [int]$env:WARMUP_SEC }     else { 15 }
$RUNS          = if ($env:RUNS)           { [int]$env:RUNS }           else { 3 }
$MODE          = if ($env:MODE)           { $env:MODE }                else { "tpcb_like" }

$SKIP_FALCON   = if ($env:SKIP_FALCON -eq "1") { $true } else { $false }
$SKIP_PG       = if ($env:SKIP_PG -eq "1")     { $true } else { $false }

$TS = (Get-Date -AsUTC -Format "yyyyMMddTHHmmssZ")
$OUT_DIR = "bench_out\$TS"

# ── Dependency check ───────────────────────────────────────────────────────
function Assert-Command($Name, $Hint) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        Write-Error "ERROR: required command '$Name' not found. $Hint"
        exit 1
    }
}

Assert-Command "pgbench" "Install PostgreSQL client tools and add to PATH."
Assert-Command "psql"    "Install PostgreSQL client tools and add to PATH."

# ── Mode flag ──────────────────────────────────────────────────────────────
$MODE_FLAG = switch ($MODE) {
    "tpcb_like"     { "" }
    "simple_update" { "-N" }
    "read_only"     { "-S" }
    "custom"        { "-f bench_configs\custom_pgbench.sql" }
    default         { Write-Error "Unknown MODE=$MODE"; exit 1 }
}

# ── Output directory ───────────────────────────────────────────────────────
New-Item -ItemType Directory -Force -Path "$OUT_DIR\pgbench\falcon"   | Out-Null
New-Item -ItemType Directory -Force -Path "$OUT_DIR\pgbench\postgres" | Out-Null

Write-Host "============================================================"
Write-Host "FalconDB vs PostgreSQL - pgbench Benchmark"
Write-Host "============================================================"
Write-Host "Timestamp : $TS"
Write-Host "Output    : $OUT_DIR\"
Write-Host "Mode      : $MODE"
Write-Host "Scale     : $SCALE"
Write-Host "Concurrency: $CONCURRENCY  Jobs: $JOBS"
Write-Host "Duration  : ${DURATION_SEC}s  Warmup: ${WARMUP_SEC}s  Runs: $RUNS"
Write-Host "============================================================"

# ── Environment snapshot ──────────────────────────────────────────────────
function Collect-EnvironmentSnapshot {
    $envFile = "$OUT_DIR\environment.txt"
    $lines = @()
    $lines += "=== Environment Snapshot ==="
    $lines += "Timestamp: $TS"

    try { $lines += "Git commit: $(git rev-parse HEAD 2>$null)" }
    catch { $lines += "Git commit: unknown" }
    try { $lines += "Git branch: $(git rev-parse --abbrev-ref HEAD 2>$null)" }
    catch { $lines += "Git branch: unknown" }

    $lines += ""
    $lines += "=== OS ==="
    $lines += "OS: $([System.Environment]::OSVersion.VersionString)"
    $lines += "Machine: $([System.Environment]::MachineName)"

    $lines += ""
    $lines += "=== CPU ==="
    try {
        $cpu = Get-CimInstance Win32_Processor -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($cpu) {
            $lines += "CPU: $($cpu.Name)"
            $lines += "Cores: $($cpu.NumberOfCores)  Logical: $($cpu.NumberOfLogicalProcessors)"
            $lines += "Max Clock: $($cpu.MaxClockSpeed) MHz"
        }
    } catch {
        $lines += "CPU info unavailable"
    }

    $lines += ""
    $lines += "=== Memory ==="
    try {
        $os = Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue
        if ($os) {
            $totalGB = [math]::Round($os.TotalVisibleMemorySize / 1MB, 1)
            $freeGB  = [math]::Round($os.FreePhysicalMemory / 1MB, 1)
            $lines += "Total: ${totalGB} GB  Free: ${freeGB} GB"
        }
    } catch {
        $lines += "Memory info unavailable"
    }

    $lines += ""
    $lines += "=== Disk ==="
    try {
        $disk = Get-PSDrive -Name (Split-Path -Qualifier (Get-Location).Path).TrimEnd(':') -ErrorAction SilentlyContinue
        if ($disk) {
            $freeGB = [math]::Round($disk.Free / 1GB, 1)
            $usedGB = [math]::Round($disk.Used / 1GB, 1)
            $lines += "Drive $($disk.Name): Free=${freeGB}GB Used=${usedGB}GB"
        }
    } catch {
        $lines += "Disk info unavailable"
    }

    $lines += ""
    $lines += "=== FalconDB Config ==="
    if (Test-Path $FALCON_CONF) {
        $lines += (Get-Content $FALCON_CONF -Raw)
    } else {
        $lines += "(config file not found: $FALCON_CONF)"
    }

    $lines | Out-File -FilePath $envFile -Encoding utf8
    Write-Host "  Environment snapshot saved to $envFile"
}

Collect-EnvironmentSnapshot

# Save config copy
if (Test-Path $FALCON_CONF) {
    Copy-Item $FALCON_CONF "$OUT_DIR\falcon_config_used.toml"
}

# ── Parse pgbench output ──────────────────────────────────────────────────
function Parse-PgbenchLog($LogFile) {
    $content = Get-Content $LogFile -Raw -ErrorAction SilentlyContinue
    if (-not $content) { return @{ TpsIncl = 0; TpsExcl = 0; LatAvg = 0 } }

    $tpsIncl = 0; $tpsExcl = 0; $latAvg = 0
    if ($content -match 'tps = ([0-9.]+) \(including') { $tpsIncl = [double]$Matches[1] }
    if ($content -match 'tps = ([0-9.]+) \(excluding') { $tpsExcl = [double]$Matches[1] }
    if ($content -match 'latency average = ([0-9.]+)') { $latAvg = [double]$Matches[1] }

    return @{ TpsIncl = $tpsIncl; TpsExcl = $tpsExcl; LatAvg = $latAvg }
}

# ── Init database ─────────────────────────────────────────────────────────
function Initialize-Database($Label, $Host, $Port, $Db, $User) {
    Write-Host "  Initializing pgbench schema on $Label ($Host`:$Port/$Db)..."

    # Create database (ignore errors)
    try {
        & psql -h $Host -p $Port -U $User -d postgres -c "CREATE DATABASE $Db;" 2>$null
    } catch {}

    & pgbench -h $Host -p $Port -U $User -d $Db -i -s $SCALE --no-vacuum *>&1 |
        Out-File "$OUT_DIR\pgbench\$Label\init.log" -Encoding utf8

    Write-Host "  $Label init complete (scale=$SCALE)."
}

# ── Run pgbench ───────────────────────────────────────────────────────────
function Run-PgbenchTarget($Label, $Host, $Port, $Db, $User) {
    $targetDir = "$OUT_DIR\pgbench\$Label"
    $results = @()

    Write-Host ""
    Write-Host "-- $Label`: warm-up (${WARMUP_SEC}s) --"
    $warmupCmd = "pgbench -h $Host -p $Port -U $User -d $Db -c $CONCURRENCY -j $JOBS -T $WARMUP_SEC $MODE_FLAG"
    Write-Host "  CMD: $warmupCmd"
    Invoke-Expression "$warmupCmd 2>&1" | Out-File "$targetDir\warmup.log" -Encoding utf8

    for ($i = 1; $i -le $RUNS; $i++) {
        Write-Host "-- $Label`: run $i/$RUNS (${DURATION_SEC}s) --"
        $runCmd = "pgbench -h $Host -p $Port -U $User -d $Db -c $CONCURRENCY -j $JOBS -T $DURATION_SEC $MODE_FLAG"
        Write-Host "  CMD: $runCmd"
        Invoke-Expression "$runCmd 2>&1" | Out-File "$targetDir\run$i.log" -Encoding utf8

        $parsed = Parse-PgbenchLog "$targetDir\run$i.log"
        $results += $parsed
        Write-Host "  Run $i`: TPS(incl)=$($parsed.TpsIncl)  TPS(excl)=$($parsed.TpsExcl)  Latency=$($parsed.LatAvg)ms"
    }

    return $results
}

# ── Compute summary ──────────────────────────────────────────────────────
function Compute-Summary($Results, $Label) {
    if ($Results.Count -eq 0) { return $null }
    $avgTpsIncl = ($Results | Measure-Object -Property TpsIncl -Average).Average
    $avgTpsExcl = ($Results | Measure-Object -Property TpsExcl -Average).Average
    $avgLat     = ($Results | Measure-Object -Property LatAvg  -Average).Average
    return @{
        Label    = $Label
        TpsIncl  = [math]::Round($avgTpsIncl, 2)
        TpsExcl  = [math]::Round($avgTpsExcl, 2)
        LatAvg   = [math]::Round($avgLat, 2)
        RunCount = $Results.Count
    }
}

# ── Generate JSON summary ─────────────────────────────────────────────────
function Generate-JsonSummary($FalconSummary, $PgSummary) {
    $jsonFile = "$OUT_DIR\summary_pgbench.json"
    $gitCommit = try { git rev-parse HEAD 2>$null } catch { "unknown" }

    $resultsObj = @{}
    if ($FalconSummary) {
        $resultsObj["falcon"] = @{
            tps_including_connections = $FalconSummary.TpsIncl
            tps_excluding_connections = $FalconSummary.TpsExcl
            latency_avg_ms           = $FalconSummary.LatAvg
            run_count                = $FalconSummary.RunCount
        }
    }
    if ($PgSummary) {
        $resultsObj["postgres"] = @{
            tps_including_connections = $PgSummary.TpsIncl
            tps_excluding_connections = $PgSummary.TpsExcl
            latency_avg_ms           = $PgSummary.LatAvg
            run_count                = $PgSummary.RunCount
        }
    }

    $json = @{
        timestamp    = $TS
        git_commit   = $gitCommit
        mode         = $MODE
        scale        = $SCALE
        concurrency  = $CONCURRENCY
        duration_sec = $DURATION_SEC
        warmup_sec   = $WARMUP_SEC
        runs         = $RUNS
        results      = $resultsObj
    } | ConvertTo-Json -Depth 4

    $json | Out-File $jsonFile -Encoding utf8
    Write-Host "  JSON summary: $jsonFile"
}

# ── Generate Markdown report ──────────────────────────────────────────────
function Generate-Report($FalconSummary, $PgSummary) {
    $report = "$OUT_DIR\REPORT.md"
    $gitCommit = try { git rev-parse HEAD 2>$null } catch { "unknown" }

    $lines = @()
    $lines += "# pgbench Benchmark Report"
    $lines += ""
    $lines += "**Date**: $TS"
    $lines += "**Git Commit**: $gitCommit"
    $lines += ""
    $lines += "## Parameters"
    $lines += ""
    $lines += "| Parameter    | Value           |"
    $lines += "|-------------|-----------------|"
    $lines += "| Mode        | $MODE           |"
    $lines += "| Scale       | $SCALE          |"
    $lines += "| Concurrency | $CONCURRENCY    |"
    $lines += "| Jobs        | $JOBS           |"
    $lines += "| Duration    | ${DURATION_SEC}s |"
    $lines += "| Warmup      | ${WARMUP_SEC}s   |"
    $lines += "| Runs        | $RUNS           |"
    $lines += ""
    $lines += "## Results"
    $lines += ""
    $lines += "| Target     | TPS (incl conn) | TPS (excl conn) | Latency avg (ms) | Runs |"
    $lines += "|-----------|-----------------|-----------------|-------------------|------|"

    foreach ($s in @($FalconSummary, $PgSummary)) {
        if ($s) {
            $lines += "| $($s.Label) | $($s.TpsIncl) | $($s.TpsExcl) | $($s.LatAvg) | $($s.RunCount) |"
        }
    }

    $lines += ""
    $lines += "## Commands Used"
    $lines += ""
    $lines += '```powershell'
    if (-not $SKIP_FALCON) {
        $lines += "pgbench -h $FALCON_HOST -p $FALCON_PORT -U $FALCON_USER -d $FALCON_DB -i -s $SCALE --no-vacuum"
        $lines += "pgbench -h $FALCON_HOST -p $FALCON_PORT -U $FALCON_USER -d $FALCON_DB -c $CONCURRENCY -j $JOBS -T $DURATION_SEC $MODE_FLAG"
    }
    if (-not $SKIP_PG) {
        $lines += "pgbench -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -i -s $SCALE --no-vacuum"
        $lines += "pgbench -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c $CONCURRENCY -j $JOBS -T $DURATION_SEC $MODE_FLAG"
    }
    $lines += '```'

    $lines += ""
    $lines += "## Environment"
    $lines += ""
    $lines += '```'
    if (Test-Path "$OUT_DIR\environment.txt") {
        $lines += (Get-Content "$OUT_DIR\environment.txt" -Raw)
    }
    $lines += '```'

    $lines += ""
    $lines += "## Notes"
    $lines += ""
    $lines += "- FalconDB authentication mode: TRUST (no password required)"
    $lines += "- WAL enabled with fdatasync durability"
    $lines += "- Results are not official TPC-B/TPC-C compliant"
    $lines += "- See docs/benchmark_methodology.md for reproducibility rules"

    $lines | Out-File $report -Encoding utf8
    Write-Host "  Report: $report"
}

# ── Main ──────────────────────────────────────────────────────────────────
$falconResults = @()
$pgResults     = @()

if (-not $SKIP_FALCON) {
    Write-Host ""
    Write-Host "=== FalconDB Target ==="
    Initialize-Database "falcon" $FALCON_HOST $FALCON_PORT $FALCON_DB $FALCON_USER
    $falconResults = Run-PgbenchTarget "falcon" $FALCON_HOST $FALCON_PORT $FALCON_DB $FALCON_USER
}

if (-not $SKIP_PG) {
    Write-Host ""
    Write-Host "=== PostgreSQL Target ==="
    Initialize-Database "postgres" $PG_HOST $PG_PORT $PG_DB $PG_USER
    $pgResults = Run-PgbenchTarget "postgres" $PG_HOST $PG_PORT $PG_DB $PG_USER
}

$falconSummary = Compute-Summary $falconResults "FalconDB"
$pgSummary     = Compute-Summary $pgResults "PostgreSQL"

Write-Host ""
Write-Host "=== Generating Report ==="
Generate-JsonSummary $falconSummary $pgSummary
Generate-Report $falconSummary $pgSummary

Write-Host ""
Write-Host "============================================================"
Write-Host "DONE - Results in: $OUT_DIR\"
Write-Host "  REPORT.md             - Human-readable report"
Write-Host "  summary_pgbench.json  - Machine-readable metrics"
Write-Host "  pgbench\*\run*.log    - Raw pgbench output"
Write-Host "  environment.txt       - System snapshot"
Write-Host "============================================================"
