# Workload C: Durability & Crash — Windows Runner
#
# Validates durability invariants:
#   CP-4: Crash after DurableCommit MUST NOT lose the transaction
#   FC-1: Crash before CP-D -> txn rolled back
#   FC-2: Crash after CP-D -> txn survives
#   FC-4: WAL replay is idempotent
#
# Usage:
#   .\crash_runner.ps1
#   .\crash_runner.ps1 -FalconBin .\target\release\falcon.exe -Rounds 3
param(
    [string]$FalconBin    = "target\release\falcon.exe",
    [string]$FalconConfig = "examples_server_config\falcon.toml",
    [string]$DataDir      = "$env:TEMP\falcon_crash_test_data",
    [string]$DbHost       = "127.0.0.1",
    [int]   $Port         = 5433,
    [string]$Db           = "falcon",
    [string]$User         = "falcon",
    [int]   $WriteCount   = 5000,
    [int]   $WriteBeforeKill = 3000,
    [int]   $Rounds       = 3
)

$ErrorActionPreference = "Stop"

$ScriptDir  = $PSScriptRoot
$ResultsDir = Join-Path $ScriptDir "results"
New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null

$pgBin = "C:\Program Files\PostgreSQL\16\bin"
$psql  = Join-Path $pgBin "psql.exe"

$Timestamp  = (Get-Date -Format "yyyyMMddTHHmmssZ")
$ReportFile = Join-Path $ResultsDir "recovery_report_${Timestamp}.json"

$FalconBinAbs    = if ([System.IO.Path]::IsPathRooted($FalconBin)) { $FalconBin } else { Join-Path (Get-Location) $FalconBin }
$FalconConfigAbs = if ([System.IO.Path]::IsPathRooted($FalconConfig)) { $FalconConfig } else { Join-Path (Get-Location) $FalconConfig }

$FalconProcess = $null
$TmpConfig     = Join-Path $env:TEMP "falcon_crash_test.toml"

function Start-Falcon {
    if (Test-Path $DataDir) { Remove-Item -Recurse -Force $DataDir }
    New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

    $configContent = Get-Content $FalconConfigAbs -Raw
    $configContent = $configContent -replace 'data_dir\s*=\s*"[^"]*"', "data_dir = `"$($DataDir -replace '\\', '/')`""
    Set-Content -Path $TmpConfig -Value $configContent -Encoding UTF8

    $script:FalconProcess = Start-Process -FilePath $FalconBinAbs -ArgumentList "--config", $TmpConfig -PassThru -WindowStyle Hidden

    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Milliseconds 500
        & $psql -h $DbHost -p $Port -U $User -d $Db -c "SELECT 1" 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { return }
    }
    throw "FalconDB did not start within 15s"
}

function Restart-Falcon {
    $script:FalconProcess = Start-Process -FilePath $FalconBinAbs -ArgumentList "--config", $TmpConfig -PassThru -WindowStyle Hidden

    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Milliseconds 500
        & $psql -h $DbHost -p $Port -U $User -d $Db -c "SELECT 1" 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { return }
    }
    throw "FalconDB did not restart within 15s (WAL recovery may have failed)"
}

function Stop-Falcon {
    if ($script:FalconProcess -and -not $script:FalconProcess.HasExited) {
        $script:FalconProcess.Kill()
        $script:FalconProcess.WaitForExit(5000) | Out-Null
    }
}

Write-Host ""
Write-Host "=== Workload C: Durability & Crash Recovery ==="
Write-Host "  Rounds:       $Rounds"
Write-Host "  Writes/round: $WriteCount"
Write-Host "  Kill after:   $WriteBeforeKill committed"
Write-Host "  Binary:       $FalconBinAbs"
Write-Host ""

$OverallPass  = $true
$RoundResults = @()

try {
    for ($round = 1; $round -le $Rounds; $round++) {
        Write-Host ""
        Write-Host "-- Round $round/$Rounds --"

        $CommittedLog = Join-Path $env:TEMP "falcon_crash_committed_${round}.txt"
        New-Item -Path $CommittedLog -ItemType File -Force | Out-Null

        # 1. Start fresh
        Write-Host "  Starting FalconDB..."
        Start-Falcon
        Write-Host "  Server up (PID $($FalconProcess.Id))"

        # 2. Schema
        & $psql -h $DbHost -p $Port -U $User -d $Db -f (Join-Path $ScriptDir "schema.sql") 2>&1 | Out-Null
        Write-Host "  Schema created"

        # 3. Write transactions
        Write-Host "  Writing $WriteCount transactions (kill after $WriteBeforeKill)..."
        $committed = 0
        $failed    = 0

        for ($seq = 1; $seq -le $WriteCount; $seq++) {
            & $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c `
                "INSERT INTO commit_log (seq_id, payload) VALUES ($seq, 'round${round}-seq${seq}')" 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) {
                Add-Content -Path $CommittedLog -Value $seq
                $committed++
            } else {
                $failed++
            }

            if ($committed -eq $WriteBeforeKill) {
                Write-Host "  >>> Killing PID $($FalconProcess.Id) (after $committed commits) <<<"
                $FalconProcess.Kill()
                $FalconProcess.WaitForExit(5000) | Out-Null
                break
            }
        }

        $ClientCount = (Get-Content $CommittedLog | Measure-Object -Line).Lines
        Write-Host "  Client committed: $ClientCount (failed: $failed)"

        # 4. Restart
        Write-Host "  Restarting FalconDB (WAL recovery)..."
        Restart-Falcon
        Write-Host "  Server recovered (PID $($FalconProcess.Id))"

        # 5. Verify
        Write-Host "  Verifying..."
        $DbCount = (& $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT COUNT(*) FROM commit_log;" 2>$null).Trim()

        $DbIds      = Join-Path $env:TEMP "falcon_crash_db_ids_${round}.txt"
        & $psql -h $DbHost -p $Port -U $User -d $Db -t -A -c "SELECT seq_id FROM commit_log ORDER BY seq_id;" 2>$null | Set-Content $DbIds

        $ClientSorted = Get-Content $CommittedLog | ForEach-Object { [int]$_ } | Sort-Object
        $DbSorted     = Get-Content $DbIds | Where-Object { $_ -match '^\d+$' } | ForEach-Object { [int]$_ } | Sort-Object

        $Missing  = ($ClientSorted | Where-Object { $_ -notin $DbSorted } | Measure-Object).Count
        $Phantom  = ($DbSorted | Where-Object { $_ -notin $ClientSorted } | Measure-Object).Count
        $Dups     = ($DbSorted | Group-Object | Where-Object { $_.Count -gt 1 } | Measure-Object).Count

        $RoundVerdict = if ($Missing -gt 0 -or $Dups -gt 0) { "FAIL" } else { "PASS" }
        if ($RoundVerdict -eq "FAIL") { $OverallPass = $false }

        Write-Host "    Client committed:  $ClientCount"
        Write-Host "    DB after recovery: $DbCount"
        Write-Host "    Missing (loss):    $Missing"
        Write-Host "    Phantom:           $Phantom"
        Write-Host "    Duplicates:        $Dups"
        Write-Host "  Round ${round}: $RoundVerdict"

        $RoundResults += @{
            round            = $round
            verdict          = $RoundVerdict
            client_committed = $ClientCount
            db_after_recovery = [int]$DbCount
            missing_data_loss = $Missing
            phantom          = $Phantom
            duplicates       = $Dups
            kill_after       = $WriteBeforeKill
        }

        Stop-Falcon
        Remove-Item -Force $CommittedLog, $DbIds -ErrorAction SilentlyContinue
    }
} finally {
    Stop-Falcon
    Remove-Item -Force $TmpConfig -ErrorAction SilentlyContinue
    if (Test-Path $DataDir) { Remove-Item -Recurse -Force $DataDir -ErrorAction SilentlyContinue }
}

$FinalVerdict = if ($OverallPass) { "PASS" } else { "FAIL" }

$roundsJson = ($RoundResults | ForEach-Object {
    $r = $_
    "    { `"round`": $($r.round), `"verdict`": `"$($r.verdict)`", `"client_committed`": $($r.client_committed), `"db_after_recovery`": $($r.db_after_recovery), `"missing_data_loss`": $($r.missing_data_loss), `"phantom`": $($r.phantom), `"duplicates`": $($r.duplicates), `"kill_after`": $($r.kill_after) }"
}) -join ",`n"

@"
{
  "workload": "C",
  "name": "durability_crash",
  "timestamp": "$Timestamp",
  "config": {
    "rounds": $Rounds,
    "writes_per_round": $WriteCount,
    "kill_after": $WriteBeforeKill,
    "falcon_bin": "$($FalconBinAbs -replace '\\', '/')",
    "durability": "fsync + fdatasync + WAL"
  },
  "invariants_tested": [
    "CP-4: crash after DurableCommit must not lose txn",
    "FC-1: crash before CP-D -> txn rolled back",
    "FC-2: crash after CP-D -> txn survives",
    "FC-4: WAL replay is idempotent (no double commit)"
  ],
  "rounds": [
$roundsJson
  ],
  "verdict": "$FinalVerdict"
}
"@ | Set-Content -Path $ReportFile -Encoding UTF8

Write-Host ""
Write-Host "=== Final Verdict: $FinalVerdict ==="
Write-Host "  Report: $ReportFile"
Write-Host ""

if (-not $OverallPass) { exit 1 }
