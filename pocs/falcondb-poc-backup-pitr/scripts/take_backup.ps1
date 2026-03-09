#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #7 — Backup & PITR: Take Full Backup (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot     = Split-Path -Parent $ScriptDir
$OutputDir   = Join-Path $PocRoot "output"

$HostAddr    = "127.0.0.1"
$FalconPort  = 5433
$FalconUser  = "falcon"
$FalconDb    = "pitr_demo"
$DataDir     = Join-Path $PocRoot "pitr_data"
$BackupDir   = Join-Path $PocRoot "backups"
$WalArchive  = Join-Path $PocRoot "wal_archive"
$Manifest    = Join-Path $OutputDir "backup_manifest.json"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

New-Item -ItemType Directory -Path $BackupDir, $WalArchive, $OutputDir -Force | Out-Null

Write-Host "`n  Taking Full Backup (T0)`n" -ForegroundColor Cyan

$backupTs    = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$backupLabel = "full_backup_" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$backupPath  = Join-Path $BackupDir $backupLabel

Info "Recording pre-backup state..."
$rowCount     = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT COUNT(*) FROM accounts;" 2>$null).Trim()
$totalBalance = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT SUM(balance) FROM accounts;" 2>$null).Trim()
Ok "Pre-backup: $rowCount accounts, total balance: $totalBalance"

Info "Creating data snapshot..."
Copy-Item -Recurse -Force $DataDir $backupPath
$backupSize = (Get-ChildItem -Recurse $backupPath | Measure-Object -Property Length -Sum).Sum
Ok "Snapshot created: $backupPath ($backupSize bytes)"

Info "Archiving WAL segments..."
$walDir = Join-Path $DataDir "wal"
$walCount = 0
if (Test-Path $walDir) {
    Get-ChildItem $walDir -File | ForEach-Object {
        Copy-Item $_.FullName $WalArchive -Force
        $walCount++
    }
}
Ok "Archived $walCount WAL segments"

$walFiles = (Get-ChildItem $WalArchive -File -ErrorAction SilentlyContinue).Count
$baseLsn = $walFiles

Info "Writing manifest..."
$manifest = @{
    backup_label         = $backupLabel
    backup_time          = $backupTs
    base_lsn             = "$baseLsn"
    backup_path          = $backupPath
    wal_archive          = $WalArchive
    data_dir             = $DataDir
    database             = $FalconDb
    row_count            = [int]$rowCount
    total_balance        = [long]$totalBalance
    backup_size_bytes    = $backupSize
    wal_segments_archived= $walCount
    status               = "completed"
}
$manifest | ConvertTo-Json -Depth 3 | Set-Content $Manifest -Encoding UTF8
Ok "Manifest written: $Manifest"

Get-ChildItem $WalArchive 2>$null | Format-Table -AutoSize | Out-File (Join-Path $OutputDir "wal_segments.log")
Ok "WAL inventory: $(Join-Path $OutputDir 'wal_segments.log')"

Write-Host ""
Write-Host "  Backup Complete (T0)"
Write-Host "  Label:    $backupLabel"
Write-Host "  Time:     $backupTs"
Write-Host "  Accounts: $rowCount"
Write-Host "  Balance:  $totalBalance"
Write-Host ""
