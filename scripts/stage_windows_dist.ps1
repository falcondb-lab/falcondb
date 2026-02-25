# ============================================================================
# FalconDB — Stage Windows Distribution for MSI Packaging
# ============================================================================
#
# Creates a clean staging directory from CI build artifacts.
# The MSI build (packaging/wix/build.ps1) consumes this staging dir.
#
# Usage:
#   .\scripts\stage_windows_dist.ps1 -Version 1.0.0
#   .\scripts\stage_windows_dist.ps1 -Version 1.0.0 -BinDir .\target\release
#
# Output:
#   dist\windows\stage\
#     bin\falcon.exe
#     conf\falcon.toml
#     VERSION
#     LICENSE
#     NOTICE
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$Version,

    [string]$BinDir = "",

    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

Write-Host "================================================================="
Write-Host "  FalconDB — Stage Windows Distribution"
Write-Host "  Version: $Version"
Write-Host "================================================================="
Write-Host ""

# ── Resolve binary directory ──
if ([string]::IsNullOrWhiteSpace($BinDir)) {
    $BinDir = Join-Path $RepoRoot "target\release"
}

# ── Build if needed ──
if (-not $SkipBuild) {
    $FalconExe = Join-Path $BinDir "falcon.exe"
    if (-not (Test-Path $FalconExe)) {
        Write-Host "[BUILD] falcon.exe not found — building release..."
        Push-Location $RepoRoot
        try {
            cargo build --release --bin falcon
            if ($LASTEXITCODE -ne 0) {
                Write-Host "  ERROR: cargo build failed" -ForegroundColor Red
                exit 1
            }
        } finally {
            Pop-Location
        }
    }
}

$FalconExe = Join-Path $BinDir "falcon.exe"
if (-not (Test-Path $FalconExe)) {
    Write-Host "ERROR: falcon.exe not found at: $FalconExe" -ForegroundColor Red
    Write-Host "  Build first:  cargo build --release --bin falcon"
    Write-Host "  Or specify:   -BinDir <path>"
    exit 1
}

# ── Prepare staging directory ──
$StageDir = Join-Path $RepoRoot "dist\windows\stage"

Write-Host "[1/4] Cleaning staging directory..."
if (Test-Path $StageDir) {
    Remove-Item -Recurse -Force $StageDir
}
New-Item -ItemType Directory -Path $StageDir -Force | Out-Null

# ── Copy binary ──
Write-Host "[2/4] Staging binary..."
$StageBin = Join-Path $StageDir "bin"
New-Item -ItemType Directory -Path $StageBin -Force | Out-Null
Copy-Item $FalconExe (Join-Path $StageBin "falcon.exe") -Force
Write-Host "  bin\falcon.exe"

# ── Copy config template ──
Write-Host "[3/4] Staging configuration and docs..."
$StageConf = Join-Path $StageDir "conf"
New-Item -ItemType Directory -Path $StageConf -Force | Out-Null

$DistConf = Join-Path $RepoRoot "dist\conf\falcon.toml"
if (Test-Path $DistConf) {
    Copy-Item $DistConf (Join-Path $StageConf "falcon.toml") -Force
} else {
    Write-Host "  WARNING: dist\conf\falcon.toml not found, generating default" -ForegroundColor Yellow
    @"
# FalconDB Configuration — default template
config_version = 3

[server]
pg_listen_addr = "0.0.0.0:5443"
admin_listen_addr = "0.0.0.0:8080"
node_id = 1

[storage]
data_dir = "C:\\ProgramData\\FalconDB\\data"
wal_enabled = true

[wal]
group_commit = true
sync_mode = "fdatasync"
"@ | Out-File -Encoding utf8 (Join-Path $StageConf "falcon.toml")
}
Write-Host "  conf\falcon.toml"

# ── VERSION file ──
$VersionContent = @"
FalconDB v$Version
Build: windows-x86_64
"@
$VersionContent | Out-File -Encoding utf8 -NoNewline (Join-Path $StageDir "VERSION")
Write-Host "  VERSION"

# ── LICENSE / NOTICE ──
$LicenseSrc = Join-Path $RepoRoot "LICENSE"
if (Test-Path $LicenseSrc) {
    Copy-Item $LicenseSrc (Join-Path $StageDir "LICENSE") -Force
} else {
    "Copyright (c) 2024–2026 FalconDB Contributors. All rights reserved." |
        Out-File -Encoding utf8 (Join-Path $StageDir "LICENSE")
}
Write-Host "  LICENSE"

$NoticeSrc = Join-Path $RepoRoot "NOTICE"
if (Test-Path $NoticeSrc) {
    Copy-Item $NoticeSrc (Join-Path $StageDir "NOTICE") -Force
} else {
    "FalconDB — Enterprise Database System`nSee LICENSE for details." |
        Out-File -Encoding utf8 (Join-Path $StageDir "NOTICE")
}
Write-Host "  NOTICE"

# ── README ──
$ReadmeSrc = Join-Path $RepoRoot "dist\README_WINDOWS.md"
if (Test-Path $ReadmeSrc) {
    Copy-Item $ReadmeSrc (Join-Path $StageDir "README.md") -Force
    Write-Host "  README.md"
}

# ── Verify ──
Write-Host "[4/4] Verifying staging directory..."
$requiredFiles = @(
    "bin\falcon.exe",
    "conf\falcon.toml",
    "VERSION",
    "LICENSE",
    "NOTICE"
)

$missing = 0
foreach ($f in $requiredFiles) {
    $fullPath = Join-Path $StageDir $f
    if (Test-Path $fullPath) {
        $size = (Get-Item $fullPath).Length
        Write-Host "  OK  $f ($size bytes)"
    } else {
        Write-Host "  MISSING  $f" -ForegroundColor Red
        $missing++
    }
}

if ($missing -gt 0) {
    Write-Host ""
    Write-Host "ERROR: $missing required file(s) missing" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "================================================================="
Write-Host "  Staging complete: $StageDir"
Write-Host "  Next: .\packaging\wix\build.ps1 -Version $Version"
Write-Host "================================================================="
