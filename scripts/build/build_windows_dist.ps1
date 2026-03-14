# ============================================================================
# FalconDB — Build Windows Green Distribution (ZIP)
# ============================================================================
#
# Produces: FalconDB-windows-x86_64.zip
#
# Structure:
#   FalconDB/
#     bin/falcon.exe
#     conf/falcon.toml
#     data/README.md
#     logs/README.md
#     scripts/install_service.ps1
#     scripts/uninstall_service.ps1
#     README_WINDOWS.md
#     VERSION
#
# Usage:
#   .\scripts\build_windows_dist.ps1 [-Release] [-SkipBuild]

param(
    [switch]$Release,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

Write-Host "================================================================="
Write-Host "  FalconDB — Windows Distribution Builder"
Write-Host "================================================================="
Write-Host ""

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$DistSource = Join-Path $RepoRoot "dist"
$BuildProfile = if ($Release) { "release" } else { "debug" }
$TargetDir = Join-Path $RepoRoot "target\$BuildProfile"
$OutputDir = Join-Path $RepoRoot "target\dist"
$StagingDir = Join-Path $OutputDir "FalconDB"
$ZipName = "FalconDB-windows-x86_64.zip"
$ZipPath = Join-Path $OutputDir $ZipName

# ── Gate 0: Build ──
if (-not $SkipBuild) {
    Write-Host "[1/5] Building falcon ($BuildProfile)..."
    Push-Location $RepoRoot
    if ($Release) {
        cargo build -p falcon_server --release
    } else {
        cargo build -p falcon_server
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  FAIL: Build failed" -ForegroundColor Red
        Pop-Location
        exit 1
    }
    Pop-Location
    Write-Host "  PASS: Build succeeded"
} else {
    Write-Host "[1/5] Skipping build (--SkipBuild)"
}

# ── Verify binary exists ──
$FalconExe = Join-Path $TargetDir "falcon.exe"
if (-not (Test-Path $FalconExe)) {
    Write-Host "ERROR: falcon.exe not found at: $FalconExe" -ForegroundColor Red
    exit 1
}

# ── Gate 1: Clean staging ──
Write-Host "[2/5] Preparing staging directory..."
if (Test-Path $StagingDir) {
    Remove-Item -Recurse -Force $StagingDir
}
if (Test-Path $ZipPath) {
    Remove-Item -Force $ZipPath
}
New-Item -ItemType Directory -Path $StagingDir -Force | Out-Null

# ── Gate 2: Copy files ──
Write-Host "[3/5] Copying distribution files..."

# bin/
$binDir = Join-Path $StagingDir "bin"
New-Item -ItemType Directory -Path $binDir -Force | Out-Null
Copy-Item $FalconExe (Join-Path $binDir "falcon.exe")
Write-Host "  bin/falcon.exe"

# conf/
$confDir = Join-Path $StagingDir "conf"
New-Item -ItemType Directory -Path $confDir -Force | Out-Null
Copy-Item (Join-Path $DistSource "conf\falcon.toml") (Join-Path $confDir "falcon.toml")
Write-Host "  conf/falcon.toml"

# data/
$dataDir = Join-Path $StagingDir "data"
New-Item -ItemType Directory -Path $dataDir -Force | Out-Null
Copy-Item (Join-Path $DistSource "data\README.md") (Join-Path $dataDir "README.md")
Write-Host "  data/README.md"

# logs/
$logsDir = Join-Path $StagingDir "logs"
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
Copy-Item (Join-Path $DistSource "logs\README.md") (Join-Path $logsDir "README.md")
Write-Host "  logs/README.md"

# scripts/
$scriptsDir = Join-Path $StagingDir "scripts"
New-Item -ItemType Directory -Path $scriptsDir -Force | Out-Null
Copy-Item (Join-Path $DistSource "scripts\install_service.ps1") (Join-Path $scriptsDir "install_service.ps1")
Copy-Item (Join-Path $DistSource "scripts\uninstall_service.ps1") (Join-Path $scriptsDir "uninstall_service.ps1")
Write-Host "  scripts/install_service.ps1"
Write-Host "  scripts/uninstall_service.ps1"

# Root files
Copy-Item (Join-Path $DistSource "README_WINDOWS.md") (Join-Path $StagingDir "README_WINDOWS.md")
Copy-Item (Join-Path $DistSource "README_WINDOWS_POC.md") (Join-Path $StagingDir "README_WINDOWS_POC.md")

# VERSION — auto-generated from workspace Cargo.toml (Single Source of Truth)
$cargoContent = Get-Content (Join-Path $RepoRoot "Cargo.toml") -Raw
if ($cargoContent -match 'version\s*=\s*"([^"]+)"') {
    $PkgVersion = $Matches[1]
} else {
    $PkgVersion = "unknown"
}
$gitHash = "unknown"
try { $gitHash = (git -C $RepoRoot rev-parse --short=8 HEAD 2>$null) } catch {}
@"
FalconDB v$PkgVersion
Build: windows-x86_64
Git: $gitHash
"@ | Set-Content -Path (Join-Path $StagingDir "VERSION") -Encoding UTF8
Write-Host "  README_WINDOWS.md"
Write-Host "  README_WINDOWS_POC.md"
Write-Host "  VERSION (auto-generated: v$PkgVersion)"

# PoC check script
Copy-Item (Join-Path $RepoRoot "scripts\poc_check_windows.ps1") (Join-Path $scriptsDir "poc_check_windows.ps1")
Write-Host "  scripts/poc_check_windows.ps1"

# ── Gate 3: Verify structure ──
Write-Host "[4/5] Verifying distribution structure..."
$requiredFiles = @(
    "bin\falcon.exe",
    "conf\falcon.toml",
    "data\README.md",
    "logs\README.md",
    "scripts\install_service.ps1",
    "scripts\uninstall_service.ps1",
    "scripts\poc_check_windows.ps1",
    "README_WINDOWS.md",
    "README_WINDOWS_POC.md",
    "VERSION"
)

$missing = 0
foreach ($f in $requiredFiles) {
    $fullPath = Join-Path $StagingDir $f
    if (Test-Path $fullPath) {
        Write-Host "  OK: $f"
    } else {
        Write-Host "  MISSING: $f" -ForegroundColor Red
        $missing++
    }
}

if ($missing -gt 0) {
    Write-Host "ERROR: $missing file(s) missing from distribution" -ForegroundColor Red
    exit 1
}

# ── Gate 4: Create ZIP ──
Write-Host "[5/5] Creating ZIP archive..."
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
Compress-Archive -Path $StagingDir -DestinationPath $ZipPath -Force

$zipSize = (Get-Item $ZipPath).Length
$zipSizeMB = [math]::Round($zipSize / 1MB, 2)

Write-Host ""
Write-Host "================================================================="
Write-Host "  Distribution built successfully!"
Write-Host ""
Write-Host "  ZIP:  $ZipPath"
Write-Host "  Size: $zipSizeMB MB"
Write-Host ""
Write-Host "  To test:"
Write-Host "    1. Extract the ZIP"
Write-Host "    2. cd FalconDB"
Write-Host "    3. .\bin\falcon.exe --config .\conf\falcon.toml"
Write-Host "    4. psql -h 127.0.0.1 -p 5443 -U falcon"
Write-Host "================================================================="
