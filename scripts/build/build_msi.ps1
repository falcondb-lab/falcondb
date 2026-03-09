# ============================================================================
# FalconDB — Build MSI Installer (WiX v4)
# ============================================================================
#
# Prerequisites:
#   1. .NET SDK 6.0+: https://dotnet.microsoft.com/download
#   2. WiX v4:        dotnet tool install --global wix
#   3. WiX Util ext:  wix extension add WixToolset.Util.wixext
#   4. Rust toolchain with x86_64-pc-windows-msvc target
#
# Usage:
#   .\scripts\build_msi.ps1
#   .\scripts\build_msi.ps1 -SkipBuild   # skip cargo build, use existing exe
#
# Output:
#   .\target\installer\FalconDB-<version>-x64.msi
# ============================================================================

param(
    [switch]$SkipBuild,
    [string]$Configuration = "release"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

Write-Host "================================================================="
Write-Host "  FalconDB — MSI Installer Build"
Write-Host "================================================================="
Write-Host ""

# ── Gate 1: Check prerequisites ──
Write-Host "[1/5] Checking prerequisites..."

$wixVersion = $null
try {
    $wixVersion = & wix --version 2>&1
    Write-Host "  WiX:  $wixVersion"
} catch {
    Write-Host "  ERROR: WiX Toolset not found." -ForegroundColor Red
    Write-Host "  Install: dotnet tool install --global wix"
    Write-Host "  Then:    wix extension add WixToolset.Util.wixext"
    exit 1
}

# ── Gate 2: Build falcon.exe ──
if ($SkipBuild) {
    Write-Host "[2/5] Skipping cargo build (using existing binary)"
} else {
    Write-Host "[2/5] Building falcon.exe (release)..."
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

$FalconExe = Join-Path $RepoRoot "target\release\falcon.exe"
if (-not (Test-Path $FalconExe)) {
    Write-Host "  ERROR: falcon.exe not found at $FalconExe" -ForegroundColor Red
    exit 1
}

# Get version from exe
$versionOutput = & $FalconExe version 2>&1
$versionLine = ($versionOutput | Select-Object -First 1) -replace "FalconDB v", ""
if ([string]::IsNullOrWhiteSpace($versionLine)) {
    $versionLine = "1.0.0"
}
# MSI requires x.y.z.w format — pad if needed
$msiVersion = $versionLine -replace "-.*", ""
$parts = $msiVersion.Split(".")
while ($parts.Count -lt 3) { $parts += "0" }
$msiVersion = ($parts[0..2]) -join "."
Write-Host "  Version: $versionLine (MSI: $msiVersion)"
Write-Host "  Binary:  $FalconExe"

# ── Gate 3: Prepare staging ──
Write-Host "[3/5] Preparing staging directories..."

$OutDir = Join-Path $RepoRoot "target\installer"
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

# Staging for binary
$BinStage = Join-Path $OutDir "stage\bin"
New-Item -ItemType Directory -Path $BinStage -Force | Out-Null
Copy-Item $FalconExe (Join-Path $BinStage "falcon.exe") -Force

# Staging for config
$ConfStage = Join-Path $OutDir "stage\conf"
New-Item -ItemType Directory -Path $ConfStage -Force | Out-Null

$DistConf = Join-Path $RepoRoot "dist\conf\falcon.toml"
if (Test-Path $DistConf) {
    Copy-Item $DistConf (Join-Path $ConfStage "falcon.toml") -Force
} else {
    # Generate default config
    & $FalconExe --print-default-config | Out-File -Encoding utf8 (Join-Path $ConfStage "falcon.toml")
}

Write-Host "  Staged: bin\falcon.exe"
Write-Host "  Staged: conf\falcon.toml"

# ── Gate 4: Build MSI ──
Write-Host "[4/5] Building MSI..."

$WxsFile = Join-Path $RepoRoot "installer\FalconDB.wxs"
$MsiFile = Join-Path $OutDir "FalconDB-$versionLine-x64.msi"

& wix build `
    -o $MsiFile `
    -d "BinSource=$BinStage" `
    -d "ConfSource=$ConfStage" `
    -ext WixToolset.Util.wixext `
    $WxsFile

if ($LASTEXITCODE -ne 0) {
    Write-Host "  ERROR: WiX build failed" -ForegroundColor Red
    exit 1
}

# ── Gate 5: Verify ──
Write-Host "[5/5] Verifying MSI..."

if (Test-Path $MsiFile) {
    $size = (Get-Item $MsiFile).Length
    $sizeMB = [math]::Round($size / 1MB, 2)
    Write-Host "  Output: $MsiFile"
    Write-Host "  Size:   $sizeMB MB"
    Write-Host ""
    Write-Host "================================================================="
    Write-Host "  MSI build complete!"
    Write-Host ""
    Write-Host "  Install:   msiexec /i `"$MsiFile`""
    Write-Host "  Silent:    msiexec /i `"$MsiFile`" /qn"
    Write-Host "  Uninstall: msiexec /x `"$MsiFile`""
    Write-Host "================================================================="
} else {
    Write-Host "  ERROR: MSI not found after build" -ForegroundColor Red
    exit 1
}
