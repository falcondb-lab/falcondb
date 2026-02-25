# ============================================================================
# FalconDB — One-Click MSI Build (WiX v4)
# ============================================================================
#
# Prerequisites:
#   1. .NET SDK 8.0+:  https://dotnet.microsoft.com/download
#   2. WiX v4:         dotnet tool install --global wix
#   3. Staging dir:    .\scripts\stage_windows_dist.ps1 -Version x.y.z
#
# Usage:
#   .\packaging\wix\build.ps1 -Version 1.0.0
#   .\packaging\wix\build.ps1 -Version 1.0.0 -SkipStage
#
# Output:
#   dist\windows\FalconDB-1.0.0-x64.msi
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$Version,

    [switch]$SkipStage,

    [string]$Configuration = "Release"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)

Write-Host "================================================================="
Write-Host "  FalconDB — MSI Build (WiX v4)"
Write-Host "  Version: $Version"
Write-Host "================================================================="
Write-Host ""

# ── Validate version format ──
if ($Version -notmatch '^\d+\.\d+\.\d+') {
    Write-Host "ERROR: Version must be in x.y.z format (got: $Version)" -ForegroundColor Red
    exit 1
}
# MSI requires exactly 3-part version
$MsiVersion = ($Version -replace '-.*', '')
$parts = $MsiVersion.Split('.')
if ($parts.Count -lt 3) {
    while ($parts.Count -lt 3) { $parts += "0" }
}
$MsiVersion = ($parts[0..2]) -join '.'
Write-Host "  MSI Version: $MsiVersion"

# ── Gate 1: Check prerequisites ──
Write-Host ""
Write-Host "[1/5] Checking prerequisites..."

# .NET SDK
try {
    $dotnetVersion = & dotnet --version 2>&1
    Write-Host "  .NET SDK: $dotnetVersion"
} catch {
    Write-Host "  ERROR: .NET SDK not found" -ForegroundColor Red
    Write-Host "  Install: https://dotnet.microsoft.com/download"
    exit 1
}

# WiX v4 (check via dotnet tool)
$wixAvailable = $false
try {
    $wixCheck = & dotnet tool list --global 2>&1 | Select-String "wix"
    if ($wixCheck) {
        Write-Host "  WiX:     installed (global tool)"
        $wixAvailable = $true
    }
} catch {}

if (-not $wixAvailable) {
    Write-Host "  WARNING: WiX global tool not detected" -ForegroundColor Yellow
    Write-Host "  Install: dotnet tool install --global wix"
    Write-Host "  Continuing with dotnet build (WiX SDK will be restored via NuGet)..."
}

# ── Gate 2: Stage files ──
Write-Host ""
if ($SkipStage) {
    Write-Host "[2/5] Skipping staging (--SkipStage)"
} else {
    Write-Host "[2/5] Staging distribution files..."
    $StageScript = Join-Path $RepoRoot "scripts\stage_windows_dist.ps1"
    if (-not (Test-Path $StageScript)) {
        Write-Host "  ERROR: $StageScript not found" -ForegroundColor Red
        exit 1
    }
    & $StageScript -Version $Version -SkipBuild
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR: Staging failed" -ForegroundColor Red
        exit 1
    }
}

$StageDir = Join-Path $RepoRoot "dist\windows\stage"
if (-not (Test-Path (Join-Path $StageDir "bin\falcon.exe"))) {
    Write-Host "  ERROR: Staging directory missing falcon.exe" -ForegroundColor Red
    Write-Host "  Run:   .\scripts\stage_windows_dist.ps1 -Version $Version"
    exit 1
}
Write-Host "  Stage dir: $StageDir"

# ── Gate 3: Resolve paths ──
Write-Host ""
Write-Host "[3/5] Resolving paths..."

$WixProjDir = $ScriptDir
$WixProj = Join-Path $WixProjDir "FalconDB.wixproj"
$OutputDir = Join-Path $RepoRoot "dist\windows"
$MsiName = "FalconDB-$Version-x64.msi"
$MsiPath = Join-Path $OutputDir $MsiName

if (-not (Test-Path $WixProj)) {
    Write-Host "  ERROR: WiX project not found: $WixProj" -ForegroundColor Red
    exit 1
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
Write-Host "  Project:  $WixProj"
Write-Host "  Output:   $MsiPath"

# ── Gate 4: Build MSI ──
Write-Host ""
Write-Host "[4/5] Building MSI..."

# Resolve StageDir to absolute path for WiX
$StageDirAbs = (Resolve-Path $StageDir).Path

Push-Location $WixProjDir
try {
    dotnet build `
        -c $Configuration `
        -p:ProductVersion=$MsiVersion `
        -p:DefineConstants="ProductVersion=$MsiVersion;StageDir=$StageDirAbs" `
        -o $OutputDir

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR: WiX build failed" -ForegroundColor Red
        exit 1
    }
} finally {
    Pop-Location
}

# Rename output if needed (dotnet build may produce different name)
$BuiltMsi = Get-ChildItem -Path $OutputDir -Filter "FalconDB*.msi" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($BuiltMsi -and $BuiltMsi.FullName -ne $MsiPath) {
    Move-Item $BuiltMsi.FullName $MsiPath -Force
}

# ── Gate 5: Verify ──
Write-Host ""
Write-Host "[5/5] Verifying MSI..."

if (Test-Path $MsiPath) {
    $size = (Get-Item $MsiPath).Length
    $sizeMB = [math]::Round($size / 1MB, 2)

    Write-Host ""
    Write-Host "================================================================="
    Write-Host "  MSI build successful!"
    Write-Host ""
    Write-Host "  File:    $MsiPath"
    Write-Host "  Size:    $sizeMB MB"
    Write-Host "  Version: $Version (MSI: $MsiVersion)"
    Write-Host ""
    Write-Host "  Install (interactive):"
    Write-Host "    msiexec /i `"$MsiPath`" /l*v install.log"
    Write-Host ""
    Write-Host "  Install (silent):"
    Write-Host "    msiexec /i `"$MsiPath`" /qn /l*v silent.log"
    Write-Host ""
    Write-Host "  Uninstall:"
    Write-Host "    msiexec /x `"$MsiPath`" /l*v uninstall.log"
    Write-Host ""
    Write-Host "  Verify service:"
    Write-Host "    sc query FalconDB"
    Write-Host ""
    Write-Host "  Verify install:"
    Write-Host "    dir `"C:\Program Files\FalconDB`""
    Write-Host "    dir `"C:\ProgramData\FalconDB`""
    Write-Host "================================================================="
} else {
    Write-Host "  ERROR: MSI not found after build" -ForegroundColor Red
    Write-Host "  Check build output above for errors"
    exit 1
}
