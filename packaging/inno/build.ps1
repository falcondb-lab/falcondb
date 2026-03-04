#Requires -Version 5.1
# FalconDB — Build Inno Setup installer
#
# Usage:
#   .\packaging\inno\build.ps1 -Version 1.2.0
#   .\packaging\inno\build.ps1 -Version 1.2.0 -IsccPath "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
#   .\packaging\inno\build.ps1 -Version 1.2.0 -SkipStage

param(
    [Parameter(Mandatory=$true)]
    [string]$Version,

    [string]$IsccPath = "",

    [string]$BinDir = "",

    [switch]$SkipStage
)

$ErrorActionPreference = "Stop"
$RepoRoot  = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$ScriptDir = $PSScriptRoot
$IssFile   = Join-Path $ScriptDir "FalconDB.iss"
$StageDir  = Join-Path $RepoRoot  "dist\windows\stage"
$OutDir    = Join-Path $RepoRoot  "dist\windows"

Write-Host "================================================================="
Write-Host "  FalconDB — Inno Setup Build"
Write-Host "  Version : $Version"
Write-Host "================================================================="
Write-Host ""

# ── 1. Locate ISCC ────────────────────────────────────────────────────────────
if ([string]::IsNullOrWhiteSpace($IsccPath)) {
    $candidates = @(
        "F:\Inno Setup 6\ISCC.exe",
        "C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
        "C:\Program Files\Inno Setup 6\ISCC.exe",
        "C:\Program Files (x86)\Inno Setup 5\ISCC.exe"
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) { $IsccPath = $c; break }
    }
    if ([string]::IsNullOrWhiteSpace($IsccPath)) {
        $found = Get-Command "ISCC.exe" -ErrorAction SilentlyContinue
        if ($found) { $IsccPath = $found.Source }
    }
}
if ([string]::IsNullOrWhiteSpace($IsccPath) -or !(Test-Path $IsccPath)) {
    Write-Host "ERROR: ISCC.exe not found." -ForegroundColor Red
    Write-Host "  Install Inno Setup 6 from https://jrsoftware.org/isinfo.php"
    Write-Host "  or pass -IsccPath <path>"
    exit 1
}
Write-Host "[ISCC] $IsccPath"
Write-Host ""

# ── 2. Stage distribution files ──────────────────────────────────────────────
if (-not $SkipStage) {
    $StageScript = Join-Path $RepoRoot "scripts\stage_windows_dist.ps1"
    if (-not (Test-Path $StageScript)) {
        Write-Host "ERROR: stage script not found: $StageScript" -ForegroundColor Red
        exit 1
    }
    $stageArgs = @("-Version", $Version)
    if (-not [string]::IsNullOrWhiteSpace($BinDir)) {
        $stageArgs += @("-BinDir", $BinDir)
    }
    Write-Host "[1/2] Staging distribution files..."
    & $StageScript @stageArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: staging failed" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "[1/2] Skipping staging (-SkipStage)"
}
Write-Host ""

# ── 3. Compile installer ──────────────────────────────────────────────────────
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

Write-Host "[2/2] Compiling installer with ISCC..."
& $IsccPath `
    "/DAppVersion=$Version" `
    "/DStageDir=$StageDir" `
    $IssFile

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: ISCC compilation failed (exit $LASTEXITCODE)" -ForegroundColor Red
    exit 1
}

# ── 4. Verify output ──────────────────────────────────────────────────────────
$Expected = Join-Path $OutDir "FalconDB-Setup-$Version-x64.exe"
if (Test-Path $Expected) {
    $size = [math]::Round((Get-Item $Expected).Length / 1MB, 1)
    Write-Host ""
    Write-Host "================================================================="
    Write-Host "  OK  FalconDB-Setup-$Version-x64.exe  ($size MB)"
    Write-Host "  Path: $Expected"
    Write-Host ""
    Write-Host "  Run installer:"
    Write-Host "    $Expected"
    Write-Host "================================================================="
} else {
    Write-Host "WARNING: expected output not found at $Expected" -ForegroundColor Yellow
    Write-Host "  Check ISCC output above for actual filename."
}
