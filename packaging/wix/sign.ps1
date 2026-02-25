# ============================================================================
# FalconDB — Authenticode + MSI Signing
# ============================================================================
#
# Signs falcon.exe and FalconDB MSI with Authenticode.
#
# Prerequisites:
#   - Windows SDK (signtool.exe in PATH or SDK installed)
#   - Code signing certificate (PFX or thumbprint in cert store)
#
# Usage (PFX):
#   .\packaging\wix\sign.ps1 -MsiPath dist\windows\FalconDB-1.0.0-x64.msi `
#       -PfxPath certs\codesign.pfx -PfxPassword $env:PFX_PASSWORD
#
# Usage (cert store thumbprint):
#   .\packaging\wix\sign.ps1 -MsiPath dist\windows\FalconDB-1.0.0-x64.msi `
#       -Thumbprint "AABBCCDD..."
#
# Usage (skip EXE signing, MSI only):
#   .\packaging\wix\sign.ps1 -MsiPath dist\windows\FalconDB-1.0.0-x64.msi `
#       -Thumbprint "AABBCCDD..." -MsiOnly
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$MsiPath,

    [string]$ExePath = "",

    [string]$PfxPath = "",
    [string]$PfxPassword = "",

    [string]$Thumbprint = "",

    [string]$TimestampServer = "http://timestamp.digicert.com",

    [switch]$MsiOnly
)

$ErrorActionPreference = "Stop"

Write-Host "================================================================="
Write-Host "  FalconDB — Code Signing"
Write-Host "================================================================="
Write-Host ""

# ── Find signtool.exe ──
$signtool = Get-Command signtool.exe -ErrorAction SilentlyContinue
if (-not $signtool) {
    # Try Windows SDK paths
    $sdkPaths = @(
        "${env:ProgramFiles(x86)}\Windows Kits\10\bin\*\x64\signtool.exe",
        "${env:ProgramFiles}\Windows Kits\10\bin\*\x64\signtool.exe"
    )
    foreach ($pattern in $sdkPaths) {
        $found = Get-Item $pattern -ErrorAction SilentlyContinue | Sort-Object FullName -Descending | Select-Object -First 1
        if ($found) {
            $signtool = $found
            break
        }
    }
}

if (-not $signtool) {
    Write-Host "ERROR: signtool.exe not found" -ForegroundColor Red
    Write-Host "  Install Windows SDK or add signtool.exe to PATH"
    exit 1
}
$signtoolPath = if ($signtool.Source) { $signtool.Source } else { $signtool.FullName }
Write-Host "  signtool: $signtoolPath"

# ── Validate inputs ──
if (-not (Test-Path $MsiPath)) {
    Write-Host "ERROR: MSI not found: $MsiPath" -ForegroundColor Red
    exit 1
}

if ([string]::IsNullOrWhiteSpace($PfxPath) -and [string]::IsNullOrWhiteSpace($Thumbprint)) {
    Write-Host "ERROR: Provide either -PfxPath or -Thumbprint" -ForegroundColor Red
    exit 1
}

# ── Build signtool arguments ──
function Get-SignArgs {
    param([string]$Target)
    $signArgs = @("sign")

    if (-not [string]::IsNullOrWhiteSpace($PfxPath)) {
        $signArgs += "/f"
        $signArgs += $PfxPath
        if (-not [string]::IsNullOrWhiteSpace($PfxPassword)) {
            $signArgs += "/p"
            $signArgs += $PfxPassword
        }
    } elseif (-not [string]::IsNullOrWhiteSpace($Thumbprint)) {
        $signArgs += "/sha1"
        $signArgs += $Thumbprint
    }

    # SHA-256 digest
    $signArgs += "/fd"
    $signArgs += "sha256"

    # Timestamp
    $signArgs += "/tr"
    $signArgs += $TimestampServer
    $signArgs += "/td"
    $signArgs += "sha256"

    # Description
    $signArgs += "/d"
    $signArgs += "FalconDB Enterprise Database Server"

    $signArgs += $Target
    return $signArgs
}

# ── Sign EXE ──
if (-not $MsiOnly) {
    if ([string]::IsNullOrWhiteSpace($ExePath)) {
        # Try to find falcon.exe in staging or Program Files
        $candidates = @(
            (Join-Path (Split-Path $MsiPath) "..\stage\bin\falcon.exe"),
            "C:\Program Files\FalconDB\bin\falcon.exe"
        )
        foreach ($c in $candidates) {
            if (Test-Path $c) { $ExePath = $c; break }
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($ExePath) -and (Test-Path $ExePath)) {
        Write-Host ""
        Write-Host "[1/2] Signing EXE: $ExePath"
        $exeArgs = Get-SignArgs -Target $ExePath
        & $signtoolPath $exeArgs
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  WARNING: EXE signing failed (continuing)" -ForegroundColor Yellow
        } else {
            Write-Host "  OK: EXE signed" -ForegroundColor Green
        }
    } else {
        Write-Host "[1/2] Skipping EXE signing (not found)"
    }
} else {
    Write-Host "[1/2] Skipping EXE signing (--MsiOnly)"
}

# ── Sign MSI ──
Write-Host ""
Write-Host "[2/2] Signing MSI: $MsiPath"
$msiArgs = Get-SignArgs -Target $MsiPath
& $signtoolPath $msiArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ERROR: MSI signing failed" -ForegroundColor Red
    exit 1
}
Write-Host "  OK: MSI signed" -ForegroundColor Green

# ── Verify ──
Write-Host ""
Write-Host "[VERIFY] Checking signature..."
& $signtoolPath verify /pa /v $MsiPath 2>&1 | Select-Object -First 20
if ($LASTEXITCODE -ne 0) {
    Write-Host "  WARNING: Signature verification returned non-zero (may need trusted root)" -ForegroundColor Yellow
} else {
    Write-Host "  OK: Signature verified" -ForegroundColor Green
}

Write-Host ""
Write-Host "================================================================="
Write-Host "  Signing complete"
Write-Host "  MSI: $MsiPath"
Write-Host "================================================================="
