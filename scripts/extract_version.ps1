# ============================================================================
# FalconDB — Extract Version from Workspace Cargo.toml (Single Source of Truth)
# ============================================================================
#
# The ONLY authoritative version source is:
#   [workspace.package] version = "x.y.z" in the root Cargo.toml
#
# This script extracts it and optionally writes dist/VERSION.
#
# Usage:
#   .\scripts\extract_version.ps1              # print version
#   .\scripts\extract_version.ps1 -WriteFiles  # also update dist/VERSION
# ============================================================================

param(
    [switch]$WriteFiles
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$CargoToml = Join-Path $RepoRoot "Cargo.toml"

# ── Extract version from workspace Cargo.toml ──
$content = Get-Content $CargoToml -Raw
if ($content -match 'version\s*=\s*"([^"]+)"') {
    $Version = $Matches[1]
} else {
    Write-Host "ERROR: Could not extract version from $CargoToml" -ForegroundColor Red
    exit 1
}

# ── Derive MSI-safe version (x.y.z, no prerelease) ──
$MsiVersion = ($Version -replace '-.*', '')
$parts = $MsiVersion.Split('.')
while ($parts.Count -lt 3) { $parts += "0" }
$MsiVersion = ($parts[0..2]) -join '.'

# ── Output ──
Write-Host $Version

if ($WriteFiles) {
    # dist/VERSION
    $VersionFile = Join-Path $RepoRoot "dist\VERSION"
    @"
FalconDB v$Version
Build: windows-x86_64
"@ | Set-Content -Path $VersionFile -Encoding UTF8 -NoNewline
    # Append final newline
    Add-Content -Path $VersionFile -Value ""
    Write-Host "  Updated: dist/VERSION -> v$Version" -ForegroundColor Green
}
