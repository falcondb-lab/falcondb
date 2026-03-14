# ============================================================================
# FalconDB — Windows Developer Environment Setup
# ============================================================================
# Installs: Rust toolchain, protoc (for gRPC codegen), and verifies MSVC.
# Run as: .\scripts\setup_windows.ps1
#
# Note: OpenSSL is NOT required for M1/M2. When TLS support lands (M3+),
#       this script will be updated to install openssl via vcpkg or winget.
# ============================================================================
$ErrorActionPreference = "Stop"

function Info($msg)  { Write-Host "[setup] $msg" -ForegroundColor Cyan }
function Ok($msg)    { Write-Host "[   ok] $msg" -ForegroundColor Green }
function Warn($msg)  { Write-Host "[ warn] $msg" -ForegroundColor Yellow }

$missing = @()

# ── 1. MSVC / C++ build tools ──────────────────────────────────────────────
Info "Checking MSVC C++ build tools..."
$clExe = Get-Command cl.exe -ErrorAction SilentlyContinue
if ($clExe) {
    Ok "cl.exe found: $($clExe.Source)"
} else {
    # Check if VS Build Tools are installed but not in PATH
    $vsWhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path $vsWhere) {
        $vsPath = & $vsWhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath 2>$null
        if ($vsPath) {
            Ok "MSVC installed at $vsPath (run from Developer Command Prompt or use vsdevcmd.bat)"
        } else {
            $missing += "MSVC"
            Warn "Visual Studio found but C++ tools not installed"
        }
    } else {
        $missing += "MSVC"
        Warn "MSVC not found. Install via:"
        Write-Host "       winget install Microsoft.VisualStudio.2022.BuildTools --override '--add Microsoft.VisualStudio.Component.VC.Tools.x86.x64 --add Microsoft.VisualStudio.Component.Windows11SDK.22621 --passive'"
    }
}

# ── 2. Rust toolchain ──────────────────────────────────────────────────────
Info "Checking Rust toolchain..."
$rustc = Get-Command rustc -ErrorAction SilentlyContinue
if ($rustc) {
    $rustVer = & rustc --version
    Ok "rustc: $rustVer"
    $cargoVer = & cargo --version
    Ok "cargo: $cargoVer"
} else {
    $missing += "Rust"
    Warn "Rust not found. Installing via rustup..."
    try {
        Invoke-WebRequest -Uri "https://win.rustup.rs/x86_64" -OutFile "$env:TEMP\rustup-init.exe"
        & "$env:TEMP\rustup-init.exe" -y --default-toolchain stable
        $env:PATH = "$env:USERPROFILE\.cargo\bin;$env:PATH"
        Ok "Rust installed. Restart your terminal to pick up PATH changes."
    } catch {
        Warn "Failed to install Rust automatically. Visit https://rustup.rs"
    }
}

# ── 3. protoc (Protocol Buffers compiler) ──────────────────────────────────
Info "Checking protoc..."
$protoc = Get-Command protoc -ErrorAction SilentlyContinue
if ($protoc) {
    $protocVer = & protoc --version
    Ok "protoc: $protocVer"
} else {
    Info "protoc not in PATH — checking vendored binary..."
    # FalconDB uses protoc-bin-vendored crate, so system protoc is optional
    Ok "protoc not required (falcon_proto uses protoc-bin-vendored crate)"
    Info "To install system protoc anyway: winget install Google.Protobuf"
}

# ── 4. psql (PostgreSQL client for testing) ────────────────────────────────
Info "Checking psql..."
$psql = Get-Command psql -ErrorAction SilentlyContinue
if ($psql) {
    $psqlVer = & psql --version
    Ok "psql: $psqlVer"
} else {
    Warn "psql not found. Recommended for demo/testing."
    Write-Host "       winget install PostgreSQL.pgAdmin  # includes psql"
    Write-Host "       # or install PostgreSQL client tools from https://www.postgresql.org/download/windows/"
}

# ── 5. Git configuration ───────────────────────────────────────────────────
Info "Checking git line-ending config..."
$git = Get-Command git -ErrorAction SilentlyContinue
if ($git) {
    $autocrlf = & git config --get core.autocrlf 2>$null
    if ($autocrlf -eq "false") {
        Ok "core.autocrlf = false (correct — .gitattributes controls EOL)"
    } else {
        Warn "core.autocrlf = '$autocrlf' — setting to false for this repo"
        git config core.autocrlf false
        Ok "Set core.autocrlf = false"
    }
} else {
    Warn "git not found"
}

# ── 6. Build test ──────────────────────────────────────────────────────────
Info "Test build..."
cargo check --workspace 2>&1 | Select-Object -Last 3
if ($LASTEXITCODE -eq 0) {
    Ok "Workspace compiles successfully"
} else {
    Warn "Build failed — check errors above"
}

# ── Summary ─────────────────────────────────────────────────────────────────
Write-Host ""
if ($missing.Count -eq 0) {
    Write-Host "=== All prerequisites satisfied ===" -ForegroundColor Green
} else {
    Write-Host "=== Missing: $($missing -join ', ') ===" -ForegroundColor Yellow
}
Write-Host ""
Write-Host "Quick start:" -ForegroundColor White
Write-Host "  cargo run -p falcon_server -- --no-wal          # Start FalconDB"
Write-Host "  psql -h 127.0.0.1 -p 5433 -U falcon            # Connect"
Write-Host "  cargo run -p falcon_server -- --print-default-config  # Show config"
Write-Host ""
