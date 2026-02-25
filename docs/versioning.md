# FalconDB — Version Management (Single Source of Truth)

## Authoritative Version Source

The **only** place where the FalconDB version is defined:

```toml
# Root Cargo.toml
[workspace.package]
version = "1.2.0"
```

**Every other location** derives from this single definition. No file, script,
badge, or binary may contain a hardcoded version that diverges.

## How Version Propagates

| Consumer | Mechanism | When |
|----------|-----------|------|
| All crate `Cargo.toml` | `version.workspace = true` | Compile time |
| `falcon --version` | `env!("CARGO_PKG_VERSION")` | Compile time (build.rs) |
| Startup log | `env!("CARGO_PKG_VERSION")` + git hash + build time | Compile time (build.rs) |
| `dist/VERSION` | `scripts/extract_version.ps1 -WriteFiles` | Build script |
| ZIP distribution | `build_windows_dist.ps1` reads Cargo.toml | Build script |
| MSI `ProductVersion` | `build_msi.ps1` / `packaging/wix/build.ps1` reads from binary | Build script |
| WiX `Variables.wxi` | `-d ProductVersion=x.y.z` passed by build.ps1 | Build script |
| README badge | Must be updated when version bumps | Manual (CI warns) |
| CHANGELOG.md | New `## [x.y.z]` section | Manual |
| CI consistency gate | `scripts/ci_version_check.sh` | Every PR |

## Build Metadata (build.rs)

`crates/falcon_server/build.rs` injects at compile time:

| Env Var | Value |
|---------|-------|
| `CARGO_PKG_VERSION` | From workspace `version` |
| `FALCONDB_GIT_HASH` | `git rev-parse --short=8 HEAD` |
| `FALCONDB_BUILD_TIME` | UTC timestamp |

## Version Bump Procedure

```bash
# 1. Edit the ONE source:
#    Cargo.toml → [workspace.package] version = "x.y.z"

# 2. Update dist/VERSION:
powershell -File scripts/extract_version.ps1 -WriteFiles

# 3. Update README badge (if major/minor change):
#    README.md → badge/version-x.y.z-blue

# 4. Add CHANGELOG.md entry

# 5. Commit + tag:
git add -A
git commit -m "release: vx.y.z"
git tag vx.y.z

# 6. CI will:
#    - Run ci_version_check.sh (consistency gate)
#    - Build ZIP + MSI with auto-extracted version
#    - Generate release notes
```

## CI Version Gate

`scripts/ci_version_check.sh` runs on every PR and verifies:

1. All crate Cargo.toml files use `version.workspace = true`
2. README badge matches workspace version
3. `dist/VERSION` matches workspace version
4. Binary `falcon --version` matches (if built)
5. No stale hardcoded version strings in `docs/`

## Release Workflow

Releases are triggered **only** by `git tag vX.Y.Z`:

```
git tag v1.2.0 → CI release job:
  1. Verify tag matches Cargo.toml version
  2. cargo build --release
  3. Build ZIP distribution
  4. Build MSI installer
  5. Generate release notes from CHANGELOG.md
  6. Upload artifacts to GitHub Releases
```

## Forbidden Patterns

- ❌ Hardcoded `"1.0.3"` anywhere except Cargo.toml `[workspace.package]`
- ❌ `version = "1.0.0"` in any crate Cargo.toml (must use `.workspace = true`)
- ❌ Manual edits to `dist/VERSION` (auto-generated)
- ❌ Release without matching `git tag vX.Y.Z`
