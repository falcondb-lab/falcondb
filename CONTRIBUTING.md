# Contributing to FalconDB

Thank you for your interest in contributing to FalconDB! This document covers
the development workflow, code standards, and submission process.

---

## Prerequisites

- **Rust 1.75+** (`rustup update stable`)
- **C/C++ toolchain** (MSVC on Windows, gcc/clang on Linux/macOS)
- **psql** (PostgreSQL client, for integration testing)
- **git** with `core.autocrlf = false` (enforced by `.gitattributes`)

### Windows Quick Setup

```powershell
.\scripts\setup_windows.ps1
```

### Linux / macOS

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
sudo apt install postgresql-client  # Debian/Ubuntu
```

---

## Development Workflow

### 1. Fork & Clone

```bash
git clone https://github.com/<you>/falcondb.git
cd falcondb
git remote add upstream https://github.com/falcondb-lab/falcondb.git
```

### 2. Create a Branch

```bash
git checkout -b feat/my-feature   # or fix/my-bugfix
```

### 3. Code → Format → Lint → Test

```bash
# Format (must pass — CI enforced)
cargo fmt --all

# Lint (0 warnings required — CI enforced)
cargo clippy --workspace -- -D warnings

# Test (must pass — CI enforced)
cargo test --workspace

# Quick check (faster iteration)
cargo check --workspace

# Java JDBC driver (requires JDK 11+ and Maven)
cd clients/falcondb-jdbc
mvn clean test
```

### 4. Commit

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(storage): add column-level statistics collection
fix(protocol): handle empty COPY IN gracefully
docs(readme): update benchmark parameters
test(cluster): add promote-under-load scenario
refactor(executor): extract window function evaluation
```

### 5. Push & Open PR

```bash
git push origin feat/my-feature
```

Open a PR against `main`. Fill out the PR template (auto-populated).

---

## Code Standards

- **No `unsafe`** without a safety comment and team review.
- **No `unwrap()`** in library crates — use `?` or explicit error handling.
- **Comments**: document public APIs with `///` doc comments. Internal comments
  only when the "why" isn't obvious from the code.
- **Tests**: every bug fix must include a regression test. New features must
  include unit tests; integration tests preferred for SQL-level features.
- **Dependencies**: prefer existing workspace dependencies. New deps require
  justification in the PR description.

### File Organization

| Location | Purpose |
|----------|---------|
| `crates/<name>/src/lib.rs` | Crate root, re-exports |
| `crates/<name>/src/*.rs` | Implementation modules |
| `crates/falcon_server/tests/*.rs` | SQL integration tests |
| `crates/falcon_cluster/src/tests.rs` | Cluster/replication tests |
| `crates/falcon_protocol_native/` | Native binary protocol codec + compression |
| `crates/falcon_native_server/` | Native protocol server + session + executor bridge |
| `clients/falcondb-jdbc/` | Java JDBC driver (Maven project) |
| `tools/native-proto-spec/vectors/` | Golden test vectors for cross-language validation |
| `scripts/` | Demo, CI, and setup scripts |
| `docs/` | Architecture decisions, design docs |

---

## CI Pipeline

All PRs must pass these gates before merge:

| Job | Description | Blocking |
|-----|-------------|:--------:|
| **Check** | `cargo check --workspace` | ✅ |
| **Test** | `cargo test --workspace` | ✅ |
| **Clippy** | `cargo clippy -- -D warnings` | ✅ |
| **Format** | `cargo fmt --check` | ✅ |
| **Failover Gate (P0)** | Replication + promote + lifecycle tests | ✅ |
| **Failover Gate (P1)** | Checkpoint, fault injection, SHOW commands | ⚠️ Warning |
| **Windows** | Build + test on Windows | ✅ |
| **Native Protocol** | `cargo test -p falcon_protocol_native -p falcon_native_server` | ✅ |
| **JDBC Smoke** | `scripts/ci_native_jdbc_smoke.sh` (Rust + Java) | ✅ |

---

## Issue Guidelines

- **Bug reports**: include Rust version, OS, minimal repro SQL, and error message.
- **Feature requests**: describe the use case, proposed SQL syntax (if applicable),
  and expected behavior.
- **Questions**: check existing docs first; if unclear, open a Discussion.

---

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
