---
name: Bug Report
about: Report a bug in FalconDB
title: "[BUG] "
labels: bug
assignees: ''
---

## Description

A clear, concise description of the bug.

## Steps to Reproduce

1. Start FalconDB with: `cargo run -p falcon_server -- <flags>`
2. Connect: `psql -h 127.0.0.1 -p 5433 -U falcon`
3. Run SQL: `...`
4. Observe error

## Expected Behavior

What should happen.

## Actual Behavior

What actually happens (include error messages, SQLSTATE codes, stack traces).

## Environment

- **OS**: (e.g., Ubuntu 22.04 / Windows 11 / macOS 14)
- **Rust version**: (`rustc --version`)
- **FalconDB version/commit**: (`git rev-parse --short HEAD`)
- **Node role**: (standalone / primary / replica)
- **WAL enabled**: (yes / no)

## Logs

```
Paste relevant log output here (RUST_LOG=debug cargo run ...)
```

## Additional Context

Any other context, screenshots, or config files.
