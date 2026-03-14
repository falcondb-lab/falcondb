## Summary

<!-- Brief description of what this PR does -->

## Changes

- 
- 

## Type

- [ ] Bug fix
- [ ] Feature
- [ ] Refactor
- [ ] Documentation
- [ ] CI / Infrastructure
- [ ] Performance

## Testing

<!-- How was this tested? Include commands. -->

```bash
cargo test -p <crate> -- <test_name>
```

- [ ] All existing tests pass (`cargo test --workspace`)
- [ ] New tests added for changed behavior
- [ ] `cargo clippy --workspace -- -D warnings` clean
- [ ] `cargo fmt --all -- --check` clean

## Risk Assessment

- **Scope**: (narrow / moderate / wide)
- **Breaking change**: (yes / no)
- **Rollback strategy**: (revert commit / feature flag / config toggle)

## Related Issues

<!-- Closes #123, Fixes #456 -->

## Checklist

- [ ] PR title follows [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] Documentation updated (if user-facing change)
- [ ] No `unwrap()` in library crates
- [ ] No new dependencies without justification
