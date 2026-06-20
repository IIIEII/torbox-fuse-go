When context usage exceeds 70%, proactively run /compact.

For Go files:
- Do not use `cat -A` to diagnose indentation.
- Do not use `sed` for code edits.
- Use Edit first.
- If Edit fails once, use a small Python script with exact string replacement.
- After edits, run `gofmt -w`.

## Development workflow

1. **Branch**: Pull fresh `main`, create a feature branch.
2. **Code**: Commit changes to the branch.
3. **Tests**: Check test coverage — if changes need new tests, add them. Run full test suite.
4. **Docs**: Check project docs (README, CHANGELOG, etc.) — update if needed.
5. **PR**: Push branch, create PR to `main`, wait for CI.
6. **Fix CI**: If CI fails, fix on the branch and push again.
7. **Merge**: Once CI is green, squash-merge into `main`.
8. **Release**: Pull merged `main`, tag with new version, push tag, monitor build.