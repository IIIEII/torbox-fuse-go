When context usage exceeds 70%, proactively run /compact.

For Go files:
- Do not use `cat -A` to diagnose indentation.
- Do not use `sed` for code edits.
- Use Edit first.
- If Edit fails once, use a small Python script with exact string replacement.
- After edits, run `gofmt -w`.