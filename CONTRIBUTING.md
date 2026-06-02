# Contributing to torbox-fuse-go

Thanks for your interest! This guide covers everything you need to start contributing.

## Prerequisites

- **Go 1.26+** (the project uses CGO — `go-sqlite3` requires a C compiler)
- **FUSE** — Linux: `libfuse3-dev`; macOS: [macFUSE](https://macfuse.github.io/) or [FUSE-T](https://github.com/mhx/fuse-t)
- **golangci-lint** — for `make lint`

## Quick start

```bash
git clone https://github.com/iiieii/torbox-fuse-go.git
cd torbox-fuse-go
make build          # compile
make test-short     # unit tests (no FUSE needed)
make lint           # golangci-lint
```

## Development workflow

### 1. Fork & branch

```bash
git checkout -b feat/my-feature main
```

Branch naming conventions:

| Prefix    | Purpose                          |
|-----------|----------------------------------|
| `feat/`   | New feature                      |
| `fix/`    | Bug fix                          |
| `refactor/` | Code restructure (no behavior change) |
| `docs/`   | Documentation only               |
| `ci/`     | CI/CD changes                    |
| `chore/`  | Maintenance (deps, tooling)     |

### 2. Make changes

- Follow existing code style — match the surrounding code.
- Add tests for new behavior. Unit tests go alongside the code (`*_test.go`).
- E2E tests (tagged `//go:build !short`) need FUSE — run them with `make test`.

### 3. Verify locally

```bash
make vet          # go vet
make lint         # golangci-lint
make test-short   # unit tests (fast, no FUSE)
make tidy-check   # go.mod/go.sum are tidy
```

If you have FUSE available:

```bash
make test         # full test suite including e2e
make test-race    # unit tests with race detector
```

### 4. Commit

We use [Conventional Commits](https://www.conventionalcommits.org/) **without scopes**:

```
type: description

feat: add priority-aware CDN semaphore
fix: prevent zombie inflight windows on file close
docs: add configuration table to README
ci: add golangci-lint to PR checks
```

**Types:** `feat`, `fix`, `docs`, `refactor`, `test`, `ci`, `chore`, `perf`

### 5. Open a PR

Fill in the PR template (see `.github/PULL_REQUEST_TEMPLATE.md`). CI must pass:

- `lint` — golangci-lint
- `test-short` — unit tests
- `test-race` — race detector
- `vet` — go vet
- `tidy-check` — go.mod is tidy
- `build` — compiles
- `docker-validate` — Dockerfile builds

E2E tests run nightly and on manual dispatch — they don't block PRs.

## Versioning

This project follows [Semantic Versioning](https://semver.org/):

- **MAJOR** — incompatible API changes (breaking config, FUSE behavior, or CLI flags)
- **MINOR** — new features (new env vars, new metrics, new filesystem behavior)
- **PATCH** — bug fixes, performance improvements, dependency updates

Releases are triggered by pushing a `v*` tag (e.g., `v0.3.0`). GoReleaser builds binaries and Docker images automatically.

### Pre-release versions

- `v1.0.0-alpha.1` — early preview, anything may change
- `v1.0.0-rc.1` — release candidate, only bug fixes accepted

Until `v1.0.0`, the API (config flags, mount layout, metrics) may change between minor versions.

## Project structure

```
cmd/torbox-media-center/   Entrypoint
internal/
  cache/      Sharded range cache with budget eviction
  catalog/    TorBox API → virtual directory tree
  config/     Environment variable parsing
  dashboard/  Real-time web dashboard for cache/stream visualization
  fusefs/     FUSE filesystem (DirNode, FileNode, mount, SyncTree)
  integration/ Cross-package integration tests
  metrics/    Prometheus metrics server
  state/      SQLite-backed persistent state
  stream/     CDN client + StreamReader with prefetch & priority
  torbox/     TorBox API client with retry and rate limiting
tests/        End-to-end stress tests
```

## Coding guidelines

- **Error handling:** Don't silently discard errors. If `errcheck` flags something, either handle it or add a comment explaining why it's safe to ignore.
- **Tests:** Every bug fix should include a regression test. New features need at least one unit test.
- **Logging:** Use `slog` with structured fields — no `fmt.Println` in production code.
- **Dependencies:** Minimize new dependencies. If you need one, explain why in the PR.
- **CGO:** The project uses `go-sqlite3` (CGO). Always set `CGO_ENABLED=1` when building.

## Questions?

Open an [issue](https://github.com/iiieii/torbox-fuse-go/issues) — no question is too small.