# torbox-fuse-go

[![CI](https://github.com/iiieii/torbox-fuse-go/actions/workflows/ci.yml/badge.svg)](https://github.com/iiieii/torbox-fuse-go/actions/workflows/ci.yml)
[![E2E](https://github.com/iiieii/torbox-fuse-go/actions/workflows/e2e.yml/badge.svg)](https://github.com/iiieii/torbox-fuse-go/actions/workflows/e2e.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Mount your [TorBox](https://torbox.app) cloud storage as a local FUSE filesystem. Stream media directly to Plex, VLC, Infuse, or any player that can read local files — no proxy, no intermediary server.

Inspired by [torbox-media-center](https://github.com/TorBox-App/torbox-media-center), the official Python implementation by the TorBox team.

## Why this version?

| | [Python (official)](https://github.com/TorBox-App/torbox-media-center) | torbox-fuse-go |
|---|---|---|
| **Language** | Python | Go |
| **Mount methods** | FUSE + STRM | FUSE only |
| **Metadata** | Built-in TorBox metadata search for organization | Extension-based classification (movies/series) |
| **Dependencies** | Python runtime + pip packages | Single static binary |
| **Resource usage** | Heavier (Python interpreter) | Lighter (compiled, no GC pauses) |
| **Streaming** | Proxy server for media | Direct CDN streaming with range cache |
| **Updates** | Scheduled full refreshes | Incremental catalog refreshes with SQLite state |
| **Resilience** | Basic retry on API errors | Exponential backoff, caching, rate limiting, priority semaphore |
| **Platform** | Linux/macOS (FUSE), any (STRM) | Linux/macOS (requires FUSE) |

**Use this version when:**
- You want a single static binary with no Python runtime
- You run on a resource-constrained server
- You don't need STRM files (you use Plex/VLC/Infuse, not Jellyfin/Emby)
- You prefer direct CDN streaming over a proxy

**Use the official Python version when:**
- You use Jellyfin or Emby (need STRM support)
- You want automatic metadata-based organization
- You're on Windows (STRM only)

## Prerequisites

- Linux with [FUSE3](https://github.com/libfuse/libfuse) or macOS with [macFUSE](https://macfuse.github.io/)
- A TorBox account on a paid plan ([sign up](https://torbox.app/subscription))

## Quick start

### Docker Compose (recommended)

```bash
cp .env.example .env   # edit TORBOX_API_KEY
docker compose up -d
```

Your files will appear at `/mnt/torbox`.

### From source

Requires a C compiler (for `go-sqlite3` via CGO).

```bash
make build
export TORBOX_API_KEY="your-api-key"
./torbox-media-center
```

## Configuration

All settings are environment variables:

| Variable | Default | Description |
|---|---|---|
| `TORBOX_API_KEY` | *(required)* | Your TorBox API key from [account settings](https://torbox.app/settings) |
| `TORBOX_API_BASE_URL` | `https://api.torbox.app/v1/api` | TorBox API base URL |
| `FUSE_MOUNT_PATH` | `/mnt/torbox` | Local mount point for the FUSE filesystem |
| `FUSE_CACHE_BUDGET_MB` | `256` | Maximum memory for the range cache (MB) |
| `FUSE_PREFETCH_WINDOW_MB` | `16` | Prefetch window size for sequential reads (MB) |
| `FUSE_STREAM_MAX_INFLIGHT` | `2` | Maximum concurrent stream requests per file |
| `FUSE_STREAM_CONCURRENCY` | `8` | Maximum concurrent CDN connections |
| `FUSE_STREAM_MAX_GLOBAL_WINDOWS` | `16` | Maximum total inflight windows across all files |
| `FUSE_CDN_URL_CACHE_TTL_SEC` | `300` | How long to cache CDN download URLs (seconds) |
| `FUSE_ATTR_TIMEOUT_SEC` | `1` | Kernel attribute cache timeout (seconds) |
| `FUSE_ENTRY_TIMEOUT_SEC` | `1` | Kernel directory entry cache timeout (seconds) |
| `CATALOG_REFRESH_INTERVAL` | `3h` | How often to refresh the file catalog |
| `METRICS_LISTEN_ADDR` | `127.0.0.1:9080` | Address for the metrics and control server |
| `DASHBOARD_ENABLED` | `true` | Enable the built-in web dashboard |
| `STATE_DB_PATH` | `/config/state.db` | Path to the SQLite state database |
| `FUSE_ALL_DIR_ENABLED` | `false` | Add a `/all` directory combining all movies and series |
| `FUSE_WRITABLE` | `false` | Enable write support: deleting files hides them locally (deletion from TorBox is always available via dashboard) |
| `DASHBOARD_USERNAME` | *(none)* | Basic Auth username for the web dashboard. If set with `DASHBOARD_PASSWORD`, all dashboard endpoints require authentication |
| `DASHBOARD_PASSWORD` | *(none)* | Basic Auth password for the web dashboard. If set with `DASHBOARD_USERNAME`, all dashboard endpoints require authentication |
| `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, `error` |

## How it works

```
TorBox API → catalog (tree builder) → FUSE mount point
                                            ↓
                              player reads file → StreamReader
                                                    ↓
                                          CDN range requests (cached)
```

1. On startup, the catalog immediately loads the file tree from the local SQLite cache, then refreshes from the TorBox API in the background
2. The FUSE mount presents this tree as a regular filesystem
3. When a player reads a file, the `StreamReader` fetches only the needed byte ranges from the TorBox CDN
4. A sharded range cache keeps hot data in memory, avoiding redundant downloads
5. A priority semaphore ensures playback streams aren't starved during library scans or large catalog refreshes
6. API requests always use `bypass_cache=true`, so you see fresh data from TorBox — never stale cached results
7. The catalog refreshes incrementally to pick up new downloads

## Webhook & on-demand refresh

By default the catalog refreshes on a timer (`CATALOG_REFRESH_INTERVAL`). All catalog refreshes query the TorBox API with `bypass_cache=true`, so the mount always reflects the latest state of your downloads — not a stale cached response from TorBox.

You can also trigger an immediate refresh manually:

```bash
curl -X POST http://localhost:9080/refresh
```

This is useful when you know a new download has completed and want the file to appear in the mount right away.

### TorBox webhook integration

TorBox can call this endpoint automatically when a download finishes. Setup requires two steps in your [TorBox account settings](https://torbox.app/settings):

1. **Integration settings** — on the [Integration settings](https://torbox.app/settings?section=integration-settings) page, set the webhook URL to:

   ```
   http://<your-host>:9080/refresh
   ```

2. **Notifications** — on the [Notifications](https://torbox.app/settings?section=notifications) page, enable webhook notifications so TorBox actually sends the POST request when downloads complete.

When a download completes, TorBox sends a POST request that triggers a catalog refresh — new files appear in the mount within seconds, without waiting for the next scheduled refresh.

> **Note:** If the metrics server is bound to `127.0.0.1` (default), you'll need to change `METRICS_LISTEN_ADDR` to `0.0.0.0:9080` so TorBox can reach it, or set up a reverse proxy.

## Dashboard authentication

When the dashboard is accessible over the network (e.g., running in Docker with a published port), you should protect it with a username and password. Set `DASHBOARD_USERNAME` and `DASHBOARD_PASSWORD` to enable HTTP Basic Auth for **all** dashboard endpoints (including the UI, API, and SSE stream).

When auth is configured:
- Opening the dashboard in a browser prompts for credentials
- API calls require an `Authorization: Basic <base64>` header
- The `/metrics` and `/healthz` endpoints remain **unauthenticated** (for Prometheus scraping)

```bash
# Example: Docker with auth
docker run -e TORBOX_API_KEY=... -e DASHBOARD_USERNAME=admin -e DASHBOARD_PASSWORD=secret ...
```

If `DASHBOARD_USERNAME` or `DASHBOARD_PASSWORD` is empty, no authentication is required.

## Read-write mode (file hiding)

By default the FUSE mount is read-only. Enable `FUSE_WRITABLE=true` to allow deleting files from the mount. This is useful for cleaning up downloads you no longer need.

**How it works:**

1. Deleting a file (or all files in a directory) from the mount **hides** it locally — the file disappears from the FUSE tree, but the download remains in TorBox.
2. Hidden files persist across catalog refreshes and restarts (stored in the SQLite `hidden_files` table).
3. When **all** files of a download are hidden, the download becomes eligible for force-deletion from TorBox via the dashboard.
4. The dashboard shows a "Hidden Downloads" section with two actions per download:
   - **Unhide** — restores all hidden files for that download (`POST /api/unhide`)
   - **Delete from TorBox** — permanently removes the download from TorBox (`POST /api/delete`)

**Example:**

```bash
# Hide a file by deleting it from the mount
rm /mnt/torbox/movies/movie.mkv

# View hidden downloads in the dashboard
curl http://localhost:9080/api/hidden

# Unhide a download (restore its files)
curl -X POST http://localhost:9080/api/unhide \
  -H 'Content-Type: application/json' \
  -d '{"download_kind":"torrent","download_id":"12345"}'

# Force-delete a download from TorBox
curl -X POST http://localhost:9080/api/delete \
  -H 'Content-Type: application/json' \
  -d '{"download_kind":"torrent","download_id":"12345"}'
```

> **Warning:** Deleting from TorBox is irreversible. Use the Unhide action first to double-check before force-deleting.

## Building

```bash
make build            # compile (requires CGO — gcc + SQLite dev headers)
```

Pre-built binaries are available on the [GitHub Releases](https://github.com/iiieii/torbox-fuse-go/releases) page. Docker images (multi-arch: amd64 + arm64) are published to [GHCR](https://github.com/iiieii/torbox-fuse-go/pkgs/container/torbox-fuse-go).

For a minimal Docker image, the included `Dockerfile` produces an Alpine-based container (~15 MB).

## Running tests

```bash
make test-short     # Unit tests only (no FUSE required)
make test           # All tests including FUSE e2e (requires FUSE driver)
make test-race       # Unit tests with race detector
make test-coverage  # Unit tests with coverage report
make lint           # golangci-lint
make vet            # go vet
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed development workflow.

## Project structure

```
cmd/torbox-media-center/   Entrypoint
internal/
  cache/      Sharded range cache with budget eviction
  catalog/    TorBox API → virtual directory tree
  config/     Environment variable parsing
  dashboard/   Web dashboard: cache visualization, hidden file management
  fusefs/     FUSE filesystem (DirNode, FileNode, mount, SyncTree, Unlink/Rmdir)
  integration/ Cross-package integration tests
  metrics/    Prometheus metrics server
  state/      SQLite-backed persistent state (file cache, hidden files)
  stream/     CDN client + StreamReader with prefetch & priority
  torbox/     TorBox API client with retry and rate limiting
tests/        End-to-end stress tests
```

## Monitoring

- **Prometheus** — the metrics server exposes `/metrics` at `METRICS_LISTEN_ADDR`. A pre-built Grafana dashboard is available at [`grafana/torbox-fuse-go.json`](grafana/torbox-fuse-go.json) for cache hit ratio, streaming activity, CDN response codes, and more.
- **Web dashboard** — a built-in live dashboard is served at the metrics address. It shows real-time file cache state, inflight windows, and reader activity.

## License

[MIT](LICENSE)