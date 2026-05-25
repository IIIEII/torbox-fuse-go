# torbox-fuse-go

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
| **Resilience** | Basic retry on API errors | Exponential backoff, caching, rate limiting |
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

```bash
go build -o torbox-media-center ./cmd/torbox-media-center
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
| `FUSE_ATTR_TIMEOUT_SEC` | `1` | Kernel attribute cache timeout (seconds) |
| `FUSE_ENTRY_TIMEOUT_SEC` | `1` | Kernel directory entry cache timeout (seconds) |
| `CATALOG_REFRESH_INTERVAL` | `3h` | How often to refresh the file catalog |
| `METRICS_LISTEN_ADDR` | `127.0.0.1:9080` | Address for the Prometheus metrics server |
| `STATE_DB_PATH` | `/config/state.db` | Path to the SQLite state database |
| `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, `error` |

## How it works

```
TorBox API → catalog (tree builder) → FUSE mount point
                                            ↓
                              player reads file → StreamReader
                                                    ↓
                                          CDN range requests (cached)
```

1. On startup, the catalog fetches your downloads from the TorBox API and builds a virtual directory tree
2. The FUSE mount presents this tree as a regular filesystem
3. When a player reads a file, the `StreamReader` fetches only the needed byte ranges from the TorBox CDN
4. A sharded range cache keeps hot data in memory, avoiding redundant downloads
5. The catalog refreshes periodically to pick up new downloads

## Building

```bash
go build -o torbox-media-center ./cmd/torbox-media-center
```

For a minimal Docker image, the included `Dockerfile` produces an Alpine-based container (~15 MB).

## Running tests

```bash
go test ./...
```

## Project structure

```
cmd/torbox-media-center/   Entrypoint
internal/
  cache/      Sharded range cache with budget eviction
  catalog/    TorBox API → virtual directory tree
  config/     Environment variable parsing
  fusefs/     FUSE filesystem (DirNode, FileNode, mount)
  metrics/    Prometheus metrics server
  state/      SQLite-backed persistent state
  stream/     CDN client + StreamReader with prefetch
  torbox/     TorBox API client with retry and rate limiting
```

## Monitoring

The metrics server exposes Prometheus metrics at `/metrics`. A pre-built Grafana dashboard is available at [`grafana/torbox-fuse-go.json`](grafana/torbox-fuse-go.json) — import it into Grafana to monitor cache hit ratio, streaming activity, CDN response codes, and more.

## License

[MIT](LICENSE)