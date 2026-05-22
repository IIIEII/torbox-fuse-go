# TorBox Media Center Go — FUSE Filesystem Design

## 1. Purpose

A Go application that mounts TorBox cached files as a local FUSE filesystem for Plex/Jellyfin. It fetches file lists from the TorBox API, organizes them into movies/series folders based on tags and filenames, and streams file ranges on demand from TorBox CDN through a memory-efficient range cache.

This replaces the Python `torbox-media-center` implementation with a Go-native approach that is simpler, uses less CPU, and is gentler on TorBox API/CDN rate limits.

## 2. Constraints

- **FUSE only** — no .strm mode
- **No metadata search API** — classification uses tags (`type=movie`, `type=series`, `type=anime`) and filename heuristics only
- **Untagged files default to movie**
- **1 concurrent TorBox API call globally** — conservative rate limiting
- **No aggressive prefetch** — delayed next-window policy only
- **Docker deployment** — signal handling, structured logs, /config volume for state

## 3. Package structure

```
cmd/torbox-media-center/    — entrypoint, signal handling, wiring
internal/
  config/          — env var parsing, validation, defaults
  torbox/          — hand-written TorBox API client (rate-limited, cached)
  catalog/         — file list → virtual tree builder (tags, naming, sorting)
  fusefs/           — go-fuse/v2 filesystem (Inode tree, Read → stream)
  stream/           — CDN range reader, inflight windows, read-ahead
  cache/            — sharded range cache + sync.Pool buffers
  state/            — SQLite inode/persistence layer
  metrics/          — /metrics and /refresh webhook HTTP server
```

Dependencies:
- `github.com/hanwen/go-fuse/v2/fs` + `go-fuse/v2/fuse`
- `github.com/mattn/go-sqlite3` (CGO)
- `net/http`, `net/url` — CDN range streaming (stdlib only)
- Standard library for everything else — no SDK dependency

Rules:
- No generated code, no SDK import
- `internal/torbox` types don't leak — catalog uses app-owned structs
- `internal/stream` never calls TorBox API directly — gets URLs from catalog/fusefs

## 4. TorBox API client

### 4.1 Client

```go
type Client struct {
    apiKey     string
    baseURL    string
    httpClient *http.Client

    // Rate limiting: 1 concurrent API call globally
    apiSem   chan struct{} // capacity 1

    // Response cache: 5-min TTL, keyed by method+URL+params
    cache    map[string]cacheEntry
    cacheMu  sync.RWMutex
    cacheTTL time.Duration
}
```

### 4.2 API calls

- `GET /api/torrents/mylist` — paginated, `bypass_cache=true`
- `GET /api/usenet/mylist` — same
- `GET /api/webdl/mylist` — same
- `GET /api/torrents/requestdl?token=...&torrent_id=...&file_id=...&redirect=true` — torrent permalink resolution
- `GET /api/usenet/requestdl?token=...&usenet_id=...&file_id=...&redirect=true` — usenet permalink resolution
- `GET /api/webdl/requestdl?token=...&web_id=...&file_id=...&redirect=true` — webdl permalink resolution

### 4.3 Rate limiting and retry

- Global semaphore capacity 1 — all API calls acquire, release after response
- Response cache with 5-min TTL on GET calls
- Retry: exponential backoff with jitter on 429/5xx, max 3 retries
- On 429: respect `Retry-After` header if present
- No retry on 4xx client errors (except 429)
- Per-request context deadline: 30s for list calls, 60s for download link

### 4.4 CDN streaming separation

CDN streaming uses its own `http.Client` — no global semaphore, no response cache, different transport tuning. The API client is only for TorBox API metadata calls.

## 5. Catalog and virtual tree

### 5.1 Data model

```go
type DownloadKind string
const (
    KindTorrent DownloadKind = "torrent"
    KindUsenet   DownloadKind = "usenet"
    KindWebDL    DownloadKind = "webdl"
)

type MediaType string
const (
    MediaMovie  MediaType = "movie"
    MediaSeries MediaType = "series"
    MediaAnime  MediaType = "anime"
)

type Download struct {
    Kind  DownloadKind
    ID    string
    Name  string
    Hash  string
    Files []File
}

type File struct {
    DownloadKind DownloadKind
    DownloadID  string
    FileID      string
    Name         string    // full path from TorBox
    Size         int64
    MimeType     string
    MediaType    MediaType // from tags or heuristic
}
```

### 5.2 Media type classification

1. Check file/item tags for `type=movie`, `type=series`, `type=anime`
2. If no tag: parse filename — season/episode pattern (`S01E02`) → series, otherwise → movie
3. Anime goes into `/series/` (same treatment for Plex)

### 5.3 Virtual tree

```
/movies/
  <title>/
    <filename>
/series/
  <title>/
    Season <N>/
      <filename>
```

- `<title>`: download item name, or first path segment if item name is a hash
- `<filename>`: original short filename from TorBox
- Season number from filename parse, default Season 1

### 5.4 Catalog refresh

- Periodic: configurable interval, default 3 hours
- Webhook: `POST /refresh` on metrics port triggers immediate refresh
- If refresh is running: webhook returns 202 "already in progress"
- During refresh: current tree stays live, swap atomically when ready
- Only cached items included (`cached: true`)

### 5.5 Content key for inode mapping

```
<kind>:<download_id>:<file_id>
```

## 6. FUSE filesystem

Using `go-fuse/v2/fs` with `Inode`-based API.

### 6.1 Tree

- `RootInode` → `/movies/`, `/series/`
- `DirInode` for directories with stable inodes from SQLite
- `FileInode` for files with attrs from catalog + stable inodes from SQLite

### 6.2 File attributes

```go
func (f *FileInode) Getattr(ctx context.Context, fh FileHandle, out *fuse.AttrOut) error {
    out.Mode = 0444 | syscall.S_IFREG
    out.Size = uint64(f.file.Size)
    out.Uid = cfg.UID
    out.Gid = cfg.GID
    return nil
}
```

### 6.3 Read path

```go
func (f *FileInode) Read(ctx context.Context, fh FileHandle, off int64, data []byte) (int, syscall.Errno) {
    n, err := f.streamer.ReadAt(ctx, f.contentKey, off, data)
    if err != nil {
        return 0, syscall.EIO
    }
    return n, 0
}
```

`data` is the FUSE destination buffer — stream reads copy directly into it, zero alloc on cache hits.

### 6.4 Mount options

```go
&fs.Options{
    AttrTimeout:     &attrTimeout,     // 1s
    EntryTimeout:    &entryTimeout,    // 1s
    NegativeTimeout: &negTimeout,      // 0s
    MountOptions: fuse.MountOptions{
        AllowOther:   true,
        MaxReadAhead: 4 << 20, // 4 MiB
        FsName:       "torbox-media-center",
    },
}
```

### 6.5 Mutation handling

Return `EROFS` for all write operations (mkdir, create, unlink, rename, etc.)

## 7. Streaming hot path

### 7.1 Read flow

```
FUSE Read(off, dest[])
  → StreamReader.ReadAt(ctx, fileKey, off, dest)
      1. Try RangeCache.CopyTo(fileKey, off, dest) — zero-alloc cache hit
      2. If miss → find or create InflightWindow
      3. Join inflight window, wait until requested bytes are ready
      4. Copy ready bytes into dest, return immediately
      5. Trigger next-window read-ahead if conditions met
```

### 7.2 InflightWindow

```go
type InflightWindow struct {
    fileKey  string
    start    int64
    end      int64
    buf      []byte           // from sync.Pool, 16 MiB
    readyTo  atomic.Int64    // bytes available so far
    done     chan struct{}    // closed when HTTP request completes
    err      error           // set on failure
    cancel   context.CancelFunc
}
```

- Multiple FUSE reads hitting the same window range join the same inflight entry
- `readyTo` advances as chunks arrive — readers return as soon as their bytes are ready
- On completion: copy buf into RangeCache, return buf to pool

### 7.3 Next-window read-ahead (delayed policy)

Triggered only when ALL conditions are met:
- `maxServedOffset >= currentWindowStart + 4 MiB`
- Next window not already cached or inflight
- Per-file inflight count < 2
- Global HTTP semaphore available
- No recent far seek for this file

### 7.4 Seek cancellation

On a read starting > 16 MiB away from any inflight window for the same file:
- Increment file session ID
- Cancel stale inflight windows (ranges don't overlap new offset)
- Evict stale session cache data opportunistically

### 7.5 CDN URL strategy

Use permalinks by default:
```
https://api.torbox.app/v1/api/torrents/requestdl?token=<token>&torrent_id=<id>&file_id=<file_id>&redirect=true
```

CDN client follows redirects automatically. No CDN URL caching in phase 1. Add resolved URL caching only if redirect latency measurements show it matters.

### 7.6 CDN client transport

```go
&http.Transport{
    MaxIdleConns:        64,
    MaxIdleConnsPerHost: 16,
    MaxConnsPerHost:     16,
    IdleConnTimeout:     90 * time.Second,
    DisableCompression:  true,
}
// No Timeout on client — use per-request context deadlines
```

### 7.7 Stream loop error handling

- `206 Partial Content` → expected, stream body
- `200 OK` → accept only if offset is 0, otherwise error
- `416 Range Not Satisfiable` → refresh file metadata once, then fail
- `401/403` → refresh download URL once, then fail
- `429` → backoff with jitter, retry up to 3 times
- `5xx` → backoff with jitter, retry up to 3 times
- Any error: cancel inflight window, notify all waiters

## 8. Range cache and state

### 8.1 RangeCache — sharded for low contention

```go
type RangeCache struct {
    shards [32]rangeShard
    budget int64           // max total bytes
    used   atomic.Int64    // current total bytes
}

type rangeShard struct {
    mu     sync.RWMutex
    blocks map[cacheKey]*RangeBlock
}

type cacheKey struct {
    fileKey string
    start   int64
}

type RangeBlock struct {
    start      int64
    end        int64
    data       []byte
    sessionID  int64
    lastAccess atomic.Int64 // unix timestamp for LRU
}
```

Operations:
- `CopyTo(fileKey, off, dst) (int, bool)` — zero-alloc cache hit copy
- `Put(fileKey, start, data []byte)` — insert block, evict if budget exceeded
- `EvictStale(activeFile string, sessionID int64)` — remove old session blocks

Eviction: when `used > budget`, evict oldest-accessed blocks first across shards, then LRU within same file.

### 8.2 sync.Pool for window buffers

```go
var windowPool = sync.Pool{
    New: func() interface{} {
        buf := make([]byte, 0, prefetchWindowSize)
        return &buf
    },
}
```

16 MiB buffers pooled and reused. Never keep references after returning to pool.

### 8.3 SQLite state DB

```sql
CREATE TABLE IF NOT EXISTS inodes (
    content_key TEXT PRIMARY KEY,
    inode       INTEGER NOT NULL,
    path        TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    content_key   TEXT PRIMARY KEY,
    download_kind TEXT NOT NULL,
    download_id   TEXT NOT NULL,
    file_id       TEXT NOT NULL,
    path          TEXT NOT NULL,
    size          INTEGER NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_inode ON inodes(inode);
```

- On refresh: upsert current files, mark missing ones stale (don't delete immediately)
- Reuse existing inode for same content_key, allocate next integer for new keys
- Path stored for debugging; content_key is the authority

## 9. Metrics, logging, and webhook

### 9.1 HTTP endpoints (opt-in, default `127.0.0.1:9080`)

```
GET /metrics       — JSON metrics snapshot
GET /healthz       — liveness check
POST /refresh      — trigger catalog refresh (202 if already running)
```

### 9.2 Metrics

```go
type Metrics struct {
    CatalogItems         int64
    CacheBytesTotal      int64
    CacheBytesActive     int64
    CacheBytesStale      int64
    CacheEntries         int64
    InflightWindows      int64
    ReadCount           int64
    CacheHitCount       int64
    StreamMissCount     int64
    StreamJoinCount     int64
    CancelledStreamCount int64
    CDNStatusCodes      map[int]int64
    APICallCount        int64
    RefreshCount        int64
    GoroutineCount      int
}
```

First-byte latency histograms are deferred to a later phase.

### 9.3 Logging (stdlib `log/slog`)

- No permanent per-read INFO logs
- DEBUG: stream lifecycle events
- INFO: startup, config, catalog refresh summary, mount status, webhook triggers
- WARN: API failures, CDN errors, rate limits (429)
- ERROR: mount failures, unrecoverable state errors

### 9.4 Configuration (env vars)

```bash
TORBOX_API_KEY              # required
TORBOX_API_BASE_URL         # default: https://api.torbox.app
FUSE_MOUNT_PATH             # default: /mnt/torbox
FUSE_CACHE_BUDGET_MB        # default: 256
FUSE_PREFETCH_WINDOW_MB     # default: 16
FUSE_STREAM_MAX_INFLIGHT    # default: 2 (per file)
FUSE_STREAM_CONCURRENCY     # default: 8 (global CDN)
FUSE_ATTR_TIMEOUT_SEC       # default: 1
FUSE_ENTRY_TIMEOUT_SEC      # default: 1
CATALOG_REFRESH_INTERVAL    # default: 3h
METRICS_LISTEN_ADDR         # default: 127.0.0.1:9080
STATE_DB_PATH               # default: /config/state.db
LOG_LEVEL                   # default: info
```

## 10. Implementation phases

### Phase 1: Config + TorBox client + catalog

- Env var parsing and validation
- Hand-written TorBox API client with rate limiting and caching
- Paginated list fetching for torrents, usenet, webdl
- Tag parsing and media type classification
- Virtual tree builder
- SQLite state DB for inodes
- Unit tests for client, classification, tree building

**Acceptance:** Given a TorBox API key, the app can fetch all cached files and print the virtual tree with stable inodes.

### Phase 2: Basic FUSE filesystem

- go-fuse mount with DirInode/FileInode tree
- Stable inode assignment from SQLite
- Directory listing, file attrs, read-only enforcement
- Atomic tree swap on catalog refresh
- Integration test: mount and ls/stat files

**Acceptance:** Plex/Jellyfin can scan the mounted tree. Restart does not churn inodes for unchanged files.

### Phase 3: Streaming hot path

- CDN range client
- Sharded range cache with CopyTo
- InflightWindow with early return
- Delayed next-window read-ahead
- Seek cancellation
- Global concurrency semaphore

**Acceptance:** Existing Plex playback scenario works. First real post-seek bytes available in under 1s. CPU materially lower than Python.

### Phase 4: Observability and polish

- Metrics HTTP endpoint (/metrics, /healthz, /refresh)
- Structured slog logging
- Docker image
- Graceful shutdown (unmount, close DB)
- Configuration documentation

### Phase 5: Optional future

- Resolved CDN URL caching (if redirect latency measurements warrant it)
- First-byte latency histograms in metrics
- Disk warmup cache for file heads/tails