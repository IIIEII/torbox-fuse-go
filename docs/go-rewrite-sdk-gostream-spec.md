# TorBox Media Center Go Rewrite Specification: SDK + GoStream-Informed Design

## 1. Purpose

This document is a companion to `docs/go-rewrite-spec.md`.

The first specification describes the desired Go rewrite from the current Python application and the successful FUSE streaming experiments. This second specification adds findings from two upstream Go projects:

- official TorBox Go SDK: `https://github.com/TorBox-App/torbox-sdk-go`
- GoStream: `https://github.com/MrRobotoGit/gostream`

The goal is not to clone GoStream or blindly depend on every SDK abstraction. The goal is to use the official SDK for stable TorBox API coverage, and borrow proven Go/FUSE streaming ideas from GoStream where they match TorBox Media Center's simpler architecture.

## 2. Summary of findings

### 2.1 TorBox Go SDK

The SDK is an OpenAPI-style generated client with service packages for:

- `torrents`
- `usenet`
- `webdownloadsdebrid`
- `user`
- `queued`
- `general`
- `notifications`
- `rssfeeds`
- `integrations`

Important SDK traits:

- `torboxapi.NewTorboxApi(config)` creates a service client with typed service fields.
- `SetAccessToken`, `SetBaseUrl`, and `SetTimeout` are available on the client.
- API calls accept `context.Context`.
- Responses carry both typed data and HTTP metadata.
- Errors carry body, headers, and status code.
- `torrents.RequestDownloadLink` exists and maps to `/api/torrents/requestdl`.
- `torrents.GetTorrentList` exists and maps to `/api/torrents/mylist`.
- equivalent list/request-download APIs exist for usenet and web downloads.

Most useful SDK features:

- typed models for TorBox list responses;
- typed request params for download-link generation;
- context-aware API calls;
- shared handling of access token, base URL, and timeout;
- structured API errors with status code and headers.

SDK caveats:

- It is generated and verbose; do not expose generated types throughout the entire app.
- Numeric IDs/sizes may be represented as float-like generated fields in some response structs; normalize at the boundary.
- It covers TorBox API calls, not CDN byte streaming. Range streaming still needs a separate optimized `net/http` client.
- The SDK `requestdl` method can return a CDN URL, but TorBox documents that stable permalinks with `redirect=true` avoid repeatedly generating/storing CDN links. The rewrite should support both patterns.

### 2.2 GoStream

GoStream is a much larger torrent/FUSE media system. It includes a BitTorrent engine, native stream reader, Plex/Jellyfin integration, warmup SSD cache, inodes, FUSE mount tuning, metrics, scheduler, and many torrent-specific features.

Most useful GoStream ideas for TorBox Media Center:

- use `github.com/hanwen/go-fuse/v2` instead of high-level Python FUSE;
- keep the read hot path allocation-light;
- copy cached bytes directly into the FUSE destination buffer;
- use chunk/window caches with sharding to reduce lock contention;
- use stable inode mapping for Plex stability across restarts;
- use directory-entry caching with TTL;
- configure FUSE mount options explicitly;
- separate API metadata/cache state from streaming hot path;
- use global semaphores/budgets for concurrent data fetches;
- deduplicate inflight prefetches by path/window key;
- use session/path-aware cache eviction so stale seek data is dropped quickly;
- expose metrics for read-ahead cache, active bytes, stale bytes, requests, errors, and latency.

GoStream features not appropriate for direct reuse:

- embedded torrent engine / GoStorm native bridge;
- torrent peer-speed adaptive preload;
- aggressive mode / torrent-specific concurrency control;
- Prowlarr/Torrentio/Plex watchlist scheduler;
- NAT-PMP and torrent connectivity logic;
- large SSD warmup cache as a default behavior.

Possible optional future borrow:

- a small disk warmup cache for file heads/tails, inspired by GoStream, but only as an opt-in feature after the memory-only range cache is stable.

## 3. Architectural decision

Use a layered Go design:

```text
cmd/torbox-media-center
  -> internal/config
  -> internal/torbox     official SDK wrapper + normalization
  -> internal/catalog    media/file tree model
  -> internal/strm       .strm writer
  -> internal/fusefs     go-fuse filesystem
  -> internal/stream     CDN HTTP range streaming pipeline
  -> internal/cache      memory range cache + metadata caches
  -> internal/state      stable inode/state DB
  -> internal/metrics    health and performance endpoint
```

Rule: generated SDK models must stop at `internal/torbox`. Downstream packages use app-owned structs.

## 4. TorBox SDK integration

### 4.1 Wrapper interface

Create an app-owned client interface:

```go
type Client interface {
    ListDownloads(ctx context.Context, kind DownloadKind, opts ListOptions) ([]Download, error)
    RequestDownloadURL(ctx context.Context, ref FileRef, opts DownloadURLOptions) (string, error)
    Permalink(ref FileRef, redirect bool) string
    CheckCached(ctx context.Context, hashes []string) (CachedAvailability, error)
    User(ctx context.Context) (UserInfo, error)
    Health(ctx context.Context) error
}
```

`DownloadKind` should support at least:

- torrent;
- usenet;
- web download.

`Download` should be normalized:

```go
type Download struct {
    Kind       DownloadKind
    ID         string
    Name       string
    Hash       string
    State      string
    IsCached   bool
    Files      []File
    UpdatedAt  time.Time
    ExpiresAt  *time.Time
}

type File struct {
    DownloadID string
    FileID     string
    Path       string
    Name       string
    Size       int64
    MediaType  MediaType
}
```

### 4.2 SDK usage policy

Use SDK for:

- `mylist` / list calls;
- request-download URL calls;
- cached availability when needed;
- user/account/status calls;
- API error metadata.

Do not use SDK for:

- FUSE reads;
- CDN range requests;
- redirect following during streaming;
- hot-path retries.

The streaming layer needs a dedicated `http.Client` with transport tuned for CDN reads.

### 4.3 Download URL strategy

Support two URL modes:

1. **permalink mode** — preferred default for `.strm` and acceptable for FUSE:

```text
https://api.torbox.app/v1/api/torrents/requestdl?token=<token>&torrent_id=<id>&file_id=<file_id>&redirect=true
```

2. **resolved CDN mode** — optional for FUSE when measuring redirect overhead:

- call SDK `RequestDownloadLink`;
- cache returned CDN URL with conservative TTL;
- invalidate on 401/403/404/416/5xx patterns as appropriate;
- never persist CDN URLs as durable state.

For `.strm`, prefer permalinks to avoid stale CDN URLs.

For FUSE, start with permalinks plus an HTTP client that follows redirects. Add resolved URL caching only if measurements show redirects materially hurt first-read latency.

## 5. GoStream-inspired FUSE design

### 5.1 Library

Use `github.com/hanwen/go-fuse/v2/fs`.

Rationale:

- mature low-level Go FUSE library;
- avoids Python/FUSE overhead;
- allows explicit mount options;
- supports stable inode behavior and direct buffer-oriented read implementation.

### 5.2 Mount options

Initial mount options:

```go
fs.Mount(mountPath, root, &fs.Options{
    AttrTimeout:     &attrTimeout,
    EntryTimeout:    &entryTimeout,
    NegativeTimeout: &negativeTimeout,
    MountOptions: fuse.MountOptions{
        AllowOther:                true,
        MaxBackground:             cfg.FUSEMaxBackground,
        MaxReadAhead:              cfg.FUSEKernelReadAheadBytes,
        RememberInodes:            true,
        ExplicitDataCacheControl:  true,
        SyncRead:                  false,
        FsName:                    "torbox-media-center",
    },
    UID: cfg.UID,
    GID: cfg.GID,
})
```

Defaults:

- attr timeout: `1s`;
- entry timeout: `1s`;
- negative timeout: `0s`;
- `MaxReadAhead`: `4 MiB`;
- `MaxBackground`: configurable, default `32`.

### 5.3 Stable inode map

Borrow the GoStream idea, but simplify it.

Use a persistent SQLite state DB, not JSON by default:

```text
state.db
  inodes(content_key TEXT PRIMARY KEY, inode INTEGER, path TEXT, updated_at TEXT)
  files(content_key TEXT PRIMARY KEY, download_kind TEXT, download_id TEXT, file_id TEXT, path TEXT, size INTEGER, updated_at TEXT)
```

Content key:

```text
<kind>:<download_id>:<file_id>
```

Directory inode key:

```text
dir:<virtual_path>
```

Requirements:

- Plex should not rediscover unchanged media after restart.
- Renames due to metadata changes should be deliberate and visible, not accidental inode churn.
- If a TorBox item disappears, mark stale first, then prune after catalog refresh confirms it.

### 5.4 Directory cache

Use a TTL directory cache similar to GoStream's `DirCache`:

```go
type DirCache struct {
    mu    sync.RWMutex
    ttl   time.Duration
    items map[string]DirCacheEntry
}
```

Use it for `Readdir` only. Invalidate when catalog refresh changes the tree.

## 6. Streaming design

The current successful Python experiment should remain the behavioral baseline:

- one unified streaming-window path;
- no separate tiny foreground segment path;
- early return as soon as requested bytes are available;
- cautious delayed next-window read-ahead;
- seek cancellation for stale windows;
- dedupe inflight windows;
- avoid aggressive speculative prefetch.

### 6.1 Components

```text
fusefs.MkvHandle.Read
  -> stream.Reader.ReadAt(ctx, file, off, dest)
      -> range cache CopyTo
      -> inflight stream join or start
      -> HTTP range request into window buffer
      -> serve requested bytes as soon as present
      -> optional delayed next-window trigger
```

### 6.2 Window sizing

Keep current tested values as first-class defaults for Plex validation:

```env
FUSE_FOREGROUND_SEGMENT_KB=2048
FUSE_PREFETCH_WINDOW_MB=16
FUSE_PREFETCH_WAIT_MS=0
```

Interpretation in Go:

- `FUSE_FOREGROUND_SEGMENT_KB`: preferred minimum served slice/alignment, not a separate network fetch path;
- `FUSE_PREFETCH_WINDOW_MB`: range stream window size;
- `FUSE_PREFETCH_WAIT_MS`: compatibility knob, default `0`.

### 6.3 Range cache

Borrow from GoStream's sharded cache, but adapt to TorBox CDN windows.

```go
type RangeCache struct {
    shards [32]*rangeShard
    used   atomic.Int64
    budget int64
}

type RangeBlock struct {
    fileKey     string
    start       int64
    end         int64
    data        []byte
    lastAccess  atomic.Int64
    sessionID   int64
}
```

Required operations:

- `CopyTo(fileKey string, off int64, dst []byte) int`
- `Put(fileKey string, start int64, data []byte)`
- `Exists(fileKey string, start int64) bool`
- `MaxCachedOffset(fileKey string) int64`
- `SwitchActiveFile(fileKey string)`
- `EvictStale(activeFile string, sessionID int64)`

Hot-path rule:

- prefer `CopyTo` into the provided FUSE destination buffer;
- avoid allocating on cache hits;
- use `sync.Pool` for 16 MiB window buffers;
- never keep references to buffers returned to the pool.

### 6.4 Inflight stream table

Use per-file/window keys:

```text
<fileKey>:<windowStart>
```

Each inflight stream has:

```go
type InflightWindow struct {
    ctx       context.Context
    cancel    context.CancelFunc
    start     int64
    end       int64
    readyFrom int64
    readyTo   atomic.Int64
    done      chan struct{}
    err       atomic.Value
    waiters   atomic.Int32
}
```

Behavior:

- A read that falls inside an inflight window joins it.
- The HTTP goroutine copies chunks into the window buffer and advances `readyTo`.
- The waiting FUSE read returns when requested bytes are ready, not when the full window completes.
- On completion, the window is inserted into `RangeCache`.
- On seek, stale inflight windows for the same file are cancelled if far from the new offset.

### 6.5 Delayed next-window read-ahead

Keep the proven delayed policy from the Python branch.

Do not start the next 16 MiB window immediately on every miss. Start it only when playback has consumed enough of the current window.

Initial rule:

```text
start next window when maxServedOffset >= currentWindowStart + 4 MiB
```

Also require:

- next window not already cached;
- next window not already inflight;
- per-file inflight count below `2`;
- global HTTP semaphore available;
- no recent far seek for that file.

This is intentionally more conservative than GoStream's aggressive torrent pump.

### 6.6 Seek cancellation

On a far seek:

- increment file session ID;
- cancel stale inflight windows whose ranges do not overlap the new target area;
- evict stale session cache data opportunistically;
- do not cancel active tail/head probes unless they overlap current request semantics.

Initial far-seek threshold:

```text
max(FUSE_PREFETCH_WINDOW_MB, 16 MiB)
```

### 6.7 HTTP CDN client

Use separate clients:

- SDK client for TorBox API;
- CDN client for range reads.

CDN transport defaults:

```go
Transport: &http.Transport{
    MaxIdleConns:        128,
    MaxIdleConnsPerHost: 32,
    MaxConnsPerHost:     16,
    IdleConnTimeout:     90 * time.Second,
    DisableCompression:  true,
}
Timeout: 0 // use per-request context deadlines instead
```

Range request headers:

```text
Range: bytes=<start>-<end>
User-Agent: torbox-media-center-go/<version>
```

The stream loop must handle:

- `206 Partial Content` expected;
- `200 OK` fallback only if server ignored range and start is zero;
- `302/303/307/308` redirects via standard client policy;
- `416` as range/end mismatch, refresh metadata once;
- `401/403` as URL/token problem, refresh resolved URL or fail clearly;
- retryable `429/5xx` with bounded backoff and context cancellation.

## 7. Optional disk warmup cache

Do not implement this in phase 1.

GoStream's SSD warmup cache is useful for torrent startup and repeated Plex probes, but TorBox Media Center's primary bottleneck is remote CDN range latency and Python CPU overhead.

Optional future phase:

- cache first `64 MiB` per active file;
- cache last `16 MiB` for metadata/tail probes;
- strict global disk quota;
- opt-in env var, default off;
- never persist full media files.

Proposed env vars:

```env
FUSE_DISK_WARMUP=false
FUSE_DISK_WARMUP_PATH=/cache/torbox-media-center
FUSE_DISK_WARMUP_QUOTA_GB=15
FUSE_DISK_WARMUP_HEAD_MB=64
FUSE_DISK_WARMUP_TAIL_MB=16
```

## 8. Metrics and observability

Borrow GoStream's idea of a lightweight metrics endpoint.

Expose `/metrics.json` or `/healthz` on an optional local HTTP port.

Minimum metrics:

- catalog item count;
- mounted state;
- range cache bytes total;
- range cache active bytes;
- range cache stale bytes;
- range cache entries;
- inflight windows count;
- current active file key/path;
- read count;
- cache hit count;
- stream miss count;
- stream join count;
- cancelled stream count;
- CDN status code counters;
- request-download URL refresh count;
- p50/p95 first-byte latency for FUSE reads;
- p50/p95 full-window download latency;
- goroutine count;
- process RSS if available.

Logging policy:

- no permanent per-read INFO logs;
- DEBUG logs may include stream lifecycle events;
- INFO logs only for startup/config, catalog refresh summary, mount status, and major state transitions;
- WARN/ERROR for TorBox/API/CDN failures.

## 9. Config additions

Keep compatibility with the existing app's env vars. Add Go-specific optional env vars:

```env
TORBOX_API_BASE_URL=https://api.torbox.app
TORBOX_API_TIMEOUT_SECONDS=10
TORBOX_DOWNLOAD_URL_MODE=permalink # permalink|resolved
FUSE_CACHE_BUDGET_MB=256
FUSE_STREAM_MAX_INFLIGHT_PER_FILE=2
FUSE_STREAM_GLOBAL_CONCURRENCY=8
FUSE_KERNEL_READAHEAD_MB=4
FUSE_MAX_BACKGROUND=32
FUSE_ATTR_TIMEOUT_SECONDS=1
FUSE_ENTRY_TIMEOUT_SECONDS=1
FUSE_NEGATIVE_TIMEOUT_SECONDS=0
METRICS_LISTEN_ADDR=127.0.0.1:9080
STATE_DB_PATH=/config/state.db
```

## 10. Implementation phases

### Phase 0: dependency spike

- Create a minimal Go module.
- Import `torbox-sdk-go` and `go-fuse/v2`.
- Confirm SDK module path works cleanly from GitHub.
- If SDK module path is not import-friendly, use a pinned replace/module strategy or isolate it behind `internal/torbox` so it can be swapped.

### Phase 1: SDK wrapper and catalog

- Implement config parsing.
- Implement `internal/torbox` wrapper.
- Normalize torrent/usenet/web-download lists into app structs.
- Implement video filtering.
- Implement raw virtual tree.
- Implement `.strm` mode with permalink URLs.

Acceptance:

- `.strm` output matches current Python behavior for raw mode.
- No FUSE required yet.

### Phase 2: basic FUSE filesystem

- Implement read-only tree using `go-fuse/v2`.
- Implement stable inode state DB.
- Implement directory cache.
- Implement file attrs and sizes from catalog.
- Return clear errors for unsupported mutation.

Acceptance:

- Plex/Jellyfin can scan mounted tree.
- Restart does not churn inodes for unchanged files.

### Phase 3: streaming hot path

- Implement CDN range client.
- Implement sharded range cache.
- Implement inflight window join.
- Implement early return before full window completion.
- Implement delayed next-window read-ahead.
- Implement seek cancellation.

Acceptance:

- Existing successful Plex test scenario works with:

```env
FUSE_FOREGROUND_SEGMENT_KB=2048
FUSE_PREFETCH_WINDOW_MB=16
FUSE_PREFETCH_WAIT_MS=0
```

- first real post-seek bytes are normally available in under `1s` on the same network conditions as the Python test;
- CPU is materially lower than Python implementation during steady playback.

### Phase 4: observability and tuning

- Add metrics endpoint.
- Add pprof behind explicit opt-in.
- Add structured debug logs.
- Tune cache budget, global concurrency, and mount options.

### Phase 5: optional disk warmup

Only after phase 3/4 are stable:

- implement opt-in head/tail disk warmup;
- enforce strict quota;
- verify it does not waste TorBox/CDN bandwidth.

## 11. Reuse / borrow matrix

| Source | Item | Use directly? | Decision |
|---|---:|---:|---|
| TorBox SDK | API service client | Yes | Use behind `internal/torbox` wrapper. |
| TorBox SDK | Typed list models | Partial | Normalize immediately into app structs. |
| TorBox SDK | Request download link | Yes | Use for optional resolved CDN mode. |
| TorBox SDK | API error metadata | Yes | Preserve status/body for logs and retry decisions. |
| TorBox SDK | CDN streaming | No | Use custom `net/http` range client. |
| GoStream | `go-fuse/v2` approach | Yes | Use same library class of solution. |
| GoStream | mount option philosophy | Yes | Borrow explicit timeouts, readahead, inode settings. |
| GoStream | sharded cache idea | Yes | Adapt for TorBox range windows. |
| GoStream | direct `CopyTo` cache hits | Yes | Important CPU reduction target. |
| GoStream | stable inode map | Yes | Simplify and store in SQLite. |
| GoStream | directory TTL cache | Yes | Simple and useful. |
| GoStream | inflight prefetch dedupe | Yes | Already proven necessary in Python branch. |
| GoStream | session-aware stale eviction | Yes | Adapt for seek-heavy Plex behavior. |
| GoStream | native torrent pump | No | TorBox CDN does not need torrent engine. |
| GoStream | peer-speed preload strategy | No | Not relevant to TorBox CDN. |
| GoStream | SSD warmup cache | Later | Opt-in future feature, not phase 1. |
| GoStream | Plex/watchlist/search ecosystem | No | Out of scope for TorBox Media Center. |

## 12. Risks

1. **SDK import stability**
   - Mitigation: isolate SDK behind wrapper; pin version/commit.

2. **Generated model awkwardness**
   - Mitigation: normalize at boundary; do not leak SDK structs.

3. **Redirect latency on permalinks**
   - Mitigation: measure first; add resolved CDN URL cache only if needed.

4. **Over-prefetch wasting CDN bandwidth**
   - Mitigation: keep delayed next-window policy and max 2 inflight windows per file.

5. **Plex inode churn**
   - Mitigation: persistent inode DB keyed by TorBox file identity.

6. **Range cache memory growth**
   - Mitigation: sharded budgeted cache, session-aware eviction, metrics.

7. **Kernel/FUSE tuning regressions**
   - Mitigation: expose mount options but keep conservative defaults.

## 13. Acceptance criteria

The SDK + GoStream-informed rewrite is acceptable when:

- `.strm` mode preserves current behavior;
- FUSE mode exposes stable read-only files with correct sizes;
- Plex playback starts and seeks without multi-second stalls under the same TorBox conditions as the current branch;
- first requested bytes can return before a full 16 MiB window downloads;
- rapid seeks cancel stale windows and do not keep downloading abandoned ranges;
- steady playback CPU is clearly below the Python implementation;
- TorBox API calls go through the SDK wrapper;
- CDN streaming goes through the custom range client;
- no aggressive speculative prefetch is enabled by default;
- metrics make cache/inflight/latency behavior inspectable.

## 14. Final recommendation

Use the official TorBox SDK for API correctness and future endpoint coverage, but keep it behind a narrow internal wrapper.

Use GoStream as an architecture reference for the FUSE and cache hot path, especially:

- `go-fuse/v2`;
- explicit mount tuning;
- stable inodes;
- sharded caches;
- direct cache-to-destination copies;
- inflight dedupe;
- metrics.

Do not import GoStream code wholesale. Its strongest ideas are valuable, but its torrent-engine assumptions do not match TorBox's CDN-backed model. The correct rewrite is a smaller TorBox-native service with GoStream-grade FUSE discipline and the current branch's conservative streaming-window behavior.
