# Plex-like read path: test coverage specification

## Purpose

Define mandatory and recommended test coverage for an application that serves media files under a Plex-like access pattern.

Primary audit target:
- read path correctness;
- range/cache/inflight coordination;
- seek handling;
- prefetch control;
- stability under real playback-like behavior.

## Audit output format

For each scenario, mark exactly one status:
- `covered-well`
- `covered-partially`
- `not-covered`
- `not-applicable`

For each `covered-partially` and `not-covered`, provide:
- current evidence;
- gap;
- risk.

## Mandatory coverage

### 1. Unit: range logic

Required scenarios:
- exact range match;
- full cover;
- partial overlap left;
- partial overlap right;
- no overlap;
- adjacent ranges;
- request clipped by EOF;
- request exactly at EOF;
- zero-length range;
- invalid range.

### 2. Unit: cache lookup/store

Required scenarios:
- full coverage lookup by non-exact start;
- exact-start hit;
- cache miss;
- overlapping block replacement preserves earlier data;
- eviction removes entries correctly;
- cache size accounting remains correct after put, overwrite, and eviction.

### 3. Unit: inflight coordination

Required scenarios:
- two reads for same range produce one backend fetch;
- subrange read joins existing inflight range;
- inflight state cleaned after success;
- inflight state cleaned after error;
- inflight state cleaned after cancel;
- stale session data evicted from cache after seek cancellation (sessionID-based).

### 4. Unit: prefetch decision logic

Required scenarios:
- prefetch starts when expected (maxServedOffset >= currentWindowStart + 4 MiB);
- prefetch does not start when range is already cached;
- prefetch does not start when range is already inflight;
- overlapping prefetch suppression works;
- next-window trigger works;
- per-file inflight count < 2 enforced;
- global HTTP semaphore availability checked before prefetch;
- prefetch skipped after recent far seek for file.

### 5. Unit: read path correctness

Required scenarios:
- read fully from cache;
- read fully from inflight data;
- read through backend on miss;
- read across window boundary (prefetchWindowSize-aligned);
- short read near EOF;
- exact byte correctness for requested range;
- no off-by-one at boundaries.

### 6. Unit: seek/error/concurrency

Required scenarios:
- seek to start;
- seek to middle;
- seek near EOF;
- repeated seeks;
- backend timeout;
- short/partial backend response;
- invalid backend range response;
- temporary backend error;
- state remains usable after error;
- race-sensitive paths covered under `go test -race`;
- CDN permalink redirect loop or redirect to wrong host does not hang;
- API rate limiter (global semaphore capacity=1) serializes concurrent API calls;
- SQLite inode stability: same inode for same content_key across DB reopen;
- catalog tree swap atomicity: refresh does not break ongoing reads.

### 7. Integration: sequential playback from start

Required scenario:
- open file;
- startup reads;
- sequential forward reads.

Required assertions:
- correct data returned;
- backend requests remain bounded;
- cache/inflight reuse occurs.

### 8. Integration: mid-file playback start

Required scenario:
- open file;
- seek to middle;
- several small reads near target offset;
- continued sequential reads forward.

Required assertions:
- first bytes after seek arrive without full-window blocking;
- repeated small reads do not create one backend fetch each;
- nearby reads reuse cache/inflight state.

### 9. Integration: EOF probe plus playback

Required scenario:
- open file;
- read or seek near EOF;
- seek to actual playback offset;
- continue normal reads.

Required assertions:
- EOF probe does not break playback path;
- no stale tail-side activity interferes with playback.

### 10. Integration: repeated small reads in one window

Required scenario:
- many small reads within one logical window.

Required assertions:
- backend request count is materially lower than read count;
- no duplicate fetch storm.

### 11. Integration: boundary crossing

Required scenario:
- reads approach end of current window and continue into next.

Required assertions:
- no corruption or gaps at boundary;
- no severe latency spike on each crossing;
- next-window behavior is controlled.

### 12. Integration: rapid sequential seeks

Required scenario:
- seek to 0% of file size;
- seek to 50% of file size;
- seek to 90% of file size;
- real reading only after final seek.

Required assertions:
- obsolete prefetch/inflight work does not grow unbounded;
- active path switches to final target correctly.

### 13. Integration: concurrent readers

Required scenarios:
- multiple readers of same file;
- multiple readers of different files.

Required assertions:
- no data corruption;
- no state leakage across files;
- concurrency limits remain effective.

### 14. Integration: backend failure recovery

Required scenario:
- backend fails temporarily, then recovers.

Required assertions:
- application does not wedge;
- subsequent reads succeed after recovery;
- cache/inflight state remains consistent.

### 15. E2E: real playback behavior

> **Platform note:** FUSE-based E2E tests require FUSE availability. On macOS,
> `osxfuse`/`macFUSE` must be installed. Guard these tests with a build tag
> (`//go:build linux || darwin`) and skip at runtime if mount fails. On CI,
> run only on Linux workers with FUSE installed.

Required scenarios:
- playback start from beginning;
- playback start from middle;
- several UI seeks/scrubs;
- sustained playback;
- restart/remount/reconnect recovery, if lifecycle exists.

Required assertions:
- playback actually starts;
- playback continues after seeks;
- no fatal stalls;
- system recovers after restart/remount.

### 16. Non-functional

Required checks:
- `go test -race` passes where applicable;
- no unbounded memory growth under repeated playback/seek scenarios;
- no goroutine/resource leaks;
- cancelled operations release resources;
- logs/metrics distinguish at minimum: cache hit, cache miss, inflight join, prefetch start, prefetch completion, error.

## Recommended coverage

### 17. Unit/integration recommended

Recommended scenarios:
- property-based tests for range algebra;
- fuzzed read/seek sequences;
- timing-controlled streaming tests;
- backpressure tests with slow consumer;
- retry/backoff tests for throttling or transient backend failures.

### 18. E2E recommended

Recommended scenarios:
- warm-cache vs cold-cache playback;
- different media containers;
- different Plex clients, if relevant;
- simultaneous multiple active streams.

## Audit summary format

Required summary sections:

### Mandatory gaps
- list all mandatory scenarios not covered or only partially covered.

### Highest-risk gaps
- top risks affecting playback correctness, startup latency, seek behavior, or backend amplification.

### Evidence index
- tests, files, suites, or logs used to justify each coverage mark.

## Evidence collection

Run the following to gather evidence before auditing:
```bash
# List all test functions
go test -json ./... 2>/dev/null | jq -r '.Test | select(. != null)' | sort -u

# List test files
find . -name '*_test.go' -not -path './.claude/*' | sort

# Run tests with race detector
go test -race -count=1 ./...
```
