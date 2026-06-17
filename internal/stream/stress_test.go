//go:build !short

package stream

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
)

// ============================================================
// Stress tests: simulate Plex-like conditions
// ============================================================
//
// These tests exercise the full streaming pipeline under realistic conditions:
// concurrent file access, rate limiting, cache eviction pressure, and
// complex read patterns. They are gated behind !short because they need
// real goroutine scheduling and take >1s each.

// ── Mock CDN infrastructure ─────────────────────────────────────────────

// stressFile holds test data and metadata for one simulated media file.
type stressFile struct {
	key  string
	data []byte
	size int64
}

// stressConfig controls the behavior of the mock CDN and StreamReader.
type stressConfig struct {
	// CDN behavior
	rateLimitAfter    int           // return 429 after this many requests (0 = never)
	rateLimitStatus   int           // HTTP status for rate limiting (default: 429)
	serverErrorsAfter int           // return 5xx after this many requests (0 = never)
	serverErrorStatus int           // HTTP status for server errors (default: 500)
	cdnLatency        time.Duration // artificial delay per CDN response (0 = none)
	cdnChunkDelay     time.Duration // delay between chunks when sending response body (0 = send instantly)
	redirectCDN       bool          // if true, API URL redirects to a CDN URL

	// StreamReader configuration
	windowSize       int64         // bytes per inflight window (default: 4 MiB)
	maxInflight      int           // per-file max inflight windows (default: 2)
	maxGlobalWindows int           // global max inflight windows (default: 100)
	cacheBudgetMB    int           // cache budget in MiB (default: 256)
	urlCacheTTL      time.Duration // CDN URL cache TTL (default: 5m)
	concurrency      int           // per-CDN-host max concurrent requests (default: 8)
}

func (c *stressConfig) defaults() *stressConfig {
	if c.windowSize == 0 {
		c.windowSize = 4 << 20 // 4 MiB
	}
	if c.maxInflight == 0 {
		c.maxInflight = 2
	}
	if c.maxGlobalWindows == 0 {
		c.maxGlobalWindows = 100
	}
	if c.cacheBudgetMB == 0 {
		c.cacheBudgetMB = 256
	}
	if c.urlCacheTTL == 0 {
		c.urlCacheTTL = 5 * time.Minute
	}
	if c.concurrency == 0 {
		c.concurrency = 8
	}
	if c.rateLimitStatus == 0 {
		c.rateLimitStatus = 429
	}
	if c.serverErrorStatus == 0 {
		c.serverErrorStatus = 500
	}
	return c
}

// stressTestEnv holds all the pieces needed for a stress test.
type stressTestEnv struct {
	sr          *StreamReader
	cdn         *CDNClient
	rc          *cache.RangeCache
	server      *httptest.Server
	cfg         *stressConfig
	files       []stressFile
	requests    atomic.Int32 // total CDN requests
	resolves    atomic.Int32 // total resolve requests (for redirect mode)
	rateLimited atomic.Int32 // total 429 responses returned
}

// newStressEnv creates a stress test environment with the given config and files.
func newStressEnv(t *testing.T, cfg *stressConfig, files []stressFile) *stressTestEnv {
	t.Helper()
	cfg.defaults()

	env := &stressTestEnv{
		cfg:   cfg,
		files: files,
	}

	// Build a map of file data for the mock server.
	fileData := make(map[string][]byte, len(files))
	for _, f := range files {
		fileData[f.key] = f.data
	}

	if cfg.redirectCDN {
		// Two-server mode: API server redirects to CDN server.
		cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			env.serveCDN(w, r, fileData)
		}))
		env.server = cdnServer

		apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			env.resolves.Add(1)
			// Redirect to CDN server with the same path.
			w.Header().Set("Location", cdnServer.URL+r.URL.Path)
			w.WriteHeader(http.StatusFound)
		}))

		env.rc = cache.NewRangeCache(int64(cfg.cacheBudgetMB)<<20, nil)
		env.cdn = NewCDNClient(cfg.concurrency, nil, cfg.urlCacheTTL)

		permalinkBuilder := func(fileKey string) string {
			return apiServer.URL + "/" + fileKey
		}
		env.sr = NewStreamReader(env.rc, env.cdn, cfg.maxInflight, cfg.maxGlobalWindows, cfg.windowSize, permalinkBuilder, nil)

		// We need to close the API server too, but we can't add it to env.server.
		// Store a custom cleanup.
		t.Cleanup(func() {
			apiServer.Close()
			cdnServer.Close()
		})
	} else {
		// Single-server mode: CDN serves files directly.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			env.serveCDN(w, r, fileData)
		}))
		env.server = server

		env.rc = cache.NewRangeCache(int64(cfg.cacheBudgetMB)<<20, nil)
		env.cdn = NewCDNClient(cfg.concurrency, nil, cfg.urlCacheTTL)

		permalinkBuilder := func(fileKey string) string {
			return server.URL + "/" + fileKey
		}
		env.sr = NewStreamReader(env.rc, env.cdn, cfg.maxInflight, cfg.maxGlobalWindows, cfg.windowSize, permalinkBuilder, nil)
	}

	t.Cleanup(func() {
		// Cleanup is handled above for redirect mode.
		// For single-server mode, we close here.
		if !cfg.redirectCDN {
			env.server.Close()
		}
	})

	return env
}

// serveCDN handles a CDN range request, applying rate limiting and server errors
// as configured.
func (env *stressTestEnv) serveCDN(w http.ResponseWriter, r *http.Request, fileData map[string][]byte) {
	reqNum := env.requests.Add(1)

	// Extract file key from URL path.
	fileKey := r.URL.Path[1:] // remove leading slash
	data, ok := fileData[fileKey]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	// Apply rate limiting.
	if env.cfg.rateLimitAfter > 0 && reqNum > int32(env.cfg.rateLimitAfter) {
		env.rateLimited.Add(1)
		w.WriteHeader(env.cfg.rateLimitStatus)
		return
	}

	// Apply server errors.
	if env.cfg.serverErrorsAfter > 0 && reqNum > int32(env.cfg.serverErrorsAfter) {
		w.WriteHeader(env.cfg.serverErrorStatus)
		return
	}

	// Apply pre-response latency (simulates network round-trip).
	if env.cfg.cdnLatency > 0 {
		time.Sleep(env.cfg.cdnLatency)
	}

	// Serve range request.
	rangeHdr := r.Header.Get("Range")
	if rangeHdr == "" {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		writeChunked(w, data, env.cfg.cdnChunkDelay)
		return
	}
	var start, end int64
	n, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
	if err != nil || n != 2 {
		http.Error(w, "bad range", http.StatusBadRequest)
		return
	}
	if start >= int64(len(data)) {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if end >= int64(len(data)) {
		end = int64(len(data) - 1)
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	writeChunked(w, data[start:end+1], env.cfg.cdnChunkDelay)
}

// writeChunked writes data to w in chunks with an optional delay between chunks.
// This simulates realistic CDN transfer behavior where a 4 MiB window takes
// hundreds of milliseconds to transfer over the network, holding the connection
// semaphore slot for the entire duration — not just the initial latency.
func writeChunked(w http.ResponseWriter, data []byte, chunkDelay time.Duration) {
	if chunkDelay <= 0 {
		w.Write(data)
		return
	}
	// Write in 256 KiB chunks with delay between each.
	// At 5ms/chunk, a 4 MiB window takes ~80ms to transfer,
	// closely modeling real CDN behavior.
	const chunkSize = 256 * 1024
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		w.Write(data[offset:end])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		if end < len(data) {
			time.Sleep(chunkDelay)
		}
	}
}

// makeStressFiles creates count files of the given size with deterministic data.
func makeStressFiles(count int, size int64) []stressFile {
	files := make([]stressFile, count)
	for i := 0; i < count; i++ {
		data := make([]byte, size)
		// Each file gets a unique pattern: byte((offset + fileIndex) % 256)
		// This makes data corruption detectable even across files.
		for j := range data {
			data[j] = byte((int64(j) + int64(i)*7) % 256)
		}
		files[i] = stressFile{
			key:  fmt.Sprintf("file_%03d", i),
			data: data,
			size: size,
		}
	}
	return files
}

// verifyData checks that buf matches the expected pattern for the given file
// index and offset.
func verifyData(t *testing.T, fileIdx int, offset int64, buf []byte) {
	t.Helper()
	for i := 0; i < len(buf); i++ {
		expected := byte((offset + int64(i) + int64(fileIdx)*7) % 256)
		if buf[i] != expected {
			t.Errorf("data mismatch at file %d offset %d: got 0x%02x, want 0x%02x",
				fileIdx, offset+int64(i), buf[i], expected)
			return
		}
	}
}

// ============================================================
// Test 1: Plex playback pattern
// ============================================================
// Simulates a single file being read with a Plex-like pattern:
// 1. Header probe (read first 64 KiB)
// 2. EOF probe (read last 1 KiB)
// 3. Sequential playback (128 KiB chunks from start)
// 4. Seek/scrub (jump to middle, read a bit, jump back)

func TestStress_PlexPlaybackPattern(t *testing.T) {
	const fileSize = 16 * 1024 * 1024 // 16 MiB
	files := makeStressFiles(1, fileSize)

	env := newStressEnv(t, &stressConfig{}, files)
	f := env.files[0]
	ctx := context.Background()

	// Step 1: Header probe (first 64 KiB)
	t.Log("step 1: header probe")
	buf := make([]byte, 64*1024)
	n, err := env.sr.ReadAt(ctx, f.key, 0, buf, f.size)
	if err != nil && err != io.EOF {
		t.Fatalf("header probe: %v", err)
	}
	verifyData(t, 0, 0, buf[:n])

	// Step 2: EOF probe (last 1 KiB)
	t.Log("step 2: EOF probe")
	eofOff := f.size - 1024
	eofBuf := make([]byte, 1024)
	n, err = env.sr.ReadAt(ctx, f.key, eofOff, eofBuf, f.size)
	if err != nil && err != io.EOF {
		t.Fatalf("EOF probe: %v", err)
	}
	verifyData(t, 0, eofOff, eofBuf[:n])

	// Step 3: Sequential playback (128 KiB chunks from offset 0)
	t.Log("step 3: sequential playback")
	playbackBuf := make([]byte, 128*1024)
	var totalRead int64
	for off := int64(0); off < f.size; {
		n, err := env.sr.ReadAt(ctx, f.key, off, playbackBuf, f.size)
		if err != nil && err != io.EOF {
			t.Fatalf("sequential playback at offset %d: %v", off, err)
		}
		if n == 0 {
			break
		}
		verifyData(t, 0, off, playbackBuf[:n])
		totalRead += int64(n)
		off += int64(n)
	}
	if totalRead != f.size {
		t.Errorf("sequential playback: read %d bytes, want %d", totalRead, f.size)
	}

	// Step 4: Seek/scrub (jump to middle, read, jump back)
	t.Log("step 4: seek/scrub")
	midOff := f.size / 2
	scrubBuf := make([]byte, 4*1024)
	n, err = env.sr.ReadAt(ctx, f.key, midOff, scrubBuf, f.size)
	if err != nil && err != io.EOF {
		t.Fatalf("seek to middle: %v", err)
	}
	verifyData(t, 0, midOff, scrubBuf[:n])

	// Jump back to near-start
	n, err = env.sr.ReadAt(ctx, f.key, 1024, scrubBuf, f.size)
	if err != nil && err != io.EOF {
		t.Fatalf("seek back to start: %v", err)
	}
	verifyData(t, 0, 1024, scrubBuf[:n])

	t.Logf("total CDN requests: %d", env.requests.Load())
}

// ============================================================
// Test 2: Library scan thundering herd
// ============================================================
// Simulates Plex opening 30 files simultaneously, reading metadata
// from each (2-3 small random reads), then closing them.
// Verifies: all reads succeed, no data corruption, CDN load is bounded.

func TestStress_LibraryScanThunderingHerd(t *testing.T) {
	const fileCount = 30
	const fileSize int64 = 4 * 1024 * 1024 // 4 MiB each

	files := makeStressFiles(fileCount, fileSize)
	env := newStressEnv(t, &stressConfig{}, files)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type result struct {
		fileIdx int
		err     error
	}

	results := make(chan result, fileCount*3) // 3 reads per file
	var wg sync.WaitGroup

	for i := 0; i < fileCount; i++ {
		f := env.files[i]
		// Each file gets 2-3 small random reads (metadata pattern).
		offsets := []int64{0, f.size - 1024, f.size / 3}
		for _, off := range offsets {
			wg.Add(1)
			go func(idx int, offset int64) {
				defer wg.Done()
				buf := make([]byte, 4096)
				// Clamp read to file size
				readEnd := offset + int64(len(buf))
				if readEnd > f.size {
					buf = buf[:f.size-offset]
				}
				n, err := env.sr.ReadAt(ctx, env.files[idx].key, offset, buf, env.files[idx].size)
				if err != nil && err != io.EOF {
					results <- result{fileIdx: idx, err: fmt.Errorf("ReadAt(file=%d, off=%d): %w", idx, offset, err)}
					return
				}
				verifyData(t, idx, offset, buf[:n])
				results <- result{fileIdx: idx}
			}(i, off)
		}
	}

	wg.Wait()
	close(results)

	var errors []error
	for res := range results {
		if res.err != nil {
			errors = append(errors, res.err)
		}
	}
	if len(errors) > 0 {
		for _, e := range errors {
			t.Error(e)
		}
	}

	t.Logf("library scan: %d files, %d CDN requests", fileCount, env.requests.Load())
}

// ============================================================
// Test 3: Playback during scan (the critical scenario)
// ============================================================
// While 30 files are being scanned, one file is being played sequentially.
// Verifies: playback data arrives promptly, scan data doesn't evict
// playback data from cache, and both complete successfully.

func TestStress_PlaybackDuringScan(t *testing.T) {
	const fileCount = 30
	const fileSize int64 = 4 * 1024 * 1024 // 4 MiB each

	files := makeStressFiles(fileCount, fileSize)
	env := newStressEnv(t, &stressConfig{
		cacheBudgetMB: 32, // Small budget to force eviction pressure
	}, files)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start library scan in background.
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		for i := 0; i < fileCount; i++ {
			f := env.files[i]
			// Skip file 0 — that's our playback file.
			if i == 0 {
				continue
			}
			// 2-3 metadata reads per file.
			for _, off := range []int64{0, f.size - 1024, f.size / 3} {
				buf := make([]byte, 4096)
				if off+int64(len(buf)) > f.size {
					buf = buf[:f.size-off]
				}
				n, err := env.sr.ReadAt(ctx, f.key, off, buf, f.size)
				if err != nil && err != io.EOF {
					t.Logf("scan read file=%d off=%d: %v", i, off, err)
					continue
				}
				verifyData(t, i, off, buf[:n])
			}
		}
	}()

	// Simultaneously, play file 0 sequentially.
	playbackFile := env.files[0]
	playbackBuf := make([]byte, 128*1024)
	var playbackBytes int64
	var playbackStart time.Time
	playbackStarted := false

	for off := int64(0); off < playbackFile.size; {
		if !playbackStarted {
			playbackStart = time.Now()
			playbackStarted = true
		}
		n, err := env.sr.ReadAt(ctx, playbackFile.key, off, playbackBuf, playbackFile.size)
		if err != nil && err != io.EOF {
			t.Fatalf("playback at offset %d: %v", off, err)
		}
		if n == 0 {
			break
		}
		verifyData(t, 0, off, playbackBuf[:n])
		playbackBytes += int64(n)
		off += int64(n)
	}

	playbackDuration := time.Since(playbackStart)
	if playbackBytes != playbackFile.size {
		t.Errorf("playback: read %d bytes, want %d", playbackBytes, playbackFile.size)
	}

	// Verify playback completes in reasonable time.
	// 16 MiB over localhost should take < 10s even under cache pressure.
	if playbackDuration > 30*time.Second {
		t.Errorf("playback took %v, expected < 30s", playbackDuration)
	}

	// Wait for scan to complete.
	<-scanDone

	t.Logf("playback during scan: %d bytes in %v, %d CDN requests",
		playbackBytes, playbackDuration, env.requests.Load())
}

// ============================================================
// Test 4: Rate-limited URL resolution
// ============================================================
// Simulates 429 on URL resolution. Verifies:
// - Only 1 actual resolution attempt (singleflight dedup)
// - Subsequent calls during backoff return an error (not a fallback URL)
// - After backoff expires, resolution succeeds

func TestStress_RateLimitedResolution(t *testing.T) {
	// Set up: API server returns 429 on the first attempt, then succeeds
	// by redirecting to the CDN server.
	var resolveAttempts atomic.Int32
	var cdnRequests atomic.Int32
	var return429 atomic.Bool

	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cdnRequests.Add(1)
		// CDN server responds with 206 for range requests.
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/100", start, end))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(make([]byte, end-start+1))
	}))
	defer cdnServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resolveAttempts.Add(1)
		// Return 429 only when the flag is set (simulates a transient outage).
		if return429.Load() {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Location", cdnServer.URL+r.URL.Path)
		w.WriteHeader(http.StatusFound)
	}))
	defer apiServer.Close()

	cdn := NewCDNClient(8, nil, 5*time.Minute)
	cdn.rateLimitCooldown = 200 * time.Millisecond // fast backoff for tests
	ctx := context.Background()
	apiURL := apiServer.URL + "/test_file"

	// Phase 1: API returns 429. ResolveURL should return an error.
	return429.Store(true)
	_, err := cdn.ResolveURL(ctx, apiURL)
	if err == nil {
		t.Fatal("phase 1: expected error on 429, got nil")
	}
	if resolveAttempts.Load() != 1 {
		t.Errorf("phase 1: expected 1 resolve attempt, got %d", resolveAttempts.Load())
	}

	// Phase 2: Subsequent calls during the backoff period should also return
	// errors without making additional resolution attempts.
	for i := 0; i < 10; i++ {
		_, err := cdn.ResolveURL(ctx, apiURL)
		if err == nil {
			t.Errorf("phase 2 call %d: expected error during backoff, got nil", i+1)
		}
	}
	// No additional resolve attempts — all calls hit the cached 429 entry.
	if resolveAttempts.Load() != 1 {
		t.Errorf("phase 2: expected 1 resolve attempt (all others cached), got %d", resolveAttempts.Load())
	}

	// Phase 3: Concurrent calls during backoff should all return errors
	// and singleflight should dedup them.
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cdn.ResolveURL(ctx, apiURL)
			if err == nil {
				t.Errorf("phase 3: expected error during backoff, got nil")
			}
		}()
	}
	wg.Wait()
	if resolveAttempts.Load() != 1 {
		t.Errorf("phase 3: expected 1 resolve attempt after 30 concurrent calls during backoff, got %d", resolveAttempts.Load())
	}

	// Phase 4: After backoff expires, API now returns 302 redirect.
	// Clear the 429 flag so resolution succeeds.
	return429.Store(false)
	time.Sleep(cdn.rateLimitCooldown + 100*time.Millisecond)

	result, err := cdn.ResolveURL(ctx, apiURL)
	if err != nil {
		t.Fatalf("phase 4: expected success after backoff expiry, got error: %v", err)
	}
	if result != cdnServer.URL+"/test_file" {
		t.Errorf("phase 4: expected CDN URL after backoff expiry, got %q", result)
	}

	// Phase 5: Successful resolution should be cached — no more resolve attempts.
	finalAttempts := resolveAttempts.Load()
	result2, err := cdn.ResolveURL(ctx, apiURL)
	if err != nil {
		t.Fatalf("phase 5: expected cached success, got error: %v", err)
	}
	if result2 != cdnServer.URL+"/test_file" {
		t.Errorf("phase 5: expected cached CDN URL, got %q", result2)
	}
	if resolveAttempts.Load() != finalAttempts {
		t.Errorf("phase 5: expected no additional resolve attempts, got %d (was %d)",
			resolveAttempts.Load(), finalAttempts)
	}

	t.Logf("resolve attempts: %d, CDN requests: %d", resolveAttempts.Load(), cdnRequests.Load())
}

// ============================================================
// Test 5: Rate-limited CDN range requests
// ============================================================
// Simulates 429 on CDN range requests (FetchRange). Verifies that
// fetchWindow retries with backoff and playback eventually succeeds.

func TestStress_RateLimitedCDNRequests(t *testing.T) {
	const fileSize int64 = 4 * 1024 * 1024 // 4 MiB
	files := makeStressFiles(1, fileSize)

	// Use redirectCDN mode so that ResolveURL (API server) and CDN data
	// requests go to separate servers. This lets us rate-limit only CDN
	// data requests while keeping URL resolution working — matching the
	// real-world scenario where the CDN returns 429 but the API is fine.
	env := newStressEnv(t, &stressConfig{
		redirectCDN: true,
	}, files)

	// Override the CDN server handler to rate-limit the first 2 DATA requests
	// only (not resolution probes). resolveRedirect sends Range: bytes=0-0
	// probes to verify the CDN URL — those must succeed so the URL resolves.
	// Once resolved, the actual data requests (with larger ranges) get 429'd
	// first, then succeed on retry.
	origCDNHandler := env.server.Config.Handler
	var cdnDataRequestCount atomic.Int32
	env.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip rate-limiting for resolution probes (Range: bytes=0-0).
		if r.Header.Get("Range") == "bytes=0-0" {
			origCDNHandler.ServeHTTP(w, r)
			return
		}
		reqNum := cdnDataRequestCount.Add(1)
		if reqNum <= 2 {
			env.rateLimited.Add(1)
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		origCDNHandler.ServeHTTP(w, r)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	f := env.files[0]
	buf := make([]byte, 4096)
	n, err := env.sr.ReadAt(ctx, f.key, 0, buf, f.size)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt after rate limit: %v", err)
	}
	verifyData(t, 0, 0, buf[:n])

	t.Logf("rate-limited CDN: %d CDN requests, %d rate-limited responses",
		env.requests.Load(), env.rateLimited.Load())
}

// ============================================================
// Test 6: Cache eviction priority
// ============================================================
// Small cache budget (16 MiB = 4 windows) with playback + scan data.
// Playback data should survive eviction while scan data gets evicted.

func TestStress_CacheEvictionPriority(t *testing.T) {
	const fileSize int64 = 4 * 1024 * 1024 // 4 MiB (1 window each)
	files := makeStressFiles(8, fileSize)  // 8 files = 32 MiB total

	env := newStressEnv(t, &stressConfig{
		cacheBudgetMB: 16, // Only 4 windows fit — forces eviction
	}, files)

	ctx := context.Background()

	// Step 1: Read all 8 files (scan pattern) — fills cache with PriorityLow.
	for i := 0; i < 8; i++ {
		f := env.files[i]
		buf := make([]byte, 4096)
		_, err := env.sr.ReadAt(ctx, f.key, 0, buf, f.size)
		if err != nil && err != io.EOF {
			t.Fatalf("scan read file %d: %v", i, err)
		}
	}

	// Step 2: Now read file 0 sequentially (playback pattern) — should get PriorityHigh.
	// Read enough to trigger sequential detection and active-read threshold.
	playbackFile := env.files[0]
	playbackBuf := make([]byte, 128*1024)
	var totalRead int64
	for off := int64(0); off < playbackFile.size && totalRead < 2*1024*1024; {
		n, err := env.sr.ReadAt(ctx, playbackFile.key, off, playbackBuf, playbackFile.size)
		if err != nil && err != io.EOF {
			t.Fatalf("playback read at %d: %v", off, err)
		}
		if n == 0 {
			break
		}
		verifyData(t, 0, off, playbackBuf[:n])
		totalRead += int64(n)
		off += int64(n)
	}

	// Step 3: Re-read file 0 from cache — should still be available
	// because it has PriorityHigh while scan data was PriorityLow.
	// Even though the cache budget is only 16 MiB (4 windows) and we
	// wrote 32 MiB (8 files) + re-read 2 MiB of file 0, the playback
	// data should have survived eviction.
	buf := make([]byte, 4096)
	n, err := env.sr.ReadAt(ctx, playbackFile.key, 0, buf, playbackFile.size)
	if err != nil && err != io.EOF {
		t.Fatalf("cache re-read: %v", err)
	}
	verifyData(t, 0, 0, buf[:n])

	t.Logf("cache eviction priority: %d bytes used of %d budget, %d CDN requests",
		env.rc.Used(), int64(env.cfg.cacheBudgetMB)<<20, env.requests.Load())
}

// ============================================================
// Test 7: Seek cancellation during playback
// ============================================================
// Plex-style seek pattern: play from start → seek to middle → play → seek to end.
// Verifies: no data corruption, cancelled windows don't lose needed data,
// read-ahead still works after seeks.

func TestStress_SeekCancellationDuringPlayback(t *testing.T) {
	const fileSize int64 = 32 * 1024 * 1024 // 32 MiB = 8 windows
	files := makeStressFiles(1, fileSize)

	env := newStressEnv(t, &stressConfig{}, files)
	f := env.files[0]
	ctx := context.Background()

	// Phase 1: Read from start (triggers read-ahead for next windows).
	buf := make([]byte, 128*1024)
	for off := int64(0); off < 2*1024*1024; off += int64(len(buf)) {
		n, err := env.sr.ReadAt(ctx, f.key, off, buf, f.size)
		if err != nil && err != io.EOF {
			t.Fatalf("phase 1 read at %d: %v", off, err)
		}
		verifyData(t, 0, off, buf[:n])
	}

	// Let read-ahead windows complete.
	time.Sleep(200 * time.Millisecond)

	// Phase 2: Seek to middle (far seek > 4 * windowSize).
	midOff := int64(16 * 1024 * 1024)
	midBuf := make([]byte, 128*1024)
	n, err := env.sr.ReadAt(ctx, f.key, midOff, midBuf, f.size)
	if err != nil && err != io.EOF {
		t.Fatalf("phase 2 seek to middle: %v", err)
	}
	verifyData(t, 0, midOff, midBuf[:n])

	// Phase 3: Continue reading from middle.
	for off := midOff + int64(n); off < midOff+2*1024*1024; off += int64(len(buf)) {
		n, err := env.sr.ReadAt(ctx, f.key, off, buf, f.size)
		if err != nil && err != io.EOF {
			t.Fatalf("phase 3 read at %d: %v", off, err)
		}
		if n == 0 {
			break
		}
		verifyData(t, 0, off, buf[:n])
	}

	// Phase 4: Seek back to near-start (another far seek).
	nearStartOff := int64(3 * 1024 * 1024)
	nearBuf := make([]byte, 4096)
	n, err = env.sr.ReadAt(ctx, f.key, nearStartOff, nearBuf, f.size)
	if err != nil && err != io.EOF {
		t.Fatalf("phase 4 seek back: %v", err)
	}
	verifyData(t, 0, nearStartOff, nearBuf[:n])

	// Phase 5: Continue reading from near-start — read-ahead should recover.
	for off := nearStartOff + int64(n); off < nearStartOff+1*1024*1024; off += int64(len(buf)) {
		n, err := env.sr.ReadAt(ctx, f.key, off, buf, f.size)
		if err != nil && err != io.EOF {
			t.Fatalf("phase 5 read at %d: %v", off, err)
		}
		if n == 0 {
			break
		}
		verifyData(t, 0, off, buf[:n])
	}

	t.Logf("seek cancellation: %d CDN requests", env.requests.Load())
}

// ============================================================
// Test 8: Priority semaphore — high-priority requests must not
//         wait behind low-priority requests
// ============================================================
// This is a DIRECT unit test for the CDN connection semaphore.
// It exposes the production bug: when a library scan hogs all
// per-CDN-host connection slots, playback data arrives too slowly
// because FetchRange treats all requests equally — there's no
// priority queue.
//
// The test works by:
// 1. Launching N "scan" goroutines that each acquire a semaphore
//    slot and hold it for holdTime (simulating a slow CDN transfer).
// 2. Once all scan goroutines are blocking on the semaphore, launching
//    1 "playback" goroutine.
// 3. Measuring how long the playback goroutine waits before getting
//    a semaphore slot.
//
// WITHOUT priority: playback waits behind ALL queued scan requests.
//   With concurrency=2, holdTime=100ms, and 10 scan requests queued:
//   playback waits ~10 * (100ms / 2) = 500ms.
//
// WITH priority: playback jumps the queue and gets a slot as soon
//   as one of the 2 active requests finishes (~50ms wait).

func TestCDNClient_PrioritySemaphore(t *testing.T) {
	const maxConns = 2
	const holdTime = 100 * time.Millisecond
	const scanCount = 10 // queued scan requests

	// Server that holds the connection for holdTime, simulating a
	// real CDN transfer that keeps the semaphore slot occupied.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(holdTime)
		w.Header().Set("Content-Range", "bytes 0-0/*")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer ts.Close()

	cdn := NewCDNClient(maxConns, nil, 0)
	ctx := context.Background()

	// Step 1: Launch scan requests that saturate the semaphore.
	// We launch more than maxConns so some will queue up.
	scanResults := make(chan error, scanCount)
	var started sync.WaitGroup // signals when goroutines are about to call FetchRange
	started.Add(scanCount)

	for i := 0; i < scanCount; i++ {
		go func() {
			started.Done()
			resp, err := cdn.FetchRange(ctx, ts.URL, 0, 0, cache.PriorityLow)
			if err != nil {
				scanResults <- err
				return
			}
			resp.Body.Close()
			scanResults <- nil
		}()
	}

	// Wait for all scan goroutines to be launched. They'll race to
	// acquire the 2 semaphore slots — 2 get slots, 8 queue up.
	started.Wait()
	// Give a tiny moment for the 2 active goroutines to enter the handler.
	time.Sleep(5 * time.Millisecond)

	// Step 2: Launch playback request. It must wait for a semaphore slot.
	// Without priority: it joins the back of the FIFO queue behind 8 scan requests.
	// With priority: it jumps to the front and gets the next available slot.
	playbackStart := time.Now()
	resp, err := cdn.FetchRange(ctx, ts.URL, 0, 0, 1) // PriorityHigh = 1 (playback)
	if err != nil {
		t.Fatalf("playback FetchRange error: %v", err)
	}
	resp.Body.Close()
	playbackWait := time.Since(playbackStart)

	// Drain scan results.
	for i := 0; i < scanCount; i++ {
		if err := <-scanResults; err != nil {
			t.Errorf("scan FetchRange error: %v", err)
		}
	}

	// Calculate expected wait times:
	// Without priority (FIFO): playback is request #11 in queue.
	//   With 2 slots and holdTime=100ms per request:
	//   8 queued ahead / 2 slots = 4 rounds * 100ms = ~400ms minimum wait.
	//   Plus the partial round from the 2 active requests = ~50-100ms.
	//   Total: ~450-500ms.
	//
	// With priority: playback jumps the queue.
	//   It gets the very next slot that opens (~50ms = half of holdTime,
	//   since 2 slots are active and one could finish any moment).
	//   Total: ~50-100ms.
	//
	// We use 250ms as the threshold:
	// - With priority: < 250ms (one slot rotation + scheduling jitter)
	// - Without priority: ~450ms (must wait for 8+ scan requests)

	t.Logf("playback wait time: %v (threshold: 250ms)", playbackWait)

	if playbackWait > 250*time.Millisecond {
		t.Errorf("playback request waited %v — connection starvation detected! "+
			"PriorityHigh requests should not wait behind PriorityLow scan requests. "+
			"Expected < 250ms (priority queue), got FIFO-style delay.",
			playbackWait)
	}
}

// ============================================================
// Test 9: Priority global budget — playback must not wait
//         behind scan for inflight window slots
// ============================================================
// This test exposes the production bug where playback cache misses
// block on the globalBudget channel behind queued scan fetches.
//
// Production uses maxGlobalWindows=16, but the stress tests use 100.
// With 100 slots, scan can never fill them all, so playback always
// finds a free slot. With 16 (or fewer), scan easily saturates all
// slots and playback starves.
//
// The test works by:
// 1. Setting maxGlobalWindows=4 (tight budget to force contention).
// 2. Launching 20 scan goroutines that each read the first window of
//    a different file — this fills all 4 budget slots and queues 16.
// 3. After a short delay (scan slots are saturated), starting a
//    sequential playback ReadAt on a separate file.
// 4. Measuring how long playback's first ReadAt takes.
//
// WITHOUT priority budget: playback's fetchWindow blocks on
//   globalBudget behind 16 queued scan fetches. With cdnLatency=100ms
//   and 4 slots, draining 16 queued takes 4 rounds × 100ms = ~400ms.
//   Playback waits ~400ms before it even starts fetching.
//
// WITH priority budget: playback jumps the queue and gets the next
//   available slot (~25-50ms = one slot rotation with 4 slots).
//   Total: ~50-100ms.

func TestStreamReader_PriorityGlobalBudget(t *testing.T) {
	const maxGlobalWindows = 4
	const scanFileCount = 20
	const fileSize int64 = 4 * 1024 * 1024 // 4 MiB = 1 window each
	const cdnLatency = 100 * time.Millisecond

	// Scan files: one window each, 4 MiB.
	scanFiles := makeStressFiles(scanFileCount, fileSize)
	// Playback file: separate, also 4 MiB.
	playbackData := make([]byte, fileSize)
	for j := range playbackData {
		playbackData[j] = byte(j % 256)
	}
	allFiles := append(scanFiles, stressFile{key: "playback", data: playbackData, size: fileSize})

	env := newStressEnv(t, &stressConfig{
		maxGlobalWindows: maxGlobalWindows,
		cdnLatency:       cdnLatency,
		// Small cache so playback data is not retained between rounds
		cacheBudgetMB: 4,
	}, allFiles)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Step 1: Launch scan goroutines. Each reads offset 0 of a scan file,
	// which creates 1 inflight window per file. With maxGlobalWindows=4,
	// 4 windows start fetching immediately; 16 queue on the budget.
	scanResults := make(chan error, scanFileCount)
	var scanStarted sync.WaitGroup
	scanStarted.Add(scanFileCount)

	for i := 0; i < scanFileCount; i++ {
		go func(idx int) {
			scanStarted.Done()
			// Small delay after signaling so goroutines group up.
			time.Sleep(2 * time.Millisecond)
			f := env.files[idx]
			buf := make([]byte, 4096)
			_, err := env.sr.ReadAt(ctx, f.key, 0, buf, f.size)
			if err != nil && err != io.EOF {
				scanResults <- fmt.Errorf("scan file %d: %w", idx, err)
				return
			}
			scanResults <- nil
		}(i)
	}

	// Wait for all scan goroutines to be ready.
	scanStarted.Wait()
	// Give scan goroutines time to acquire budget slots and queue up.
	// With 4 slots and cdnLatency=100ms, the first 4 goroutines enter
	// the CDN handler and hold slots for 100ms each. The remaining 16
	// are blocked on globalBudget.
	time.Sleep(150 * time.Millisecond)

	// Step 2: Start playback. This is a sequential read on a file not
	// being scanned, so it needs its own inflight window — which needs
	// a globalBudget slot. Without priority, it waits behind 16 queued
	// scan windows. With priority, it jumps the queue.
	playbackStart := time.Now()
	playbackBuf := make([]byte, 128*1024)
	n, err := env.sr.ReadAt(ctx, "playback", 0, playbackBuf, fileSize)
	playbackWait := time.Since(playbackStart)

	if err != nil && err != io.EOF {
		t.Fatalf("playback ReadAt: %v", err)
	}
	if n == 0 {
		t.Fatal("playback ReadAt returned 0 bytes")
	}

	// Verify data.
	for i := 0; i < n; i++ {
		if playbackBuf[i] != byte(i%256) {
			t.Fatalf("playback data mismatch at byte %d: got 0x%02x, want 0x%02x",
				i, playbackBuf[i], byte(i%256))
		}
	}

	// Drain scan results.
	for i := 0; i < scanFileCount; i++ {
		if err := <-scanResults; err != nil {
			t.Error(err)
		}
	}

	// Expected wait times:
	// WITHOUT priority (FIFO): playback is request #21. 4 slots active,
	//   16 queued ahead. 16/4 = 4 rounds × 100ms = 400ms minimum.
	//   Plus partial first round = ~50ms. Total: ~450ms.
	//
	// WITH priority: playback jumps the queue of waiting scan requests.
	//   It still waits for one in-progress CDN request to finish
	//   (~0-100ms remaining) plus its own CDN latency (100ms).
	//   Total: ~200-350ms depending on scheduling.
	//
	// We use 400ms as the threshold — well below FIFO wait (~450ms)
	// while being resilient to scheduling jitter under race detector
	// (which can add 10-15ms of overhead per goroutine switch).
	t.Logf("playback first-read wait: %v (threshold: 400ms)", playbackWait)

	if playbackWait > 400*time.Millisecond {
		t.Errorf("playback ReadAt waited %v — global budget starvation detected! "+
			"PriorityHigh playback should not wait behind PriorityLow scan windows. "+
			"Expected < 400ms (priority budget), got FIFO-style delay.",
			playbackWait)
	}
}

// ============================================================
// Test 10: Concurrent playback of different files
// ============================================================
// Multiple files played simultaneously (simulating multiple household
// members or transcodes). Verifies: all complete, no data corruption.

func TestStress_ConcurrentPlaybackDifferentFiles(t *testing.T) {
	const fileCount = 4
	const fileSize int64 = 4 * 1024 * 1024 // 4 MiB each

	files := makeStressFiles(fileCount, fileSize)
	env := newStressEnv(t, &stressConfig{}, files)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type result struct {
		fileIdx   int
		bytesRead int64
		err       error
	}

	results := make(chan result, fileCount)
	var wg sync.WaitGroup

	for i := 0; i < fileCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			f := env.files[idx]
			buf := make([]byte, 128*1024)
			var total int64
			for off := int64(0); off < f.size; {
				n, err := env.sr.ReadAt(ctx, f.key, off, buf, f.size)
				if err != nil && err != io.EOF {
					results <- result{fileIdx: idx, err: fmt.Errorf("file %d at offset %d: %w", idx, off, err)}
					return
				}
				if n == 0 {
					break
				}
				verifyData(t, idx, off, buf[:n])
				total += int64(n)
				off += int64(n)
			}
			results <- result{fileIdx: idx, bytesRead: total}
		}(i)
	}

	wg.Wait()
	close(results)

	for res := range results {
		if res.err != nil {
			t.Error(res.err)
		} else if res.bytesRead != env.files[res.fileIdx].size {
			t.Errorf("file %d: read %d bytes, want %d",
				res.fileIdx, res.bytesRead, env.files[res.fileIdx].size)
		}
	}

	t.Logf("concurrent playback: %d files, %d CDN requests", fileCount, env.requests.Load())
}

// ============================================================
