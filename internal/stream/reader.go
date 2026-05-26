// Package stream provides the CDN range HTTP client and StreamReader that
// manages inflight windows, early return, read-ahead, and seek cancellation
// for the TorBox FUSE filesystem streaming hot path.
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/metrics"
)

const (
	// maxRetries is the maximum number of retry attempts for retryable CDN
	// errors (429 rate limit, 5xx server errors). Total attempts = 1 + maxRetries.
	maxRetries = 3

	// maxBackoff caps the exponential backoff duration.
	maxBackoff = 30 * time.Second

	// readChunkSize is the size of each chunk read from the CDN response body.
	// 32 KiB balances syscall overhead with prompt early-return updates.
	readChunkSize = 32 * 1024
)

// PermalinkBuilder returns the CDN URL for a given fileKey.
type PermalinkBuilder func(fileKey string) string

// StreamReader manages inflight windows and delegates to the cache and CDN client.
// It supports early return (readers return as soon as their requested bytes are
// ready, not when the full window completes), read-ahead of the next window,
// and seek cancellation for far seeks.
type StreamReader struct {
	cache         *cache.RangeCache
	cdn           *CDNClient
	inflight      sync.Map // inflightKey -> *inflightWindow
	sessions      sync.Map // fileKey -> *fileSession
	windowSize    int64
	maxInflight   int
	permalinkFor  PermalinkBuilder
	metrics       *metrics.Metrics
}

// inflightKey identifies an inflight window by file key and window start offset.
type inflightKey struct {
	fileKey string
	start   int64
}

// inflightWindow tracks an in-progress CDN fetch for a single window.
// Readers wait on readyCond until their requested bytes are available (early return).
//
// Synchronization: buf, total, and err are protected by readyCond.L.
// fetchWindow holds the lock for each chunk append + Broadcast, and
// waitForBytes holds the lock while copying from buf. readyTo and done
// are atomic and can be read without the lock.
type inflightWindow struct {
	key        inflightKey
	buf        []byte
	readyTo    atomic.Int64
	total      int64
	err        error
	done       atomic.Bool
	readyCond  *sync.Cond
	cancelFunc context.CancelFunc
	waiters    atomic.Int32
	fileSize   int64
}

// fileSession tracks per-file read-ahead state.
type fileSession struct {
	lastSeek atomic.Int64
}

// NewStreamReader creates a StreamReader with the given dependencies.
// windowSize controls the size of each CDN fetch window (e.g. 16 MiB).
func NewStreamReader(rc *cache.RangeCache, cdn *CDNClient, maxInflight int, windowSize int64, permalinkFor PermalinkBuilder, m *metrics.Metrics) *StreamReader {
	return &StreamReader{
		cache:        rc,
		cdn:          cdn,
		windowSize:   windowSize,
		maxInflight:  maxInflight,
		permalinkFor: permalinkFor,
		metrics:      m,
	}
}

// ReadAt reads len(p) bytes from offset off for the given fileKey.
// It loops across window boundaries to fill p completely, avoiding short reads
// that FUSE interprets as EOF. Returns io.EOF only when the read reaches or
// exceeds fileSize.
func (sr *StreamReader) ReadAt(ctx context.Context, fileKey string, off int64, p []byte, fileSize int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if sr.metrics != nil {
		sr.metrics.ReadCount.Add(1)
	}

	// Check for seek cancellation once at the start — not inside the loop.
	// This avoids triggering panicky cancellation when ReadAt loops across
	// window boundaries or when multiple FUSE reads access different regions
	// of the same file concurrently (e.g. Plex reading header + EOF probe).
	sr.maybeCancelOnSeek(fileKey, off)

	var totalN int
	curOff := off
	rem := p

	for len(rem) > 0 {
		// Don't read past the file end.
		if curOff >= fileSize {
			return totalN, io.EOF
		}

		// Clamp the read size to the file size to avoid requesting bytes past EOF.
		maxRead := int(fileSize - curOff)
		if len(rem) > maxRead {
			rem = rem[:maxRead]
		}

		n, err := sr.readWindow(ctx, fileKey, curOff, rem, fileSize)
		totalN += n
		if err != nil {
			return totalN, err
		}
		if n == 0 {
			// No progress — shouldn't happen, but bail to avoid infinite loop.
			break
		}
		curOff += int64(n)
		rem = rem[n:]
	}

	if curOff >= fileSize {
		return totalN, io.EOF
	}
	return totalN, nil
}

// readWindow reads up to len(p) bytes from a single window at the given offset.
// It may return fewer bytes than requested if the available data in the current
// window is less than len(p). The caller (ReadAt) handles spanning multiple
// windows.
func (sr *StreamReader) readWindow(ctx context.Context, fileKey string, off int64, p []byte, fileSize int64) (int, error) {
	// Try cache first — zero-alloc hot path.
	if n, ok := sr.cache.CopyTo(fileKey, off, p); ok {
		slog.Debug("stream read cache hit", "fileKey", fileKey, "offset", off, "size", len(p), "n", n)
		if sr.metrics != nil {
			sr.metrics.CacheHitCount.Add(1)
		}
		// Trigger read-ahead on cache hits too — sequential playback
		// typically hits cached data, and skipping read-ahead here means
		// the next window is never prefetched.
		ws := sr.windowStart(off)
		sess := sr.getOrCreateSession(fileKey)
		sr.maybeReadAhead(fileKey, off+int64(n), ws, fileSize, sess)
		return n, nil
	}

	slog.Debug("stream read cache miss", "fileKey", fileKey, "offset", off, "size", len(p))

	// Determine which window this read falls in.
	ws := sr.windowStart(off)

	// Get or create the file session (for lastSeek tracking).
	sess := sr.getOrCreateSession(fileKey)

	// Find or create an inflight window.
	win, created := sr.getOrCreateWindow(fileKey, ws, fileSize)

	if sr.metrics != nil {
		if created {
			sr.metrics.StreamMissCount.Add(1)
		} else {
			sr.metrics.StreamJoinCount.Add(1)
		}
	}

	// Register as a waiter so seek cancellation won't cancel this window
	// while we're using it.
	win.waiters.Add(1)

	// Wait until the requested bytes are ready (early return).
	n, err := sr.waitForBytes(ctx, win, off, p)

	win.waiters.Add(-1)

	if err != nil {
		slog.Debug("stream read error", "fileKey", fileKey, "offset", off, "err", err)
		return n, err
	}

	// Trigger read-ahead if conditions are met.
	sr.maybeReadAhead(fileKey, off+int64(n), ws, fileSize, sess)

	return n, nil
}

// windowStart returns the start offset of the window containing off.
func (sr *StreamReader) windowStart(off int64) int64 {
	return (off / sr.windowSize) * sr.windowSize
}

// maybeCancelOnSeek checks if this read represents a far seek (>4 * windowSize
// away from ALL inflight windows for the same file). If so, it cancels inflight
// windows that are far from the new read position to save bandwidth.
// It does NOT evict cached data — other concurrent readers may still need it.
func (sr *StreamReader) maybeCancelOnSeek(fileKey string, off int64) {
	seekThreshold := sr.windowSize * 4
	farSeek := true

	// Check if this offset is close to ANY inflight window for this file.
	// If it's near an existing window, this is not a seek — it's a parallel read.
	sr.inflight.Range(func(key, value any) bool {
		ik := key.(inflightKey)
		if ik.fileKey != fileKey {
			return true
		}
		distance := off - ik.start
		if distance < 0 {
			distance = -distance
		}
		if distance <= seekThreshold {
			farSeek = false
			return false // stop iterating — close window found
		}
		return true
	})

	if !farSeek {
		return
	}

	sess := sr.getOrCreateSession(fileKey)

	// Cancel orphaned inflight windows far from the new read position.
	// Only cancel windows with waiters == 0 — these have no active readers
	// (e.g. read-ahead prefetch data that no one asked for).
	cancelled := false
	sr.inflight.Range(func(key, value any) bool {
		ik := key.(inflightKey)
		if ik.fileKey != fileKey {
			return true
		}
		win := value.(*inflightWindow)
		distance := off - ik.start
		if distance < 0 {
			distance = -distance
		}
		if distance > seekThreshold && win.waiters.Load() == 0 {
			win.cancelFunc()
			cancelled = true
			if sr.metrics != nil {
				sr.metrics.InflightWindows.Add(-1)
			}
		}
		return true
	})

	// Only suppress read-ahead if we actually cancelled something.
	// A far seek detection alone must NOT suppress read-ahead — Plex reads
	// from multiple regions (header, EOF probe) and every ReadAt triggers
	// farSeek, which would permanently disable read-ahead otherwise.
	if cancelled {
		if sr.metrics != nil {
			sr.metrics.CancelledStreamCount.Add(1)
		}
		sess.lastSeek.Store(time.Now().UnixNano())
	}

	// Do NOT evict cached data — concurrent readers may still need it.
	// Cache eviction is handled by the LRU budget mechanism.
}

// getOrCreateSession returns the file session for the given fileKey,
// creating one if it doesn't exist. The session tracks lastSeek for
// read-ahead suppression.
func (sr *StreamReader) getOrCreateSession(fileKey string) *fileSession {
	if v, ok := sr.sessions.Load(fileKey); ok {
		return v.(*fileSession)
	}
	sess := &fileSession{}
	actual, _ := sr.sessions.LoadOrStore(fileKey, sess)
	return actual.(*fileSession)
}

// getOrCreateWindow finds an existing inflight window or creates a new one.
// It returns the window and whether a new one was created (true) or an
// existing one was found (false).
func (sr *StreamReader) getOrCreateWindow(fileKey string, ws int64, fileSize int64) (*inflightWindow, bool) {
	ik := inflightKey{fileKey: fileKey, start: ws}

	if v, ok := sr.inflight.Load(ik); ok {
		win := v.(*inflightWindow)
		if win.done.Load() && win.err != nil {
			// Errored window with all retries exhausted — remove it so a
			// fresh window can be created. The cleanup goroutine may not have
			// run yet, so we eagerly delete here.
			sr.inflight.Delete(ik)
			if sr.metrics != nil {
				sr.metrics.InflightWindows.Add(-1)
			}
		} else {
			return win, false
		}
	}

	// Create new inflight window.
	// buf is set by fetchWindow once the CDN response arrives — readers wait
	// on readyCond until readyTo >= their needed offset, so nil buf is safe
	// until data arrives.
	wctx, cancel := context.WithCancel(context.Background())
	win := &inflightWindow{
		key:        ik,
		readyCond:  sync.NewCond(&sync.Mutex{}),
		cancelFunc: cancel,
		fileSize:   fileSize,
	}

	actual, loaded := sr.inflight.LoadOrStore(ik, win)
	if loaded {
		// Another goroutine created the window first.
		cancel() // cancel our unused context
		return actual.(*inflightWindow), false
	}

	// Start the CDN fetch in a goroutine.
	go sr.fetchWindow(wctx, fileKey, ws, win)

	if sr.metrics != nil {
		sr.metrics.InflightWindows.Add(1)
	}

	return win, true
}

// waitForBytes waits until the requested bytes are available in the window
// and copies them into p. Returns the number of bytes copied.
// Supports early return: as soon as readyTo >= off + needed, we return.
func (sr *StreamReader) waitForBytes(ctx context.Context, win *inflightWindow, off int64, p []byte) (int, error) {
	winStart := win.key.start
	offInWin := off - winStart
	needed := offInWin + int64(len(p))

	win.readyCond.L.Lock()
	for {
		// Check if caller context was cancelled.
		select {
		case <-ctx.Done():
			win.readyCond.L.Unlock()
			return 0, ctx.Err()
		default:
		}

		ready := win.readyTo.Load()
		if ready >= needed {
			break
		}
		if win.done.Load() {
			break
		}
		win.readyCond.Wait()
	}

	// Lock is held here (cond.Wait reacquires before returning).
	// fetchWindow modifies buf/err/total under this same lock, so reading
	// them while holding the lock avoids the data race.
	if win.err != nil {
		win.readyCond.L.Unlock()
		return 0, fmt.Errorf("stream window fetch: %w", win.err)
	}

	// Copy available bytes from window buffer.
	avail := win.readyTo.Load() - offInWin
	if avail <= 0 {
		win.readyCond.L.Unlock()
		return 0, fmt.Errorf("stream window: no data available at offset %d", off)
	}
	if avail > int64(len(p)) {
		avail = int64(len(p))
	}
	copy(p, win.buf[offInWin:offInWin+avail])
	win.readyCond.L.Unlock()
	return int(avail), nil
}

// fetchWindow performs the CDN fetch for an inflight window, reading the
// response body chunk-by-chunk and updating readyTo as data arrives.
// It retries retryable errors (429, 5xx) with exponential backoff,
// transparently to waiting readers.
//
// Synchronization: buf and err are modified under readyCond.L so that
// waitForBytes can safely read them while holding the same lock.
func (sr *StreamReader) fetchWindow(ctx context.Context, fileKey string, winStart int64, win *inflightWindow) {
	defer func() {
		win.readyCond.L.Lock()
		win.done.Store(true)
		win.readyCond.Broadcast()
		win.readyCond.L.Unlock()
	}()

	url := sr.permalinkFor(fileKey)
	winEnd := winStart + sr.windowSize - 1

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Reset partial data from previous attempt.
			win.readyCond.L.Lock()
			win.buf = nil
			win.total = 0
			win.readyTo.Store(0)
			win.readyCond.Broadcast() // wake waiters so they re-check
			win.readyCond.L.Unlock()

			backoff := time.Duration(1<<uint(attempt-1)) * time.Second // 1s, 2s, 4s
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			slog.Warn("fetch window retry", "fileKey", fileKey, "start", winStart, "attempt", attempt, "backoff", backoff)
			select {
			case <-ctx.Done():
				win.readyCond.L.Lock()
				win.err = ctx.Err()
				win.readyCond.Broadcast()
				win.readyCond.L.Unlock()
				return
			case <-time.After(backoff):
			}
		}

		slog.Debug("fetching window", "fileKey", fileKey, "start", winStart, "end", winEnd, "url", url)

		resp, err := sr.cdn.FetchRange(ctx, url, winStart, winEnd)
		if err != nil {
			slog.Warn("fetch window error", "fileKey", fileKey, "start", winStart, "err", err)
			lastErr = err
			if isRetryable(err) && attempt < maxRetries {
				continue
			}
			win.readyCond.L.Lock()
			win.err = err
			win.readyCond.Broadcast()
			win.readyCond.L.Unlock()
			return
		}

		// Read response body chunk-by-chunk, updating readyTo as data arrives
		// so readers get early return. Lock held only during buf mutations
		// and Broadcast — released between chunks so readers can proceed.
		win.readyCond.L.Lock()
		win.buf = make([]byte, 0, sr.windowSize)
		win.readyCond.L.Unlock()
		chunk := make([]byte, readChunkSize)
		success := true
		for {
			n, readErr := resp.Body.Read(chunk)
			if n > 0 {
				win.readyCond.L.Lock()
				win.buf = append(win.buf, chunk[:n]...)
				win.total = int64(len(win.buf))
				win.readyTo.Store(win.total)
				win.readyCond.Broadcast()
				win.readyCond.L.Unlock()
			}
			if readErr != nil {
				if readErr == io.EOF {
					break
				}
				slog.Warn("fetch window read error", "fileKey", fileKey, "start", winStart, "err", readErr)
				lastErr = readErr
				success = false
				break
			}
		}
		resp.Body.Close()

		win.readyCond.L.Lock()
		bufLen := len(win.buf)
		win.readyCond.L.Unlock()

		if success && bufLen > 0 {
			slog.Debug("fetch window complete", "fileKey", fileKey, "start", winStart, "bytes", bufLen)

			// Store in cache (no session tag -- cache data is never evicted by seek cancellation,
			// only by LRU budget eviction).
			win.readyCond.L.Lock()
			cachedBuf := win.buf
			win.readyCond.L.Unlock()
			sr.cache.Put(fileKey, winStart, cachedBuf)

			// Mark window as done before triggering read-ahead so that the
			// inflight count check excludes this completed window.
			win.readyCond.L.Lock()
			win.done.Store(true)
			win.readyCond.L.Unlock()

			// Trigger read-ahead of the next window now that this window's data is
			// cached and available.
			sess := sr.getOrCreateSession(fileKey)
			sr.maybeReadAhead(fileKey, winStart+sr.windowSize, winStart, win.fileSize, sess)

			// Remove from inflight map after a short delay to allow late joiners.
			// The window data is already in the cache for future reads.
			go sr.cleanupWindow(win)

			return
		}

		// Retry if retryable.
		if !isRetryable(lastErr) || attempt >= maxRetries {
			win.readyCond.L.Lock()
			win.err = lastErr
			win.readyCond.Broadcast()
			win.readyCond.L.Unlock()
			return
		}
	}

	win.readyCond.L.Lock()
	win.err = lastErr
	win.readyCond.Broadcast()
	win.readyCond.L.Unlock()

	// Errored window -- remove after 5s to allow future reads to retry.
	go sr.cleanupWindowOnError(win)
}

// cleanupWindow removes a completed (successful) inflight window from the map
// after a short delay to allow late joiners.
func (sr *StreamReader) cleanupWindow(win *inflightWindow) {
	time.Sleep(100 * time.Millisecond)
	// Only delete if this window is still registered (not already replaced).
	if v, ok := sr.inflight.Load(win.key); ok && v.(*inflightWindow) == win {
		sr.inflight.Delete(win.key)
		if sr.metrics != nil {
			sr.metrics.InflightWindows.Add(-1)
		}
	}
	// Clean up session if no inflight windows remain for this file.
	sr.maybeCleanupSession(win.key.fileKey)
}

// cleanupWindowOnError removes an errored inflight window from the map after
// a delay, allowing future reads to retry with a fresh window.
func (sr *StreamReader) cleanupWindowOnError(win *inflightWindow) {
	time.Sleep(5 * time.Second)
	if v, ok := sr.inflight.Load(win.key); ok && v.(*inflightWindow) == win {
		sr.inflight.Delete(win.key)
		if sr.metrics != nil {
			sr.metrics.InflightWindows.Add(-1)
		}
	}
	sr.maybeCleanupSession(win.key.fileKey)
}

// maybeCleanupSession removes the file session if no inflight windows remain.
func (sr *StreamReader) maybeCleanupSession(fileKey string) {
	hasInflight := false
	sr.inflight.Range(func(key, value any) bool {
		if key.(inflightKey).fileKey == fileKey {
			hasInflight = true
			return false
		}
		return true
	})
	if !hasInflight {
		sr.sessions.Delete(fileKey)
	}
}

// isRetryable returns true for errors that may succeed on retry:
// 429 (rate limit), 5xx (server errors), and network timeouts.
// context.Canceled is NOT retryable — it's an explicit cancellation.
func isRetryable(err error) bool {
	var hse *HTTPStatusError
	if errors.As(err, &hse) {
		return hse.StatusCode == 429 || (hse.StatusCode >= 500 && hse.StatusCode < 600)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	// Unknown errors (network failures, DNS, etc.) are retryable.
	return true
}

// maybeReadAhead triggers a prefetch of the next window if conditions are met:
//   - The read end offset is at least windowSize/2 into the current window
//   - The next window is not cached and not already inflight
//   - Per-file inflight count < maxInflight
//   - No recent far seek for this file
func (sr *StreamReader) maybeReadAhead(fileKey string, endOff, winStart, fileSize int64, sess *fileSession) {
	nextStart := winStart + sr.windowSize

	// Don't prefetch beyond EOF — there's nothing to fetch.
	if nextStart >= fileSize {
		return
	}

	// Check: read extends past readAheadThreshold into the current window.
	readAheadThreshold := sr.windowSize / 2
	if endOff < winStart+readAheadThreshold {
		return
	}

	// Check: no recent far seek (within last 2 seconds).
	if time.Since(time.Unix(0, sess.lastSeek.Load())) < 2*time.Second {
		return
	}

	// Check: next window not already cached.
	nextBuf := make([]byte, 1)
	if _, ok := sr.cache.CopyTo(fileKey, nextStart, nextBuf); ok {
		return
	}

	// Check: next window not already inflight.
	nextKey := inflightKey{fileKey: fileKey, start: nextStart}
	if _, ok := sr.inflight.Load(nextKey); ok {
		return
	}

	// Check: per-file active inflight count < maxInflight.
	// Completed windows (done=true, waiting before removal) don't count
	// because their data is already in the cache and they're not consuming
	// CDN bandwidth.
	inflightCount := 0
	sr.inflight.Range(func(key, value any) bool {
		ik := key.(inflightKey)
		if ik.fileKey == fileKey && !value.(*inflightWindow).done.Load() {
			inflightCount++
		}
		return true
	})
	if inflightCount >= sr.maxInflight {
		return
	}

	// Create read-ahead window.
	_, _ = sr.getOrCreateWindow(fileKey, nextStart, fileSize)
}