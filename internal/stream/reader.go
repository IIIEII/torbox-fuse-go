// Package stream provides the CDN range client and StreamReader for the
// TorBox FUSE streaming hot path.
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

	// smallReadThreshold is the maximum request size (in bytes) that qualifies
	// as a "small read" — metadata scans and EOF probes from media players.
	// Reads at or below this threshold may bypass the window system and fetch
	// only the requested bytes directly from CDN.
	smallReadThreshold = 256 * 1024 // 256 KiB

	// sequentialReadWindow defines how far back from the last read offset a
	// new read must fall to be considered sequential. Reads within this
	// distance of the previous read are counted toward the sequential streak.
	sequentialReadWindow = 4 * 1024 * 1024 // 4 MiB

	// sequentialReadsRequired is the number of sequential reads needed before
	// read-ahead kicks in. Set to 1 so that the first sequential read (from
	// offset 0) enables read-ahead, while random metadata scans that jump
	// around never accumulate enough sequential streaks.
	sequentialReadsRequired = 1
)

// PermalinkBuilder returns the CDN URL for a given fileKey.
type PermalinkBuilder func(fileKey string) string

// StreamReader manages inflight windows and delegates to the cache and CDN client.
// It supports early return (readers return as soon as their requested bytes are
// ready, not when the full window completes), read-ahead of the next window,
// and seek cancellation for far seeks.
type StreamReader struct {
	cache        *cache.RangeCache
	cdn          *CDNClient
	inflight     sync.Map // inflightKey -> *inflightWindow
	sessions     sync.Map // fileKey -> *fileSession
	windowSize   int64
	maxInflight  int
	globalBudget chan struct{} // limits total inflight windows across all files
	permalinkFor PermalinkBuilder
	metrics      *metrics.Metrics
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

// fileSession tracks per-file read patterns for read-ahead decisions.
type fileSession struct {
	lastSeek          atomic.Int64  // nano timestamp of last far-seek
	lastReadOff       atomic.Int64  // offset of last ReadAt call
	sequentialReads    atomic.Int32  // count of sequential reads (resets on seek/random)
	randomReads       atomic.Int32  // count of random-access reads (for small-read eligibility)
}

// NewStreamReader creates a StreamReader with the given dependencies.
// windowSize controls the size of each CDN fetch window (e.g. 16 MiB).
// maxGlobalWindows limits the total number of inflight windows across all files,
// capping inflight buffer memory at maxGlobalWindows x windowSize.
func NewStreamReader(rc *cache.RangeCache, cdn *CDNClient, maxInflight, maxGlobalWindows int, windowSize int64, permalinkFor PermalinkBuilder, m *metrics.Metrics) *StreamReader {
	return &StreamReader{
		cache:        rc,
		cdn:          cdn,
		windowSize:   windowSize,
		maxInflight:  maxInflight,
		globalBudget: make(chan struct{}, maxGlobalWindows),
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

	// Track access pattern for read-ahead decisions.
	sess := sr.getOrCreateSession(fileKey)
	sr.updateAccessPattern(fileKey, off, int64(len(p)), sess)

	// Small reads (metadata scans, EOF probes) from random-access patterns
	// bypass the window system to avoid fetching 16 MiB for a 64 KiB request.
	// The result is still cached for future reuse.
	if int64(len(p)) <= smallReadThreshold && sess.randomReads.Load() > 0 && !isSequential(sess) {
		if n, err := sr.smallRead(ctx, fileKey, off, p, fileSize); err == nil {
			return n, err
		}
		// Fall through to window path if small read fails — it may succeed
		// via the normal window mechanism (e.g. if data is in an inflight window).
	}

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

// smallRead handles small requests (≤ smallReadThreshold) for random-access
// patterns by issuing a direct CDN range request for exactly the needed bytes.
// The result is stored in the cache for future reuse. Returns (n, nil) on
// success, or (0, err) on failure — the caller should fall through to the
// window path.
func (sr *StreamReader) smallRead(ctx context.Context, fileKey string, off int64, p []byte, fileSize int64) (int, error) {
	// Try cache first — might already have this data from a window fetch.
	if n, ok := sr.cache.CopyTo(fileKey, off, p); ok {
		slog.Debug("small read cache hit", "fileKey", fileKey, "offset", off, "size", len(p), "n", n)
		if sr.metrics != nil {
			sr.metrics.CacheHitCount.Add(1)
		}
		return n, nil
	}

	// Check if there's already an inflight window covering this offset —
	// if so, it's better to join the window than to issue a separate request.
	ws := sr.windowStart(off)
	if _, ok := sr.inflight.Load(inflightKey{fileKey: fileKey, start: ws}); ok {
		return 0, fmt.Errorf("inflight window exists")
	}

	end := off + int64(len(p)) - 1
	if end >= fileSize {
		end = fileSize - 1
	}

	url := sr.permalinkFor(fileKey)
	resp, err := sr.cdn.FetchRange(ctx, url, off, end)
	if err != nil {
		slog.Debug("small read fetch error", "fileKey", fileKey, "offset", off, "err", err)
		return 0, err
	}
	defer resp.Body.Close()

	data := make([]byte, int64(len(p)))
	n, err := io.ReadFull(resp.Body, data)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return 0, fmt.Errorf("small read body: %w", err)
	}
	data = data[:n]

	if n > 0 {
		sr.cache.Put(fileKey, off, data)
		copy(p, data)
	}

	slog.Debug("small read complete", "fileKey", fileKey, "offset", off, "size", n)
	return n, nil
}

// updateAccessPattern tracks whether reads are sequential or random-access.
// Sequential reads increment the streak counter; random reads reset it and
// increment the random counter. Read-ahead only activates after
// sequentialReadsRequired sequential reads. Small-read bypass only activates
// after at least one random-access read is observed.
func (sr *StreamReader) updateAccessPattern(fileKey string, off, size int64, sess *fileSession) {
	lastOff := sess.lastReadOff.Load()
	sess.lastReadOff.Store(off + size)

	// First read for this file — assume sequential (start of playback).
	// This ensures read-ahead activates after the second sequential read,
	// rather than requiring three reads to kick in.
	if lastOff == 0 {
		if off == 0 {
			sess.sequentialReads.Add(1)
		}
		return
	}

	// Sequential: current offset is within sequentialReadWindow of the previous end.
	if off >= lastOff-sequentialReadWindow && off <= lastOff+sequentialReadWindow {
		sess.sequentialReads.Add(1)
	} else {
		// Random access (far seek, metadata scan) — reset streak.
		sess.sequentialReads.Store(0)
		sess.randomReads.Add(1)
	}
}

// isSequential returns true if the file access pattern is sequential enough
// to warrant read-ahead (at least sequentialReadsRequired sequential reads).
func isSequential(sess *fileSession) bool {
	return sess.sequentialReads.Load() >= sequentialReadsRequired
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
	sess := sr.getOrCreateSession(fileKey)
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

	// Cancel completed read-ahead windows far from the new read position.
	// Only cancel windows that are done (data already cached) with waiters == 0.
	// In-progress windows (!done) must NOT be cancelled — their data is still
	// being streamed from CDN and will be needed by future reads. Canceling an
	// in-flight window wastes the CDN bandwidth already spent and forces a fresh
	// round-trip when the player returns to that range.
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
		if distance > seekThreshold && win.done.Load() && win.waiters.Load() == 0 {
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
// creating one if it doesn't exist. The session tracks access pattern
// and last-seek for read-ahead suppression.
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
// Before fetching from CDN, it checks the cache for a contiguous prefix starting
// at winStart. If found, only the missing suffix is fetched from CDN, saving
// bandwidth on re-fetches after interrupted or cancelled windows.
//
// Synchronization: buf and err are modified under readyCond.L so that
// waitForBytes can safely read them while holding the same lock.
func (sr *StreamReader) fetchWindow(ctx context.Context, fileKey string, winStart int64, win *inflightWindow) {
	// Acquire global budget slot — blocks if too many windows are inflight.
	// This prevents OOM when many files are opened simultaneously.
	select {
	case sr.globalBudget <- struct{}{}:
	case <-ctx.Done():
		win.readyCond.L.Lock()
		win.err = ctx.Err()
		win.done.Store(true)
		win.readyCond.Broadcast()
		win.readyCond.L.Unlock()
		go sr.cleanupWindowOnError(win)
		return
	}

	defer func() {
		<-sr.globalBudget
		win.readyCond.L.Lock()
		win.done.Store(true)
		win.readyCond.Broadcast()
		win.readyCond.L.Unlock()
	}()

	url := sr.permalinkFor(fileKey)
	winEnd := winStart + sr.windowSize
	if winEnd > win.fileSize {
		winEnd = win.fileSize
	}

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

		// Check cache for a contiguous prefix starting at winStart.
		// If partial data was cached from a previous interrupted fetch,
		// we only need to fetch the missing suffix from CDN.
		var cachedLen int64
		if cl, ok := sr.cache.CachedPrefixLen(fileKey, winStart); ok && cl > 0 {
			cachedLen = cl
		}

		windowLen := winEnd - winStart

		// If the entire window is already cached, copy from cache and we're done.
		// This shouldn't normally happen (readWindow would have hit the cache),
		// but handle it gracefully.
		if cachedLen >= windowLen {
			win.readyCond.L.Lock()
			win.buf = make([]byte, windowLen)
			n, _ := sr.cache.CopyTo(fileKey, winStart, win.buf)
			win.total = int64(n)
			win.readyTo.Store(win.total)
			win.readyCond.Broadcast()
			win.readyCond.L.Unlock()
			sr.cache.Put(fileKey, winStart, win.buf)
			win.readyCond.L.Lock()
			win.done.Store(true)
			win.readyCond.L.Unlock()
			sess := sr.getOrCreateSession(fileKey)
			sr.maybeReadAhead(fileKey, winStart+sr.windowSize, winStart, win.fileSize, sess)
			go sr.cleanupWindow(win)
			return
		}

		// Determine CDN fetch range: skip the cached prefix if present.
		fetchStart := winStart + cachedLen
		fetchEnd := winEnd - 1 // FetchRange uses inclusive end

		// Prepare the buffer: pre-fill cached prefix if present.
		win.readyCond.L.Lock()
		if cachedLen > 0 {
			win.buf = make([]byte, cachedLen)
			n, _ := sr.cache.CopyTo(fileKey, winStart, win.buf)
			win.buf = win.buf[:n]
			win.total = int64(len(win.buf))
			win.readyTo.Store(win.total)
			win.readyCond.Broadcast()
		} else {
			win.buf = make([]byte, 0, sr.windowSize)
		}
		win.readyCond.L.Unlock()

		if cachedLen > 0 {
			slog.Debug("cache prefix hit", "fileKey", fileKey, "start", winStart, "cachedLen", cachedLen, "fetchStart", fetchStart)
		}

		slog.Debug("fetching window", "fileKey", fileKey, "start", winStart, "end", winEnd, "fetchStart", fetchStart, "url", url)

		resp, err := sr.cdn.FetchRange(ctx, url, fetchStart, fetchEnd)
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

		// Stream CDN response body into win.buf starting after the cached prefix.
		fetched, success := sr.streamInto(resp, cachedLen, win, fileKey, winStart)
		resp.Body.Close()

		win.readyCond.L.Lock()
		bufLen := len(win.buf)
		win.readyCond.L.Unlock()

		if bufLen > 0 {
			win.readyCond.L.Lock()
			cachedBuf := win.buf
			win.readyCond.L.Unlock()
			sr.cache.Put(fileKey, winStart, cachedBuf)
		}

		if success && bufLen > 0 {
			slog.Debug("fetch window complete", "fileKey", fileKey, "start", winStart, "bytes", bufLen, "cachedPrefix", cachedLen)

			win.readyCond.L.Lock()
			win.done.Store(true)
			win.readyCond.L.Unlock()

			sess := sr.getOrCreateSession(fileKey)
			sr.maybeReadAhead(fileKey, winStart+sr.windowSize, winStart, win.fileSize, sess)
			go sr.cleanupWindow(win)
			return
		}

		if !success {
			lastErr = fmt.Errorf("stream read failed after %d bytes", fetched)
		} else {
			lastErr = fmt.Errorf("no data received")
		}
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

	go sr.cleanupWindowOnError(win)
}

// streamInto reads the CDN response body into win.buf, appending chunks starting
// at the given buffer offset (bufOff). It updates readyTo as data arrives and
// broadcasts to wake waiting readers. After each chunk, it checks whether all
// waiters have left (waiters == 0) and if so, cancels the remaining fetch to
// avoid downloading data that nobody needs. Returns (bytesRead, success).
func (sr *StreamReader) streamInto(resp *http.Response, bufOff int64, win *inflightWindow, fileKey string, winStart int64) (int64, bool) {
	chunk := make([]byte, readChunkSize)
	var totalRead int64
	for {
		n, readErr := resp.Body.Read(chunk)
		if n > 0 {
			win.readyCond.L.Lock()
			win.buf = append(win.buf, chunk[:n]...)
			win.total = int64(len(win.buf))
			win.readyTo.Store(bufOff + totalRead + int64(n))
			win.readyCond.Broadcast()
			win.readyCond.L.Unlock()
			totalRead += int64(n)
		}
		if readErr != nil {
			if readErr == io.EOF {
				return totalRead, true
			}
			slog.Warn("stream read error", "fileKey", fileKey, "start", winStart, "err", readErr)
			return totalRead, false
		}

	}
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
//   - The access pattern is sequential (at least sequentialReadsRequired
//     sequential reads)
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

	// Only prefetch for sequential access patterns. Random access (metadata
	// scans, EOF probes) doesn't benefit from read-ahead and wastes bandwidth.
	if !isSequential(sess) {
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