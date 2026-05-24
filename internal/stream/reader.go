// Package stream provides the CDN range HTTP client and StreamReader that
// manages inflight windows, early return, read-ahead, and seek cancellation
// for the TorBox FUSE filesystem streaming hot path.
package stream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
)

const (
	// windowSize is the size of each inflight fetch window (4 MiB).
	windowSize int64 = 4 * 1024 * 1024

	// seekThreshold is the distance in bytes that triggers seek cancellation.
	// A read more than 16 MiB away from any inflight window for the same file
	// increments the session ID and cancels stale windows.
	seekThreshold int64 = 16 * 1024 * 1024

	// readAheadThreshold is how far into the current window the read offset
	// must be before read-ahead is triggered.
	readAheadThreshold int64 = 4 * 1024 * 1024
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
	maxInflight   int
	prefetchBytes int64
	permalinkFor  PermalinkBuilder
}

// inflightKey identifies an inflight window by file key and window start offset.
type inflightKey struct {
	fileKey string
	start   int64
}

// inflightWindow tracks an in-progress CDN fetch for a single 4 MiB window.
// Readers wait on readyCond until their requested bytes are available (early return).
//
// Synchronization guarantee: err is written by fetchWindow before done.Broadcast()
// and read by waitForBytes after done.Wait() returns, establishing a happens-before.
// No additional locking is needed for err because the Broadcast/Wait pair provides
// the necessary memory ordering.
type inflightWindow struct {
	key        inflightKey
	sessionID  int64
	buf        []byte
	readyTo    atomic.Int64
	total      int64
	err        error
	done       atomic.Bool
	readyCond  *sync.Cond
	cancelFunc context.CancelFunc
}

// fileSession tracks per-file session state for seek cancellation.
type fileSession struct {
	id       atomic.Int64
	lastSeek atomic.Int64
}

// NewStreamReader creates a StreamReader with the given dependencies.
func NewStreamReader(rc *cache.RangeCache, cdn *CDNClient, maxInflight int, prefetchBytes int64, permalinkFor PermalinkBuilder) *StreamReader {
	return &StreamReader{
		cache:         rc,
		cdn:           cdn,
		maxInflight:   maxInflight,
		prefetchBytes: prefetchBytes,
		permalinkFor:  permalinkFor,
	}
}

// ReadAt reads len(p) bytes from offset off for the given fileKey.
// It first checks the cache (zero-alloc hit via CopyTo). On miss, it finds or
// creates an inflight window, waits for the requested bytes to be ready
// (early return), and copies them into p.
// Returns io.EOF when the read reaches or exceeds fileSize.
func (sr *StreamReader) ReadAt(ctx context.Context, fileKey string, off int64, p []byte, fileSize int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Try cache first — zero-alloc hot path.
	if n, ok := sr.cache.CopyTo(fileKey, off, p); ok {
		return n, nil
	}

	// Determine which window this read falls in.
	ws := windowStart(off)

	// Check for seek cancellation — if the read is far from inflight windows.
	sr.maybeCancelOnSeek(fileKey, off)

	// Get or create the file session.
	sess := sr.getOrCreateSession(fileKey)

	// Find or create an inflight window.
	win := sr.getOrCreateWindow(fileKey, ws, sess.id.Load())

	// Wait until the requested bytes are ready (early return).
	n, err := sr.waitForBytes(ctx, win, off, p)
	if err != nil {
		return n, err
	}

	// Trigger read-ahead if conditions are met.
	sr.maybeReadAhead(fileKey, off, ws, sess)

	// Return EOF if we've reached the end of the file.
	if off+int64(n) >= fileSize {
		return n, io.EOF
	}
	return n, nil
}

// windowStart returns the start offset of the 4 MiB window containing off.
func windowStart(off int64) int64 {
	return (off / windowSize) * windowSize
}

// maybeCancelOnSeek checks if this read represents a far seek (>16 MiB away
// from any inflight window for the same file). If so, it increments the
// session ID, cancels stale windows, and evicts stale cache data.
func (sr *StreamReader) maybeCancelOnSeek(fileKey string, off int64) {
	farSeek := false

	// Check distance from all inflight windows for this file.
	sr.inflight.Range(func(key, value any) bool {
		ik := key.(inflightKey)
		if ik.fileKey != fileKey {
			return true
		}
		distance := off - ik.start
		if distance < 0 {
			distance = -distance
		}
		if distance > seekThreshold {
			farSeek = true
			return false // stop iterating
		}
		return true
	})

	if !farSeek {
		return
	}

	// Increment session to invalidate stale windows.
	sess := sr.getOrCreateSession(fileKey)
	newID := sess.id.Add(1)
	sess.lastSeek.Store(time.Now().UnixNano())

	slog.Debug("seek cancellation",
		"fileKey", fileKey,
		"newSession", newID,
		"offset", off,
	)

	// Cancel stale inflight windows (session ID < newID).
	sr.inflight.Range(func(key, value any) bool {
		ik := key.(inflightKey)
		if ik.fileKey != fileKey {
			return true
		}
		win := value.(*inflightWindow)
		if win.sessionID < newID {
			win.cancelFunc()
		}
		return true
	})

	// Evict stale cache data.
	sr.cache.EvictStale(fileKey, newID)

	// Clean up session if no inflight windows remain for this file.
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

// getOrCreateSession returns the file session for the given fileKey,
// creating one if it doesn't exist.
func (sr *StreamReader) getOrCreateSession(fileKey string) *fileSession {
	if v, ok := sr.sessions.Load(fileKey); ok {
		return v.(*fileSession)
	}
	sess := &fileSession{}
	sess.id.Store(1) // start at 1 so 0 means "no session"
	actual, _ := sr.sessions.LoadOrStore(fileKey, sess)
	return actual.(*fileSession)
}

// getOrCreateWindow finds an existing inflight window or creates a new one.
func (sr *StreamReader) getOrCreateWindow(fileKey string, ws, sessionID int64) *inflightWindow {
	ik := inflightKey{fileKey: fileKey, start: ws}

	if v, ok := sr.inflight.Load(ik); ok {
		win := v.(*inflightWindow)
		if !win.done.Load() {
			return win
		}
		// Window completed with error — allow retry by removing it.
		if win.err != nil {
			sr.inflight.Delete(ik)
			// fall through to create new window
		} else {
			return win
		}
	}

	// Create new inflight window.
	wctx, cancel := context.WithCancel(context.Background())
	win := &inflightWindow{
		key:        ik,
		sessionID:  sessionID,
		buf:        make([]byte, windowSize),
		readyCond:  sync.NewCond(&sync.Mutex{}),
		cancelFunc: cancel,
	}

	actual, loaded := sr.inflight.LoadOrStore(ik, win)
	if loaded {
		// Another goroutine created the window first.
		cancel() // cancel our unused context
		return actual.(*inflightWindow)
	}

	// Start the CDN fetch in a goroutine.
	go sr.fetchWindow(wctx, fileKey, ws, win)

	return win
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
	win.readyCond.L.Unlock()

	if win.err != nil {
		return 0, fmt.Errorf("stream window fetch: %w", win.err)
	}

	// Copy available bytes from window buffer.
	avail := win.readyTo.Load() - offInWin
	if avail <= 0 {
		return 0, fmt.Errorf("stream window: no data available at offset %d", off)
	}
	if avail > int64(len(p)) {
		avail = int64(len(p))
	}
	copy(p, win.buf[offInWin:offInWin+avail])
	return int(avail), nil
}

// fetchWindow performs the CDN fetch for an inflight window and updates
// readyTo as data arrives, unblocking waiting readers via early return.
func (sr *StreamReader) fetchWindow(ctx context.Context, fileKey string, winStart int64, win *inflightWindow) {
	defer func() {
		win.done.Store(true)
		win.readyCond.Broadcast()
	}()

	url := sr.permalinkFor(fileKey)
	winEnd := winStart + windowSize - 1

	data, err := sr.cdn.FetchRange(ctx, url, winStart, winEnd)
	if err != nil {
		win.err = err
		return
	}

	win.total = int64(len(data))
	copy(win.buf, data)
	win.readyTo.Store(win.total)

	// Store in cache with session tag.
	sr.cache.PutWithSession(fileKey, winStart, data, win.sessionID)

	// Remove from inflight map after a short delay to allow late joiners.
	// The window data is already in the cache for future reads.
	go func() {
		time.Sleep(100 * time.Millisecond)
		// Only delete if this window is still registered (not already replaced).
		if v, ok := sr.inflight.Load(win.key); ok && v.(*inflightWindow) == win {
			sr.inflight.Delete(win.key)
		}
		// Clean up session if no inflight windows remain for this file.
		hasInflight := false
		sr.inflight.Range(func(key, value any) bool {
			if key.(inflightKey).fileKey == win.key.fileKey {
				hasInflight = true
				return false
			}
			return true
		})
		if !hasInflight {
			sr.sessions.Delete(win.key.fileKey)
		}
	}()
}

// maybeReadAhead triggers a prefetch of the next window if conditions are met:
//   - The read offset is at least 4 MiB into the current window
//   - The next window is not cached and not already inflight
//   - Per-file inflight count < maxInflight
//   - No recent far seek for this file
func (sr *StreamReader) maybeReadAhead(fileKey string, off, winStart int64, sess *fileSession) {
	nextStart := winStart + windowSize

	// Check: read is at least 4 MiB into the current window.
	if off < winStart+readAheadThreshold {
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

	// Check: per-file inflight count < maxInflight.
	inflightCount := 0
	sr.inflight.Range(func(key, _ any) bool {
		ik := key.(inflightKey)
		if ik.fileKey == fileKey {
			inflightCount++
		}
		return true
	})
	if inflightCount >= sr.maxInflight {
		return
	}

	// Create read-ahead window.
	sr.getOrCreateWindow(fileKey, nextStart, sess.id.Load())
}