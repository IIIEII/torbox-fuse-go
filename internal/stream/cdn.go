// Package stream provides the CDN range client and StreamReader for the
// TorBox FUSE streaming hot path.
package stream

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"golang.org/x/sync/singleflight"
)

// hostSem holds a per-CDN-host priority-aware concurrency semaphore.
// High-priority requests (playback) are served before low-priority ones (scan)
// when both are waiting for a slot. This prevents connection starvation:
// without priority, a library scan filling all slots causes playback to wait
// behind the entire FIFO queue.
//
// Implementation: sync.Cond-based queue where waiters are sorted by priority.
// When a slot is released, the highest-priority waiter is signaled.
type hostSem struct {
	mu      sync.Mutex
	cond    *sync.Cond
	limit   int // max concurrent holders
	holding int // current number of holders
	waiters []*semWaiter
}

// semWaiter represents a goroutine waiting for a semaphore slot.
type semWaiter struct {
	priority uint8 // 0=low, 1=high (higher value = higher priority)
	ready    bool  // set to true when this waiter has been granted a slot
}

// acquire blocks until a semaphore slot is available, respecting priority.
// Higher-priority requests are served first when multiple goroutines are waiting.
// Returns an error if ctx is cancelled while waiting.
func (hs *hostSem) acquire(ctx context.Context, priority uint8) error {
	hs.mu.Lock()

	// Fast path: slot available, no waiters to jump ahead of.
	if hs.holding < hs.limit && len(hs.waiters) == 0 {
		hs.holding++
		hs.mu.Unlock()
		return nil
	}

	// Slow path: enqueue and wait.
	w := &semWaiter{priority: priority}
	// Insert in priority order (high priority at front).
	insertPos := len(hs.waiters)
	for i, existing := range hs.waiters {
		if existing.priority < priority {
			insertPos = i
			break
		}
	}
	if insertPos == len(hs.waiters) {
		hs.waiters = append(hs.waiters, w)
	} else {
		hs.waiters = append(hs.waiters[:insertPos+1], hs.waiters[insertPos:]...)
		hs.waiters[insertPos] = w
	}

	// Wait loop: Cond.Wait requires hs.mu to be locked on entry.
	// It unlocks during wait and re-locks before returning.
	done := ctx.Done()
	for !w.ready {
		if err := ctx.Err(); err != nil {
			// Context cancelled — remove our waiter from the queue.
			for i, existing := range hs.waiters {
				if existing == w {
					hs.waiters = append(hs.waiters[:i], hs.waiters[i+1:]...)
					break
				}
			}
			hs.mu.Unlock()
			return err
		}
		hs.cond.Wait()
		// After Wait returns, hs.mu is held again. Check if context is done
		// before the next loop iteration checks w.ready.
		select {
		case <-done:
			if w.ready {
				// Granted just before cancellation — take the slot.
				hs.mu.Unlock()
				return nil
			}
			// Remove waiter and return error.
			for i, existing := range hs.waiters {
				if existing == w {
					hs.waiters = append(hs.waiters[:i], hs.waiters[i+1:]...)
					break
				}
			}
			hs.mu.Unlock()
			return ctx.Err()
		default:
		}
	}
	hs.mu.Unlock()
	return nil
}

// release returns a semaphore slot and wakes the highest-priority waiter.
func (hs *hostSem) release() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.holding--

	if len(hs.waiters) > 0 {
		// Grant slot to the highest-priority waiter (front of queue).
		w := hs.waiters[0]
		hs.waiters = hs.waiters[1:]
		w.ready = true
		hs.holding++
		hs.cond.Broadcast() // wake all waiters so the granted one can proceed
	}
}

// maxRedirects is the maximum number of HTTP redirects to follow.
const maxRedirects = 5

// HTTPStatusError represents an unexpected HTTP status code from the CDN.
type HTTPStatusError struct {
	StatusCode int
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("cdn returned status %d", e.StatusCode)
}

// cdnURLCacheEntry stores a resolved CDN URL or a failed resolution with timing.
// When failed is true, resolvedURL is empty and the entry represents a backoff
// period during which we should not retry resolution.
type cdnURLCacheEntry struct {
	resolvedURL string
	resolvedAt  time.Time
	failed      bool // true if resolution returned 429/5xx — backoff, don't retry
}

// defaultRateLimitCooldown is how long to wait before retrying a URL that returned 429.
// This prevents thundering-herd re-resolution storms when the API is rate-limiting
// (e.g. at container startup when 30+ files resolve their CDN URLs simultaneously).
const defaultRateLimitCooldown = 10 * time.Second

// CDNClient performs HTTP range requests to TorBox CDN URLs with per-host
// concurrency limits. Each CDN host gets its own semaphore, so requests to
// different hosts don't compete for the same slots. This maximizes parallelism
// when files are spread across multiple CDN servers (e.g. nexus-040, nexus-096,
// nexus-128, etc.).
// It caches API redirect resolutions so subsequent range requests go directly
// to the CDN URL instead of hitting the TorBox API every time.
// URL resolution uses singleflight to dedup concurrent requests for the same
// API URL, preventing thundering-herd API rate limiting.
// Rate-limited URLs (429) are blacklisted for rateLimitCooldown to avoid
// overwhelming the API with retries during startup or burst scenarios.
type CDNClient struct {
	client            *http.Client
	maxConnsPerHost   int
	hostSems          sync.Map // string -> *hostSem - per-CDN-host semaphore
	metrics           *metrics.Metrics
	urlCache          sync.Map // apiURL string -> *cdnURLCacheEntry
	urlCacheTTL       time.Duration
	rateLimitCooldown time.Duration      // backoff for 429/5xx URL resolution failures
	resolveGroup      singleflight.Group // dedup parallel URL resolution requests
}

// NewCDNClient creates a CDNClient that allows at most maxConns concurrent
// range requests per CDN host. The underlying HTTP transport is tuned for
// streaming: compression disabled, idle connections kept warm.
//
// urlCacheTTL controls how long resolved CDN URLs are cached. Set to 0 to
// disable caching (every request hits the API). Typical value: 5m.
//
// The client handles redirects manually to preserve the Range header across
// redirects. Go's default http.Client drops Range on cross-origin 301/302.
func NewCDNClient(maxConns int, m *metrics.Metrics, urlCacheTTL time.Duration) *CDNClient {
	return &CDNClient{
		client: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse // handle redirects manually
			},
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				MaxConnsPerHost:     16,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},
		maxConnsPerHost:   maxConns,
		metrics:           m,
		urlCacheTTL:       urlCacheTTL,
		rateLimitCooldown: defaultRateLimitCooldown,
	}
}

// SetRateLimitCooldown sets the backoff duration for failed URL resolution
// (429/5xx). Intended for tests; production code uses the default.
func (c *CDNClient) SetRateLimitCooldown(d time.Duration) {
	c.rateLimitCooldown = d
}

// getHostSem returns the per-host semaphore for the given host, creating one
// lazily if needed.
func (c *CDNClient) getHostSem(host string) *hostSem {
	if v, ok := c.hostSems.Load(host); ok {
		return v.(*hostSem)
	}
	hs := &hostSem{limit: c.maxConnsPerHost}
	hs.cond = sync.NewCond(&hs.mu)
	actual, _ := c.hostSems.LoadOrStore(host, hs)
	return actual.(*hostSem)
}

// hostFromURL extracts the host (with port if non-standard) from a URL.
// Falls back to the raw URL on parse error.
func hostFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return rawURL
	}
	return u.Host
}

// ResolveURL resolves an API redirect URL to the direct CDN URL. It caches
// successful resolutions for urlCacheTTL and failed resolutions (429/5xx)
// for rateLimitBackoff. This prevents thundering-herd API requests when the
// container starts and Plex opens 30+ files simultaneously — the first 429
// is cached as a negative result, and subsequent requests for the same URL
// fail fast without hammering the API until the backoff expires.
//
// Uses singleflight to dedup concurrent resolution requests for the same
// API URL, preventing parallel requests from all hitting the API at once.
//
// Returns an error when resolution fails (429, 5xx, network error). The
// caller must retry — no fallback to the raw API URL because it does not
// support Range requests correctly (the API endpoint returns a redirect that
// drops the Range header, making streaming impossible).
func (c *CDNClient) ResolveURL(ctx context.Context, apiURL string) (string, error) {
	if c.urlCacheTTL <= 0 {
		return apiURL, nil
	}

	// Check cache first — covers both successful and failed resolutions.
	if v, ok := c.urlCache.Load(apiURL); ok {
		entry := v.(*cdnURLCacheEntry)
		if entry.failed {
			// Failed resolution (429/5xx) — check if backoff period has elapsed.
			if time.Since(entry.resolvedAt) < c.rateLimitCooldown {
				// Still in backoff — return error without retrying.
				return "", fmt.Errorf("cdn url resolution rate limited (backoff expires in %v): %w",
					c.rateLimitCooldown-time.Since(entry.resolvedAt), fmt.Errorf("rate limited"))
			}
			// Backoff expired — fall through to re-resolve.
		} else if time.Since(entry.resolvedAt) < c.urlCacheTTL {
			// Successful resolution still valid.
			return entry.resolvedURL, nil
		}
	}

	// Dedup concurrent resolution requests for the same URL.
	result, err, _ := c.resolveGroup.Do(apiURL, func() (interface{}, error) {
		resolved, resolveErr := c.resolveRedirect(ctx, apiURL)
		if resolveErr != nil {
			// Cache the failure for rateLimitCooldown to prevent API hammering.
			c.urlCache.Store(apiURL, &cdnURLCacheEntry{
				resolvedAt: time.Now(),
				failed:     true,
			})
			return nil, resolveErr
		}

		slog.Debug("cdn url resolved", "apiURL", apiURL, "cdnURL", resolved)
		c.urlCache.Store(apiURL, &cdnURLCacheEntry{
			resolvedURL: resolved,
			resolvedAt:  time.Now(),
		})
		return resolved, nil
	})

	if err != nil {
		slog.Warn("cdn url resolution failed", "url", apiURL, "err", err)
		return "", fmt.Errorf("cdn url resolution: %w", err)
	}

	return result.(string), nil
}

// InvalidateURL removes the cached CDN URL for the given API URL. Call this
// when a CDN request with a cached URL fails with 403/401 (URL expired), so
// the next request re-resolves through the API.
func (c *CDNClient) InvalidateURL(apiURL string) {
	c.urlCache.Delete(apiURL)
}

// resolveRedirect follows the redirect chain for the given URL and returns
// the final resolved URL. Uses a GET with Range: bytes=0-0 to minimize data
// transfer while ensuring CDN compatibility.
//
// Returns an error for 429 (rate limited) and 5xx (server error) responses
// so that ResolveURL does NOT cache the unresolved API URL.
func (c *CDNClient) resolveRedirect(ctx context.Context, rawURL string) (string, error) {
	currentURL := rawURL
	for redirects := 0; redirects < maxRedirects; redirects++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, currentURL, nil)
		if err != nil {
			return "", fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Range", "bytes=0-0")
		req.Header.Set("User-Agent", "torbox-media-center-go/1.0")

		resp, err := c.client.Do(req)
		if err != nil {
			return "", fmt.Errorf("request: %w", err)
		}

		if resp.StatusCode == http.StatusMovedPermanently ||
			resp.StatusCode == http.StatusFound ||
			resp.StatusCode == http.StatusSeeOther ||
			resp.StatusCode == http.StatusTemporaryRedirect ||
			resp.StatusCode == http.StatusPermanentRedirect {
			loc := resp.Header.Get("Location")
			resp.Body.Close()
			if loc == "" {
				return "", fmt.Errorf("redirect with no Location header")
			}
			resolved, err := resolveURL(currentURL, loc)
			if err != nil {
				return "", fmt.Errorf("resolve redirect URL: %w", err)
			}
			currentURL = resolved
			continue
		}

		// Rate-limited: do NOT treat as successful resolution.
		// Returning an error prevents ResolveURL from caching the bad URL.
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			slog.Warn("cdn url resolution rate limited", "url", rawURL, "status", resp.StatusCode)
			return "", fmt.Errorf("rate limited: status %d", resp.StatusCode)
		}

		// Server errors: do NOT treat as successful resolution.
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			slog.Warn("cdn url resolution server error", "url", rawURL, "status", resp.StatusCode)
			return "", fmt.Errorf("server error: status %d", resp.StatusCode)
		}

		resp.Body.Close()

		// Any other non-redirect response means we've reached the final URL.
		// 206 (range accepted), 200, 416 (range not satisfiable for 0-0 on
		// empty file) - all mean the URL resolved successfully.
		return currentURL, nil
	}

	return "", fmt.Errorf("too many redirects (%d) for %s", maxRedirects, rawURL)
}

// FetchRange issues an HTTP GET with a Range header for bytes [start, end] to
// the given URL. It blocks until a per-host semaphore slot is available, sends
// the request, and returns the HTTP response with the body still open for streaming.
// The caller must close resp.Body when done reading.
//
// Priority controls the queue order when the per-host semaphore is full:
//   - cache.PriorityHigh (1): playback requests jump ahead of queued scan requests
//   - cache.PriorityLow (0): scan/metadata requests wait in FIFO order
//
// Concurrency is limited per CDN host: each host gets its own semaphore with
// maxConnsPerHost slots. This means requests to different CDN servers (e.g.
// nexus-040 vs nexus-128) don't compete for the same slots, maximizing
// parallelism when files are spread across multiple CDNs.
//
// It resolves the API redirect URL to a direct CDN URL (cached with TTL)
// before issuing the range request, avoiding repeated API hits.
//
// It follows redirects manually to preserve the Range header, which Go's
// default http.Client drops on 301/302 redirects.
//
// It accepts:
//   - 206 Partial Content (expected)
//   - 200 OK only when start == 0 (full response fallback)
//   - 429, 5xx: returns HTTPStatusError (caller decides whether to retry)
func (c *CDNClient) FetchRange(ctx context.Context, rawURL string, start, end int64, priority uint8) (*http.Response, error) {
	// Resolve API URL to direct CDN URL first - we need the host for per-host
	// rate limiting, and we need the resolved URL for the request anyway.
	// If resolution fails (e.g. rate-limited or server error), propagate the error
	// so the caller can retry — no fallback to the raw API URL since it doesn't
	// support Range requests.
	resolvedURL, err := c.ResolveURL(ctx, rawURL)
	if err != nil {
		return nil, fmt.Errorf("resolve CDN URL: %w", err)
	}

	// Acquire per-host semaphore: limits concurrent requests to each CDN host.
	// Priority-aware: high-priority requests (playback) jump ahead of
	// low-priority ones (scan) in the wait queue.
	host := hostFromURL(resolvedURL)
	hs := c.getHostSem(host)
	if err := hs.acquire(ctx, priority); err != nil {
		return nil, fmt.Errorf("semaphore: %w", err)
	}
	defer hs.release()

	if c.metrics != nil {
		c.metrics.CDNRequestCount.Add(1)
	}

	slog.Debug("cdn fetch range request", "url", resolvedURL, "start", start, "end", end, "host", host)

	resp, err := c.doRangeRequest(ctx, resolvedURL, start, end)
	if err != nil {
		return nil, err
	}

	// If the CDN URL returned 403/401 (expired), invalidate cache, re-resolve, retry once.
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()
		slog.Warn("cdn url expired, re-resolving", "url", resolvedURL, "status", resp.StatusCode)
		c.InvalidateURL(rawURL)
		newResolved, reResolveErr := c.ResolveURL(ctx, rawURL)
		if reResolveErr != nil {
			return nil, fmt.Errorf("re-resolve CDN URL after %d: %w", resp.StatusCode, reResolveErr)
		}
		if newResolved != resolvedURL {
			slog.Debug("cdn re-resolved url", "apiURL", rawURL, "newURL", newResolved)
			return c.doRangeRequest(ctx, newResolved, start, end)
		}
		return nil, &HTTPStatusError{StatusCode: resp.StatusCode}
	}

	return resp, nil
}

// doRangeRequest sends a single HTTP range request and follows any
// CDN-level redirects. Returns the response with body open for streaming.
func (c *CDNClient) doRangeRequest(ctx context.Context, reqURL string, start, end int64) (*http.Response, error) {
	currentURL := reqURL
	for redirects := 0; redirects < maxRedirects; redirects++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, currentURL, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		req.Header.Set("User-Agent", "torbox-media-center-go/1.0")

		resp, err := c.client.Do(req)
		if err != nil {
			slog.Warn("cdn request error", "url", currentURL, "err", err)
			return nil, fmt.Errorf("cdn request: %w", err)
		}

		if c.metrics != nil {
			c.metrics.IncCDNStatusCode(resp.StatusCode)
		}

		// Handle CDN-level redirects.
		if resp.StatusCode == http.StatusMovedPermanently ||
			resp.StatusCode == http.StatusFound ||
			resp.StatusCode == http.StatusSeeOther ||
			resp.StatusCode == http.StatusTemporaryRedirect ||
			resp.StatusCode == http.StatusPermanentRedirect {
			loc := resp.Header.Get("Location")
			resp.Body.Close()
			if loc == "" {
				return nil, fmt.Errorf("redirect with no Location header")
			}
			resolved, err := resolveURL(currentURL, loc)
			if err != nil {
				return nil, fmt.Errorf("resolve redirect URL: %w", err)
			}
			currentURL = resolved
			continue
		}

		switch resp.StatusCode {
		case http.StatusPartialContent:
			// expected
		case http.StatusOK:
			if start != 0 {
				resp.Body.Close()
				slog.Warn("cdn returned 200 for non-zero offset", "url", currentURL, "start", start)
				return nil, fmt.Errorf("server returned 200 OK for non-zero offset %d", start)
			}
		default:
			resp.Body.Close()
			slog.Warn("cdn returned unexpected status", "url", currentURL, "status", resp.StatusCode)
			return nil, &HTTPStatusError{StatusCode: resp.StatusCode}
		}

		return resp, nil
	}

	return nil, fmt.Errorf("too many redirects (%d) for %s", maxRedirects, reqURL)
}

// resolveURL resolves a redirect Location against the base URL.
// Handles both absolute and relative redirect URLs.
func resolveURL(baseURL, location string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse base URL: %w", err)
	}
	loc, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parse location: %w", err)
	}
	return base.ResolveReference(loc).String(), nil
}
