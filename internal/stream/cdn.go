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
)

// maxRedirects is the maximum number of HTTP redirects to follow.
const maxRedirects = 5

// HTTPStatusError represents an unexpected HTTP status code from the CDN.
type HTTPStatusError struct {
	StatusCode int
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("cdn returned status %d", e.StatusCode)
}

// cdnURLCacheEntry stores a resolved CDN URL with its resolution time.
type cdnURLCacheEntry struct {
	resolvedURL string
	resolvedAt  time.Time
}

// CDNClient performs HTTP range requests to TorBox CDN URLs with a concurrency
// semaphore that limits the number of in-flight requests.
// It caches API redirect resolutions so subsequent range requests go directly
// to the CDN URL instead of hitting the TorBox API every time.
type CDNClient struct {
	client      *http.Client
	sem         chan struct{}
	metrics     *metrics.Metrics
	urlCache    sync.Map // apiURL string -> *cdnURLCacheEntry
	urlCacheTTL time.Duration
}

// NewCDNClient creates a CDNClient that allows at most maxConns concurrent
// range requests. The underlying HTTP transport is tuned for streaming:
// compression disabled, idle connections kept warm.
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
				DisableCompression: true,
			},
		},
		sem:         make(chan struct{}, maxConns),
		metrics:     m,
		urlCacheTTL: urlCacheTTL,
	}
}

// ResolveURL resolves an API redirect URL to the direct CDN URL. It caches the
// result and re-resolves after urlCacheTTL expires. Falls back to the API URL
// on resolution failure, so streaming still works (just slower).
//
// Resolution uses a GET request with Range: bytes=0-0 to follow the redirect
// chain while transferring minimal data. HEAD is not used because some CDNs
// don't support it for redirect URLs.
func (c *CDNClient) ResolveURL(ctx context.Context, apiURL string) string {
	if c.urlCacheTTL <= 0 {
		return apiURL
	}

	if v, ok := c.urlCache.Load(apiURL); ok {
		entry := v.(*cdnURLCacheEntry)
		if time.Since(entry.resolvedAt) < c.urlCacheTTL {
			return entry.resolvedURL
		}
	}

	resolved, err := c.resolveRedirect(ctx, apiURL)
	if err != nil {
		slog.Warn("cdn url resolution failed, using api url directly", "url", apiURL, "err", err)
		return apiURL
	}

	slog.Debug("cdn url resolved", "apiURL", apiURL, "cdnURL", resolved)
	c.urlCache.Store(apiURL, &cdnURLCacheEntry{
		resolvedURL: resolved,
		resolvedAt:  time.Now(),
	})
	return resolved
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

		resp.Body.Close()

		// Any non-redirect response means we've reached the final URL.
		// 206 (range accepted), 200, 416 (range not satisfiable for 0-0 on
		// empty file) — all mean the URL resolved successfully.
		return currentURL, nil
	}

	return "", fmt.Errorf("too many redirects (%d) for %s", maxRedirects, rawURL)
}

// FetchRange issues an HTTP GET with a Range header for bytes [start, end] to
// the given URL. It blocks until a semaphore slot is available, sends the
// request, and returns the HTTP response with the body still open for streaming.
// The caller must close resp.Body when done reading.
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
func (c *CDNClient) FetchRange(ctx context.Context, rawURL string, start, end int64) (*http.Response, error) {
	c.sem <- struct{}{}
	defer func() { <-c.sem }()

	if c.metrics != nil {
		c.metrics.CDNRequestCount.Add(1)
	}

	// Resolve API URL to direct CDN URL (cached with TTL).
	resolvedURL := c.ResolveURL(ctx, rawURL)

	slog.Debug("cdn fetch range request", "url", resolvedURL, "start", start, "end", end)

	resp, err := c.doRangeRequest(ctx, resolvedURL, start, end)
	if err != nil {
		return nil, err
	}

	// If the CDN URL returned 403/401 (expired), invalidate cache, re-resolve, retry once.
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()
		slog.Warn("cdn url expired, re-resolving", "url", resolvedURL, "status", resp.StatusCode)
		c.InvalidateURL(rawURL)
		newResolved := c.ResolveURL(ctx, rawURL)
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