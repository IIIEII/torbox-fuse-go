// Package stream provides the CDN range client and StreamReader for the
// TorBox FUSE streaming hot path.
package stream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/metrics"
)

// maxResponseSize is the maximum CDN response body size (8 MiB).
// This prevents unbounded memory if the CDN returns 200 OK instead of 206.
const maxResponseSize = 8 * 1024 * 1024

// maxRedirects is the maximum number of HTTP redirects to follow.
const maxRedirects = 5

// CDNClient performs HTTP range requests to TorBox CDN URLs with a concurrency
// semaphore that limits the number of in-flight requests.
type CDNClient struct {
	client  *http.Client
	sem     chan struct{}
	metrics *metrics.Metrics
}

// NewCDNClient creates a CDNClient that allows at most maxConns concurrent
// range requests. The underlying HTTP transport is tuned for streaming:
// compression disabled, idle connections kept warm.
//
// The client handles redirects manually to preserve the Range header across
// redirects. Go's default http.Client drops Range on cross-origin 301/302.
func NewCDNClient(maxConns int, m *metrics.Metrics) *CDNClient {
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
		sem:     make(chan struct{}, maxConns),
		metrics: m,
	}
}

// FetchRange issues an HTTP GET with a Range header for bytes [start, end] to
// the given URL. It blocks until a semaphore slot is available, sends the
// request, and returns the response body bytes.
//
// It follows redirects manually to preserve the Range header, which Go's
// default http.Client drops on 301/302 redirects.
//
// It accepts:
//   - 206 Partial Content (expected)
//   - 200 OK only when start == 0 (full response fallback)
func (c *CDNClient) FetchRange(ctx context.Context, rawURL string, start, end int64) ([]byte, error) {
	c.sem <- struct{}{}
	defer func() { <-c.sem }()

	if c.metrics != nil {
		c.metrics.CDNRequestCount.Add(1)
	}

	slog.Debug("cdn fetch range request", "url", rawURL, "start", start, "end", end)

	currentURL := rawURL
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

		// Handle redirects by following the Location header manually.
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

		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusPartialContent:
			// expected
		case http.StatusOK:
			if start != 0 {
				slog.Warn("cdn returned 200 for non-zero offset", "url", currentURL, "start", start)
				return nil, fmt.Errorf("server returned 200 OK for non-zero offset %d", start)
			}
		default:
			slog.Warn("cdn returned unexpected status", "url", currentURL, "status", resp.StatusCode)
			return nil, fmt.Errorf("cdn returned status %d", resp.StatusCode)
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
		if err != nil {
			slog.Warn("cdn read error", "url", currentURL, "err", err)
			return nil, fmt.Errorf("read cdn response: %w", err)
		}
		if len(body) >= int(maxResponseSize) {
			slog.Warn("cdn response too large", "url", currentURL, "size", len(body))
			return nil, fmt.Errorf("cdn response too large (exceeded %d bytes)", maxResponseSize)
		}

		slog.Debug("cdn fetch range response", "url", currentURL, "status", resp.StatusCode, "bodySize", len(body))
		return body, nil
	}

	return nil, fmt.Errorf("too many redirects (%d) for %s", maxRedirects, rawURL)
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