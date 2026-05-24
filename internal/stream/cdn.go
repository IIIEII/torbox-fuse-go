// Package stream provides the CDN range client and StreamReader for the
// TorBox FUSE streaming hot path.
package stream

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxResponseSize is the maximum CDN response body size (8 MiB).
// This prevents unbounded memory if the CDN returns 200 OK instead of 206.
const maxResponseSize = 8 * 1024 * 1024

// CDNClient performs HTTP range requests to TorBox CDN URLs with a concurrency
// semaphore that limits the number of in-flight requests.
type CDNClient struct {
	client *http.Client
	sem    chan struct{}
}

// NewCDNClient creates a CDNClient that allows at most maxConns concurrent
// range requests. The underlying HTTP transport is tuned for streaming:
// compression disabled, idle connections kept warm.
func NewCDNClient(maxConns int) *CDNClient {
	return &CDNClient{
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				MaxConnsPerHost:     16,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},
		sem: make(chan struct{}, maxConns),
	}
}

// FetchRange issues an HTTP GET with a Range header for bytes [start, end] to
// the given URL. It blocks until a semaphore slot is available, sends the
// request, and returns the response body bytes.
//
// It accepts:
//   - 206 Partial Content (expected)
//   - 200 OK only when start == 0 (full response fallback)
//
// Any other status code or a 200 for non-zero start returns an error.
func (c *CDNClient) FetchRange(ctx context.Context, url string, start, end int64) ([]byte, error) {
	c.sem <- struct{}{}
	defer func() { <-c.sem }()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	req.Header.Set("User-Agent", "torbox-media-center-go/1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cdn request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		// expected
	case http.StatusOK:
		if start != 0 {
			return nil, fmt.Errorf("server returned 200 OK for non-zero offset %d", start)
		}
	default:
		return nil, fmt.Errorf("cdn returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return nil, fmt.Errorf("read cdn response: %w", err)
	}
	if len(body) >= int(maxResponseSize) {
		return nil, fmt.Errorf("cdn response too large (exceeded %d bytes)", maxResponseSize)
	}
	return body, nil
}