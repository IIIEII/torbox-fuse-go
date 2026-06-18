package torbox

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/catalog"
)

// maxCacheEntries limits the number of API response entries cached in memory.
const maxCacheEntries = 100

// maxAPIResponseSize limits the size of API response bodies read into memory (50 MB).
// This prevents OOM from a misbehaving API endpoint returning an unbounded stream.
const maxAPIResponseSize = 50 * 1024 * 1024

type Config interface {
	APIKey() string
	APIBaseURL() string
	APITimeout() time.Duration
}

type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	apiSem     chan struct{}
	cache      map[string]cacheEntry
	cacheMu    sync.RWMutex
	cacheTTL   time.Duration
}

type cacheEntry struct {
	data      []byte
	createdAt time.Time
}

func NewClient(cfg Config) *Client {
	return &Client{
		apiKey:  cfg.APIKey(),
		baseURL: cfg.APIBaseURL(),
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 5,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		apiSem:   make(chan struct{}, 1),
		cache:    make(map[string]cacheEntry),
		cacheTTL: 5 * time.Minute,
	}
}

func (c *Client) ListDownloads(ctx context.Context, kind catalog.DownloadKind) ([]catalog.Download, error) {
	var apiKind string
	switch kind {
	case catalog.KindTorrent:
		apiKind = "torrents"
	case catalog.KindUsenet:
		apiKind = "usenet"
	case catalog.KindWebDL:
		apiKind = "webdl"
	default:
		return nil, fmt.Errorf("unknown download kind: %s", kind)
	}

	path := "/" + apiKind + "/mylist"
	var allDownloads []catalog.Download
	offset := 0
	limit := 1000

	for {
		data, err := c.apiGet(ctx, path, map[string]string{
			"limit":        fmt.Sprintf("%d", limit),
			"offset":       fmt.Sprintf("%d", offset),
			"bypass_cache": "true",
		})
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", kind, err)
		}

		var resp apiListResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			return nil, fmt.Errorf("parse %s response: %w", kind, err)
		}

		if len(resp.Data) == 0 {
			break
		}

		for _, item := range resp.Data {
			if !item.Cached {
				continue
			}
			dl := catalog.Download{
				Kind: kind,
				ID:   fmt.Sprintf("%.0f", item.ID),
				Name: item.Name,
				Hash: item.Hash,
			}
			tags := extractTags(item.Tags)
			for _, f := range item.Files {
				file := catalog.File{
					DownloadKind: kind,
					DownloadID:   dl.ID,
					FileID:       fmt.Sprintf("%.0f", f.ID),
					Name:         f.Name,
					Size:         int64(f.Size),
					MimeType:     f.MimeType,
				}
				shortName := f.ShortName
				if shortName == "" {
					if idx := strings.LastIndex(f.Name, "/"); idx >= 0 {
						shortName = f.Name[idx+1:]
					} else {
						shortName = f.Name
					}
				}
				file.MediaType = catalog.ClassifyMediaType(tags, shortName, catalog.MediaMovie)
				dl.Files = append(dl.Files, file)
			}
			allDownloads = append(allDownloads, dl)
		}

		if len(resp.Data) < limit {
			break
		}
		offset += limit
	}

	return allDownloads, nil
}

func extractTags(raw interface{}) []string {
	if raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case []interface{}:
		var tags []string
		for _, item := range v {
			tags = append(tags, fmt.Sprintf("%v", item))
		}
		return tags
	case map[string]interface{}:
		var tags []string
		for key, val := range v {
			tags = append(tags, key+"="+fmt.Sprintf("%v", val))
		}
		return tags
	case string:
		return []string{v}
	default:
		return nil
	}
}

func PermalinkURL(baseURL, token string, kind catalog.DownloadKind, downloadID, fileID string) string {
	idParam := "torrent_id"
	apiPath := "torrents"
	switch kind {
	case catalog.KindUsenet:
		idParam = "usenet_id"
		apiPath = "usenet"
	case catalog.KindWebDL:
		idParam = "web_id"
		apiPath = "webdl"
	}
	return fmt.Sprintf("%s/%s/requestdl?token=%s&%s=%s&file_id=%s&redirect=true",
		baseURL, apiPath, token, idParam, downloadID, fileID)
}

func (c *Client) apiGet(ctx context.Context, path string, params map[string]string) ([]byte, error) {
	// Build deterministic cache key from sorted parameters.
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	cacheKey := path
	for _, k := range keys {
		cacheKey += "?" + k + "=" + params[k]
	}

	// If the caller explicitly asked to bypass cache, skip the local cache
	// lookup entirely — we want fresh data from the API.
	if params["bypass_cache"] != "true" {
		c.cacheMu.RLock()
		if entry, ok := c.cache[cacheKey]; ok {
			if time.Since(entry.createdAt) < c.cacheTTL {
				c.cacheMu.RUnlock()
				return entry.data, nil
			}
		}
		c.cacheMu.RUnlock()
	}

	c.apiSem <- struct{}{}
	defer func() { <-c.apiSem }()

	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		q := req.URL.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if attempt < maxRetries-1 {
				c.backoff(ctx, attempt)
				continue
			}
			return nil, fmt.Errorf("request failed after %d attempts: %w", maxRetries, err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(io.LimitReader(resp.Body, maxAPIResponseSize))
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt < maxRetries-1 {
				retryAfter := resp.Header.Get("Retry-After")
				if retryAfter != "" {
					if secs, err := time.ParseDuration(retryAfter + "s"); err == nil {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-time.After(secs):
						}
					}
				}
				c.backoff(ctx, attempt)
				continue
			}
			return nil, fmt.Errorf("rate limited after %d retries: %s", maxRetries, resp.Status)
		}

		if resp.StatusCode >= 500 {
			if attempt < maxRetries-1 {
				c.backoff(ctx, attempt)
				continue
			}
			return nil, fmt.Errorf("server error after %d retries: %s", maxRetries, resp.Status)
		}

		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("client error: %s", resp.Status)
		}

		c.cacheMu.Lock()
		c.cache[cacheKey] = cacheEntry{data: body, createdAt: time.Now()}
		// Evict oldest entries if cache exceeds limit.
		if len(c.cache) > maxCacheEntries {
			oldest := ""
			oldestTime := time.Now()
			for k, v := range c.cache {
				if v.createdAt.Before(oldestTime) {
					oldestTime = v.createdAt
					oldest = k
				}
			}
			if oldest != "" {
				delete(c.cache, oldest)
			}
		}
		c.cacheMu.Unlock()

		return body, nil
	}

	return nil, fmt.Errorf("unreachable")
}

// DeleteDownload removes a download from TorBox by its kind and ID.
// It calls the appropriate TorBox API endpoint based on the download kind:
//   - torrent:  POST /torrents/deletetorrent?id={downloadID}
//   - usenet:   POST /usenet/deleteusenet?id={downloadID}
//   - webdl:    POST /webdl/deletewebdownload?id={downloadID}
func (c *Client) DeleteDownload(ctx context.Context, kind catalog.DownloadKind, downloadID string) error {
	var apiPath string
	switch kind {
	case catalog.KindTorrent:
		apiPath = "/torrents/deletetorrent"
	case catalog.KindUsenet:
		apiPath = "/usenet/deleteusenet"
	case catalog.KindWebDL:
		apiPath = "/webdl/deletewebdownload"
	default:
		return fmt.Errorf("unknown download kind: %s", kind)
	}

	_, err := c.apiPost(ctx, apiPath, map[string]string{"id": downloadID})
	if err != nil {
		return fmt.Errorf("delete %s %s: %w", kind, downloadID, err)
	}

	slog.Info("deleted download from torbox", "kind", kind, "download_id", downloadID)
	return nil
}

// apiPost sends a POST request with the given parameters and returns the response body.
func (c *Client) apiPost(ctx context.Context, path string, params map[string]string) ([]byte, error) {
	c.apiSem <- struct{}{}
	defer func() { <-c.apiSem }()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	q := req.URL.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAPIResponseSize))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("client error: %s (%s)", resp.Status, string(body))
	}

	return body, nil
}

// RedactToken returns a URL string with any "token=" query parameter
// value replaced by "***" to avoid leaking the API key in logs.
func RedactToken(rawURL string) string {
	idx := strings.Index(rawURL, "token=")
	if idx < 0 {
		return rawURL
	}
	// Find the start of the token value (after "token=")
	valStart := idx + len("token=")
	// Find the end of the token value (next & or end of string)
	valEnd := strings.Index(rawURL[valStart:], "&")
	if valEnd < 0 {
		return rawURL[:valStart] + "***"
	}
	return rawURL[:valStart] + "***" + rawURL[valStart+valEnd:]
}

func (c *Client) backoff(ctx context.Context, attempt int) {
	delay := time.Duration(100*1<<uint(attempt))*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond
	slog.Debug("backoff", "attempt", attempt, "delay", delay)
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}
