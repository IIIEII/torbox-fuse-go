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

func PermalinkURL(token string, kind catalog.DownloadKind, downloadID, fileID string) string {
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
	return fmt.Sprintf("/api/%s/requestdl?token=%s&%s=%s&file_id=%s&redirect=true",
		apiPath, token, idParam, downloadID, fileID)
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

	c.cacheMu.RLock()
	if entry, ok := c.cache[cacheKey]; ok {
		if time.Since(entry.createdAt) < c.cacheTTL {
			c.cacheMu.RUnlock()
			return entry.data, nil
		}
	}
	c.cacheMu.RUnlock()

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

		body, err := io.ReadAll(resp.Body)
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

func (c *Client) backoff(ctx context.Context, attempt int) {
	delay := time.Duration(100*1<<uint(attempt))*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond
	slog.Debug("backoff", "attempt", attempt, "delay", delay)
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}