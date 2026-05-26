package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/metrics"
)

func TestFetchRange_206PartialContent(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			t.Error("missing Range header")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var start, end int64
		_, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if err != nil {
			t.Errorf("cannot parse Range header %q: %v", rangeHdr, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		body := make([]byte, end-start+1)
		for i := range body {
			body[i] = byte((start + int64(i)) % 256)
		}

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", start, end))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(body)
	}))
	defer ts.Close()

	cdn := NewCDNClient(4, nil, 0)
	resp, err := cdn.FetchRange(context.Background(), ts.URL, 100, 199)
	if err != nil {
		t.Fatalf("FetchRange returned error: %v", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	if len(data) != 100 {
		t.Fatalf("expected 100 bytes, got %d", len(data))
	}
	for i, b := range data {
		want := byte((100 + int64(i)) % 256)
		if b != want {
			t.Errorf("data[%d] = 0x%02x, want 0x%02x", i, b, want)
		}
	}
}

func TestFetchRange_200OK_ZeroOffset(t *testing.T) {
	body := []byte("hello world")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		var start int64
		fmt.Sscanf(rangeHdr, "bytes=%d-", &start)
		if start == 0 {
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		w.Write(body[start:])
	}))
	defer ts.Close()

	cdn := NewCDNClient(4, nil, 0)
	resp, err := cdn.FetchRange(context.Background(), ts.URL, 0, int64(len(body)-1))
	if err != nil {
		t.Fatalf("FetchRange returned error: %v", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	if string(data) != string(body) {
		t.Fatalf("expected %q, got %q", body, data)
	}
}

func TestFetchRange_200OK_NonZeroOffset(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("full body"))
	}))
	defer ts.Close()

	cdn := NewCDNClient(4, nil, 0)
	_, err := cdn.FetchRange(context.Background(), ts.URL, 100, 199)
	if err == nil {
		t.Fatal("expected error for 200 OK at non-zero offset, got nil")
	}
}

func TestFetchRange_ConcurrencyLimit(t *testing.T) {
	const maxConns = 2
	var active atomic.Int32
	var maxActive atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := active.Add(1)
		for {
			prev := maxActive.Load()
			if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
				break
			}
		}

		time.Sleep(50 * time.Millisecond)

		active.Add(-1)
		w.Header().Set("Content-Range", "bytes 0-0/*")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer ts.Close()

	cdn := NewCDNClient(maxConns, nil, 0)

	const totalReqs = 6
	errCh := make(chan error, totalReqs)
	for i := 0; i < totalReqs; i++ {
		go func() {
			resp, err := cdn.FetchRange(context.Background(), ts.URL, 0, 0)
			if err != nil {
				errCh <- err
				return
			}
			resp.Body.Close()
			errCh <- nil
		}()
	}

	for i := 0; i < totalReqs; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("FetchRange error: %v", err)
		}
	}

	observed := maxActive.Load()
	if observed > int32(maxConns) {
		t.Fatalf("observed %d concurrent requests, limit is %d", observed, maxConns)
	}
}

func TestFetchRange_FollowsRedirect(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			t.Error("Range header missing after redirect")
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer cdn.Close()

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", cdn.URL+r.URL.Path)
		w.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()

	client := NewCDNClient(4, nil, 0)
	resp, err := client.FetchRange(context.Background(), redirect.URL, 0, 99)
	if err != nil {
		t.Fatalf("FetchRange after redirect: %v", err)
	}
	defer resp.Body.Close()

	result, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	if len(result) != 100 {
		t.Fatalf("expected 100 bytes, got %d", len(result))
	}
	for i, b := range result {
		if b != byte(i%256) {
			t.Errorf("result[%d] = 0x%02x, want 0x%02x", i, b, byte(i%256))
			break
		}
	}
}

func TestFetchRange_TooManyRedirects(t *testing.T) {
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", r.URL.String())
		w.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()

	client := NewCDNClient(4, nil, 0)
	_, err := client.FetchRange(context.Background(), redirect.URL, 0, 99)
	if err == nil {
		t.Fatal("expected error for redirect loop, got nil")
	}
}

func TestFetchRange_IncrementsCDNRequestCount(t *testing.T) {
	m := &metrics.Metrics{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/*")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer ts.Close()

	cdn := NewCDNClient(4, m, 0)

	resp, err := cdn.FetchRange(context.Background(), ts.URL, 0, 0)
	if err != nil {
		t.Fatalf("FetchRange returned error: %v", err)
	}
	resp.Body.Close()

	if got := m.CDNRequestCount.Load(); got != 1 {
		t.Errorf("CDNRequestCount = %d, want 1", got)
	}

	resp, err = cdn.FetchRange(context.Background(), ts.URL, 0, 0)
	if err != nil {
		t.Fatalf("second FetchRange returned error: %v", err)
	}
	resp.Body.Close()

	if got := m.CDNRequestCount.Load(); got != 2 {
		t.Errorf("CDNRequestCount = %d after second call, want 2", got)
	}
}

func TestFetchRange_IncCDNStatusCode(t *testing.T) {
	m := &metrics.Metrics{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/*")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer ts.Close()

	cdn := NewCDNClient(4, m, 0)

	resp, err := cdn.FetchRange(context.Background(), ts.URL, 0, 0)
	if err != nil {
		t.Fatalf("FetchRange returned error: %v", err)
	}
	resp.Body.Close()

	counter, ok := m.CDNStatusCodes.Load(206)
	if !ok {
		t.Fatal("no CDNStatusCodes entry for 206")
	}
	if counter.(*atomic.Int64).Load() != 1 {
		t.Errorf("CDNStatusCodes[206] = %d, want 1", counter.(*atomic.Int64).Load())
	}
}

func TestFetchRange_NilMetricsNoPanic(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/*")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer ts.Close()

	cdn := NewCDNClient(4, nil, 0)

	resp, err := cdn.FetchRange(context.Background(), ts.URL, 0, 0)
	if err != nil {
		t.Fatalf("FetchRange with nil metrics returned error: %v", err)
	}
	resp.Body.Close()
}

func TestFetchRange_HTTPStatusError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	cdn := NewCDNClient(4, nil, 0)
	_, err := cdn.FetchRange(context.Background(), ts.URL, 0, 0)
	if err == nil {
		t.Fatal("expected error for 429, got nil")
	}

	var hse *HTTPStatusError
	if !ErrorAs(err, &hse) {
		t.Fatalf("expected HTTPStatusError, got %T: %v", err, err)
	}
	if hse.StatusCode != 429 {
		t.Errorf("HTTPStatusError.StatusCode = %d, want 429", hse.StatusCode)
	}
}

// ErrorAs wraps errors.As for test use.
func ErrorAs(err error, target any) bool {
	return errors.As(err, target) //nolint:errcheck // test helper
}

func TestResolveURL_CacheHit(t *testing.T) {
	var resolveCount atomic.Int32
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resolveCount.Add(1)
		w.Header().Set("Content-Range", "bytes 0-0/100")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer cdn.Close()

	c := NewCDNClient(4, nil, 5*time.Minute)

	// First resolution — should hit the CDN.
	got := c.ResolveURL(context.Background(), cdn.URL)
	if got != cdn.URL {
		t.Errorf("ResolveURL first call = %q, want %q", got, cdn.URL)
	}
	if resolveCount.Load() != 1 {
		t.Errorf("resolveCount after first call = %d, want 1", resolveCount.Load())
	}

	// Second resolution — should be cached, no new request.
	got2 := c.ResolveURL(context.Background(), cdn.URL)
	if got2 != cdn.URL {
		t.Errorf("ResolveURL cached call = %q, want %q", got2, cdn.URL)
	}
	if resolveCount.Load() != 1 {
		t.Errorf("resolveCount after cached call = %d, want 1 (should be cached)", resolveCount.Load())
	}
}

func TestResolveURL_CacheExpiry(t *testing.T) {
	var resolveCount atomic.Int32
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resolveCount.Add(1)
		w.Header().Set("Content-Range", "bytes 0-0/100")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer cdn.Close()

	// Very short TTL so it expires quickly.
	c := NewCDNClient(4, nil, 50*time.Millisecond)

	c.ResolveURL(context.Background(), cdn.URL)
	if resolveCount.Load() != 1 {
		t.Fatalf("resolveCount after first call = %d, want 1", resolveCount.Load())
	}

	// Wait for TTL to expire.
	time.Sleep(100 * time.Millisecond)

	c.ResolveURL(context.Background(), cdn.URL)
	if resolveCount.Load() != 2 {
		t.Errorf("resolveCount after expiry = %d, want 2 (should re-resolve)", resolveCount.Load())
	}
}

func TestResolveURL_FollowsRedirect(t *testing.T) {
	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/100")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer cdnServer.Close()

	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", cdnServer.URL)
		w.WriteHeader(http.StatusFound)
	}))
	defer redirectServer.Close()

	c := NewCDNClient(4, nil, 5*time.Minute)

	got := c.ResolveURL(context.Background(), redirectServer.URL)
	if got != cdnServer.URL {
		t.Errorf("ResolveURL = %q, want %q (should follow redirect to CDN)", got, cdnServer.URL)
	}

	// Verify cached — no new redirect request.
	got2 := c.ResolveURL(context.Background(), redirectServer.URL)
	if got2 != cdnServer.URL {
		t.Errorf("ResolveURL cached = %q, want %q", got2, cdnServer.URL)
	}
}

func TestResolveURL_ZeroTTL(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/100")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer cdn.Close()

	c := NewCDNClient(4, nil, 0) // TTL=0 disables caching

	got := c.ResolveURL(context.Background(), cdn.URL)
	if got != cdn.URL {
		t.Errorf("ResolveURL with TTL=0 = %q, want %q (pass-through)", got, cdn.URL)
	}

	// Verify nothing was cached.
	if _, ok := c.urlCache.Load(cdn.URL); ok {
		t.Error("urlCache should be empty when TTL=0")
	}
}

func TestInvalidateURL(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/100")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer cdn.Close()

	c := NewCDNClient(4, nil, 5*time.Minute)

	// Resolve and cache.
	c.ResolveURL(context.Background(), cdn.URL)
	if _, ok := c.urlCache.Load(cdn.URL); !ok {
		t.Fatal("expected URL to be cached after ResolveURL")
	}

	// Invalidate.
	c.InvalidateURL(cdn.URL)
	if _, ok := c.urlCache.Load(cdn.URL); ok {
		t.Error("expected URL to be removed from cache after InvalidateURL")
	}
}

func TestFetchRange_ExpiredCDNURLRetry(t *testing.T) {
	var requestCount atomic.Int32
	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.Header().Set("Content-Range", "bytes 0-0/100")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer cdnServer.Close()

	// API server: redirects to CDN on first call, then returns 403 on CDN requests,
	// then redirects to a new CDN URL on the second API call.
	var apiCallCount atomic.Int32
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := apiCallCount.Add(1)
		w.Header().Set("Location", cdnServer.URL)
		w.WriteHeader(http.StatusFound)
		_ = count
	}))
	defer apiServer.Close()

	c := NewCDNClient(4, nil, 5*time.Minute)

	// First, resolve the URL through the API.
	resolved := c.ResolveURL(context.Background(), apiServer.URL)
	if resolved != cdnServer.URL {
		t.Fatalf("ResolveURL = %q, want %q", resolved, cdnServer.URL)
	}

	// Now fetch range from the resolved URL should work.
	resp, err := c.FetchRange(context.Background(), apiServer.URL, 0, 0)
	if err != nil {
		t.Fatalf("FetchRange error: %v", err)
	}
	resp.Body.Close()
}

func TestResolveRedirectLocation(t *testing.T) {
	tests := []struct {
		base     string
		location string
		want     string
	}{
		{
			base:     "https://api.torbox.app/v1/api/torrents/requestdl?token=x",
			location: "https://cdn.torbox.app/files/abc?token=y",
			want:     "https://cdn.torbox.app/files/abc?token=y",
		},
		{
			base:     "https://api.torbox.app/v1/api/torrents/requestdl?token=x",
			location: "/v1/api/torrents/requestdl?token=x&redirect=true",
			want:     "https://api.torbox.app/v1/api/torrents/requestdl?token=x&redirect=true",
		},
	}
	for _, tt := range tests {
		got, err := resolveURL(tt.base, tt.location)
		if err != nil {
			t.Errorf("resolveURL(%q, %q) error: %v", tt.base, tt.location, err)
			continue
		}
		if got != tt.want {
			t.Errorf("resolveURL(%q, %q) = %q, want %q", tt.base, tt.location, got, tt.want)
		}
	}
}