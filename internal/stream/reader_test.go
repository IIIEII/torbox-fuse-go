package stream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
)

// newMockCDNServer creates an httptest.Server that delegates to handler.
func newMockCDNServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(handler)
}

// TestReadAt_CacheHit verifies that a ReadAt returns immediately when data
// is already in the RangeCache without touching the CDN.
func TestReadAt_CacheHit(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20)
	sr := NewStreamReader(rc, NewCDNClient(4), 2, 4<<20, func(fileKey string) string {
		t.Fatal("permalinkFor should not be called on cache hit")
		return ""
	})

	// Pre-populate the cache with data at offset 0 for file "f1"
	testData := []byte("hello world")
	rc.Put("f1", 0, testData)

	buf := make([]byte, len(testData))
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("expected %d bytes, got %d", len(testData), n)
	}
	if string(buf[:n]) != string(testData) {
		t.Fatalf("expected %q, got %q", testData, buf[:n])
	}
}

// TestReadAt_CacheMissFetchesFromCDN verifies that on a cache miss,
// an inflight window is created, data is fetched from the CDN, and
// the result is returned to the caller and stored in the cache.
func TestReadAt_CacheMissFetchesFromCDN(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20)
	cdn := NewCDNClient(4)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	})

	// Start a mock HTTP server that serves range requests
	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.WriteHeader(200)
			w.Write([]byte("full response"))
			return
		}
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		// Return bytes from start to end inclusive
		data := []byte("ABCDEFGHIJ") // 10 bytes of test data
		if start >= int64(len(data)) {
			w.WriteHeader(416)
			return
		}
		if end >= int64(len(data)) {
			end = int64(len(data) - 1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(206)
		w.Write(data[start : end+1])
	})
	defer server.Close()

	// Override permalinkFor to point at our mock server
	sr.permalinkFor = func(fileKey string) string {
		return server.URL + "/" + fileKey
	}

	buf := make([]byte, 5)
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes, got %d", n)
	}
	if string(buf[:n]) != "ABCDE" {
		t.Fatalf("expected \"ABCDE\", got %q", buf[:n])
	}

	// Verify the data was cached
	cached := make([]byte, 5)
	cn, hit := rc.CopyTo("f1", 0, cached)
	if !hit {
		t.Fatal("expected data to be cached after CDN fetch")
	}
	if cn != 5 {
		t.Fatalf("expected 5 cached bytes, got %d", cn)
	}
}

// TestReadAt_MultipleReadersJoinInflightWindow verifies that concurrent
// reads at overlapping offsets join the same inflight window rather than
// creating separate CDN requests.
func TestReadAt_MultipleReadersJoinInflightWindow(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20)
	cdn := NewCDNClient(4)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	})

	var requestCount atomic.Int32

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Small delay to make it more likely concurrent readers join same window
		time.Sleep(50 * time.Millisecond)

		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)

		data := []byte("ABCDEFGHIJLKMNOPQRSTUVWXYZ")
		if start >= int64(len(data)) {
			w.WriteHeader(416)
			return
		}
		if end >= int64(len(data)) {
			end = int64(len(data) - 1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(206)
		w.Write(data[start : end+1])
	})
	defer server.Close()

	sr.permalinkFor = func(fileKey string) string {
		return server.URL + "/" + fileKey
	}

	var wg sync.WaitGroup
	results := make([]struct {
		n   int
		err error
		buf []byte
	}, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			buf := make([]byte, 4)
			off := int64(idx * 2) // offsets 0, 2, 4 — all within same window
			n, err := sr.ReadAt(context.Background(), "f1", off, buf)
			results[idx].n = n
			results[idx].err = err
			results[idx].buf = buf
		}(i)
	}
	wg.Wait()

	for i, res := range results {
		if res.err != nil {
			t.Errorf("reader %d: %v", i, res.err)
		}
		if res.n == 0 {
			t.Errorf("reader %d: expected >0 bytes, got %d", i, res.n)
		}
	}

	// All three reads should have been served by a single inflight window
	// (one CDN request, not three)
	if got := requestCount.Load(); got > 2 {
		t.Errorf("expected at most 2 CDN requests (window + maybe read-ahead), got %d", got)
	}
}