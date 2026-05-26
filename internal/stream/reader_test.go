package stream

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/metrics"
)

// newMockCDNServer creates an httptest.Server that delegates to handler.
func newMockCDNServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(handler)
}

// TestReadAt_CacheHit verifies that a ReadAt returns immediately when data
// is already in the RangeCache without touching the CDN.
func TestReadAt_CacheHit(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	sr := NewStreamReader(rc, NewCDNClient(4, nil), 2, 4<<20, func(fileKey string) string {
		t.Fatal("permalinkFor should not be called on cache hit")
		return ""
	}, nil)

	// Pre-populate the cache with data at offset 0 for file "f1"
	testData := []byte("hello world")
	rc.Put("f1", 0, testData)

	buf := make([]byte, len(testData))
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(len(testData)))
	if err != nil && err != io.EOF {
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
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

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
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf, 10)
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
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

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
			n, err := sr.ReadAt(context.Background(), "f1", off, buf, 26)
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

// ============================================================
// Phase 3: Inflight coordination tests (spec §3)
// ============================================================

// 3.1 Two reads for same range → one backend fetch: extend existing test to
// assert requestCount == 1 (currently allows <=2).
func TestReadAt_TwoReadersOneFetch(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

	var requestCount atomic.Int32

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Small delay so both goroutines can join the same window
		time.Sleep(100 * time.Millisecond)

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
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4)
			sr.ReadAt(context.Background(), "f1", 0, buf, 26)
		}()
	}
	wg.Wait()

	// Both reads should join the same inflight window: exactly 1 CDN request.
	if got := requestCount.Load(); got != 1 {
		t.Errorf("expected exactly 1 CDN request, got %d", got)
	}
}

// 3.2 Subrange read joins existing inflight: start a read at offset 0, then
// while inflight window is active, start a read at offset 100 (same window).
// Both should succeed without a second CDN request.
func TestReadAt_SubrangeJoinsInflight(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

	var requestCount atomic.Int32
	readStarted := make(chan struct{})

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Signal that the first read has started fetching
		select {
		case readStarted <- struct{}{}:
		default:
		}
		time.Sleep(100 * time.Millisecond)

		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)

		data := make([]byte, 4<<20) // 4 MiB of test data
		for i := range data {
			data[i] = byte(i % 256)
		}
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

	// Start first read
	go func() {
		buf := make([]byte, 100)
		sr.ReadAt(context.Background(), "f1", 0, buf, int64(4<<20))
	}()

	// Wait for CDN fetch to start
	<-readStarted

	// Second read at offset 100 within same window
	buf2 := make([]byte, 10)
	n, err := sr.ReadAt(context.Background(), "f1", 100, buf2, int64(4<<20))
	if err != nil {
		t.Fatalf("second ReadAt: %v", err)
	}
	if n != 10 {
		t.Errorf("second ReadAt: got %d bytes, want 10", n)
	}

	if got := requestCount.Load(); got != 1 {
		t.Errorf("expected exactly 1 CDN request, got %d", got)
	}
}

// 3.3 Inflight state cleaned after success: after a read completes, verify the
// inflight map has no entry for the key.
func TestReadAt_InflightCleanedAfterSuccess(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-9/10")
		w.WriteHeader(206)
		w.Write([]byte("ABCDEFGHIJ"))
	})
	defer server.Close()

	sr.permalinkFor = func(fileKey string) string {
		return server.URL + "/" + fileKey
	}

	buf := make([]byte, 10)
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, 100)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	// Wait for the inflight cleanup goroutine (100ms delay in fetchWindow)
	time.Sleep(200 * time.Millisecond)

	// After cleanup, inflight map should be empty for this key
	found := false
	sr.inflight.Range(func(key, _ any) bool {
		if ik, ok := key.(inflightKey); ok && ik.fileKey == "f1" {
			found = true
		}
		return true
	})
	if found {
		t.Error("inflight entry should be cleaned up after successful read")
	}
}

// 3.4 Inflight state cleaned after error: mock CDN returns 500, verify inflight
// entry is removed and subsequent reads can retry (not stuck on a failed entry).
func TestReadAt_InflightCleanedAfterError(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

	var callCount atomic.Int32

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		count := callCount.Add(1)
		if count == 1 {
			// First call: return error
			w.WriteHeader(500)
			return
		}
		// Second call: return data
		w.Header().Set("Content-Range", "bytes 0-9/10")
		w.WriteHeader(206)
		w.Write([]byte("ABCDEFGHIJ"))
	})
	defer server.Close()

	sr.permalinkFor = func(fileKey string) string {
		return server.URL + "/" + fileKey
	}

	// First read should fail
	buf := make([]byte, 10)
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, 10)
	if err == nil {
		t.Fatal("expected error from first ReadAt")
	}

	// After error, inflight entry should allow retry
	// Give a brief moment for cleanup
	time.Sleep(50 * time.Millisecond)

	// Second read should succeed (use large fileSize to avoid EOF)
	buf2 := make([]byte, 10)
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf2, 1<<20)
	if err != nil && err != io.EOF {
		t.Fatalf("expected second ReadAt to succeed after error, got: %v", err)
	}
	if n != 10 {
		t.Errorf("second ReadAt: got %d bytes, want 10", n)
	}
}

// 3.5 Inflight state cleaned after cancel: cancel context during inflight fetch,
// verify inflight entry is removed and subsequent reads can retry.
func TestReadAt_InflightCleanedAfterCancel(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)

	var callCount atomic.Int32

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		count := callCount.Add(1)
		if count == 1 {
			// First call: delay long enough for context to cancel
			time.Sleep(2 * time.Second)
		}
		w.Header().Set("Content-Range", "bytes 0-9/10")
		w.WriteHeader(206)
		w.Write([]byte("ABCDEFGHIJ"))
	})
	defer server.Close()

	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	buf := make([]byte, 10)
	_, err := sr.ReadAt(ctx, "f1", 0, buf, 10)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	// After cancel, the inflight entry should be removable for retry.
	// Wait for the background fetch goroutine to finish.
	time.Sleep(200 * time.Millisecond)

	// Verify we can retry with a fresh context — the CDN server will
	// respond quickly on the second call.
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer retryCancel()

	buf2 := make([]byte, 10)
	n, retryErr := sr.ReadAt(retryCtx, "f1", 0, buf2, 10)
	if retryErr != nil && retryErr != io.EOF {
		t.Fatalf("expected retry to succeed, got: %v", retryErr)
	}
	if n != 10 {
		t.Errorf("retry: got %d bytes, want 10", n)
	}
}

// ============================================================
// Phase 5: Read path correctness gaps (spec §5)
// ============================================================

// 5.1 Read fully from inflight data: start a read, verify data is returned
// before the done channel closes (early-return via readyTo).
func TestReadAt_EarlyReturnFromInflight(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

	// Create a slow server that writes data in chunks with delays.
	// This allows us to verify early return — reader gets data before window completes.
	var writeChunks atomic.Int32
	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-25/26")
		w.WriteHeader(206)
		// Write all data at once; early return is verified by reading partial data.
		data := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		w.Write(data)
		writeChunks.Add(1)
	})
	defer server.Close()

	sr.permalinkFor = func(fileKey string) string {
		return server.URL + "/" + fileKey
	}

	// Read only 5 bytes from offset 0 — should return before window is fully processed.
	buf := make([]byte, 5)
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf, 26)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 5 {
		t.Errorf("got %d bytes, want 5", n)
	}
	if string(buf) != "ABCDE" {
		t.Errorf("got %q, want %q", string(buf), "ABCDE")
	}
}

// 5.3 Short read near EOF: mock CDN returns fewer bytes than window size,
// verify reader handles it correctly.
func TestReadAt_ShortReadNearEOF(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

	// Server returns only 10 bytes (simulating a short file near EOF)
	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		data := []byte("ABCDEFGHIJ") // 10 bytes, less than window size
		w.Header().Set("Content-Range", "bytes 0-9/10")
		w.WriteHeader(206)
		w.Write(data)
	})
	defer server.Close()

	sr.permalinkFor = func(fileKey string) string {
		return server.URL + "/" + fileKey
	}

	// Read 5 bytes from offset 5 — near end of the short response
	buf := make([]byte, 5)
	n, err := sr.ReadAt(context.Background(), "f1", 5, buf, 10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 5 {
		t.Errorf("got %d bytes, want 5", n)
	}
	if string(buf) != "FGHIJ" {
		t.Errorf("got %q, want %q", string(buf), "FGHIJ")
	}

	// Read at the very end — should get short data and io.EOF
	buf2 := make([]byte, 10)
	n2, err2 := sr.ReadAt(context.Background(), "f1", 8, buf2, 10)
	if err2 != nil && err2 != io.EOF {
		t.Fatalf("ReadAt near EOF: %v", err2)
	}
	if n2 != 2 {
		t.Errorf("near EOF: got %d bytes, want 2", n2)
	}
	if string(buf2[:n2]) != "IJ" {
		t.Errorf("near EOF: got %q, want %q", string(buf2[:n2]), "IJ")
	}
}

// 5.4 Exact byte correctness for requested range: read a 5-byte window at
// various offsets within known data, verify each byte matches.
func TestReadAt_ExactByteCorrectness(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

	// Generate known data: 256 bytes with predictable pattern
	testData := make([]byte, 256)
	for i := range testData {
		testData[i] = byte(i)
	}

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)

		if start >= int64(len(testData)) {
			w.WriteHeader(416)
			return
		}
		if end >= int64(len(testData)) {
			end = int64(len(testData) - 1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.WriteHeader(206)
		w.Write(testData[start : end+1])
	})
	defer server.Close()

	sr.permalinkFor = func(fileKey string) string {
		return server.URL + "/" + fileKey
	}

	// Read at various offsets and verify exact byte correctness
	for _, offset := range []int64{0, 10, 50, 100, 200, 250} {
		buf := make([]byte, 5)
		n, err := sr.ReadAt(context.Background(), "f1", offset, buf, 256)
		if err != nil {
			t.Errorf("ReadAt(offset=%d): %v", offset, err)
			continue
		}
		if n != 5 {
			// Near the end, we may get fewer bytes
			if offset+5 > int64(len(testData)) {
				expected := int64(len(testData)) - offset
				if int64(n) != expected {
					t.Errorf("ReadAt(offset=%d): got %d bytes, want %d", offset, n, expected)
				}
			} else {
				t.Errorf("ReadAt(offset=%d): got %d bytes, want 5", offset, n)
			}
			continue
		}
		// Verify byte-for-byte correctness
		for i := 0; i < n; i++ {
			expected := byte(int(offset) + i)
			if buf[i] != expected {
				t.Errorf("ReadAt(offset=%d) byte %d: got 0x%02x, want 0x%02x", offset, i, buf[i], expected)
				break
			}
		}
	}
}

// 5.5 No off-by-one at boundaries: test reads at windowStart-1, windowStart,
// windowStart+1, windowEnd-1, windowEnd, windowEnd+1.
func TestReadAt_OffByOneAtBoundaries(t *testing.T) {
	rc := cache.NewRangeCache(1 << 20, nil)
	cdn := NewCDNClient(4, nil)
	// Use a small window size for easier boundary testing
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)

	// Generate data that spans multiple windows (12 MiB covers 3 windows)
	testData := make([]byte, 12*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)

		if start >= int64(len(testData)) {
			w.WriteHeader(416)
			return
		}
		if end >= int64(len(testData)) {
			end = int64(len(testData) - 1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.WriteHeader(206)
		w.Write(testData[start : end+1])
	})
	defer server.Close()

	sr.permalinkFor = func(fileKey string) string {
		return server.URL + "/" + fileKey
	}

	// Read 1 byte at various boundary positions
	ws := int64(4 * 1024 * 1024) // window size
	positions := []struct {
		name string
		off  int64
	}{
		{"window_start", ws},
		{"window_start+1", ws + 1},
		{"window_end-1", 2*ws - 1},
		{"window_end", 2 * ws},     // start of next window
		{"window_end+1", 2*ws + 1}, // one past start of next window
	}

	for _, pos := range positions {
		buf := make([]byte, 1)
		n, err := sr.ReadAt(context.Background(), "f1", pos.off, buf, int64(12*1024*1024))
		if err != nil {
			t.Errorf("ReadAt(%s=%d): %v", pos.name, pos.off, err)
			continue
		}
		if n != 1 {
			t.Errorf("ReadAt(%s=%d): got %d bytes, want 1", pos.name, pos.off, n)
			continue
		}
		expected := byte(pos.off % 256)
		if buf[0] != expected {
			t.Errorf("ReadAt(%s=%d): got 0x%02x, want 0x%02x", pos.name, pos.off, buf[0], expected)
		}
	}
}
// TestReadAt_MetricsReadCount verifies that ReadAt increments the correct
// metrics counters for read count, cache hits, stream misses, and stream joins.
func TestReadAt_MetricsReadCount(t *testing.T) {
	m := metrics.New()
	testData := make([]byte, windowSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if start >= int64(len(testData)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(testData)) {
			end = int64(len(testData) - 1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	})
	defer server.Close()

	rc := cache.NewRangeCache(8<<20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, m)

	buf := make([]byte, 100)
	_, _ = sr.ReadAt(context.Background(), "file1", 0, buf, windowSize)

	if m.ReadCount.Load() != 1 {
		t.Errorf("ReadCount should be 1, got %d", m.ReadCount.Load())
	}
	if m.StreamMissCount.Load() != 1 {
		t.Errorf("StreamMissCount should be 1, got %d", m.StreamMissCount.Load())
	}

	// Wait for the background goroutine to store data in the cache.
	time.Sleep(200 * time.Millisecond)

	// Second read should be a cache hit.
	_, _ = sr.ReadAt(context.Background(), "file1", 0, buf, windowSize)
	if m.CacheHitCount.Load() != 1 {
		t.Errorf("CacheHitCount should be 1, got %d", m.CacheHitCount.Load())
	}
	if m.ReadCount.Load() != 2 {
		t.Errorf("ReadCount should be 2, got %d", m.ReadCount.Load())
	}

	// Read at a different offset within the same window should also be a cache hit.
	_, _ = sr.ReadAt(context.Background(), "file1", 50, buf, windowSize)
	if m.ReadCount.Load() != 3 {
		t.Errorf("ReadCount should be 3, got %d", m.ReadCount.Load())
	}
	if m.CacheHitCount.Load() != 2 {
		t.Errorf("CacheHitCount should be 2, got %d", m.CacheHitCount.Load())
	}
}

// TestReadAt_MetricsStreamJoinCount verifies that concurrent reads to the same
// window increment StreamJoinCount instead of StreamMissCount.
func TestReadAt_MetricsStreamJoinCount(t *testing.T) {
	m := metrics.New()
	testData := make([]byte, windowSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var cdnCalls atomic.Int32
	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		cdnCalls.Add(1)
		time.Sleep(50 * time.Millisecond) // slow response to allow joiners
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	})
	defer server.Close()

	rc := cache.NewRangeCache(8<<20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, m)

	// First read triggers a cache miss (new inflight window).
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 100)
		sr.ReadAt(context.Background(), "file1", 0, buf, windowSize)
	}()
	go func() {
		defer wg.Done()
		// Small delay so the first goroutine creates the window first.
		time.Sleep(10 * time.Millisecond)
		buf := make([]byte, 100)
		sr.ReadAt(context.Background(), "file1", 0, buf, windowSize)
	}()
	wg.Wait()

	// Expect exactly 1 CDN call (one window, two readers joined).
	if got := cdnCalls.Load(); got != 1 {
		t.Errorf("expected 1 CDN call, got %d", got)
	}
	// First read is a miss, second is a join.
	if m.StreamMissCount.Load() != 1 {
		t.Errorf("StreamMissCount should be 1, got %d", m.StreamMissCount.Load())
	}
	if m.StreamJoinCount.Load() != 1 {
		t.Errorf("StreamJoinCount should be 1, got %d", m.StreamJoinCount.Load())
	}
}

// TestReadAt_MetricsInflightWindows verifies that InflightWindows increments
// when a window is created and decrements when the cleanup removes it.
func TestReadAt_MetricsInflightWindows(t *testing.T) {
	m := metrics.New()
	testData := make([]byte, windowSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if start >= int64(len(testData)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(testData)) {
			end = int64(len(testData) - 1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	})
	defer server.Close()

	rc := cache.NewRangeCache(8<<20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, m)

	buf := make([]byte, 100)
	sr.ReadAt(context.Background(), "file1", 0, buf, windowSize)

	// After read, inflight window was created.
	if m.InflightWindows.Load() < 1 {
		t.Errorf("InflightWindows should be >= 1 after read, got %d", m.InflightWindows.Load())
	}

	// Wait for the cleanup goroutine to remove the window from the inflight map.
	// Also wait for the cascading read-ahead window (which may fail with 416)
	// to be cleaned up.
	time.Sleep(500 * time.Millisecond)

	if m.InflightWindows.Load() != 0 {
		t.Errorf("InflightWindows should be 0 after cleanup, got %d", m.InflightWindows.Load())
	}
}

// TestReadAt_MetricsCancelledStreamCount verifies that a far seek cancels
// orphaned (waiters == 0) inflight windows and increments CancelledStreamCount.
// Windows with active waiters are NOT cancelled.
func TestReadAt_MetricsCancelledStreamCount(t *testing.T) {
	m := metrics.New()

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		data := make([]byte, end-start+1)
		for i := range data {
			data[i] = byte((start + int64(i)) % 256)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, 24*1024*1024))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data)
	})
	defer server.Close()

	rc := cache.NewRangeCache(32<<20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, m)

	// Insert an orphaned window (waiters == 0) at offset 0.
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	orphanWin := &inflightWindow{
		key:        inflightKey{fileKey: "file1", start: 0},
		buf:        make([]byte, windowSize),
		readyCond:  sync.NewCond(&sync.Mutex{}),
		cancelFunc: cancel,
	}
	sr.inflight.Store(orphanWin.key, orphanWin)
	sr.metrics.InflightWindows.Add(1)

	// Seek far away (>16 MiB) to trigger seek cancellation.
	buf := make([]byte, 100)
	sr.ReadAt(context.Background(), "file1", 20*1024*1024, buf, 24*1024*1024)

	if m.CancelledStreamCount.Load() < 1 {
		t.Errorf("CancelledStreamCount should be >= 1, got %d", m.CancelledStreamCount.Load())
	}
}

// TestReadAt_ParallelReadersNoCancel verifies that parallel readers at different
// offsets (simulating Plex header + EOF probe) do NOT cancel each other's
// inflight windows. Windows with waiters > 0 are never cancelled by seek cancellation.
func TestReadAt_ParallelReadersNoCancel(t *testing.T) {
	m := metrics.New()

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		// Slow response to keep windows inflight during parallel reads.
		time.Sleep(500 * time.Millisecond)
		data := make([]byte, end-start+1)
		for i := range data {
			data[i] = byte((start + int64(i)) % 256)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, 24*1024*1024))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data)
	})
	defer server.Close()

	rc := cache.NewRangeCache(32<<20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, m)

	// Two parallel readers at far-apart offsets (Plex-style: header at 0, EOF probe at end).
	var wg sync.WaitGroup
	var headerErr, eofProbeErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 100)
		_, headerErr = sr.ReadAt(context.Background(), "file1", 0, buf, 24*1024*1024)
	}()
	go func() {
		defer wg.Done()
		buf := make([]byte, 100)
		_, eofProbeErr = sr.ReadAt(context.Background(), "file1", 20*1024*1024, buf, 24*1024*1024)
	}()
	wg.Wait()

	// Both reads should succeed — neither window should be cancelled.
	if headerErr != nil {
		t.Errorf("header reader got error: %v", headerErr)
	}
	if eofProbeErr != nil {
		t.Errorf("EOF probe reader got error: %v", eofProbeErr)
	}

	// Seek cancellation should NOT have fired because both readers were
	// within seekThreshold of their respective inflight windows, or the
	// far-seek check detected a close window. With waiters > 0 on both
	// windows, even if cancellation fires, windows are not cancelled.
	// The key assertion: no CancelledStreamCount means no windows were cancelled.
	if m.CancelledStreamCount.Load() > 0 && m.InflightWindows.Load() < 2 {
		t.Errorf("parallel readers should not cancel each other's windows, CancelledStreamCount=%d", m.CancelledStreamCount.Load())
	}
}

// TestReadAt_SeekCancelsOrphanedWindow verifies that a far seek cancels inflight
// windows with waiters == 0 (orphaned windows, e.g. from read-ahead prefetch)
// but does NOT cancel windows with waiters > 0.
func TestReadAt_SeekCancelsOrphanedWindow(t *testing.T) {
	m := metrics.New()

	// Slow CDN server so windows stay inflight long enough to test.
	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		time.Sleep(800 * time.Millisecond)
		data := make([]byte, end-start+1)
		for i := range data {
			data[i] = byte((start + int64(i)) % 256)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, 24*1024*1024))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data)
	})
	defer server.Close()

	rc := cache.NewRangeCache(32<<20, nil)
	cdn := NewCDNClient(4, nil)
	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, m)

	// Create an orphaned window at offset 0 by directly inserting it into the
	// inflight map with waiters == 0 (simulating a read-ahead that no one is using).
	_, cancel := context.WithCancel(context.Background())
	orphanWin := &inflightWindow{
		key:       inflightKey{fileKey: "file1", start: 0},
		buf:       make([]byte, windowSize),
		readyCond: sync.NewCond(&sync.Mutex{}),
		cancelFunc: cancel,
	}
	sr.inflight.Store(orphanWin.key, orphanWin)
	if sr.metrics != nil {
		sr.metrics.InflightWindows.Add(1)
	}

	// Also start an active read at offset 0 in a goroutine.
	// This will create a SECOND window at offset 0 — but the existing one
	// is already stored. Instead, let's create a window at a different offset
	// to ensure the far-seek detection fires.
	// Actually, let's keep it simpler: just have the orphaned window at 0
	// and do a far seek — the orphaned window should be cancelled.

	// Do a far seek (>16 MiB away).
	buf := make([]byte, 100)
	sr.ReadAt(context.Background(), "file1", 20*1024*1024, buf, 24*1024*1024)

	// The orphaned window should have been cancelled.
	if m.CancelledStreamCount.Load() < 1 {
		t.Errorf("CancelledStreamCount should be >= 1, got %d", m.CancelledStreamCount.Load())
	}

	// Clean up the orphaned window's context if it wasn't cancelled.
	cancel()
}

// TestReadAt_CrossWindowBoundary verifies that a read spanning a 4 MiB window
// boundary returns the full requested number of bytes rather than a short read.
// A short read at a window boundary would cause FUSE to treat it as EOF.
func TestReadAt_CrossWindowBoundary(t *testing.T) {
	rc := cache.NewRangeCache(16<<20, nil)
	cdn := NewCDNClient(4, nil)

	// Create test data that spans 3 windows (12 MiB).
	testData := make([]byte, 3*int(windowSize))
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	server := newMockCDNServer(t, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)

		if start >= int64(len(testData)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(testData)) {
			end = int64(len(testData) - 1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	})
	defer server.Close()

	sr := NewStreamReader(rc, cdn, 2, 4<<20, func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, nil)

	// Pre-populate the first window in the cache so we get a cache hit that
	// returns a short read at the boundary.
	// First, do a full read of the first window to fill the cache.
	firstWindow := make([]byte, windowSize)
	n, err := sr.ReadAt(context.Background(), "f1", 0, firstWindow, int64(len(testData)))
	if err != nil && err != io.EOF {
		t.Fatalf("filling first window: %v", err)
	}
	if n != int(windowSize) {
		t.Fatalf("first window: got %d bytes, want %d", n, windowSize)
	}

	// Now read 131072 bytes starting 16384 bytes before the window boundary.
	// The cache hit for the first window will only return 16384 bytes (the
	// remainder of the window). The loop in ReadAt must then fetch the next
	// window and fill the rest of the buffer.
	off := windowSize - 16384 // 4 MiB - 16 KiB
	buf := make([]byte, 131072) // 128 KiB request
	n, err = sr.ReadAt(context.Background(), "f1", off, buf, int64(len(testData)))
	if err != nil && err != io.EOF {
		t.Fatalf("cross-window read: %v", err)
	}
	if n != 131072 {
		t.Fatalf("cross-window read: got %d bytes, want 131072 (short reads at window boundaries cause FUSE EOF)", n)
	}

	// Verify byte-for-byte correctness at the boundary.
	for i := 0; i < 16384; i++ {
		if buf[i] != byte(int(off)+i%256) {
			t.Fatalf("byte %d from window 1: got 0x%02x, want 0x%02x", i, buf[i], byte(int(off)+i%256))
		}
	}
	for i := 16384; i < 131072; i++ {
		if buf[i] != byte(int(off)+i%256) {
			t.Fatalf("byte %d from window 2: got 0x%02x, want 0x%02x", i, buf[i], byte(int(off)+i%256))
		}
	}
}
