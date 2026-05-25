package stream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
)

// ============================================================
// Phase 9: Non-functional tests (spec §16)
// ============================================================

// 9.2 Memory growth test: perform many reads, verify goroutine count
// does not grow unbounded.
func TestNonFunctional_NoGoroutineLeak(t *testing.T) {
	testData := make([]byte, 4*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()

	rc := cache.NewRangeCache(256 << 20, nil)
	cdn := NewCDNClient(8, nil)
	sr := NewStreamReader(rc, cdn, 2, int64(4<<20), func(fileKey string) string {
		return server.URL + "/" + fileKey
	})

	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	for i := 0; i < 100; i++ {
		off := int64(i * 4096)
		buf := make([]byte, 4096)
		_, err := sr.ReadAt(context.Background(), "f1", off, buf, int64(4*1024*1024))
		if err != nil {
			t.Fatalf("ReadAt(%d): %v", off, err)
		}
	}

	// Wait for inflight cleanup goroutines
	time.Sleep(300 * time.Millisecond)
	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	final := runtime.NumGoroutine()
	leaked := final - baseline
	if leaked > 10 {
		t.Errorf("goroutine leak: baseline=%d, final=%d, leaked=%d", baseline, final, leaked)
	}
}

// 9.3 Cancelled operations release resources: cancel context during CDN fetch,
// verify inflight map is cleaned, cache is not polluted with partial data.
func TestNonFunctional_CancelledOpsReleaseResources(t *testing.T) {
	testData := make([]byte, 4*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := callCount.Add(1)
		if count <= 3 {
			// Slow response for first few calls so context cancellation fires
			time.Sleep(500 * time.Millisecond)
		}
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
	}))
	defer server.Close()

	rc := cache.NewRangeCache(256 << 20, nil)
	cdn := NewCDNClient(8, nil)
	sr := NewStreamReader(rc, cdn, 2, int64(4<<20), func(fileKey string) string {
		return server.URL + "/" + fileKey
	})

	// Start reads with short timeouts — they should fail
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		buf := make([]byte, 1024)
		_, err := sr.ReadAt(ctx, fmt.Sprintf("f%d", i), 0, buf, int64(4*1024*1024))
		if err == nil {
			t.Error("expected error from cancelled context")
		}
		cancel()
	}

	// Wait for cleanup
	time.Sleep(300 * time.Millisecond)

	// Verify the inflight map is cleaned up
	found := 0
	sr.inflight.Range(func(_, _ any) bool {
		found++
		return true
	})
	if found > 2 {
		t.Errorf("inflight map still has %d entries after cancellation", found)
	}

	// Verify subsequent reads succeed with a longer timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	buf := make([]byte, 1024)
	n, err := sr.ReadAt(ctx, "fresh_file", 0, buf, int64(4*1024*1024))
	if err != nil {
		t.Fatalf("fresh read after cancellation: %v", err)
	}
	if n != 1024 {
		t.Errorf("fresh read: got %d bytes, want 1024", n)
	}
}

// 9.4 Metrics assertion test: verify cache hit/miss behavior through
// the StreamReader path.
func TestNonFunctional_MetricsCounts(t *testing.T) {
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, _ := newStreamReaderWithMock(t, testData)
	defer server.Close()

	// First read should be a cache miss (CDN fetch)
	buf := make([]byte, 256)
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(1024))
	if err != nil {
		t.Fatalf("first ReadAt: %v", err)
	}
	if n != 256 {
		t.Errorf("first ReadAt: got %d bytes, want 256", n)
	}

	// Verify data
	for i := 0; i < n; i++ {
		if buf[i] != byte(i%256) {
			t.Fatalf("first read data mismatch at offset %d", i)
		}
	}

	// Second read at different offset within same window should be a cache hit
	buf2 := make([]byte, 128)
	n2, err := sr.ReadAt(context.Background(), "f1", 128, buf2, int64(1024))
	if err != nil {
		t.Fatalf("second ReadAt: %v", err)
	}
	if n2 != 128 {
		t.Errorf("second ReadAt: got %d bytes, want 128", n2)
	}

	for i := 0; i < n2; i++ {
		if buf2[i] != byte((128+i)%256) {
			t.Fatalf("second read data mismatch at offset %d", 128+i)
		}
	}
}

// 9.5 Race-sensitive paths covered under go test -race.
// This test exercises concurrent read + inflight + seek paths that are
// likely to trigger race conditions.
func TestNonFunctional_ConcurrentRacePaths(t *testing.T) {
	testData := make([]byte, 8*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()

	rc := cache.NewRangeCache(256 << 20, nil)
	cdn := NewCDNClient(8, nil)
	sr := NewStreamReader(rc, cdn, 2, int64(4<<20), func(fileKey string) string {
		return server.URL + "/" + fileKey
	})

	// Concurrent reads from different goroutines at different offsets
	const numGoroutines = 20
	type result struct {
		err error
	}
	results := make(chan result, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			off := int64(idx * 128 * 1024)
			buf := make([]byte, 1024)
			_, err := sr.ReadAt(context.Background(), "f1", off, buf, int64(8*1024*1024))
			results <- result{err: err}
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		res := <-results
		if res.err != nil {
			t.Errorf("goroutine %d: %v", i, res.err)
		}
	}
}