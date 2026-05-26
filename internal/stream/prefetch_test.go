package stream

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
)

// ============================================================
// Phase 4: Prefetch / read-ahead tests (spec §4)
//
// NOTE: readAheadThreshold (2 MiB) is half of windowSize (4 MiB),
// so read-ahead triggers when the read offset is at least halfway
// through the current window. This gives the CDN time to fetch the
// next window while the reader finishes the current one.
// ============================================================

// 4.3 Test: prefetch does not start when range is already cached —
// pre-populate next window in cache, verify no extra CDN request.
func TestPrefetch_SkippedWhenAlreadyCached(t *testing.T) {
	testData := make([]byte, 8*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
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
	}, nil)

	// Pre-populate the next window in cache
	nextWindowData := testData[4*1024*1024 : 8*1024*1024]
	rc.Put("f1", 4*1024*1024, nextWindowData)

	// Read the first window
	buf := make([]byte, 4*1024*1024)
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(8*1024*1024))
	if err != nil {
		t.Fatalf("ReadAt(0): %v", err)
	}

	// Count CDN requests — should only be 1 (for the first window),
	// since the next window was already cached.
	time.Sleep(200 * time.Millisecond)
	if got := requestCount.Load(); got > 1 {
		t.Errorf("expected at most 1 CDN request (next window was cached), got %d", got)
	}
}

// 4.4 Test: prefetch does not start when range is already inflight —
// start a read that creates an inflight window covering the next range,
// verify no duplicate fetch.
func TestPrefetch_SkippedWhenAlreadyInflight(t *testing.T) {
	testData := make([]byte, 8*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Slow responses to keep inflight windows active longer
		time.Sleep(200 * time.Millisecond)
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
	}, nil)

	// Start a read at offset 4 MiB (second window) to create an inflight entry
	go func() {
		buf := make([]byte, 1024)
		sr.ReadAt(context.Background(), "f1", 4*1024*1024, buf, int64(8*1024*1024))
	}()

	// Wait for the second window fetch to start
	time.Sleep(50 * time.Millisecond)

	// Now read the first window — the next window is already inflight
	buf := make([]byte, 4*1024*1024)
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(8*1024*1024))
	if err != nil {
		t.Fatalf("ReadAt(0): %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Total CDN requests should be at most 2 (one per window)
	if got := requestCount.Load(); got > 2 {
		t.Errorf("expected at most 2 CDN requests, got %d", got)
	}
}

// 4.5 Test: overlapping prefetch suppression — rapid sequential reads
// don't create multiple prefetch goroutines for the same window.
func TestPrefetch_SuppressionNoDuplicateFetches(t *testing.T) {
	testData := make([]byte, 8*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
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
	}, nil)

	// Rapid sequential reads in the first window
	for i := 0; i < 10; i++ {
		off := int64(i * 100 * 1024) // 100 KiB intervals
		buf := make([]byte, 1024)
		_, err := sr.ReadAt(context.Background(), "f1", off, buf, int64(8*1024*1024))
		if err != nil {
			t.Fatalf("ReadAt(%d): %v", off, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// Should be at most 1 CDN request (first window only; all reads
	// are below the 2 MiB read-ahead threshold, so no prefetch triggers).
	if got := requestCount.Load(); got > 1 {
		t.Errorf("expected at most 1 CDN request for 10 rapid reads, got %d", got)
	}
}

// 4.7 Test: per-file inflight limit only counts active (not completed) windows.
// Completed windows whose data is cached should not block read-ahead, since
// they no longer consume CDN bandwidth.
func TestPrefetch_PerFileInflightLimit(t *testing.T) {
	testData := make([]byte, 16*1024*1024) // 16 MiB = 4 windows
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
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
	// maxInflight=1 means at most 1 active inflight window per file.
	// Completed (done=true) windows are excluded from the count.
	sr := NewStreamReader(rc, cdn, 1, int64(4<<20), func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, nil)

	// Read past readAheadThreshold (2 MiB) to trigger read-ahead.
	buf := make([]byte, 3*1024*1024) // 3 MiB read
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(8*1024*1024))
	if err != nil {
		t.Fatalf("ReadAt(0): %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Should have 2 CDN requests: the first window (read) and the second window
	// (read-ahead triggered because the completed first window doesn't count
	// against maxInflight).
	if got := requestCount.Load(); got < 2 {
		t.Errorf("expected at least 2 CDN requests (first window + read-ahead), got %d", got)
	}
}

// 4.9 Test: read-ahead works after a far seek with no orphaned windows.
// A far seek that doesn't cancel any windows (none are orphaned) should
// NOT suppress read-ahead — this is the Plex scenario where header and
// EOF-probe reads run in parallel.
func TestPrefetch_WorkAfterFarSeekNoOrphans(t *testing.T) {
	testData := make([]byte, 32*1024*1024) // 32 MiB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
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
	}, nil)

	// Read from offset 0 (small read, no read-ahead triggered)
	buf := make([]byte, 1024)
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(32*1024*1024))
	if err != nil {
		t.Fatalf("ReadAt(0): %v", err)
	}

	// Wait for the inflight window cleanup goroutine (100ms delay) to remove
	// the window from the inflight map, so the far seek finds no orphaned
	// windows to cancel.
	time.Sleep(200 * time.Millisecond)

	// Seek far away (> 16 MiB) — no orphaned windows to cancel,
	// so lastSeek should NOT be set and read-ahead should work.
	farOffset := int64(20 * 1024 * 1024)
	buf2 := make([]byte, 1024)
	_, err = sr.ReadAt(context.Background(), "f1", farOffset, buf2, int64(32*1024*1024))
	if err != nil {
		t.Fatalf("ReadAt(%d): %v", farOffset, err)
	}

	// Verify data at far offset is correct
	for i := 0; i < len(buf2); i++ {
		if buf2[i] != byte((farOffset+int64(i))%256) {
			t.Fatalf("data mismatch at offset %d", farOffset+int64(i))
		}
	}

	// Verify that lastSeek was NOT set (no orphaned windows were cancelled).
	sess := sr.getOrCreateSession("f1")
	lastSeek := time.Unix(0, sess.lastSeek.Load())
	if time.Since(lastSeek) < 5*time.Second {
		t.Error("lastSeek should NOT be recent when no windows were cancelled")
	}
}

// TestPrefetch_TriggersWhenPastThreshold verifies that read-ahead actually
// triggers when the read offset is past the readAheadThreshold (2 MiB into
// the current 4 MiB window).
func TestPrefetch_TriggersWhenPastThreshold(t *testing.T) {
	testData := make([]byte, 16*1024*1024) // 16 MiB = 4 windows
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
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
	}, nil)

	// Read past the readAheadThreshold (2 MiB into the window).
	// This should trigger read-ahead of the next window.
	buf := make([]byte, 3*1024*1024) // 3 MiB read from offset 0
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(len(testData)))
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt(0, 3MiB): %v", err)
	}
	if n != len(buf) {
		t.Fatalf("ReadAt(0, 3MiB): got %d bytes, want %d", n, len(buf))
	}

	// Wait for the read-ahead fetch to complete.
	time.Sleep(500 * time.Millisecond)

	// Should have fetched at least 2 windows: the first (for the read)
	// and the second (from read-ahead triggered by passing the threshold).
	if got := requestCount.Load(); got < 2 {
		t.Errorf("expected at least 2 CDN requests (first window + read-ahead), got %d", got)
	}

	// Verify the next window is in cache (read-ahead fetched it).
	nextWindowBuf := make([]byte, 1024)
	copied, ok := rc.CopyTo("f1", 4*1024*1024, nextWindowBuf)
	if !ok {
		t.Error("expected next window to be in cache after read-ahead, but cache miss")
	} else if copied != 1024 {
		t.Errorf("expected 1024 bytes from next window cache, got %d", copied)
	}
}