package stream

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
)

// ============================================================
// Phase 7: Integration tests (spec §7–14)
// ============================================================

// newStreamReaderWithMock creates a StreamReader backed by a mock CDN server
// that serves the given testData with range request support.
func newStreamReaderWithMock(t *testing.T, testData []byte) (*StreamReader, *httptest.Server, *atomic.Int32) {
	t.Helper()
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
			return
		}
		var start, end int64
		n, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if err != nil || n != 2 {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if start >= int64(len(testData)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(testData)) {
			end = int64(len(testData) - 1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	}))

	rc := cache.NewRangeCache(256 << 20, nil)
	cdn := NewCDNClient(8, nil)
	sr := NewStreamReader(rc, cdn, 2, int64(4<<20), func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, nil)

	return sr, server, &requestCount
}

// 7.1 Sequential playback from start: read 4 MiB sequentially in 128 KiB chunks,
// verify data correctness, count CDN requests (should be ~1 for first window).
func TestIntegration_SequentialPlaybackFromStart(t *testing.T) {
	testData := make([]byte, 4*1024*1024) // 4 MiB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, requestCount := newStreamReaderWithMock(t, testData)
	defer server.Close()

	const chunkSize = 128 * 1024
	buf := make([]byte, chunkSize)
	offset := int64(0)

	for offset < int64(len(testData)) {
		n, err := sr.ReadAt(context.Background(), "f1", offset, buf, int64(4*1024*1024))
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt(offset=%d): %v", offset, err)
		}
		if n > len(buf) {
			t.Fatalf("ReadAt(offset=%d): got %d bytes, max %d", offset, n, len(buf))
		}
		for i := 0; i < n; i++ {
			if buf[i] != byte((offset+int64(i))%256) {
				t.Fatalf("data mismatch at offset %d: got 0x%02x, want 0x%02x",
					offset+int64(i), buf[i], byte((offset+int64(i))%256))
			}
		}
		offset += int64(n)
	}

	// After sequential read of 4 MiB window, CDN requests should be 1
	// (the first window fetch; all subsequent reads are cache hits).
	if got := requestCount.Load(); got > 2 {
		t.Errorf("expected at most 2 CDN requests (window + maybe read-ahead), got %d", got)
	}
}

// 7.2 Mid-file playback start: seek to 50% offset, read several small chunks.
func TestIntegration_MidFilePlaybackStart(t *testing.T) {
	testData := make([]byte, 8*1024*1024) // 8 MiB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, requestCount := newStreamReaderWithMock(t, testData)
	defer server.Close()

	// Seek to 50%
	midOffset := int64(len(testData) / 2)
	buf := make([]byte, 128*1024)
	n, err := sr.ReadAt(context.Background(), "f1", midOffset, buf, int64(8*1024*1024))
	if err != nil {
		t.Fatalf("ReadAt(mid=%d): %v", midOffset, err)
	}
	if n != len(buf) {
		t.Errorf("ReadAt(mid=%d): got %d bytes, want %d", midOffset, n, len(buf))
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte((midOffset+int64(i))%256) {
			t.Fatalf("data mismatch at mid-read offset %d", midOffset+int64(i))
		}
	}

	// Subsequent reads in same window should not need another CDN request
	firstCount := requestCount.Load()
	n2, err := sr.ReadAt(context.Background(), "f1", midOffset+int64(len(buf)), buf, int64(8*1024*1024))
	if err != nil {
		t.Fatalf("second ReadAt: %v", err)
	}
	if n2 == 0 {
		t.Error("second ReadAt returned 0 bytes")
	}
	// CDN request count should not have grown significantly
	if got := requestCount.Load(); got > firstCount+1 {
		t.Errorf("subsequent read caused too many CDN requests: was %d, now %d", firstCount, got)
	}
}

// 7.3 EOF probe plus playback: read last 1 KiB, then read from offset 0.
func TestIntegration_EOFProbeThenPlayback(t *testing.T) {
	testData := make([]byte, 4*1024*1024+12345) // slightly over 4 MiB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, _ := newStreamReaderWithMock(t, testData)
	defer server.Close()

	// Read last 1 KiB (EOF probe)
	eofOffset := int64(len(testData) - 1024)
	buf := make([]byte, 1024)
	n, err := sr.ReadAt(context.Background(), "f1", eofOffset, buf, int64(4*1024*1024+12345))
	if err != nil && err != io.EOF {
		t.Fatalf("EOF probe ReadAt(%d): %v", eofOffset, err)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte((eofOffset+int64(i))%256) {
			t.Fatalf("EOF data mismatch at offset %d", eofOffset+int64(i))
		}
	}

	// Now read from start — should work fine
	buf2 := make([]byte, 4096)
	n2, err := sr.ReadAt(context.Background(), "f1", 0, buf2, int64(4*1024*1024+12345))
	if err != nil {
		t.Fatalf("playback ReadAt(0): %v", err)
	}
	if n2 != len(buf2) {
		t.Errorf("playback ReadAt(0): got %d bytes, want %d", n2, len(buf2))
	}
	for i := 0; i < n2; i++ {
		if buf2[i] != byte(i%256) {
			t.Fatalf("playback data mismatch at offset %d", i)
		}
	}
}

// 7.4 Repeated small reads in one window: many 4 KiB reads within one 4 MiB window,
// verify CDN request count is low.
func TestIntegration_RepeatedSmallReadsInOneWindow(t *testing.T) {
	testData := make([]byte, 4*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, requestCount := newStreamReaderWithMock(t, testData)
	defer server.Close()

	const numReads = 50
	const readSize = 4096

	for i := 0; i < numReads; i++ {
		off := int64(i * readSize)
		buf := make([]byte, readSize)
		n, err := sr.ReadAt(context.Background(), "f1", off, buf, int64(4*1024*1024))
		if err != nil {
			t.Fatalf("ReadAt(%d): %v", off, err)
		}
		if n != readSize {
			t.Errorf("ReadAt(%d): got %d bytes, want %d", off, n, readSize)
		}
		for j := 0; j < n; j++ {
			if buf[j] != byte((off+int64(j))%256) {
				t.Fatalf("data mismatch at offset %d", off+int64(j))
			}
		}
	}

	// 50 small reads should result in at most 2 CDN requests
	// (first window + maybe read-ahead for next window).
	if got := requestCount.Load(); got > 2 {
		t.Errorf("expected at most 2 CDN requests for %d small reads, got %d", numReads, got)
	}
}

// 7.7 Concurrent readers same file: multiple goroutines reading different offsets
// of the same file, verify no data corruption.
func TestIntegration_ConcurrentReadersSameFile(t *testing.T) {
	testData := make([]byte, 4*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, _ := newStreamReaderWithMock(t, testData)
	defer server.Close()

	type result struct {
		offset int64
		data   []byte
		err    error
	}

	const numReaders = 5
	results := make(chan result, numReaders)

	for i := 0; i < numReaders; i++ {
		go func(idx int) {
			off := int64(idx * 64 * 1024) // different offsets within same window
			buf := make([]byte, 1024)
			n, err := sr.ReadAt(context.Background(), "f1", off, buf, int64(4*1024*1024))
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{offset: off, data: buf[:n]}
		}(i)
	}

	for i := 0; i < numReaders; i++ {
		res := <-results
		if res.err != nil {
			t.Errorf("reader %d: %v", i, res.err)
			continue
		}
		for j, b := range res.data {
			if b != byte((res.offset+int64(j))%256) {
				t.Errorf("data corruption at offset %d: got 0x%02x, want 0x%02x",
					res.offset+int64(j), b, byte((res.offset+int64(j))%256))
				break
			}
		}
	}
}

// 7.8 Concurrent readers different files: multiple goroutines reading different
// files, verify no state leakage.
func TestIntegration_ConcurrentReadersDifferentFiles(t *testing.T) {
	fileData := map[string][]byte{
		"f1": make([]byte, 1024),
		"f2": make([]byte, 1024),
	}
	for key, data := range fileData {
		for i := range data {
			data[i] = byte(i % 256)
		}
		// Make each file's data distinguishable
		data[0] = byte(key[1]) // '1' or '2'
	}

	// Create a server that serves different data per file key
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Extract file key from path
		fileKey := r.URL.Path[1:] // remove leading slash
		data, ok := fileData[fileKey]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		rangeHdr := r.Header.Get("Range")
		var start, end int64
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if start >= int64(len(data)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(data)) {
			end = int64(len(data) - 1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer server.Close()

	rc := cache.NewRangeCache(256 << 20, nil)
	cdn := NewCDNClient(8, nil)
	sr := NewStreamReader(rc, cdn, 2, int64(4<<20), func(fileKey string) string {
		return server.URL + "/" + fileKey
	}, nil)

	type result struct {
		fileKey string
		data    []byte
		err     error
	}
	results := make(chan result, 2)

	for key := range fileData {
		go func(k string) {
			buf := make([]byte, 1)
			n, err := sr.ReadAt(context.Background(), k, 0, buf, 1024)
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{fileKey: k, data: buf[:n]}
		}(key)
	}

	for i := 0; i < 2; i++ {
		res := <-results
		if res.err != nil {
			t.Errorf("reader for %s: %v", res.fileKey, res.err)
			continue
		}
		expected := fileData[res.fileKey][0]
		if res.data[0] != expected {
			t.Errorf("state leakage: file %s first byte got 0x%02x, want 0x%02x",
				res.fileKey, res.data[0], expected)
		}
	}
}

// 7.9 Backend failure recovery: CDN returns errors for first 2 requests, then succeeds.
func TestIntegration_BackendFailureRecovery(t *testing.T) {
	testData := make([]byte, 4*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := callCount.Add(1)
		if count <= 2 {
			// First 2 calls fail
			w.WriteHeader(http.StatusInternalServerError)
			return
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
	}, nil)

	// First read should fail (error window is removed after error, allowing retry)
	buf := make([]byte, 1024)
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(4*1024*1024))
	if err == nil {
		t.Error("expected first read to fail")
	}

	// Second read should also fail
	_, err = sr.ReadAt(context.Background(), "f1", 0, buf, int64(4*1024*1024))
	if err == nil {
		t.Error("expected second read to fail")
	}

	// Third read should succeed
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(4*1024*1024))
	if err != nil {
		t.Fatalf("expected third read to succeed, got: %v", err)
	}
	if n != len(buf) {
		t.Errorf("third read: got %d bytes, want %d", n, len(buf))
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte(i%256) {
			t.Fatalf("data mismatch at offset %d after recovery", i)
		}
	}
}