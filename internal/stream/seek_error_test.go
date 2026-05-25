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
// Phase 6: Seek, error, and concurrency tests (spec §6)
// ============================================================

// 6.2 Seek to start: after reading from middle, seek back to offset 0,
// verify data is correct.
func TestSeek_BackToStart(t *testing.T) {
	testData := make([]byte, 32*1024*1024) // 32 MiB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, _ := newStreamReaderWithMock(t, testData)
	defer server.Close()

	// Read from middle first (offset 16 MiB — triggers far seek from 0)
	midOffset := int64(16 * 1024 * 1024)
	buf := make([]byte, 4096)
	n, err := sr.ReadAt(context.Background(), "f1", midOffset, buf, int64(32*1024*1024))
	if err != nil {
		t.Fatalf("mid ReadAt: %v", err)
	}
	if n != 4096 {
		t.Errorf("mid ReadAt: got %d bytes, want 4096", n)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte((midOffset+int64(i))%256) {
			t.Fatalf("mid data mismatch at offset %d", midOffset+int64(i))
		}
	}

	// Seek back to start
	buf2 := make([]byte, 4096)
	n2, err := sr.ReadAt(context.Background(), "f1", 0, buf2, int64(32*1024*1024))
	if err != nil {
		t.Fatalf("start ReadAt: %v", err)
	}
	if n2 != 4096 {
		t.Errorf("start ReadAt: got %d bytes, want 4096", n2)
	}
	for i := 0; i < n2; i++ {
		if buf2[i] != byte(i%256) {
			t.Fatalf("start data mismatch at offset %d", i)
		}
	}
}

// 6.3 Seek to middle: read from start, then read from middle.
func TestSeek_ToMiddle(t *testing.T) {
	testData := make([]byte, 32*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, _ := newStreamReaderWithMock(t, testData)
	defer server.Close()

	// Read from start
	buf := make([]byte, 4096)
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(32*1024*1024))
	if err != nil {
		t.Fatalf("start ReadAt: %v", err)
	}
	if n != 4096 {
		t.Errorf("start ReadAt: got %d bytes, want 4096", n)
	}

	// Seek to middle (offset 20 MiB — far seek from offset 0)
	midOffset := int64(20 * 1024 * 1024)
	buf2 := make([]byte, 4096)
	n2, err := sr.ReadAt(context.Background(), "f1", midOffset, buf2, int64(32*1024*1024))
	if err != nil {
		t.Fatalf("mid ReadAt: %v", err)
	}
	if n2 != 4096 {
		t.Errorf("mid ReadAt: got %d bytes, want 4096", n2)
	}
	for i := 0; i < n2; i++ {
		if buf2[i] != byte((midOffset+int64(i))%256) {
			t.Fatalf("mid data mismatch at offset %d", midOffset+int64(i))
		}
	}
}

// 6.4 Seek near EOF: read from near file end, verify short data without error.
func TestSeek_NearEOF(t *testing.T) {
	testData := make([]byte, 4*1024*1024 + 12345) // slightly over 4 MiB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, _ := newStreamReaderWithMock(t, testData)
	defer server.Close()

	// Read near EOF
	eofOffset := int64(len(testData) - 100)
	buf := make([]byte, 200) // request more than available
	n, err := sr.ReadAt(context.Background(), "f1", eofOffset, buf, int64(4*1024*1024+12345))
	if err != nil && err != io.EOF {
		t.Fatalf("EOF ReadAt: %v", err)
	}
	// Should get only 100 bytes (remaining in file)
	if n != 100 {
		t.Errorf("EOF ReadAt: got %d bytes, want 100", n)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte((eofOffset+int64(i))%256) {
			t.Fatalf("EOF data mismatch at offset %d", eofOffset+int64(i))
		}
	}
}

// 6.5 Repeated seeks: rapid seek to different offsets, verify no stale data.
func TestSeek_RepeatedSeesksNoStaleData(t *testing.T) {
	testData := make([]byte, 48*1024*1024) // 48 MiB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	sr, server, _ := newStreamReaderWithMock(t, testData)
	defer server.Close()

	// Rapid seeks to different offsets
	offsets := []int64{
		0,
		32 * 1024 * 1024, // 32 MiB (far seek)
		8 * 1024 * 1024,  // 8 MiB (far seek back)
		40 * 1024 * 1024, // 40 MiB (far seek forward)
	}

	for _, off := range offsets {
		buf := make([]byte, 256)
		n, err := sr.ReadAt(context.Background(), "f1", off, buf, int64(48*1024*1024))
		if err != nil {
			t.Errorf("ReadAt(%d): %v", off, err)
			continue
		}
		if n != 256 {
			t.Errorf("ReadAt(%d): got %d bytes, want 256", off, n)
			continue
		}
		for i := 0; i < n; i++ {
			if buf[i] != byte((off+int64(i))%256) {
				t.Errorf("stale data at offset %d: got 0x%02x, want 0x%02x",
					off+int64(i), buf[i], byte((off+int64(i))%256))
				break
			}
		}
	}
}

// 6.6 Backend timeout: mock CDN with delay longer than context deadline.
func TestSeek_BackendTimeout(t *testing.T) {
	testData := make([]byte, 4*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Delay longer than the context deadline
		time.Sleep(5 * time.Second)
		w.Header().Set("Content-Range", "bytes 0-9/10")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("0123456789"))
	}))
	defer server.Close()

	rc := cache.NewRangeCache(256 << 20, nil)
	cdn := NewCDNClient(8, nil)
	sr := NewStreamReader(rc, cdn, 2, int64(4<<20), func(fileKey string) string {
		return server.URL + "/" + fileKey
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	buf := make([]byte, 10)
	_, err := sr.ReadAt(ctx, "f1", 0, buf, int64(10))
	if err == nil {
		t.Error("expected error from timeout")
	}
}

// 6.8 Invalid backend range response: CDN returns 416 Range Not Satisfiable.
func TestSeek_InvalidRangeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}))
	defer server.Close()

	rc := cache.NewRangeCache(256 << 20, nil)
	cdn := NewCDNClient(8, nil)
	sr := NewStreamReader(rc, cdn, 2, int64(4<<20), func(fileKey string) string {
		return server.URL + "/" + fileKey
	})

	buf := make([]byte, 10)
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(10))
	if err == nil {
		t.Error("expected error from 416 response")
	}
}

// 6.9 Temporary backend error: CDN returns 500 on first request, 200 on retry.
func TestSeek_TemporaryBackendError(t *testing.T) {
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := callCount.Add(1)
		if count == 1 {
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
	})

	// First read should fail
	buf := make([]byte, 10)
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(1024))
	if err == nil {
		t.Error("expected first read to fail")
	}

	// Retry should succeed
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(1024))
	if err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}
	if n != 10 {
		t.Errorf("retry: got %d bytes, want 10", n)
	}
}

// 6.10 State remains usable after error.
func TestSeek_StateUsableAfterError(t *testing.T) {
	testData := make([]byte, 4*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := callCount.Add(1)
		if count == 1 {
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
	})

	// First read fails
	buf := make([]byte, 10)
	_, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(4*1024*1024))
	if err == nil {
		t.Error("expected error on first read")
	}

	// Second read succeeds
	n, err := sr.ReadAt(context.Background(), "f1", 0, buf, int64(4*1024*1024))
	if err != nil {
		t.Fatalf("expected second read to succeed, got: %v", err)
	}
	if n != 10 {
		t.Errorf("second read: got %d bytes, want 10", n)
	}

	// Third read at different offset also works
	buf2 := make([]byte, 256)
	n2, err := sr.ReadAt(context.Background(), "f1", 512, buf2, int64(4*1024*1024))
	if err != nil {
		t.Fatalf("expected third read to succeed, got: %v", err)
	}
	if n2 != 256 {
		t.Errorf("third read: got %d bytes, want 256", n2)
	}
	for i := 0; i < n2; i++ {
		if buf2[i] != byte((512+int64(i))%256) {
			t.Fatalf("data mismatch at offset %d", 512+int64(i))
		}
	}
}