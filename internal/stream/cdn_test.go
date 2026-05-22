package stream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchRange_206PartialContent(t *testing.T) {
	// Set up a mock HTTP server that validates the Range header and returns 206.
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

	cdn := NewCDNClient(4)
	data, err := cdn.FetchRange(context.Background(), ts.URL, 100, 199)
	if err != nil {
		t.Fatalf("FetchRange returned error: %v", err)
	}
	if len(data) != 100 {
		t.Fatalf("expected 100 bytes, got %d", len(data))
	}
	// Verify content: byte at index i should be (100+i) % 256
	for i, b := range data {
		want := byte((100 + int64(i)) % 256)
		if b != want {
			t.Errorf("data[%d] = 0x%02x, want 0x%02x", i, b, want)
		}
	}
}

func TestFetchRange_200OK_ZeroOffset(t *testing.T) {
	// Some CDNs return 200 OK instead of 206 when offset is 0.
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

	cdn := NewCDNClient(4)
	data, err := cdn.FetchRange(context.Background(), ts.URL, 0, int64(len(body)-1))
	if err != nil {
		t.Fatalf("FetchRange returned error: %v", err)
	}
	if string(data) != string(body) {
		t.Fatalf("expected %q, got %q", body, data)
	}
}

func TestFetchRange_200OK_NonZeroOffset(t *testing.T) {
	// Server returns 200 for a non-zero offset — should be an error.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("full body"))
	}))
	defer ts.Close()

	cdn := NewCDNClient(4)
	_, err := cdn.FetchRange(context.Background(), ts.URL, 100, 199)
	if err == nil {
		t.Fatal("expected error for 200 OK at non-zero offset, got nil")
	}
}

func TestFetchRange_ConcurrencyLimit(t *testing.T) {
	// Verify the semaphore limits concurrent requests.
	const maxConns = 2
	var active atomic.Int32
	var maxActive atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := active.Add(1)
		// Track the maximum concurrent requests seen.
		for {
			prev := maxActive.Load()
			if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
				break
			}
		}

		time.Sleep(50 * time.Millisecond) // hold the slot open

		active.Add(-1)
		w.Header().Set("Content-Range", "bytes 0-0/*")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte{0x00})
	}))
	defer ts.Close()

	cdn := NewCDNClient(maxConns)

	const totalReqs = 6
	errCh := make(chan error, totalReqs)
	for i := 0; i < totalReqs; i++ {
		go func() {
			_, err := cdn.FetchRange(context.Background(), ts.URL, 0, 0)
			errCh <- err
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