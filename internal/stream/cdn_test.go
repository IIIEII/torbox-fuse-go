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

func TestFetchRange_FollowsRedirect(t *testing.T) {
	// Set up a CDN target that returns 206 with Range header preserved.
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

	// Set up a redirect server that 302s to the CDN target.
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", cdn.URL+r.URL.Path)
		w.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()

	client := NewCDNClient(4)
	result, err := client.FetchRange(context.Background(), redirect.URL, 0, 99)
	if err != nil {
		t.Fatalf("FetchRange after redirect: %v", err)
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
	// Server redirects to itself, causing redirect loop.
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", r.URL.String())
		w.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()

	client := NewCDNClient(4)
	_, err := client.FetchRange(context.Background(), redirect.URL, 0, 99)
	if err == nil {
		t.Fatal("expected error for redirect loop, got nil")
	}
}

func TestResolveURL(t *testing.T) {
	tests := []struct {
		base      string
		location  string
		want      string
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