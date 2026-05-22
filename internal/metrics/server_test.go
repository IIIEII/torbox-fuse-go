package metrics

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetricsSnapshot(t *testing.T) {
	m := New()
	m.CatalogItems.Add(42)
	m.CacheBytesTotal.Add(1024)
	m.CacheBytesActive.Add(512)
	m.CacheBytesStale.Add(128)
	m.CacheEntries.Add(10)
	m.InflightWindows.Add(3)
	m.ReadCount.Add(100)
	m.CacheHitCount.Add(80)
	m.StreamMissCount.Add(5)
	m.StreamJoinCount.Add(2)
	m.CancelledStreamCount.Add(1)
	m.APICallCount.Add(7)
	m.RefreshCount.Add(2)

	snap := m.Snapshot()

	tests := []struct {
		name string
		got  int64
		want int64
	}{
		{"CatalogItems", snap["catalog_items"].(int64), 42},
		{"CacheBytesTotal", snap["cache_bytes_total"].(int64), 1024},
		{"CacheBytesActive", snap["cache_bytes_active"].(int64), 512},
		{"CacheBytesStale", snap["cache_bytes_stale"].(int64), 128},
		{"CacheEntries", snap["cache_entries"].(int64), 10},
		{"InflightWindows", snap["inflight_windows"].(int64), 3},
		{"ReadCount", snap["read_count"].(int64), 100},
		{"CacheHitCount", snap["cache_hit_count"].(int64), 80},
		{"StreamMissCount", snap["stream_miss_count"].(int64), 5},
		{"StreamJoinCount", snap["stream_join_count"].(int64), 2},
		{"CancelledStreamCount", snap["cancelled_stream_count"].(int64), 1},
		{"APICallCount", snap["api_call_count"].(int64), 7},
		{"RefreshCount", snap["refresh_count"].(int64), 2},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %d, want %d", tt.name, tt.got, tt.want)
		}
	}

	if _, ok := snap["goroutine_count"]; !ok {
		t.Error("goroutine_count missing from snapshot")
	}
}

func TestCDNStatusCodes(t *testing.T) {
	m := New()
	m.IncCDNStatusCode(200)
	m.IncCDNStatusCode(200)
	m.IncCDNStatusCode(404)

	snap := m.Snapshot()
	cdnCodes, ok := snap["cdn_status_codes"]
	if !ok {
		t.Fatal("cdn_status_codes missing from snapshot")
	}
	codes := cdnCodes.(map[string]int64)
	if codes["cdn_200"] != 2 {
		t.Errorf("cdn_200 = %d, want 2", codes["cdn_200"])
	}
	if codes["cdn_404"] != 1 {
		t.Errorf("cdn_404 = %d, want 1", codes["cdn_404"])
	}
}

func TestHealthzEndpoint(t *testing.T) {
	m := New()
	handler := NewHandler(m, nil)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if strings.TrimSpace(string(body)) != "ok" {
		t.Errorf("body = %q, want %q", strings.TrimSpace(string(body)), "ok")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	m := New()
	m.CatalogItems.Add(99)
	m.APICallCount.Add(5)

	handler := NewHandler(m, nil)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}

	if ci, ok := result["catalog_items"].(float64); !ok || ci != 99 {
		t.Errorf("catalog_items = %v, want 99", result["catalog_items"])
	}
	if ac, ok := result["api_call_count"].(float64); !ok || ac != 5 {
		t.Errorf("api_call_count = %v, want 5", result["api_call_count"])
	}
	if _, ok := result["goroutine_count"]; !ok {
		t.Error("goroutine_count missing from response")
	}
}

func TestRefreshEndpointSuccess(t *testing.T) {
	m := New()
	var called atomic.Bool
	refreshFn := func(ctx context.Context) error {
		called.Store(true)
		return nil
	}

	handler := NewHandler(m, refreshFn)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/refresh", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /refresh: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	if !called.Load() {
		t.Error("refresh function was not called")
	}
}

func TestRefreshEndpointAlreadyInProgress(t *testing.T) {
	m := New()
	var running atomic.Bool
	done := make(chan struct{})

	refreshFn := func(ctx context.Context) error {
		if !running.CompareAndSwap(false, true) {
			return errAlreadyInProgress
		}
		<-done // block until test signals
		running.Store(false)
		return nil
	}

	handler := NewHandler(m, refreshFn)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Start first refresh in a goroutine.
	go func() {
		http.Post(srv.URL+"/refresh", "application/json", strings.NewReader("{}"))
	}()

	// Wait for the first refresh to be running.
	for i := 0; i < 50; i++ {
		if running.Load() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !running.Load() {
		t.Fatal("first refresh never started")
	}

	// Second refresh should get 202 (already in progress).
	resp, err := http.Post(srv.URL+"/refresh", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /refresh (2nd): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("second refresh status = %d, want %d; body: %s", resp.StatusCode, http.StatusAccepted, body)
	}

	// Let the first refresh finish.
	close(done)
}

// errAlreadyInProgress is the sentinel error that the server recognizes.
var errAlreadyInProgress = &alreadyInProgressError{}

type alreadyInProgressError struct{}

func (alreadyInProgressError) Error() string { return "refresh already in progress" }

func TestRefreshEndpointMethodNotAllowed(t *testing.T) {
	m := New()
	handler := NewHandler(m, nil)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/refresh")
	if err != nil {
		t.Fatalf("GET /refresh: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestNotFoundRoute(t *testing.T) {
	m := New()
	handler := NewHandler(m, nil)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/nonexistent")
	if err != nil {
		t.Fatalf("GET /nonexistent: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestRefreshEndpointNotConfigured(t *testing.T) {
	m := New()
	handler := NewHandler(m, nil) // no refreshFn
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/refresh", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /refresh: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestServerStartAndShutdown(t *testing.T) {
	m := New()
	s := NewServer(m, "127.0.0.1:0", nil)

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for server to be ready.
	var resp *http.Response
	var err error
	for i := 0; i < 20; i++ {
		resp, err = http.Get("http://" + s.Addr() + "/healthz")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}