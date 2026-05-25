package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetricsPrometheusOutput(t *testing.T) {
	m := New()
	m.CatalogItems.Add(42)
	m.APICallCount.Add(5)
	m.IncCDNStatusCode(200)
	m.IncCDNStatusCode(200)
	m.IncCDNStatusCode(404)

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
	if !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	text := string(body)

	// Verify counter and gauge TYPE declarations.
	if !strings.Contains(text, "# TYPE torbox_catalog_items counter") {
		t.Error("missing torbox_catalog_items counter TYPE declaration")
	}
	if !strings.Contains(text, "torbox_catalog_items 42") {
		t.Error("missing torbox_catalog_items value line")
	}
	if !strings.Contains(text, "# TYPE torbox_cache_bytes_active gauge") {
		t.Error("missing torbox_cache_bytes_active gauge TYPE declaration")
	}
	if !strings.Contains(text, "torbox_api_call_count_total 5") {
		t.Error("missing torbox_api_call_count_total value line")
	}
	if !strings.Contains(text, "torbox_goroutine_count") {
		t.Error("missing torbox_goroutine_count metric")
	}

	// Verify CDN status code labels.
	if !strings.Contains(text, `torbox_cdn_response_count_total{code="200"} 2`) {
		t.Error("missing cdn 200 count")
	}
	if !strings.Contains(text, `torbox_cdn_response_count_total{code="404"} 1`) {
		t.Error("missing cdn 404 count")
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
		<-done
		running.Store(false)
		return nil
	}

	handler := NewHandler(m, refreshFn)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	go func() {
		http.Post(srv.URL+"/refresh", "application/json", strings.NewReader("{}"))
	}()

	for i := 0; i < 50; i++ {
		if running.Load() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !running.Load() {
		t.Fatal("first refresh never started")
	}

	resp, err := http.Post(srv.URL+"/refresh", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /refresh (2nd): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("second refresh status = %d, want %d; body: %s", resp.StatusCode, http.StatusAccepted, body)
	}

	close(done)
}

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
	handler := NewHandler(m, nil)
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