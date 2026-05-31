package dashboard

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"github.com/iiieii/torbox-fuse-go/internal/stream"
)

func newTestDashboard(t *testing.T) *Dashboard {
	t.Helper()
	rc := cache.NewRangeCache(4*1024*1024, nil)
	cdn := stream.NewCDNClient(2, nil, 0)
	sr := stream.NewStreamReader(rc, cdn, 2, 16, 1024*1024, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)
	m := metrics.New()
	return New(sr, rc, nil, m)
}

func TestServerIndex(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("GET /: status %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", ct)
	}
	body := w.Body.String()
	if !strings.Contains(body, "TorBox Media Center") {
		t.Fatalf("response body missing title")
	}
}

func TestServerCSS(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/style.css", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("GET /style.css: status %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/css") {
		t.Fatalf("Content-Type = %q, want text/css", ct)
	}
}

func TestServerJS(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/app.js", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("GET /app.js: status %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "javascript") {
		t.Fatalf("Content-Type = %q, want application/javascript", ct)
	}
}

func TestServerSnapshot(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("GET /api/snapshot: status %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	body := w.Body.String()
	// Should be valid JSON with expected fields.
	if !strings.Contains(body, `"timestamp"`) {
		t.Fatalf("snapshot missing timestamp field")
	}
	if !strings.Contains(body, `"summary"`) {
		t.Fatalf("snapshot missing summary field")
	}
}

func TestServerSnapshotMethodNotAllowed(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/snapshot", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 405 {
		t.Fatalf("POST /api/snapshot: status %d, want 405", w.Code)
	}
}

func TestServerSSE(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	// Use httptest.Server for SSE test (needs real connection for streaming).
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Connect to SSE endpoint and read one event.
	client := ts.Client()
	client.Timeout = 3 * time.Second

	resp, err := client.Get(ts.URL + "/api/state")
	if err != nil {
		t.Fatalf("GET /api/state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("GET /api/state: status %d, want 200", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Read one SSE event from the stream.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	// Find a "data:" line.
	var gotData bool
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			gotData = true
			// The data should be valid JSON with a timestamp.
			jsonStr := strings.TrimPrefix(line, "data: ")
			if !strings.Contains(jsonStr, `"timestamp"`) {
				t.Fatalf("SSE data missing timestamp: %s", jsonStr[:100])
			}
			break
		}
	}
	if !gotData {
		t.Fatal("expected at least one SSE data event, got none")
	}
}

func TestServerIndexNotFound(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 404 {
		t.Fatalf("GET /nonexistent: status %d, want 404", w.Code)
	}
}
