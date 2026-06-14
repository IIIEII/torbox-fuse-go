package dashboard

import (
	"bufio"
	"bytes"
	"encoding/json"
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
	return New(sr, rc, nil, m, nil, nil)
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

func TestServerHiddenEndpoint(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	// GET /api/hidden returns 200 with JSON array (empty when no stateDB).
	req := httptest.NewRequest(http.MethodGet, "/api/hidden", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("GET /api/hidden: status %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	// Without stateDB, hidden downloads should be null (not crash).
	body := w.Body.String()
	if !strings.Contains(body, "null") && !strings.Contains(body, "[]") {
		t.Fatalf("expected null or empty array, got: %s", body)
	}
}

func TestServerUnhideBadMethod(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/unhide", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 405 {
		t.Fatalf("GET /api/unhide: status %d, want 405", w.Code)
	}
}

func TestServerDeleteBadMethod(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/delete", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 405 {
		t.Fatalf("GET /api/delete: status %d, want 405", w.Code)
	}
}

func TestServerUnhideMissingFields(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	// POST with empty JSON body.
	body := bytes.NewBufferString("{}")
	req := httptest.NewRequest(http.MethodPost, "/api/unhide", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Fatalf("POST /api/unhide with empty body: status %d, want 400", w.Code)
	}
}

func TestServerDeleteMissingFields(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	// POST with only download_kind, missing download_id.
	payload := `{"download_kind": "torrent"}`
	req := httptest.NewRequest(http.MethodPost, "/api/delete", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Fatalf("POST /api/delete missing download_id: status %d, want 400", w.Code)
	}
}

func TestServerUnhideInvalidJSON(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/unhide", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Fatalf("POST /api/unhide with invalid JSON: status %d, want 400", w.Code)
	}
}

func TestServerSnapshotIncludesHiddenDownloads(t *testing.T) {
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
	// The response should include hidden_downloads field (null without stateDB).
	body := w.Body.String()
	if !strings.Contains(body, "hidden_downloads") {
		t.Fatalf("snapshot missing hidden_downloads field: %s", body[:200])
	}
	// Parse to verify structure.
	var snap map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		t.Fatalf("failed to parse snapshot JSON: %v", err)
	}
	if _, ok := snap["hidden_downloads"]; !ok {
		t.Fatal("hidden_downloads key missing from snapshot")
	}
}
