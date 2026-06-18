package dashboard

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	srv := NewServer(d, "", "")

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
	srv := NewServer(d, "", "")

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
	srv := NewServer(d, "", "")

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
	srv := NewServer(d, "", "")

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
	srv := NewServer(d, "", "")

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
	srv := NewServer(d, "", "")

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
	srv := NewServer(d, "", "")

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
	srv := NewServer(d, "", "")
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
	srv := NewServer(d, "", "")
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
	srv := NewServer(d, "", "")
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
	srv := NewServer(d, "", "")
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
	srv := NewServer(d, "", "")
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
	srv := NewServer(d, "", "")
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
	srv := NewServer(d, "", "")
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

func TestBasicAuth_NoAuthRequired(t *testing.T) {
	d := newTestDashboard(t)
	// No auth configured — empty username/password.
	srv := NewServer(d, "", "")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("GET /api/snapshot without auth: status %d, want 200 (no auth required)", w.Code)
	}
}

func TestBasicAuth_RequiredButNotProvided(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d, "admin", "secret123")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 401 {
		t.Fatalf("GET / without auth: status %d, want 401", w.Code)
	}
	if w.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("expected WWW-Authenticate header")
	}
}

func TestBasicAuth_CorrectCredentials(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d, "admin", "secret123")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot", nil)
	req.SetBasicAuth("admin", "secret123")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("GET /api/snapshot with correct auth: status %d, want 200", w.Code)
	}
}

func TestBasicAuth_WrongPassword(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d, "admin", "secret123")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot", nil)
	req.SetBasicAuth("admin", "wrong")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 401 {
		t.Fatalf("GET /api/snapshot with wrong password: status %d, want 401", w.Code)
	}
}

func TestBasicAuth_WrongUsername(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d, "admin", "secret123")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot", nil)
	req.SetBasicAuth("root", "secret123")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 401 {
		t.Fatalf("GET /api/snapshot with wrong username: status %d, want 401", w.Code)
	}
}

func TestBasicAuth_ProtectsAllEndpoints(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d, "admin", "secret123")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	endpoints := []struct {
		method string
		path   string
		body   string
	}{
		{"GET", "/", ""},
		{"GET", "/api/snapshot", ""},
		{"GET", "/api/hidden", ""},
		{"POST", "/api/unhide", `{"download_kind":"torrent","download_id":"1"}`},
		{"POST", "/api/delete", `{"download_kind":"torrent","download_id":"1"}`},
	}

	for _, ep := range endpoints {
		var body io.Reader
		if ep.body != "" {
			body = strings.NewReader(ep.body)
		}
		req := httptest.NewRequest(ep.method, ep.path, body)
		if ep.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != 401 {
			t.Errorf("%s %s without auth: status %d, want 401", ep.method, ep.path, w.Code)
		}
	}
}

func TestBasicAuth_AuthRequiredMethod(t *testing.T) {
	d := newTestDashboard(t)

	noAuth := NewServer(d, "", "")
	if noAuth.AuthRequired() {
		t.Error("AuthRequired() = true for empty username/password, want false")
	}

	withAuth := NewServer(d, "admin", "secret")
	if !withAuth.AuthRequired() {
		t.Error("AuthRequired() = false for non-empty username/password, want true")
	}

	onlyUser := NewServer(d, "admin", "")
	if onlyUser.AuthRequired() {
		t.Error("AuthRequired() = true for only username, want false (both required)")
	}

	onlyPass := NewServer(d, "", "secret")
	if onlyPass.AuthRequired() {
		t.Error("AuthRequired() = true for only password, want false (both required)")
	}
}

func TestDownloadKindValidation_UnhideInvalid(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d, "", "")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	for _, kind := range []string{"invalid", "torrents", "", "TORRENT"} {
		body := fmt.Sprintf(`{"download_kind": %q, "download_id": "123"}`, kind)
		req := httptest.NewRequest(http.MethodPost, "/api/unhide", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != 400 {
			t.Errorf("unhide with kind=%q: status %d, want 400", kind, w.Code)
		}
	}
}

func TestDownloadKindValidation_DeleteInvalid(t *testing.T) {
	d := newTestDashboard(t)
	srv := NewServer(d, "", "")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	for _, kind := range []string{"invalid", "usenets", "", "WEBDL"} {
		body := fmt.Sprintf(`{"download_kind": %q, "download_id": "456"}`, kind)
		req := httptest.NewRequest(http.MethodPost, "/api/delete", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != 400 {
			t.Errorf("delete with kind=%q: status %d, want 400", kind, w.Code)
		}
	}
}

func TestDownloadKindValidation_ValidKinds(t *testing.T) {
	// Valid kinds should pass validation (they may fail at the TorBox call,
	// but should get past the kind-check with 500, not 400).
	d := newTestDashboard(t)
	srv := NewServer(d, "", "")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	for _, kind := range []string{"torrent", "usenet", "webdl"} {
		body := fmt.Sprintf(`{"download_kind": %q, "download_id": "123"}`, kind)
		req := httptest.NewRequest(http.MethodPost, "/api/unhide", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		// Kind passes validation; the call may fail because stateDB is nil,
		// but it should NOT be a 400 (bad kind).
		if w.Code == 400 {
			body := w.Body.String()
			t.Errorf("unhide with kind=%q: got 400, but kind is valid. Body: %s", kind, body)
		}
	}
}
