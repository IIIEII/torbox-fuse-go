package torbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/catalog"
)

type testConfig struct {
	baseURL string
}

func (c *testConfig) APIKey() string            { return "testkey" }
func (c *testConfig) APIBaseURL() string        { return c.baseURL }
func (c *testConfig) APITimeout() time.Duration { return 30 * time.Second }

func TestListTorrents(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/torrents/mylist" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"id":     float64(123),
					"hash":   "abc123",
					"name":   "Test Movie",
					"cached": true,
					"files": []map[string]interface{}{
						{
							"id":         float64(456),
							"short_name": "test.mkv",
							"name":       "Test Movie/test.mkv",
							"size":       float64(1073741824),
							"mimetype":   "video/x-matroska",
						},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	cfg := &testConfig{baseURL: ts.URL}
	client := NewClient(cfg)
	downloads, err := client.ListDownloads(context.Background(), catalog.KindTorrent)
	if err != nil {
		t.Fatalf("ListDownloads error: %v", err)
	}
	if len(downloads) != 1 {
		t.Fatalf("expected 1 download, got %d", len(downloads))
	}
	d := downloads[0]
	if d.Kind != catalog.KindTorrent {
		t.Errorf("Kind = %q, want torrent", d.Kind)
	}
	if d.ID != "123" {
		t.Errorf("ID = %q, want 123", d.ID)
	}
	if len(d.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(d.Files))
	}
	if d.Files[0].FileID != "456" {
		t.Errorf("FileID = %q, want 456", d.Files[0].FileID)
	}
}

func TestRateLimitingConcurrency(t *testing.T) {
	var concurrent int64
	var maxConcurrent int64
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt64(&concurrent, 1)
		mu.Lock()
		if cur > maxConcurrent {
			maxConcurrent = cur
		}
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt64(&concurrent, -1)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"data": []interface{}{}})
	}))
	defer ts.Close()

	cfg := &testConfig{baseURL: ts.URL}
	client := NewClient(cfg)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.ListDownloads(context.Background(), catalog.KindTorrent)
		}()
	}
	wg.Wait()

	if maxConcurrent > 1 {
		t.Errorf("max concurrent API calls = %d, want <= 1", maxConcurrent)
	}
}

func TestRetryOn429(t *testing.T) {
	attempts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"data": []interface{}{}})
	}))
	defer ts.Close()

	cfg := &testConfig{baseURL: ts.URL}
	client := NewClient(cfg)
	_, err := client.ListDownloads(context.Background(), catalog.KindTorrent)
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if attempts < 3 {
		t.Errorf("expected at least 3 attempts, got %d", attempts)
	}
}

func TestPermalinkURL(t *testing.T) {
	baseURL := "https://api.torbox.app/v1/api"
	tests := []struct {
		kind    catalog.DownloadKind
		id      string
		fileID  string
		wantURL string
	}{
		{catalog.KindTorrent, "100", "200", "https://api.torbox.app/v1/api/torrents/requestdl?token=testkey&torrent_id=100&file_id=200&redirect=true"},
		{catalog.KindUsenet, "100", "200", "https://api.torbox.app/v1/api/usenet/requestdl?token=testkey&usenet_id=100&file_id=200&redirect=true"},
		{catalog.KindWebDL, "100", "200", "https://api.torbox.app/v1/api/webdl/requestdl?token=testkey&web_id=100&file_id=200&redirect=true"},
	}
	for _, tt := range tests {
		got := PermalinkURL(baseURL, "testkey", tt.kind, tt.id, tt.fileID)
		if got != tt.wantURL {
			t.Errorf("PermalinkURL(%q, %v, %q, %q, %q) = %q, want %q",
				baseURL, tt.kind, "testkey", tt.id, tt.fileID, got, tt.wantURL)
		}
	}
}

func TestNoRetryOn4xx(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()

	cfg := &testConfig{baseURL: ts.URL}
	client := NewClient(cfg)
	_, err := client.ListDownloads(context.Background(), catalog.KindTorrent)
	if err == nil {
		t.Fatal("expected error for 403 response")
	}
}

func TestSkipsUncached(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"id":     float64(1),
					"hash":   "abc",
					"name":   "Uncached Item",
					"cached": false,
					"files": []map[string]interface{}{
						{
							"id":         float64(10),
							"short_name": "file.mkv",
							"name":       "file.mkv",
							"size":       float64(1024),
							"mimetype":   "video/x-matroska",
						},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	cfg := &testConfig{baseURL: ts.URL}
	client := NewClient(cfg)
	downloads, err := client.ListDownloads(context.Background(), catalog.KindTorrent)
	if err != nil {
		t.Fatal(err)
	}
	if len(downloads) != 0 {
		t.Errorf("expected 0 downloads (uncached), got %d", len(downloads))
	}
}

func TestDeleteDownload(t *testing.T) {
	tests := []struct {
		kind     catalog.DownloadKind
		wantPath string
		wantBody map[string]interface{}
	}{
		{catalog.KindTorrent, "/torrents/controltorrent", map[string]interface{}{"torrent_id": float64(12345), "operation": "delete"}},
		{catalog.KindUsenet, "/usenet/controlusenetdownload", map[string]interface{}{"usenet_id": "12345", "operation": "delete"}},
		{catalog.KindWebDL, "/webdl/controlwebdownload", map[string]interface{}{"webdl_id": float64(12345), "operation": "delete"}},
	}
	for _, tt := range tests {
		t.Run(string(tt.kind), func(t *testing.T) {
			var gotPath string
			var gotMethod string
			var gotBody map[string]interface{}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotMethod = r.Method
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				gotBody = body
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": true,
				})
			}))
			defer ts.Close()

			cfg := &testConfig{baseURL: ts.URL}
			client := NewClient(cfg)

			err := client.DeleteDownload(context.Background(), tt.kind, "12345")
			if err != nil {
				t.Fatalf("DeleteDownload returned error: %v", err)
			}
			if gotPath != tt.wantPath {
				t.Errorf("path = %q, want %q", gotPath, tt.wantPath)
			}
			if gotMethod != http.MethodPost {
				t.Errorf("method = %q, want %q", gotMethod, http.MethodPost)
			}
			for k, v := range tt.wantBody {
				if gotBody[k] != v {
					t.Errorf("body[%q] = %v, want %v", k, gotBody[k], v)
				}
			}
		})
	}
}

func TestRedactToken(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "token parameter redacted",
			url:  "https://api.torbox.app/v1/api/torrents/requestdl?token=secretkey123&torrent_id=100&file_id=200&redirect=true",
			want: "https://api.torbox.app/v1/api/torrents/requestdl?token=***&torrent_id=100&file_id=200&redirect=true",
		},
		{
			name: "no token parameter",
			url:  "https://api.torbox.app/v1/api/torrents/mylist",
			want: "https://api.torbox.app/v1/api/torrents/mylist",
		},
		{
			name: "empty token value",
			url:  "https://example.com/path?token=&id=1",
			want: "https://example.com/path?token=***&id=1",
		},
		{
			name: "unparseable URL returned as-is",
			url:  "://invalid",
			want: "://invalid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactToken(tt.url)
			if got != tt.want {
				t.Errorf("RedactToken(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

func TestDeleteDownload_NotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"detail":  "torrent not found",
		})
	}))
	defer ts.Close()

	cfg := &testConfig{baseURL: ts.URL}
	client := NewClient(cfg)

	err := client.DeleteDownload(context.Background(), catalog.KindTorrent, "99999")
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
}
