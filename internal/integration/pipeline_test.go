package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/catalog"
	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"github.com/iiieii/torbox-fuse-go/internal/state"
	"github.com/iiieii/torbox-fuse-go/internal/stream"
	"github.com/iiieii/torbox-fuse-go/internal/torbox"
)

func TestFullPipeline_CatalogToStreamRead(t *testing.T) {
	// Mock TorBox API server — only serves list endpoints.
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/api/torrents/mylist":
			resp := map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id":     float64(100),
						"hash":   "abc123def456",
						"name":   "Test Movie 2024",
						"cached": true,
						"files": []map[string]interface{}{
							{
								"id":         float64(200),
								"short_name": "Test.Movie.2024.mkv",
								"name":       "Test Movie 2024/Test.Movie.2024.mkv",
								"size":       float64(4 * 1024 * 1024),
								"mimetype":   "video/x-matroska",
							},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		case "/v1/api/usenet/mylist", "/v1/api/webdl/mylist":
			json.NewEncoder(w).Encode(map[string]interface{}{"data": []interface{}{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer apiServer.Close()

	// Mock CDN server that serves range requests.
	cdnData := make([]byte, 4*1024*1024)
	for i := range cdnData {
		cdnData[i] = byte(i % 256)
	}
	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.WriteHeader(http.StatusOK)
			w.Write(cdnData)
			return
		}
		var start, end int
		n, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if err != nil || n != 2 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if end >= len(cdnData) {
			end = len(cdnData) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(cdnData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(cdnData[start : end+1])
	}))
	defer cdnServer.Close()

	// Create TorBox API client pointing to mock server.
	cfg := &testConfig{baseURL: apiServer.URL + "/v1/api", apiKey: "test-key"}
	tbClient := torbox.NewClient(cfg)

	// Create state DB in temp dir.
	db, err := state.Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create metrics.
	m := metrics.New()

	// Create catalog and refresh.
	cat := catalog.NewCatalog(tbClient, db, m)
	ctx := context.Background()
	if err := cat.Refresh(ctx); err != nil {
		t.Fatalf("catalog refresh: %v", err)
	}

	// Verify catalog has items.
	tree := cat.Tree()
	if tree == nil {
		t.Fatal("tree is nil after refresh")
	}

	moviesDirs := tree.ListDir("/movies")
	if len(moviesDirs) == 0 {
		t.Fatal("no movie directories in tree")
	}

	// Look up the file.
	movieFile := tree.Lookup("/movies/Test Movie 2024/Test.Movie.2024.mkv")
	if movieFile == nil {
		t.Fatal("movie file not found in tree")
	}
	if movieFile.Size != 4*1024*1024 {
		t.Errorf("file size = %d, want %d", movieFile.Size, 4*1024*1024)
	}

	// Verify inode stability.
	inode1, err := db.LookupInode(movieFile.ContentKey())
	if err != nil {
		t.Fatalf("lookup inode: %v", err)
	}
	inode2, err := db.AssignInode(movieFile.ContentKey(), "/movies/Test Movie 2024/Test.Movie.2024.mkv")
	if err != nil {
		t.Fatalf("assign inode: %v", err)
	}
	if inode1 != inode2 {
		t.Errorf("inode changed: %d -> %d", inode1, inode2)
	}

	// Set up stream reader with mock CDN.
	rc := cache.NewRangeCache(256 * 1024 * 1024)
	cdnClient := stream.NewCDNClient(8)
	permalinkBuilder := func(fileKey string) string {
		return cdnServer.URL
	}
	prefetchBytes := int64(16 * 1024 * 1024)
	streamer := stream.NewStreamReader(rc, cdnClient, 2, prefetchBytes, permalinkBuilder)

	// Read from stream.
	buf := make([]byte, 1024)
	n, err := streamer.ReadAt(ctx, movieFile.ContentKey(), 0, buf, movieFile.Size)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 1024 {
		t.Errorf("ReadAt returned %d bytes, want 1024", n)
	}

	// Verify data correctness.
	for i := 0; i < n; i++ {
		if buf[i] != byte(i%256) {
			t.Errorf("buf[%d] = %d, want %d", i, buf[i], byte(i%256))
			break
		}
	}

	// Second read should be a cache hit.
	buf2 := make([]byte, 512)
	n2, err := streamer.ReadAt(ctx, movieFile.ContentKey(), 0, buf2, movieFile.Size)
	if err != nil {
		t.Fatalf("ReadAt (cached): %v", err)
	}
	if n2 != 512 {
		t.Errorf("cached ReadAt returned %d bytes, want 512", n2)
	}

	t.Logf("pipeline test passed: catalog=%d items, inode=%d, read=%d bytes", len(moviesDirs), inode1, n)
}

// testConfig implements torbox.Config for testing.
type testConfig struct {
	baseURL string
	apiKey  string
}

func (c *testConfig) APIKey() string           { return c.apiKey }
func (c *testConfig) APIBaseURL() string        { return c.baseURL }
func (c *testConfig) APITimeout() time.Duration { return 30 * time.Second }