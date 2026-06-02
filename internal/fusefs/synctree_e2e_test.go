//go:build !short

package fusefs

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/catalog"
	"github.com/iiieii/torbox-fuse-go/internal/config"
	"github.com/iiieii/torbox-fuse-go/internal/state"
	"github.com/iiieii/torbox-fuse-go/internal/stream"
	"github.com/iiieii/torbox-fuse-go/internal/torbox"
)

// TestSyncTree_AddsNewFile verifies that SyncTree makes a newly added file
// appear in the FUSE mount after a catalog refresh.
func TestSyncTree_AddsNewFile(t *testing.T) {
	requireFUSE(t)

	// ── Mock CDN server ────────────────────────────────────────────────
	testData := []byte("synctree-test-data-0123456789abcdef")
	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
			return
		}
		var start, end int
		fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if end >= len(testData) {
			end = len(testData) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	}))
	defer cdnServer.Close()

	// ── Initial catalog: one movie ────────────────────────────────────
	initialTree := catalog.BuildTree([]catalog.Download{
		{
			Kind: catalog.KindTorrent,
			ID:   "100",
			Name: "First.Movie.2024",
			Files: []catalog.File{
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "100",
					FileID:       "200",
					Name:         "First.Movie.2024/First.Movie.2024.mkv",
					Size:         int64(len(testData)),
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaMovie,
				},
			},
		},
	})

	// ── State DB ──────────────────────────────────────────────────────
	stateDir := t.TempDir()
	db, err := state.Open(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	defer db.Close()

	// ── Stream reader with mock CDN ───────────────────────────────────
	rc := cache.NewRangeCache(256*1024*1024, nil)
	cdnClient := stream.NewCDNClient(8, nil, 0)
	permalinkBuilder := func(fileKey string) string { return cdnServer.URL }
	streamer := stream.NewStreamReader(rc, cdnClient, 2, 100, int64(16*1024*1024), permalinkBuilder, nil)

	// ── Config ────────────────────────────────────────────────────────
	cfg := &config.Config{
		APIKey:            "test",
		APIBaseURL:        "http://localhost:0",
		CacheBudgetMB:     256,
		PrefetchWindowMB:  16,
		StreamMaxInflight: 2,
		StreamConcurrency: 8,
		AttrTimeoutSec:    1,
		EntryTimeoutSec:   1,
		UID:               uint32(os.Getuid()),
		GID:               uint32(os.Getgid()),
	}

	tbClient := torbox.NewClient(&e2eTorboxConfig{apiKey: "test", baseURL: "http://localhost:0"})

	// ── Create FUSE root and mount ────────────────────────────────────
	cat := catalog.NewCatalogFromTree(initialTree)
	root := NewRootNode(cat, db, streamer, cfg, tbClient)
	cat.SetOnRefresh(func() { root.SyncTree(context.Background()) })

	mountDir := t.TempDir()
	cfg.MountPath = mountDir

	attrTimeout := time.Duration(cfg.AttrTimeoutSec) * time.Second
	entryTimeout := time.Duration(cfg.EntryTimeoutSec) * time.Second
	server, err := fs.Mount(mountDir, root, &fs.Options{
		AttrTimeout:  &attrTimeout,
		EntryTimeout: &entryTimeout,
		MountOptions: fuse.MountOptions{
			FsName: "torbox-media-center",
			Debug:  false,
		},
	})
	if err != nil {
		t.Fatalf("fs.Mount: %v", err)
	}
	defer server.Unmount()

	// Wait for mount to stabilise.
	time.Sleep(200 * time.Millisecond)

	// ── Verify initial state: one movie ──────────────────────────────
	moviesDir := filepath.Join(mountDir, "movies")
	entries, err := os.ReadDir(moviesDir)
	if err != nil {
		t.Fatalf("ReadDir(movies): %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 movie dir initially, got %d", len(entries))
	}
	t.Logf("initial movies: %v", dirEntryNamesFs(entries))

	// ── Simulate catalog refresh with a second movie ──────────────────
	updatedTree := catalog.BuildTree([]catalog.Download{
		{
			Kind: catalog.KindTorrent,
			ID:   "100",
			Name: "First.Movie.2024",
			Files: []catalog.File{
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "100",
					FileID:       "200",
					Name:         "First.Movie.2024/First.Movie.2024.mkv",
					Size:         int64(len(testData)),
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaMovie,
				},
			},
		},
		{
			Kind: catalog.KindTorrent,
			ID:   "101",
			Name: "Second.Movie.2025",
			Files: []catalog.File{
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "101",
					FileID:       "201",
					Name:         "Second.Movie.2025/Second.Movie.2025.mkv",
					Size:         int64(len(testData)),
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaMovie,
				},
			},
		},
	})

	// Swap the tree and trigger SyncTree (same as what Catalog.Refresh does).
	cat.SetTree(updatedTree)
	root.SyncTree(context.Background())

	// Give the kernel a moment to pick up the notification.
	time.Sleep(500 * time.Millisecond)

	// ── Verify: two movies now visible ────────────────────────────────
	entries, err = os.ReadDir(moviesDir)
	if err != nil {
		t.Fatalf("ReadDir(movies) after sync: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 movie dirs after SyncTree, got %d: %v", len(entries), dirEntryNamesFs(entries))
	}
	t.Logf("movies after sync: %v", dirEntryNamesFs(entries))

	// Verify the new movie directory is readable.
	newMovieDir := filepath.Join(moviesDir, "Second Movie 2025")
	fileEntries, err := os.ReadDir(newMovieDir)
	if err != nil {
		t.Fatalf("ReadDir(new movie): %v", err)
	}
	if len(fileEntries) != 1 {
		t.Fatalf("expected 1 file in new movie dir, got %d", len(fileEntries))
	}

	// Verify the new file is stat-able.
	newFilePath := filepath.Join(newMovieDir, "Second.Movie.2025.mkv")
	fi, err := os.Stat(newFilePath)
	if err != nil {
		t.Fatalf("Stat(new file): %v", err)
	}
	if fi.Size() != int64(len(testData)) {
		t.Errorf("new file size = %d, want %d", fi.Size(), len(testData))
	}
	if !fi.Mode().IsRegular() {
		t.Errorf("new file mode = %v, expected regular file", fi.Mode())
	}
}
