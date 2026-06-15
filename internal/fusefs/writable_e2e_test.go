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

// TestUnlink_HidesFile verifies that deleting a file via FUSE (Writable mode)
// hides the file from the mount and records it as hidden in the state DB.
// It also verifies that the DirNode fields (stateDB, cat, tbClient) are
// properly propagated — a regression test for the nil-pointer crash where
// DirNode only had cfg set and the other three fields were nil.
func TestUnlink_HidesFile(t *testing.T) {
	requireFUSE(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ── Mock CDN server (not read, but needed by StreamReader) ────────
	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not needed", http.StatusNotFound)
	}))
	defer cdnServer.Close()

	// ── Mock TorBox API server (records delete requests) ──────────────
	var deleteRequests []string
	tbServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			deleteRequests = append(deleteRequests, r.URL.Path+"?"+r.URL.Query().Encode())
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"success": true}`)
	}))
	defer tbServer.Close()

	// ── Build catalog: one download with two files ────────────────────
	tree := catalog.BuildTree([]catalog.Download{
		{
			Kind: catalog.KindTorrent,
			ID:   "100",
			Name: "Test.Show.S01",
			Hash: "abc123",
			Files: []catalog.File{
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "100",
					FileID:       "200",
					Name:         "Test.Show.S01/Test.Show.S01E01.mkv",
					Size:         1024,
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaSeries,
				},
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "100",
					FileID:       "201",
					Name:         "Test.Show.S01/Test.Show.S01E02.mkv",
					Size:         2048,
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaSeries,
				},
			},
		},
	}, false)

	// ── State DB ─────────────────────────────────────────────────────
	stateDir := t.TempDir()
	db, err := state.Open(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	defer db.Close()

	// ── Stream reader (unused, but needed by RootNode) ──────────────
	rc := cache.NewRangeCache(256*1024*1024, nil)
	cdnClient := stream.NewCDNClient(8, nil, 0)
	permalinkBuilder := func(fileKey string) string { return cdnServer.URL }
	streamer := stream.NewStreamReader(rc, cdnClient, 2, 100, int64(16*1024*1024), permalinkBuilder, nil)

	// ── Config with Writable = true ────────────────────────────────────
	cfg := &config.Config{
		APIKey:            "test-api-key",
		APIBaseURL:        tbServer.URL,
		CacheBudgetMB:     256,
		PrefetchWindowMB:  16,
		StreamMaxInflight: 2,
		StreamConcurrency: 8,
		AttrTimeoutSec:    1,
		EntryTimeoutSec:   1,
		UID:               uint32(os.Getuid()),
		GID:               uint32(os.Getgid()),
		Writable:          true,
	}

	tbClient := torbox.NewClient(&e2eTorboxConfig{apiKey: "test", baseURL: tbServer.URL})

	// ── Create FUSE root and mount ─────────────────────────────────────
	cat := catalog.NewCatalogFromTree(tree)
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
	defer func() {
		server.Unmount()
		done := make(chan struct{})
		go func() { server.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Log("timeout waiting for server.Wait after unmount")
		}
	}()

	if err := waitForMount(t, ctx, mountDir); err != nil {
		t.Fatalf("mount did not become ready: %v", err)
	}

	// ── Verify initial state: 2 files in series directory ─────────────
	seriesDir := filepath.Join(mountDir, "series", "Test.Show.S01")
	entries, err := os.ReadDir(seriesDir)
	if err != nil {
		t.Fatalf("ReadDir(series): %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 files initially, got %d: %v", len(entries), dirEntryNamesFs(entries))
	}
	t.Logf("initial files: %v", dirEntryNamesFs(entries))

	// ── Delete (unlink) one file ───────────────────────────────────────
	fileToDelete := filepath.Join(seriesDir, "Test.Show.S01E01.mkv")
	if err := os.Remove(fileToDelete); err != nil {
		t.Fatalf("os.Remove(%s): %v", fileToDelete, err)
	}

	// ── Verify: only one file remains visible ─────────────────────────
	entries, err = os.ReadDir(seriesDir)
	if err != nil {
		t.Fatalf("ReadDir(series) after unlink: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file after unlink, got %d: %v", len(entries), dirEntryNamesFs(entries))
	}
	if entries[0].Name() != "Test.Show.S01E02.mkv" {
		t.Errorf("expected Test.Show.S01E02.mkv, got %s", entries[0].Name())
	}

	// ── Verify: file is marked as hidden in DB ────────────────────────
	hidden, err := db.IsHidden("torrent:100:200")
	if err != nil {
		t.Fatalf("IsHidden: %v", err)
	}
	if !hidden {
		t.Error("file should be hidden in DB after unlink")
	}

	// The other file should NOT be hidden.
	hidden, err = db.IsHidden("torrent:100:201")
	if err != nil {
		t.Fatalf("IsHidden(other file): %v", err)
	}
	if hidden {
		t.Error("other file should NOT be hidden")
	}

	// ── Verify: no TorBox delete call yet (download not fully hidden) ──
	if len(deleteRequests) != 0 {
		t.Errorf("expected no TorBox delete calls, got %d: %v", len(deleteRequests), deleteRequests)
	}

	// ── Delete the second file ────────────────────────────────────────
	fileToDelete2 := filepath.Join(seriesDir, "Test.Show.S01E02.mkv")
	if err := os.Remove(fileToDelete2); err != nil {
		t.Fatalf("os.Remove(%s): %v", fileToDelete2, err)
	}

	// ── Verify: directory is now empty ──────────────────────────────────
	entries, err = os.ReadDir(seriesDir)
	if err != nil {
		t.Fatalf("ReadDir(series) after second unlink: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 files after both unlinked, got %d: %v", len(entries), dirEntryNamesFs(entries))
	}

	// ── Verify: both files hidden in DB ────────────────────────────────
	hidden, err = db.IsHidden("torrent:100:201")
	if err != nil {
		t.Fatalf("IsHidden(second file): %v", err)
	}
	if !hidden {
		t.Error("second file should be hidden in DB after unlink")
	}

	// ── Verify: TorBox delete was called (download fully hidden) ───────
	// Allow a brief moment for the async TorBox call.
	time.Sleep(500 * time.Millisecond)
	if len(deleteRequests) == 0 {
		t.Error("expected TorBox delete call after all files hidden, got none")
	} else {
		t.Logf("TorBox delete requests: %v", deleteRequests)
		found := false
		for _, req := range deleteRequests {
			if req == "/torrents/deletetorrent?id=100" {
				found = true
			}
		}
		if !found {
			t.Errorf("expected delete request for torrent:100, got %v", deleteRequests)
		}
	}
}
