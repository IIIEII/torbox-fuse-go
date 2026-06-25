//go:build !short

package fusefs

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
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
	// BuildTree places series files under /series/{title}/Season {N}/.
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

	// Seed the files table so IsDownloadFullyHidden can count them.
	if err := db.UpsertFiles([]state.FileRecord{
		{ContentKey: "torrent:100:200", DownloadKind: "torrent", DownloadID: "100", FileID: "200", Path: "Test.Show.S01/Test.Show.S01E01.mkv", Size: 1024},
		{ContentKey: "torrent:100:201", DownloadKind: "torrent", DownloadID: "100", FileID: "201", Path: "Test.Show.S01/Test.Show.S01E02.mkv", Size: 2048},
	}); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

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

	// ── Verify initial state: 2 files in Season 1 directory ──────────
	seasonDir := filepath.Join(mountDir, "series", "Test.Show.S01", "Season 1")
	entries, err := os.ReadDir(seasonDir)
	if err != nil {
		t.Fatalf("ReadDir(season dir): %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 files initially, got %d: %v", len(entries), dirEntryNamesFs(entries))
	}
	t.Logf("initial files: %v", dirEntryNamesFs(entries))

	// ── Delete (unlink) one file ───────────────────────────────────────
	fileToDelete := filepath.Join(seasonDir, "Test.Show.S01E01.mkv")
	if err := os.Remove(fileToDelete); err != nil {
		t.Fatalf("os.Remove(%s): %v", fileToDelete, err)
	}

	// ── Verify: only one file remains visible ─────────────────────────
	entries, err = os.ReadDir(seasonDir)
	if err != nil {
		t.Fatalf("ReadDir(season dir) after unlink: %v", err)
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

	// ── Verify: no TorBox delete call (deletion is manual via dashboard) ──
	if len(deleteRequests) != 0 {
		t.Errorf("expected no TorBox delete calls (deletion is manual), got %d: %v", len(deleteRequests), deleteRequests)
	}

	// ── Delete the second file ────────────────────────────────────────
	fileToDelete2 := filepath.Join(seasonDir, "Test.Show.S01E02.mkv")
	if err := os.Remove(fileToDelete2); err != nil {
		t.Fatalf("os.Remove(%s): %v", fileToDelete2, err)
	}

	// ── Verify: directory is now empty (or pruned) ─────────────────────
	// After both files are unlinked, pruneEmptyDirs may remove the now-empty
	// Season directory from the tree (correct production behavior). The prune
	// notification is async, so we poll: either ReadDir returns 0 entries, or
	// the directory is gone (ENOENT) — both confirm no files remain.
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, err = os.ReadDir(seasonDir)
		if err == nil && len(entries) == 0 {
			break
		}
		if os.IsNotExist(err) {
			break // directory was pruned — acceptable
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatalf("ReadDir(season dir) after second unlink: %v", err)
			}
			t.Fatalf("expected 0 files after both unlinked, got %d: %v", len(entries), dirEntryNamesFs(entries))
		}
		time.Sleep(50 * time.Millisecond)
	}

	// ── Verify: both files are now hidden in DB ───────────────────────
	hidden, err = db.IsHidden("torrent:100:201")
	if err != nil {
		t.Fatalf("IsHidden(second file): %v", err)
	}
	if !hidden {
		t.Error("second file should be hidden in DB after unlink")
	}

	// ── Verify: still no TorBox delete call (deletion is manual) ──────
	if len(deleteRequests) != 0 {
		t.Errorf("expected no TorBox delete calls (deletion is manual), got %d: %v", len(deleteRequests), deleteRequests)
	}
}

// TestUnlink_ReadOnly_ReturnsEROFS verifies that deleting a file via FUSE
// returns EROFS when Writable mode is disabled.
func TestUnlink_ReadOnly_ReturnsEROFS(t *testing.T) {
	requireFUSE(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ── Mock CDN server (not read, but needed by StreamReader) ────────
	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not needed", http.StatusNotFound)
	}))
	defer cdnServer.Close()

	// ── Build catalog: one movie file ─────────────────────────────────
	tree := catalog.BuildTree([]catalog.Download{
		{
			Kind: catalog.KindTorrent,
			ID:   "200",
			Name: "Test.Movie.2024",
			Hash: "def456",
			Files: []catalog.File{
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "200",
					FileID:       "300",
					Name:         "Test.Movie.2024/Test.Movie.2024.mkv",
					Size:         4096,
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaMovie,
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

	// ── Config with Writable = false (default) ────────────────────────
	cfg := &config.Config{
		APIKey:            "test-api-key",
		APIBaseURL:        "http://localhost:0",
		CacheBudgetMB:     256,
		PrefetchWindowMB:  16,
		StreamMaxInflight: 2,
		StreamConcurrency: 8,
		AttrTimeoutSec:    1,
		EntryTimeoutSec:   1,
		UID:               uint32(os.Getuid()),
		GID:               uint32(os.Getgid()),
		Writable:          false, // read-only
	}

	tbClient := torbox.NewClient(&e2eTorboxConfig{apiKey: "test", baseURL: "http://localhost:0"})

	// ── Create FUSE root and mount ─────────────────────────────────────
	cat := catalog.NewCatalogFromTree(tree)
	root := NewRootNode(cat, db, streamer, cfg, tbClient)

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

	// ── Verify: file exists ───────────────────────────────────────────
	filePath := filepath.Join(mountDir, "movies", "Test.Movie.2024", "Test.Movie.2024.mkv")
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("Stat(movie file): %v", err)
	}

	// ── Verify: os.Remove returns EROFS (read-only filesystem) ─────────
	err = os.Remove(filePath)
	if err == nil {
		t.Fatal("expected os.Remove to fail on read-only mount, but it succeeded")
	}
	// On macOS, the error is EROFS; on Linux it may be EPERM.
	// Both map to "read-only file system" or "operation not permitted".
	pathErr, ok := err.(*os.PathError)
	if !ok {
		t.Fatalf("expected *os.PathError, got %T: %v", err, err)
	}
	if pathErr.Err != syscall.EROFS && pathErr.Err != syscall.EPERM {
		t.Errorf("expected EROFS or EPERM, got %v (raw: %v)", pathErr.Err, err)
	}

	// ── Verify: file is still visible after failed remove ──────────────
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("file should still exist after failed remove: %v", err)
	}

	// ── Verify: file is NOT marked as hidden in DB ────────────────────
	hidden, err := db.IsHidden("torrent:200:300")
	if err != nil {
		t.Fatalf("IsHidden: %v", err)
	}
	if hidden {
		t.Error("file should NOT be hidden in DB after failed unlink")
	}
}

// TestRmdir_HidesAllFiles verifies that removing a directory via FUSE Rmdir
// (Writable mode) hides all files in that directory. When all files of a
// download are hidden, the download is logged as ready for manual deletion
// via the dashboard — no automatic TorBox deletion is performed.
//
// This is a regression test for two bugs:
//  1. Rmdir called RmAllChildren() before collecting fileNodes, so fileNodes
//     was always empty and no files were hidden.
//  2. NotifyEntry inside Unlink/Rmdir caused a FUSE deadlock on macOS because
//     the kernel is waiting for the handler to return while NotifyEntry tries
//     to write to /dev/fuse.
func TestRmdir_HidesAllFiles(t *testing.T) {
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

	// ── Build catalog: one download with three files in Season 1 ──────
	tree := catalog.BuildTree([]catalog.Download{
		{
			Kind: catalog.KindTorrent,
			ID:   "500",
			Name: "My.Series.S02",
			Hash: "xyz789",
			Files: []catalog.File{
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "500",
					FileID:       "501",
					Name:         "My.Series.S02/My.Series.S02E01.mkv",
					Size:         1024,
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaSeries,
				},
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "500",
					FileID:       "502",
					Name:         "My.Series.S02/My.Series.S02E02.mkv",
					Size:         2048,
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaSeries,
				},
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "500",
					FileID:       "503",
					Name:         "My.Series.S02/My.Series.S02E03.mkv",
					Size:         3072,
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

	// Seed the files table so IsDownloadFullyHidden can count them.
	if err := db.UpsertFiles([]state.FileRecord{
		{ContentKey: "torrent:500:501", DownloadKind: "torrent", DownloadID: "500", FileID: "501", Path: "My.Series.S02/My.Series.S02E01.mkv", Size: 1024},
		{ContentKey: "torrent:500:502", DownloadKind: "torrent", DownloadID: "500", FileID: "502", Path: "My.Series.S02/My.Series.S02E02.mkv", Size: 2048},
		{ContentKey: "torrent:500:503", DownloadKind: "torrent", DownloadID: "500", FileID: "503", Path: "My.Series.S02/My.Series.S02E03.mkv", Size: 3072},
	}); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

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

	// ── Verify initial state: 3 files in Season 2 directory ──────────
	seasonDir := filepath.Join(mountDir, "series", "My.Series.S02", "Season 2")
	entries, err := os.ReadDir(seasonDir)
	if err != nil {
		t.Fatalf("ReadDir(season dir): %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 files initially, got %d: %v", len(entries), dirEntryNamesFs(entries))
	}
	t.Logf("initial files: %v", dirEntryNamesFs(entries))

	// ── Remove the Season 2 directory (Rmdir) ──────────────────────────
	// Plex deletes entire season directories this way.
	if err := os.Remove(seasonDir); err != nil {
		t.Fatalf("os.Remove(season dir): %v", err)
	}

	// ── Verify: all three files are hidden in DB ─────────────────────────
	// Unlink/Rmdir only hides files; deletion from TorBox is manual via dashboard.
	for _, key := range []string{
		"torrent:500:501",
		"torrent:500:502",
		"torrent:500:503",
	} {
		hidden, err := db.IsHidden(key)
		if err != nil {
			t.Fatalf("IsHidden(%s): %v", key, err)
		}
		if !hidden {
			t.Errorf("file %s should be hidden after rmdir", key)
		}
	}

	// ── Verify: download is marked as fully hidden ──────────────────────
	fullyHidden, err := db.IsDownloadFullyHidden("torrent", "500")
	if err != nil {
		t.Fatalf("IsDownloadFullyHidden: %v", err)
	}
	if !fullyHidden {
		t.Error("download should be fully hidden after all files removed")
	}

	// ── Verify: NO TorBox delete call (deletion is manual via dashboard) ──
	if len(deleteRequests) != 0 {
		t.Errorf("expected no TorBox delete calls (deletion is manual), got %d: %v", len(deleteRequests), deleteRequests)
	}
}

// TestUnlink_HidesFromAllDir verifies that when a file is unlinked in /series/,
// it also disappears from /all/. This is a regression test for a bug where
// HideFile only removed the file from the directory where Unlink was called,
// leaving stale entries in /all/ until restart.
func TestUnlink_HidesFromAllDir(t *testing.T) {
	requireFUSE(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ── Mock CDN server (not read, but needed by StreamReader) ────────
	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not needed", http.StatusNotFound)
	}))
	defer cdnServer.Close()

	// ── Build catalog: one series with two episodes, /all enabled ──────
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
	}, true) // allDir = true

	// ── State DB ─────────────────────────────────────────────────────
	stateDir := t.TempDir()
	db, err := state.Open(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	defer db.Close()

	if err := db.UpsertFiles([]state.FileRecord{
		{ContentKey: "torrent:100:200", DownloadKind: "torrent", DownloadID: "100", FileID: "200", Path: "/series/Test.Show.S01/Season 1/Test.Show.S01E01.mkv", Size: 1024},
		{ContentKey: "torrent:100:201", DownloadKind: "torrent", DownloadID: "100", FileID: "201", Path: "/series/Test.Show.S01/Season 1/Test.Show.S01E02.mkv", Size: 2048},
	}); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

	// ── Stream reader (unused, but needed by RootNode) ──────────────
	rc := cache.NewRangeCache(256*1024*1024, nil)
	cdnClient := stream.NewCDNClient(8, nil, 0)
	permalinkBuilder := func(fileKey string) string { return cdnServer.URL }
	streamer := stream.NewStreamReader(rc, cdnClient, 2, 100, int64(16*1024*1024), permalinkBuilder, nil)

	// ── Config with Writable = true and AllDir = true ───────────────
	cfg := &config.Config{
		APIKey:            "test-api-key",
		APIBaseURL:        "http://localhost:0",
		CacheBudgetMB:     256,
		PrefetchWindowMB:  16,
		StreamMaxInflight: 2,
		StreamConcurrency: 8,
		AttrTimeoutSec:    1,
		EntryTimeoutSec:   1,
		UID:               uint32(os.Getuid()),
		GID:               uint32(os.Getgid()),
		Writable:          true,
		AllDirEnabled:     true,
	}

	tbClient := torbox.NewClient(&e2eTorboxConfig{apiKey: "test", baseURL: "http://localhost:0"})

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

	// ── Verify initial state: file exists in both /series/ and /all/ ──
	seriesFile := filepath.Join(mountDir, "series", "Test.Show.S01", "Season 1", "Test.Show.S01E01.mkv")
	allFile := filepath.Join(mountDir, "all", "Test.Show.S01", "Test.Show.S01E01.mkv")

	if _, err := os.Stat(seriesFile); err != nil {
		t.Fatalf("series file should exist initially: %v", err)
	}
	if _, err := os.Stat(allFile); err != nil {
		t.Fatalf("all file should exist initially: %v", err)
	}

	// ── Delete (unlink) the file from /series/ ────────────────────────
	if err := os.Remove(seriesFile); err != nil {
		t.Fatalf("os.Remove(series file): %v", err)
	}

	// ── Verify: file is gone from /series/ ────────────────────────────
	// The kernel drops the dentry for the unlinked entry automatically as
	// part of the Unlink reply, so this is deterministic.
	if _, err := os.Stat(seriesFile); !os.IsNotExist(err) {
		t.Errorf("series file should be gone after unlink, got err=%v", err)
	}

	// ── Verify: file is ALSO gone from /all/ (the bug being fixed) ────
	// Cross-directory cache invalidation (NotifyEntry on /all/) is dispatched
	// asynchronously from the Unlink handler to avoid deadlocking on /dev/fuse,
	// so the kernel dentry cache for /all/ is invalidated eventually rather than
	// synchronously. Poll until the file disappears.
	if err := waitForGone(t, ctx, allFile); err != nil {
		t.Errorf("all file should be gone after unlink (cross-directory hide): %v", err)
	}

	// ── Verify: file is marked as hidden in DB ────────────────────────
	hidden, err := db.IsHidden("torrent:100:200")
	if err != nil {
		t.Fatalf("IsHidden: %v", err)
	}
	if !hidden {
		t.Error("file should be hidden in DB after unlink")
	}
}

// TestRmdir_RemovesDirectory verifies that when a directory is removed via Rmdir,
// the directory itself is removed from the FUSE tree (not just its files hidden).
// This is a regression test for a bug where empty directories would reappear
// after restart because Rmdir hid the files but left the directory in place.
func TestRmdir_RemovesDirectory(t *testing.T) {
	requireFUSE(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ── Mock CDN server (not read, but needed by StreamReader) ────────
	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not needed", http.StatusNotFound)
	}))
	defer cdnServer.Close()

	// ── Build catalog: one series with two episodes ────────────────────
	tree := catalog.BuildTree([]catalog.Download{
		{
			Kind: catalog.KindTorrent,
			ID:   "600",
			Name: "My.Show.S03",
			Hash: "zzz999",
			Files: []catalog.File{
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "600",
					FileID:       "601",
					Name:         "My.Show.S03/My.Show.S03E01.mkv",
					Size:         1024,
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaSeries,
				},
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "600",
					FileID:       "602",
					Name:         "My.Show.S03/My.Show.S03E02.mkv",
					Size:         2048,
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaSeries,
				},
			},
		},
	}, true) // allDir = true

	// ── State DB ─────────────────────────────────────────────────────
	stateDir := t.TempDir()
	db, err := state.Open(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	defer db.Close()

	if err := db.UpsertFiles([]state.FileRecord{
		{ContentKey: "torrent:600:601", DownloadKind: "torrent", DownloadID: "600", FileID: "601", Path: "/series/My.Show.S03/Season 3/My.Show.S03E01.mkv", Size: 1024},
		{ContentKey: "torrent:600:602", DownloadKind: "torrent", DownloadID: "600", FileID: "602", Path: "/series/My.Show.S03/Season 3/My.Show.S03E02.mkv", Size: 2048},
	}); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

	// ── Stream reader ────────────────────────────────────────────────
	rc := cache.NewRangeCache(256*1024*1024, nil)
	cdnClient := stream.NewCDNClient(8, nil, 0)
	permalinkBuilder := func(fileKey string) string { return cdnServer.URL }
	streamer := stream.NewStreamReader(rc, cdnClient, 2, 100, int64(16*1024*1024), permalinkBuilder, nil)

	// ── Config with Writable = true and AllDir = true ───────────────
	cfg := &config.Config{
		APIKey:            "test-api-key",
		APIBaseURL:        "http://localhost:0",
		CacheBudgetMB:     256,
		PrefetchWindowMB:  16,
		StreamMaxInflight: 2,
		StreamConcurrency: 8,
		AttrTimeoutSec:    1,
		EntryTimeoutSec:   1,
		UID:               uint32(os.Getuid()),
		GID:               uint32(os.Getgid()),
		Writable:          true,
		AllDirEnabled:     true,
	}

	tbClient := torbox.NewClient(&e2eTorboxConfig{apiKey: "test", baseURL: "http://localhost:0"})

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

	// ── Verify initial state: Season 3 directory exists ──────────────
	seasonDir := filepath.Join(mountDir, "series", "My.Show.S03", "Season 3")
	entries, err := os.ReadDir(seasonDir)
	if err != nil {
		t.Fatalf("ReadDir(season dir): %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 files initially, got %d: %v", len(entries), dirEntryNamesFs(entries))
	}
	t.Logf("initial files: %v", dirEntryNamesFs(entries))

	// ── Verify /all/ directory also has the show ─────────────────────
	allShowDir := filepath.Join(mountDir, "all", "My.Show.S03")
	allEntries, err := os.ReadDir(allShowDir)
	if err != nil {
		t.Fatalf("ReadDir(all show dir): %v", err)
	}
	if len(allEntries) != 2 {
		t.Fatalf("expected 2 files in /all/ initially, got %d: %v", len(allEntries), dirEntryNamesFs(allEntries))
	}

	// ── Remove the Season 3 directory via Rmdir ──────────────────────────
	if err := os.Remove(seasonDir); err != nil {
		t.Fatalf("os.Remove(season dir): %v", err)
	}

	// ── Verify: Season 3 directory is gone from /series/ ─────────────
	if _, err := os.Stat(seasonDir); !os.IsNotExist(err) {
		t.Errorf("Season 3 directory should be gone after rmdir, got err=%v", err)
	}

	// ── Verify: files are also gone from /all/ ────────────────────────
	// Cross-directory cache invalidation (NotifyEntry on /all/) is dispatched
	// asynchronously from the Rmdir handler to avoid deadlocking on /dev/fuse,
	// so poll until the files disappear from the kernel dentry cache.
	for _, fname := range []string{"My.Show.S03E01.mkv", "My.Show.S03E02.mkv"} {
		p := filepath.Join(allShowDir, fname)
		if err := waitForGone(t, ctx, p); err != nil {
			t.Errorf("file %s should be gone from /all/ after rmdir: %v", fname, err)
		}
	}

	// ── Verify: all files are hidden in DB ────────────────────────────
	for _, key := range []string{"torrent:600:601", "torrent:600:602"} {
		hidden, err := db.IsHidden(key)
		if err != nil {
			t.Fatalf("IsHidden(%s): %v", key, err)
		}
		if !hidden {
			t.Errorf("file %s should be hidden after rmdir", key)
		}
	}
}
