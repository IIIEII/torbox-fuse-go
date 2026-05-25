//go:build !short

package fusefs

import (
	"context"
	"crypto/sha256"
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

// TestMountE2E mounts a real FUSE filesystem, lists directories, stats files,
// reads file contents through the FUSE layer, and verifies SHA256 hashes
// against known test data served by a mock CDN server.
func TestMountE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping FUSE mount e2e test in short mode")
	}
	if os.Getuid() != 0 {
		// On macOS, FUSE mounts may require specific permissions.
		// Check if macFUSE/FUSE-T is available.
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs"); err != nil && os.Getenv("FUSE_T") == "" {
			t.Skip("skipping: no FUSE driver found (macFUSE or FUSE-T required)")
		}
	}

	// Overall test timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ── Generate known test data (4 MiB of sequential bytes) ──────────
	const dataSize = 4 * 1024 * 1024
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	expectedHash := sha256.Sum256(testData)
	t.Logf("expected SHA256: %x", expectedHash)

	// ── Mock CDN server serving range requests ────────────────────────
	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			// Full response (only valid for offset 0).
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
			return
		}
		var start, end int
		n, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if err != nil || n != 2 {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if start >= len(testData) {
			http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= len(testData) {
			end = len(testData) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	}))
	defer cdnServer.Close()

	// ── Build catalog tree manually ───────────────────────────────────
	tree := catalog.BuildTree([]catalog.Download{
		{
			Kind: catalog.KindTorrent,
			ID:   "100",
			Name: "Test.Movie.2024",
			Hash: "abc123def456",
			Files: []catalog.File{
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "100",
					FileID:       "200",
					Name:         "Test.Movie.2024/Test.Movie.2024.mkv",
					Size:         dataSize,
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaMovie,
				},
			},
		},
	})

	// Verify the tree looks right before mounting.
	entries := tree.ListDir("/movies")
	if len(entries) == 0 {
		t.Fatal("catalog tree has no movie entries")
	}
	t.Logf("catalog tree entries under /movies: %v", dirEntryNames(entries))

	// cleanTitle replaces dots/underscores with spaces, so the directory name
	// becomes "Test Movie 2024" while the filename keeps its original dots.
	movieFile := tree.Lookup("/movies/Test Movie 2024/Test.Movie.2024.mkv")
	if movieFile == nil {
		t.Fatal("movie file not found in catalog tree")
	}

	// ── State DB in temp directory ────────────────────────────────────
	stateDir := t.TempDir()
	db, err := state.Open(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	defer db.Close()

	// ── Stream reader with mock CDN ──────────────────────────────────
	rc := cache.NewRangeCache(256 * 1024 * 1024)
	cdnClient := stream.NewCDNClient(8)
	permalinkBuilder := func(fileKey string) string {
		return cdnServer.URL
	}
	streamer := stream.NewStreamReader(rc, cdnClient, 2, int64(16*1024*1024), permalinkBuilder)

	// ── Config ────────────────────────────────────────────────────────
	cfg := &config.Config{
		APIKey:            "test-api-key",
		APIBaseURL:        "http://localhost:0",
		MountPath:         "", // set below
		CacheBudgetMB:     256,
		PrefetchWindowMB:  16,
		StreamMaxInflight: 2,
		StreamConcurrency: 8,
		AttrTimeoutSec:   1,
		EntryTimeoutSec:  1,
		UID:              uint32(os.Getuid()),
		GID:              uint32(os.Getgid()),
	}

	// ── TorBox client (unused for streaming; permalinkBuilder replaces it) ──
	tbClient := torbox.NewClient(&e2eTorboxConfig{apiKey: "test", baseURL: "http://localhost:0"})

	// ── Create FUSE root ─────────────────────────────────────────────
	root := NewRootNode(tree, db, streamer, cfg, tbClient)

	// ── Create mount point ────────────────────────────────────────────
	mountDir := t.TempDir()
	cfg.MountPath = mountDir

	// ── Mount the filesystem ──────────────────────────────────────────
	attrTimeout := time.Duration(cfg.AttrTimeoutSec) * time.Second
	entryTimeout := time.Duration(cfg.EntryTimeoutSec) * time.Second
	negTimeout := time.Duration(0)

	server, err := fs.Mount(mountDir, root, &fs.Options{
		AttrTimeout:     &attrTimeout,
		EntryTimeout:    &entryTimeout,
		NegativeTimeout: &negTimeout,
		MountOptions: fuse.MountOptions{
			MaxReadAhead: 4 << 20,
			FsName:       "torbox-media-center",
			Debug:        false,
		},
	})
	if err != nil {
		t.Fatalf("fs.Mount(%s): %v", mountDir, err)
	}

	// Ensure cleanup: unmount after test.
	defer func() {
		server.Unmount()
		// Wait for the server to finish so we don't leak goroutines.
		done := make(chan struct{})
		go func() {
			server.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Log("timeout waiting for server.Wait after unmount")
		}
	}()

	// ── Wait for the mount to become ready ─────────────────────────────
	if err := waitForMount(t, ctx, mountDir); err != nil {
		t.Fatalf("mount did not become ready: %v", err)
	}

	// ── Verify directory listing ───────────────────────────────────────
	moviesDir := filepath.Join(mountDir, "movies")
	dirEntries, err := os.ReadDir(moviesDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", moviesDir, err)
	}
	foundMovieDir := false
	for _, e := range dirEntries {
		if e.Name() == "Test Movie 2024" {
			foundMovieDir = true
			break
		}
	}
	if !foundMovieDir {
		t.Errorf("movie directory not found in listing; entries: %v", fsEntryNames(dirEntries))
	}

	// ── Verify file stat ──────────────────────────────────────────────
	// cleanTitle replaces dots/underscores with spaces, so the directory
	// name is "Test Movie 2024" while the filename keeps its dots.
	filePath := filepath.Join(mountDir, "movies", "Test Movie 2024", "Test.Movie.2024.mkv")
	fi, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat(%s): %v", filePath, err)
	}
	if fi.Size() != dataSize {
		t.Errorf("file size = %d, want %d", fi.Size(), dataSize)
	}
	if fi.Mode()&0o444 != 0o444 {
		t.Errorf("file mode = %v, want readable (0o444)", fi.Mode())
	}

	// ── Read file contents and verify SHA256 hash ──────────────────────
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", filePath, err)
	}
	if len(data) != dataSize {
		t.Errorf("read %d bytes, want %d", len(data), dataSize)
	}

	gotHash := sha256.Sum256(data)
	if gotHash != expectedHash {
		t.Errorf("SHA256 mismatch\ngot:  %x\nwant: %x", gotHash, expectedHash)
	} else {
		t.Logf("SHA256 verified: %x", gotHash)
	}
}

// ── Test helpers ────────────────────────────────────────────────────────────

// waitForMount polls until the FUSE mount point is functional (can list
// directory entries) or the context expires. It does not require specific
// entries — just that the mount is responsive.
func waitForMount(t *testing.T, ctx context.Context, mountDir string) error {
	t.Helper()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for mount at %s: %w", mountDir, ctx.Err())
		default:
		}

		f, err := os.Open(mountDir)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		_, err = f.Readdirnames(0)
		f.Close()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return nil
	}
}

// dirEntryNames returns the names of catalog DirEntry slices for logging.
func dirEntryNames(entries []catalog.DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

// fsEntryNames returns the names of os.DirEntry slices for logging.
func fsEntryNames(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

// e2eTorboxConfig implements torbox.Config for the e2e test.
type e2eTorboxConfig struct {
	apiKey  string
	baseURL string
}

func (c *e2eTorboxConfig) APIKey() string            { return c.apiKey }
func (c *e2eTorboxConfig) APIBaseURL() string         { return c.baseURL }
func (c *e2eTorboxConfig) APITimeout() time.Duration  { return 30 * time.Second }

// Ensure unmount is available on the platform.
var _ = syscall.Unmount