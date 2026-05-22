package catalog

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"github.com/iiieii/torbox-fuse-go/internal/state"
)

// ---------------------------------------------------------------------------
// Mock Downloader
// ---------------------------------------------------------------------------

// mockDownloader implements Downloader for tests.
type mockDownloader struct {
	mu     sync.Mutex
	data   map[DownloadKind][]Download
	delays map[DownloadKind]time.Duration
	errors map[DownloadKind]error
	calls  atomic.Int64
}

func newMockDownloader() *mockDownloader {
	return &mockDownloader{
		data:   make(map[DownloadKind][]Download),
		delays: make(map[DownloadKind]time.Duration),
		errors: make(map[DownloadKind]error),
	}
}

func (m *mockDownloader) ListDownloads(ctx context.Context, kind DownloadKind) ([]Download, error) {
	m.calls.Add(1)
	if delay, ok := m.delays[kind]; ok {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	if err, ok := m.errors[kind]; ok {
		return nil, err
	}
	return m.data[kind], nil
}

func (m *mockDownloader) setDownloads(kind DownloadKind, downloads []Download) {
	m.mu.Lock()
	m.data[kind] = downloads
	m.mu.Unlock()
}

func (m *mockDownloader) setDelay(kind DownloadKind, d time.Duration) {
	m.delays[kind] = d
}

func (m *mockDownloader) setError(kind DownloadKind, err error) {
	m.errors[kind] = err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func sampleDownloads() map[DownloadKind][]Download {
	return map[DownloadKind][]Download{
		KindTorrent: {
			{
				Kind: KindTorrent, ID: "1", Name: "The.Matrix.1999", Hash: "abc123",
				Files: []File{
					{
						DownloadKind: KindTorrent, DownloadID: "1", FileID: "10",
						Name:      "The.Matrix.1999.mkv",
						Size:     1500000000,
						MimeType: "video/x-matroska",
						MediaType: MediaMovie,
					},
				},
			},
		},
		KindUsenet: {
			{
				Kind: KindUsenet, ID: "2", Name: "Breaking.Bad.S01", Hash: "def456",
				Files: []File{
					{
						DownloadKind: KindUsenet, DownloadID: "2", FileID: "20",
						Name:      "Breaking.Bad.S01E01.mkv",
						Size:     800000000,
						MimeType: "video/x-matroska",
						MediaType: MediaSeries,
					},
				},
			},
		},
		KindWebDL: {
			{
				Kind: KindWebDL, ID: "3", Name: "Inception.2010", Hash: "ghi789",
				Files: []File{
					{
						DownloadKind: KindWebDL, DownloadID: "3", FileID: "30",
						Name:      "Inception.2010.mkv",
						Size:     2000000000,
						MimeType: "video/x-matroska",
						MediaType: MediaMovie,
					},
				},
			},
		},
	}
}

func openTestDB(t *testing.T) *state.DB {
	t.Helper()
	db, err := state.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestCatalog_Refresh(t *testing.T) {
	dl := newMockDownloader()
	for kind, downloads := range sampleDownloads() {
		dl.setDownloads(kind, downloads)
	}

	db := openTestDB(t)
	m := metrics.New()

	cat := NewCatalog(dl, db, m)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := cat.Refresh(ctx); err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Verify tree is set.
	tree := cat.Tree()
	if tree == nil {
		t.Fatal("Tree() returned nil after refresh")
	}

	// Verify we can look up files from each download kind.
	tests := []struct {
		path string
	}{
		{"/movies/The Matrix 1999/The.Matrix.1999.mkv"},
		{"/series/Breaking Bad S01/Season 1/Breaking.Bad.S01E01.mkv"},
		{"/movies/Inception 2010/Inception.2010.mkv"},
	}
	for _, tt := range tests {
		f := tree.Lookup(tt.path)
		if f == nil {
			t.Errorf("Lookup(%q) returned nil", tt.path)
			continue
		}
		if f.Size == 0 {
			t.Errorf("Lookup(%q): file has zero size", tt.path)
		}
	}

	// Verify CatalogItems metric was updated.
	if m.CatalogItems.Load() == 0 {
		t.Error("CatalogItems metric is zero after refresh")
	}

	// Verify inodes were assigned.
	if m.RefreshCount.Load() != 1 {
		t.Errorf("RefreshCount = %d, want 1", m.RefreshCount.Load())
	}
}

func TestCatalog_RefreshAlreadyRunning(t *testing.T) {
	dl := newMockDownloader()
	// Add a delay to torrents so the first refresh takes time.
	dl.setDelay(KindTorrent, 2*time.Second)
	for kind, downloads := range sampleDownloads() {
		dl.setDownloads(kind, downloads)
	}

	db := openTestDB(t)
	m := metrics.New()

	cat := NewCatalog(dl, db, m)

	// Start first refresh in background.
	done := make(chan error, 1)
	go func() {
		done <- cat.Refresh(context.Background())
	}()

	// Give the first refresh time to start and block on the delay.
	time.Sleep(100 * time.Millisecond)

	// Second refresh should fail with ErrRefreshInProgress.
	err := cat.Refresh(context.Background())
	if err != ErrRefreshInProgress {
		t.Errorf("second Refresh returned err=%v, want ErrRefreshInProgress", err)
	}

	// Wait for first refresh to complete.
	if err := <-done; err != nil {
		t.Fatalf("first Refresh failed: %v", err)
	}
}

func TestCatalog_TreeBeforeRefresh(t *testing.T) {
	dl := newMockDownloader()
	db := openTestDB(t)
	m := metrics.New()

	cat := NewCatalog(dl, db, m)

	// Tree() should return a non-nil empty tree before first refresh.
	tree := cat.Tree()
	if tree == nil {
		t.Fatal("Tree() returned nil before first refresh, expected empty tree")
	}
	// The empty tree should have no entries at root.
	if entries := tree.ListDir("/"); len(entries) != 0 {
		t.Errorf("root entries = %d, want 0", len(entries))
	}
}

func TestCatalog_RefreshCancelledContext(t *testing.T) {
	dl := newMockDownloader()
	// Add a long delay that will be cancelled.
	dl.setDelay(KindTorrent, 10*time.Second)

	db := openTestDB(t)
	m := metrics.New()

	cat := NewCatalog(dl, db, m)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := cat.Refresh(ctx)
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

func TestCatalog_RefreshIdempotentInodes(t *testing.T) {
	dl := newMockDownloader()
	dl.setDownloads(KindTorrent, []Download{
		{
			Kind: KindTorrent, ID: "1", Name: "Stable.Movie", Hash: "abc",
			Files: []File{
				{
					DownloadKind: KindTorrent, DownloadID: "1", FileID: "10",
					Name:      "movie.mkv",
					Size:     1000,
					MimeType: "video/x-matroska",
					MediaType: MediaMovie,
				},
			},
		},
	})

	db := openTestDB(t)
	m := metrics.New()

	cat := NewCatalog(dl, db, m)

	// First refresh.
	if err := cat.Refresh(context.Background()); err != nil {
		t.Fatalf("first Refresh failed: %v", err)
	}

	key := "torrent:1:10"
	inode1, err := db.LookupInode(key)
	if err != nil {
		t.Fatalf("lookup inode after first refresh: %v", err)
	}

	// Second refresh should assign same inode.
	if err := cat.Refresh(context.Background()); err != nil {
		t.Fatalf("second Refresh failed: %v", err)
	}

	inode2, err := db.LookupInode(key)
	if err != nil {
		t.Fatalf("lookup inode after second refresh: %v", err)
	}

	if inode1 != inode2 {
		t.Errorf("inode changed across refreshes: first=%d second=%d", inode1, inode2)
	}
}

func TestCatalog_RefreshAPICallCount(t *testing.T) {
	dl := newMockDownloader()

	db := openTestDB(t)
	m := metrics.New()

	cat := NewCatalog(dl, db, m)

	if err := cat.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// We expect exactly 3 API calls (one per kind: torrents, usenet, webdl).
	if dl.calls.Load() != 3 {
		t.Errorf("downloader calls = %d, want 3", dl.calls.Load())
	}
	if m.APICallCount.Load() != 3 {
		t.Errorf("APICallCount metric = %d, want 3", m.APICallCount.Load())
	}
}

func TestCatalog_RefreshAPIError(t *testing.T) {
	dl := newMockDownloader()
	dl.setDownloads(KindTorrent, []Download{})
	dl.setError(KindUsenet, context.DeadlineExceeded)

	db := openTestDB(t)
	m := metrics.New()

	cat := NewCatalog(dl, db, m)

	err := cat.Refresh(context.Background())
	if err == nil {
		t.Error("expected error when API fails")
	}
}