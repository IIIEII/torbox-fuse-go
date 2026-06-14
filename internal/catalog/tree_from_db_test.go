package catalog

import (
	"context"
	"path"
	"testing"

	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"github.com/iiieii/torbox-fuse-go/internal/state"
)

func TestBuildTreeFromDB(t *testing.T) {
	records := []state.FileRecord{
		{
			ContentKey:   "torrent:1:10",
			DownloadKind: "torrent",
			DownloadID:   "1",
			FileID:       "10",
			Path:         "/movies/The.Matrix.1999/The.Matrix.1999.mkv",
			Size:         1500000000,
		},
		{
			ContentKey:   "usenet:2:20",
			DownloadKind: "usenet",
			DownloadID:   "2",
			FileID:       "20",
			Path:         "/series/Breaking.Bad/Season 1/Breaking.Bad.S01E01.mkv",
			Size:         800000000,
		},
		{
			ContentKey:   "webdl:3:30",
			DownloadKind: "webdl",
			DownloadID:   "3",
			FileID:       "30",
			Path:         "/movies/Inception/Inception.2010.mkv",
			Size:         2000000000,
		},
	}

	tree := BuildTreeFromDB(records, false)

	// Verify file lookups.
	tests := []struct {
		path     string
		wantSize int64
	}{
		{"/movies/The.Matrix.1999/The.Matrix.1999.mkv", 1500000000},
		{"/series/Breaking.Bad/Season 1/Breaking.Bad.S01E01.mkv", 800000000},
		{"/movies/Inception/Inception.2010.mkv", 2000000000},
	}
	for _, tt := range tests {
		f := tree.Lookup(tt.path)
		if f == nil {
			t.Errorf("Lookup(%q) returned nil", tt.path)
			continue
		}
		if f.Size != tt.wantSize {
			t.Errorf("Lookup(%q): size = %d, want %d", tt.path, f.Size, tt.wantSize)
		}
	}

	// Verify parent directories exist.
	for _, dirPath := range []string{"/movies", "/series", "/movies/The.Matrix.1999", "/series/Breaking.Bad", "/series/Breaking.Bad/Season 1"} {
		entries := tree.ListDir(dirPath)
		if len(entries) == 0 {
			t.Errorf("ListDir(%q) returned no entries", dirPath)
		}
	}

	// Verify root has movies and series.
	rootEntries := tree.ListDir("/")
	names := make(map[string]bool)
	for _, e := range rootEntries {
		names[e.Name] = true
	}
	if !names["movies"] {
		t.Error("root missing 'movies' directory")
	}
	if !names["series"] {
		t.Error("root missing 'series' directory")
	}
}

func TestBuildTreeFromDB_AllDir(t *testing.T) {
	records := []state.FileRecord{
		{
			ContentKey:   "torrent:1:10",
			DownloadKind: "torrent",
			DownloadID:   "1",
			FileID:       "10",
			Path:         "/movies/The.Matrix.1999/The.Matrix.1999.mkv",
			Size:         1500000000,
		},
		{
			ContentKey:   "usenet:2:20",
			DownloadKind: "usenet",
			DownloadID:   "2",
			FileID:       "20",
			Path:         "/series/Breaking Bad/Season 1/Breaking.Bad.S01E01.mkv",
			Size:         800000000,
		},
	}

	tree := BuildTreeFromDB(records, true)

	// Verify /all exists.
	allEntries := tree.ListDir("/all")
	if len(allEntries) == 0 {
		t.Fatal("ListDir(/all) returned no entries with allDir=true")
	}

	// Verify /all subdirectories.
	titles := make(map[string]bool)
	for _, e := range allEntries {
		titles[e.Name] = true
	}
	if !titles["The.Matrix.1999"] {
		t.Error("/all missing 'The.Matrix.1999'")
	}
	if !titles["Breaking Bad"] {
		t.Error("/all missing 'Breaking Bad'")
	}

	// Verify files inside /all title dirs.
	matrixAll := tree.ListDir("/all/The.Matrix.1999")
	if len(matrixAll) == 0 {
		t.Error("ListDir(/all/The.Matrix.1999) returned no entries")
	}
}

func TestBuildTreeFromDB_Empty(t *testing.T) {
	tree := BuildTreeFromDB(nil, false)

	// Should have no files, but structure should be valid.
	rootEntries := tree.ListDir("/")
	if len(rootEntries) != 0 {
		t.Errorf("empty DB tree root has %d entries, want 0", len(rootEntries))
	}
}

func TestBuildTreeFromDB_ContentKeyRoundtrip(t *testing.T) {
	records := []state.FileRecord{
		{
			ContentKey:   "torrent:42:100",
			DownloadKind: "torrent",
			DownloadID:   "42",
			FileID:       "100",
			Path:         "/movies/Test/Test.mkv",
			Size:         1024,
		},
	}

	tree := BuildTreeFromDB(records, false)
	f := tree.Lookup("/movies/Test/Test.mkv")
	if f == nil {
		t.Fatal("Lookup returned nil")
	}

	// Verify ContentKey() roundtrips correctly.
	gotKey := f.ContentKey()
	if gotKey != "torrent:42:100" {
		t.Errorf("ContentKey() = %q, want %q", gotKey, "torrent:42:100")
	}
}

func TestCatalog_LoadFromDB(t *testing.T) {
	db := openTestDB(t)

	// Pre-populate DB with file records + inodes.
	records := []state.FileRecord{
		{
			ContentKey:   "torrent:1:10",
			DownloadKind: "torrent",
			DownloadID:   "1",
			FileID:       "10",
			Path:         "/movies/TestMovie/TestMovie.mkv",
			Size:         2048,
		},
	}
	if err := db.UpsertFiles(records); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}
	if _, err := db.AssignInode("torrent:1:10", "/movies/TestMovie/TestMovie.mkv"); err != nil {
		t.Fatalf("AssignInode: %v", err)
	}

	m := metricsForTest()
	cat := NewCatalog(nil, db, m, false)

	if err := cat.LoadFromDB(context.Background()); err != nil {
		t.Fatalf("LoadFromDB: %v", err)
	}

	tree := cat.Tree()
	f := tree.Lookup("/movies/TestMovie/TestMovie.mkv")
	if f == nil {
		t.Fatal("Lookup after LoadFromDB returned nil")
	}
	if f.Size != 2048 {
		t.Errorf("file size = %d, want 2048", f.Size)
	}

	if m.CatalogItems.Load() != 1 {
		t.Errorf("CatalogItems = %d, want 1", m.CatalogItems.Load())
	}
}

func TestCatalog_LoadFromDB_Empty(t *testing.T) {
	db := openTestDB(t)
	m := metricsForTest()
	cat := NewCatalog(nil, db, m, false)

	// Empty DB — should not error, tree stays empty.
	if err := cat.LoadFromDB(context.Background()); err != nil {
		t.Fatalf("LoadFromDB on empty DB: %v", err)
	}

	tree := cat.Tree()
	rootEntries := tree.ListDir("/")
	if len(rootEntries) != 0 {
		t.Errorf("empty DB: root has %d entries, want 0", len(rootEntries))
	}
}

func TestCatalog_LoadFromDB_TriggersOnRefresh(t *testing.T) {
	db := openTestDB(t)

	records := []state.FileRecord{
		{
			ContentKey:   "torrent:1:10",
			DownloadKind: "torrent",
			DownloadID:   "1",
			FileID:       "10",
			Path:         "/movies/Test/Test.mkv",
			Size:         1024,
		},
	}
	if err := db.UpsertFiles(records); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

	m := metricsForTest()
	cat := NewCatalog(nil, db, m, false)

	called := false
	cat.SetOnRefresh(func() { called = true })

	if err := cat.LoadFromDB(context.Background()); err != nil {
		t.Fatalf("LoadFromDB: %v", err)
	}

	if !called {
		t.Error("onRefresh callback was not called after LoadFromDB")
	}
}

func TestBuildTreeFromDB_PathWithoutLeadingSlash(t *testing.T) {
	// Path.Dir("movies/Test/Test.mkv") returns "movies/Test" (no leading slash).
	// This should still work because buildParentDirs handles relative paths.
	records := []state.FileRecord{
		{
			ContentKey:   "torrent:1:10",
			DownloadKind: "torrent",
			DownloadID:   "1",
			FileID:       "10",
			Path:         "movies/Test/Test.mkv",
			Size:         1024,
		},
	}

	tree := BuildTreeFromDB(records, false)

	// The path won't have a leading slash, but tree should still have entries.
	dirPath := path.Dir(records[0].Path)
	entries := tree.ListDir(dirPath)
	found := false
	for _, e := range entries {
		if e.Name == "Test.mkv" && e.File != nil {
			found = true
		}
	}
	if !found {
		t.Errorf("file not found in tree at path %q", dirPath)
	}
}

// metricsForTest creates a minimal metrics instance for catalog tests.
func metricsForTest() *metrics.Metrics {
	return metrics.New()
}
