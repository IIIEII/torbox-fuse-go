package state

import (
	"path/filepath"
	"testing"
)

func TestAssignInodes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	inodes := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		ino, err := db.AssignInode("key"+string(rune('A'+i)), "/path/file"+string(rune('A'+i)))
		if err != nil {
			t.Fatalf("AssignInode %d: %v", i, err)
		}
		inodes[i] = ino
	}

	// Verify monotonically increasing
	for i := 1; i < len(inodes); i++ {
		if inodes[i] <= inodes[i-1] {
			t.Errorf("inode %d (%d) not greater than inode %d (%d)", i, inodes[i], i-1, inodes[i-1])
		}
	}

	// Verify lookup returns same values
	for i := 0; i < 3; i++ {
		key := "key" + string(rune('A'+i))
		got, err := db.LookupInode(key)
		if err != nil {
			t.Fatalf("LookupInode(%q): %v", key, err)
		}
		if got != inodes[i] {
			t.Errorf("LookupInode(%q): got %d, want %d", key, got, inodes[i])
		}
	}
}

func TestAssignInode_Idempotent(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ino1, err := db.AssignInode("same-key", "/path/file")
	if err != nil {
		t.Fatalf("AssignInode first call: %v", err)
	}
	ino2, err := db.AssignInode("same-key", "/path/file")
	if err != nil {
		t.Fatalf("AssignInode second call: %v", err)
	}
	if ino1 != ino2 {
		t.Errorf("AssignInode same key: got %d then %d, want equal", ino1, ino2)
	}
}

func TestInodeStabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")

	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open first: %v", err)
	}

	ino, err := db1.AssignInode("stable-key", "/movies/film.mkv")
	if err != nil {
		t.Fatalf("AssignInode: %v", err)
	}

	if err := db1.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open second: %v", err)
	}
	defer db2.Close()

	got, err := db2.LookupInode("stable-key")
	if err != nil {
		t.Fatalf("LookupInode after reopen: %v", err)
	}
	if got != ino {
		t.Errorf("inode after reopen: got %d, want %d", got, ino)
	}
}

func TestUpsertFiles(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	records := []FileRecord{
		{
			ContentKey:   "torrent:100:1",
			DownloadKind: "torrent",
			DownloadID:   "100",
			FileID:       "1",
			Path:         "/movies/The Matrix/The.Matrix.1999.mkv",
			Size:         2048,
		},
		{
			ContentKey:   "torrent:200:2",
			DownloadKind: "torrent",
			DownloadID:   "200",
			FileID:       "2",
			Path:         "/series/Breaking Bad/S01/Breaking.Bad.S01E01.mkv",
			Size:         1024,
		},
	}

	if err := db.UpsertFiles(records); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

	// Verify lookup of first record
	rec1, err := db.LookupFile("torrent:100:1")
	if err != nil {
		t.Fatalf("LookupFile(torrent:100:1): %v", err)
	}
	if rec1.ContentKey != "torrent:100:1" {
		t.Errorf("ContentKey: got %q, want %q", rec1.ContentKey, "torrent:100:1")
	}
	if rec1.DownloadKind != "torrent" {
		t.Errorf("DownloadKind: got %q, want %q", rec1.DownloadKind, "torrent")
	}
	if rec1.DownloadID != "100" {
		t.Errorf("DownloadID: got %q, want %q", rec1.DownloadID, "100")
	}
	if rec1.FileID != "1" {
		t.Errorf("FileID: got %q, want %q", rec1.FileID, "1")
	}
	if rec1.Path != "/movies/The Matrix/The.Matrix.1999.mkv" {
		t.Errorf("Path: got %q, want %q", rec1.Path, "/movies/The Matrix/The.Matrix.1999.mkv")
	}
	if rec1.Size != 2048 {
		t.Errorf("Size: got %d, want %d", rec1.Size, 2048)
	}

	// Verify lookup of second record
	rec2, err := db.LookupFile("torrent:200:2")
	if err != nil {
		t.Fatalf("LookupFile(torrent:200:2): %v", err)
	}
	if rec2.ContentKey != "torrent:200:2" {
		t.Errorf("ContentKey: got %q, want %q", rec2.ContentKey, "torrent:200:2")
	}
	if rec2.Size != 1024 {
		t.Errorf("Size: got %d, want %d", rec2.Size, 1024)
	}
}

func TestUpsertFiles_UpdateExisting(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	original := []FileRecord{
		{
			ContentKey:   "torrent:100:1",
			DownloadKind: "torrent",
			DownloadID:   "100",
			FileID:       "1",
			Path:         "/movies/The Matrix/The.Matrix.1999.mkv",
			Size:         2048,
		},
	}
	if err := db.UpsertFiles(original); err != nil {
		t.Fatalf("UpsertFiles original: %v", err)
	}

	updated := []FileRecord{
		{
			ContentKey:   "torrent:100:1",
			DownloadKind: "torrent",
			DownloadID:   "100",
			FileID:       "1",
			Path:         "/movies/The Matrix/The.Matrix.1999.repack.mkv",
			Size:         4096,
		},
	}
	if err := db.UpsertFiles(updated); err != nil {
		t.Fatalf("UpsertFiles updated: %v", err)
	}

	rec, err := db.LookupFile("torrent:100:1")
	if err != nil {
		t.Fatalf("LookupFile after update: %v", err)
	}
	if rec.Path != "/movies/The Matrix/The.Matrix.1999.repack.mkv" {
		t.Errorf("Path after update: got %q, want %q", rec.Path, "/movies/The Matrix/The.Matrix.1999.repack.mkv")
	}
	if rec.Size != 4096 {
		t.Errorf("Size after update: got %d, want %d", rec.Size, 4096)
	}
}

func TestListFiles(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Empty DB returns empty slice, not error.
	files, err := db.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles on empty DB: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("ListFiles on empty DB: got %d records, want 0", len(files))
	}

	// Insert records.
	records := []FileRecord{
		{
			ContentKey:   "torrent:100:1",
			DownloadKind: "torrent",
			DownloadID:   "100",
			FileID:       "1",
			Path:         "/movies/The Matrix/The.Matrix.1999.mkv",
			Size:         2048,
		},
		{
			ContentKey:   "usenet:200:2",
			DownloadKind: "usenet",
			DownloadID:   "200",
			FileID:       "2",
			Path:         "/series/Breaking Bad/Season 1/Breaking.Bad.S01E01.mkv",
			Size:         1024,
		},
	}
	if err := db.UpsertFiles(records); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

	files, err = db.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("ListFiles: got %d records, want 2", len(files))
	}

	// Verify records by content key for deterministic lookup.
	byKey := make(map[string]FileRecord)
	for _, f := range files {
		byKey[f.ContentKey] = f
	}
	if rec, ok := byKey["torrent:100:1"]; !ok {
		t.Error("missing torrent:100:1")
	} else if rec.Path != "/movies/The Matrix/The.Matrix.1999.mkv" {
		t.Errorf("Path: got %q, want %q", rec.Path, "/movies/The Matrix/The.Matrix.1999.mkv")
	}
	if rec, ok := byKey["usenet:200:2"]; !ok {
		t.Error("missing usenet:200:2")
	} else if rec.Size != 1024 {
		t.Errorf("Size: got %d, want %d", rec.Size, 1024)
	}
}

func TestLookupFile_NotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	rec, err := db.LookupFile("nonexistent")
	if err != nil {
		t.Fatalf("LookupFile(nonexistent): %v", err)
	}
	if rec != nil {
		t.Errorf("LookupFile(nonexistent): got %v, want nil", rec)
	}
}

func TestLookupInode_NotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.LookupInode("nonexistent")
	if err == nil {
		t.Error("LookupInode(nonexistent): expected error, got nil")
	}
}

func TestHideFile(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Insert a file record first.
	records := []FileRecord{
		{ContentKey: "torrent:1:10", DownloadKind: "torrent", DownloadID: "1", FileID: "10", Path: "/movies/Test/Test.mkv", Size: 1024},
	}
	if err := db.UpsertFiles(records); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

	// File is not hidden initially.
	hidden, err := db.IsHidden("torrent:1:10")
	if err != nil {
		t.Fatalf("IsHidden: %v", err)
	}
	if hidden {
		t.Error("file should not be hidden initially")
	}

	// Hide the file.
	if err := db.HideFile("torrent:1:10"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}

	hidden, err = db.IsHidden("torrent:1:10")
	if err != nil {
		t.Fatalf("IsHidden after hide: %v", err)
	}
	if !hidden {
		t.Error("file should be hidden after HideFile")
	}

	// HideFile is idempotent.
	if err := db.HideFile("torrent:1:10"); err != nil {
		t.Fatalf("HideFile idempotent: %v", err)
	}
}

func TestUnhideFile(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	if err := db.HideFile("torrent:1:10"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}

	if err := db.UnhideFile("torrent:1:10"); err != nil {
		t.Fatalf("UnhideFile: %v", err)
	}

	hidden, err := db.IsHidden("torrent:1:10")
	if err != nil {
		t.Fatalf("IsHidden after unhide: %v", err)
	}
	if hidden {
		t.Error("file should not be hidden after UnhideFile")
	}

	// Unhide non-existent key is a no-op.
	if err := db.UnhideFile("nonexistent"); err != nil {
		t.Fatalf("UnhideFile nonexistent: %v", err)
	}
}

func TestUnhideDownload(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Hide files from the same download.
	if err := db.HideFile("torrent:1:10"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}
	if err := db.HideFile("torrent:1:20"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}
	// Also hide a file from a different download.
	if err := db.HideFile("usenet:2:30"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}

	// Unhide all files for download torrent:1.
	if err := db.UnhideDownload("torrent", "1"); err != nil {
		t.Fatalf("UnhideDownload: %v", err)
	}

	// torrent:1 files should be unhidden.
	if hidden, _ := db.IsHidden("torrent:1:10"); hidden {
		t.Error("torrent:1:10 should be unhidden")
	}
	if hidden, _ := db.IsHidden("torrent:1:20"); hidden {
		t.Error("torrent:1:20 should be unhidden")
	}
	// usenet:2:30 should still be hidden.
	if hidden, _ := db.IsHidden("usenet:2:30"); !hidden {
		t.Error("usenet:2:30 should still be hidden")
	}
}

func TestIsDownloadFullyHidden(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Insert file records for a download with 2 files.
	records := []FileRecord{
		{ContentKey: "torrent:1:10", DownloadKind: "torrent", DownloadID: "1", FileID: "10", Path: "/movies/T/T1.mkv", Size: 1000},
		{ContentKey: "torrent:1:20", DownloadKind: "torrent", DownloadID: "1", FileID: "20", Path: "/movies/T/T2.mkv", Size: 2000},
	}
	if err := db.UpsertFiles(records); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

	// Not hidden at all.
	fully, err := db.IsDownloadFullyHidden("torrent", "1")
	if err != nil {
		t.Fatalf("IsDownloadFullyHidden: %v", err)
	}
	if fully {
		t.Error("download should not be fully hidden when no files are hidden")
	}

	// Hide one file — not fully hidden.
	if err := db.HideFile("torrent:1:10"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}
	fully, err = db.IsDownloadFullyHidden("torrent", "1")
	if err != nil {
		t.Fatalf("IsDownloadFullyHidden partial: %v", err)
	}
	if fully {
		t.Error("download should not be fully hidden when only 1 of 2 files is hidden")
	}

	// Hide second file — now fully hidden.
	if err := db.HideFile("torrent:1:20"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}
	fully, err = db.IsDownloadFullyHidden("torrent", "1")
	if err != nil {
		t.Fatalf("IsDownloadFullyHidden full: %v", err)
	}
	if !fully {
		t.Error("download should be fully hidden when all files are hidden")
	}

	// Non-existent download is not fully hidden.
	fully, err = db.IsDownloadFullyHidden("torrent", "999")
	if err != nil {
		t.Fatalf("IsDownloadFullyHidden nonexistent: %v", err)
	}
	if fully {
		t.Error("non-existent download should not be fully hidden")
	}
}

func TestListHiddenFiles(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Empty — no hidden files.
	files, err := db.ListHiddenFiles()
	if err != nil {
		t.Fatalf("ListHiddenFiles empty: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("ListHiddenFiles empty: got %d, want 0", len(files))
	}

	// Insert and hide files.
	records := []FileRecord{
		{ContentKey: "torrent:1:10", DownloadKind: "torrent", DownloadID: "1", FileID: "10", Path: "/movies/A/A.mkv", Size: 1000},
		{ContentKey: "torrent:1:20", DownloadKind: "torrent", DownloadID: "1", FileID: "20", Path: "/movies/A/B.mkv", Size: 2000},
	}
	if err := db.UpsertFiles(records); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}
	if err := db.HideFile("torrent:1:10"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}

	files, err = db.ListHiddenFiles()
	if err != nil {
		t.Fatalf("ListHiddenFiles: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("ListHiddenFiles: got %d, want 1", len(files))
	}
	if files[0].ContentKey != "torrent:1:10" {
		t.Errorf("ContentKey: got %q, want %q", files[0].ContentKey, "torrent:1:10")
	}
}

func TestListHiddenDownloads(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Insert two downloads, hide some files.
	records := []FileRecord{
		{ContentKey: "torrent:1:10", DownloadKind: "torrent", DownloadID: "1", FileID: "10", Path: "/movies/Film/Film.mkv", Size: 1000},
		{ContentKey: "torrent:1:20", DownloadKind: "torrent", DownloadID: "1", FileID: "20", Path: "/movies/Film/Extra.mkv", Size: 500},
		{ContentKey: "usenet:2:30", DownloadKind: "usenet", DownloadID: "2", FileID: "30", Path: "/series/Show/Show.mkv", Size: 3000},
	}
	if err := db.UpsertFiles(records); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

	// Hide one file from download 1, making it partially hidden.
	if err := db.HideFile("torrent:1:20"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}

	downloads, err := db.ListHiddenDownloads()
	if err != nil {
		t.Fatalf("ListHiddenDownloads: %v", err)
	}
	if len(downloads) != 1 {
		t.Fatalf("ListHiddenDownloads: got %d downloads, want 1", len(downloads))
	}
	d := downloads[0]
	if d.DownloadKind != "torrent" || d.DownloadID != "1" {
		t.Errorf("got %s:%s, want torrent:1", d.DownloadKind, d.DownloadID)
	}
	if d.HiddenCount != 1 {
		t.Errorf("HiddenCount: got %d, want 1", d.HiddenCount)
	}
	if d.TotalCount != 2 {
		t.Errorf("TotalCount: got %d, want 2", d.TotalCount)
	}
}

func TestReplaceFiles_RemovesStale(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Insert initial files.
	err := db.UpsertFiles([]FileRecord{
		{ContentKey: "torrent:1:10", DownloadKind: "torrent", DownloadID: "1", FileID: "10", Path: "/movies/Film A/film.mkv", Size: 1000},
		{ContentKey: "torrent:1:20", DownloadKind: "torrent", DownloadID: "1", FileID: "20", Path: "/movies/Film A/sub.srt", Size: 50},
		{ContentKey: "torrent:2:30", DownloadKind: "torrent", DownloadID: "2", FileID: "30", Path: "/movies/Film B/film.mkv", Size: 2000},
	})
	if err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}

	// Verify we have 3 files.
	files, err := db.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(files))
	}

	// Assign inodes so we can check they're cleaned up.
	_, err = db.AssignInode("torrent:1:10", "/movies/Film A/film.mkv")
	if err != nil {
		t.Fatalf("AssignInode: %v", err)
	}
	_, err = db.AssignInode("torrent:1:20", "/movies/Film A/sub.srt")
	if err != nil {
		t.Fatalf("AssignInode: %v", err)
	}
	_, err = db.AssignInode("torrent:2:30", "/movies/Film B/film.mkv")
	if err != nil {
		t.Fatalf("AssignInode: %v", err)
	}

	// Hide one file to check it's cleaned up too.
	if err := db.HideFile("torrent:1:20"); err != nil {
		t.Fatalf("HideFile: %v", err)
	}

	// Replace with only 2 files (torrent:2:30 removed, torrent:1:20 no longer in API).
	staleCount, err := db.ReplaceFiles([]FileRecord{
		{ContentKey: "torrent:1:10", DownloadKind: "torrent", DownloadID: "1", FileID: "10", Path: "/movies/Film A/film.mkv", Size: 1000},
		{ContentKey: "torrent:3:40", DownloadKind: "torrent", DownloadID: "3", FileID: "40", Path: "/movies/Film C/film.mkv", Size: 3000},
	})
	if err != nil {
		t.Fatalf("ReplaceFiles: %v", err)
	}
	if staleCount != 2 {
		t.Errorf("staleCount: got %d, want 2", staleCount)
	}

	// Verify files table now has only 2 records.
	files, err = db.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files after ReplaceFiles, got %d", len(files))
	}

	// Verify hidden_files was also cleaned up.
	hidden, err := db.HiddenSet()
	if err != nil {
		t.Fatalf("HiddenSet: %v", err)
	}
	if len(hidden) != 0 {
		t.Errorf("expected 0 hidden files after ReplaceFiles, got %d", len(hidden))
	}

	// Verify inode for removed file still exists (it's kept for stability).
	_, err = db.LookupInode("torrent:1:20")
	if err != nil {
		t.Logf("inode for removed file: %v (kept for path stability)", err)
	}
}

// openTestDB creates a DB in a temp directory and registers cleanup.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open test DB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}