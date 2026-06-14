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