package dashboard

import (
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"github.com/iiieii/torbox-fuse-go/internal/stream"
)

// newTestDashboardWithCache creates a Dashboard where the StreamReader and
// Dashboard share the same RangeCache. This is essential because
// SnapshotFiles() iterates the StreamReader's internal cache, so cached
// data must go into the cache that the StreamReader owns.
func newTestDashboardWithCache(t *testing.T) (*Dashboard, *cache.RangeCache) {
	t.Helper()
	m := metrics.New()
	rc := cache.NewRangeCache(4*1024*1024, m) // 4 MiB budget
	cdn := stream.NewCDNClient(2, nil, 0)
	sr := stream.NewStreamReader(rc, cdn, 2, 16, 1024*1024, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)
	d := New(sr, rc, nil, m)
	return d, rc
}

// newTestDashboardStreamReader creates a Dashboard + StreamReader where both
// share the same RangeCache. Returns the Dashboard, StreamReader, and cache.
func newTestDashboardStreamReader(t *testing.T) (*Dashboard, *stream.StreamReader, *cache.RangeCache) {
	t.Helper()
	m := metrics.New()
	rc := cache.NewRangeCache(4*1024*1024, m)
	cdn := stream.NewCDNClient(2, nil, 0)
	sr := stream.NewStreamReader(rc, cdn, 2, 16, 1024*1024, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)
	d := New(sr, rc, nil, m)
	return d, sr, rc
}

func TestSnapshotEmptyCache(t *testing.T) {
	d, _ := newTestDashboardWithCache(t)

	snap := d.Snapshot()

	if snap.Summary.BudgetSlotsTotal != 16 {
		t.Errorf("BudgetSlotsTotal = %d, want 16", snap.Summary.BudgetSlotsTotal)
	}
	if len(snap.Active) != 0 {
		t.Errorf("Active = %d files, want 0", len(snap.Active))
	}
	if len(snap.RecentlyClosed) != 0 {
		t.Errorf("RecentlyClosed = %d files, want 0", len(snap.RecentlyClosed))
	}
}

func TestSnapshotWithCachedData(t *testing.T) {
	d, rc := newTestDashboardWithCache(t)

	// Put some data into the shared cache.
	rc.PutWithPriority("torrent:1:100", 0, make([]byte, 4096), cache.PriorityHigh)
	rc.Put("torrent:1:100", 4096, make([]byte, 4096))

	snap := d.Snapshot()

	// Should have one active file with 2 cached blocks.
	if len(snap.Active) != 1 {
		t.Fatalf("Active = %d files, want 1", len(snap.Active))
	}

	f := snap.Active[0]
	if f.FileKey != "torrent:1:100" {
		t.Errorf("FileKey = %q, want %q", f.FileKey, "torrent:1:100")
	}
	if len(f.CachedBlocks) != 2 {
		t.Errorf("CachedBlocks = %d, want 2", len(f.CachedBlocks))
	}

	// First block should be PriorityHigh, second PriorityLow.
	if f.CachedBlocks[0].Priority != cache.PriorityHigh {
		t.Errorf("Block 0 priority = %d, want PriorityHigh", f.CachedBlocks[0].Priority)
	}
	if f.CachedBlocks[1].Priority != cache.PriorityLow {
		t.Errorf("Block 1 priority = %d, want PriorityLow", f.CachedBlocks[1].Priority)
	}
	// Pattern should be "idle" — no session, no readers.
	if f.Pattern != "idle" {
		t.Errorf("Pattern = %q, want %q", f.Pattern, "idle")
	}
}

func TestSnapshotPathResolution(t *testing.T) {
	d, rc := newTestDashboardWithCache(t)

	// Put data in cache.
	rc.Put("torrent:42:999", 0, make([]byte, 1024))

	snap := d.Snapshot()
	if len(snap.Active) != 1 {
		t.Fatalf("Active = %d, want 1", len(snap.Active))
	}
	// Without a stateDB, path resolution returns the fileKey as-is.
	if snap.Active[0].FilePath != "torrent:42:999" {
		t.Errorf("FilePath = %q, want %q", snap.Active[0].FilePath, "torrent:42:999")
	}
}

func TestSnapshotSummary(t *testing.T) {
	d, rc := newTestDashboardWithCache(t)
	_ = rc

	snap := d.Snapshot()

	// Budget should be 16 (default maxGlobalWindows).
	if snap.Summary.BudgetSlotsTotal != 16 {
		t.Errorf("BudgetSlotsTotal = %d, want 16", snap.Summary.BudgetSlotsTotal)
	}
	// Cache budget should be 4 MiB (what we created).
	if snap.Summary.CacheBytesBudget != 4*1024*1024 {
		t.Errorf("CacheBytesBudget = %d, want %d", snap.Summary.CacheBytesBudget, 4*1024*1024)
	}
}

func TestSnapshotReaderTracking(t *testing.T) {
	d, sr, _ := newTestDashboardStreamReader(t)

	// Track some readers.
	sr.TrackReader("file1", 1, 100)
	sr.TrackReader("file1", 2, 500)

	snap := d.Snapshot()

	// file1 should appear as an active file with read positions.
	var found bool
	for _, f := range snap.Active {
		if f.FileKey == "file1" {
			found = true
			if len(f.ReadOffsets) != 2 {
				t.Errorf("ReadOffsets = %d, want 2", len(f.ReadOffsets))
			}
			break
		}
	}
	if !found {
		t.Error("expected file1 in active files")
	}

	// Untrack one reader.
	sr.UntrackReader("file1", 1)

	snap2 := d.Snapshot()
	for _, f := range snap2.Active {
		if f.FileKey == "file1" {
			if len(f.ReadOffsets) != 1 {
				t.Errorf("ReadOffsets = %d after untrack, want 1", len(f.ReadOffsets))
			}
		}
	}
}

func TestSnapshotRecentlyClosedEmpty(t *testing.T) {
	d, sr, _ := newTestDashboardStreamReader(t)

	// CancelFile on a file with no inflight windows doesn't add an entry.
	sr.CancelFile("nonexistent:0:0")

	snap := d.Snapshot()
	for _, cf := range snap.RecentlyClosed {
		t.Errorf("unexpected recently closed file: %q", cf.FileKey)
	}
}

func TestSnapshotTimestamp(t *testing.T) {
	d, _ := newTestDashboardWithCache(t)

	snap := d.Snapshot()

	// Timestamp should be parseable as RFC3339.
	if _, err := time.Parse(time.RFC3339, snap.Timestamp); err != nil {
		t.Errorf("Timestamp %q is not valid RFC3339: %v", snap.Timestamp, err)
	}
}

func TestSnapshotSortOrder(t *testing.T) {
	d, rc := newTestDashboardWithCache(t)

	// Put data in cache for multiple files.
	rc.Put("torrent:2:200", 0, make([]byte, 1024))
	rc.Put("torrent:1:100", 0, make([]byte, 1024))
	rc.Put("torrent:3:300", 0, make([]byte, 1024))

	snap := d.Snapshot()

	// Active files should be sorted by FilePath (which falls back to fileKey
	// when no stateDB is available, so they sort by fileKey).
	if len(snap.Active) < 2 {
		t.Skipf("only %d active files, need at least 2", len(snap.Active))
	}
	for i := 1; i < len(snap.Active); i++ {
		if snap.Active[i-1].FilePath > snap.Active[i].FilePath {
			t.Errorf("active files not sorted: %q > %q", snap.Active[i-1].FilePath, snap.Active[i].FilePath)
		}
	}
}

// --- E2E scenarios using public API only ---

// TestE2E_ReadOffsetsAtCorrectPositions verifies that read cursors appear
// at the expected positions in the snapshot. This simulates the "курсор
// чтения отображается на своём месте" requirement.
func TestE2E_ReadOffsetsAtCorrectPositions(t *testing.T) {
	d, sr, rc := newTestDashboardStreamReader(t)
	_ = rc

	const fileSize int64 = 100 * 1024 * 1024 // 100 MiB

	// Simulate Plex reading from the middle of a file.
	sr.TrackReader("movie.mkv", 1, fileSize/2) // 50% position

	snap := d.Snapshot()
	var movie *FileSnapshotJSON
	for i := range snap.Active {
		if snap.Active[i].FileKey == "movie.mkv" {
			movie = &snap.Active[i]
			break
		}
	}
	if movie == nil {
		t.Fatal("expected movie.mkv in active files")
	}
	if len(movie.ReadOffsets) != 1 {
		t.Fatalf("ReadOffsets = %d, want 1", len(movie.ReadOffsets))
	}
	if movie.ReadOffsets[0] != fileSize/2 {
		t.Errorf("ReadOffset = %d, want %d", movie.ReadOffsets[0], fileSize/2)
	}
}

// TestE2E_MultipleReadersDifferentOffsets verifies that two concurrent
// FUSE handles show both read cursors. This simulates two Plex clients
// reading the same file at different offsets.
func TestE2E_MultipleReadersDifferentOffsets(t *testing.T) {
	d, sr, _ := newTestDashboardStreamReader(t)

	const fileSize int64 = 100 * 1024 * 1024

	// Two handles reading at different positions.
	sr.TrackReader("movie.mkv", 1, fileSize/4)   // 25%
	sr.TrackReader("movie.mkv", 2, 3*fileSize/4) // 75%

	snap := d.Snapshot()
	var movie *FileSnapshotJSON
	for i := range snap.Active {
		if snap.Active[i].FileKey == "movie.mkv" {
			movie = &snap.Active[i]
			break
		}
	}
	if movie == nil {
		t.Fatal("expected movie.mkv in active files")
	}
	if len(movie.ReadOffsets) != 2 {
		t.Fatalf("ReadOffsets = %d, want 2", len(movie.ReadOffsets))
	}

	// Both offsets should be present (order not guaranteed from map iteration).
	offs := make(map[int64]bool)
	for _, o := range movie.ReadOffsets {
		offs[o] = true
	}
	if !offs[fileSize/4] {
		t.Errorf("missing read offset %d", fileSize/4)
	}
	if !offs[3*fileSize/4] {
		t.Errorf("missing read offset %d", 3*fileSize/4)
	}
}

// TestE2E_FileWithCachedBlocksShowsInActive verifies that a file with
// cached data (but no active session/readers) appears in Active with
// correct cached block info and priority coloring.
func TestE2E_FileWithCachedBlocksShowsInActive(t *testing.T) {
	d, rc := newTestDashboardWithCache(t)

	// Simulate a file that was previously downloaded and is still in cache.
	// High-priority block at 0-4KiB, low-priority block at 4-8KiB.
	rc.PutWithPriority("movie.mkv", 0, make([]byte, 4096), cache.PriorityHigh)
	rc.Put("movie.mkv", 4096, make([]byte, 4096))

	snap := d.Snapshot()
	if len(snap.Active) != 1 {
		t.Fatalf("Active = %d files, want 1", len(snap.Active))
	}

	f := snap.Active[0]
	if f.FileKey != "movie.mkv" {
		t.Fatalf("FileKey = %q, want movie.mkv", f.FileKey)
	}
	if len(f.CachedBlocks) != 2 {
		t.Fatalf("CachedBlocks = %d, want 2", len(f.CachedBlocks))
	}
	// High priority block first.
	if f.CachedBlocks[0].Priority != cache.PriorityHigh {
		t.Errorf("Block 0 priority = %d, want PriorityHigh (green in UI)", f.CachedBlocks[0].Priority)
	}
	// Low priority block second.
	if f.CachedBlocks[1].Priority != cache.PriorityLow {
		t.Errorf("Block 1 priority = %d, want PriorityLow (yellow in UI)", f.CachedBlocks[1].Priority)
	}
	// No inflight windows.
	if len(f.Inflight) != 0 {
		t.Errorf("Inflight = %d, want 0", len(f.Inflight))
	}
	// No active readers.
	if len(f.ReadOffsets) != 0 {
		t.Errorf("ReadOffsets = %d, want 0", len(f.ReadOffsets))
	}
}

// TestE2E_ReaderUntrackedRemovesOffset verifies that when a FUSE handle
// closes (Release), the read cursor disappears from the snapshot.
// This tests the "курсор чтения исчезает" part of the lifecycle.
func TestE2E_ReaderUntrackedRemovesOffset(t *testing.T) {
	d, sr, _ := newTestDashboardStreamReader(t)

	sr.TrackReader("movie.mkv", 1, 50*1024*1024)
	sr.TrackReader("movie.mkv", 2, 75*1024*1024)

	// Both cursors present.
	snap := d.Snapshot()
	findMovie := func(s *DashboardSnapshot) *FileSnapshotJSON {
		for i := range s.Active {
			if s.Active[i].FileKey == "movie.mkv" {
				return &s.Active[i]
			}
		}
		return nil
	}
	m := findMovie(snap)
	if m == nil {
		t.Fatal("expected movie.mkv in active files")
	}
	if len(m.ReadOffsets) != 2 {
		t.Fatalf("ReadOffsets = %d, want 2 before untrack", len(m.ReadOffsets))
	}

	// First handle closes — one cursor removed.
	sr.UntrackReader("movie.mkv", 1)
	snap = d.Snapshot()
	m = findMovie(snap)
	if m == nil {
		t.Fatal("expected movie.mkv still in active after one untrack")
	}
	if len(m.ReadOffsets) != 1 {
		t.Fatalf("ReadOffsets = %d after one untrack, want 1", len(m.ReadOffsets))
	}

	// Second handle closes — no more cursors, file still in Active if cache exists.
	sr.UntrackReader("movie.mkv", 2)
	snap = d.Snapshot()
	m = findMovie(snap)
	// File may or may not be in Active depending on whether there are cached blocks.
	// With no cache data and no readers, it disappears from the snapshot.
	if m != nil && len(m.ReadOffsets) != 0 {
		t.Errorf("ReadOffsets = %d after all untracks, want 0", len(m.ReadOffsets))
	}
}
