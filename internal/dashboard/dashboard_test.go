package dashboard

import (
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"github.com/iiieii/torbox-fuse-go/internal/stream"
)

// newTestStreamReader creates a StreamReader with a test CDN for unit tests.
func newTestStreamReader(t *testing.T) *stream.StreamReader {
	t.Helper()
	rc := cache.NewRangeCache(4*1024*1024, nil) // 4 MiB budget
	cdn := stream.NewCDNClient(2, nil, 0)       // 2 concurrent, no URL caching
	return stream.NewStreamReader(rc, cdn, 2, 16, 1024*1024, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)
}

func TestSnapshotEmptyCache(t *testing.T) {
	m := metrics.New()
	rc := cache.NewRangeCache(1024*1024, m) // 1 MiB budget
	sr := newTestStreamReader(t)

	d := New(sr, rc, nil, m)

	snap := d.Snapshot()

	if snap.Summary.BudgetSlotsTotal != 16 {
		t.Errorf("BudgetSlotsTotal = %d, want 16", snap.Summary.BudgetSlotsTotal)
	}
	if len(snap.Active) != 0 {
		t.Errorf("Active = %d files, want 0", len(snap.Active))
	}
	if len(snap.Cached) != 0 {
		t.Errorf("Cached = %d files, want 0", len(snap.Cached))
	}
}

func TestSnapshotWithCachedData(t *testing.T) {
	m := metrics.New()
	rc := cache.NewRangeCache(1024*1024, m)
	sr := newTestStreamReader(t)

	// Put some data into the cache.
	rc.PutWithPriority("torrent:1:100", 0, make([]byte, 4096), cache.PriorityHigh)
	rc.Put("torrent:1:100", 4096, make([]byte, 4096))

	d := New(sr, rc, nil, m)

	snap := d.Snapshot()

	// Should have one cached file with 2 blocks.
	if len(snap.Cached) != 1 {
		t.Fatalf("Cached = %d files, want 1", len(snap.Cached))
	}

	f := snap.Cached[0]
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
}

func TestSnapshotPathResolution(t *testing.T) {
	m := metrics.New()
	rc := cache.NewRangeCache(1024*1024, m)

	// Put data in cache.
	rc.Put("torrent:42:999", 0, make([]byte, 1024))

	sr := newTestStreamReader(t)
	d := New(sr, rc, nil, m)

	// Without a stateDB, path resolution should return the fileKey as-is.
	snap := d.Snapshot()
	if len(snap.Cached) != 1 {
		t.Fatalf("Cached = %d, want 1", len(snap.Cached))
	}
	if snap.Cached[0].FilePath != "torrent:42:999" {
		t.Errorf("FilePath = %q, want %q", snap.Cached[0].FilePath, "torrent:42:999")
	}
}

func TestSnapshotSummary(t *testing.T) {
	m := metrics.New()
	rc := cache.NewRangeCache(16*1024*1024, m) // 16 MiB budget
	sr := newTestStreamReader(t)

	d := New(sr, rc, nil, m)

	snap := d.Snapshot()

	// Budget should be 16 (default maxGlobalWindows).
	if snap.Summary.BudgetSlotsTotal != 16 {
		t.Errorf("BudgetSlotsTotal = %d, want 16", snap.Summary.BudgetSlotsTotal)
	}
	// Cache budget should be 16 MiB.
	if snap.Summary.CacheBytesBudget != 16*1024*1024 {
		t.Errorf("CacheBytesBudget = %d, want %d", snap.Summary.CacheBytesBudget, 16*1024*1024)
	}
}

func TestSnapshotReaderTracking(t *testing.T) {
	m := metrics.New()
	rc := cache.NewRangeCache(4*1024*1024, m)
	sr := newTestStreamReader(t)

	// Track some readers.
	sr.TrackReader("file1", 1, 100)
	sr.TrackReader("file1", 2, 500)

	d := New(sr, rc, nil, m)

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

func TestSnapshotRecentlyClosed(t *testing.T) {
	m := metrics.New()
	rc := cache.NewRangeCache(4*1024*1024, m)
	sr := newTestStreamReader(t)

	// CancelFile records in recentlyClosed if there was any inflight window.
	// Since we can't easily create inflight windows in unit tests (they require CDN),
	// we'll verify that recentlyClosed starts empty and that CancelFile
	// on a file with no inflight windows doesn't add an entry (fileSize=0).
	sr.CancelFile("nonexistent:0:0")

	d := New(sr, rc, nil, m)
	snap := d.Snapshot()

	// No recently closed files because "nonexistent:0:0" had no inflight windows
	// (so no fileSize was captured).
	for _, cf := range snap.RecentlyClosed {
		t.Errorf("unexpected recently closed file: %q", cf.FileKey)
	}
}

func TestSnapshotTimestamp(t *testing.T) {
	m := metrics.New()
	rc := cache.NewRangeCache(1024*1024, m)
	sr := newTestStreamReader(t)

	d := New(sr, rc, nil, m)
	snap := d.Snapshot()

	// Timestamp should be parseable as RFC3339.
	if _, err := time.Parse(time.RFC3339, snap.Timestamp); err != nil {
		t.Errorf("Timestamp %q is not valid RFC3339: %v", snap.Timestamp, err)
	}
}

func TestSnapshotSortOrder(t *testing.T) {
	m := metrics.New()
	rc := cache.NewRangeCache(4*1024*1024, m)
	sr := newTestStreamReader(t)

	// Put data in cache for multiple files.
	rc.Put("torrent:2:200", 0, make([]byte, 1024))
	rc.Put("torrent:1:100", 0, make([]byte, 1024))
	rc.Put("torrent:3:300", 0, make([]byte, 1024))

	d := New(sr, rc, nil, m)
	snap := d.Snapshot()

	// Cached files should be sorted by FilePath (which falls back to fileKey
	// when no stateDB is available, so they sort by fileKey).
	if len(snap.Cached) < 2 {
		t.Skipf("only %d cached files, need at least 2", len(snap.Cached))
	}
	for i := 1; i < len(snap.Cached); i++ {
		if snap.Cached[i-1].FilePath > snap.Cached[i].FilePath {
			t.Errorf("cached files not sorted: %q > %q", snap.Cached[i-1].FilePath, snap.Cached[i].FilePath)
		}
	}
}
