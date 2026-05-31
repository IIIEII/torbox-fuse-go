package stream

import (
	"context"
	"sync"
	"testing"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
)

func TestSnapshotFilesEmpty(t *testing.T) {
	sr := newTestStreamReader(t)
	files := sr.SnapshotFiles()
	if len(files) != 0 {
		t.Fatalf("expected 0 files from empty stream reader, got %d", len(files))
	}
}

func TestTrackUntrackReader(t *testing.T) {
	sr := newTestStreamReader(t)

	sr.TrackReader("file1", 1, 100)
	sr.TrackReader("file1", 2, 500)
	sr.TrackReader("file2", 3, 200)

	// Verify readers are tracked.
	if v, ok := sr.readers.Load("file1"); !ok {
		t.Fatal("expected file1 readers to exist")
	} else {
		rm := v.(*readersMap)
		rm.mu.Lock()
		if len(rm.pos) != 2 {
			t.Fatalf("expected 2 readers for file1, got %d", len(rm.pos))
		}
		rm.mu.Unlock()
	}

	// Untrack one reader.
	sr.UntrackReader("file1", 1)

	if v, ok := sr.readers.Load("file1"); !ok {
		t.Fatal("expected file1 readers to still exist")
	} else {
		rm := v.(*readersMap)
		rm.mu.Lock()
		if len(rm.pos) != 1 {
			t.Fatalf("expected 1 reader for file1 after untrack, got %d", len(rm.pos))
		}
		rm.mu.Unlock()
	}

	// Untrack the last reader — should remove the entry.
	sr.UntrackReader("file1", 2)
	if _, ok := sr.readers.Load("file1"); ok {
		t.Fatal("expected file1 readers to be removed after last untrack")
	}
}

func TestRecentlyClosedFiles(t *testing.T) {
	sr := newTestStreamReader(t)

	// CancelFile records in recentlyClosed when it finds inflight windows
	// with fileSize > 0. Set up an inflight window manually.
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	win := &inflightWindow{
		key:        inflightKey{fileKey: "test:1:1", start: 0},
		readyCond:  sync.NewCond(&sync.Mutex{}),
		cancelFunc: cancel,
		fileSize:   12345,
	}
	win.done.Store(true)
	sr.inflight.Store(win.key, win)

	sr.CancelFile("test:1:1")

	closed := sr.RecentlyClosedFiles()
	if len(closed) != 1 {
		t.Fatalf("expected 1 recently closed file, got %d", len(closed))
	}
	if closed[0].FileKey != "test:1:1" {
		t.Fatalf("expected file key 'test:1:1', got %q", closed[0].FileKey)
	}
	if closed[0].FileSize != 12345 {
		t.Fatalf("expected file size 12345, got %d", closed[0].FileSize)
	}
}

func TestRecentlyClosedFilesEmpty(t *testing.T) {
	sr := newTestStreamReader(t)

	// CancelFile on a file with no inflight windows should not record anything.
	sr.CancelFile("nonexistent")

	closed := sr.RecentlyClosedFiles()
	if len(closed) != 0 {
		t.Fatalf("expected 0 recently closed files, got %d", len(closed))
	}
}

func TestBudgetLimitAndHolding(t *testing.T) {
	sr := newTestStreamReader(t)

	limit := sr.BudgetLimit()
	if limit != 16 { // default maxGlobalWindows in test helper
		t.Fatalf("expected budget limit 16, got %d", limit)
	}

	// No inflight windows — holding should be 0.
	holding := sr.BudgetHolding()
	if holding != 0 {
		t.Fatalf("expected budget holding 0, got %d", holding)
	}

	// Add an inflight window that is NOT done — it should be counted.
	win := &inflightWindow{
		key:       inflightKey{fileKey: "budgettest:1:1", start: 0},
		readyCond: sync.NewCond(&sync.Mutex{}),
		fileSize:  1000,
	}
	win.readyTo.Store(500)
	win.done.Store(false) // still in progress
	sr.inflight.Store(win.key, win)

	holding = sr.BudgetHolding()
	if holding != 1 {
		t.Fatalf("expected budget holding 1 with one active window, got %d", holding)
	}

	// Mark the window as done — it should no longer be counted.
	win.done.Store(true)
	holding = sr.BudgetHolding()
	if holding != 0 {
		t.Fatalf("expected budget holding 0 with all windows done, got %d", holding)
	}
}

func TestSnapshotFilesWithInflight(t *testing.T) {
	sr := newTestStreamReader(t)

	// Set up an inflight window manually to verify snapshot picks it up.
	win := &inflightWindow{
		key:       inflightKey{fileKey: "torrent:1:1", start: 0},
		readyCond: sync.NewCond(&sync.Mutex{}),
		fileSize:  1000,
	}
	win.readyTo.Store(500) // partially downloaded
	win.done.Store(false)
	sr.inflight.Store(win.key, win)

	// Also add a session for this file.
	sess := sr.getOrCreateSession("torrent:1:1")
	sess.sequentialReads.Store(3)
	sess.lastReadOff.Store(450)
	sess.recentReads.Store(5)
	sess.lastReadTime.Store(0) // will be detected as idle

	files := sr.SnapshotFiles()
	if len(files) == 0 {
		t.Fatal("expected at least one file in snapshot")
	}

	found := false
	for _, f := range files {
		if f.FileKey == "torrent:1:1" {
			found = true
			if f.FileSize != 1000 {
				t.Fatalf("expected file size 1000, got %d", f.FileSize)
			}
			if len(f.Inflight) == 0 {
				t.Fatal("expected at least one inflight window")
			}
			if f.Inflight[0].Start != 0 {
				t.Fatalf("expected inflight start 0, got %d", f.Inflight[0].Start)
			}
			if f.Inflight[0].ReadyTo != 500 {
				t.Fatalf("expected inflight readyTo 500, got %d", f.Inflight[0].ReadyTo)
			}
		}
	}
	if !found {
		t.Fatal("expected to find torrent:1:1 in snapshot")
	}
}

// newTestStreamReader creates a StreamReader with a test CDN for unit tests.
func newTestStreamReader(t *testing.T) *StreamReader {
	t.Helper()
	rc := cache.NewRangeCache(4*1024*1024, nil) // 4 MiB budget
	cdn := NewCDNClient(2, nil, 0)               // 2 concurrent, no URL caching
	return NewStreamReader(rc, cdn, 2, 16, 1024*1024, func(fileKey string) string {
		return "http://cdn.example.com/" + fileKey
	}, nil)
}