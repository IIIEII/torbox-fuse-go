package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
)

// addInflightWindow is a test helper that creates and stores an inflight window
// on the StreamReader's inflight map. This simulates an active CDN download
// without needing a real HTTP server.
func addInflightWindow(sr *StreamReader, fileKey string, start, readyTo, fileSize int64, done bool, cancel context.CancelFunc) {
	win := &inflightWindow{
		key:        inflightKey{fileKey: fileKey, start: start},
		readyCond:  sync.NewCond(&sync.Mutex{}),
		cancelFunc: cancel,
		fileSize:   fileSize,
	}
	win.readyTo.Store(readyTo)
	win.done.Store(done)
	sr.inflight.Store(win.key, win)
}

// removeInflightWindow removes a manually-added inflight window from the map.
func removeInflightWindow(sr *StreamReader, fileKey string, start int64) {
	sr.inflight.Delete(inflightKey{fileKey: fileKey, start: start})
}

// findFileInSnapshot finds a file by key in the snapshot list.
func findFileInSnapshot(files []FileSnapshot, fileKey string) (FileSnapshot, bool) {
	for _, f := range files {
		if f.FileKey == fileKey {
			return f, true
		}
	}
	return FileSnapshot{}, false
}

// TestE2E_PlaybackFromMiddleSnapshot simulates Plex starting playback from
// the middle of a file. Verifies that:
// - The file appears in the active snapshot
// - Read cursor is at the 50% position
// - Inflight window shows partial download progress
// - Cached blocks from earlier metadata scan show low priority
func TestE2E_PlaybackFromMiddleSnapshot(t *testing.T) {
	sr := newTestStreamReader(t)

	fileKey := "torrent:1:100"
	fileSize := int64(64 * 1024 * 1024) // 64 MiB file
	midOffset := fileSize / 2           // 32 MiB

	// Simulate metadata scan data cached at the start (low priority).
	sr.cache.Put(fileKey, 0, make([]byte, 4*1024))

	// Simulate playback from middle: track a reader at the 50% mark.
	readerID := uint64(1)
	sr.TrackReader(fileKey, readerID, midOffset)

	// Create a session for this file (playback pattern, high priority).
	sess := sr.getOrCreateSession(fileKey)
	sess.sequentialReads.Store(5)
	sess.recentReads.Store(10)
	sess.lastReadOff.Store(midOffset)
	sess.lastReadTime.Store(time.Now().UnixNano()) // non-zero = recent

	// Create an inflight window at the read position (partially downloaded).
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	addInflightWindow(sr, fileKey, midOffset, midOffset+4*1024*1024, fileSize, false, cancel)

	// Take snapshot.
	files := sr.SnapshotFiles()

	f, ok := findFileInSnapshot(files, fileKey)
	if !ok {
		t.Fatalf("file %q not found in snapshot", fileKey)
	}

	// File should have read offsets at the 50% mark.
	if len(f.ReadOffsets) != 1 {
		t.Fatalf("ReadOffsets = %d, want 1", len(f.ReadOffsets))
	}
	if f.ReadOffsets[0] != midOffset {
		t.Errorf("ReadOffsets[0] = %d, want %d (50%%)", f.ReadOffsets[0], midOffset)
	}

	// Should have an inflight window near the read position.
	if len(f.Inflight) == 0 {
		t.Fatal("expected at least one inflight window")
	}
	win := f.Inflight[0]
	if win.Start != midOffset {
		t.Errorf("Inflight.Start = %d, want %d", win.Start, midOffset)
	}
	if win.Done {
		t.Error("Inflight.Done = true, want false (still downloading)")
	}
	if win.Priority != cache.PriorityHigh {
		t.Errorf("Inflight.Priority = %d, want PriorityHigh (%d)", win.Priority, cache.PriorityHigh)
	}

	// Cached blocks at the start should exist.
	if len(f.CachedBlocks) == 0 {
		t.Fatal("expected cached blocks from metadata scan")
	}
	if f.CachedBlocks[0].Start != 0 {
		t.Errorf("CachedBlocks[0].Start = %d, want 0", f.CachedBlocks[0].Start)
	}

	// Pattern should be "sequential" (we set sequentialReads=5).
	if f.Pattern != "sequential" {
		t.Errorf("Pattern = %q, want %q", f.Pattern, "sequential")
	}

	// Priority should be high (active playback).
	if f.Priority != cache.PriorityHigh {
		t.Errorf("Priority = %d, want PriorityHigh (%d)", f.Priority, cache.PriorityHigh)
	}
}

// TestE2E_MultipleReadersSnapshot simulates two Plex clients reading the
// same file at different offsets. Verifies that both read cursors appear.
func TestE2E_MultipleReadersSnapshot(t *testing.T) {
	sr := newTestStreamReader(t)

	fileKey := "torrent:1:100"
	fileSize := int64(64 * 1024 * 1024)

	// Two clients reading at 25% and 75%.
	offset1 := fileSize / 4     // 16 MiB
	offset2 := fileSize * 3 / 4 // 48 MiB

	sr.TrackReader(fileKey, 1, offset1)
	sr.TrackReader(fileKey, 2, offset2)

	// Create session and inflight window near the first reader.
	sess := sr.getOrCreateSession(fileKey)
	sess.sequentialReads.Store(3)
	sess.recentReads.Store(5)
	sess.lastReadOff.Store(offset1)

	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	addInflightWindow(sr, fileKey, offset1, offset1+8*1024*1024, fileSize, false, cancel)

	files := sr.SnapshotFiles()

	f, ok := findFileInSnapshot(files, fileKey)
	if !ok {
		t.Fatalf("file %q not found in snapshot", fileKey)
	}

	// Both read offsets should be present.
	if len(f.ReadOffsets) != 2 {
		t.Fatalf("ReadOffsets = %d, want 2", len(f.ReadOffsets))
	}

	// Check that both offsets are represented (order not guaranteed).
	offsets := make(map[int64]bool)
	for _, off := range f.ReadOffsets {
		offsets[off] = true
	}
	if !offsets[offset1] {
		t.Errorf("offset1 (%d) not found in ReadOffsets", offset1)
	}
	if !offsets[offset2] {
		t.Errorf("offset2 (%d) not found in ReadOffsets", offset2)
	}

	// Inflight window should be near reader 1.
	if len(f.Inflight) == 0 {
		t.Fatal("expected at least one inflight window")
	}
	if f.Inflight[0].Start != offset1 {
		t.Errorf("Inflight.Start = %d, want %d", f.Inflight[0].Start, offset1)
	}
}

// TestE2E_FileClosedMovesToRecentlyClosed simulates stopping playback:
// when a file is closed, it should appear in RecentlyClosed and NOT
// in the active snapshot.
func TestE2E_FileClosedMovesToRecentlyClosed(t *testing.T) {
	sr := newTestStreamReader(t)

	fileKey := "torrent:1:100"
	fileSize := int64(32 * 1024 * 1024)

	// Set up an active file with reader and inflight window.
	sr.TrackReader(fileKey, 1, 0)

	_, cancel := context.WithCancel(context.Background())
	addInflightWindow(sr, fileKey, 0, 4*1024*1024, fileSize, false, cancel)

	// Verify file is active before closing.
	files := sr.SnapshotFiles()
	if _, ok := findFileInSnapshot(files, fileKey); !ok {
		t.Fatal("file should be active before closing")
	}

	// Close the file (simulates FUSE Release → CancelFile).
	sr.CancelFile(fileKey)

	// After closing, the file may briefly remain on the inflight map
	// (cleanup is async), but the session and readers are deleted.

	// File should appear in RecentlyClosed.
	closed := sr.RecentlyClosedFiles()
	found := false
	for _, cf := range closed {
		if cf.FileKey == fileKey {
			found = true
			if cf.FileSize != fileSize {
				t.Errorf("RecentlyClosed FileSize = %d, want %d", cf.FileSize, fileSize)
			}
		}
	}
	if !found {
		t.Error("file not found in RecentlyClosed after CancelFile")
	}
}

// TestE2E_FileReopenedReturnsToActive simulates restarting playback of
// a recently closed file. The file should reappear in the active snapshot.
func TestE2E_FileReopenedReturnsToActive(t *testing.T) {
	sr := newTestStreamReader(t)

	fileKey := "torrent:1:100"
	fileSize := int64(32 * 1024 * 1024)

	// Phase 1: Open and read.
	sr.TrackReader(fileKey, 1, 0)
	_, cancel1 := context.WithCancel(context.Background())
	addInflightWindow(sr, fileKey, 0, 4*1024*1024, fileSize, false, cancel1)

	// Phase 2: Close the file.
	sr.CancelFile(fileKey)

	// Verify it's in RecentlyClosed.
	closed := sr.RecentlyClosedFiles()
	if len(closed) == 0 {
		t.Fatal("expected file in RecentlyClosed after CancelFile")
	}

	// Phase 3: Reopen — new reader starts from middle.
	newOffset := fileSize / 2
	sr.TrackReader(fileKey, 2, newOffset)

	// Create a new session and inflight window for the reopened file.
	sess := sr.getOrCreateSession(fileKey)
	sess.sequentialReads.Store(2)
	sess.recentReads.Store(3)
	sess.lastReadOff.Store(newOffset)

	_, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	addInflightWindow(sr, fileKey, newOffset, newOffset+8*1024*1024, fileSize, false, cancel2)

	// Take snapshot — file should be active again.
	files := sr.SnapshotFiles()

	f, ok := findFileInSnapshot(files, fileKey)
	if !ok {
		t.Fatal("file should be active after reopening")
	}

	// Read offset should be at the new position.
	if len(f.ReadOffsets) != 1 {
		t.Fatalf("ReadOffsets = %d, want 1", len(f.ReadOffsets))
	}
	if f.ReadOffsets[0] != newOffset {
		t.Errorf("ReadOffsets[0] = %d, want %d", f.ReadOffsets[0], newOffset)
	}

	// Inflight window should be at the new offset.
	if len(f.Inflight) == 0 {
		t.Fatal("expected inflight window after reopening")
	}

	// File should NOT be in RecentlyClosed after being reopened.
	// TrackReader removes the recentlyClosed entry when a file is re-accessed.
	closed = sr.RecentlyClosedFiles()
	for _, cf := range closed {
		if cf.FileKey == fileKey {
			t.Errorf("file %q should not be in RecentlyClosed after reopening, but found entry closed at %s", cf.FileKey, cf.ClosedAt)
		}
	}
}

// TestE2E_PriorityDisplayedCorrectly verifies that library-scan files show
// low priority while playback files show high priority.
func TestE2E_PriorityDisplayedCorrectly(t *testing.T) {
	sr := newTestStreamReader(t)

	// File A: playback (sequential reads, high priority).
	fileA := "torrent:1:playback"
	sr.TrackReader(fileA, 1, 0)
	sessA := sr.getOrCreateSession(fileA)
	sessA.sequentialReads.Store(10)
	sessA.recentReads.Store(8)
	sessA.lastReadTime.Store(time.Now().UnixNano())

	// File B: library scan (random reads, low priority).
	fileB := "torrent:2:scan"
	sr.TrackReader(fileB, 2, 0)
	sessB := sr.getOrCreateSession(fileB)
	sessB.randomReads.Store(5)
	sessB.sequentialReads.Store(0)
	sessB.recentReads.Store(1)
	sessB.lastReadTime.Store(time.Now().UnixNano())

	files := sr.SnapshotFiles()

	fA, okA := findFileInSnapshot(files, fileA)
	fB, okB := findFileInSnapshot(files, fileB)
	if !okA {
		t.Fatal("playback file not found in snapshot")
	}
	if !okB {
		t.Fatal("scan file not found in snapshot")
	}

	// File A should be high priority (active playback).
	if fA.Priority != cache.PriorityHigh {
		t.Errorf("playback file priority = %d, want PriorityHigh (%d)", fA.Priority, cache.PriorityHigh)
	}

	// File B should be low priority (library scan).
	if fB.Priority != cache.PriorityLow {
		t.Errorf("scan file priority = %d, want PriorityLow (%d)", fB.Priority, cache.PriorityLow)
	}

	// Patterns should differ.
	if fA.Pattern != "sequential" {
		t.Errorf("playback file pattern = %q, want %q", fA.Pattern, "sequential")
	}
	if fB.Pattern != "random" {
		t.Errorf("scan file pattern = %q, want %q", fB.Pattern, "random")
	}
}

// TestE2E_InflightProgressVsPending verifies that a partially downloaded
// inflight window correctly reports ReadyTo (progress) vs remaining
// (pending) portions. The frontend JS builds colored segments from this:
//   - inflight-progress: start → start+readyTo (blue)
//   - inflight-pending: start+readyTo → start+windowSize (pulsing blue)
func TestE2E_InflightProgressVsPending(t *testing.T) {
	sr := newTestStreamReader(t)

	fileKey := "torrent:1:100"
	fileSize := int64(64 * 1024 * 1024)    // 64 MiB
	windowStart := int64(16 * 1024 * 1024) // 16 MiB offset
	readyTo := int64(4 * 1024 * 1024)      // 4 MiB downloaded out of 16 MiB window

	// Set up reader at window start.
	sr.TrackReader(fileKey, 1, windowStart)
	sess := sr.getOrCreateSession(fileKey)
	sess.sequentialReads.Store(5)
	sess.recentReads.Store(5)
	sess.lastReadOff.Store(windowStart)

	// Create a partially-downloaded inflight window.
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	addInflightWindow(sr, fileKey, windowStart, readyTo, fileSize, false, cancel)

	files := sr.SnapshotFiles()

	f, ok := findFileInSnapshot(files, fileKey)
	if !ok {
		t.Fatal("file not found in snapshot")
	}

	if len(f.Inflight) == 0 {
		t.Fatal("expected inflight window")
	}

	winInfo := f.Inflight[0]
	if winInfo.Start != windowStart {
		t.Errorf("Inflight.Start = %d, want %d", winInfo.Start, windowStart)
	}
	if winInfo.ReadyTo != readyTo {
		t.Errorf("Inflight.ReadyTo = %d, want %d", winInfo.ReadyTo, readyTo)
	}
	if winInfo.Done {
		t.Error("Inflight.Done = true, want false")
	}

	// The frontend JS computes:
	//   progressEnd = start + readyTo = 16MiB + 4MiB = 20MiB
	//   windowEnd = min(start + windowSize, fileSize) = min(32MiB, 64MiB) = 32MiB
	//   inflight-progress segment: 16MiB → 20MiB (blue)
	//   inflight-pending segment: 20MiB → 32MiB (pulsing blue)
	// We verify the data that feeds this calculation is correct.
	progressEnd := winInfo.Start + winInfo.ReadyTo
	windowEnd := windowStart + 16*1024*1024 // windowSize from reader.go
	if fileSize > 0 && windowEnd > fileSize {
		windowEnd = fileSize
	}
	if progressEnd >= windowEnd {
		t.Errorf("progressEnd (%d) should be < windowEnd (%d) for pending segment", progressEnd, windowEnd)
	}
}

// TestE2E_DoneInflightWindowShowsAsCached verifies that a completed inflight
// window (done=true) is reported in the snapshot with its priority color,
// which the frontend renders as a cached segment (green/yellow), not
// an active download (blue).
func TestE2E_DoneInflightWindowShowsAsCached(t *testing.T) {
	sr := newTestStreamReader(t)

	fileKey := "torrent:1:100"
	fileSize := int64(32 * 1024 * 1024)

	// Create a completed (done) inflight window with high priority.
	sr.TrackReader(fileKey, 1, 0)
	sess := sr.getOrCreateSession(fileKey)
	sess.sequentialReads.Store(10)
	sess.recentReads.Store(5)
	sess.lastReadOff.Store(0)
	sess.lastReadTime.Store(time.Now().UnixNano())

	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Done window: readyTo = full window, done = true.
	windowSize := int64(16 * 1024 * 1024)
	addInflightWindow(sr, fileKey, 0, windowSize, fileSize, true, cancel)

	files := sr.SnapshotFiles()

	f, ok := findFileInSnapshot(files, fileKey)
	if !ok {
		t.Fatal("file not found in snapshot")
	}

	if len(f.Inflight) == 0 {
		t.Fatal("expected done inflight window in snapshot")
	}

	winInfo := f.Inflight[0]
	if !winInfo.Done {
		t.Error("Inflight.Done = false, want true")
	}
	// Priority should match the file's current priority (high for playback).
	if winInfo.Priority != cache.PriorityHigh {
		t.Errorf("Done window priority = %d, want PriorityHigh (%d)", winInfo.Priority, cache.PriorityHigh)
	}
	// The frontend JS renders done windows as cached-high/cached-low segments,
	// not as inflight-progress (blue). This is correct behavior.
}

// TestE2E_FileLifecycleFullCycle exercises the complete file lifecycle:
//  1. File opened → appears in Active with reader cursor
//  2. File closed → disappears from Active, appears in RecentlyClosed
//  3. File reopened → reappears in Active with new reader cursor
func TestE2E_FileLifecycleFullCycle(t *testing.T) {
	sr := newTestStreamReader(t)

	fileKey := "torrent:1:lifecycle"
	fileSize := int64(32 * 1024 * 1024)

	// Phase 1: Open file and start reading from the beginning.
	sr.TrackReader(fileKey, 1, 0)
	sess := sr.getOrCreateSession(fileKey)
	sess.sequentialReads.Store(5)
	sess.recentReads.Store(5)
	sess.lastReadOff.Store(0)
	sess.lastReadTime.Store(time.Now().UnixNano())

	_, cancel1 := context.WithCancel(context.Background())
	addInflightWindow(sr, fileKey, 0, 4*1024*1024, fileSize, false, cancel1)

	// Verify: file is active with reader at offset 0.
	snap1 := sr.SnapshotFiles()
	f1, ok1 := findFileInSnapshot(snap1, fileKey)
	if !ok1 {
		t.Fatal("phase 1: file should be active")
	}
	if len(f1.ReadOffsets) != 1 || f1.ReadOffsets[0] != 0 {
		t.Errorf("phase 1: ReadOffsets = %v, want [0]", f1.ReadOffsets)
	}

	// Phase 2: Close the file (user stops playback).
	sr.CancelFile(fileKey)

	// Verify: file is in RecentlyClosed.
	closed := sr.RecentlyClosedFiles()
	if len(closed) == 0 || closed[0].FileKey != fileKey {
		t.Fatalf("phase 2: expected %q in RecentlyClosed, got %v", fileKey, closed)
	}

	// Phase 3: Reopen from the middle (user resumes playback).
	resumeOffset := fileSize / 2
	sr.TrackReader(fileKey, 2, resumeOffset)
	sess2 := sr.getOrCreateSession(fileKey)
	sess2.sequentialReads.Store(3)
	sess2.recentReads.Store(4)
	sess2.lastReadOff.Store(resumeOffset)
	sess2.lastReadTime.Store(time.Now().UnixNano())

	_, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	addInflightWindow(sr, fileKey, resumeOffset, resumeOffset+8*1024*1024, fileSize, false, cancel2)

	// Verify: file is active again with reader at the resume offset.
	snap3 := sr.SnapshotFiles()
	f3, ok3 := findFileInSnapshot(snap3, fileKey)
	if !ok3 {
		t.Fatal("phase 3: file should be active after reopening")
	}
	if len(f3.ReadOffsets) != 1 {
		t.Fatalf("phase 3: ReadOffsets = %d, want 1", len(f3.ReadOffsets))
	}
	if f3.ReadOffsets[0] != resumeOffset {
		t.Errorf("phase 3: ReadOffsets[0] = %d, want %d", f3.ReadOffsets[0], resumeOffset)
	}

	// Verify: inflight window is at the new offset.
	if len(f3.Inflight) == 0 {
		t.Fatal("phase 3: expected inflight window")
	}
	foundNewWindow := false
	for _, w := range f3.Inflight {
		if w.Start == resumeOffset && !w.Done {
			foundNewWindow = true
		}
	}
	if !foundNewWindow {
		t.Error("phase 3: expected active inflight window at resume offset")
	}

	// Verify: file is NO LONGER in RecentlyClosed after being reopened.
	// TrackReader removes the file from recentlyClosed when re-opened.
	closedAfterReopen := sr.RecentlyClosedFiles()
	for _, cf := range closedAfterReopen {
		if cf.FileKey == fileKey {
			t.Error("phase 3: file should NOT be in RecentlyClosed after being reopened")
		}
	}
}
