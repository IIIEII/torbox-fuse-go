package cache

import (
	"testing"
)

func TestFileBlocksEmpty(t *testing.T) {
	rc := NewRangeCache(1024*1024, nil) // 1 MiB budget
	blocks := rc.FileBlocks("nonexistent")
	if len(blocks) != 0 {
		t.Errorf("FileBlocks on empty cache = %d blocks, want 0", len(blocks))
	}
}

func TestFileBlocksSingleFile(t *testing.T) {
	rc := NewRangeCache(1024*1024, nil)

	// Put two blocks for "movie1".
	rc.PutWithPriority("movie1", 0, make([]byte, 4096), PriorityHigh)
	rc.PutWithPriority("movie1", 4096, make([]byte, 8192), PriorityLow)

	blocks := rc.FileBlocks("movie1")
	if len(blocks) != 2 {
		t.Fatalf("FileBlocks = %d blocks, want 2", len(blocks))
	}

	// Verify block details.
	var found0, found4096 bool
	for _, b := range blocks {
		switch b.Start {
		case 0:
			found0 = true
			if b.End != 4096 {
				t.Errorf("block at 0: End = %d, want 4096", b.End)
			}
			if b.Priority != PriorityHigh {
				t.Errorf("block at 0: Priority = %d, want %d", b.Priority, PriorityHigh)
			}
		case 4096:
			found4096 = true
			if b.End != 12288 {
				t.Errorf("block at 4096: End = %d, want 12288", b.End)
			}
			if b.Priority != PriorityLow {
				t.Errorf("block at 4096: Priority = %d, want %d", b.Priority, PriorityLow)
			}
		}
	}
	if !found0 {
		t.Error("missing block at offset 0")
	}
	if !found4096 {
		t.Error("missing block at offset 4096")
	}
}

func TestFileBlocksMultipleFiles(t *testing.T) {
	rc := NewRangeCache(1024*1024, nil)

	rc.Put("movie1", 0, make([]byte, 4096))
	rc.Put("movie2", 0, make([]byte, 8192))

	if len(rc.FileBlocks("movie1")) != 1 {
		t.Errorf("FileBlocks(movie1) = %d, want 1", len(rc.FileBlocks("movie1")))
	}
	if len(rc.FileBlocks("movie2")) != 1 {
		t.Errorf("FileBlocks(movie2) = %d, want 1", len(rc.FileBlocks("movie2")))
	}
}

func TestAllFileKeysEmpty(t *testing.T) {
	rc := NewRangeCache(1024*1024, nil)
	keys := rc.AllFileKeys()
	if len(keys) != 0 {
		t.Errorf("AllFileKeys on empty cache = %d, want 0", len(keys))
	}
}

func TestAllFileKeysDedup(t *testing.T) {
	rc := NewRangeCache(1024*1024, nil)

	// Put multiple blocks for the same file — should deduplicate.
	rc.Put("movie1", 0, make([]byte, 4096))
	rc.Put("movie1", 4096, make([]byte, 8192))
	rc.Put("movie2", 0, make([]byte, 2048))

	keys := rc.AllFileKeys()
	if len(keys) != 2 {
		t.Errorf("AllFileKeys = %d keys, want 2: %v", len(keys), keys)
	}

	// Verify both keys are present.
	seen := make(map[string]bool)
	for _, k := range keys {
		seen[k] = true
	}
	if !seen["movie1"] || !seen["movie2"] {
		t.Errorf("expected movie1 and movie2, got %v", keys)
	}
}

func TestBudgetMethod(t *testing.T) {
	rc := NewRangeCache(16*1024*1024, nil) // 16 MiB
	if rc.Budget() != 16*1024*1024 {
		t.Errorf("Budget() = %d, want %d", rc.Budget(), 16*1024*1024)
	}
}