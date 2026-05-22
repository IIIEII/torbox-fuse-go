package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- NewRangeCache ---

func TestNewRangeCache_SetsBudget(t *testing.T) {
	rc := NewRangeCache(1024)
	if rc == nil {
		t.Fatal("NewRangeCache returned nil")
	}
	if rc.budget != 1024 {
		t.Errorf("budget: got %d, want 1024", rc.budget)
	}
}

func TestNewRangeCache_ZeroBudget(t *testing.T) {
	rc := NewRangeCache(0)
	if rc == nil {
		t.Fatal("NewRangeCache returned nil")
	}
	// Zero budget means everything gets evicted immediately
}

// --- CopyTo ---

func TestCopyTo_HitExactOffset(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("hello world"))

	dst := make([]byte, 11)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok {
		t.Fatal("CopyTo: expected cache hit")
	}
	if n != 11 {
		t.Errorf("CopyTo n: got %d, want 11", n)
	}
	if string(dst) != "hello world" {
		t.Errorf("CopyTo dst: got %q, want %q", string(dst), "hello world")
	}
}

func TestCopyTo_HitPartialOverlap(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("hello world"))

	// Read from offset 6 within the block that starts at 0
	dst := make([]byte, 5)
	n, ok := rc.CopyTo("file1", 6, dst)
	if !ok {
		t.Fatal("CopyTo: expected cache hit")
	}
	if n != 5 {
		t.Errorf("CopyTo n: got %d, want 5", n)
	}
	if string(dst[:n]) != "world" {
		t.Errorf("CopyTo dst: got %q, want %q", string(dst[:n]), "world")
	}
}

func TestCopyTo_Miss(t *testing.T) {
	rc := NewRangeCache(4096)
	dst := make([]byte, 10)
	n, ok := rc.CopyTo("nonexistent", 0, dst)
	if ok {
		t.Error("CopyTo: expected cache miss")
	}
	if n != 0 {
		t.Errorf("CopyTo n on miss: got %d, want 0", n)
	}
}

func TestCopyTo_OffsetOutOfRange(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("hello"))

	// Request offset beyond block
	dst := make([]byte, 5)
	n, ok := rc.CopyTo("file1", 100, dst)
	if ok {
		t.Error("CopyTo: expected miss for offset beyond block")
	}
	if n != 0 {
		t.Errorf("CopyTo n: got %d, want 0", n)
	}
}

func TestCopyTo_OffsetBeforeBlock(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 100, []byte("hello"))

	// Request offset before the block starts
	dst := make([]byte, 5)
	n, ok := rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("CopyTo: expected miss for offset before block")
	}
	if n != 0 {
		t.Errorf("CopyTo n: got %d, want 0", n)
	}
}

func TestCopyTo_PartialCopyAtEnd(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("hello"))

	// Request a large buffer but block only has 5 bytes from offset 3
	dst := make([]byte, 20)
	n, ok := rc.CopyTo("file1", 3, dst)
	if !ok {
		t.Fatal("CopyTo: expected cache hit")
	}
	if n != 2 {
		t.Errorf("CopyTo n: got %d, want 2", n)
	}
	if string(dst[:n]) != "lo" {
		t.Errorf("CopyTo dst: got %q, want %q", string(dst[:n]), "lo")
	}
}

func TestCopyTo_MultipleBlocks(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("hello "))
	rc.Put("file1", 6, []byte("world"))

	// First block
	dst := make([]byte, 6)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 6 || string(dst) != "hello " {
		t.Errorf("CopyTo first block: ok=%v n=%d dst=%q", ok, n, string(dst))
	}

	// Second block
	dst2 := make([]byte, 5)
	n2, ok2 := rc.CopyTo("file1", 6, dst2)
	if !ok2 || n2 != 5 || string(dst2) != "world" {
		t.Errorf("CopyTo second block: ok=%v n=%d dst=%q", ok2, n2, string(dst2))
	}
}

func TestCopyTo_ZeroAllocHit(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("hello"))

	dst := make([]byte, 5)
	// This should not allocate — just copy into existing buffer
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 5 || string(dst) != "hello" {
		t.Errorf("CopyTo zero-alloc: ok=%v n=%d dst=%q", ok, n, string(dst))
	}
}

// --- Put ---

func TestPut_StoresData(t *testing.T) {
	rc := NewRangeCache(4096)
	data := []byte("test data")
	rc.Put("file1", 0, data)

	dst := make([]byte, 9)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 9 || string(dst[:n]) != "test data" {
		t.Errorf("After Put: ok=%v n=%d data=%q", ok, n, string(dst[:n]))
	}
}

func TestPut_CopiesData(t *testing.T) {
	rc := NewRangeCache(4096)
	data := []byte("original")
	rc.Put("file1", 0, data)

	// Mutate original; cache should be unaffected
	data[0] = 'X'

	dst := make([]byte, 8)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 8 || string(dst[:n]) != "original" {
		t.Errorf("After mutating source: ok=%v n=%d data=%q want %q", ok, n, string(dst[:n]), "original")
	}
}

func TestPut_OverwriteExistingBlock(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("old"))
	rc.Put("file1", 0, []byte("new"))

	dst := make([]byte, 3)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 3 || string(dst) != "new" {
		t.Errorf("Overwrite: ok=%v n=%d data=%q", ok, n, string(dst))
	}
}

// --- Budget enforcement ---

func TestPut_BudgetEviction(t *testing.T) {
	// Budget of 10 bytes
	rc := NewRangeCache(10)

	rc.Put("file1", 0, []byte("12345")) // 5 bytes
	rc.Put("file1", 5, []byte("67890")) // 5 bytes, total=10, at budget

	// Adding another block should trigger eviction
	rc.Put("file2", 0, []byte("abcde")) // 5 bytes, total would be 15

	// The oldest-accessed block (file1@0) should be evicted
	dst := make([]byte, 5)
	_, ok := rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("file1@0 should have been evicted")
	}

	// file1@5 should still be present
	n, ok := rc.CopyTo("file1", 5, dst)
	if !ok || n != 5 || string(dst) != "67890" {
		t.Errorf("file1@5: ok=%v n=%d data=%q", ok, n, string(dst))
	}

	// file2@0 should be present
	n, ok = rc.CopyTo("file2", 0, dst)
	if !ok || n != 5 || string(dst) != "abcde" {
		t.Errorf("file2@0: ok=%v n=%d data=%q", ok, n, string(dst))
	}
}

func TestPut_BudgetEvictionMultipleBlocks(t *testing.T) {
	// Budget of 6 bytes
	rc := NewRangeCache(6)

	rc.Put("a", 0, []byte("111")) // 3 bytes
	time.Sleep(time.Millisecond) // ensure distinct timestamps for LRU
	rc.Put("b", 0, []byte("222")) // 3 bytes, total=6, at budget

	// Add a 3-byte block — should evict oldest (a@0)
	rc.Put("c", 0, []byte("333"))

	dst := make([]byte, 3)
	_, ok := rc.CopyTo("a", 0, dst)
	if ok {
		t.Error("a@0 should have been evicted")
	}

	// b@0 and c@0 should survive
	_, ok = rc.CopyTo("b", 0, dst)
	if !ok {
		t.Error("b@0 should still be cached")
	}
	_, ok = rc.CopyTo("c", 0, dst)
	if !ok {
		t.Error("c@0 should still be cached")
	}
}

func TestPut_ZeroBudgetEvictsImmediately(t *testing.T) {
	rc := NewRangeCache(0)
	rc.Put("file1", 0, []byte("data"))

	dst := make([]byte, 4)
	_, ok := rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("With zero budget, Put should immediately evict")
	}
}

func TestPut_BudgetTracksUsedBytes(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("1234")) // 4 bytes
	rc.Put("file1", 4, []byte("5678")) // 4 bytes

	if rc.used.Load() != 8 {
		t.Errorf("used: got %d, want 8", rc.used.Load())
	}
}

func TestPut_OverwriteAdjustsUsed(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("1234")) // 4 bytes
	rc.Put("file1", 0, []byte("12345678")) // 8 bytes (overwrite, old was 4)

	if rc.used.Load() != 8 {
		t.Errorf("used after overwrite: got %d, want 8", rc.used.Load())
	}
}

// --- PutWithSession ---

func TestPutWithSession_StoresSessionID(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.PutWithSession("file1", 0, []byte("data"), 42)

	// Verify the data is stored
	dst := make([]byte, 4)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 4 || string(dst) != "data" {
		t.Errorf("PutWithSession data: ok=%v n=%d data=%q", ok, n, string(dst))
	}

	// Verify session via EvictStale behavior
	rc.PutWithSession("file1", 0, []byte("dat2"), 43)
	rc.EvictStale("file1", 44)

	_, ok = rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("After EvictStale with currentSession=44, block with sessionID=43 should be evicted")
	}
}

// --- EvictStale ---

func TestEvictStale_RemovesOldSessionBlocks(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.PutWithSession("file1", 0, []byte("old"), 1)
	rc.PutWithSession("file1", 100, []byte("cur"), 5)
	rc.PutWithSession("file1", 200, []byte("new"), 10)

	rc.EvictStale("file1", 5)

	dst := make([]byte, 3)
	// sessionID=1 < currentSession=5 => evicted
	_, ok := rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("sessionID=1 block should be evicted")
	}

	// sessionID=5 is NOT < 5 => kept
	n, ok := rc.CopyTo("file1", 100, dst)
	if !ok || n != 3 || string(dst) != "cur" {
		t.Errorf("sessionID=5 block: ok=%v n=%d data=%q", ok, n, string(dst))
	}

	// sessionID=10 > 5 => kept
	n, ok = rc.CopyTo("file1", 200, dst)
	if !ok || n != 3 || string(dst) != "new" {
		t.Errorf("sessionID=10 block: ok=%v n=%d data=%q", ok, n, string(dst))
	}
}

func TestEvictStale_ZeroSessionIDNotEvicted(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("no-session")) // sessionID = 0

	rc.EvictStale("file1", 100)

	dst := make([]byte, 9)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 9 {
		t.Errorf("sessionID=0 block should NOT be evicted: ok=%v n=%d", ok, n)
	}
}

func TestEvictStale_NoBlocksForFile(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.PutWithSession("other", 0, []byte("data"), 1)

	// Should not panic and should not affect other files
	rc.EvictStale("nonexistent", 100)

	dst := make([]byte, 4)
	n, ok := rc.CopyTo("other", 0, dst)
	if !ok || n != 4 {
		t.Errorf("Other file should be unaffected: ok=%v n=%d", ok, n)
	}
}

func TestEvictStale_AdjustsUsedBytes(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.PutWithSession("file1", 0, []byte("1234"), 1) // 4 bytes
	rc.PutWithSession("file1", 4, []byte("5678"), 5) // 4 bytes, total=8

	rc.EvictStale("file1", 5)

	// Only block with sessionID=1 should be evicted (4 bytes)
	if rc.used.Load() != 4 {
		t.Errorf("used after EvictStale: got %d, want 4", rc.used.Load())
	}
}

func TestEvictStale_OnlyAffectsTargetFileKey(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.PutWithSession("file1", 0, []byte("old"), 1)
	rc.PutWithSession("file2", 0, []byte("old2"), 1)

	rc.EvictStale("file1", 5)

	dst := make([]byte, 4)
	_, ok := rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("file1 block should be evicted")
	}

	n, ok := rc.CopyTo("file2", 0, dst)
	if !ok || n != 4 || string(dst) != "old2" {
		t.Errorf("file2 block should be unaffected: ok=%v n=%d", ok, n)
	}
}

// --- Sharding ---

func TestShard_DifferentFileKeysDistributeAcrossShards(t *testing.T) {
	rc := NewRangeCache(1 << 20)

	// Insert many blocks with different fileKeys; at least some should land
	// in different shards (probabilistic, but with 100 keys vs 32 shards
	// it's virtually certain)
	shardCounts := make(map[int]int)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("file%d", i)
		rc.Put(key, 0, []byte("data"))
	}

	for i := range rc.shards {
		rc.shards[i].mu.RLock()
		count := len(rc.shards[i].blocks)
		rc.shards[i].mu.RUnlock()
		if count > 0 {
			shardCounts[count] = shardCounts[count] + 1
		}
	}

	// We should see multiple distinct shard fill levels
	if len(shardCounts) < 2 {
		t.Errorf("Expected blocks in multiple shards, got %d distinct fill levels", len(shardCounts))
	}
}

// --- Concurrency ---

func TestConcurrent_PutAndCopyTo(t *testing.T) {
	rc := NewRangeCache(1 << 20)

	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Writers
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("file%d", id)
			data := []byte(fmt.Sprintf("data-from-%d", id))
			for i := 0; i < iterations; i++ {
				rc.Put(key, int64(i*10), data)
			}
		}(g)
	}

	// Readers
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("file%d", id)
			dst := make([]byte, 64)
			for i := 0; i < iterations; i++ {
				rc.CopyTo(key, int64(i*10), dst)
			}
		}(g)
	}

	wg.Wait()
	// If we get here without panics or races, the test passes.
}

func TestConcurrent_PutAndEvictStale(t *testing.T) {
	rc := NewRangeCache(1 << 20)

	const goroutines = 20
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("file%d", id)
			for i := 0; i < iterations; i++ {
				rc.PutWithSession(key, int64(i*10), []byte("data"), int64(i))
				rc.EvictStale(key, int64(i)+1)
			}
		}(g)
	}

	wg.Wait()
}

func TestConcurrent_BudgetEviction(t *testing.T) {
	rc := NewRangeCache(1024)

	const goroutines = 20
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("file%d", id)
			data := make([]byte, 32)
			for i := 0; i < iterations; i++ {
				rc.Put(key, int64(i*32), data)
			}
		}(g)
	}

	wg.Wait()

	// used should never exceed budget
	used := rc.used.Load()
	if used > rc.budget {
		t.Errorf("used %d exceeds budget %d after concurrent Puts", used, rc.budget)
	}
}

// --- lastAccess ---

func TestCopyTo_UpdatesLastAccess(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("first"))
	rc.Put("file2", 0, []byte("second"))

	// Access file2 after a small delay to ensure different timestamps
	time.Sleep(10 * time.Millisecond)
	dst := make([]byte, 6)
	rc.CopyTo("file2", 0, dst)

	// Now add enough data to trigger eviction.
	// file1 should be evicted first since it has the older lastAccess.
	rc.Put("file3", 0, make([]byte, 4090)) // nearly fills budget, triggers eviction of oldest

	_, ok := rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("file1 should have been evicted (older lastAccess)")
	}
}

// --- Edge cases ---

func TestCopyTo_EmptyDst(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte("hello"))

	n, ok := rc.CopyTo("file1", 0, nil)
	if ok || n != 0 {
		t.Errorf("Empty dst: ok=%v n=%d, want ok=false n=0", ok, n)
	}
}

func TestPut_EmptyData(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.Put("file1", 0, []byte{})

	dst := make([]byte, 1)
	_, ok := rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("Empty data block should be a miss or not stored")
	}
}

func TestEvictStale_CurrentSessionEqualsBlockSession(t *testing.T) {
	rc := NewRangeCache(4096)
	rc.PutWithSession("file1", 0, []byte("data"), 5)

	rc.EvictStale("file1", 5)

	dst := make([]byte, 4)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 4 {
		t.Errorf("Block with sessionID == currentSession should NOT be evicted: ok=%v n=%d", ok, n)
	}
}