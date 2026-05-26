package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/metrics"
)

// --- NewRangeCache ---

func TestNewRangeCache_SetsBudget(t *testing.T) {
	rc := NewRangeCache(1024, nil)
	if rc == nil {
		t.Fatal("NewRangeCache returned nil")
	}
	if rc.budget != 1024 {
		t.Errorf("budget: got %d, want 1024", rc.budget)
	}
}

func TestNewRangeCache_ZeroBudget(t *testing.T) {
	rc := NewRangeCache(0, nil)
	if rc == nil {
		t.Fatal("NewRangeCache returned nil")
	}
	// Zero budget means everything gets evicted immediately
}

// --- CopyTo ---

func TestCopyTo_Miss(t *testing.T) {
	rc := NewRangeCache(4096, nil)
	dst := make([]byte, 10)
	n, ok := rc.CopyTo("nonexistent", 0, dst)
	if ok {
		t.Error("CopyTo: expected cache miss")
	}
	if n != 0 {
		t.Errorf("CopyTo n on miss: got %d, want 0", n)
	}
}

func TestCopyTo_ZeroAllocHit(t *testing.T) {
	rc := NewRangeCache(4096, nil)
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
	rc := NewRangeCache(4096, nil)
	data := []byte("test data")
	rc.Put("file1", 0, data)

	dst := make([]byte, 9)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 9 || string(dst[:n]) != "test data" {
		t.Errorf("After Put: ok=%v n=%d data=%q", ok, n, string(dst[:n]))
	}
}

func TestPut_CopiesData(t *testing.T) {
	rc := NewRangeCache(4096, nil)
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
	rc := NewRangeCache(4096, nil)
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
	rc := NewRangeCache(10, nil)

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
	rc := NewRangeCache(6, nil)

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
	rc := NewRangeCache(0, nil)
	rc.Put("file1", 0, []byte("data"))

	dst := make([]byte, 4)
	_, ok := rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("With zero budget, Put should immediately evict")
	}
}

func TestPut_BudgetTracksUsedBytes(t *testing.T) {
	rc := NewRangeCache(4096, nil)
	rc.Put("file1", 0, []byte("1234")) // 4 bytes
	rc.Put("file1", 4, []byte("5678")) // 4 bytes

	if rc.used.Load() != 8 {
		t.Errorf("used: got %d, want 8", rc.used.Load())
	}
}

func TestPut_OverwriteAdjustsUsed(t *testing.T) {
	rc := NewRangeCache(4096, nil)
	rc.Put("file1", 0, []byte("1234")) // 4 bytes
	rc.Put("file1", 0, []byte("12345678")) // 8 bytes (overwrite, old was 4)

	if rc.used.Load() != 8 {
		t.Errorf("used after overwrite: got %d, want 8", rc.used.Load())
	}
}

// --- PutWithSession ---

func TestPutWithSession_StoresSessionID(t *testing.T) {
	rc := NewRangeCache(4096, nil)
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
	rc := NewRangeCache(4096, nil)
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
	rc := NewRangeCache(4096, nil)
	rc.Put("file1", 0, []byte("no-session")) // sessionID = 0

	rc.EvictStale("file1", 100)

	dst := make([]byte, 9)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 9 {
		t.Errorf("sessionID=0 block should NOT be evicted: ok=%v n=%d", ok, n)
	}
}

func TestEvictStale_NoBlocksForFile(t *testing.T) {
	rc := NewRangeCache(4096, nil)
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
	rc := NewRangeCache(4096, nil)
	rc.PutWithSession("file1", 0, []byte("1234"), 1) // 4 bytes
	rc.PutWithSession("file1", 4, []byte("5678"), 5) // 4 bytes, total=8

	rc.EvictStale("file1", 5)

	// Only block with sessionID=1 should be evicted (4 bytes)
	if rc.used.Load() != 4 {
		t.Errorf("used after EvictStale: got %d, want 4", rc.used.Load())
	}
}

func TestEvictStale_OnlyAffectsTargetFileKey(t *testing.T) {
	rc := NewRangeCache(4096, nil)
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

func TestEvictStale_CurrentSessionEqualsBlockSession(t *testing.T) {
	rc := NewRangeCache(4096, nil)
	rc.PutWithSession("file1", 0, []byte("data"), 5)

	rc.EvictStale("file1", 5)

	dst := make([]byte, 4)
	n, ok := rc.CopyTo("file1", 0, dst)
	if !ok || n != 4 {
		t.Errorf("Block with sessionID == currentSession should NOT be evicted: ok=%v n=%d", ok, n)
	}
}

// --- Sharding ---

func TestShard_DifferentFileKeysDistributeAcrossShards(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)

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
	rc := NewRangeCache(1 << 20, nil)

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
	rc := NewRangeCache(1 << 20, nil)

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
	rc := NewRangeCache(1024, nil)

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
	rc := NewRangeCache(4096, nil)
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

func TestPut_EmptyData(t *testing.T) {
	rc := NewRangeCache(4096, nil)
	rc.Put("file1", 0, []byte{})

	dst := make([]byte, 1)
	_, ok := rc.CopyTo("file1", 0, dst)
	if ok {
		t.Error("Empty data block should be a miss or not stored")
	}
}

// ============================================================
// Phase 1: Range logic unit tests (spec §1)
// ============================================================

// 1.1 Exact range match: request exactly the block that was stored.
func TestCopyTo_ExactRangeMatch(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	data := []byte("ABCDEFGHIJ") // 10 bytes at offset 0
	rc.Put("f1", 0, data)

	dst := make([]byte, 10)
	n, ok := rc.CopyTo("f1", 0, dst)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if n != 10 {
		t.Errorf("got %d bytes, want 10", n)
	}
	if string(dst) != "ABCDEFGHIJ" {
		t.Errorf("got %q, want %q", string(dst), "ABCDEFGHIJ")
	}
}

// 1.2 Full cover: request a range fully contained within a larger block.
func TestCopyTo_FullCover_SubrangeWithinLargerBlock(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	rc.Put("f1", 0, []byte("ABCDEFGHIJ")) // 10 bytes

	// Request bytes 3–6 ("DEFG"), fully inside the block
	dst := make([]byte, 4)
	n, ok := rc.CopyTo("f1", 3, dst)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if n != 4 {
		t.Errorf("got %d bytes, want 4", n)
	}
	if string(dst[:n]) != "DEFG" {
		t.Errorf("got %q, want %q", string(dst[:n]), "DEFG")
	}
}

// 1.3 Partial overlap left: request starts before block, overlaps left portion.
func TestCopyTo_PartialOverlapLeft_MissBeforeBlock(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	rc.Put("f1", 100, []byte("HELLO")) // block at [100, 105)

	// Request from offset 98 — starts before the block, so this is a miss.
	// CopyTo only matches blocks where off >= block.start && off < block.end.
	dst := make([]byte, 10)
	n, ok := rc.CopyTo("f1", 98, dst)
	if ok {
		t.Errorf("expected miss for offset before block start, got hit n=%d data=%q", n, string(dst[:n]))
	}
}

// 1.4 Partial overlap right: request extends beyond block end, gets partial data.
func TestCopyTo_PartialOverlapRight_ClippedAtBlockEnd(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	rc.Put("f1", 0, []byte("HELLO")) // 5 bytes at offset 0

	// Request 10 bytes from offset 0 — block only has 5 bytes
	dst := make([]byte, 10)
	n, ok := rc.CopyTo("f1", 0, dst)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if n != 5 {
		t.Errorf("got %d bytes, want 5 (block size)", n)
	}
	if string(dst[:n]) != "HELLO" {
		t.Errorf("got %q, want %q", string(dst[:n]), "HELLO")
	}
}

// 1.5 No overlap: request at offset completely outside any block → miss.
func TestCopyTo_NoOverlap_DistantOffset(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	rc.Put("f1", 0, []byte("data")) // block at [0, 4)

	dst := make([]byte, 10)
	n, ok := rc.CopyTo("f1", 1000, dst)
	if ok {
		t.Errorf("expected miss for offset far from block, got hit n=%d", n)
	}
	if n != 0 {
		t.Errorf("expected 0 bytes on miss, got %d", n)
	}
}

// 1.6 Adjacent ranges: two blocks at consecutive offsets, read each independently.
func TestCopyTo_AdjacentRanges(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	rc.Put("f1", 0, []byte("AAAA"))
	rc.Put("f1", 4, []byte("BBBB"))

	dst1 := make([]byte, 4)
	n1, ok1 := rc.CopyTo("f1", 0, dst1)
	if !ok1 || n1 != 4 || string(dst1) != "AAAA" {
		t.Errorf("first block: ok=%v n=%d data=%q", ok1, n1, string(dst1))
	}

	dst2 := make([]byte, 4)
	n2, ok2 := rc.CopyTo("f1", 4, dst2)
	if !ok2 || n2 != 4 || string(dst2) != "BBBB" {
		t.Errorf("second block: ok=%v n=%d data=%q", ok2, n2, string(dst2))
	}
}

// 1.7 Request clipped by EOF: block at end of file, request past block end
// returns available bytes only.
func TestCopyTo_ClippedByEOF(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	rc.Put("f1", 0, []byte("ABC")) // 3 bytes at offset 0

	// Request 10 bytes from offset 1 — block only has 2 bytes from offset 1
	dst := make([]byte, 10)
	n, ok := rc.CopyTo("f1", 1, dst)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if n != 2 {
		t.Errorf("got %d bytes, want 2 (remaining in block from offset 1)", n)
	}
	if string(dst[:n]) != "BC" {
		t.Errorf("got %q, want %q", string(dst[:n]), "BC")
	}
}

// 1.8 Request exactly at EOF: offset equals block end → miss.
func TestCopyTo_RequestAtBlockEnd(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	rc.Put("f1", 0, []byte("ABC")) // block covers [0, 3)

	// Offset 3 is exactly at the end of the block — should be a miss
	dst := make([]byte, 10)
	n, ok := rc.CopyTo("f1", 3, dst)
	if ok {
		t.Errorf("expected miss at block end offset, got hit n=%d data=%q", n, string(dst[:n]))
	}
}

// 1.9 Zero-length range: empty destination buffer → returns 0, false.
func TestCopyTo_ZeroLengthRange(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	rc.Put("f1", 0, []byte("ABC"))

	n, ok := rc.CopyTo("f1", 0, []byte{})
	if ok {
		t.Errorf("expected miss for empty buffer, got hit n=%d", n)
	}
	if n != 0 {
		t.Errorf("expected 0 bytes, got %d", n)
	}
}

// 1.10 Invalid range: negative offset → miss (verify no panic).
func TestCopyTo_NegativeOffset_NoPanic(t *testing.T) {
	rc := NewRangeCache(1 << 20, nil)
	rc.Put("f1", 0, []byte("ABC"))

	dst := make([]byte, 10)
	n, ok := rc.CopyTo("f1", -1, dst)
	if ok {
		t.Errorf("expected miss for negative offset, got hit n=%d", n)
	}
	if n != 0 {
		t.Errorf("expected 0 bytes, got %d", n)
	}
}

// --- CachedPrefixLen ---

func TestCachedPrefixLen_Hit(t *testing.T) {
	rc := NewRangeCache(1<<20, nil)
	data := []byte("ABCDEFGHIJ") // 10 bytes at offset 0
	rc.Put("f1", 0, data)

	length, ok := rc.CachedPrefixLen("f1", 0)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if length != 10 {
		t.Errorf("got length %d, want 10", length)
	}
}

func TestCachedPrefixLen_Miss(t *testing.T) {
	rc := NewRangeCache(1<<20, nil)

	length, ok := rc.CachedPrefixLen("f1", 0)
	if ok {
		t.Error("expected cache miss")
	}
	if length != 0 {
		t.Errorf("got length %d on miss, want 0", length)
	}
}

func TestCachedPrefixLen_MissAtDifferentOffset(t *testing.T) {
	rc := NewRangeCache(1<<20, nil)
	rc.Put("f1", 0, []byte("ABCDEFGHIJ"))

	// Look for a block starting at offset 5 — block starts at 0, not 5
	length, ok := rc.CachedPrefixLen("f1", 5)
	if ok {
		t.Error("expected miss for offset that doesn't match block start")
	}
	if length != 0 {
		t.Errorf("got length %d, want 0", length)
	}
}

func TestCachedPrefixLen_DifferentFileKey(t *testing.T) {
	rc := NewRangeCache(1<<20, nil)
	rc.Put("f1", 0, []byte("ABCDEFGHIJ"))

	// Look for a block in file f2 at offset 0 — no such block
	_, ok := rc.CachedPrefixLen("f2", 0)
	if ok {
		t.Error("expected miss for different file key")
	}
}

func TestCachedPrefixLen_LargeOffset(t *testing.T) {
	rc := NewRangeCache(16<<20, nil) // 16 MiB budget to fit 4 MiB block
	data := make([]byte, 4*1024*1024) // 4 MiB block at 16 MiB offset
	rc.Put("f1", 16*1024*1024, data)

	length, ok := rc.CachedPrefixLen("f1", 16*1024*1024)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if length != int64(len(data)) {
		t.Errorf("got length %d, want %d", length, len(data))
	}
}

func TestCachedPrefixLen_AfterOverwrite(t *testing.T) {
	rc := NewRangeCache(1<<20, nil)
	rc.Put("f1", 0, []byte("old_data")) // 8 bytes

	length, ok := rc.CachedPrefixLen("f1", 0)
	if !ok || length != 8 {
		t.Errorf("before overwrite: ok=%v length=%d, want ok=true length=8", ok, length)
	}

	rc.Put("f1", 0, []byte("new_longer_data")) // 15 bytes

	length, ok = rc.CachedPrefixLen("f1", 0)
	if !ok {
		t.Fatal("expected cache hit after overwrite")
	}
	if length != 15 {
		t.Errorf("after overwrite: got length %d, want 15", length)
	}
}

// ============================================================
// Phase 2: Cache lookup/store gaps (spec §2)
// ============================================================

// 2.1 Full coverage lookup by non-exact start: verify that
// CopyTo(fileKey, offset_within_block, dst) hits the block stored at aligned
// start, including prefetch-window key alignment (offset 17 MiB hitting block
// starting at 16 MiB).
func TestCopyTo_PrefetchWindowKeyAlignment(t *testing.T) {
	rc := NewRangeCache(10 << 20, nil) // 10 MiB budget

	// Store a 4 MiB block at offset 16 MiB (aligned to prefetchWindowSize)
	blockSize := 4 * 1024 * 1024
	data := make([]byte, blockSize)
	for i := range data {
		data[i] = byte((i + 0xAB) & 0xFF)
	}
	rc.Put("f1", 16*1024*1024, data)

	// Read from offset 17 MiB — within the block that starts at 16 MiB
	off := int64(17 * 1024 * 1024)
	dst := make([]byte, 1024)
	n, ok := rc.CopyTo("f1", off, dst)
	if !ok {
		t.Fatal("expected cache hit for offset within block at aligned start")
	}
	if n != len(dst) {
		t.Errorf("got %d bytes, want %d", n, len(dst))
	}
	// Verify data correctness: offset 17 MiB is 1 MiB into the block
	expectedOff := off - 16*1024*1024
	for i := 0; i < n; i++ {
		if dst[i] != data[expectedOff+int64(i)] {
			t.Errorf("byte mismatch at index %d: got %d, want %d", i, dst[i], data[expectedOff+int64(i)])
			break
		}
	}
}

// 2.2 Overlapping block replacement: Put block at (fileKey, 0) with "old",
// then Put at (fileKey, 0) with "new". Verify other blocks are still intact.
func TestPut_OverwritePreservesOtherBlocks(t *testing.T) {
	rc := NewRangeCache(4096, nil)
	rc.Put("f1", 0, []byte("old_data_at_0"))
	rc.Put("f1", 100, []byte("neighbor_at_100"))

	// Overwrite the block at offset 0
	rc.Put("f1", 0, []byte("new_0"))

	// Verify overwritten block has new data
	dst := make([]byte, 20)
	n, ok := rc.CopyTo("f1", 0, dst)
	if !ok || string(dst[:n]) != "new_0" {
		t.Errorf("overwritten block: ok=%v data=%q, want %q", ok, string(dst[:n]), "new_0")
	}

	// Verify neighbor block is still intact
	dst2 := make([]byte, 20)
	n2, ok2 := rc.CopyTo("f1", 100, dst2)
	if !ok2 {
		t.Error("neighbor block should still be cached")
	}
	if string(dst2[:n2]) != "neighbor_at_100" {
		t.Errorf("neighbor block: got %q, want %q", string(dst2[:n2]), "neighbor_at_100")
	}
}

// ============================================================
// Metrics integration tests
// ============================================================

func TestMetrics_PutBlock_IncrementsCounters(t *testing.T) {
	m := metrics.New()
	rc := NewRangeCache(4096, m)

	rc.Put("file1", 0, []byte("1234")) // 4 bytes

	if m.CacheBytesTotal.Load() != 4 {
		t.Errorf("CacheBytesTotal: got %d, want 4", m.CacheBytesTotal.Load())
	}
	if m.CacheBytesActive.Load() != 4 {
		t.Errorf("CacheBytesActive: got %d, want 4", m.CacheBytesActive.Load())
	}
	if m.CacheEntries.Load() != 1 {
		t.Errorf("CacheEntries: got %d, want 1", m.CacheEntries.Load())
	}
}

func TestMetrics_PutBlock_MultipleBlocks(t *testing.T) {
	m := metrics.New()
	rc := NewRangeCache(4096, m)

	rc.Put("file1", 0, []byte("1234")) // 4 bytes
	rc.Put("file1", 4, []byte("5678")) // 4 bytes, total 8

	if m.CacheBytesTotal.Load() != 8 {
		t.Errorf("CacheBytesTotal: got %d, want 8", m.CacheBytesTotal.Load())
	}
	if m.CacheBytesActive.Load() != 8 {
		t.Errorf("CacheBytesActive: got %d, want 8", m.CacheBytesActive.Load())
	}
	if m.CacheEntries.Load() != 2 {
		t.Errorf("CacheEntries: got %d, want 2", m.CacheEntries.Load())
	}
}

func TestMetrics_PutBlock_OverwriteAdjustsMetrics(t *testing.T) {
	m := metrics.New()
	rc := NewRangeCache(4096, m)

	rc.Put("file1", 0, []byte("1234")) // 4 bytes
	rc.Put("file1", 0, []byte("12345678")) // 8 bytes, replaces old 4-byte block

	// CacheBytesTotal adjusted on replacement: 0 + 4 - 4 + 8 = 8
	if m.CacheBytesTotal.Load() != 8 {
		t.Errorf("CacheBytesTotal: got %d, want 8", m.CacheBytesTotal.Load())
	}
	// CacheBytesActive tracks current live bytes: 8 (old 4 removed, new 8 added)
	if m.CacheBytesActive.Load() != 8 {
		t.Errorf("CacheBytesActive: got %d, want 8", m.CacheBytesActive.Load())
	}
	// CacheEntries: still 1 (overwrite, not a new entry)
	if m.CacheEntries.Load() != 1 {
		t.Errorf("CacheEntries: got %d, want 1", m.CacheEntries.Load())
	}
}

func TestMetrics_EvictOne_DecrementsCounters(t *testing.T) {
	m := metrics.New()
	// Budget of 5 bytes — adding 6 bytes triggers eviction
	rc := NewRangeCache(5, m)

	rc.Put("file1", 0, []byte("12345")) // 5 bytes, at budget
	rc.Put("file2", 0, []byte("6"))     // 1 byte, triggers eviction of oldest

	// After eviction: file1@0 evicted (5 bytes removed), file2@0 remains (1 byte)
	if m.CacheBytesActive.Load() != 1 {
		t.Errorf("CacheBytesActive: got %d, want 1", m.CacheBytesActive.Load())
	}
	if m.CacheEntries.Load() != 1 {
		t.Errorf("CacheEntries: got %d, want 1", m.CacheEntries.Load())
	}
}

func TestMetrics_EvictStale_DecrementsCounters(t *testing.T) {
	m := metrics.New()
	rc := NewRangeCache(4096, m)

	rc.PutWithSession("file1", 0, []byte("1234"), 1) // 4 bytes, session 1
	rc.PutWithSession("file1", 4, []byte("5678"), 5) // 4 bytes, session 5

	rc.EvictStale("file1", 5) // evicts session 1 block (4 bytes)

	if m.CacheBytesStale.Load() != 4 {
		t.Errorf("CacheBytesStale: got %d, want 4", m.CacheBytesStale.Load())
	}
	if m.CacheBytesActive.Load() != 4 {
		t.Errorf("CacheBytesActive: got %d, want 4", m.CacheBytesActive.Load())
	}
	if m.CacheEntries.Load() != 1 {
		t.Errorf("CacheEntries: got %d, want 1", m.CacheEntries.Load())
	}
}

func TestMetrics_NilMetrics_NoPanic(t *testing.T) {
	rc := NewRangeCache(4096, nil)

	// All operations should work without panicking
	rc.Put("file1", 0, []byte("1234"))
	rc.PutWithSession("file2", 0, []byte("5678"), 1)
	rc.EvictStale("file2", 2)

	dst := make([]byte, 4)
	rc.CopyTo("file1", 0, dst)
}