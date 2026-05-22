// Package cache provides a sharded range cache for zero-alloc cache-hit reads
// with budget eviction and session-aware stale eviction.
package cache

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

const numShards = 32

// RangeCache is a sharded cache for byte-range blocks keyed by file and offset.
// Each shard is protected by its own RWMutex so concurrent reads to different
// shards never block each other. Budget enforcement evicts the least-recently-
// accessed blocks when total usage exceeds the configured byte budget.
type RangeCache struct {
	shards [numShards]rangeShard
	budget int64
	used   atomic.Int64
}

// rangeShard holds a partition of the cache protected by a single lock.
type rangeShard struct {
	mu     sync.RWMutex
	blocks map[cacheKey]*RangeBlock
}

// cacheKey identifies a unique block within a shard.
type cacheKey struct {
	fileKey string
	start   int64
}

// RangeBlock holds a contiguous byte range for a file.
type RangeBlock struct {
	start      int64
	end        int64
	data       []byte
	sessionID  int64
	lastAccess atomic.Int64
}

// NewRangeCache creates a RangeCache with the given byte budget.
// When total cached data exceeds budget, oldest-accessed blocks are evicted.
func NewRangeCache(budgetBytes int64) *RangeCache {
	rc := &RangeCache{budget: budgetBytes}
	for i := range rc.shards {
		rc.shards[i].blocks = make(map[cacheKey]*RangeBlock)
	}
	return rc
}

// shardIndex returns the shard index for the given fileKey and start offset.
// Uses FNV-32 hash of fileKey XOR'd with start, mod numShards.
func shardIndex(fileKey string, start int64) uint32 {
	h := fnv.New32()
	h.Write([]byte(fileKey))
	return (h.Sum32() ^ uint32(start)) % numShards
}

// CopyTo copies cached bytes for (fileKey, off) directly into dst.
// It searches for a block whose [start, end) range contains off and copies
// the overlapping portion into dst. Returns the number of bytes copied and
// true on hit; (0, false) on miss.
// This is the zero-alloc hot path: the read lock permits concurrent access
// and bytes are copied directly into the caller-supplied buffer.
func (rc *RangeCache) CopyTo(fileKey string, off int64, dst []byte) (int, bool) {
	if len(dst) == 0 {
		return 0, false
	}

	// Search all shards for a block that contains the requested offset.
	// Different offsets for the same fileKey may land in different shards,
	// so we must check each shard.
	for i := 0; i < numShards; i++ {
		sh := &rc.shards[i]
		sh.mu.RLock()
		for key, blk := range sh.blocks {
			if key.fileKey == fileKey && off >= blk.start && off < blk.end {
				blkOff := off - blk.start
				avail := blk.data[blkOff:]
				n := copy(dst, avail)
				blk.lastAccess.Store(nowNano())
				sh.mu.RUnlock()
				return n, true
			}
		}
		sh.mu.RUnlock()
	}
	return 0, false
}

// Put inserts a block at (fileKey, start). The data slice is copied so the
// caller may reuse their buffer. Empty data is silently ignored.
// If the total cached size exceeds the budget after insertion, the
// oldest-accessed blocks are evicted until under budget.
func (rc *RangeCache) Put(fileKey string, start int64, data []byte) {
	rc.putBlock(fileKey, start, data, 0)
}

// PutWithSession is like Put but tags the block with a session ID for later
// stale eviction via EvictStale.
func (rc *RangeCache) PutWithSession(fileKey string, start int64, data []byte, sessionID int64) {
	rc.putBlock(fileKey, start, data, sessionID)
}

func (rc *RangeCache) putBlock(fileKey string, start int64, data []byte, sessionID int64) {
	if len(data) == 0 {
		return
	}

	buf := make([]byte, len(data))
	copy(buf, data)

	blk := &RangeBlock{
		start:     start,
		end:       start + int64(len(data)),
		data:      buf,
		sessionID: sessionID,
	}
	blk.lastAccess.Store(nowNano())

	si := shardIndex(fileKey, start)
	sh := &rc.shards[si]

	sh.mu.Lock()
	key := cacheKey{fileKey: fileKey, start: start}
	// If a block already exists at this key, subtract its size.
	if old, ok := sh.blocks[key]; ok {
		rc.used.Add(-int64(len(old.data)))
	}
	sh.blocks[key] = blk
	sh.mu.Unlock()

	rc.used.Add(int64(len(buf)))
	rc.evict()
}

// EvictStale removes all blocks for fileKey whose sessionID is positive and
// strictly less than currentSession. This lets callers invalidate data from
// previous download sessions without touching sessionless or current-session
// blocks.
func (rc *RangeCache) EvictStale(fileKey string, currentSession int64) {
	// We need to check every shard since different offsets for the same
	// fileKey may land in different shards.
	for i := range rc.shards {
		sh := &rc.shards[i]
		sh.mu.Lock()
		for key, blk := range sh.blocks {
			if key.fileKey == fileKey && blk.sessionID > 0 && blk.sessionID < currentSession {
				rc.used.Add(-int64(len(blk.data)))
				delete(sh.blocks, key)
			}
		}
		sh.mu.Unlock()
	}
}

// evict removes the oldest-accessed blocks across all shards until the total
// cached size is at or below the budget.
func (rc *RangeCache) evict() {
	for rc.used.Load() > rc.budget {
		rc.evictOne()
	}
}

// evictOne finds and removes the single block with the oldest lastAccess
// across all shards.
func (rc *RangeCache) evictOne() {
	var oldestKey cacheKey
	var oldestShard uint32
	var oldestTime int64 = -1

	// First pass: find the oldest block (read locks only).
	for i := range rc.shards {
		sh := &rc.shards[i]
		sh.mu.RLock()
		for key, blk := range sh.blocks {
			la := blk.lastAccess.Load()
			if oldestTime == -1 || la < oldestTime {
				oldestTime = la
				oldestKey = key
				oldestShard = uint32(i)
			}
		}
		sh.mu.RUnlock()
	}

	if oldestTime == -1 {
		return // nothing to evict
	}

	// Second pass: delete the oldest block (write lock on its shard).
	sh := &rc.shards[oldestShard]
	sh.mu.Lock()
	// Re-verify the block is still present (a concurrent writer may have
	// replaced or removed it).
	if blk, ok := sh.blocks[oldestKey]; ok {
		rc.used.Add(-int64(len(blk.data)))
		delete(sh.blocks, oldestKey)
	}
	sh.mu.Unlock()
}

// Used returns the current total bytes stored in the cache.
func (rc *RangeCache) Used() int64 {
	return rc.used.Load()
}

// nowNano returns a nanosecond timestamp for LRU comparisons.
// It is a variable so tests can override it to control time progression.
var nowNano = func() int64 {
	return time.Now().UnixNano()
}