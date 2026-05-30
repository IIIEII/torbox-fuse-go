//go:build !short

package stream

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
)

// ============================================================
// Test 11: Budget holding never exceeds global limit
// ============================================================
// Verifies that budget.holding() (which tracks windows and smallReads holding
// budget slots) never exceeds maxGlobalWindows. After the fix, both window
// fetches and smallReads acquire a budget slot before making CDN requests.

func TestStreamReader_InflightWindowsRespectsBudget(t *testing.T) {
	const maxGlobalWindows = 4
	const fileCount = 20
	const fileSize int64 = 4 * 1024 * 1024 // 4 MiB = 1 window each
	const cdnLatency = 200 * time.Millisecond

	files := makeStressFiles(fileCount, fileSize)
	env := newStressEnv(t, &stressConfig{
		maxGlobalWindows: maxGlobalWindows,
		cdnLatency:       cdnLatency,
		cacheBudgetMB:    4, // small cache to force re-fetches
	}, files)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Poll budget holding count to find peak. This tracks actual concurrent
	// budget holders (windows that have acquired a slot), not just registered
	// inflight windows.
	var peakHolding atomic.Int32
	stopPoll := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopPoll:
				return
			default:
			}
			count := env.sr.budget.holding()
			for {
				peak := peakHolding.Load()
				if count <= peak {
					break
				}
				if peakHolding.CompareAndSwap(peak, count) {
					break
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Launch all scan goroutines simultaneously.
	var wg sync.WaitGroup
	results := make(chan error, fileCount)
	for i := 0; i < fileCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			f := env.files[idx]
			buf := make([]byte, 4096)
			if _, err := env.sr.ReadAt(ctx, f.key, 0, buf, f.size); err != nil && err != io.EOF {
				results <- fmt.Errorf("file %d: %w", idx, err)
				return
			}
			results <- nil
		}(i)
	}

	wg.Wait()
	close(results)
	close(stopPoll)

	for res := range results {
		if res != nil {
			t.Error(res)
		}
	}

	peak := peakHolding.Load()
	t.Logf("peak budget holding: %d (limit: %d)", peak, maxGlobalWindows)

	if peak > int32(maxGlobalWindows) {
		t.Errorf("budget holding exceeded global limit: peak %d, limit is %d. "+
			"Windows or smallReads are bypassing budget.acquire().",
			peak, maxGlobalWindows)
	}
}

// ============================================================
// Test 12: smallRead respects global budget
// ============================================================
// Verifies that smallRead (direct CDN fetch for metadata-sized reads)
// acquires a budget slot before making CDN requests. Previously, smallRead
// called FetchRange directly without budget.acquire(), allowing metadata
// scans to exceed the global inflight window budget.

func TestStreamReader_SmallReadRespectsBudget(t *testing.T) {
	const maxGlobalWindows = 4
	const scanFileCount = 20
	const fileSize int64 = 4 * 1024 * 1024

	files := makeStressFiles(scanFileCount, fileSize)
	env := newStressEnv(t, &stressConfig{
		maxGlobalWindows: maxGlobalWindows,
		cdnLatency:       100 * time.Millisecond,
		cacheBudgetMB:    4,
	}, files)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Poll budget holding count to find peak.
	var peakHolding atomic.Int32
	stopPoll := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopPoll:
				return
			default:
			}
			count := env.sr.budget.holding()
			for {
				peak := peakHolding.Load()
				if count <= peak {
					break
				}
				if peakHolding.CompareAndSwap(peak, count) {
					break
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Launch metadata scan: each file gets 3 small random reads (4 KiB each).
	// These go through the smallRead path (random access, ≤ 256 KiB).
	var wg sync.WaitGroup
	results := make(chan error, scanFileCount*3)
	for i := 0; i < scanFileCount; i++ {
		f := env.files[i]
		for _, off := range []int64{0, f.size - 1024, f.size / 3} {
			wg.Add(1)
			go func(idx int, offset int64) {
				defer wg.Done()
				buf := make([]byte, 4096)
				if offset+int64(len(buf)) > env.files[idx].size {
					buf = buf[:env.files[idx].size-offset]
				}
				if _, err := env.sr.ReadAt(ctx, env.files[idx].key, offset, buf, env.files[idx].size); err != nil && err != io.EOF {
					results <- fmt.Errorf("scan file %d off %d: %w", idx, offset, err)
					return
				}
				results <- nil
			}(i, off)
		}
	}

	wg.Wait()
	close(results)
	close(stopPoll)

	for res := range results {
		if res != nil {
			t.Error(res)
		}
	}

	peak := peakHolding.Load()
	t.Logf("peak budget holding during metadata scan: %d (limit: %d)", peak, maxGlobalWindows)

	if peak > int32(maxGlobalWindows) {
		t.Errorf("smallRead bypasses budget: peak %d concurrent holders, limit is %d. "+
			"smallRead must acquire a budget slot before FetchRange.",
			peak, maxGlobalWindows)
	}
}

// ============================================================
// Test 13: Read-ahead inherits file priority, not always PriorityHigh
// ============================================================
// Production bug: maybeReadAhead() used to pass PriorityHigh to
// getOrCreateWindow even when the triggering file was a scan file.
// After the fix, read-ahead inherits the same budget priority as the
// triggering read.
//
// This test verifies that read-ahead for a file with small sequential reads
// (below activeReadThreshold) gets PriorityLow, not PriorityHigh.

func TestStreamReader_ReadAheadInheritsFilePriority(t *testing.T) {
	const maxGlobalWindows = 4
	const fileSize int64 = 16 * 1024 * 1024 // 16 MiB = 4 windows
	const cdnLatency = 50 * time.Millisecond

	// One file with deterministic data.
	data := make([]byte, fileSize)
	for j := range data {
		data[j] = byte(j % 256)
	}
	files := []stressFile{{key: "scan_file", data: data, size: fileSize}}

	env := newStressEnv(t, &stressConfig{
		maxGlobalWindows: maxGlobalWindows,
		cdnLatency:       cdnLatency,
		cacheBudgetMB:    64,
	}, files)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Track budget acquisitions by priority.
	var lowAcquires atomic.Int32
	var highAcquires atomic.Int32

	env.sr.budget = &priorityTrackingBudget{
		inner:        newBudgetSem(maxGlobalWindows),
		lowAcquires:  &lowAcquires,
		highAcquires: &highAcquires,
	}

	// Read the file sequentially using small reads (4 KiB).
	// These are below budgetPriorityThreshold (16 KiB), so they get PriorityLow.
	// After sequentialReadsRequired=1 sequential reads, read-ahead kicks in.
	// The read-ahead window should also inherit PriorityLow.
	//
	// We do only 3 reads (below activeReadThreshold=4) so the file stays in
	// "scan" territory — it doesn't qualify as active playback.
	buf := make([]byte, 4*1024) // 4 KiB reads — below budgetPriorityThreshold
	var totalRead int64
	for off := int64(0); off < fileSize && totalRead < 3*4*1024; {
		n, err := env.sr.ReadAt(ctx, "scan_file", off, buf, fileSize)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt at offset %d: %v", off, err)
		}
		if n == 0 {
			break
		}
		totalRead += int64(n)
		off += int64(n)
	}

	// Wait for all in-flight windows to complete.
	time.Sleep(500 * time.Millisecond)

	lowCount := lowAcquires.Load()
	highCount := highAcquires.Load()
	t.Logf("budget acquires: low=%d, high=%d", lowCount, highCount)

	// With 4 KiB reads and only 3 reads (below activeReadThreshold=4),
	// all budget acquires should be PriorityLow. The file doesn't qualify
	// as active playback, and read-ahead should inherit the file's PriorityLow.
	if highCount > 0 {
		t.Errorf("read-ahead should inherit file priority (PriorityLow for small scan reads), "+
			"not always use PriorityHigh. Got %d high-priority budget acquires (expected 0). "+
			"maybeReadAhead() may be hardcoding PriorityHigh instead of inheriting the file's priority.",
			highCount)
	}
}

// ============================================================
// Test 14: Playback throughput sustained during scan with multiple CDN hosts
// ============================================================
// Production has 8+ CDN hosts, each with its own semaphore (maxConnsPerHost=8).
// With 8 hosts × 8 connections = 64 parallel CDN requests, a library scan
// can saturate all bandwidth. Playback must maintain throughput despite this.
//
// This test uses multiple CDN hosts (simulated via redirect mode) to verify
// that playback doesn't starve when scan traffic spreads across hosts.

func TestStreamReader_PlaybackSustainedDuringScan_MultiHost(t *testing.T) {
	const maxGlobalWindows = 16 // production value
	const maxConnsPerHost = 8    // production value
	const scanFileCount = 30
	const fileSize int64 = 4 * 1024 * 1024 // 4 MiB each
	const playbackSize int64 = 16 * 1024 * 1024 // 16 MiB playback file

	scanFiles := makeStressFiles(scanFileCount, fileSize)

	// Create a larger playback file with deterministic data.
	playbackData := make([]byte, playbackSize)
	for j := range playbackData {
		playbackData[j] = byte(j % 256)
	}
	allFiles := append(scanFiles, stressFile{key: "playback", data: playbackData, size: playbackSize})

	env := newStressEnv(t, &stressConfig{
		maxGlobalWindows: maxGlobalWindows,
		concurrency:      maxConnsPerHost,
		cdnLatency:       50 * time.Millisecond,
		cacheBudgetMB:    64,
		redirectCDN:      true, // simulate multiple CDN hosts
	}, allFiles)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start playback in one goroutine.
	playbackDone := make(chan struct{})
	go func() {
		defer close(playbackDone)
		buf := make([]byte, 128*1024)
		var total int64
		for off := int64(0); off < playbackSize; {
			n, err := env.sr.ReadAt(ctx, "playback", off, buf, playbackSize)
			if err != nil && err != io.EOF {
				return
			}
			if n == 0 {
				break
			}
			total += int64(n)
			off += int64(n)
		}
	}()

	// Start library scan in parallel — small random reads on many files.
	var scanWg sync.WaitGroup
	for i := 0; i < scanFileCount; i++ {
		scanWg.Add(1)
		go func(idx int) {
			defer scanWg.Done()
			f := env.files[idx]
			buf := make([]byte, 4096)
			// Read 3 random offsets per file (metadata scan pattern).
			for _, off := range []int64{0, f.size / 2, f.size - 1024} {
				env.sr.ReadAt(ctx, f.key, off, buf, f.size) //nolint:errcheck
			}
		}(i)
	}

	scanWg.Wait()

	// Wait for playback to finish with timeout.
	select {
	case <-playbackDone:
		// Playback completed — good.
	case <-time.After(10 * time.Second):
		t.Error("playback did not complete within 10s after scan finished — possible starvation")
	}
}

// priorityTrackingBudget wraps budgetSem and counts acquisitions by priority.
type priorityTrackingBudget struct {
	inner        *budgetSem
	lowAcquires  *atomic.Int32
	highAcquires *atomic.Int32
}

func (pt *priorityTrackingBudget) acquire(ctx context.Context, priority uint8) error {
	err := pt.inner.acquire(ctx, priority)
	if err != nil {
		return err
	}
	if priority == uint8(cache.PriorityHigh) {
		pt.highAcquires.Add(1)
	} else {
		pt.lowAcquires.Add(1)
	}
	return nil
}

func (pt *priorityTrackingBudget) holding() int32 {
	return pt.inner.holding()
}

func (pt *priorityTrackingBudget) release() {
	pt.inner.release()
}

func (pt *priorityTrackingBudget) budgetLimit() int {
	return pt.inner.budgetLimit()
}

