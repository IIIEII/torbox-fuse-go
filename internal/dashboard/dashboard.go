// Package dashboard provides a real-time web visualization of the FUSE streaming
// cache state — which files are downloading, which ranges are cached, where active
// readers are positioned, and recently closed files.
package dashboard

import (
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"github.com/iiieii/torbox-fuse-go/internal/state"
	"github.com/iiieii/torbox-fuse-go/internal/stream"
)

// DashboardSnapshot is the JSON-serializable state snapshot sent to the frontend.
type DashboardSnapshot struct {
	Timestamp      string               `json:"timestamp"`
	Summary        SummaryInfo          `json:"summary"`
	Active         []FileSnapshotJSON   `json:"active"`
	RecentlyClosed []ClosedFileInfoJSON `json:"recently_closed"`
}

// SummaryInfo holds global resource usage stats.
type SummaryInfo struct {
	BudgetSlotsUsed  int32 `json:"budget_slots_used"`
	BudgetSlotsTotal int   `json:"budget_slots_total"`
	CacheBytesUsed   int64 `json:"cache_bytes_used"`
	CacheBytesBudget int64 `json:"cache_bytes_budget"`
	InflightWindows  int64 `json:"inflight_windows"`
	CacheEntries     int64 `json:"cache_entries"`
}

// FileSnapshotJSON is the JSON representation of a file's streaming state.
type FileSnapshotJSON struct {
	FileKey      string                `json:"file_key"`
	FilePath     string                `json:"file_path"`
	FileSize     int64                 `json:"file_size"`
	Priority     uint8                 `json:"priority"`
	PriorityName string                `json:"priority_name"`
	Pattern      string                `json:"pattern"`
	CachedBlocks []cache.BlockInfo     `json:"cached_blocks"`
	Inflight     []stream.InflightInfo `json:"inflight"`
	ReadOffsets  []int64               `json:"read_offsets"`
}

// ClosedFileInfoJSON is the JSON representation of a recently closed file.
type ClosedFileInfoJSON struct {
	FileKey  string `json:"file_key"`
	FilePath string `json:"file_path"`
	FileSize int64  `json:"file_size"`
	ClosedAt string `json:"closed_at"`
}

// Dashboard collects snapshots from the streaming subsystem and resolves
// file keys to human-readable paths.
type Dashboard struct {
	streamer  *stream.StreamReader
	cache     *cache.RangeCache
	stateDB   *state.DB
	metrics   *metrics.Metrics
	pathCache sync.Map // fileKey -> resolved path (avoids repeated DB lookups)
}

// New creates a Dashboard with the given dependencies.
func New(streamer *stream.StreamReader, cache *cache.RangeCache, stateDB *state.DB, m *metrics.Metrics) *Dashboard {
	return &Dashboard{
		streamer: streamer,
		cache:    cache,
		stateDB:  stateDB,
		metrics:  m,
	}
}

// resolvePath resolves a file content key to a human-readable path.
// Uses a sync.Map cache to avoid repeated DB queries.
func (d *Dashboard) resolvePath(fileKey string) string {
	if v, ok := d.pathCache.Load(fileKey); ok {
		return v.(string)
	}
	if d.stateDB != nil {
		rec, err := d.stateDB.LookupFile(fileKey)
		if err != nil {
			slog.Debug("dashboard: lookup file path", "fileKey", fileKey, "err", err)
		}
		if rec != nil {
			d.pathCache.Store(fileKey, rec.Path)
			return rec.Path
		}
	}
	// Fallback: use the file key as-is.
	d.pathCache.Store(fileKey, fileKey)
	return fileKey
}

// Snapshot collects a complete dashboard state snapshot.
func (d *Dashboard) Snapshot() *DashboardSnapshot {
	activeSnapshots := d.streamer.SnapshotFiles()

	// Build recently closed file list first so we can deduplicate.
	closedFiles := d.streamer.RecentlyClosedFiles()
	closedKeys := make(map[string]struct{}, len(closedFiles))
	closedJSON := make([]ClosedFileInfoJSON, 0, len(closedFiles))
	for _, cf := range closedFiles {
		closedKeys[cf.FileKey] = struct{}{}
		closedJSON = append(closedJSON, ClosedFileInfoJSON{
			FileKey:  cf.FileKey,
			FilePath: d.resolvePath(cf.FileKey),
			FileSize: cf.FileSize,
			ClosedAt: cf.ClosedAt.Format(time.RFC3339),
		})
	}

	// Build JSON snapshots for active files, excluding any that are
	// already in the recently-closed list. A file that was just closed
	// may still have cached data (making it appear in SnapshotFiles),
	// but it should only show in the Recently Closed section.
	activeJSON := make([]FileSnapshotJSON, 0, len(activeSnapshots))
	for _, fs := range activeSnapshots {
		if _, inClosed := closedKeys[fs.FileKey]; inClosed {
			continue
		}
		activeJSON = append(activeJSON, d.fileSnapshotToJSON(fs))
	}

	// Sort active files by file path for stable ordering.
	sort.Slice(activeJSON, func(i, j int) bool {
		return activeJSON[i].FilePath < activeJSON[j].FilePath
	})

	summary := SummaryInfo{
		BudgetSlotsUsed:  d.streamer.BudgetHolding(),
		BudgetSlotsTotal: d.streamer.BudgetLimit(),
		CacheBytesUsed:   d.cache.Used(),
		CacheBytesBudget: d.cache.Budget(),
	}
	if d.metrics != nil {
		summary.InflightWindows = d.metrics.InflightWindows.Load()
		summary.CacheEntries = d.metrics.CacheEntries.Load()
	}

	return &DashboardSnapshot{
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		Summary:        summary,
		Active:         activeJSON,
		RecentlyClosed: closedJSON,
	}
}

// fileSnapshotToJSON converts a stream.FileSnapshot to a JSON-friendly struct.
func (d *Dashboard) fileSnapshotToJSON(fs stream.FileSnapshot) FileSnapshotJSON {
	priorityName := "low"
	if fs.Priority == cache.PriorityHigh {
		priorityName = "high"
	}
	return FileSnapshotJSON{
		FileKey:      fs.FileKey,
		FilePath:     d.resolvePath(fs.FileKey),
		FileSize:     fs.FileSize,
		Priority:     fs.Priority,
		PriorityName: priorityName,
		Pattern:      fs.Pattern,
		CachedBlocks: fs.CachedBlocks,
		Inflight:     fs.Inflight,
		ReadOffsets:  fs.ReadOffsets,
	}
}
