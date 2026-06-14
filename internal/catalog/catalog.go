// Package catalog provides the Catalog orchestrator that fetches downloads
// from the TorBox API, classifies files, builds the virtual filesystem tree,
// assigns stable inodes, and swaps the tree atomically for FUSE consumption.
package catalog

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"github.com/iiieii/torbox-fuse-go/internal/state"
)

// Downloader is the interface used by Catalog to fetch downloads from the
// TorBox API. It is satisfied by *torbox.Client; the indirection avoids an
// import cycle between the catalog and torbox packages.
type Downloader interface {
	ListDownloads(ctx context.Context, kind DownloadKind) ([]Download, error)
}

// Catalog orchestrates periodic refresh of the virtual filesystem tree
// from the TorBox API. It holds the current tree in an atomic.Value
// so that the FUSE layer can read it without locking.
type Catalog struct {
	client  Downloader
	stateDB *state.DB
	metrics *metrics.Metrics

	allDirEnabled bool // whether to include the /all flat directory

	tree       atomic.Value // stores *Tree
	refreshing atomic.Bool

	onRefresh func() // called after a successful tree swap (used by FUSE to sync)
}

// NewCatalogFromTree creates a Catalog pre-loaded with the given tree.
// This is intended for tests that need a Catalog without a real API client.
func NewCatalogFromTree(tree *Tree) *Catalog {
	c := &Catalog{}
	c.tree.Store(tree)
	return c
}

// NewCatalog creates a Catalog with the given dependencies. The initial tree
// is empty; call Refresh to populate it. The allDirEnabled flag controls whether
// the /all directory is included in the virtual filesystem tree.
func NewCatalog(client Downloader, stateDB *state.DB, m *metrics.Metrics, allDirEnabled bool) *Catalog {
	c := &Catalog{
		client:        client,
		stateDB:       stateDB,
		metrics:       m,
		allDirEnabled: allDirEnabled,
	}
	// Initialise with an empty tree so Tree() never returns nil.
	c.tree.Store(&Tree{dirs: make(map[string][]DirEntry)})
	return c
}

// Tree returns the current filesystem tree. Safe to call from any goroutine.
func (c *Catalog) Tree() *Tree {
	return c.tree.Load().(*Tree)
}

// SetOnRefresh registers a callback that is called after a successful catalog
// refresh (tree swap). The FUSE layer uses this to synchronise the kernel's
// directory tree with the new catalog. The callback is called without arguments;
// the callee should call Tree() to get the latest tree.
func (c *Catalog) SetOnRefresh(fn func()) {
	c.onRefresh = fn
}

// Refresh fetches all downloads from the TorBox API (torrents, usenet, webdl),
// builds the virtual filesystem tree, assigns stable inodes, upserts file
// records into the state database, and atomically swaps the tree.
//
// Returns ErrRefreshInProgress if a refresh is already running.
func (c *Catalog) Refresh(ctx context.Context) error {
	if !c.refreshing.CompareAndSwap(false, true) {
		return ErrRefreshInProgress
	}
	defer c.refreshing.Store(false)

	c.metrics.RefreshCount.Add(1)
	slog.Info("catalog refresh started")

	var allDownloads []Download
	kinds := []DownloadKind{KindTorrent, KindUsenet, KindWebDL}
	for _, kind := range kinds {
		downloads, err := c.client.ListDownloads(ctx, kind)
		if err != nil {
			slog.Error("fetch downloads", "kind", kind, "err", err)
			return fmt.Errorf("upsert file records: %w", err)
		}
		c.metrics.APICallCount.Add(1)
		slog.Info("fetched downloads", "kind", kind, "count", len(downloads))
		allDownloads = append(allDownloads, downloads...)
	}

	slog.Info("total downloads fetched", "count", len(allDownloads))

	// Build the virtual filesystem tree.
	tree := BuildTree(allDownloads, c.allDirEnabled)

	// Walk all files in the tree and assign inodes + upsert file records.
	var files []state.FileRecord
	for dirPath, entries := range tree.dirs {
		for _, entry := range entries {
			if entry.File == nil {
				continue
			}
			f := entry.File
			contentKey := f.ContentKey()
			filePath := dirPath + "/" + entry.Name

			_, err := c.stateDB.AssignInode(contentKey, filePath)
			if err != nil {
				slog.Error("assign inode", "key", contentKey, "path", filePath, "err", err)
				continue
			}

			files = append(files, state.FileRecord{
				ContentKey:   contentKey,
				DownloadKind: string(f.DownloadKind),
				DownloadID:   f.DownloadID,
				FileID:       f.FileID,
				Path:         filePath,
				Size:         f.Size,
			})
		}
	}

	if err := c.stateDB.UpsertFiles(files); err != nil {
		slog.Error("upsert file records", "err", err)
		return fmt.Errorf("upsert file records: %w", err)
	}

	// Swap tree atomically.
	c.tree.Store(tree)

	// Notify FUSE layer to sync the in-memory tree with the new catalog.
	if c.onRefresh != nil {
		c.onRefresh()
	}

	// Update metrics.
	c.metrics.CatalogItems.Store(int64(len(files)))
	slog.Info("catalog refresh complete", "files", len(files))

	return nil
}

// LoadFromDB loads the catalog tree from the state database, bypassing the
// TorBox API entirely. This enables fast startup: the FUSE mount can show files
// immediately from cached data while the API refresh runs in the background.
// Returns nil if the database contains no file records (first run).
func (c *Catalog) LoadFromDB(ctx context.Context) error {
	records, err := c.stateDB.ListFiles()
	if err != nil {
		return fmt.Errorf("load from db: %w", err)
	}
	if len(records) == 0 {
		slog.Info("no cached files in db, skipping db load")
		return nil
	}

	tree := BuildTreeFromDB(records, c.allDirEnabled)
	c.tree.Store(tree)

	if c.onRefresh != nil {
		c.onRefresh()
	}

	c.metrics.CatalogItems.Store(int64(len(records)))
	slog.Info("loaded catalog from db cache", "files", len(records))
	return nil
}

// SetTree replaces the catalog tree atomically and triggers the onRefresh
// callback. This is used in tests to simulate a catalog refresh without
// needing a real API server, and by the webhook refresh handler when the
// catalog is rebuilt from cached data.
func (c *Catalog) SetTree(tree *Tree) {
	c.tree.Store(tree)
	if c.onRefresh != nil {
		c.onRefresh()
	}
}

// ErrRefreshInProgress is returned when Refresh is called while a refresh
// is already running.
var ErrRefreshInProgress = refreshInProgressError{}

type refreshInProgressError struct{}

func (refreshInProgressError) Error() string { return "refresh already in progress" }
