// Package catalog provides the Catalog orchestrator that fetches downloads
// from the TorBox API, classifies files, builds the virtual filesystem tree,
// assigns stable inodes, and swaps the tree atomically for FUSE consumption.
package catalog

import (
	"context"
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

	tree       atomic.Value // stores *Tree
	refreshing atomic.Bool
}

// NewCatalog creates a Catalog with the given dependencies. The initial tree
// is empty; call Refresh to populate it.
func NewCatalog(client Downloader, stateDB *state.DB, m *metrics.Metrics) *Catalog {
	c := &Catalog{
		client:  client,
		stateDB: stateDB,
		metrics: m,
	}
	// Initialise with an empty tree so Tree() never returns nil.
	c.tree.Store(&Tree{dirs: make(map[string][]DirEntry)})
	return c
}

// Tree returns the current filesystem tree. Safe to call from any goroutine.
func (c *Catalog) Tree() *Tree {
	return c.tree.Load().(*Tree)
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
			return err
		}
		c.metrics.APICallCount.Add(1)
		slog.Info("fetched downloads", "kind", kind, "count", len(downloads))
		allDownloads = append(allDownloads, downloads...)
	}

	slog.Info("total downloads fetched", "count", len(allDownloads))

	// Build the virtual filesystem tree.
	tree := BuildTree(allDownloads)

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
		return err
	}

	// Swap tree atomically.
	c.tree.Store(tree)

	// Update metrics.
	c.metrics.CatalogItems.Store(int64(len(files)))
	slog.Info("catalog refresh complete", "files", len(files))

	return nil
}

// ErrRefreshInProgress is returned when Refresh is called while a refresh
// is already running.
var ErrRefreshInProgress = refreshInProgressError{}

type refreshInProgressError struct{}

func (refreshInProgressError) Error() string { return "refresh already in progress" }