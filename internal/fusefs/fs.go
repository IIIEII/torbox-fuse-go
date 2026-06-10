// Package fusefs implements a read-only FUSE filesystem that mounts
// TorBox cached media as a local directory tree for Plex/Jellyfin.
package fusefs

import (
	"context"
	"io"
	"log/slog"
	"path"
	"sync/atomic"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/iiieii/torbox-fuse-go/internal/catalog"
	"github.com/iiieii/torbox-fuse-go/internal/config"
	"github.com/iiieii/torbox-fuse-go/internal/state"
	"github.com/iiieii/torbox-fuse-go/internal/stream"
	"github.com/iiieii/torbox-fuse-go/internal/torbox"
)

// ---------------------------------------------------------------------------
// RootNode
// ---------------------------------------------------------------------------

// RootNode is the FUSE root inode ("/"). It builds the entire virtual tree
// from a catalog.Tree in OnAdd, creating persistent child inodes so the
// kernel cannot forget them. When the catalog is refreshed, SyncTree
// incrementally adds new entries and removes deleted ones.
type RootNode struct {
	fs.Inode

	cat      *catalog.Catalog
	stateDB  *state.DB
	streamer *stream.StreamReader
	cfg      *config.Config
	tbClient *torbox.Client // needed for permalink URL construction
}

// NewRootNode creates a RootNode with all required dependencies.
func NewRootNode(cat *catalog.Catalog, stateDB *state.DB, streamer *stream.StreamReader, cfg *config.Config, tbClient *torbox.Client) *RootNode {
	return &RootNode{
		cat:      cat,
		stateDB:  stateDB,
		streamer: streamer,
		cfg:      cfg,
		tbClient: tbClient,
	}
}

// OnAdd is called when the FUSE mount is initialised. It walks the catalog
// tree and creates persistent inodes for every directory and file.
func (r *RootNode) OnAdd(ctx context.Context) {
	r.addCategoryDir(ctx, "movies", "/movies")
	r.addCategoryDir(ctx, "series", "/series")
	if r.cfg.AllDirEnabled {
		r.addCategoryDir(ctx, "all", "/all")
	}
}

// addCategoryDir creates a top-level category directory (movies/series) and
// recursively populates its children from the catalog tree.
func (r *RootNode) addCategoryDir(ctx context.Context, name, catalogPath string) {
	entries := r.cat.Tree().ListDir(catalogPath)
	if len(entries) == 0 {
		return
	}

	ino, err := r.stateDB.AssignInode("dir:"+catalogPath, catalogPath)
	if err != nil {
		slog.Error("assign inode for dir", "path", catalogPath, "err", err)
		return
	}

	dirNode := &DirNode{
		cfg: r.cfg,
	}
	child := r.NewPersistentInode(ctx, dirNode, fs.StableAttr{
		Mode: syscall.S_IFDIR | 0755,
		Ino:  ino,
	})
	r.AddChild(name, child, false)

	// Recursively populate subdirectories and file leaves.
	r.populateDir(ctx, child, catalogPath)
}

// populateDir walks the catalog tree at the given path and adds child inodes
// to the given parent FUSE inode.
func (r *RootNode) populateDir(ctx context.Context, parent *fs.Inode, catalogPath string) {
	entries := r.cat.Tree().ListDir(catalogPath)
	for _, e := range entries {
		childPath := catalogPath + "/" + e.Name

		if e.File != nil {
			// Leaf file node.
			r.addFileNode(ctx, parent, e.Name, childPath, e.File)
		} else {
			// Subdirectory node.
			r.addSubDirNode(ctx, parent, e.Name, childPath)
		}
	}
}

// addSubDirNode creates a persistent directory inode and recursively populates it.
func (r *RootNode) addSubDirNode(ctx context.Context, parent *fs.Inode, name, catalogPath string) {
	ino, err := r.stateDB.AssignInode("dir:"+catalogPath, catalogPath)
	if err != nil {
		slog.Error("assign inode for dir", "path", catalogPath, "err", err)
		return
	}

	dirNode := &DirNode{
		cfg: r.cfg,
	}
	child := parent.NewPersistentInode(ctx, dirNode, fs.StableAttr{
		Mode: syscall.S_IFDIR | 0755,
		Ino:  ino,
	})
	parent.AddChild(name, child, false)

	// Recurse into this subdirectory.
	r.populateDir(ctx, child, catalogPath)
}

// addFileNode creates a persistent regular-file inode backed by the stream reader.
func (r *RootNode) addFileNode(ctx context.Context, parent *fs.Inode, name, catalogPath string, f *catalog.File) {
	contentKey := f.ContentKey()
	ino, err := r.stateDB.AssignInode(contentKey, catalogPath)
	if err != nil {
		slog.Error("assign inode for file", "path", catalogPath, "err", err)
		return
	}

	// Build the permalink URL for this file using the TorBox client.
	permalinkURL := torbox.PermalinkURL(
		r.cfg.APIBaseURL,
		r.cfg.APIKey,
		f.DownloadKind,
		f.DownloadID,
		f.FileID,
	)

	fileNode := &FileNode{
		fileKey:      contentKey,
		permalinkURL: permalinkURL,
		size:         uint64(f.Size),
		streamer:     r.streamer,
		cfg:          r.cfg,
	}
	child := parent.NewPersistentInode(ctx, fileNode, fs.StableAttr{
		Mode: syscall.S_IFREG | 0444,
		Ino:  ino,
	})
	parent.AddChild(name, child, false)
}

// SyncTree incrementally updates the FUSE inode tree to match the latest
// catalog state. After a catalog refresh, the in-memory tree is swapped
// atomically but the FUSE inodes (visible to Plex via the mount) are stale.
// SyncTree walks both trees and adds new entries, removes deleted ones, and
// recurses into shared subdirectories. It must be called after Catalog.Refresh
// completes.
func (r *RootNode) SyncTree(ctx context.Context) {
	newTree := r.cat.Tree()
	catPaths := []string{"/movies", "/series"}
	if r.cfg.AllDirEnabled {
		catPaths = append(catPaths, "/all")
	}
	for _, catPath := range catPaths {
		name := path.Base(catPath)
		child := r.GetChild(name)
		if child == nil {
			// Category didn't exist before — create it.
			r.addCategoryDir(ctx, name, catPath)
			r.NotifyEntry(name) //nolint:errcheck // kernel notification best-effort
			continue
		}
		r.syncDir(ctx, child, catPath, newTree)
	}
}

// syncDir incrementally merges a FUSE directory inode with the latest
// catalog tree at the given catalogPath. It adds new entries, removes
// deleted ones, and recurses into shared subdirectories.
func (r *RootNode) syncDir(ctx context.Context, fuseDir *fs.Inode, catalogPath string, newTree *catalog.Tree) {
	newEntries := newTree.ListDir(catalogPath)
	newNames := make(map[string]catalog.DirEntry, len(newEntries))
	for _, e := range newEntries {
		newNames[e.Name] = e
	}

	// Remove entries that no longer exist in the catalog.
	for name := range fuseDir.Children() {
		if _, exists := newNames[name]; !exists {
			ch := fuseDir.GetChild(name)
			if ch != nil {
				ch.RmAllChildren()
			}
			fuseDir.RmChild(name)
			fuseDir.NotifyEntry(name) //nolint:errcheck // kernel notification best-effort
			slog.Debug("fuse tree: removed entry", "path", catalogPath+"/"+name)
		}
	}

	// Add new entries and recurse into shared subdirectories.
	for _, e := range newEntries {
		childPath := catalogPath + "/" + e.Name
		existing := fuseDir.GetChild(e.Name)

		if existing == nil {
			// New entry — add it.
			if e.File != nil {
				r.addFileNode(ctx, fuseDir, e.Name, childPath, e.File)
			} else {
				r.addSubDirNode(ctx, fuseDir, e.Name, childPath)
			}
			fuseDir.NotifyEntry(e.Name) //nolint:errcheck // kernel notification best-effort
			slog.Debug("fuse tree: added entry", "path", childPath)
		} else if e.File == nil {
			// Shared subdirectory — recurse to sync its children.
			r.syncDir(ctx, existing, childPath, newTree)
		}
		// Shared file entries are skipped — their inode is stable and
		// content is fetched on demand from the CDN.
	}
}

// Getattr returns root directory attributes.
func (r *RootNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0755
	out.Uid = r.cfg.UID
	out.Gid = r.cfg.GID
	out.Nlink = 2
	return 0
}

// Getxattr returns ENOATTR for extended attributes on the root node.
func (r *RootNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	return 0, errNoAttr
}

// Listxattr returns an empty xattr list for the root node.
func (r *RootNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	return 0, 0
}

// Statfs returns filesystem statistics for the root.
func (r *RootNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// Return reasonable defaults; the kernel uses this for df etc.
	out.Bsize = 4096
	out.Blocks = 1 << 20 // placeholder
	out.Bfree = 0
	out.Bavail = 0
	out.Files = 1 << 16 // placeholder
	out.Ffree = 0
	out.NameLen = 255
	return 0
}

// ---------------------------------------------------------------------------
// DirNode
// ---------------------------------------------------------------------------

// DirNode represents a read-only directory in the FUSE tree.
// Lookup and Readdir are handled automatically by go-fuse from the persistent
// children created during OnAdd, but we implement Getattr to return
// directory attributes and block write operations.
type DirNode struct {
	fs.Inode

	cfg *config.Config
}

// Getattr returns directory attributes.
func (d *DirNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0755
	out.Uid = d.cfg.UID
	out.Gid = d.cfg.GID
	out.Nlink = 2
	return 0
}

// Mkdir rejects directory creation (read-only filesystem).
func (d *DirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}

// Unlink rejects file removal (read-only filesystem).
func (d *DirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return syscall.EROFS
}

// Rmdir rejects directory removal (read-only filesystem).
func (d *DirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return syscall.EROFS
}

// Rename rejects renames (read-only filesystem).
func (d *DirNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return syscall.EROFS
}

// Create rejects file creation (read-only filesystem).
func (d *DirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, nil, 0, syscall.EROFS
}

// Symlink rejects symlink creation (read-only filesystem).
func (d *DirNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	return nil, syscall.EROFS
}

// Link rejects hard-link creation (read-only filesystem).
func (d *DirNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	return nil, syscall.EROFS
}

// Setattr rejects attribute changes (read-only filesystem).
func (d *DirNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return syscall.EROFS
}

// Getxattr returns ENOATTR for all xattr queries. This silences the macOS
// "Unimplemented opcode OPCODE-60" spam for com.apple.FinderInfo etc.
func (d *DirNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	return 0, errNoAttr
}

// Listxattr returns an empty xattr list.
func (d *DirNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	return 0, 0
}

// ---------------------------------------------------------------------------
// FileNode
// ---------------------------------------------------------------------------

// nextReaderID is a monotonically increasing counter for unique FUSE handle IDs.
// Each Open call gets a unique readerID for dashboard read-position tracking.
var nextReaderID atomic.Uint64

// fileHandle is a per-open-instance FUSE file handle that carries a unique
// readerID for dashboard read-position tracking. Each Open call creates a new
// fileHandle; each Release call cleans up exactly its own readerID.
type fileHandle struct {
	readerID uint64 // unique per-open, assigned in Open
}

// FileNode represents a read-only regular file backed by the TorBox CDN via
// the stream.StreamReader. Reads are served directly into the FUSE buffer
// for zero-alloc cache hits.
type FileNode struct {
	fs.Inode

	fileKey      string // content key passed to StreamReader
	permalinkURL string // CDN permalink for this file
	size         uint64
	streamer     *stream.StreamReader
	cfg          *config.Config
}

// Getattr returns file attributes.
func (f *FileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFREG | 0444
	out.Size = f.size
	out.Uid = f.cfg.UID
	out.Gid = f.cfg.GID
	out.Nlink = 1
	out.Blksize = 4096
	out.Blocks = (f.size + 4095) / 4096
	return 0
}

// Open rejects non-readonly opens and returns a per-handle fileHandle
// with a unique reader ID for dashboard read-position tracking.
// Each Open call gets its own readerID so that multiple concurrent FUSE
// handles on the same file (e.g. Plex header read + EOF probe + playback)
// are tracked independently and cleaned up correctly in Release.
func (f *FileNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	slog.Debug("fuse open", "fileKey", f.fileKey, "flags", flags)
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		slog.Warn("fuse open rejected: not read-only", "fileKey", f.fileKey, "flags", flags)
		return nil, 0, syscall.EACCES
	}
	handle := &fileHandle{readerID: nextReaderID.Add(1)}
	return handle, fuse.FOPEN_KEEP_CACHE, 0
}

// Read reads file data from the stream reader directly into the FUSE
// destination buffer. On cache hits this is zero-alloc (cache.CopyTo
// writes straight into dest).
func (f *FileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := f.streamer.ReadAt(ctx, f.fileKey, off, dest, int64(f.size))
	if err != nil && err != io.EOF {
		slog.Warn("fuse read error", "fileKey", f.fileKey, "offset", off, "reqSize", len(dest), "err", err)
		return nil, syscall.EIO
	}
	// Track read position for dashboard visualization using the per-handle readerID.
	if h, ok := fh.(*fileHandle); ok && h.readerID > 0 {
		f.streamer.TrackReader(f.fileKey, h.readerID, off+int64(n))
	}
	slog.Debug("fuse read", "fileKey", f.fileKey, "offset", off, "reqSize", len(dest), "n", n, "eof", err == io.EOF)
	return fuse.ReadResultData(dest[:n]), 0
}

// Setattr rejects attribute changes (read-only filesystem).
func (f *FileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return syscall.EROFS
}

// Write rejects writes (read-only filesystem).
func (f *FileNode) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	return 0, syscall.EROFS
}

// Allocate rejects allocation (read-only filesystem).
func (f *FileNode) Allocate(ctx context.Context, fh fs.FileHandle, off, size uint64, mode uint32) syscall.Errno {
	return syscall.EROFS
}

// Fsync is a no-op for a read-only CDN-backed file.
func (f *FileNode) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	return 0
}

// Release is called by the FUSE kernel when a file handle is closed.
// Each Open creates a unique fileHandle with its own readerID; Release
// untracks exactly that reader and cancels inflight windows only when the
// last handle for this file is closed (preventing stale read cursors and
// orphaned downloads after seek or stop).
func (f *FileNode) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	h := fh.(*fileHandle)
	slog.Debug("fuse release", "fileKey", f.fileKey, "readerID", h.readerID)
	f.streamer.UntrackReader(f.fileKey, h.readerID)
	// Cancel inflight windows only if no other handles remain open for this file.
	if !f.streamer.HasReaders(f.fileKey) {
		f.streamer.CancelFile(f.fileKey)
	}
	return 0
}

// Getxattr returns ENOATTR for all xattr queries. This silences the macOS
// "Unimplemented opcode OPCODE-60" spam for com.apple.FinderInfo etc.
func (f *FileNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	return 0, errNoAttr
}

// Listxattr returns an empty xattr list.
func (f *FileNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	return 0, 0
}

// Compile-time interface checks.
var (
	_ fs.InodeEmbedder   = (*RootNode)(nil)
	_ fs.NodeOnAdder     = (*RootNode)(nil)
	_ fs.NodeGetattrer   = (*RootNode)(nil)
	_ fs.NodeStatfser    = (*RootNode)(nil)
	_ fs.NodeGetxattrer  = (*RootNode)(nil)
	_ fs.NodeListxattrer = (*RootNode)(nil)

	_ fs.InodeEmbedder   = (*DirNode)(nil)
	_ fs.NodeGetattrer   = (*DirNode)(nil)
	_ fs.NodeMkdirer     = (*DirNode)(nil)
	_ fs.NodeUnlinker    = (*DirNode)(nil)
	_ fs.NodeRmdirer     = (*DirNode)(nil)
	_ fs.NodeRenamer     = (*DirNode)(nil)
	_ fs.NodeCreater     = (*DirNode)(nil)
	_ fs.NodeSymlinker   = (*DirNode)(nil)
	_ fs.NodeLinker      = (*DirNode)(nil)
	_ fs.NodeSetattrer   = (*DirNode)(nil)
	_ fs.NodeGetxattrer  = (*DirNode)(nil)
	_ fs.NodeListxattrer = (*DirNode)(nil)

	_ fs.InodeEmbedder   = (*FileNode)(nil)
	_ fs.NodeGetattrer   = (*FileNode)(nil)
	_ fs.NodeOpener      = (*FileNode)(nil)
	_ fs.NodeReader      = (*FileNode)(nil)
	_ fs.NodeSetattrer   = (*FileNode)(nil)
	_ fs.NodeWriter      = (*FileNode)(nil)
	_ fs.NodeAllocater   = (*FileNode)(nil)
	_ fs.NodeFsyncer     = (*FileNode)(nil)
	_ fs.NodeReleaser    = (*FileNode)(nil)
	_ fs.NodeGetxattrer  = (*FileNode)(nil)
	_ fs.NodeListxattrer = (*FileNode)(nil)
)

// permalinkBuilderFor creates a PermalinkBuilder function that resolves
// a file's content key to its CDN permalink URL using the TorBox client.
// This is used as the callback in stream.NewStreamReader.
func PermalinkBuilderFromClient(cfg *config.Config, tbClient *torbox.Client) stream.PermalinkBuilder {
	return func(fileKey string) string {
		// fileKey is "kind:downloadID:fileID" — parse it.
		kind, downloadID, fileID := parseContentKey(fileKey)
		return torbox.PermalinkURL(cfg.TorboxConfig().APIBaseURL(), cfg.APIKey, kind, downloadID, fileID)
	}
}

// parseContentKey splits a content key "kind:downloadID:fileID" into its parts.
func parseContentKey(key string) (kind catalog.DownloadKind, downloadID, fileID string) {
	// Content key format is "torrent:123:456" — split on ":".
	parts := make([]string, 0, 3)
	start := 0
	for i := 0; i < len(key); i++ {
		if key[i] == ':' {
			parts = append(parts, key[start:i])
			start = i + 1
		}
	}
	parts = append(parts, key[start:])

	if len(parts) >= 3 {
		kind = catalog.DownloadKind(parts[0])
		downloadID = parts[1]
		fileID = parts[2]
	}
	return
}
