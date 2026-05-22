// Package fusefs implements a read-only FUSE filesystem that mounts
// TorBox cached media as a local directory tree for Plex/Jellyfin.
package fusefs

import (
	"context"
	"log/slog"
	"sync"
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
// kernel cannot forget them.
type RootNode struct {
	fs.Inode

	mu       sync.RWMutex
	tree     *catalog.Tree
	stateDB  *state.DB
	streamer *stream.StreamReader
	cfg      *config.Config
	tbClient *torbox.Client // needed for permalink URL construction
}

// NewRootNode creates a RootNode with all required dependencies.
func NewRootNode(tree *catalog.Tree, stateDB *state.DB, streamer *stream.StreamReader, cfg *config.Config, tbClient *torbox.Client) *RootNode {
	return &RootNode{
		tree:     tree,
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
}

// addCategoryDir creates a top-level category directory (movies/series) and
// recursively populates its children from the catalog tree.
func (r *RootNode) addCategoryDir(ctx context.Context, name, catalogPath string) {
	entries := r.tree.ListDir(catalogPath)
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
	entries := r.tree.ListDir(catalogPath)
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
		r.cfg.APIKey,
		f.DownloadKind,
		f.DownloadID,
		f.FileID,
	)

	fileNode := &FileNode{
		fileKey:     contentKey,
		permalinkURL: permalinkURL,
		size:        uint64(f.Size),
		streamer:    r.streamer,
		cfg:         r.cfg,
	}
	child := parent.NewPersistentInode(ctx, fileNode, fs.StableAttr{
		Mode: syscall.S_IFREG | 0444,
		Ino:  ino,
	})
	parent.AddChild(name, child, false)
}

// Getattr returns root directory attributes.
func (r *RootNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = syscall.S_IFDIR | 0755
	out.Attr.Uid = r.cfg.UID
	out.Attr.Gid = r.cfg.GID
	out.Attr.Nlink = 2
	return 0
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
	out.Attr.Mode = syscall.S_IFDIR | 0755
	out.Attr.Uid = d.cfg.UID
	out.Attr.Gid = d.cfg.GID
	out.Attr.Nlink = 2
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

// ---------------------------------------------------------------------------
// FileNode
// ---------------------------------------------------------------------------

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
	out.Attr.Mode = syscall.S_IFREG | 0444
	out.Attr.Size = f.size
	out.Attr.Uid = f.cfg.UID
	out.Attr.Gid = f.cfg.GID
	out.Attr.Nlink = 1
	out.Attr.Blksize = 4096
	out.Attr.Blocks = (f.size + 4095) / 4096
	return 0
}

// Open rejects non-readonly opens.
func (f *FileNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Allow O_RDONLY (0) and read-only combinations.
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		return nil, 0, syscall.EACCES
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Read reads file data from the stream reader directly into the FUSE
// destination buffer. On cache hits this is zero-alloc (cache.CopyTo
// writes straight into dest).
func (f *FileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := f.streamer.ReadAt(ctx, f.fileKey, off, dest)
	if err != nil {
		return nil, syscall.EIO
	}
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

// Compile-time interface checks.
var (
	_ fs.InodeEmbedder      = (*RootNode)(nil)
	_ fs.NodeOnAdder        = (*RootNode)(nil)
	_ fs.NodeGetattrer      = (*RootNode)(nil)
	_ fs.NodeStatfser       = (*RootNode)(nil)

	_ fs.InodeEmbedder      = (*DirNode)(nil)
	_ fs.NodeGetattrer      = (*DirNode)(nil)
	_ fs.NodeMkdirer        = (*DirNode)(nil)
	_ fs.NodeUnlinker       = (*DirNode)(nil)
	_ fs.NodeRmdirer        = (*DirNode)(nil)
	_ fs.NodeRenamer        = (*DirNode)(nil)
	_ fs.NodeCreater        = (*DirNode)(nil)
	_ fs.NodeSymlinker      = (*DirNode)(nil)
	_ fs.NodeLinker         = (*DirNode)(nil)
	_ fs.NodeSetattrer      = (*DirNode)(nil)

	_ fs.InodeEmbedder      = (*FileNode)(nil)
	_ fs.NodeGetattrer      = (*FileNode)(nil)
	_ fs.NodeOpener         = (*FileNode)(nil)
	_ fs.NodeReader         = (*FileNode)(nil)
	_ fs.NodeSetattrer      = (*FileNode)(nil)
	_ fs.NodeWriter         = (*FileNode)(nil)
	_ fs.NodeAllocater      = (*FileNode)(nil)
	_ fs.NodeFsyncer        = (*FileNode)(nil)
)

// permalinkBuilderFor creates a PermalinkBuilder function that resolves
// a file's content key to its CDN permalink URL using the TorBox client.
// This is used as the callback in stream.NewStreamReader.
func PermalinkBuilderFromClient(cfg *config.Config, tbClient *torbox.Client) stream.PermalinkBuilder {
	return func(fileKey string) string {
		// fileKey is "kind:downloadID:fileID" — parse it.
		kind, downloadID, fileID := parseContentKey(fileKey)
		return torbox.PermalinkURL(cfg.APIKey, kind, downloadID, fileID)
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