# Plan: Writable FUSE with Hide/Delete & Dashboard Undo

## Problem

Users want to delete files from the FUSE mount and manage downloads. The design
simplifies to: **Unlink always hides (soft-delete)**. The dashboard shows which
downloads have hidden files and lets the user either **restore** (unhide) or
**permanently delete from TorBox**.

## Key simplification

There are **two distinct actions** and the user decides which one:

1. **Hide** (`rm` in FUSE) — removes the file from the virtual tree but does NOT
   touch TorBox. Reversible instantly via dashboard "Unhide" button.
2. **Delete from TorBox** (dashboard button) — permanently removes the download
   from TorBox via API. This is an explicit, separate action with confirmation.

This means:
- No auto-delete when "all files hidden"
- No need to store magnet/hash for re-download
- No complex "partially hidden" → "fully deleted" transitions
- Dashboard clearly distinguishes "hidden" from "deleted from TorBox"

## Architecture

### 1. Hidden files model

A `hidden_files` table in SQLite tracks which content keys are hidden.
Hidden files are filtered from the FUSE tree on every tree build/refresh.

```sql
CREATE TABLE IF NOT EXISTS hidden_files (
    content_key TEXT PRIMARY KEY,
    hidden_at   TEXT NOT NULL
);
```

### 2. Deletion log

A `deletions` table records downloads that were deleted from TorBox via the
dashboard. This is purely for the dashboard UI — to show "you deleted X on date Y".
There is no "restore from TorBox" — once deleted, it's gone from TorBox.

```sql
CREATE TABLE IF NOT EXISTS deletions (
    download_key  TEXT PRIMARY KEY,  -- "kind:downloadID"
    download_kind TEXT NOT NULL,
    download_id   TEXT NOT NULL,
    download_name TEXT NOT NULL,
    file_count    INTEGER NOT NULL,
    total_size    INTEGER NOT NULL,
    deleted_at    TEXT NOT NULL
);
```

### 3. Component changes

#### `internal/state/db.go`

New methods:
- `HideFile(contentKey string) error` — insert into `hidden_files`
- `UnhideFile(contentKey string) error` — remove from `hidden_files`
- `IsHidden(contentKey string) (bool, error)` — check if hidden
- `ListHiddenFiles() ([]FileRecord, error)` — all hidden file records
- `UnhideAllForDownload(downloadKind, downloadID string) error` — clear hides for a download
- `IsDownloadFullyHidden(downloadKind, downloadID string) (bool, error)` — all video files hidden?
- `RecordDeletion(kind, id, name string, fileCount int, totalSize int64) error`
- `ListDeletions(limit int) ([]DeletionRecord, error)` — for dashboard
- `ClearDeletion(downloadKey string) error` — remove deletion record (forget)
- `ClearHiddenForDownload(downloadKind, downloadID string) error` — unhide all files of a download

New types:
```go
type DeletionRecord struct {
    DownloadKey  string
    DownloadKind string
    DownloadID   string
    DownloadName string
    FileCount    int
    TotalSize    int64
    DeletedAt    string
}
```

#### `internal/catalog/catalog.go`

- Add `ApplyHides(tree *Tree, db *state.DB) *Tree` — filters hidden files from the tree.
  For each `DirEntry` with a `File`, check `IsHidden(contentKey)`. If hidden, remove entry.
  Remove empty directories after filtering.
- Modify `Refresh` and `LoadFromDB` to call `ApplyHides` before tree swap.

#### `internal/catalog/tree.go` or `tree_from_db.go`

- Add `FilterHidden(keys map[string]bool) *Tree` method on `*Tree` — returns a new
  tree with hidden files removed and empty directories pruned.

#### `internal/torbox/client.go`

- Add `DeleteDownload(ctx context.Context, kind catalog.DownloadKind, downloadID string) error`
  Maps to:
  - `POST /torrents/deletetorrent?id={downloadID}` for torrents
  - `POST /usenet/deleteusenet?id={downloadID}` for usenet
  - `POST /webdl/deletewebdownload?id={downloadID}` for webdl

#### `internal/fusefs/fs.go`

When `cfg.Writable` is true:
- `DirNode.Unlink(ctx, name)` → look up child `FileNode`, get `contentKey`,
  call `stateDB.HideFile(contentKey)`, remove inode from tree via `parent.RmChild(name)`.
  Log the action. **Does NOT call TorBox delete API.**
- `DirNode.Rmdir(ctx, name)` → look up all `FileNode` children in that directory,
  hide each one. Remove directory inode from tree.
  Log the action. **Does NOT call TorBox delete API.**

When `cfg.Writable` is false (default): return `syscall.EROFS` as before.

RootNode needs access to `stateDB` for `HideFile` calls. It already has it.

#### `internal/config/config.go`

Add `Writable bool` field, env `FUSE_WRITABLE` (default: `false`).

#### `internal/dashboard/dashboard.go`

Add to `DashboardSnapshot`:
```go
type HiddenDownloadJSON struct {
    DownloadKey  string   `json:"download_key"`   // "torrent:12345"
    DownloadName string  `json:"download_name"`
    HiddenFiles  []HiddenFileJSON `json:"hidden_files"`
    TotalFiles   int      `json:"total_files"`    // all video files in download
    AllHidden    bool     `json:"all_hidden"`      // true if every video file is hidden
}

type HiddenFileJSON struct {
    ContentKey string `json:"content_key"`
    FilePath   string `json:"file_path"`
    FileSize   int64  `json:"file_size"`
    HiddenAt   string `json:"hidden_at"`
}

type DeletionRecordJSON struct {
    DownloadKey  string `json:"download_key"`
    DownloadName string `json:"download_name"`
    FileCount    int    `json:"file_count"`
    TotalSize    int64  `json:"total_size"`
    DeletedAt    string `json:"deleted_at"`
}
```

Snapshot now includes:
- `HiddenDownloads []HiddenDownloadJSON` — grouped by download, shows partial/total hides
- `RecentlyDeleted []DeletionRecordJSON` — downloads removed from TorBox

#### `internal/dashboard/server.go`

New API endpoints:
- `GET /api/hidden` — list hidden downloads (grouped by download, with all-hidden flag)
- `POST /api/unhide` — unhide a file or all files of a download, then refresh catalog
- `POST /api/delete-download` — delete a download from TorBox (with confirmation),
  record in deletions table, unhide its files, refresh catalog
- `GET /api/deletions` — list recent TorBox deletions
- `POST /api/forget-deletion` — remove deletion record from log (don't restore, just forget)

#### `cmd/torbox-media-center/main.go`

- Pass `cfg.Writable` to `RootNode`
- Pass `tbClient` and `cat` to dashboard for unhide/delete operations
- No other startup changes needed

### 4. FUSE Unlink flow (detailed)

```
1. User: rm /mnt/torbox/movies/The Matrix/The.Matrix.1999.mkv
2. FUSE calls DirNode.Unlink("The.Matrix.1999.mkv")
3. Unlink handler:
   a. Look up child inode → get FileNode
   b. Extract contentKey = "torrent:12345:678"
   c. stateDB.HideFile(contentKey) — mark as hidden in SQLite
   d. Remove inode from FUSE tree: parent.RmChild("The.Matrix.1999.mkv")
   e. parent.NotifyEntry("The.Matrix.1999.mkv") — kernel cache invalidation
   f. Log: "file hidden", "content_key", "path"
   g. **Do NOT call TorBox delete API.**
```

### 5. Dashboard Delete from TorBox flow

```
1. User: clicks "Delete from TorBox" for download "torrent:12345"
2. Dashboard POST /api/delete-download {"download_key": "torrent:12345"}
3. Handler:
   a. Parse download_key → kind="torrent", downloadID="12345"
   b. Look up download name and file count from catalog tree (before deletion)
   c. tbClient.DeleteDownload(ctx, "torrent", "12345") — call TorBox API
   d. stateDB.RecordDeletion("torrent", "12345", "The Matrix", N, totalSize)
   e. stateDB.ClearHiddenForDownload("torrent", "12345") — clean up hide list
   f. catalog.Refresh(ctx) — tree rebuild removes the download
   g. Return success
```

### 6. Dashboard Unhide / Restore flow

```
1. User: clicks "Unhide" for download "torrent:12345"
2. Dashboard POST /api/unhide {"download_key": "torrent:12345"}
3. Handler:
   a. stateDB.UnhideAllForDownload("torrent", "12345")
   b. catalog.Refresh(ctx) OR catalog.LoadFromDB(ctx) + ApplyHides — files reappear
   c. Return success
```

### 7. Edge cases

- **Empty directory after Unlink**: When all files in a directory are hidden,
  the directory becomes empty. `ApplyHides` prunes empty directories.
  If the user unhides, the next refresh restores the directory.

- **Multi-file download, partial hide**: User deletes 2 of 5 files.
  Files are hidden from FUSE, 3 remain visible. Dashboard shows
  "2 of 5 files hidden" with a per-download summary.

- **Rmdir on a title directory**: `rm -rf /mnt/torbox/movies/The Matrix/`
  hides all files in that directory. Dashboard shows "all files hidden" with
  a "Delete from TorBox" button and an "Unhide all" button.

- **Read-only mode (default)**: All write operations return EROFS.
  Hidden/deletion tables remain empty. Dashboard shows empty sections.

- **Delete from TorBox fails**: Return error to dashboard. Download stays in
  TorBox, files stay hidden. User can retry.

- **Dashboard "Forget" button**: Removes deletion record from the log.
  Doesn't restore anything — just cleans up the UI. Useful for old deletions.

## Files changed

| File | Change |
|------|--------|
| `internal/state/db.go` | Add hidden_files/deletions tables, Hide/Unhide/IsHidden/RecordDeletion/ListDeletions/ClearDeletion methods |
| `internal/state/db_test.go` | Tests for new methods |
| `internal/torbox/client.go` | Add `DeleteDownload` method |
| `internal/torbox/client_test.go` | Test for `DeleteDownload` |
| `internal/catalog/catalog.go` | Add `ApplyHides`, modify Refresh/LoadFromDB |
| `internal/catalog/tree.go` | Add `FilterHidden` method on Tree |
| `internal/fusefs/fs.go` | Implement writable Unlink/Rmdir with hide logic |
| `internal/fusefs/mount.go` | No changes needed (writable is per-inode, not mount option) |
| `internal/config/config.go` | Add `Writable` field, `FUSE_WRITABLE` env |
| `internal/dashboard/dashboard.go` | Add HiddenDownloads, RecentlyDeleted to snapshot |
| `internal/dashboard/server.go` | Add /api/hidden, /api/unhide, /api/delete-download, /api/deletions, /api/forget-deletion |
| `cmd/torbox-media-center/main.go` | Pass `cfg.Writable` to RootNode |