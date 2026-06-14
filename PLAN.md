# Plan: Fast Startup from DB Cache

## Problem

On restart, the app blocks on `cat.Refresh(ctx)` — a full API round-trip — before
the FUSE mount shows any files. If the TorBox API is slow or down, the app exits
with `os.Exit(1)` and the mount is completely empty.

## What we have

The SQLite `files` table already stores everything needed to build the tree:
- `content_key` (kind:downloadID:fileID)
- `download_kind`, `download_id`, `file_id`
- `path` (full virtual path like `/movies/The Matrix/film.mkv`)
- `size`

The `inodes` table already stores stable inode assignments.

## Approach

**Load tree from DB first, mount immediately, then refresh from API in background.**

### Step 1 — Add `ListFiles` to `state.DB`

New method that reads all file records from the `files` table:

```go
func (db *DB) ListFiles() ([]FileRecord, error)
```

Returns all rows. Empty slice (not error) if table is empty (first run).

### Step 2 — Add `BuildTreeFromDB` to `catalog`

New function that converts `[]state.FileRecord` → `catalog.Tree`:

```go
func BuildTreeFromDB(records []state.FileRecord, allDir bool) *Tree
```

Logic: parse each record's `path` to reconstruct the directory structure.
Since `path` already contains the full virtual path (e.g. `/movies/The Matrix/film.mkv`),
we can directly populate `Tree.dirs` without re-classifying media types.

For each record:
- `dirPath = path.Dir(record.Path)`
- `fileName = path.Base(record.Path)`
- Build `DirEntry{Name: fileName, File: &File{...}}` from DB fields
- Call `buildParentDirs()` and sort, same as `BuildTree`

The `MimeType` field is not stored in DB — it's only used for the `video/`
filter in `BuildTree`. Since we only ever stored video files (the filter ran
before upsert), all DB records are already video. We set `MimeType = "video/x-matroska"`
as a placeholder — it's never read back at FUSE level.

`MediaType` is also not stored — but the path already encodes the classification
(`/movies/...` vs `/series/...`). We don't need to reconstruct it; the only
consumer is `BuildTree.addDownload()` which we're bypassing entirely.

### Step 3 — Add `LoadFromDB` method to `Catalog`

```go
func (c *Catalog) LoadFromDB(ctx context.Context) error
```

Reads all files from DB, builds tree via `BuildTreeFromDB`, swaps it atomically,
and triggers `onRefresh` callback. Returns nil if DB is empty (first run).

### Step 4 — Change startup sequence in `main.go`

**Before:**
```go
// Block until API responds — mount is empty until this succeeds
if err := cat.Refresh(ctx); err != nil {
    slog.Error("initial catalog refresh", "err", err)
    os.Exit(1)
}
```

**After:**
```go
// 1. Load cached tree from DB (instant, no network)
if err := cat.LoadFromDB(ctx); err != nil {
    slog.Warn("load from db", "err", err) // non-fatal
} else {
    slog.Info("loaded catalog from db cache")
}

// 2. Mount FUSE immediately — files visible right away
root := fusefs.NewRootNode(cat, stateDB, streamer, cfg, tbClient)
cat.SetOnRefresh(func() { root.SyncTree(context.Background()) })

// 3. Background: refresh from API (replaces stale DB data)
go func() {
    slog.Info("background catalog refresh from API")
    if err := cat.Refresh(ctx); err != nil {
        slog.Warn("background catalog refresh", "err", err) // non-fatal
    }
}()
```

Key changes:
- `LoadFromDB` is called first — instant, no network
- FUSE mount happens immediately after, even if DB was empty
- API refresh runs in a goroutine — never blocks startup
- API failure is a warning, not a fatal error

### Step 5 — Move periodic refresh goroutine startup

Currently the periodic refresh ticker starts after the mount. No change needed
here — the initial background refresh goroutine is separate from the ticker.

### Step 6 — Tests

1. `TestBuildTreeFromDB` — verify tree construction from `[]FileRecord`
2. `TestCatalog_LoadFromDB` — verify `LoadFromDB` populates tree from DB
3. `TestCatalog_LoadFromDB_Empty` — empty DB returns nil error, tree stays empty
4. `TestBuildTreeFromDB_AllDir` — verify `/all` directory reconstruction
5. `TestDB_ListFiles` — verify `ListFiles` returns all records

## Edge cases

- **First run (empty DB):** `LoadFromDB` returns nil, tree stays empty. The
  background API refresh populates everything. Same UX as today but without
  blocking — the mount just appears empty for a few seconds.

- **API down on startup:** Mount shows stale (but valid) data from DB. The
  warning log makes it clear. Next periodic refresh will try again.

- **Stale data after API comes back:** The background `cat.Refresh` call
  overwrites the DB-loaded tree via `UpsertFiles` + tree swap + `SyncTree`.
  Files appear/disappear naturally.

- **MediaType not in DB:** Not needed — the virtual path (`/movies/...`,
  `/series/...`) already encodes the classification. We only need `path`,
  `download_kind`, `download_id`, `file_id`, `size` to reconstruct `File`.

- **MimeType not in DB:** All stored records are already video (filtered
  before upsert). Set placeholder `"video/x-matroska"` — never read by FUSE.

## Files changed

| File | Change |
|------|--------|
| `internal/state/db.go` | Add `ListFiles() ([]FileRecord, error)` |
| `internal/catalog/catalog.go` | Add `LoadFromDB`, `BuildTreeFromDB` |
| `cmd/torbox-media-center/main.go` | New startup sequence |
| `internal/state/db_test.go` | Test `ListFiles` |
| `internal/catalog/catalog_test.go` | Tests for `LoadFromDB`, `BuildTreeFromDB` |