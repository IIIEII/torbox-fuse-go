package catalog

import (
	"path"
	"sort"

	"github.com/iiieii/torbox-fuse-go/internal/state"
)

// BuildTreeFromDB reconstructs a virtual filesystem tree from database file
// records. This enables instant startup without an API round-trip: the tree
// is built from cached data and later refreshed from the API in the background.
//
// Each record's Path field contains the full virtual path (e.g.
// "/movies/The Matrix/film.mkv"), so we can directly reconstruct the directory
// layout without re-classifying media types.
//
// MimeType and MediaType are not stored in the DB, but all persisted records
// are video files (the video/ filter ran before upsert). We set placeholder
// values that are never consumed by the FUSE layer.
func BuildTreeFromDB(records []state.FileRecord, allDir bool) *Tree {
	t := &Tree{dirs: make(map[string][]DirEntry)}
	seen := make(map[string]bool) // deduplicate by content key

	for _, rec := range records {
		if seen[rec.ContentKey] {
			continue
		}
		seen[rec.ContentKey] = true

		dirPath := path.Dir(rec.Path)
		fileName := path.Base(rec.Path)

		f := &File{
			DownloadKind: DownloadKind(rec.DownloadKind),
			DownloadID:   rec.DownloadID,
			FileID:       rec.FileID,
			Name:         fileName,
			Size:         rec.Size,
			MimeType:     "video/x-matroska", // placeholder; all DB records are video
			// MediaType is not needed — the path already encodes classification.
		}

		t.dirs[dirPath] = append(t.dirs[dirPath], DirEntry{
			Name: fileName,
			File: f,
		})
	}

	// Build /all directory if enabled.
	if allDir && len(records) > 0 {
		buildAllDirFromDB(t, records, seen)
	}

	t.buildParentDirs()

	// Sort every directory's entries alphabetically by name.
	for p := range t.dirs {
		sort.Slice(t.dirs[p], func(i, j int) bool {
			return t.dirs[p][i].Name < t.dirs[p][j].Name
		})
	}

	return t
}

// buildAllDirFromDB populates the /all directory from DB records, mirroring
// the logic in addAllDir. Each file appears under /all/<title>/<filename>,
// where title is extracted from the path (second path segment).
func buildAllDirFromDB(t *Tree, records []state.FileRecord, seen map[string]bool) {
	for _, rec := range records {
		dirPath := path.Dir(rec.Path)
		// Extract title: for "/movies/The Matrix/film.mkv" → "The Matrix"
		// For "/series/Breaking Bad/Season 1/ep.mkv" → "Breaking Bad"
		segments := splitPath(dirPath)
		var title string
		if len(segments) >= 2 {
			title = segments[1] // first segment after /movies or /series
		} else {
			continue
		}

		allDirPath := path.Join("/all", title)
		fileName := path.Base(rec.Path)

		// ContentKey dedup is already handled by caller's seen map for the main
		// loop. Files that appear under both /movies and /series share the same
		// content key, so we only add them once to /all.
		key := rec.ContentKey
		if seen["all:"+key] {
			continue
		}
		seen["all:"+key] = true

		// Look up the file we already added to the main tree.
		for _, e := range t.dirs[dirPath] {
			if e.Name == fileName && e.File != nil {
				t.dirs[allDirPath] = append(t.dirs[allDirPath], DirEntry{
					Name: fileName,
					File: e.File, // share the same File pointer
				})
				break
			}
		}
	}
}

// ApplyHides removes entries from the tree whose content keys appear in the
// hidden set. Directories that become empty after hiding are also removed.
// This is called after BuildTree or BuildTreeFromDB, before the tree is
// swapped into the catalog.
func ApplyHides(tree *Tree, hidden map[string]bool) *Tree {
	if len(hidden) == 0 {
		return tree
	}

	// Build a new tree excluding hidden files.
	filtered := &Tree{dirs: make(map[string][]DirEntry)}

	for dirPath, entries := range tree.dirs {
		var kept []DirEntry
		for _, e := range entries {
			if e.File != nil && hidden[e.File.ContentKey()] {
				continue // skip hidden file
			}
			kept = append(kept, e)
		}
		if len(kept) > 0 {
			filtered.dirs[dirPath] = kept
		}
	}

	// Rebuild parent dirs since removing files may have emptied directories.
	filtered.buildParentDirs()

	// Sort entries.
	for p := range filtered.dirs {
		sort.Slice(filtered.dirs[p], func(i, j int) bool {
			return filtered.dirs[p][i].Name < filtered.dirs[p][j].Name
		})
	}

	return filtered
}

// splitPath splits a clean path into its non-empty segments.
// e.g. "/movies/The Matrix" → ["movies", "The Matrix"]
func splitPath(p string) []string {
	p = path.Clean(p)
	if p == "/" || p == "." {
		return nil
	}
	var parts []string
	for _, seg := range splitPathRaw(p) {
		if seg != "" {
			parts = append(parts, seg)
		}
	}
	return parts
}

// splitPathRaw splits on "/".
func splitPathRaw(p string) []string {
	// Simple split — avoid importing strings just for this.
	var parts []string
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			if i > start {
				parts = append(parts, p[start:i])
			}
			start = i + 1
		}
	}
	if start < len(p) {
		parts = append(parts, p[start:])
	}
	return parts
}
