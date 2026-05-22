package catalog

import (
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// DirEntry represents a named entry within a virtual directory.
// If File is non-nil the entry is a leaf file; otherwise it is a sub-directory.
type DirEntry struct {
	Name string
	File *File
}

// Tree represents a virtual filesystem built from a set of Downloads.
// The keys of dirs are clean, normalised paths such as
// "/movies/The Matrix", "/series/Breaking Bad/Season 1", etc.
type Tree struct {
	dirs map[string][]DirEntry
}

// BuildTree constructs a virtual filesystem tree from a slice of Downloads.
//
// Mapping rules:
//   - Movies  → /movies/<title>/<filename>
//   - Series  → /series/<title>/Season <N>/<filename>
//   - Anime   → /series/<title>/Season <N>/<filename>
//   - Only video files (MIME type starting with "video/") are included.
//   - If the download name looks like a hex hash (≥16 hex chars), the first
//     path segment of the file's Name is used as the title instead.
//   - Season number is parsed from the filename; default is Season 1.
//   - Untagged files default to movie.
//   - Directory entries are sorted alphabetically.
func BuildTree(downloads []Download) *Tree {
	t := &Tree{dirs: make(map[string][]DirEntry)}
	for i := range downloads {
		t.addDownload(&downloads[i])
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

// ListDir returns the sorted directory entries for the given path.
// The path must be clean (e.g. "/movies/The Matrix").
// Returns nil if the path does not exist.
func (t *Tree) ListDir(p string) []DirEntry {
	return t.dirs[path.Clean(p)]
}

// Lookup resolves an absolute path to a File.
// If the path identifies a file entry, the File is returned.
// Returns nil for directories or non-existent paths.
func (t *Tree) Lookup(p string) *File {
	clean := path.Clean(p)
	dir := path.Dir(clean)
	name := path.Base(clean)
	for _, e := range t.dirs[dir] {
		if e.Name == name && e.File != nil {
			return e.File
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// hashRe matches strings that look like hex hashes: 16 or more consecutive
// hex characters.
var hashRe = regexp.MustCompile(`^[0-9a-fA-F]{16,}$`)

// seasonRe extracts the season number from season/episode patterns in filenames.
var seasonRe = regexp.MustCompile(`(?i)\bS(\d{1,2})E\d{1,2}\b|\bS(\d{1,2})\b`)

// addDownload maps all video files from a single Download into the tree.
func (t *Tree) addDownload(d *Download) {
	title := downloadTitle(d)

	for i := range d.Files {
		f := &d.Files[i]
		if !strings.HasPrefix(f.MimeType, "video/") {
			continue
		}

		mt := f.MediaType
		if mt == "" {
			mt = MediaMovie
		}
		// Anime maps to series path.
		if mt == MediaAnime {
			mt = MediaSeries
		}

		filename := path.Base(f.Name)

		var dirPath string
		switch mt {
		case MediaSeries:
			season := extractSeason(f.Name)
			dirPath = path.Join("/series", title, "Season "+strconv.Itoa(season))
		default: // MediaMovie or anything else
			dirPath = path.Join("/movies", title)
		}

		t.dirs[dirPath] = append(t.dirs[dirPath], DirEntry{
			Name: filename,
			File: f,
		})
	}
}

// buildParentDirs populates intermediate directory entries (virtual folders)
// so that ListDir can walk the tree from the root.
func (t *Tree) buildParentDirs() {
	// Collect all directories we need to register as entries in their parents.
	// We iterate in a loop because adding parent dirs may create new parents
	// that also need to be registered.
	seen := make(map[string]bool)
	for {
		var additions []struct {
			parent string
			entry  DirEntry
		}
		for p := range t.dirs {
			if seen[p] {
				continue
			}
			seen[p] = true
			parent := path.Dir(p)
			if parent == p {
				continue // root, skip
			}
			// Check if this parent already has an entry for the child directory name.
			childName := path.Base(p)
			found := false
			for _, e := range t.dirs[parent] {
				if e.Name == childName && e.File == nil {
					found = true
					break
				}
			}
			if !found {
				additions = append(additions, struct {
					parent string
					entry  DirEntry
				}{
					parent: parent,
					entry:  DirEntry{Name: childName},
				})
			}
		}
		if len(additions) == 0 {
			break
		}
		for _, a := range additions {
			t.dirs[a.parent] = append(t.dirs[a.parent], a.entry)
		}
	}
}

// downloadTitle determines the display title for a download.
// If the download name looks like a hex hash (16+ hex characters), the first
// path segment from the first file's Name is used instead (preserving dots).
// Otherwise dots and underscores in the name are replaced with spaces.
func downloadTitle(d *Download) string {
	if hashRe.MatchString(d.Name) && len(d.Files) > 0 {
		if seg := firstPathSegment(d.Files[0].Name); seg != "" {
			return seg
		}
	}
	return cleanTitle(d.Name)
}

// firstPathSegment returns the first non-empty segment of a path, unmodified.
func firstPathSegment(p string) string {
	for _, seg := range strings.Split(p, "/") {
		seg = strings.TrimSpace(seg)
		if seg != "" {
			return seg
		}
	}
	return ""
}

// cleanTitle replaces dots and underscores with spaces and trims whitespace.
func cleanTitle(s string) string {
	s = strings.ReplaceAll(s, ".", " ")
	s = strings.ReplaceAll(s, "_", " ")
	return strings.TrimSpace(s)
}

// extractSeason parses a season number from a filename.
// It looks for S01E02 or S01 patterns. Returns 1 as default if no pattern is found.
func extractSeason(filename string) int {
	m := seasonRe.FindStringSubmatch(filename)
	if m != nil {
		for i := 1; i < len(m); i++ {
			if m[i] != "" {
				n, err := strconv.Atoi(m[i])
				if err == nil && n > 0 {
					return n
				}
			}
		}
	}
	return 1
}