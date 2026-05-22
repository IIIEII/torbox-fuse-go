package catalog

import (
	"testing"
)

// --- BuildTree tests ---

func TestBuildTree_MovieFiles(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "100",
			Name: "The Matrix",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "100",
					FileID:       "1",
					Name:         "The.Matrix.1999.1080p.mkv",
					Size:         2048,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// /movies/ should list "The Matrix"
	entries := tree.ListDir("/movies")
	if len(entries) != 1 {
		t.Fatalf("ListDir(/movies): got %d entries, want 1", len(entries))
	}
	if entries[0].Name != "The Matrix" {
		t.Errorf("ListDir(/movies): got %q, want %q", entries[0].Name, "The Matrix")
	}

	// /movies/The Matrix/ should list the file
	entries = tree.ListDir("/movies/The Matrix")
	if len(entries) != 1 {
		t.Fatalf("ListDir(/movies/The Matrix): got %d entries, want 1", len(entries))
	}
	if entries[0].Name != "The.Matrix.1999.1080p.mkv" {
		t.Errorf("ListDir(/movies/The Matrix): got %q, want %q", entries[0].Name, "The.Matrix.1999.1080p.mkv")
	}
	if entries[0].File == nil {
		t.Fatal("File entry should have non-nil File pointer")
	}
}

func TestBuildTree_SeriesFiles(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "200",
			Name: "Breaking Bad",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "200",
					FileID:       "2",
					Name:         "Breaking.Bad.S01E01.720p.mkv",
					Size:         1024,
					MimeType:     "video/x-matroska",
					MediaType:    MediaSeries,
				},
				{
					DownloadKind: KindTorrent,
					DownloadID:   "200",
					FileID:       "3",
					Name:         "Breaking.Bad.S02E05.720p.mkv",
					Size:         1024,
					MimeType:     "video/x-matroska",
					MediaType:    MediaSeries,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// /series/ should list "Breaking Bad"
	entries := tree.ListDir("/series")
	if len(entries) != 1 {
		t.Fatalf("ListDir(/series): got %d entries, want 1", len(entries))
	}
	if entries[0].Name != "Breaking Bad" {
		t.Errorf("ListDir(/series): got %q, want %q", entries[0].Name, "Breaking Bad")
	}

	// /series/Breaking Bad/ should list Season 1 and Season 2
	entries = tree.ListDir("/series/Breaking Bad")
	if len(entries) != 2 {
		t.Fatalf("ListDir(/series/Breaking Bad): got %d entries, want 2", len(entries))
	}
	if entries[0].Name != "Season 1" {
		t.Errorf("entries[0].Name: got %q, want %q", entries[0].Name, "Season 1")
	}
	if entries[1].Name != "Season 2" {
		t.Errorf("entries[1].Name: got %q, want %q", entries[1].Name, "Season 2")
	}

	// /series/Breaking Bad/Season 1/ should list the file
	entries = tree.ListDir("/series/Breaking Bad/Season 1")
	if len(entries) != 1 {
		t.Fatalf("ListDir(Season 1): got %d entries, want 1", len(entries))
	}
	if entries[0].Name != "Breaking.Bad.S01E01.720p.mkv" {
		t.Errorf("Season 1 entry: got %q, want %q", entries[0].Name, "Breaking.Bad.S01E01.720p.mkv")
	}
}

func TestBuildTree_AnimeGoesToSeries(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "300",
			Name: "Attack on Titan",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "300",
					FileID:       "4",
					Name:         "Shingeki.no.Kyojin.S01E01.mkv",
					Size:         512,
					MimeType:     "video/x-matroska",
					MediaType:    MediaAnime,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// Anime should go into /series/, not /anime/
	entries := tree.ListDir("/series")
	if len(entries) != 1 {
		t.Fatalf("ListDir(/series): got %d entries, want 1", len(entries))
	}
	if entries[0].Name != "Attack on Titan" {
		t.Errorf("ListDir(/series): got %q, want %q", entries[0].Name, "Attack on Titan")
	}
}

func TestBuildTree_HashNameUsesFirstPathSegment(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "400",
			Name: "a1b2c3d4e5f6a7b8",
			Hash: "a1b2c3d4e5f6a7b8",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "400",
					FileID:       "5",
					Name:         "Great.Movie/Great.Movie.2024.mkv",
					Size:         4096,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// When name looks like a hash, use first path segment from filename as title
	entries := tree.ListDir("/movies")
	if len(entries) != 1 {
		t.Fatalf("ListDir(/movies): got %d entries, want 1", len(entries))
	}
	if entries[0].Name != "Great.Movie" {
		t.Errorf("Hash name fallback: got %q, want %q", entries[0].Name, "Great.Movie")
	}
}

func TestBuildTree_FilterNonVideoFiles(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "500",
			Name: "Some Movie",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "500",
					FileID:       "6",
					Name:         "movie.mkv",
					Size:         2048,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
				{
					DownloadKind: KindTorrent,
					DownloadID:   "500",
					FileID:       "7",
					Name:         "subtitle.srt",
					Size:         50,
					MimeType:     "application/x-subrip",
					MediaType:    MediaMovie,
				},
				{
					DownloadKind: KindTorrent,
					DownloadID:   "500",
					FileID:       "8",
					Name:         "poster.jpg",
					Size:         100,
					MimeType:     "image/jpeg",
					MediaType:    MediaMovie,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// Only video files should appear
	entries := tree.ListDir("/movies/Some Movie")
	if len(entries) != 1 {
		t.Fatalf("ListDir(/movies/Some Movie): got %d entries, want 1 (only video)", len(entries))
	}
	if entries[0].Name != "movie.mkv" {
		t.Errorf("Filtered entry: got %q, want %q", entries[0].Name, "movie.mkv")
	}
}

func TestBuildTree_SortedEntries(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "600",
			Name: "Collection",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "600",
					FileID:       "a",
					Name:         "Z-File.mkv",
					Size:         100,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
				{
					DownloadKind: KindTorrent,
					DownloadID:   "600",
					FileID:       "b",
					Name:         "A-File.mkv",
					Size:         200,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
				{
					DownloadKind: KindTorrent,
					DownloadID:   "600",
					FileID:       "c",
					Name:         "M-File.mkv",
					Size:         300,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	entries := tree.ListDir("/movies/Collection")
	if len(entries) != 3 {
		t.Fatalf("ListDir: got %d entries, want 3", len(entries))
	}
	if entries[0].Name != "A-File.mkv" {
		t.Errorf("entries[0]: got %q, want %q", entries[0].Name, "A-File.mkv")
	}
	if entries[1].Name != "M-File.mkv" {
		t.Errorf("entries[1]: got %q, want %q", entries[1].Name, "M-File.mkv")
	}
	if entries[2].Name != "Z-File.mkv" {
		t.Errorf("entries[2]: got %q, want %q", entries[2].Name, "Z-File.mkv")
	}
}

func TestBuildTree_UntaggedDefaultsToMovie(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "700",
			Name: "Mystery Film",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "700",
					FileID:       "9",
					Name:         "mystery.mkv",
					Size:         2048,
					MimeType:     "video/x-matroska",
					MediaType:    "", // untagged
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// Untagged files should default to movie
	entries := tree.ListDir("/movies")
	if len(entries) != 1 {
		t.Fatalf("ListDir(/movies): got %d entries, want 1", len(entries))
	}
	if entries[0].Name != "Mystery Film" {
		t.Errorf("Untagged file: got %q, want %q", entries[0].Name, "Mystery Film")
	}
}

func TestBuildTree_DefaultSeasonIsOne(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "800",
			Name: "A Show",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "800",
					FileID:       "10",
					Name:         "A.Show.E01.720p.mkv",
					Size:         1024,
					MimeType:     "video/x-matroska",
					MediaType:    MediaSeries,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// E01 without S01 should default to Season 1
	entries := tree.ListDir("/series/A Show")
	if len(entries) != 1 {
		t.Fatalf("ListDir(/series/A Show): got %d entries, want 1", len(entries))
	}
	if entries[0].Name != "Season 1" {
		t.Errorf("Default season: got %q, want %q", entries[0].Name, "Season 1")
	}
}

func TestBuildTree_NestedFilepath(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "900",
			Name: "Some Show",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "900",
					FileID:       "11",
					Name:         "Season 2/Some.Show.S02E03.mkv",
					Size:         1024,
					MimeType:     "video/x-matroska",
					MediaType:    MediaSeries,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// File should be placed under /series/<title>/Season <N>/
	// Season is extracted from the S02E03 pattern, not from the directory in the name
	entries := tree.ListDir("/series/Some Show/Season 2")
	if len(entries) != 1 {
		t.Fatalf("ListDir(Season 2): got %d entries, want 1", len(entries))
	}
	// The filename stored should be just the base name
	if entries[0].Name != "Some.Show.S02E03.mkv" {
		t.Errorf("File name: got %q, want %q", entries[0].Name, "Some.Show.S02E03.mkv")
	}
}

// --- extractSeason tests ---

func TestExtractSeason_S01E02(t *testing.T) {
	got := extractSeason("Show.Name.S01E02.1080p.mkv")
	if got != 1 {
		t.Errorf("extractSeason(S01E02): got %d, want 1", got)
	}
}

func TestExtractSeason_S12(t *testing.T) {
	got := extractSeason("Show.S12.720p.mkv")
	if got != 12 {
		t.Errorf("extractSeason(S12): got %d, want 12", got)
	}
}

func TestExtractSeason_E05_DefaultsToOne(t *testing.T) {
	got := extractSeason("Show.E05.720p.mkv")
	if got != 1 {
		t.Errorf("extractSeason(E05): got %d, want 1", got)
	}
}

func TestExtractSeason_NoMatch_DefaultsToOne(t *testing.T) {
	got := extractSeason("movie.2024.mkv")
	if got != 1 {
		t.Errorf("extractSeason(no match): got %d, want 1", got)
	}
}

func TestExtractSeason_S02E05(t *testing.T) {
	got := extractSeason("Show.Name.S02E05.mkv")
	if got != 2 {
		t.Errorf("extractSeason(S02E05): got %d, want 2", got)
	}
}

// --- Lookup tests ---

func TestLookup_MovieFile(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "100",
			Name: "The Matrix",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "100",
					FileID:       "1",
					Name:         "The.Matrix.1999.1080p.mkv",
					Size:         2048,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	f := tree.Lookup("/movies/The Matrix/The.Matrix.1999.1080p.mkv")
	if f == nil {
		t.Fatal("Lookup: expected file, got nil")
	}
	if f.FileID != "1" {
		t.Errorf("Lookup FileID: got %q, want %q", f.FileID, "1")
	}
}

func TestLookup_SeriesFile(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "200",
			Name: "Breaking Bad",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "200",
					FileID:       "2",
					Name:         "Breaking.Bad.S01E01.720p.mkv",
					Size:         1024,
					MimeType:     "video/x-matroska",
					MediaType:    MediaSeries,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	f := tree.Lookup("/series/Breaking Bad/Season 1/Breaking.Bad.S01E01.720p.mkv")
	if f == nil {
		t.Fatal("Lookup: expected file, got nil")
	}
	if f.FileID != "2" {
		t.Errorf("Lookup FileID: got %q, want %q", f.FileID, "2")
	}
}

func TestLookup_NotFound(t *testing.T) {
	tree := BuildTree(nil)

	f := tree.Lookup("/movies/Nonexistent/file.mkv")
	if f != nil {
		t.Errorf("Lookup nonexistent: got %v, want nil", f)
	}
}

func TestLookup_EmptyPath(t *testing.T) {
	tree := BuildTree(nil)

	f := tree.Lookup("")
	if f != nil {
		t.Errorf("Lookup empty path: got %v, want nil", f)
	}
}

// --- ListDir tests ---

func TestListDir_NonexistentPath(t *testing.T) {
	tree := BuildTree(nil)

	entries := tree.ListDir("/nonexistent")
	if entries != nil {
		t.Errorf("ListDir(nonexistent): got %v, want nil", entries)
	}
}

func TestListDir_RootDirectories(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "100",
			Name: "Movie A",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "100",
					FileID:       "1",
					Name:         "a.mkv",
					Size:         100,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
			},
		},
		{
			Kind: KindTorrent,
			ID:   "200",
			Name: "Show B",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "200",
					FileID:       "2",
					Name:         "b.mkv",
					Size:         100,
					MimeType:     "video/x-matroska",
					MediaType:    MediaSeries,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	rootEntries := tree.ListDir("/")
	if len(rootEntries) != 2 {
		t.Fatalf("ListDir(/): got %d entries, want 2", len(rootEntries))
	}
	// Should be sorted: movies before series
	if rootEntries[0].Name != "movies" {
		t.Errorf("root[0]: got %q, want %q", rootEntries[0].Name, "movies")
	}
	if rootEntries[1].Name != "series" {
		t.Errorf("root[1]: got %q, want %q", rootEntries[1].Name, "series")
	}
}

func TestListDir_OnlyMoviesNoSeries(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "100",
			Name: "Movie Only",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "100",
					FileID:       "1",
					Name:         "movie.mkv",
					Size:         100,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	rootEntries := tree.ListDir("/")
	if len(rootEntries) != 1 {
		t.Fatalf("ListDir(/): got %d entries, want 1", len(rootEntries))
	}
	if rootEntries[0].Name != "movies" {
		t.Errorf("root[0]: got %q, want %q", rootEntries[0].Name, "movies")
	}

	// /series should return nil (no series entries)
	seriesEntries := tree.ListDir("/series")
	if seriesEntries != nil {
		t.Errorf("ListDir(/series): got %v, want nil", seriesEntries)
	}
}

func TestBuildTree_MultipleDownloads(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "100",
			Name: "Movie A",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "100",
					FileID:       "1",
					Name:         "Movie.A.mkv",
					Size:         100,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
			},
		},
		{
			Kind: KindTorrent,
			ID:   "200",
			Name: "Show B",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "200",
					FileID:       "2",
					Name:         "Show.B.S01E01.mkv",
					Size:         200,
					MimeType:     "video/x-matroska",
					MediaType:    MediaSeries,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// Both /movies and /series should exist
	rootEntries := tree.ListDir("/")
	if len(rootEntries) != 2 {
		t.Fatalf("ListDir(/): got %d entries, want 2", len(rootEntries))
	}

	// Movie entry
	movieEntries := tree.ListDir("/movies/Movie A")
	if len(movieEntries) != 1 {
		t.Fatalf("ListDir(/movies/Movie A): got %d entries, want 1", len(movieEntries))
	}

	// Series entry
	seriesEntries := tree.ListDir("/series/Show B/Season 1")
	if len(seriesEntries) != 1 {
		t.Fatalf("ListDir(/series/Show B/Season 1): got %d entries, want 1", len(seriesEntries))
	}
}

func TestBuildTree_DirEntryHasNoFileForDirectories(t *testing.T) {
	downloads := []Download{
		{
			Kind: KindTorrent,
			ID:   "100",
			Name: "The Film",
			Files: []File{
				{
					DownloadKind: KindTorrent,
					DownloadID:   "100",
					FileID:       "1",
					Name:         "film.mkv",
					Size:         100,
					MimeType:     "video/x-matroska",
					MediaType:    MediaMovie,
				},
			},
		},
	}

	tree := BuildTree(downloads)

	// Directory entries should have nil File
	moviesEntries := tree.ListDir("/movies")
	if len(moviesEntries) != 1 {
		t.Fatalf("ListDir(/movies): got %d entries, want 1", len(moviesEntries))
	}
	if moviesEntries[0].File != nil {
		t.Error("Directory entry should have nil File pointer")
	}

	titleEntries := tree.ListDir("/movies/The Film")
	if len(titleEntries) != 1 {
		t.Fatalf("ListDir(/movies/The Film): got %d entries, want 1", len(titleEntries))
	}
	if titleEntries[0].File == nil {
		t.Error("File entry should have non-nil File pointer")
	}
}