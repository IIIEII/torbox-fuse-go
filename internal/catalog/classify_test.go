package catalog

import "testing"

func TestClassifyMediaType_TagMovie(t *testing.T) {
	got := ClassifyMediaType([]string{"type=movie"}, "any.mkv", MediaMovie)
	if got != MediaMovie {
		t.Errorf("ClassifyMediaType with type=movie tag: got %q, want %q", got, MediaMovie)
	}
}

func TestClassifyMediaType_TagSeries(t *testing.T) {
	got := ClassifyMediaType([]string{"type=series"}, "any.mkv", MediaMovie)
	if got != MediaSeries {
		t.Errorf("ClassifyMediaType with type=series tag: got %q, want %q", got, MediaSeries)
	}
}

func TestClassifyMediaType_TagAnime(t *testing.T) {
	got := ClassifyMediaType([]string{"type=anime"}, "any.mkv", MediaMovie)
	if got != MediaSeries {
		t.Errorf("ClassifyMediaType with type=anime tag: got %q, want %q", got, MediaSeries)
	}
}

func TestClassifyMediaType_TagCaseInsensitive(t *testing.T) {
	got := ClassifyMediaType([]string{"Type=Movie"}, "any.mkv", MediaSeries)
	if got != MediaMovie {
		t.Errorf("ClassifyMediaType case-insensitive tag: got %q, want %q", got, MediaMovie)
	}
}

func TestClassifyMediaType_NoTag_SeasonEpisode(t *testing.T) {
	got := ClassifyMediaType(nil, "Show.Name.S01E02.1080p.mkv", MediaMovie)
	if got != MediaSeries {
		t.Errorf("ClassifyMediaType S01E02 pattern: got %q, want %q", got, MediaSeries)
	}
}

func TestClassifyMediaType_NoTag_SeasonOnly(t *testing.T) {
	got := ClassifyMediaType(nil, "Show.Name.S02.720p.mkv", MediaMovie)
	if got != MediaSeries {
		t.Errorf("ClassifyMediaType S02 pattern: got %q, want %q", got, MediaSeries)
	}
}

func TestClassifyMediaType_NoTag_EpisodeOnly(t *testing.T) {
	got := ClassifyMediaType(nil, "Show.E05.720p.mkv", MediaMovie)
	if got != MediaSeries {
		t.Errorf("ClassifyMediaType E05 pattern: got %q, want %q", got, MediaSeries)
	}
}

func TestClassifyMediaType_NoTag_NoPattern(t *testing.T) {
	got := ClassifyMediaType(nil, "A.Great.Movie.2024.mkv", MediaMovie)
	if got != MediaMovie {
		t.Errorf("ClassifyMediaType fallback: got %q, want %q", got, MediaMovie)
	}
}

func TestClassifyMediaType_NoTag_FallbackSeries(t *testing.T) {
	got := ClassifyMediaType(nil, "Some.File.mkv", MediaSeries)
	if got != MediaSeries {
		t.Errorf("ClassifyMediaType fallback to series: got %q, want %q", got, MediaSeries)
	}
}

func TestContentKey(t *testing.T) {
	f := File{
		DownloadKind: KindTorrent,
		DownloadID:   "12345",
		FileID:       "67890",
	}
	want := "torrent:12345:67890"
	got := f.ContentKey()
	if got != want {
		t.Errorf("ContentKey(): got %q, want %q", got, want)
	}
}