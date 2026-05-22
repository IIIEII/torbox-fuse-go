// Package catalog defines domain types and classification logic for
// TorBox cached files that are mounted as a local FUSE filesystem.
package catalog

// DownloadKind identifies the source of a download.
type DownloadKind string

const (
	KindTorrent DownloadKind = "torrent"
	KindUsenet  DownloadKind = "usenet"
	KindWebDL   DownloadKind = "webdl"
)

// MediaType classifies a file's content category.
type MediaType string

const (
	MediaMovie  MediaType = "movie"
	MediaSeries MediaType = "series"
	MediaAnime  MediaType = "anime"
)

// Download represents a top-level TorBox download that contains one or more files.
type Download struct {
	Kind  DownloadKind
	ID    string
	Name  string
	Hash  string
	Files []File
}

// File represents a single file within a download.
type File struct {
	DownloadKind DownloadKind
	DownloadID   string
	FileID       string
	Name         string
	Size         int64
	MimeType     string
	MediaType    MediaType
}

// ContentKey returns a unique, stable identifier for a file within the
// filesystem. It combines the download kind, download ID, and file ID.
func (f *File) ContentKey() string {
	return string(f.DownloadKind) + ":" + f.DownloadID + ":" + f.FileID
}