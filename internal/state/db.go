// Package state provides SQLite-backed persistence for stable inode
// assignments and file metadata. Stable inodes prevent Plex and similar
// media scanners from re-scanning unchanged files across restarts.
package state

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

// FileRecord holds metadata for a single file in the TorBox cache.
type FileRecord struct {
	ContentKey   string
	DownloadKind string
	DownloadID   string
	FileID       string
	Path         string
	Size         int64
}

// DB wraps an SQLite connection for inode and file metadata persistence.
type DB struct {
	mu   sync.Mutex
	conn *sql.DB

	// nextInode is the next available inode number. It is initialised
	// from the max(inode) in the inodes table on Open and incremented
	// under mu.
	nextInode uint64
}

// Open creates or opens an SQLite database at path, sets WAL journal mode,
// ensures the required tables and indexes exist, and initialises the
// next-inode counter from existing data.
func Open(path string) (*DB, error) {
	conn, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("state: open db: %w", err)
	}

	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("state: ping db: %w", err)
	}

	// Create tables.
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS inodes (
			content_key TEXT PRIMARY KEY,
			inode       INTEGER NOT NULL,
			path        TEXT NOT NULL,
			updated_at  TEXT NOT NULL
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_inode ON inodes(inode)`,
		`CREATE TABLE IF NOT EXISTS files (
			content_key   TEXT PRIMARY KEY,
			download_kind TEXT NOT NULL,
			download_id   TEXT NOT NULL,
			file_id       TEXT NOT NULL,
			path          TEXT NOT NULL,
			size          INTEGER NOT NULL,
			updated_at    TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS hidden_files (
			content_key TEXT PRIMARY KEY,
			hidden_at  TEXT NOT NULL
		)`,
	}
	for _, stmt := range ddl {
		if _, err := conn.Exec(stmt); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("state: exec ddl: %w", err)
		}
	}

	db := &DB{conn: conn}

	// Initialise nextInode from existing data.
	var maxInode sql.NullInt64
	row := conn.QueryRow(`SELECT COALESCE(MAX(inode), 0) FROM inodes`)
	if err := row.Scan(&maxInode); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("state: scan max inode: %w", err)
	}
	db.nextInode = uint64(maxInode.Int64) + 1

	return db, nil
}

// AssignInode returns the existing inode for contentKey if one exists,
// otherwise allocates the next available inode number and persists it.
func (db *DB) AssignInode(contentKey, path string) (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check for existing inode.
	var inode uint64
	err := db.conn.QueryRow(
		`SELECT inode FROM inodes WHERE content_key = ?`, contentKey,
	).Scan(&inode)
	if err == nil {
		return inode, nil
	}
	if err != sql.ErrNoRows {
		return 0, fmt.Errorf("state: query inode: %w", err)
	}

	// Allocate new inode.
	inode = db.nextInode
	db.nextInode++

	now := sql.NullString{}
	// Use current timestamp if available, otherwise empty string.
	row := db.conn.QueryRow(`SELECT strftime('%Y-%m-%dT%H:%M:%SZ', 'now')`)
	if err := row.Scan(&now); err != nil {
		now.String = ""
	}

	_, err = db.conn.Exec(
		`INSERT INTO inodes (content_key, inode, path, updated_at) VALUES (?, ?, ?, ?)`,
		contentKey, inode, path, now,
	)
	if err != nil {
		return 0, fmt.Errorf("state: insert inode: %w", err)
	}

	return inode, nil
}

// LookupInode returns the inode number for the given content key.
// Returns an error if the content key is not found.
func (db *DB) LookupInode(contentKey string) (uint64, error) {
	var inode uint64
	err := db.conn.QueryRow(
		`SELECT inode FROM inodes WHERE content_key = ?`, contentKey,
	).Scan(&inode)
	if err != nil {
		return 0, fmt.Errorf("state: lookup inode for %q: %w", contentKey, err)
	}
	return inode, nil
}

// UpsertFiles performs a bulk upsert of file records using INSERT OR REPLACE.
func (db *DB) UpsertFiles(files []FileRecord) error {
	if len(files) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("state: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO files
			(content_key, download_kind, download_id, file_id, path, size, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
	`)
	if err != nil {
		return fmt.Errorf("state: prepare upsert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, f := range files {
		if _, err := stmt.Exec(
			f.ContentKey, f.DownloadKind, f.DownloadID,
			f.FileID, f.Path, f.Size,
		); err != nil {
			return fmt.Errorf("state: upsert file %q: %w", f.ContentKey, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit upsert: %w", err)
	}
	return nil
}

// LookupFile returns file metadata for the given content key.
// Returns nil if the content key is not found.
func (db *DB) LookupFile(contentKey string) (*FileRecord, error) {
	var f FileRecord
	err := db.conn.QueryRow(
		`SELECT content_key, download_kind, download_id, file_id, path, size
		 FROM files WHERE content_key = ?`, contentKey,
	).Scan(&f.ContentKey, &f.DownloadKind, &f.DownloadID,
		&f.FileID, &f.Path, &f.Size)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("state: lookup file for %q: %w", contentKey, err)
	}
	return &f, nil
}

// ListFiles returns all file records from the database.
// Returns an empty slice (not error) if no records exist.
func (db *DB) ListFiles() ([]FileRecord, error) {
	rows, err := db.conn.Query(
		`SELECT content_key, download_kind, download_id, file_id, path, size FROM files`,
	)
	if err != nil {
		return nil, fmt.Errorf("state: list files: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var files []FileRecord
	for rows.Next() {
		var f FileRecord
		if err := rows.Scan(&f.ContentKey, &f.DownloadKind, &f.DownloadID,
			&f.FileID, &f.Path, &f.Size); err != nil {
			return nil, fmt.Errorf("state: scan file: %w", err)
		}
		files = append(files, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: list files rows: %w", err)
	}
	return files, nil
}

// HideFile marks a file as hidden so it will be excluded from the FUSE tree.
func (db *DB) HideFile(contentKey string) error {
	_, err := db.conn.Exec(
		`INSERT OR IGNORE INTO hidden_files (content_key, hidden_at) VALUES (?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))`,
		contentKey,
	)
	if err != nil {
		return fmt.Errorf("state: hide file %q: %w", contentKey, err)
	}
	return nil
}

// UnhideFile removes a file from the hidden list so it reappears in the FUSE tree.
func (db *DB) UnhideFile(contentKey string) error {
	_, err := db.conn.Exec(`DELETE FROM hidden_files WHERE content_key = ?`, contentKey)
	if err != nil {
		return fmt.Errorf("state: unhide file %q: %w", contentKey, err)
	}
	return nil
}

// UnhideDownload removes all hidden-file entries for a given download.
func (db *DB) UnhideDownload(downloadKind, downloadID string) error {
	prefix := downloadKind + ":" + downloadID + ":"
	_, err := db.conn.Exec(`DELETE FROM hidden_files WHERE content_key LIKE ?`, prefix+"%")
	if err != nil {
		return fmt.Errorf("state: unhide download %s:%s: %w", downloadKind, downloadID, err)
	}
	return nil
}

// IsHidden checks whether a file is in the hidden list.
func (db *DB) IsHidden(contentKey string) (bool, error) {
	var count int
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM hidden_files WHERE content_key = ?`, contentKey).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("state: is hidden %q: %w", contentKey, err)
	}
	return count > 0, nil
}

// IsDownloadFullyHidden checks whether all video files of a download are hidden.
// It compares the count of hidden files for a download against the total file count
// in the files table.
func (db *DB) IsDownloadFullyHidden(downloadKind, downloadID string) (bool, error) {
	prefix := downloadKind + ":" + downloadID + ":"
	var hiddenCount int
	err := db.conn.QueryRow(
		`SELECT COUNT(*) FROM hidden_files WHERE content_key LIKE ?`, prefix+"%",
	).Scan(&hiddenCount)
	if err != nil {
		return false, fmt.Errorf("state: count hidden for %s:%s: %w", downloadKind, downloadID, err)
	}
	var totalCount int
	err = db.conn.QueryRow(
		`SELECT COUNT(*) FROM files WHERE download_kind = ? AND download_id = ?`,
		downloadKind, downloadID,
	).Scan(&totalCount)
	if err != nil {
		return false, fmt.Errorf("state: count total for %s:%s: %w", downloadKind, downloadID, err)
	}
	return totalCount > 0 && hiddenCount >= totalCount, nil
}

// ListHiddenFiles returns all file records that are currently hidden.
func (db *DB) ListHiddenFiles() ([]FileRecord, error) {
	rows, err := db.conn.Query(`
		SELECT f.content_key, f.download_kind, f.download_id, f.file_id, f.path, f.size
		FROM files f
		JOIN hidden_files h ON f.content_key = h.content_key
		ORDER BY h.hidden_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("state: list hidden files: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var files []FileRecord
	for rows.Next() {
		var f FileRecord
		if err := rows.Scan(&f.ContentKey, &f.DownloadKind, &f.DownloadID,
			&f.FileID, &f.Path, &f.Size); err != nil {
			return nil, fmt.Errorf("state: scan hidden file: %w", err)
		}
		files = append(files, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: list hidden files rows: %w", err)
	}
	return files, nil
}

// HiddenDownload represents a download that has some or all of its files hidden.
type HiddenDownload struct {
	DownloadKind string
	DownloadID   string
	DownloadName string // derived from first file's path
	HiddenCount  int
	TotalCount   int
	TotalSize    int64
}

// ListHiddenDownloads returns downloads that have at least one hidden file,
// grouped by download. This is used by the dashboard to show which downloads
// are partially or fully hidden.
func (db *DB) ListHiddenDownloads() ([]HiddenDownload, error) {
	rows, err := db.conn.Query(`
		SELECT
			f.download_kind,
			f.download_id,
			COUNT(CASE WHEN h.content_key IS NOT NULL THEN 1 END) AS hidden_count,
			COUNT(*) AS total_count,
			SUM(f.size) AS total_size,
			MIN(f.path) AS first_path
		FROM files f
		LEFT JOIN hidden_files h ON f.content_key = h.content_key
		GROUP BY f.download_kind, f.download_id
		HAVING hidden_count > 0
		ORDER BY f.download_kind, f.download_id
	`)
	if err != nil {
		return nil, fmt.Errorf("state: list hidden downloads: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var downloads []HiddenDownload
	for rows.Next() {
		var d HiddenDownload
		var firstPath string
		if err := rows.Scan(&d.DownloadKind, &d.DownloadID, &d.HiddenCount, &d.TotalCount, &d.TotalSize, &firstPath); err != nil {
			return nil, fmt.Errorf("state: scan hidden download: %w", err)
		}
		// Derive download name from first file's path: "/movies/The Matrix/film.mkv" → "The Matrix"
		parts := strings.SplitN(firstPath, "/", 4)
		if len(parts) >= 3 {
			d.DownloadName = parts[2]
		} else {
			d.DownloadName = firstPath
		}
		downloads = append(downloads, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: list hidden downloads rows: %w", err)
	}
	return downloads, nil
}

// HiddenSet returns the set of all hidden content keys as a map for fast
// lookup. Returns an empty map (not nil) if no files are hidden.
func (db *DB) HiddenSet() (map[string]bool, error) {
	rows, err := db.conn.Query(`SELECT content_key FROM hidden_files`)
	if err != nil {
		return nil, fmt.Errorf("state: hidden set: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]bool)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("state: scan hidden key: %w", err)
		}
		result[key] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: hidden set rows: %w", err)
	}
	return result, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}
