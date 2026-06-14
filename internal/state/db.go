// Package state provides SQLite-backed persistence for stable inode
// assignments and file metadata. Stable inodes prevent Plex and similar
// media scanners from re-scanning unchanged files across restarts.
package state

import (
	"database/sql"
	"fmt"
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
	defer rows.Close()

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

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}
