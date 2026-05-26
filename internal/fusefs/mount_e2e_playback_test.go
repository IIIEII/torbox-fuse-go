//go:build !short

package fusefs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/catalog"
	"github.com/iiieii/torbox-fuse-go/internal/config"
	"github.com/iiieii/torbox-fuse-go/internal/state"
	"github.com/iiieii/torbox-fuse-go/internal/stream"
	"github.com/iiieii/torbox-fuse-go/internal/torbox"
)

// Phase 8: E2E playback tests (spec §15)

// mountFUSEForTest sets up a complete FUSE mount with mock CDN for testing.
// Returns the mount directory and a cleanup function.
func mountFUSEForTest(t *testing.T, dataSize int, cdnHandler http.HandlerFunc) (string, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping FUSE mount e2e test in short mode")
	}
	if os.Getuid() != 0 {
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs"); err != nil && os.Getenv("FUSE_T") == "" {
			t.Skip("skipping: no FUSE driver found (macFUSE or FUSE-T required)")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	cdnServer := httptest.NewServer(cdnHandler)

	tree := catalog.BuildTree([]catalog.Download{
		{
			Kind: catalog.KindTorrent,
			ID:   "100",
			Name: "Test.Movie.2024",
			Hash: "abc123def456",
			Files: []catalog.File{
				{
					DownloadKind: catalog.KindTorrent,
					DownloadID:   "100",
					FileID:       "200",
					Name:         "Test.Movie.2024/Test.Movie.2024.mkv",
					Size:         int64(dataSize),
					MimeType:     "video/x-matroska",
					MediaType:    catalog.MediaMovie,
				},
			},
		},
	})

	stateDir := t.TempDir()
	db, err := state.Open(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}

	rc := cache.NewRangeCache(256 * 1024 * 1024, nil)
	cdnClient := stream.NewCDNClient(8, nil, 0)
	permalinkBuilder := func(fileKey string) string {
		return cdnServer.URL
	}
	streamer := stream.NewStreamReader(rc, cdnClient, 2, int64(16*1024*1024), permalinkBuilder, nil)

	cfg := &config.Config{
		APIKey:            "test-api-key",
		APIBaseURL:        "http://localhost:0",
		CacheBudgetMB:     256,
		PrefetchWindowMB:  16,
		StreamMaxInflight: 2,
		StreamConcurrency: 8,
		AttrTimeoutSec:   1,
		EntryTimeoutSec:  1,
		UID:              uint32(os.Getuid()),
		GID:              uint32(os.Getgid()),
	}

	tbClient := torbox.NewClient(&e2eTorboxConfig{apiKey: "test", baseURL: "http://localhost:0"})
	root := NewRootNode(tree, db, streamer, cfg, tbClient)

	mountDir := t.TempDir()
	cfg.MountPath = mountDir

	attrTimeout := time.Duration(cfg.AttrTimeoutSec) * time.Second
	entryTimeout := time.Duration(cfg.EntryTimeoutSec) * time.Second
	negTimeout := time.Duration(0)

	server, err := fs.Mount(mountDir, root, &fs.Options{
		AttrTimeout:     &attrTimeout,
		EntryTimeout:    &entryTimeout,
		NegativeTimeout: &negTimeout,
		MountOptions: fuse.MountOptions{
			MaxReadAhead: 4 << 20,
			FsName:       "torbox-media-center",
			Debug:        false,
		},
	})
	if err != nil {
		db.Close()
		cdnServer.Close()
		t.Fatalf("fs.Mount(%s): %v", mountDir, err)
	}

	if err := waitForMount(t, ctx, mountDir); err != nil {
		server.Unmount()
		db.Close()
		cdnServer.Close()
		cancel()
		t.Fatalf("mount did not become ready: %v", err)
	}

	cleanup := func() {
		server.Unmount()
		done := make(chan struct{})
		go func() {
			server.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Log("timeout waiting for server.Wait after unmount")
		}
		db.Close()
		cdnServer.Close()
		cancel()
	}

	return mountDir, cleanup
}

// movieFilePath returns the path to the test movie file within the mount.
func movieFilePath(mountDir string) string {
	return filepath.Join(mountDir, "movies", "Test Movie 2024", "Test.Movie.2024.mkv")
}

// 8.1 Playback start from middle: seek to 50% offset via FUSE, read data,
// verify SHA256 of read portion.
func TestE2E_PlaybackFromMiddle(t *testing.T) {
	const dataSize = 4 * 1024 * 1024

	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	mountDir, cleanup := mountFUSEForTest(t, dataSize, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
			return
		}
		var start, end int
		n, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if err != nil || n != 2 {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if start >= len(testData) {
			http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= len(testData) {
			end = len(testData) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	})
	defer cleanup()

	filePath := movieFilePath(mountDir)

	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Open(%s): %v", filePath, err)
	}
	defer f.Close()

	// Seek to 50%
	midOffset := int64(dataSize / 2)
	_, err = f.Seek(midOffset, 0)
	if err != nil {
		t.Fatalf("Seek(%d): %v", midOffset, err)
	}

	// Read 128 KiB from middle
	readSize := 128 * 1024
	buf := make([]byte, readSize)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != readSize {
		t.Errorf("read %d bytes, want %d", n, readSize)
	}

	// Verify data correctness
	for i := 0; i < n; i++ {
		if buf[i] != byte((midOffset+int64(i))%256) {
			t.Errorf("data mismatch at offset %d: got 0x%02x, want 0x%02x",
				midOffset+int64(i), buf[i], byte((midOffset+int64(i))%256))
			break
		}
	}
}

// 8.2 Seek/scrub simulation: read from start, then seek to middle, then seek
// to near-end, verify correct data at each position.
func TestE2E_SeekScrubSimulation(t *testing.T) {
	const dataSize = 4 * 1024 * 1024

	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	mountDir, cleanup := mountFUSEForTest(t, dataSize, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
			return
		}
		var start, end int
		n, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if err != nil || n != 2 {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if start >= len(testData) {
			http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= len(testData) {
			end = len(testData) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	})
	defer cleanup()

	filePath := movieFilePath(mountDir)

	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Open(%s): %v", filePath, err)
	}
	defer f.Close()

	const chunkSize = 4096
	buf := make([]byte, chunkSize)

	// Read from start
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read at start: %v", err)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte(i%256) {
			t.Fatalf("data mismatch at start offset %d", i)
		}
	}

	// Seek to middle
	midOffset := int64(dataSize / 2)
	_, err = f.Seek(midOffset, 0)
	if err != nil {
		t.Fatalf("Seek to middle: %v", err)
	}
	n, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read at middle: %v", err)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte((midOffset+int64(i))%256) {
			t.Fatalf("data mismatch at middle offset %d", midOffset+int64(i))
		}
	}

	// Seek to near-end
	endOffset := int64(dataSize - chunkSize)
	_, err = f.Seek(endOffset, 0)
	if err != nil {
		t.Fatalf("Seek to near-end: %v", err)
	}
	n, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read at near-end: %v", err)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte((endOffset+int64(i))%256) {
			t.Fatalf("data mismatch at near-end offset %d", endOffset+int64(i))
		}
	}
}

// 8.3 Sustained playback: read entire file in 128 KiB chunks sequentially,
// verify no errors and correct hash.
func TestE2E_SustainedPlayback(t *testing.T) {
	const dataSize = 4 * 1024 * 1024

	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	expectedHash := sha256.Sum256(testData)

	mountDir, cleanup := mountFUSEForTest(t, dataSize, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
			return
		}
		var start, end int
		n, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if err != nil || n != 2 {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if start >= len(testData) {
			http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= len(testData) {
			end = len(testData) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[start : end+1])
	})
	defer cleanup()

	filePath := movieFilePath(mountDir)

	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Open(%s): %v", filePath, err)
	}
	defer f.Close()

	hasher := sha256.New()
	const chunkSize = 128 * 1024
	buf := make([]byte, chunkSize)

	totalRead := 0
	for {
		n, err := f.Read(buf)
		if n > 0 {
			hasher.Write(buf[:n])
			totalRead += n
		}
		if err != nil {
			break
		}
	}

	if totalRead != dataSize {
		t.Errorf("read %d bytes, want %d", totalRead, dataSize)
	}

	gotHash := sha256.Sum256(nil)
	hasher.Sum(gotHash[:0])
	// Re-compute since Sum256 appends
	var gotHashArr [32]byte
	copy(gotHashArr[:], hasher.Sum(nil))

	if gotHashArr != expectedHash {
		t.Errorf("SHA256 mismatch after sustained playback\ngot:  %x\nwant: %x", gotHashArr, expectedHash)
	}
}