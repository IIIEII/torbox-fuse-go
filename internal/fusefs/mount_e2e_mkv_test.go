//go:build !short

package fusefs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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

// checkTool looks up a binary in PATH. It calls t.Skipf if the tool is not
// found and returns the absolute path otherwise.
func checkTool(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s not found in PATH, skipping MKV e2e test", name)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("resolving absolute path for %s: %v", name, err)
	}
	return abs
}

// generateMKV runs ffmpeg to produce a minimal MKV file. The caller can pass
// extra ffmpeg arguments (e.g. additional -i or -filter_complex flags).
// Returns the path to the generated file.
func generateMKV(t *testing.T, extraArgs ...string) string {
	t.Helper()

	ffmpeg := checkTool(t, "ffmpeg")

	output := filepath.Join(t.TempDir(), "test.mkv")

	args := []string{
		"-y",
		"-f", "lavfi",
		"-i", "testsrc=duration=5:size=320x240:rate=24",
	}
	args = append(args, extraArgs...)
	args = append(args,
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-f", "matroska",
		output,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, ffmpeg, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("ffmpeg failed: %v", err)
	}

	return output
}

// mountFUSEForMKV creates a FUSE mount backed by a real MKV file on disk.
// The MKV bytes are served through a mock CDN server with Range support.
// Returns the mount directory and a cleanup function.
func mountFUSEForMKV(t *testing.T, mkvPath string) (string, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping FUSE mount e2e test in short mode")
	}
	if os.Getuid() != 0 {
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs"); err != nil && os.Getenv("FUSE_T") == "" {
			t.Skip("skipping: no FUSE driver found (macFUSE or FUSE-T required)")
		}
	}

	mkvData, err := os.ReadFile(mkvPath)
	if err != nil {
		t.Fatalf("reading MKV file: %v", err)
	}
	dataSize := len(mkvData)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", dataSize))
			w.WriteHeader(http.StatusOK)
			w.Write(mkvData)
			return
		}
		var start, end int
		n, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if err != nil || n != 2 {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if start >= dataSize {
			http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= dataSize {
			end = dataSize - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, dataSize))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(mkvData[start : end+1])
	}))

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
		cdnServer.Close()
		cancel()
		t.Fatalf("state.Open: %v", err)
	}

	rc := cache.NewRangeCache(256 * 1024 * 1024, nil)
	cdnClient := stream.NewCDNClient(8)
	permalinkBuilder := func(fileKey string) string {
		return cdnServer.URL
	}
	streamer := stream.NewStreamReader(rc, cdnClient, 2, int64(16*1024*1024), permalinkBuilder)

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
		cancel()
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

// ffprobeResult holds parsed ffprobe output.
type ffprobeResult struct {
	Format struct {
		FormatName string `json:"format_name"`
		Duration   string `json:"duration"`
	} `json:"format"`
	Streams []struct {
		CodecType string `json:"codec_type"`
	} `json:"streams"`
}

// verifyMKV runs ffprobe and ffmpeg seek checks on an MKV file mounted via
// FUSE. It verifies format name, duration, stream types, and that seeks at
// each offset succeed.
func verifyMKV(t *testing.T, mountPath string, expectedDuration float64, seekOffsets []float64) {
	t.Helper()

	ffprobe := checkTool(t, "ffprobe")
	ffmpeg := checkTool(t, "ffmpeg")

	// Run ffprobe
	cmd := exec.Command(ffprobe,
		"-v", "quiet",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		mountPath,
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("ffprobe failed: %v", err)
	}

	var result ffprobeResult
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("parsing ffprobe output: %v", err)
	}

	// Check format name contains "matroska"
	if result.Format.FormatName == "" {
		t.Fatal("ffprobe returned empty format_name")
	}
	found := false
	// format_name may be comma-separated (e.g. "matroska,webm")
	for _, name := range strings.Split(result.Format.FormatName, ",") {
		if strings.TrimSpace(name) == "matroska" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("format_name = %q, want it to contain matroska", result.Format.FormatName)
	}

	// Check streams contain video
	hasVideo := false
	for _, s := range result.Streams {
		if s.CodecType == "video" {
			hasVideo = true
			break
		}
	}
	if !hasVideo {
		t.Error("no video stream found in MKV")
	}

	// Check duration within tolerance
	if result.Format.Duration == "" {
		t.Fatal("ffprobe returned empty duration")
	}
	gotDuration, err := strconv.ParseFloat(result.Format.Duration, 64)
	if err != nil {
		t.Fatalf("parsing duration %q: %v", result.Format.Duration, err)
	}
	if math.Abs(gotDuration-expectedDuration) > 1.0 {
		t.Errorf("duration = %.2f, want %.2f (tolerance 1s)", gotDuration, expectedDuration)
	}

	// Seek at each offset
	for _, offset := range seekOffsets {
		cmd := exec.Command(ffmpeg,
			"-ss", fmt.Sprintf("%.2f", offset),
			"-i", mountPath,
			"-frames:v", "1",
			"-f", "null", "-",
		)
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			t.Errorf("ffmpeg seek at offset %.2fs failed: %v", offset, err)
		} else {
			t.Logf("seek at %.2fs OK", offset)
		}
	}
}

func TestE2E_MKVPlaybackQuick(t *testing.T) {
	mkvPath := generateMKV(t)
	mountDir, cleanup := mountFUSEForMKV(t, mkvPath)
	defer cleanup()

	filePath := filepath.Join(mountDir, "movies", "Test Movie 2024", "Test.Movie.2024.mkv")

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat %s: %v", filePath, err)
	}
	t.Logf("file size: %d bytes", info.Size())

	verifyMKV(t, filePath, 5.0, []float64{0, 2.5})
}

// generateLargeMKV generates a large (~100MB) MKV file using ffmpeg.
// It uses mandelbrot (poorly compressible fractal) at 1080p to ensure the file
// reaches ~100MB regardless of codec efficiency. The test is skipped if ffmpeg
// is not available.
func generateLargeMKV(t *testing.T) string {
	t.Helper()
	ffmpeg := checkTool(t, "ffmpeg")

	output := filepath.Join(t.TempDir(), "test_large.mkv")

	// mandelbrot produces high-entropy frames that resist compression.
	// 20 seconds at 1080p/30fps with ultrafast produces ~120MB.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, ffmpeg,
		"-y",
		"-t", "20",
		"-f", "lavfi",
		"-i", "mandelbrot=size=1920x1080:rate=30",
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-f", "matroska",
		output,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("ffmpeg large MKV generation failed: %v", err)
	}

	fi, err := os.Stat(output)
	if err != nil {
		t.Fatalf("stat generated MKV: %v", err)
	}
	t.Logf("large MKV size: %d bytes (%.1f MB)", fi.Size(), float64(fi.Size())/1024/1024)

	return output
}

// sha256OfFile computes the SHA256 hash of the file at the given path.
func sha256OfFile(t *testing.T, path string) [32]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file for SHA256 (%s): %v", path, err)
	}
	return sha256.Sum256(data)
}

// TestE2E_MKVPlaybackLarge generates a large (~120MB) MKV, mounts it via FUSE,
// verifies playback with ffprobe/ffmpeg seek, and checks full SHA256 integrity.
func TestE2E_MKVPlaybackLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large MKV playback test in short mode")
	}

	mkvPath := generateLargeMKV(t)

	originalHash := sha256OfFile(t, mkvPath)
	t.Logf("original SHA256: %x", originalHash)

	mountDir, cleanup := mountFUSEForMKV(t, mkvPath)
	defer cleanup()

	filePath := filepath.Join(mountDir, "movies", "Test Movie 2024", "Test.Movie.2024.mkv")

	fi, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat(%s): %v", filePath, err)
	}
	t.Logf("mounted file size: %d bytes (%.1f MB)", fi.Size(), float64(fi.Size())/1024/1024)

	verifyMKV(t, filePath, 20.0, []float64{0, 5, 18})

	mountedHash := sha256OfFile(t, filePath)
	if mountedHash != originalHash {
		t.Errorf("SHA256 mismatch\noriginal: %x\nmounted: %x", originalHash, mountedHash)
	} else {
		t.Logf("SHA256 integrity verified: %x", mountedHash)
	}
}