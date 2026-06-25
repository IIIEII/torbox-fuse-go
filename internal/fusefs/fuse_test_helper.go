//go:build !short

package fusefs

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/catalog"
)

// requireFUSE skips the test if FUSE is not available on the current platform.
//
// On Linux: checks that /dev/fuse exists and is accessible (non-root users
// can mount FUSE filesystems via /dev/fuse on most distributions).
//
// On macOS: checks for macFUSE or FUSE-T installation (root always has access;
// non-root needs a third-party FUSE driver).
func requireFUSE(t *testing.T) {
	t.Helper()

	if runtime.GOOS == "linux" {
		// On Linux, FUSE is available via /dev/fuse. Check accessibility.
		if _, err := os.Stat("/dev/fuse"); err != nil {
			t.Skip("skipping: /dev/fuse not found (FUSE not available)")
		}
		// /dev/fuse exists — non-root users can typically mount FUSE
		// on modern Linux distributions. If we're root, we're fine too.
		return
	}

	if runtime.GOOS == "darwin" {
		// On macOS, root can always mount FUSE.
		if os.Getuid() == 0 {
			return
		}
		// Non-root needs macFUSE or FUSE-T.
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs"); err != nil && os.Getenv("FUSE_T") == "" {
			t.Skip("skipping: no FUSE driver found (macFUSE or FUSE-T required)")
		}
		return
	}

	// Other platforms: skip unless running as root.
	if os.Getuid() != 0 {
		t.Skip("skipping: FUSE mount requires root on this platform")
	}
}

// waitForMount polls until the FUSE mount point is functional (can list
// directory entries) or the context expires.
func waitForMount(t *testing.T, ctx context.Context, mountDir string) error {
	t.Helper()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for mount at %s: %w", mountDir, ctx.Err())
		default:
		}

		f, err := os.Open(mountDir)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		_, err = f.Readdirnames(0)
		_ = f.Close() // best-effort close in poll loop
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return nil
	}
}

// waitForGone polls until the given path no longer exists (os.Stat returns
// ENOENT) or the context expires. This is the correct way to assert
// cross-directory disappearance after Unlink/Rmdir: FUSE dentry cache
// invalidation for entries in *other* directories (e.g. /all/ when the file
// was unlinked from /series/) is dispatched asynchronously from the handler
// to avoid deadlocking on /dev/fuse, so it is eventually-consistent rather
// than instantaneous. The entry where the operation was performed is gone
// immediately (the kernel drops its dentry on the Unlink reply itself); only
// sibling-directory mirrors need this poll.
func waitForGone(t *testing.T, ctx context.Context, path string) error {
	t.Helper()
	for {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			t.Fatalf("waitForGone: unexpected error for %s: %v", path, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s still exists after timeout (cross-directory cache invalidation did not complete)", path)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// dirEntryNames returns the names of catalog DirEntry slices for logging.
func dirEntryNames(entries []catalog.DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

// fsEntryNames returns the names of os.DirEntry slices for logging.
func fsEntryNames(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

// dirEntryNamesFs extracts sorted names from os.DirEntry slices.
func dirEntryNamesFs(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

// e2eTorboxConfig implements torbox.Config for e2e tests.
type e2eTorboxConfig struct {
	apiKey  string
	baseURL string
}

func (c *e2eTorboxConfig) APIKey() string           { return c.apiKey }
func (c *e2eTorboxConfig) APIBaseURL() string        { return c.baseURL }
func (c *e2eTorboxConfig) APITimeout() time.Duration { return 30 * time.Second }