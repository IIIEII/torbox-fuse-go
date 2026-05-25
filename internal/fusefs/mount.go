// Package fusefs provides the FUSE filesystem mounting logic for the
// TorBox Media Center virtual filesystem.
package fusefs

import (
	"context"
	"log"
	"log/slog"
	"runtime"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/iiieii/torbox-fuse-go/internal/config"
)

// Mount creates a FUSE server, mounts it at mountPath, and blocks until
// the context is cancelled or the server is unmounted.
func Mount(ctx context.Context, mountPath string, root *RootNode, cfg *config.Config) error {
	attrTimeout := time.Duration(cfg.AttrTimeoutSec) * time.Second
	entryTimeout := time.Duration(cfg.EntryTimeoutSec) * time.Second
	negTimeout := time.Duration(0)

	opts := &fs.Options{
		AttrTimeout:     &attrTimeout,
		EntryTimeout:    &entryTimeout,
		NegativeTimeout: &negTimeout,
		MountOptions: fuse.MountOptions{
			AllowOther:   true,
			MaxReadAhead: 4 << 20, // 4 MiB
			FsName:       "torbox-media-center",
		},
	}

	// On macOS, macFUSE uses different opcode numbers than Linux FUSE.
	// For example, opcode 60 (macOS GETXATTR) maps to opcode 22 on Linux.
	// go-fuse only handles Linux opcodes, so macFUSE-specific opcodes
	// fall through to the default handler which logs "Unimplemented opcode
	// OPCODE-XX" for every request. macOS does not cache ENOSYS for xattr
	// queries and retries on every file access, causing continuous log spam.
	//
	// Suppress these messages by redirecting the default log.Logger to a
	// writer that drops "Unimplemented opcode" lines. go-fuse uses both
	// opts.Logger (for most messages) and log.Printf (for some error paths
	// like "Unknown opcode"), so we intercept log.Default() to cover both.
	if runtime.GOOS == "darwin" {
		suppress := newSuppressUnimplementedLogger()
		opts.MountOptions.Logger = suppress
		log.SetOutput(suppressWriter{})
	}

	server, err := fs.Mount(mountPath, root, opts)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = server.Unmount()
	}()

	server.Wait()
	return nil
}

// suppressWriter is an io.Writer that drops lines containing
// "Unimplemented opcode" or "Unknown opcode" and forwards everything
// else to slog. This filters macFUSE-specific opcode spam (e.g. opcode 60
// for GETXATTR) that go-fuse doesn't handle.
//
// Both go-fuse's MountOptions.Logger (which uses log.Printf with no
// date/time prefix) and log.Default() (which adds LstdFlags prefix like
// "2026/05/25 10:58:02") write through this writer, so we match anywhere
// in the line rather than just at the start.
type suppressWriter struct{}

func (suppressWriter) Write(p []byte) (int, error) {
	s := string(p)
	if strings.Contains(s, "Unimplemented opcode") || strings.Contains(s, "Unknown opcode") {
		return len(p), nil
	}
	slog.Info(strings.TrimSpace(s))
	return len(p), nil
}

// newSuppressUnimplementedLogger returns a *log.Logger that writes through
// suppressWriter, dropping opcode-related spam lines.
func newSuppressUnimplementedLogger() *log.Logger {
	return log.New(suppressWriter{}, "", 0)
}