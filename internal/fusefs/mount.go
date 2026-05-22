// Package fusefs provides the FUSE filesystem mounting logic for the
// TorBox Media Center virtual filesystem.
package fusefs

import (
	"context"
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

	server, err := fs.Mount(mountPath, root, &fs.Options{
		AttrTimeout:     &attrTimeout,
		EntryTimeout:    &entryTimeout,
		NegativeTimeout: &negTimeout,
		MountOptions: fuse.MountOptions{
			AllowOther:   true,
			MaxReadAhead: 4 << 20, // 4 MiB
			FsName:       "torbox-media-center",
		},
	})
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		server.Unmount()
	}()

	server.Wait()
	return nil
}