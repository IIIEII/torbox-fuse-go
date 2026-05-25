//go:build linux

package fusefs

import "syscall"

// On Linux, ENOATTR is ENODATA (ENOATTR is macOS-only).
const errNoAttr = syscall.ENODATA