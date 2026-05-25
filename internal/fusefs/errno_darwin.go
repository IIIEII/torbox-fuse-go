//go:build darwin

package fusefs

import "syscall"

// errNoAttr is the error returned for missing extended attributes.
// On macOS this is syscall.ENOATTR.
var errNoAttr = syscall.ENOATTR