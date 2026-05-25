// Package fusefs provides platform-specific errno constants.
//
// On macOS, the "attribute not found" error is ENOATTR (syscall.ENOATTR).
// On Linux, the equivalent is ENODATA (syscall.ENODATA).
// We unify them under the name errNoXattr so the rest of the code
// stays platform-agnostic.
package fusefs