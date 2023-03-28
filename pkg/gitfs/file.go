package gitfs

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
)

type FileHandler interface {
	fs.FileHandle
}

type File interface {
	NewFileHandler() FileHandler
	UnRef(release func()) error
	Ref() int
	Release(ctx context.Context) error
}
