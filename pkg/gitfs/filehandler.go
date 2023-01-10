package gitfs

import (
	"context"
	"syscall"
	"time"

	"github.com/adlternative/tinygitfs/pkg/datasource"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/adlternative/tinygitfs/pkg/page"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
)

type FileHandler struct {
	pagePool *page.Pool
	inode    metadata.Ino
	datasource.DataSource
}

func NewFileHandler(ctx context.Context, inode metadata.Ino, dataSource datasource.DataSource) (*FileHandler, error) {
	pagePool, err := page.NewPagePool(ctx, dataSource, inode)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer cancel()
		timer := time.NewTimer(10 * time.Second)
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case <-timer.C:
				pagePool.Purge()
			}
		}
	}()

	return &FileHandler{
		pagePool:   pagePool,
		inode:      inode,
		DataSource: dataSource,
	}, nil
}

var _ = (fs.FileHandle)((*FileHandler)(nil))
var _ = (fs.FileWriter)((*FileHandler)(nil))
var _ = (fs.FileReader)((*FileHandler)(nil))

func (fh *FileHandler) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	log.WithFields(
		log.Fields{
			"length": len(data),
			"offset": off,
			"inode":  fh.inode,
		}).Debug("Write")

	written, err := fh.pagePool.Write(ctx, data, off)
	if err != nil {
		log.WithError(err).Errorf("pagePool write failed")
		return written, syscall.EIO
	}

	eno := fh.Meta.WriteUpdate(ctx, fh.inode, (uint64)(off)+(uint64)(written))
	if eno != syscall.F_OK {
		return 0, eno
	}
	return written, syscall.F_OK
}

func (fh *FileHandler) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"dest length": len(dest),
			"offset":      off,
			"inode":       fh.inode,
		}).Debug("Read")

	result, err := fh.pagePool.Read(ctx, dest, off)
	if err != nil {
		log.WithError(err).Errorf("pagePool Read failed")
		return result, syscall.EIO
	}
	return result, syscall.F_OK
}
