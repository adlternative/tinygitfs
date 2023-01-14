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
				pagePool.Fsync(ctx)
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
var _ = (fs.FileFlusher)((*FileHandler)(nil))
var _ = (fs.FileFsyncer)((*FileHandler)(nil))

func (fh *FileHandler) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	log.WithFields(
		log.Fields{
			"flags": flags,
			"inode": fh.inode,
		}).Debug("Fsync")
	err := fh.pagePool.Fsync(context.Background())
	if err != nil {
		log.WithError(err).Error("fsync failed")
		return syscall.EIO
	}
	return syscall.F_OK
}

func (fh *FileHandler) Flush(ctx context.Context) syscall.Errno {
	log.WithFields(
		log.Fields{
			"inode": fh.inode,
		}).Debug("Flush")
	err := fh.pagePool.Fsync(context.Background())
	if err != nil {
		log.WithError(err).Error("fsync failed")
		return syscall.EIO
	}
	return syscall.F_OK
}

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
	eno := fh.Meta.ReadUpdate(ctx, fh.inode)
	if eno != syscall.F_OK {
		return result, eno
	}

	return result, syscall.F_OK
}
