package gitfs

import (
	"context"
	"fmt"
	"github.com/adlternative/tinygitfs/pkg/datasource"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/adlternative/tinygitfs/pkg/page"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
	"sync"
	"syscall"
	"time"
)

type RegularFileHandler struct {
	file *RegularFile
}

type RegularFile struct {
	inode    metadata.Ino
	pagePool *page.Pool
	ref      int
	*datasource.DataSource
	mu          *sync.Mutex
	releaseOnce *sync.Once
	gitfs       *GitFs
}

func NewRegularFile(ctx context.Context, inode metadata.Ino, dataSource *datasource.DataSource, gitFs *GitFs) (*RegularFile, error) {
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
				if err := pagePool.Fsync(ctx); err != nil {
					log.WithField("inode", inode).WithError(err).Error("page pool fsync failed")
				}
			}
		}
	}()

	return &RegularFile{
		pagePool:    pagePool,
		inode:       inode,
		DataSource:  dataSource,
		mu:          &sync.Mutex{},
		releaseOnce: &sync.Once{},
		gitfs:       gitFs,
	}, nil
}

func (file *RegularFile) NewFileHandler() FileHandler {
	file.mu.Lock()
	defer file.mu.Unlock()

	file.ref++

	return &RegularFileHandler{
		file: file,
	}
}

func (file *RegularFile) UnRef(release func()) error {
	file.mu.Lock()
	defer file.mu.Unlock()

	file.ref--
	if file.ref < 0 {
		log.Errorf("file ref down to negative value: %d", file.ref)
		return fmt.Errorf("file ref down to negative value: %d", file.ref)
	} else if file.ref == 0 {
		file.releaseOnce.Do(release)
	}
	return nil
}

func (file *RegularFile) Ref() int {
	file.mu.Lock()
	defer file.mu.Unlock()

	return file.ref
}

func (file *RegularFile) Release(ctx context.Context) error {
	return file.gitfs.ReleaseFile(ctx, file.inode)
}

func (file *RegularFile) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return file.pagePool.Setattr(ctx, in, out)
}

var _ = (fs.FileHandle)((*RegularFileHandler)(nil))
var _ = (fs.FileWriter)((*RegularFileHandler)(nil))
var _ = (fs.FileReader)((*RegularFileHandler)(nil))
var _ = (fs.FileFlusher)((*RegularFileHandler)(nil))
var _ = (fs.FileFsyncer)((*RegularFileHandler)(nil))
var _ = (fs.FileReleaser)((*RegularFileHandler)(nil))
var _ = (fs.FileGetattrer)((*RegularFileHandler)(nil))
var _ = (fs.FileSetattrer)((*RegularFileHandler)(nil))

// Setattr set the attr to memattr
func (fh *RegularFileHandler) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return fh.file.Setattr(ctx, in, out)
}

func (fh *RegularFileHandler) getattr(ctx context.Context) (*metadata.Attr, syscall.Errno) {
	attr, eno := fh.file.Meta.Getattr(ctx, fh.file.inode)
	if eno != 0 {
		return nil, eno
	}
	fh.file.pagePool.MemAttr().CopyToAttr(attr)
	return attr, eno
}

// Getattr get the attr from meta attr & memattr
func (fh *RegularFileHandler) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	attr, eno := fh.getattr(ctx)
	if eno != 0 {
		return eno
	}
	metadata.ToAttrOut(fh.file.inode, attr, &out.Attr)
	return syscall.F_OK
}

// Release file handler release
func (fh *RegularFileHandler) Release(ctx context.Context) syscall.Errno {
	log.WithFields(
		log.Fields{
			"inode": fh.file.inode,
		}).Debug("Release")

	err := fh.file.Release(ctx)
	if err != nil {
		return syscall.ENOENT
	}

	return syscall.F_OK
}

// Fsync sync all file page pool data to file.
func (fh *RegularFileHandler) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	log.WithFields(
		log.Fields{
			"flags": flags,
			"inode": fh.file.inode,
		}).Debug("Fsync")
	err := fh.file.pagePool.Fsync(ctx)
	if err != nil {
		log.WithError(err).Error("fsync failed")
		return syscall.EIO
	}
	return syscall.F_OK
}

// Flush will be called when file closed. (maybe called many times)
// We just do fsync here...
func (fh *RegularFileHandler) Flush(ctx context.Context) syscall.Errno {
	log.WithFields(
		log.Fields{
			"inode": fh.file.inode,
		}).Debug("Flush")

	err := fh.file.pagePool.Fsync(ctx)
	if err != nil {
		log.WithError(err).Error("flush fsync failed")
		return syscall.EIO
	}

	return syscall.F_OK
}

// Write will write the dest data to file begin at offset
func (fh *RegularFileHandler) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	log.WithFields(
		log.Fields{
			"length": len(data),
			"offset": off,
			"inode":  fh.file.inode,
		}).Debug("Write")

	written, err := fh.file.pagePool.Write(ctx, data, off)
	if err != nil {
		log.WithError(err).Errorf("pagePool write failed")
		return written, syscall.EIO
	}

	return written, syscall.F_OK
}

// Read will read the file data begin at offset to dest, read size no large then dest length
func (fh *RegularFileHandler) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"dest length": len(dest),
			"offset":      off,
			"inode":       fh.file.inode,
		}).Debug("Read")

	result, err := fh.file.pagePool.Read(ctx, dest, off)
	if err != nil {
		log.WithError(err).Errorf("pagePool Read failed")
		return result, syscall.EIO
	}
	return result, syscall.F_OK
}
