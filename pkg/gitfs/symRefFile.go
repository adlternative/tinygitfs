package gitfs

import (
	"context"
	"fmt"
	"github.com/adlternative/tinygitfs/pkg/datasource"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/go-redis/redis/v8"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
	"sync"
	"syscall"
)

type SymRefFile struct {
	inode metadata.Ino
	*datasource.DataSource
	gitfs       *GitFs
	mu          *sync.Mutex
	ref         int
	releaseOnce *sync.Once

	buf   []byte
	clean bool
}

type SymRefFileHandler struct {
	file *SymRefFile
}

func NewSymRefFile(ctx context.Context, inode metadata.Ino, dataSource *datasource.DataSource, gitFs *GitFs) (*SymRefFile, error) {
	var buf []byte
	data, err := dataSource.Meta.RefGet(ctx, inode)
	if err == nil {
		buf = []byte(data)
	} else if err == redis.Nil {
		buf = make([]byte, 0, 36)
	} else {
		return nil, err
	}

	return &SymRefFile{
		inode:       inode,
		DataSource:  dataSource,
		gitfs:       gitFs,
		mu:          &sync.Mutex{},
		releaseOnce: &sync.Once{},
		buf:         buf,
		clean:       true,
	}, nil
}

func (file *SymRefFile) NewFileHandler() FileHandler {
	file.mu.Lock()
	defer file.mu.Unlock()

	file.ref++

	return &SymRefFileHandler{
		file: file,
	}
}

func (file *SymRefFile) UnRef(release func()) error {
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

func (file *SymRefFile) Ref() int {
	file.mu.Lock()
	defer file.mu.Unlock()

	return file.ref
}

func (file *SymRefFile) Release(ctx context.Context) error {
	return file.gitfs.ReleaseFile(ctx, file.inode)
}

// Write will write the dest data to file begin at offset
func (fh *SymRefFileHandler) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	log.WithFields(
		log.Fields{
			"length": len(data),
			"offset": off,
			"inode":  fh.file.inode,
		}).Debug("Write")

	fh.file.mu.Lock()
	defer fh.file.mu.Unlock()

	length := int64(len(data))
	destLen := int64(len(fh.file.buf))
	if destLen < off+length {
		newBuffer := make([]byte, off+length)
		copy(newBuffer, fh.file.buf[:destLen])
		fh.file.buf = newBuffer
	}

	copy(fh.file.buf[off:off+length], data)
	written = uint32(length)
	fh.file.clean = false

	return written, syscall.F_OK
}

// Read will read the file data begin at offset to dest, read size no large then dest length
func (fh *SymRefFileHandler) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"dest length": len(dest),
			"offset":      off,
			"inode":       fh.file.inode,
		}).Debug("Read")

	fh.file.mu.Lock()
	defer fh.file.mu.Unlock()

	totalLength := len(fh.file.buf)
	if totalLength > len(dest) {
		totalLength = len(dest)
	}

	copy(dest, fh.file.buf[off:off+int64(totalLength)])

	return fuse.ReadResultData(dest[:totalLength]), syscall.F_OK
}

// Fsync sync all file page pool data to file.
func (fh *SymRefFileHandler) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	log.WithFields(
		log.Fields{
			"flags": flags,
			"inode": fh.file.inode,
		}).Debug("Fsync")

	fh.file.mu.Lock()
	defer fh.file.mu.Unlock()

	if !fh.file.clean {
		err := fh.file.DataSource.Meta.RefSet(ctx, fh.file.inode, string(fh.file.buf))
		if err != nil {
			return syscall.EIO
		}
		fh.file.clean = true
	}

	// update file size
	attr, eno := fh.file.DataSource.Meta.Getattr(ctx, fh.file.inode)
	if eno != syscall.F_OK {
		return eno
	}
	attr.Length = uint64(len(fh.file.buf))

	err := fh.file.Meta.SetattrDirectly(ctx, fh.file.inode, attr)
	if err != nil {
		return syscall.EIO
	}

	return syscall.F_OK
}

// Flush will be called when file closed. (maybe called many times)
// We just do fsync here...
func (fh *SymRefFileHandler) Flush(ctx context.Context) syscall.Errno {
	log.WithFields(
		log.Fields{
			"inode": fh.file.inode,
		}).Debug("Flush")

	fh.file.mu.Lock()
	defer fh.file.mu.Unlock()

	if !fh.file.clean {
		err := fh.file.DataSource.Meta.RefSet(ctx, fh.file.inode, string(fh.file.buf))
		if err != nil {
			return syscall.EIO
		}
		fh.file.clean = true
	}

	// update file size
	attr, eno := fh.file.DataSource.Meta.Getattr(ctx, fh.file.inode)
	if eno != syscall.F_OK {
		return eno
	}
	attr.Length = uint64(len(fh.file.buf))

	err := fh.file.Meta.SetattrDirectly(ctx, fh.file.inode, attr)
	if err != nil {
		return syscall.EIO
	}

	return syscall.F_OK
}

// Setattr set the attr to memattr
func (fh *SymRefFileHandler) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	fh.file.mu.Lock()
	defer fh.file.mu.Unlock()

	attr, eno := fh.file.DataSource.Meta.Getattr(ctx, fh.file.inode)
	if eno != syscall.F_OK {
		return eno
	}
	if atime, ok := in.GetATime(); ok {
		metadata.SetTime(&attr.Atime, &attr.Atimensec, atime)
	}
	if ctime, ok := in.GetCTime(); ok {
		metadata.SetTime(&attr.Ctime, &attr.Ctimensec, ctime)
	}
	if uid, ok := in.GetUID(); ok {
		attr.Uid = uid
	}
	if gid, ok := in.GetGID(); ok {
		attr.Gid = gid
	}
	if mode, ok := in.GetMode(); ok {
		attr.Mode = uint16(mode)
	}
	if size, ok := in.GetSize(); ok {
		attr.Length = size
	}
	err := fh.file.Meta.SetattrDirectly(ctx, fh.file.inode, attr)
	if err != nil {
		return syscall.EIO
	}
	metadata.ToAttrOut(fh.file.inode, attr, &out.Attr)

	if uint64(len(fh.file.buf)) > attr.Length {
		fh.file.buf = fh.file.buf[:attr.Length]
		fh.file.clean = false
	}

	return syscall.F_OK
}

// Getattr get the attr from meta attr & memattr
func (fh *SymRefFileHandler) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	fh.file.mu.Lock()
	defer fh.file.mu.Unlock()

	attr, eno := fh.file.Meta.Getattr(ctx, fh.file.inode)
	if eno != 0 {
		return eno
	}
	metadata.ToAttrOut(fh.file.inode, attr, &out.Attr)
	return syscall.F_OK
}
