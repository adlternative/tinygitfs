package page

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
	"sync"
	"syscall"
)

type MemAttr struct {
	attr *metadata.Attr
	mu   *sync.Mutex
	pool *Pool
}

func NewMemAttr(ctx context.Context, pool *Pool, inode metadata.Ino) (*MemAttr, error) {
	attr, eno := pool.Meta.Getattr(ctx, inode)
	if eno != syscall.F_OK {
		return nil, eno
	}

	return &MemAttr{
		attr: attr,
		pool: pool,
		mu:   &sync.Mutex{},
	}, nil
}

func (memAttr *MemAttr) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	memAttr.mu.Lock()
	defer memAttr.mu.Unlock()

	attr := memAttr.attr
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
		if size < attr.Length {
			// invalid pages
			err := memAttr.pool.TruncateWithLock(ctx, size)
			if err != nil {
				return syscall.EIO
			}
		}
	}

	metadata.ToAttrOut(memAttr.pool.inode, attr, &out.Attr)
	return syscall.F_OK
}

func (memAttr *MemAttr) Length() uint64 {
	memAttr.mu.Lock()
	defer memAttr.mu.Unlock()
	return memAttr.attr.Length
}

func (memAttr *MemAttr) UpdateLength(lens uint64) {
	memAttr.mu.Lock()
	defer memAttr.mu.Unlock()
	memAttr.attr.Length = lens
}

func (memAttr *MemAttr) UpdateLengthIfMore(lens uint64) {
	memAttr.mu.Lock()
	defer memAttr.mu.Unlock()
	if lens > memAttr.attr.Length {
		memAttr.attr.Length = lens
	}
}

func (memAttr *MemAttr) CopyToAttr(dst *metadata.Attr) {
	memAttr.mu.Lock()
	defer memAttr.mu.Unlock()
	metadata.CopySomeAttr(memAttr.attr, dst)
}

func (memAttr *MemAttr) CopyFromAttr(src *metadata.Attr) {
	memAttr.mu.Lock()
	defer memAttr.mu.Unlock()
	metadata.CopySomeAttr(src, memAttr.attr)
}
