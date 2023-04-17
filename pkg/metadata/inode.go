package metadata

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"strconv"
	"syscall"
	"time"
)

type Ino int64

func (i Ino) String() string {
	return strconv.FormatUint(uint64(i), 10)
}

func inodeKey(inode Ino) string {
	return "i" + inode.String()
}

// MkNod create a new inode
func (r *RedisMeta) MkNod(ctx context.Context, parent Ino, _type uint8, name string, mode uint32, dev uint32) (*Attr, Ino, syscall.Errno) {
	attr := &Attr{}
	ino, err := r.nextInode(ctx)
	if err != nil {
		log.Error("get next inode failed")
		return nil, 0, errno(err)
	}

	ts := time.Now()
	SetTime(&attr.Atime, &attr.Atimensec, ts)
	SetTime(&attr.Mtime, &attr.Mtimensec, ts)
	SetTime(&attr.Ctime, &attr.Ctimensec, ts)

	attr.Typ = _type
	attr.Mode = uint16(mode)
	attr.Rdev = dev
	attr.Uid = uint32(uid)
	attr.Gid = uint32(gid)

	if _type == TypeDirectory {
		attr.Nlink = 2
		attr.Length = 4 << 10
	} else {
		attr.Nlink = 1
		if _type == TypeSymlink {
			attr.Length = uint64(len(name))
		} else {
			attr.Length = 0
		}
	}
	// if parent.dentries have new node
	_, find, err := r.GetDentry(ctx, parent, name)
	if err != nil {
		log.WithError(err).Error("get dentry failed")
		return nil, 0, errno(err)
	}
	if find {
		attr, eno := r.Getattr(ctx, ino)
		if eno != 0 {
			log.WithField("errno", eno).Error("get attr failed")
			return nil, 0, eno
		}
		return attr, 0, syscall.EEXIST
	}

	jsonAttr, err := json.Marshal(attr)
	r.rdb.Set(ctx, inodeKey(ino), jsonAttr, 0)
	//log.WithField("inode", ino).Info("create inode")

	err = r.SetDentry(ctx, parent, name, ino, _type)
	if err != nil {
		return nil, 0, errno(err)
	}
	eno := r.Ref(ctx, parent)
	if eno != syscall.F_OK {
		return nil, 0, eno
	}

	return attr, ino, 0
}
