package metadata

import (
	"context"
	"encoding/json"
	"github.com/hanwen/go-fuse/v2/fuse"
	"syscall"
)

const (
	TypeFile      = 1 // type for regular file
	TypeDirectory = 2 // type for directory
	TypeSymlink   = 3 // type for symlink
	TypeFIFO      = 4 // type for FIFO node
	TypeBlockDev  = 5 // type for block device
	TypeCharDev   = 6 // type for character device
	TypeSocket    = 7 // type for socket
)

// Attr is inode attributes information
type Attr struct {
	Flags     uint8  `json:"flags,omitempty"`     // reserved flags
	Typ       uint8  `json:"type,omitempty"`      // type of a node
	Mode      uint16 `json:"mode,omitempty"`      // permission mode
	Uid       uint32 `json:"uid,omitempty"`       // owner id
	Gid       uint32 `json:"gid,omitempty"`       // group id of owner
	Atime     uint64 `json:"atime,omitempty"`     // last access time
	Mtime     uint64 `json:"mtime,omitempty"`     // last modified time
	Ctime     uint64 `json:"ctime,omitempty"`     // last change time for meta
	Atimensec uint32 `json:"atimensec,omitempty"` // nanosecond part of atime
	Mtimensec uint32 `json:"mtimensec,omitempty"` // nanosecond part of mtime
	Ctimensec uint32 `json:"ctimensec,omitempty"` // nanosecond part of ctime
	Nlink     uint32 `json:"nlink,omitempty"`     // number of links (sub-directories or hardlinks)
	Length    uint64 `json:"length,omitempty"`    // length of regular file
	Rdev      uint32 `json:"rdev,omitempty"`      // device number
}

// SMode is the file mode including type and unix permission.
func (a Attr) SMode() uint32 {
	return TypeToStatType(a.Typ) | uint32(a.Mode)
}

func ToAttrOut(ino Ino, attr *Attr, out *fuse.Attr) {
	out.Ino = uint64(ino)
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Mode = attr.SMode()
	out.Nlink = attr.Nlink
	out.Atime = attr.Atime
	out.Atimensec = attr.Atimensec
	out.Mtime = attr.Mtime
	out.Mtimensec = attr.Mtimensec
	out.Ctime = attr.Ctime
	out.Ctimensec = attr.Ctimensec

	var size, blocks uint64
	switch attr.Typ {
	case TypeDirectory:
		fallthrough
	case TypeSymlink:
		fallthrough
	case TypeFile:
		size = attr.Length
		blocks = (size + 511) >> 9
	case TypeBlockDev:
		fallthrough
	case TypeCharDev:
		out.Rdev = attr.Rdev
	}
	out.Size = size
	out.Blocks = blocks
	setBlksize(out, 0x10000) //64K
}

func CopySomeAttr(src, dst *Attr) {
	dst.Length = src.Length
	dst.Atime = src.Length
	dst.Atimensec = src.Atimensec
	dst.Ctime = src.Length
	dst.Ctimensec = src.Ctimensec
	dst.Gid = src.Gid
	dst.Uid = src.Uid
	dst.Mode = src.Mode
}

func TypeToStatType(_type uint8) uint32 {
	switch _type & 0x7F {
	case TypeDirectory:
		return syscall.S_IFDIR
	case TypeSymlink:
		return syscall.S_IFLNK
	case TypeFile:
		return syscall.S_IFREG
	case TypeFIFO:
		return syscall.S_IFIFO
	case TypeSocket:
		return syscall.S_IFSOCK
	case TypeBlockDev:
		return syscall.S_IFBLK
	case TypeCharDev:
		return syscall.S_IFCHR
	default:
		panic(_type)
	}
}

func GetFiletype(mode uint16) uint8 {
	switch mode & (syscall.S_IFMT & 0xffff) {
	case syscall.S_IFIFO:
		return TypeFIFO
	case syscall.S_IFSOCK:
		return TypeSocket
	case syscall.S_IFLNK:
		return TypeSymlink
	case syscall.S_IFREG:
		return TypeFile
	case syscall.S_IFBLK:
		return TypeBlockDev
	case syscall.S_IFDIR:
		return TypeDirectory
	case syscall.S_IFCHR:
		return TypeCharDev
	}
	return TypeFile
}

// Getattr return the attributes of the specified inode
func (r *RedisMeta) Getattr(ctx context.Context, ino Ino) (*Attr, syscall.Errno) {
	attr := &Attr{}
	data, err := r.rdb.Get(ctx, inodeKey(ino)).Bytes()
	if err != nil {
		return nil, errno(err)
	}
	err = json.Unmarshal(data, attr)
	if err != nil {
		return nil, errno(err)
	}
	return attr, 0
}

func (r *RedisMeta) SetattrDirectly(ctx context.Context, ino Ino, attr *Attr) error {
	jsonAttr, err := json.Marshal(&attr)
	if err != nil {
		return err
	}
	_, err = r.rdb.Set(ctx, inodeKey(ino), jsonAttr, 0).Result()

	if err != nil {
		return err
	}
	return nil
}

func (r *RedisMeta) Ref(ctx context.Context, inode Ino) syscall.Errno {
	attr, eno := r.Getattr(ctx, inode)
	if eno != syscall.F_OK {
		return eno
	}
	attr.Nlink++
	err := r.SetattrDirectly(ctx, inode, attr)
	if err != nil {
		return errno(err)
	}
	return syscall.F_OK
}
