package metadata

import (
	"context"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
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

var (
	uid int
	gid int
)

func init() {
	uid = os.Getuid()
	gid = os.Getgid()
}

type Ino int64

type DentryData struct {
	Ino Ino   `json:"inode"`
	Typ uint8 `json:"type"`
}

type Dentry struct {
	DentryData
	name string
}

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

type DirStream struct {
	ino  Ino
	meta *RedisMeta

	totalCnt int
	curPos   int

	dentries []*Dentry
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

// SMode is the file mode including type and unix permission.
func (a Attr) SMode() uint32 {
	return TypeToStatType(a.Typ) | uint32(a.Mode)
}

func (i Ino) String() string {
	return strconv.FormatUint(uint64(i), 10)
}

func inodeKey(inode Ino) string {
	return "i" + inode.String()
}

func dentryKey(inode Ino) string {
	return "d" + inode.String()
}

type ChunkAttr struct {
	Offset      int64  `json:"offset"`
	Length      int    `json:"length"`
	StoragePath string `json:"storagePath"`
}

func chunkKey(inode Ino) string {
	return "c" + inode.String()
}

// SetTime set given time and timesec to given time.Time
func SetTime(time *uint64, timensec *uint32, t time.Time) {
	sec := uint64(t.Unix())
	nsec := uint32(t.Nanosecond())

	if time != nil {
		*time = sec
	}
	if timensec != nil {
		*timensec = nsec
	}
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

func NewDirStream(ctx context.Context, ino Ino, meta *RedisMeta) (*DirStream, error) {
	ds := &DirStream{
		ino:    ino,
		curPos: 0,
		meta:   meta,
	}
	dentries, err := meta.GetAllDentries(ctx, ino)
	if err != nil {
		return nil, err
	}
	ds.dentries = dentries
	ds.totalCnt = len(dentries)

	return ds, nil
}

func (ds *DirStream) HasNext() bool {
	return ds.curPos < ds.totalCnt
}

func (ds *DirStream) Next() (de fuse.DirEntry, eno syscall.Errno) {
	ctx := context.TODO()

	if ds.curPos > ds.totalCnt {
		return de, syscall.EIO
	}

	dentry := ds.dentries[ds.curPos]
	de.Name = dentry.name
	de.Ino = uint64(dentry.Ino)
	attr, eno := ds.meta.Getattr(ctx, dentry.Ino)
	if eno != 0 {
		return de, eno
	}
	de.Mode = uint32(attr.Mode)
	ds.curPos++
	return
}

func (ds *DirStream) Close() {
	// nothing
}

func Align4K(length uint64) int64 {
	if length == 0 {
		return 1 << 12
	}
	return int64((((length - 1) >> 12) + 1) << 12)
}
