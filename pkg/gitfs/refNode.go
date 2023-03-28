package gitfs

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/go-redis/redis/v8"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
	"syscall"
)

type GitRefNode struct {
	Node
}

// Open a file for read/write...
func (node *GitRefNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.WithFields(
		log.Fields{
			"flags":     flags,
			"inode":     node.inode,
			"node type": node.nodeType,
		}).Debug("Open")

	fh, err := node.gitfs.OpenRefFile(ctx, node.inode)
	if err != nil {
		return nil, 0, syscall.EIO
	}

	return fh, 0, syscall.F_OK
}

// Setattr If a file handle is passed, the Setattr() function of the file handle is called,
// otherwise the metadata is written directly to the metadata on disk.
func (node *GitRefNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	fields := log.Fields{}
	fields = make(map[string]interface{})
	fields["node type"] = node.nodeType
	fields["inode"] = node.inode

	if atime, ok := in.GetATime(); ok {
		fields["atime"] = atime
	}
	if ctime, ok := in.GetCTime(); ok {
		fields["ctime"] = ctime
	}
	if uid, ok := in.GetUID(); ok {
		fields["uid"] = uid
	}
	if gid, ok := in.GetGID(); ok {
		fields["gid"] = gid
	}
	if mode, ok := in.GetMode(); ok {
		fields["mode"] = mode
	}
	if size, ok := in.GetSize(); ok {
		fields["size"] = size
	}

	if f != nil {
		log.WithFields(fields).Debug("RefFile Setattr")
		f.(*RefFileHandler).Setattr(ctx, in, out)
	}
	log.WithFields(fields).Debug("Node Setattr")

	attr, eno := node.gitfs.DefaultDataSource.Meta.Getattr(ctx, node.inode)
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
		data, err := node.gitfs.DefaultDataSource.Meta.RefGet(ctx, node.inode)
		if err != nil {
			if err == redis.Nil {
				return syscall.ENOENT
			}
			return syscall.EIO
		}

		if uint64(len(data)) > attr.Length {
			err = node.gitfs.DefaultDataSource.Meta.RefSet(ctx, node.inode, data[:attr.Length])
			if err != nil {
				return syscall.EIO
			}
		}

	}

	err := node.gitfs.DefaultDataSource.Meta.SetattrDirectly(ctx, node.inode, attr)
	if err != nil {
		return syscall.EIO
	}

	metadata.ToAttrOut(node.inode, attr, &out.Attr)

	return syscall.F_OK
}

// Getattr If a file handle is passed, the Getattr() function of the file handle is called,
// otherwise the metadata is loaded directly from meta driver
func (node *GitRefNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if f != nil {
		return f.(*RefFileHandler).Getattr(ctx, out)
	}

	log.WithFields(
		log.Fields{
			"inode":     node.inode,
			"node type": node.nodeType,
		}).Trace("Getattr")

	attr, eno := node.gitfs.DefaultDataSource.Meta.Getattr(ctx, node.inode)
	if eno != 0 {
		return eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)
	return 0
}
