package gitfs

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/datasource"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/adlternative/tinygitfs/pkg/page"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
	"syscall"
)

type NewNodeFn = func(dataSource datasource.DataSource, ino metadata.Ino, name string) fs.InodeEmbedder

func defaultNewNode(dataSource datasource.DataSource, ino metadata.Ino, name string) fs.InodeEmbedder {
	return &Node{
		inode:      ino,
		name:       name,
		DataSource: dataSource,
		newNodeFn:  defaultNewNode,
	}
}

type Node struct {
	fs.Inode

	inode metadata.Ino
	name  string

	datasource.DataSource
	newNodeFn NewNodeFn
}

var _ = (fs.NodeGetattrer)((*Node)(nil))
var _ = (fs.NodeMknoder)((*Node)(nil))
var _ = (fs.NodeMkdirer)((*Node)(nil))
var _ = (fs.NodeReaddirer)((*Node)(nil))
var _ = (fs.NodeOpendirer)((*Node)(nil))
var _ = (fs.NodeLookuper)((*Node)(nil))
var _ = (fs.NodeRmdirer)((*Node)(nil))
var _ = (fs.NodeUnlinker)((*Node)(nil))
var _ = (fs.NodeSetattrer)((*Node)(nil))
var _ = (fs.NodeRenamer)((*Node)(nil))
var _ = (fs.NodeLinker)((*Node)(nil))
var _ = (fs.NodeOpener)((*Node)(nil))
var _ = (fs.NodeStatfser)((*Node)(nil))

func (node *Node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	out.NameLen = 255
	out.Frsize = page.PageSize
	out.Bsize = page.PageSize

	totalInodeCount, err := node.Meta.TotalInodeCount(ctx)
	if err != nil {
		log.WithError(err).Errorf("Statfs failed to get total inode count")
		return syscall.EIO
	}
	out.Files = totalInodeCount

	curInodeCount, err := node.Meta.CurInodeCount(ctx)
	if err != nil {
		log.WithError(err).Errorf("Statfs failed to get cur inode count")
		return syscall.EIO
	}
	out.Ffree = totalInodeCount - curInodeCount

	useSpace, err := node.Meta.UsedSpace(ctx)
	if err != nil {
		log.WithError(err).Errorf("Statfs failed to get used space")
		return syscall.EIO
	}
	totalSpace, err := node.Meta.TotalSpace(ctx)
	if err != nil {
		log.WithError(err).Errorf("Statfs failed to get total space")
		return syscall.EIO
	}
	out.Blocks = totalSpace / uint64(out.Bsize)
	out.Bfree = (totalSpace - useSpace) / uint64(out.Bsize)
	out.Bavail = out.Bfree

	log.WithFields(
		log.Fields{
			"totalInodeCount": totalInodeCount,
			"curInodeCount":   curInodeCount,
			"totalSpace":      totalSpace,
			"usedSpace":       useSpace,
			"blocks":          out.Blocks,
			"bfree":           out.Bfree,
			"bavail":          out.Bavail,
			"ffree":           out.Ffree,
			"files":           out.Files,
		}).Debug("Statfs")

	return syscall.F_OK
}

func (node *Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (newNode *fs.Inode, errno syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":   name,
			"target": target.EmbeddedInode(),
			"inode":  node.inode,
		}).Trace("Link")

	targetInode := metadata.Ino(target.EmbeddedInode().StableAttr().Ino)
	attr, eno := node.Meta.Link(ctx, node.inode, targetInode, name)
	if eno != syscall.F_OK {
		return nil, eno
	}

	metadata.ToAttrOut(targetInode, attr, &out.Attr)

	return node.NewInode(ctx, node.newNodeFn(node.DataSource, targetInode, name), fs.StableAttr{
		Mode: uint32(attr.Mode),
		Ino:  uint64(targetInode),
		Gen:  1,
	}), syscall.F_OK
}

func (node *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newParentInode := newParent.EmbeddedInode().StableAttr().Ino

	log.WithFields(
		log.Fields{
			"name":      name,
			"newParent": newParentInode,
			"newName":   newName,
			"flags":     flags,
			"inode":     node.inode,
		}).Debug("Rename")

	return node.Meta.Rename(ctx, node.inode, name, metadata.Ino(newParentInode), newName)
}

func (node *Node) Opendir(ctx context.Context) syscall.Errno {
	log.WithFields(
		log.Fields{
			"inode": node.inode,
		}).Trace("Opendir")

	attr, eno := node.Meta.Getattr(ctx, node.inode)
	if eno != syscall.F_OK {
		return eno
	}
	if attr.Typ != metadata.TypeDirectory {
		return syscall.ENOTDIR
	}

	return syscall.F_OK
}

func (node *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"parent inode": node.inode,
		}).Trace("Readdir")
	ds, err := metadata.NewDirStream(ctx, node.inode, node.Meta)
	if err != nil {
		return nil, syscall.ENOENT
	}
	return ds, 0
}

func (node *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if f != nil {
		return f.(*FileHandler).Getattr(ctx, out)
	}

	log.WithFields(
		log.Fields{
			"inode": node.inode,
		}).Trace("Getattr")

	attr, eno := node.Meta.Getattr(ctx, node.inode)
	if eno != 0 {
		return eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)
	return 0
}

func (node *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":         name,
			"parent inode": node.inode,
		}).Trace("Lookup")
	entry, find, err := node.Meta.GetDentry(ctx, node.inode, name)
	if err != nil || !find {
		return nil, syscall.ENOENT
	}
	attr, eno := node.Meta.Getattr(ctx, entry.Ino)
	if eno != 0 {
		return nil, eno
	}

	metadata.ToAttrOut(entry.Ino, attr, &out.Attr)

	//log.Printf("%s %d %o\n", name, entry.Ino, out.Attr.Mode)

	return node.NewInode(ctx, node.newNodeFn(node.DataSource, entry.Ino, name), fs.StableAttr{
		Mode: uint32(out.Mode),
		Ino:  uint64(entry.Ino),
		Gen:  1,
	}), 0

}

func (node *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":         name,
			"mode":         mode,
			"parent inode": node.inode,
		}).Debug("Mkdir")
	attr, ino, eno := node.Meta.MkNod(ctx, node.inode, metadata.TypeDirectory, name, mode, 0)
	if eno != 0 {
		return nil, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)

	log.WithFields(
		log.Fields{
			"inode": ino,
		}).Debug("Mkdir Result")

	return node.NewInode(ctx, node.newNodeFn(node.DataSource, ino, name), fs.StableAttr{
		Mode: out.Mode,
		Ino:  uint64(ino),
		Gen:  1,
	}), 0
}

func (node *Node) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":         name,
			"mode":         mode,
			"dev":          dev,
			"parent inode": node.inode,
		}).Debug("Mknod")
	_type := metadata.GetFiletype(uint16(mode))
	if _type == 0 {
		return nil, syscall.EPERM
	}

	attr, ino, eno := node.Meta.MkNod(ctx, node.inode, _type, name, mode, dev)
	if eno != 0 {
		return nil, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)

	log.WithFields(
		log.Fields{
			"inode": ino,
		}).Debug("Mknod Result")

	return node.NewInode(ctx, node.newNodeFn(node.DataSource, ino, name), fs.StableAttr{
		Mode: out.Mode,
		Ino:  uint64(ino),
		Gen:  1,
	}), 0
}

func (node *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":         name,
			"flags":        flags,
			"mode":         mode,
			"parent inode": node.inode,
		}).Debug("Create")
	attr, ino, eno := node.Meta.MkNod(ctx, node.inode, metadata.TypeFile, name, mode, 0)
	if eno != 0 {
		return nil, 0, 0, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)

	log.WithFields(
		log.Fields{
			"inode": ino,
		}).Debug("Create Result")

	fileHandler, err := GlobalGitFs.OpenFile(ctx, ino)
	if err != nil {
		return nil, 0, 0, syscall.ENOENT
	}
	return node.NewInode(ctx, node.newNodeFn(node.DataSource, ino, name), fs.StableAttr{
		Mode: out.Mode,
		Ino:  uint64(ino),
		Gen:  1,
	}), fileHandler, 0, syscall.F_OK
}

func (node *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.WithFields(
		log.Fields{
			"flags": flags,
			"inode": node.inode,
		}).Debug("Open")

	fh, err := GlobalGitFs.OpenFile(ctx, node.inode)
	if err != nil {
		return nil, 0, syscall.EIO
	}

	return fh, 0, syscall.F_OK
}

func (node *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	log.WithFields(
		log.Fields{
			"name":         name,
			"parent inode": node.inode,
		}).Debug("Rmdir")
	return node.Meta.Rmdir(ctx, node.inode, name)
}

func (node *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	log.WithFields(
		log.Fields{
			"name":  name,
			"inode": node.inode,
		}).Debug("Unlink")
	return node.Meta.Unlink(ctx, node.inode, name)
}

func (node *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	fields := log.Fields{}
	fields = make(map[string]interface{})
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
	log.WithFields(fields).Debug("Setattr")

	if f != nil {
		return f.(*FileHandler).Setattr(ctx, in, out)
	}

	attr, eno := node.Meta.Getattr(ctx, node.inode)
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

	err := node.Meta.SetattrDirectly(ctx, node.inode, attr)
	if err != nil {
		return syscall.EIO
	}

	metadata.ToAttrOut(node.inode, attr, &out.Attr)
	return syscall.F_OK
}
