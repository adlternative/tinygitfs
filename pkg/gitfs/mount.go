package gitfs

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/datasource"
	"os"
	"runtime"
	"syscall"

	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
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

type GitFs struct {
	Node
}

func NewGitFs(ctx context.Context, metaDataUrl string, dataOption *data.Option) (*GitFs, error) {
	Meta, err := metadata.NewRedisMeta(metaDataUrl)
	if err != nil {
		return nil, err
	}
	err = Meta.Init(ctx)
	if err != nil {
		return nil, err
	}

	minioData, err := data.NewMinioData(dataOption)
	if err != nil {
		return nil, err
	}
	err = minioData.Init()
	if err != nil {
		return nil, err
	}

	return &GitFs{
		Node: Node{
			inode: 1,
			name:  "",
			DataSource: datasource.DataSource{
				Meta: Meta,
				Data: minioData,
			},
			newNodeFn: defaultNewNode,
		},
	}, nil
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
	if newParentInode == 0 {
		newParentInode = 1
	}

	log.WithFields(
		log.Fields{
			"name":      name,
			"newParent": newParent.EmbeddedInode(),
			"newName":   newName,
			"flags":     flags,
			"inode":     node.inode,
		}).Trace("Rename")

	return node.Meta.Rename(ctx, node.inode, name, metadata.Ino(newParentInode), newName)
}

func (node *Node) Opendir(ctx context.Context) syscall.Errno {
	log.WithFields(
		log.Fields{
			"inode": node.inode,
		}).Trace("Opendir")
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
		Mode: uint32(attr.Mode),
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
		}).Trace("Mkdir")
	attr, ino, eno := node.Meta.MkNod(ctx, node.inode, metadata.TypeDirectory, name, mode, 0)
	if eno != 0 {
		return nil, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)
	return node.NewInode(ctx, node.newNodeFn(node.DataSource, ino, name), fs.StableAttr{
		Mode: mode,
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
		}).Trace("Mknod")
	_type := metadata.GetFiletype(uint16(mode))
	if _type == 0 {
		return nil, syscall.EPERM
	}

	attr, ino, eno := node.Meta.MkNod(ctx, node.inode, _type, name, mode, dev)
	if eno != 0 {
		return nil, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)
	return node.NewInode(ctx, node.newNodeFn(node.DataSource, ino, name), fs.StableAttr{
		Mode: mode,
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
		}).Trace("Create")
	attr, ino, eno := node.Meta.MkNod(ctx, node.inode, metadata.TypeFile, name, mode, 0)
	if eno != 0 {
		return nil, 0, 0, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)

	fileHandler, err := NewFileHandler(ctx, ino, node.DataSource)
	if err != nil {
		return nil, 0, 0, syscall.ENOATTR
	}
	return node.NewInode(ctx, node.newNodeFn(node.DataSource, ino, name), fs.StableAttr{
		Mode: mode,
		Ino:  uint64(ino),
		Gen:  1,
	}), fileHandler, 0, syscall.F_OK
}

func (node *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if (flags & syscall.O_RDONLY) != 0 {
		return nil, 0, syscall.EROFS
	}
	fileHandler, err := NewFileHandler(ctx, node.inode, node.DataSource)
	if err != nil {
		return nil, 0, syscall.ENOATTR
	}
	return fileHandler, 0, syscall.F_OK
}

func (node *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	log.WithFields(
		log.Fields{
			"name":         name,
			"parent inode": node.inode,
		}).Trace("Rmdir")
	return node.Meta.Rmdir(ctx, node.inode, name)
}

func (node *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	log.WithFields(
		log.Fields{
			"name":         name,
			"parent inode": node.inode,
		}).Trace("Unlink")
	return node.Meta.Unlink(ctx, node.inode, name)
}

func (node *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	fields := log.Fields{}
	fields = make(map[string]interface{})

	fields["parent inode"] = node.inode

	if atime, ok := in.GetATime(); ok {
		fields["atime"] = atime
	}
	if ctime, ok := in.GetCTime(); ok {
		fields["ctime"] = ctime
	}
	if uid, ok := in.GetUID(); ok {
		fields["uid"] = uid
	}
	if gid, ok := in.GetUID(); ok {
		fields["gid"] = gid
	}
	if mode, ok := in.GetMode(); ok {
		fields["mode"] = mode
	}
	if size, ok := in.GetSize(); ok {
		fields["size"] = size
	}

	log.WithFields(fields).Trace("Setattr")

	attr, eno := node.Meta.Setattr(ctx, node.inode, in)
	if eno != syscall.F_OK {
		return eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)
	return syscall.F_OK
}

func Mount(ctx context.Context, mntDir string, debug bool, metaDataUrl string, dataOption *data.Option) (*fuse.Server, error) {
	gitfs, err := NewGitFs(ctx, metaDataUrl, dataOption)
	if err != nil {
		return nil, err
	}

	opts := fuse.MountOptions{
		FsName:               "tinygitfs",
		Name:                 "tinygitfs",
		SingleThreaded:       false,
		MaxBackground:        50,
		EnableLocks:          true,
		DisableXAttrs:        true,
		IgnoreSecurityLabels: true,
		MaxWrite:             1 << 20,
		MaxReadAhead:         1 << 20,
		DirectMount:          true,
		AllowOther:           os.Getuid() == 0,
		Debug:                debug,
	}

	if opts.AllowOther {
		// Make the kernel check file permissions for us
		opts.Options = append(opts.Options, "default_permissions")
	}
	if runtime.GOOS == "darwin" {
		opts.Options = append(opts.Options, "fssubtype=tinygitfs")
		opts.Options = append(opts.Options, "volname=tinygitfs")
		opts.Options = append(opts.Options, "daemon_timeout=60", "iosize=65536", "novncache")
	}

	server, err := fs.Mount(mntDir, gitfs, &fs.Options{
		MountOptions: opts,
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}
