package gitfs

import (
	"context"
	"os"
	"runtime"
	"syscall"

	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
)

type NewNodeFn = func(dataSource DataSource, ino metadata.Ino, name string) fs.InodeEmbedder

func defaultNewNode(dataSource DataSource, ino metadata.Ino, name string) fs.InodeEmbedder {
	return &Node{
		inode:      ino,
		name:       name,
		DataSource: dataSource,
		newNodeFn:  defaultNewNode,
	}
}

type DataSource struct {
	redisMeta *metadata.RedisMeta
}

type Node struct {
	fs.Inode

	inode metadata.Ino
	name  string

	DataSource
	newNodeFn NewNodeFn
}

type GitFs struct {
	Node
}

func NewGitFs(ctx context.Context, metaDataUrl string) (*GitFs, error) {
	redisMeta, err := metadata.NewRedisMeta(metaDataUrl)
	if err != nil {
		return nil, err
	}
	err = redisMeta.Init(ctx)
	if err != nil {
		return nil, err
	}

	return &GitFs{
		Node: Node{
			inode:      1,
			name:       "",
			DataSource: DataSource{redisMeta: redisMeta},
			newNodeFn:  defaultNewNode,
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

func (node *Node) Opendir(ctx context.Context) syscall.Errno {
	log.WithFields(
		log.Fields{
			"inode": node.inode,
		}).Info("Opendir")
	return syscall.F_OK
}

func (node *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"parent inode": node.inode,
		}).Info("Readdir")
	ds, err := metadata.NewDirStream(ctx, node.inode, node.redisMeta)
	if err != nil {
		return nil, syscall.ENOENT
	}
	return ds, 0
}

func (node *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.WithFields(
		log.Fields{
			"inode": node.inode,
		}).Info("Getattr")

	attr, eno := node.redisMeta.Getattr(ctx, node.inode)
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
		}).Info("Lookup")
	entry, find, err := node.redisMeta.GetDentry(ctx, node.inode, name)
	if err != nil || !find {
		return nil, syscall.ENOENT
	}
	attr, eno := node.redisMeta.Getattr(ctx, entry.Ino)
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
		}).Info("Mkdir")
	attr, ino, eno := node.redisMeta.MkNod(ctx, node.inode, metadata.TypeDirectory, name, mode, 0)
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
		}).Info("Mknod")
	_type := metadata.GetFiletype(uint16(mode))
	if _type == 0 {
		return nil, syscall.EPERM
	}

	attr, ino, eno := node.redisMeta.MkNod(ctx, node.inode, _type, name, mode, dev)
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
		}).Info("Create")
	attr, ino, eno := node.redisMeta.MkNod(ctx, node.inode, metadata.TypeFile, name, mode, 0)
	if eno != 0 {
		return nil, 0, 0, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)
	return node.NewInode(ctx, node.newNodeFn(node.DataSource, ino, name), fs.StableAttr{
		Mode: mode,
		Ino:  uint64(ino),
		Gen:  1,
	}), 0, 0, 0
}

func (node *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	log.WithFields(
		log.Fields{
			"name":         name,
			"parent inode": node.inode,
		}).Info("Rmdir")
	return node.redisMeta.Rmdir(ctx, node.inode, name)
}

func (node *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	log.WithFields(
		log.Fields{
			"name":         name,
			"parent inode": node.inode,
		}).Info("Unlink")
	return node.redisMeta.Unlink(ctx, node.inode, name)
}

func (node *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var fields log.Fields

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
		fields["mode"] = size
	}

	log.WithFields(fields).Info("Setattr")

	attr, eno := node.redisMeta.Setattr(ctx, node.inode, in)
	if eno != syscall.F_OK {
		return eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)
	return syscall.F_OK
}

func Mount(ctx context.Context, mntDir string, debug bool, metaDataUrl string) error {
	gitfs, err := NewGitFs(ctx, metaDataUrl)
	if err != nil {
		return err
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
		return err
	}

	server.Wait()

	return nil
}
