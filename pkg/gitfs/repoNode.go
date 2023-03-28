package gitfs

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
	"syscall"
)

type GitRepoNode struct {
	Node
}

func (node *GitRepoNode) NewNode(ino metadata.Ino, name string, _type uint8) fs.InodeEmbedder {
	switch name {
	case "HEAD", "HEAD.lock", "FETCH_HEAD", "FETCH_HEAD.lock", "ORIG_HEAD", "ORIG_HEAD.lock":
		return &GitSymRefNode{
			Node: Node{
				nodeType: "GitSymRefNode",
				inode:    ino,
				name:     name,
				gitfs:    node.Node.gitfs,
			},
		}
	case "refs":
		return &GitRefsNode{
			Node: Node{
				nodeType: "GitRefsNode",
				inode:    ino,
				name:     name,
				gitfs:    node.Node.gitfs,
			},
		}
	default:
		return &Node{
			nodeType: "Node",
			inode:    ino,
			name:     name,
			gitfs:    node.Node.gitfs,
		}
	}
}

func (node *GitRepoNode) NewFile(ctx context.Context, ino metadata.Ino, name string, _type uint8) (fs.FileHandle, syscall.Errno) {
	switch name {
	case "HEAD", "HEAD.lock", "FETCH_HEAD", "FETCH_HEAD.lock", "ORIG_HEAD", "ORIG_HEAD.lock":
		fileHandler, err := node.gitfs.OpenSymRefFile(ctx, ino)
		if err != nil {
			return nil, syscall.ENOENT
		}
		return fileHandler, syscall.F_OK
	default:
		fileHandler, err := node.gitfs.OpenFile(ctx, ino)
		if err != nil {
			return nil, syscall.ENOENT
		}
		return fileHandler, syscall.F_OK
	}
}

// Link hard link a file to another inode
func (node *GitRepoNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (newNode *fs.Inode, errno syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":      name,
			"target":    target.EmbeddedInode(),
			"inode":     node.inode,
			"node type": node.nodeType,
		}).Trace("Link")

	targetInode := metadata.Ino(target.EmbeddedInode().StableAttr().Ino)
	attr, eno := node.gitfs.DefaultDataSource.Meta.Link(ctx, node.inode, targetInode, name)
	if eno != syscall.F_OK {
		return nil, eno
	}

	metadata.ToAttrOut(targetInode, attr, &out.Attr)

	return node.NewInode(ctx, node.NewNode(targetInode, name, attr.Typ), fs.StableAttr{
		Mode: uint32(attr.Mode),
		Ino:  uint64(targetInode),
		Gen:  1,
	}), syscall.F_OK
}

// Mkdir create a directory, and create a fuse node for it
func (node *GitRepoNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":         name,
			"mode":         mode,
			"parent inode": node.inode,
			"node type":    node.nodeType,
		}).Debug("Mkdir")
	attr, ino, eno := node.gitfs.DefaultDataSource.Meta.MkNod(ctx, node.inode, metadata.TypeDirectory, name, mode, 0)
	if eno != 0 {
		return nil, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)

	log.WithFields(
		log.Fields{
			"inode": ino,
		}).Debug("Mkdir Result")

	newNode := node.NewNode(ino, name, metadata.TypeDirectory)
	return node.NewInode(ctx, newNode, fs.StableAttr{
		Mode: out.Mode,
		Ino:  uint64(ino),
		Gen:  1,
	}), 0
}

// Mknod create a node, and create a fuse node for it
func (node *GitRepoNode) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":         name,
			"mode":         mode,
			"dev":          dev,
			"parent inode": node.inode,
			"node type":    node.nodeType,
		}).Debug("Mknod")
	_type := metadata.GetFiletype(uint16(mode))
	if _type == 0 {
		return nil, syscall.EPERM
	}

	attr, ino, eno := node.gitfs.DefaultDataSource.Meta.MkNod(ctx, node.inode, _type, name, mode, dev)
	if eno != 0 {
		return nil, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)

	log.WithFields(
		log.Fields{
			"inode": ino,
		}).Debug("Mknod Result")

	newNode := node.NewNode(ino, name, attr.Typ)
	return node.NewInode(ctx, newNode, fs.StableAttr{
		Mode: out.Mode,
		Ino:  uint64(ino),
		Gen:  1,
	}), 0
}

// Create a file, and create a fuse node for it
func (node *GitRepoNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":         name,
			"flags":        flags,
			"mode":         mode,
			"parent inode": node.inode,
			"node type":    node.nodeType,
		}).Debug("Create")
	attr, ino, eno := node.gitfs.DefaultDataSource.Meta.MkNod(ctx, node.inode, metadata.TypeFile, name, mode, 0)
	if eno != 0 {
		return nil, 0, 0, eno
	}
	metadata.ToAttrOut(node.inode, attr, &out.Attr)

	log.WithFields(
		log.Fields{
			"inode": ino,
		}).Debug("Create Result")

	fileHandler, eno := node.NewFile(ctx, ino, name, attr.Typ)
	if eno != syscall.F_OK {
		return nil, 0, 0, eno
	}

	newNode := node.NewNode(ino, name, metadata.TypeFile)

	return node.NewInode(ctx, newNode, fs.StableAttr{
		Mode: out.Mode,
		Ino:  uint64(ino),
		Gen:  1,
	}), fileHandler, 0, syscall.F_OK
}

// Lookup check file info, and create a fuse node for it
func (node *GitRepoNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.WithFields(
		log.Fields{
			"name":         name,
			"parent inode": node.inode,
			"node type":    node.nodeType,
		}).Trace("Lookup")
	entry, find, err := node.gitfs.DefaultDataSource.Meta.GetDentry(ctx, node.inode, name)
	if err != nil || !find {
		return nil, syscall.ENOENT
	}
	attr, eno := node.gitfs.DefaultDataSource.Meta.Getattr(ctx, entry.Ino)
	if eno != 0 {
		return nil, eno
	}

	metadata.ToAttrOut(entry.Ino, attr, &out.Attr)

	//log.Printf("%s %d %o\n", name, entry.Ino, out.Attr.Mode)

	newNode := node.NewNode(entry.Ino, name, attr.Typ)
	return node.NewInode(ctx, newNode, fs.StableAttr{
		Mode: uint32(out.Mode),
		Ino:  uint64(entry.Ino),
		Gen:  1,
	}), 0

}
