package gitfs

import (
	"context"
	"fmt"
	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/datasource"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"os"
	"runtime"
	"sync"
)

type GitFs struct {
	*Node
	files   map[metadata.Ino]*File
	filesMu *sync.Mutex
}

func (gitFs *GitFs) OpenFile(ctx context.Context, inode metadata.Ino) (*FileHandler, error) {
	var err error

	gitFs.filesMu.Lock()
	defer gitFs.filesMu.Unlock()

	file, ok := gitFs.files[inode]
	if !ok {
		file, err = NewFile(ctx, inode, gitFs.DataSource, gitFs)
		if err != nil {
			return nil, err
		}
		gitFs.files[inode] = file
	}
	return file.NewFileHandler(), nil
}

func (gitFs *GitFs) ReleaseFile(ctx context.Context, inode metadata.Ino) error {
	gitFs.filesMu.Lock()
	defer gitFs.filesMu.Unlock()

	file, ok := gitFs.files[inode]
	if !ok {
		return fmt.Errorf("cannot find the file want to release: %d", inode)
	}
	return file.UnRef(func() {
		delete(gitFs.files, inode)
	})
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

	root := &Node{
		inode: 1,
		name:  "",
		DataSource: datasource.DataSource{
			Meta: Meta,
			Data: minioData,
		},
		newNodeFn: defaultNewNode,
	}

	gitfs := &GitFs{
		files:   make(map[metadata.Ino]*File),
		filesMu: &sync.Mutex{},
		Node:    root,
	}
	root.gitfs = gitfs

	return gitfs, nil
}

func Mount(ctx context.Context, mntDir string, debug bool, metaDataUrl string, dataOption *data.Option) (*fuse.Server, error) {
	var err error

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
		RootStableAttr: &fs.StableAttr{
			Ino: uint64(gitfs.inode),
			Gen: 1,
		},
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}
