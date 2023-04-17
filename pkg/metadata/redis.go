package metadata

import (
	"context"
	"syscall"
)

func (r *RedisMeta) Link(ctx context.Context, parent Ino, target Ino, name string) (*Attr, syscall.Errno) {
	return r.link(ctx, parent, target, name, false)
}

func (r *RedisMeta) link(ctx context.Context, parent Ino, target Ino, name string, allowLinkDir bool) (*Attr, syscall.Errno) {
	_, find, err := r.GetDentry(ctx, parent, name)
	if err != nil {
		return nil, errno(err)
	}
	if find {
		return nil, syscall.EEXIST
	}

	// target.link++
	attr, eno := r.Getattr(ctx, target)
	if eno != syscall.F_OK {
		return nil, eno
	}
	if !allowLinkDir && attr.Typ == TypeDirectory {
		return nil, syscall.EISDIR
	}

	attr.Nlink++
	r.SetattrDirectly(ctx, target, attr)

	// d[parent][name] = target
	err = r.SetDentry(ctx, parent, name, target, attr.Typ)
	if err != nil {
		return nil, errno(err)
	}

	// parent.link++
	eno = r.Ref(ctx, parent)
	if eno != syscall.F_OK {
		return nil, eno
	}
	return attr, syscall.F_OK
}

func (r *RedisMeta) Unlink(ctx context.Context, parent Ino, name string) syscall.Errno {
	return r.unlink(ctx, parent, name, false)
}

func (r *RedisMeta) unlink(ctx context.Context, parent Ino, name string, allowUnlinkDir bool) syscall.Errno {
	dentry, find, err := r.GetDentry(ctx, parent, name)
	if !allowUnlinkDir && dentry.Typ == TypeDirectory {
		return syscall.EISDIR
	}
	if err != nil {
		return errno(err)
	}
	if !find {
		return syscall.ENOENT
	}
	attr, eno := r.Getattr(ctx, dentry.Ino)
	if eno != syscall.F_OK {
		return eno
	}

	attr.Nlink--

	r.rdb.HDel(ctx, dentryKey(parent), name)
	if attr.Nlink == 0 {
		r.rdb.Del(ctx, inodeKey(dentry.Ino))
		r.UpdateUsedSpace(ctx, -int64(attr.Length))
	} else if err := r.SetattrDirectly(ctx, dentry.Ino, attr); err != nil {
		return errno(err)
	}

	pattr, eno := r.Getattr(ctx, parent)
	pattr.Nlink--
	if err := r.SetattrDirectly(ctx, parent, pattr); err != nil {
		return errno(err)
	}

	return syscall.F_OK
}

func (r *RedisMeta) Rename(ctx context.Context, parent Ino, oldName string, newParent Ino, newName string) syscall.Errno {
	if parent == newParent && oldName == newName {
		return syscall.F_OK
	}

	dentry, find, err := r.GetDentry(ctx, parent, oldName)
	if err != nil {
		return errno(err)
	}
	if !find {
		return syscall.ENOENT
	}

	replaceDentry, find, err := r.GetDentry(ctx, newParent, newName)
	if err != nil {
		return errno(err)
	}
	// if newDir[newName] exists, unlink it
	if find {
		if replaceDentry.Typ == TypeDirectory {
			return syscall.EISDIR
		}
		if dentry.Typ == TypeDirectory {
			return syscall.ENOTDIR
		}

		eno := r.Unlink(ctx, newParent, newName)
		if eno != syscall.F_OK {
			return eno
		}
	}
	// link newDir[newName]
	_, eno := r.link(ctx, newParent, dentry.Ino, newName, true)
	if eno != syscall.F_OK {
		return eno
	}
	// unlink
	eno = r.unlink(ctx, parent, oldName, true)
	if eno != syscall.F_OK {
		return eno
	}

	return syscall.F_OK
}
