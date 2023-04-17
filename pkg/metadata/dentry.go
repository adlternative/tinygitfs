package metadata

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/hanwen/go-fuse/v2/fuse"
	"syscall"
)

type DentryData struct {
	Ino Ino   `json:"inode"`
	Typ uint8 `json:"type"`
}

type Dentry struct {
	DentryData
	name string
}

func dentryKey(inode Ino) string {
	return "d" + inode.String()
}

type DirStream struct {
	ino  Ino
	meta *RedisMeta

	totalCnt int
	curPos   int

	dentries []*Dentry
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

// GetDentry check if directory parent have a dentry with the name, if have, return the dentry
func (r *RedisMeta) GetDentry(ctx context.Context, parent Ino, name string) (*Dentry, bool, error) {
	d := &Dentry{
		name: name,
	}
	data, err := r.rdb.HGet(ctx, dentryKey(parent), name).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, false, nil
		}
		return nil, false, err
	}

	err = json.Unmarshal(data, &d.DentryData)
	if err != nil {
		return nil, false, err
	}
	return d, true, nil
}

func (r *RedisMeta) SetDentry(ctx context.Context, parent Ino, name string, inode Ino, typ uint8) error {
	jsonDentry, err := json.Marshal(&DentryData{
		Ino: inode,
		Typ: typ,
	})
	if err != nil {
		return err
	}
	return r.rdb.HSet(ctx, dentryKey(parent), name, jsonDentry).Err()
}

func (r *RedisMeta) DelDentry(ctx context.Context, parent Ino, name string) error {
	return r.rdb.HDel(ctx, dentryKey(parent), name).Err()
}

// GetDirectoryLength get the dentries' number of the directory with specified inode
func (r *RedisMeta) GetDirectoryLength(ctx context.Context, ino Ino) (int64, error) {
	lens, err := r.rdb.HLen(ctx, dentryKey(ino)).Result()
	if err != nil {
		return 0, err
	}
	return lens, nil
}

// GetAllDentries return all dentries of the directory with specified inode
func (r *RedisMeta) GetAllDentries(ctx context.Context, ino Ino) ([]*Dentry, error) {
	var dentries []*Dentry

	result, err := r.rdb.HGetAll(ctx, dentryKey(ino)).Result()
	if err != nil {
		return dentries, err
	}

	for name, info := range result {
		dentry := &Dentry{
			name: name,
		}
		err := json.Unmarshal([]byte(info), &dentry.DentryData)
		if err != nil {
			return dentries, err
		}
		dentries = append(dentries, dentry)
	}
	return dentries, nil
}

// Rmdir remove a directory with name in parent inode
func (r *RedisMeta) Rmdir(ctx context.Context, parent Ino, name string) syscall.Errno {
	dentry, find, err := r.GetDentry(ctx, parent, name)
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
	if attr.Typ != TypeDirectory {
		return syscall.EPERM
	}
	if attr.Nlink != 2 {
		return syscall.ENOTEMPTY
	}
	attr.Nlink -= 2

	r.rdb.HDel(ctx, dentryKey(parent), name)
	r.rdb.Del(ctx, inodeKey(dentry.Ino))

	pattr, eno := r.Getattr(ctx, parent)
	pattr.Nlink--
	if err := r.SetattrDirectly(ctx, parent, pattr); err != nil {
		return errno(err)
	}

	return syscall.F_OK
}
