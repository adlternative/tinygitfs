package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type RedisMeta struct {
	rdb *redis.Client
}

func newRedisClient(url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %s", url, err)
	}
	if opt.Password == "" && os.Getenv("REDIS_PASSWORD") != "" {
		opt.Password = os.Getenv("REDIS_PASSWORD")
	}

	opt.MaxRetries = 3
	opt.MinRetryBackoff = time.Millisecond * 100
	opt.MaxRetryBackoff = time.Minute * 1
	opt.ReadTimeout = time.Second * 30
	opt.WriteTimeout = time.Second * 5

	return redis.NewClient(opt), nil
}

// NewRedisMeta create a new meta instenance
func NewRedisMeta(url string) (*RedisMeta, error) {
	rdb, err := newRedisClient(url)
	if err != nil {
		return nil, err
	}

	return &RedisMeta{
		rdb: rdb,
	}, nil
}

func (r *RedisMeta) Init(ctx context.Context) error {
	rootInode := Ino(1)

	// have initialed
	if _, err := r.rdb.Get(context.Background(), inodeKey(rootInode)).Result(); err == nil {
		return nil
	}

	rootAttr := &Attr{
		Typ:    TypeDirectory,
		Mode:   uint16(0755),
		Nlink:  2,
		Length: 4 << 10,
		Uid:    uint32(uid),
		Gid:    uint32(gid),
		Parent: 1,
	}
	ts := time.Now()
	SetTime(&rootAttr.Atime, &rootAttr.Atimensec, ts)
	SetTime(&rootAttr.Mtime, &rootAttr.Mtimensec, ts)
	SetTime(&rootAttr.Ctime, &rootAttr.Ctimensec, ts)

	// root attr 序列化后写到 i1
	jsonAttr, err := json.Marshal(&rootAttr)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, inodeKey(rootInode), jsonAttr, 0).Err()
}

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if eno, ok := err.(syscall.Errno); ok {
		return eno
	}
	if err == redis.Nil {
		return syscall.ENOENT
	}
	if strings.HasPrefix(err.Error(), "OOM") {
		return syscall.ENOSPC
	}
	return syscall.EIO
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

// MkNod create a new inode
func (r *RedisMeta) MkNod(ctx context.Context, parent Ino, _type uint8, name string, mode uint32, dev uint32) (*Attr, Ino, syscall.Errno) {
	attr := &Attr{}
	ino, err := r.nextInode(ctx)
	if err != nil {
		log.Error("get next inode failed")
		return nil, 0, errno(err)
	}

	ts := time.Now()
	SetTime(&attr.Atime, &attr.Atimensec, ts)
	SetTime(&attr.Mtime, &attr.Mtimensec, ts)
	SetTime(&attr.Ctime, &attr.Ctimensec, ts)

	attr.Typ = _type
	attr.Mode = uint16(mode)
	attr.Rdev = dev
	attr.Uid = uint32(os.Getuid())
	attr.Gid = uint32(os.Getgid())
	attr.Parent = parent

	if _type == TypeDirectory {
		attr.Nlink = 2
		attr.Length = 4 << 10
	} else {
		attr.Nlink = 1
		if _type == TypeSymlink {
			attr.Length = uint64(len(name))
		} else {
			attr.Length = 0
		}
	}
	// if parent.dentries have new node
	_, find, err := r.GetDentry(ctx, parent, name)
	if err != nil {
		log.WithError(err).Error("get dentry failed")
		return nil, 0, errno(err)
	}
	if find {
		attr, eno := r.Getattr(ctx, ino)
		if eno != 0 {
			log.WithField("errno", eno).Error("get attr failed")
			return nil, 0, eno
		}
		return attr, 0, syscall.EEXIST
	}

	pattr, eno := r.Getattr(ctx, parent)
	if eno != 0 {
		log.WithField("errno", eno).Error("get parent attr failed")
		return nil, 0, eno
	}

	pattr.Nlink++

	jsonAttr, err := json.Marshal(attr)
	r.rdb.Set(ctx, inodeKey(ino), jsonAttr, 0)

	jsonPAttr, err := json.Marshal(pattr)
	r.rdb.Set(ctx, inodeKey(parent), jsonPAttr, 0)

	jsonDentry, err := json.Marshal(&DentryData{
		Ino: ino,
		Typ: _type,
	})

	r.rdb.HSet(ctx, dentryKey(parent), name, jsonDentry)

	return attr, ino, 0
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

// nextInode get next inode which can be used
func (r *RedisMeta) nextInode(ctx context.Context) (Ino, error) {
	ino, err := r.rdb.Incr(ctx, "nextinode").Uint64()
	if err != nil {
		return -1, err
	}
	if ino == 1 {
		ino, err = r.rdb.Incr(ctx, "nextinode").Uint64()
		if err != nil {
			return -1, err
		}
	}
	return Ino(ino), err
}
