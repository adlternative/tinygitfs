package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

const TotalInode = "totalinode"
const CurInode = "nextinode"
const UsedSpace = "usedspace"
const TotalSpace = "totalspace"

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
	}
	ts := time.Now()
	SetTime(&rootAttr.Atime, &rootAttr.Atimensec, ts)
	SetTime(&rootAttr.Mtime, &rootAttr.Mtimensec, ts)
	SetTime(&rootAttr.Ctime, &rootAttr.Ctimensec, ts)

	// root attr 序列化后写到 i1
	err := r.SetattrDirectly(ctx, rootInode, rootAttr)
	if err != nil {
		return err
	}
	r.SetTotalInodeCount(ctx, 1<<30)
	if err != nil {
		return err
	}
	r.SetTotalSpace(ctx, 1<<30)
	if err != nil {
		return err
	}
	return nil
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

	debug.PrintStack()
	log.WithError(err).Error("meet bad error")
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
	attr.Uid = uint32(uid)
	attr.Gid = uint32(gid)

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

	jsonAttr, err := json.Marshal(attr)
	r.rdb.Set(ctx, inodeKey(ino), jsonAttr, 0)
	//log.WithField("inode", ino).Info("create inode")

	err = r.SetDentry(ctx, parent, name, ino, _type)
	if err != nil {
		return nil, 0, errno(err)
	}
	eno := r.Ref(ctx, parent)
	if eno != syscall.F_OK {
		return nil, 0, eno
	}

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

func (r *RedisMeta) SetattrDirectly(ctx context.Context, ino Ino, attr *Attr) error {
	jsonAttr, err := json.Marshal(&attr)
	if err != nil {
		return err
	}
	_, err = r.rdb.Set(ctx, inodeKey(ino), jsonAttr, 0).Result()

	if err != nil {
		return err
	}
	return nil
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

func (r *RedisMeta) Ref(ctx context.Context, inode Ino) syscall.Errno {
	attr, eno := r.Getattr(ctx, inode)
	if eno != syscall.F_OK {
		return eno
	}
	attr.Nlink++
	err := r.SetattrDirectly(ctx, inode, attr)
	if err != nil {
		return errno(err)
	}
	return syscall.F_OK
}

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
		r.UpdateUsedSpace(ctx, -Align4K(attr.Length))
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

// nextInode get next inode which can be used
func (r *RedisMeta) nextInode(ctx context.Context) (Ino, error) {
	ino, err := r.rdb.Incr(ctx, CurInode).Uint64()
	if err != nil {
		return -1, err
	}
	if ino == 1 {
		ino, err = r.rdb.Incr(ctx, CurInode).Uint64()
		if err != nil {
			return -1, err
		}
	}
	return Ino(ino), err
}

func (r *RedisMeta) SetTotalInodeCount(ctx context.Context, totalInodeCount uint64) error {
	return r.rdb.Set(ctx, TotalInode, totalInodeCount, -1).Err()
}

func (r *RedisMeta) TotalInodeCount(ctx context.Context) (uint64, error) {
	return r.rdb.Get(ctx, TotalInode).Uint64()
}

func (r *RedisMeta) CurInodeCount(ctx context.Context) (uint64, error) {
	ino, err := r.rdb.Get(ctx, CurInode).Uint64()
	if err == redis.Nil {
		return 0, nil
	}
	return ino, err
}

func (r *RedisMeta) UpdateUsedSpace(ctx context.Context, size int64) error {
	return r.rdb.IncrBy(ctx, UsedSpace, size).Err()
}

func (r *RedisMeta) SetUseSpace(ctx context.Context, size int64) error {
	return r.rdb.Set(ctx, UsedSpace, size, -1).Err()
}

func (r *RedisMeta) UsedSpace(ctx context.Context) (uint64, error) {
	usedSpace, err := r.rdb.Get(ctx, UsedSpace).Uint64()
	if err == redis.Nil {
		return 0, nil
	}
	return usedSpace, err
}

func (r *RedisMeta) SetTotalSpace(ctx context.Context, totalSpace uint64) error {
	return r.rdb.Set(ctx, TotalSpace, totalSpace, -1).Err()
}

func (r *RedisMeta) TotalSpace(ctx context.Context) (uint64, error) {
	return r.rdb.Get(ctx, TotalSpace).Uint64()
}

// SetChunkMeta
// inode[pagenum] -> { offset. length, storagePath }
// because redis hash cannot support trim easily,
// so it will better to use redis list to do this.
func (r *RedisMeta) SetChunkMeta(ctx context.Context, inode Ino, pageNum int64, offset int64, lens int, storagePath string) error {
	log.WithFields(log.Fields{
		"inode":       inode,
		"pageNum":     pageNum,
		"offset":      offset,
		"length":      lens,
		"storagePath": storagePath,
	}).Debug("Redis SetChunkMeta")

	jsonChunkAttr, err := json.Marshal(&ChunkAttr{
		Offset:      offset,
		Length:      lens,
		StoragePath: storagePath,
	})
	if err != nil {
		return err
	}

	totalPageNum, err := r.rdb.LLen(ctx, chunkKey(inode)).Result()
	if pageNum == totalPageNum {
		return r.rdb.RPush(ctx, chunkKey(inode), jsonChunkAttr).Err()
	} else if pageNum < totalPageNum {
		return r.rdb.LSet(ctx, chunkKey(inode), pageNum, jsonChunkAttr).Err()
	} else {
		log.WithFields(
			log.Fields{
				"pageNum":      pageNum,
				"totalPageNum": totalPageNum,
			}).Errorf("set chunk meta out of file total pageNum")
		return fmt.Errorf("set chunk meta out of file total pageNum")
	}
}

func (r *RedisMeta) GetChunkMeta(ctx context.Context, inode Ino, pageNum int64) (*ChunkAttr, bool, error) {
	chunkAttr := &ChunkAttr{}

	jsonChunkAttr, err := r.rdb.LIndex(ctx, chunkKey(inode), pageNum).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, false, nil
		}
		return nil, false, err
	}

	err = json.Unmarshal(jsonChunkAttr, chunkAttr)
	if err != nil {
		return nil, false, err
	}
	return chunkAttr, true, nil
}

func (r *RedisMeta) TruncateChunkMeta(ctx context.Context, inode Ino, pageNum int64, lastPageLength int) error {
	log.WithFields(
		log.Fields{
			"inode":          inode,
			"pageNum":        pageNum,
			"lastPageLength": lastPageLength,
		}).Debug("TruncateChunkMeta")

	err := r.rdb.LTrim(ctx, chunkKey(inode), 0, pageNum).Err()
	if err != nil {
		return err
	}
	if lastPageLength == 0 {
		return r.rdb.RPop(ctx, chunkKey(inode)).Err()
	}

	lastChunkAttr, ok, err := r.GetChunkMeta(ctx, inode, pageNum)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("cannot find last chunk attr, inode=%d, pageNum=%d", inode, pageNum)
	}
	lastChunkAttr.Length = lastPageLength

	jsonChunkAttr, err := json.Marshal(lastChunkAttr)
	if err != nil {
		return err
	}

	return r.rdb.LSet(ctx, chunkKey(inode), pageNum, jsonChunkAttr).Err()
}

func (r *RedisMeta) ReadUpdate(ctx context.Context, inode Ino) syscall.Errno {
	attr, eno := r.Getattr(ctx, inode)
	if eno != syscall.F_OK {
		return eno
	}

	SetTime(&attr.Atime, &attr.Atimensec, time.Now())
	r.SetattrDirectly(ctx, inode, attr)
	return syscall.F_OK
}
