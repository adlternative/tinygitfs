package metadata

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"os"
	"runtime/debug"
	"strings"
	"syscall"
	"time"
)

var (
	uid int
	gid int
)

func init() {
	uid = os.Getuid()
	gid = os.Getgid()
}

type RedisMeta struct {
	rdb *redis.Client
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

// SetTime set given time and timesec to given time.Time
func SetTime(time *uint64, timensec *uint32, t time.Time) {
	sec := uint64(t.Unix())
	nsec := uint32(t.Nanosecond())

	if time != nil {
		*time = sec
	}
	if timensec != nil {
		*timensec = nsec
	}
}
