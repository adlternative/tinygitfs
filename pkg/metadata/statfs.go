package metadata

import (
	"context"
	"github.com/go-redis/redis/v8"
)

const TotalInode = "totalinode"
const CurInode = "nextinode"
const UsedSpace = "usedspace"
const TotalSpace = "totalspace"

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
