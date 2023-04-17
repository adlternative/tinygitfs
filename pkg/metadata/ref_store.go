package metadata

import "context"

func refKey(inode Ino) string {
	return "r" + inode.String()
}

func (r *RedisMeta) RefSet(ctx context.Context, inode Ino, value string) error {
	return r.rdb.Set(ctx, refKey(inode), value, -1).Err()
}

func (r *RedisMeta) RefGet(ctx context.Context, inode Ino) (string, error) {
	return r.rdb.Get(ctx, refKey(inode)).Result()
}

func (r *RedisMeta) RefDel(ctx context.Context, inode Ino) error {
	return r.rdb.Del(ctx, refKey(inode)).Err()
}
