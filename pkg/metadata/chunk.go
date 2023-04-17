package metadata

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"strconv"
)

type ChunkAttr struct {
	Offset      int64  `json:"offset"`
	Length      int    `json:"length"`
	StoragePath string `json:"storagePath"`
}

func chunkKey(inode Ino) string {
	return "c" + inode.String()
}

// SetChunkMeta
// inode[pagenum] -> { offset. length, storagePath }
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

	err = r.rdb.HSet(ctx, chunkKey(inode), pageNum, jsonChunkAttr).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisMeta) DeleteChunkMeta(ctx context.Context, inode Ino, pageNum int64) error {
	log.WithFields(log.Fields{
		"inode":   inode,
		"pageNum": pageNum,
	}).Debug("Redis DeleteChunkMeta")

	err := r.rdb.HDel(ctx, chunkKey(inode), strconv.FormatInt(pageNum, 10)).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisMeta) GetChunkMeta(ctx context.Context, inode Ino, pageNum int64) (*ChunkAttr, bool, error) {
	chunkAttr := &ChunkAttr{}

	jsonChunkAttr, err := r.rdb.HGet(ctx, chunkKey(inode), strconv.FormatInt(pageNum, 10)).Bytes()
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

func (r *RedisMeta) TruncateChunkMeta(ctx context.Context, inode Ino, lastPageNum int64, lastPageLength int) error {
	chunkAttrs, err := r.rdb.HGetAll(ctx, chunkKey(inode)).Result()
	if err != nil {
		return err
	}

	for key, jsonChunkAttr := range chunkAttrs {
		curPageNum, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return err
		}
		if curPageNum > lastPageNum || (curPageNum == lastPageNum && lastPageLength == 0) {
			err := r.rdb.HDel(ctx, chunkKey(inode), key).Err()
			if err != nil {
				return err
			}
		} else if curPageNum == lastPageNum {
			chunkAttr := &ChunkAttr{}
			err = json.Unmarshal([]byte(jsonChunkAttr), chunkAttr)
			if err != nil {
				return err
			}
			chunkAttr.Length = lastPageLength
			jsonChunkAttr, err := json.Marshal(chunkAttr)
			if err != nil {
				return err
			}

			err = r.rdb.HSet(ctx, chunkKey(inode), lastPageNum, jsonChunkAttr).Err()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
