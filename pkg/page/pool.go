package page

import (
	"context"
	"fmt"
	"sync"
	"syscall"

	"github.com/adlternative/tinygitfs/pkg/datasource"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/adlternative/tinygitfs/pkg/utils"
	"github.com/hanwen/go-fuse/v2/fuse"
	lru "github.com/hashicorp/golang-lru/v2"
	log "github.com/sirupsen/logrus"
)

const PoolSize = 64 << 20

type Pool struct {
	inode metadata.Ino
	cache *lru.Cache[int64, *Page]
	datasource.DataSource

	mu      *sync.RWMutex
	memAttr *MemAttr
}

func NewPagePool(ctx context.Context, dataSource datasource.DataSource, inode metadata.Ino) (*Pool, error) {
	pool := &Pool{
		inode:      inode,
		DataSource: dataSource,
		mu:         &sync.RWMutex{},
	}

	memAttr, err := NewMemAttr(ctx, pool, inode)
	if err != nil {
		return nil, err
	}
	pool.memAttr = memAttr

	cache, err := lru.NewWithEvict[int64, *Page](PoolSize/PageSize,
		func(pageNum int64, page *Page) {
			err := page.Fsync(ctx, dataSource, inode)
			if err != nil {
				log.WithFields(log.Fields{
					"pageNum": pageNum,
					"inode":   inode,
				}).WithError(err).Error("page fsync failed")
			}
		})
	if err != nil {
		return nil, err
	}
	pool.cache = cache

	return pool, nil
}

func (p *Pool) MemAttr() *MemAttr {
	return p.memAttr
}

// TruncateWithLock remove/truncate the cache pages which offset larger than size\
func (p *Pool) TruncateWithLock(ctx context.Context, size uint64) error {
	lastPageNum := int64(size / PageSize)
	lastPageLength := int(size % PageSize)

	// truncate page pool
	for _, pageNum := range p.cache.Keys() {
		if pageNum > lastPageNum || (lastPageLength == 0 && pageNum == lastPageNum) {
			p.cache.Remove(pageNum)
		} else if pageNum == lastPageNum {
			page, ok := p.cache.Peek(pageNum)
			if !ok {
				return fmt.Errorf("pagepool cache key %d peek failed", pageNum)
			}
			page.Truncate(int64(lastPageLength))
		}
	}

	return nil
}

// Fsync write all dirty pages to minio, and write the meta of the file
func (p *Pool) Fsync(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	attr, eno := p.Meta.Getattr(ctx, p.inode)
	if eno != syscall.F_OK {
		if eno == syscall.ENOENT {
			log.WithFields(
				log.Fields{
					"inode": p.inode,
				}).Debugf("fsync: inode not found, but ignore the error")
			return nil
		}
		return eno
	}

	// data page -> minio
	err := p.fsync(ctx, defaultCheck)
	if err != nil {
		return err
	}

	// metadata inode attr -> redis
	curLength := attr.Length
	memattr := p.MemAttr()
	memattr.CopyToAttr(attr)
	p.Meta.SetattrDirectly(ctx, p.inode, attr)
	if attr.Length > curLength {
		err = p.Meta.UpdateUsedSpace(ctx, int64(attr.Length-curLength))
		if err != nil {
			return err
		}
	} else if attr.Length < curLength {
		// if file truncate, chunk metadata -> redis
		lastPageNum := int64(attr.Length / PageSize)
		lastPageLength := int(attr.Length % PageSize)
		err = p.Meta.TruncateChunkMeta(ctx, p.inode, lastPageNum, lastPageLength)
		if err != nil {
			return err
		}

		err = p.Meta.UpdateUsedSpace(ctx, -int64(curLength-attr.Length))
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pool) fsync(ctx context.Context, checkFn func(int64) bool) error {
	for _, k := range p.cache.Keys() {
		if !checkFn(k) {
			continue
		}

		page, ok := p.cache.Peek(k)
		if !ok {
			return fmt.Errorf("pagepool cache key %d peek failed", k)
		}
		err := page.Fsync(ctx, p.DataSource, p.inode)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pool) Write(ctx context.Context, data []byte, off int64) (uint32, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	totalSize := int64(len(data))
	leftSize := totalSize
	curOffset := off
	dataOffset := int64(0)

	for leftSize > 0 {
		pageNum := curOffset / PageSize
		pageOffset := curOffset % PageSize
		dataLen := PageSize - pageOffset
		if dataLen > leftSize {
			dataLen = leftSize
		}

		page, err := p.getPage(ctx, pageNum)
		if err != nil {
			return uint32(totalSize - leftSize), err
		}

		page.Write(pageOffset, data[dataOffset:dataOffset+dataLen])

		leftSize -= dataLen
		dataOffset += dataLen
		curOffset = (pageNum + 1) * PageSize
		p.MemAttr().UpdateLengthIfMore(uint64(off + totalSize - leftSize))
	}
	return uint32(totalSize - leftSize), nil
}

func (p *Pool) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	totalSize := int64(len(dest))

	memAttr := p.MemAttr()
	fileLength := int64(memAttr.Length())

	if fileLength < totalSize {
		totalSize = fileLength
	}

	leftSize := totalSize
	curOffset := off
	dataOffset := int64(0)

	for leftSize > 0 {
		pageNum := curOffset / PageSize
		pageOffset := curOffset % PageSize

		page, find, err := p.checkPage(ctx, pageNum)
		if err != nil {
			return fuse.ReadResultData(dest[:totalSize-leftSize]), err
		}
		if !find {
			log.Debugf("cannot find inode %d chunk %d", p.inode, pageNum)
			break
		}

		dataSize := page.Read(pageOffset, dest[dataOffset:], leftSize)

		leftSize -= dataSize
		dataOffset += dataSize
		curOffset = (pageNum + 1) * PageSize

		if dataSize < PageSize {
			break
		}
	}

	return fuse.ReadResultData(dest[:totalSize-leftSize]), nil
}

// getPage if cache have the page, return it; otherwise load from disk
func (p *Pool) getPage(ctx context.Context, pageNum int64) (*Page, error) {
	if int64(p.MemAttr().Length()) <= pageNum*PageSize {
		page := NewPage(pageNum)
		p.cache.Add(pageNum, page)
		return page, nil
	}

	page, find := p.cache.Get(pageNum)
	if !find {
		var err error

		page, find, err = p.loadPage(ctx, pageNum)
		if err != nil {
			return nil, err
		}
		if !find {
			page = NewPage(pageNum)
		}
		p.cache.Add(pageNum, page)
	}
	return page, nil
}

// CheckPage check if cache and disk have the chunk, if so, load it to page; else return non-exist
func (p *Pool) checkPage(ctx context.Context, pageNum int64) (*Page, bool, error) {
	if int64(p.MemAttr().Length()) <= pageNum*PageSize {
		return nil, false, nil
	}

	page, find := p.cache.Get(pageNum)
	if !find {
		var err error

		page, find, err = p.loadPage(ctx, pageNum)
		if err != nil {
			return nil, false, err
		}
		if !find {
			return nil, false, nil
		}
		p.cache.Add(pageNum, page)
	}
	return page, true, nil
}

// loadPage check if disk have the page, if so, load it; else return false
func (p *Pool) loadPage(ctx context.Context, pageNum int64) (*Page, bool, error) {
	var page *Page

	chunkAttr, find, err := p.Meta.GetChunkMeta(ctx, p.inode, pageNum)
	if err != nil {
		return nil, false, err
	}
	if !find {
		return nil, false, nil
	}

	reader, err := p.Data.Get(chunkAttr.StoragePath, 0, PageSize)
	if err != nil {
		return nil, false, err
	} else {
		page, err = NewPageWithReader(pageNum, reader, PageSize)
		if err != nil {
			return nil, false, err
		}
	}

	return page, true, nil
}

func (p *Pool) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.MemAttr().Setattr(ctx, in, out)
}

func defaultCheck(key int64) bool {
	return true
}

func storagePath(inode metadata.Ino, pageNum int64) string {
	return fmt.Sprintf("chunks/%d/%d/%s", inode, pageNum, utils.RandStringBytes(32))
}
