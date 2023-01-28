package page

import (
	"bytes"
	"context"
	"fmt"
	"github.com/adlternative/tinygitfs/pkg/datasource"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	"github.com/adlternative/tinygitfs/pkg/utils"
	"github.com/hanwen/go-fuse/v2/fuse"
	lru "github.com/hashicorp/golang-lru/v2"
	log "github.com/sirupsen/logrus"
	"io"
	"sync"
	"syscall"
)

const PageSize = 1 << 20
const PoolSize = 64 << 20

type Page struct {
	pageNumber int64
	data       []byte
	clean      bool
	size       int64
	mu         *sync.Mutex
}

func (p *Page) SetSize(size int64) {
	p.size = size
}

func NewPage(pageNumber int64) *Page {
	return &Page{
		pageNumber: pageNumber,
		data:       make([]byte, PageSize),
		clean:      true,
		size:       0,
		mu:         &sync.Mutex{},
	}
}

func NewPageWithReader(pageNumber int64, reader io.Reader, totalSize int64) (*Page, error) {
	page := NewPage(pageNumber)
	curSize := int64(0)
	for curSize < totalSize {
		n, err := reader.Read(page.data[curSize:])
		log.WithFields(log.Fields{
			"curSize":  curSize,
			"readSize": n,
		}).Debug("page read")

		if err != nil {
			if err == io.EOF {
				curSize += int64(n)
				break
			}
			return nil, err
		}
		curSize += int64(n)
	}

	log.WithFields(log.Fields{
		"curSize": curSize,
	}).Debug("page total")

	page.SetSize(curSize)
	return page, nil

}

type Pool struct {
	inode metadata.Ino
	cache *lru.Cache[int64, *Page]
	datasource.DataSource
	mu *sync.RWMutex
}

func NewPagePool(ctx context.Context, dataSource datasource.DataSource, inode metadata.Ino) (*Pool, error) {
	pool := &Pool{
		inode:      inode,
		DataSource: dataSource,
		mu:         &sync.RWMutex{},
	}

	cache, err := lru.NewWithEvict[int64, *Page](PoolSize/PageSize,
		func(pageNum int64, page *Page) {
			err := pool.FSyncPage(ctx, page)
			if err != nil {
				log.WithError(err).Error("page pool fsync failed")
			}
		})
	if err != nil {
		return nil, err
	}

	pool.cache = cache
	return pool, nil
}

func (p *Pool) Truncate(ctx context.Context, size uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

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
			page.size = int64(lastPageLength)
		}
	}

	return nil
}

func defaultCheck(key int64) bool {
	return true
}

func (p *Pool) Fsync(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fsync(ctx, defaultCheck)
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
		err := p.FSyncPage(ctx, page)
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

		page, err := p.GetPage(ctx, pageNum)
		if err != nil {
			return uint32(totalSize - leftSize), err
		}
		page.mu.Lock()
		copy(page.data[pageOffset:pageOffset+dataLen], data[dataOffset:dataOffset+dataLen])
		page.clean = false
		//p.dirtyPages.PushBack(page)
		if page.size < pageOffset+dataLen {
			page.SetSize(pageOffset + dataLen)
		}
		page.mu.Unlock()

		leftSize -= dataLen
		dataOffset += dataLen
		curOffset = (pageNum + 1) * PageSize
	}
	return uint32(totalSize - leftSize), nil
}

func (p *Pool) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	totalSize := int64(len(dest))
	leftSize := totalSize
	curOffset := off
	dataOffset := int64(0)

	for curPageSize := int64(PageSize); leftSize > 0 && curPageSize == PageSize; {
		pageNum := curOffset / PageSize
		pageOffset := curOffset % PageSize
		page, find, err := p.CheckPage(ctx, pageNum)
		if err != nil {
			return fuse.ReadResultData(dest[:totalSize-leftSize]), err
		}
		if !find {
			log.Debugf("cannot find inode %d chunk %d", p.inode, pageNum)
			break
		}
		page.mu.Lock()
		curPageSize = page.size

		if !page.clean {
			err := p.fSyncPage(ctx, page)
			if err != nil {
				page.mu.Unlock()
				return fuse.ReadResultData(dest[:totalSize-leftSize]), err
			}
		}
		dataSize := curPageSize - pageOffset
		if dataSize < 0 {
			page.mu.Unlock()
			break
		} else if dataSize > leftSize {
			dataSize = leftSize
		}

		//log.WithFields(
		//	log.Fields{
		//		"totalSize":  totalSize,
		//		"leftSize":   leftSize,
		//		"dataOffset": dataOffset,
		//		"dataSize":   dataSize,
		//		"pageOffset": pageOffset,
		//		"page.size":  page.size,
		//		"curOffset":  curOffset,
		//	}).Debug("Pool Read")

		copy(dest[dataOffset:dataOffset+dataSize], page.data[pageOffset:pageOffset+dataSize])

		page.mu.Unlock()

		leftSize -= dataSize
		dataOffset += dataSize
		curOffset = (pageNum + 1) * PageSize
	}

	return fuse.ReadResultData(dest[:totalSize-leftSize]), nil
}

func storagePath(inode metadata.Ino, pageNum int64) string {
	return fmt.Sprintf("chunks/%d/%d/%s", inode, pageNum, utils.RandStringBytes(32))
}

func (p *Pool) FSyncPage(ctx context.Context, page *Page) error {
	page.mu.Lock()
	defer page.mu.Unlock()

	return p.fSyncPage(ctx, page)
}

func (p *Pool) fSyncPage(ctx context.Context, page *Page) error {
	if page.clean {
		return nil
	}
	// TODO txn
	path := storagePath(p.inode, page.pageNumber)
	err := p.Data.Put(path, bytes.NewReader(page.data[:page.size]))
	if err != nil {
		log.WithError(err).Errorf("set chunk data failed")
		return err
	}
	err = p.Meta.SetChunkMeta(ctx, p.inode, page.pageNumber, page.pageNumber*PageSize, int(page.size), path)
	if err != nil {
		log.WithError(err).Errorf("set chunk metadata failed")
		return err
	}

	eno := p.Meta.WriteUpdate(ctx, p.inode, uint64(page.pageNumber*PageSize+page.size))
	if eno != syscall.F_OK {
		log.WithError(err).Errorf("write update failed")
		return fmt.Errorf("write update failed")
	}

	page.clean = true
	return nil
}

// GetPage if cache have the page, return it; otherwise load from disk
func (p *Pool) GetPage(ctx context.Context, pageNum int64) (*Page, error) {
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
func (p *Pool) CheckPage(ctx context.Context, pageNum int64) (*Page, bool, error) {
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

	reader, err := p.Data.Get(chunkAttr.StoragePath, 0, int64(chunkAttr.Length))
	if err != nil {
		return nil, false, err
	} else {
		page, err = NewPageWithReader(pageNum, reader, int64(chunkAttr.Length))
		if err != nil {
			return nil, false, err
		}
	}

	return page, true, nil
}
