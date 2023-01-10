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
)

const pageSize = 1 << 20
const poolSize = 64 << 20

type Page struct {
	pageNumber int64
	data       []byte
	clean      bool
	size       int64
}

func (p *Page) SetSize(size int64) {
	p.size = size
}

func NewPage(pageNumber int64) *Page {
	return &Page{
		pageNumber: pageNumber,
		data:       make([]byte, pageSize),
		clean:      true,
		size:       0,
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
}

func NewPagePool(ctx context.Context, dataSource datasource.DataSource, inode metadata.Ino) (*Pool, error) {
	cache, err := lru.NewWithEvict[int64, *Page](poolSize/pageSize,
		func(pageNum int64, page *Page) {
			if !page.clean {
				// TODO txn
				path := storagePath(inode, pageNum)

				err := dataSource.Data.Put(path, bytes.NewReader(page.data[:page.size]))
				if err != nil {
					log.WithError(err).Errorf("set chunk data failed")
					return
				}
				err = dataSource.Meta.SetChunkMeta(ctx, inode, page.pageNumber, page.pageNumber*pageSize, int(page.size), path)
				if err != nil {
					log.WithError(err).Errorf("set chunk metadata failed")
					return
				}
				page.clean = true
			}
		})
	if err != nil {
		return nil, err
	}
	return &Pool{
		inode:      inode,
		DataSource: dataSource,
		cache:      cache,
	}, nil
}

func (p *Pool) Purge() {
	log.WithField("page pool size", p.cache.Len()).Infof("page pool purge")
	p.cache.Purge()
}

func (p *Pool) Write(ctx context.Context, data []byte, off int64) (uint32, error) {
	totalSize := int64(len(data))
	leftSize := totalSize
	curOffset := off
	dataOffset := int64(0)

	for leftSize > 0 {
		pageNum := curOffset / pageSize
		pageOffset := curOffset % pageSize
		dataLen := pageSize - pageOffset
		if dataLen > leftSize {
			dataLen = leftSize
		}

		page, err := p.GetPage(ctx, pageNum)
		if err != nil {
			return uint32(totalSize - leftSize), err
		}
		copy(page.data[pageOffset:pageOffset+dataLen], data[dataOffset:dataOffset+dataLen])
		page.clean = false
		if page.size < pageOffset+dataLen {
			page.SetSize(pageOffset + dataLen)
		}
		leftSize -= dataLen
		dataOffset += dataLen
		curOffset = (pageNum + 1) * pageSize
	}
	return uint32(totalSize - leftSize), nil
}

func (p *Pool) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, error) {
	totalSize := int64(len(dest))
	leftSize := totalSize
	curOffset := off
	dataOffset := int64(0)

	for leftSize > 0 {
		pageNum := curOffset / pageSize
		pageOffset := curOffset % pageSize
		page, find, err := p.CheckPage(ctx, pageNum)
		if err != nil {
			return fuse.ReadResultData(dest[:totalSize-leftSize]), err
		}
		if !find {
			return fuse.ReadResultData(dest[:totalSize-leftSize]), fmt.Errorf("cannot find inode %d chunk %d", p.inode, pageNum)
		}
		if !page.clean {
			err := p.SyncPage(ctx, pageNum, page)
			if err != nil {
				return fuse.ReadResultData(dest[:totalSize-leftSize]), err
			}
		}

		if pageOffset > page.size {
			return fuse.ReadResultData(dest[:totalSize-leftSize]),
				fmt.Errorf("read inode %d chunk %d out of range: pageOffset:%d > page.size:%d", p.inode, pageNum, pageOffset, page.size)
		}

		dataSize := page.size - pageOffset
		if dataSize > leftSize {
			dataSize = leftSize
		}

		log.WithFields(
			log.Fields{
				"totalSize":  totalSize,
				"leftSize":   leftSize,
				"dataOffset": dataOffset,
				"dataSize":   dataSize,
				"pageOffset": pageOffset,
				"page.size":  page.size,
				"curOffset":  curOffset,
			}).Debug("Pool Read")

		copy(dest[dataOffset:dataOffset+dataSize], page.data[pageOffset:pageOffset+dataSize])

		leftSize -= dataSize
		dataOffset += dataSize
		curOffset = (pageNum + 1) * pageSize
	}

	return fuse.ReadResultData(dest[:totalSize-leftSize]), nil
}

func storagePath(inode metadata.Ino, pageNum int64) string {
	return fmt.Sprintf("chunks/%d/%d/%s", inode, pageNum, utils.RandStringBytes(32))
}

func (p *Pool) SyncPage(ctx context.Context, pageNum int64, page *Page) error {
	if page.clean {
		return nil
	}
	// TODO txn
	path := storagePath(p.inode, pageNum)
	err := p.Data.Put(path, bytes.NewReader(page.data[:page.size]))
	if err != nil {
		log.WithError(err).Errorf("set chunk data failed")
		return err
	}
	err = p.Meta.SetChunkMeta(ctx, p.inode, page.pageNumber, page.pageNumber*pageSize, int(page.size), path)
	if err != nil {
		log.WithError(err).Errorf("set chunk metadata failed")
		return err
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

	reader, err := p.Data.Get(chunkAttr.StoragePath, 0, pageSize)
	if err != nil {
		return nil, false, err
	} else {
		page, err = NewPageWithReader(pageNum, reader, pageSize)
		if err != nil {
			return nil, false, err
		}
	}

	return page, true, nil
}
