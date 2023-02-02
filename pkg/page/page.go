package page

import (
	"bytes"
	"context"
	"github.com/adlternative/tinygitfs/pkg/datasource"
	"github.com/adlternative/tinygitfs/pkg/metadata"
	log "github.com/sirupsen/logrus"
	"io"
	"sync"
)

const PageSize = 1 << 20

type Page struct {
	pageNumber int64
	data       []byte
	clean      bool
	size       int64
	mu         *sync.RWMutex
}

func (p *Page) Write(offset int64, data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	length := int64(len(data))
	copy(p.data[offset:offset+length], data)
	p.clean = false

	if p.size < offset+length {
		p.size = offset + length
	}
}

func (p *Page) Read(offset int64, data []byte, length int64) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.size-offset <= 0 || length <= 0 {
		return 0
	} else if p.size-offset < length {
		length = p.size - offset
	}

	copy(data[:length], p.data[offset:offset+length])
	return length
}

func (p *Page) Fsync(ctx context.Context, source datasource.DataSource, inode metadata.Ino) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.clean {
		return nil
	}
	// TODO txn
	path := storagePath(inode, p.pageNumber)
	err := source.Data.Put(path, bytes.NewReader(p.data[:p.size]))
	if err != nil {
		log.WithError(err).Errorf("set chunk data failed")
		return err
	}
	err = source.Meta.SetChunkMeta(ctx, inode, p.pageNumber, p.pageNumber*PageSize, int(p.size), path)
	if err != nil {
		log.WithError(err).Errorf("set chunk metadata failed")
		return err
	}

	p.clean = true
	return nil
}

func NewPage(pageNumber int64) *Page {
	return &Page{
		pageNumber: pageNumber,
		data:       make([]byte, PageSize),
		clean:      true,
		size:       0,
		mu:         &sync.RWMutex{},
	}
}

func NewPageWithReader(pageNumber int64, reader io.Reader, totalSize int64) (*Page, error) {
	page := NewPage(pageNumber)
	curSize := int64(0)
	for curSize < totalSize {
		n, err := reader.Read(page.data[curSize:])

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
		"pageNumber": pageNumber,
		"readSize":   curSize,
		"expectSize": totalSize,
	}).Debug("page read")

	page.size = curSize
	return page, nil

}
