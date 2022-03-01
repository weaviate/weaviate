package db

import (
	"crypto/rand"
	"sync"

	"github.com/pkg/errors"
)

type bufferedRandomGen struct {
	buf    []byte
	offset int
	sync.Mutex
}

func (b *bufferedRandomGen) Make(n int) ([]byte, error) {
	b.Lock()
	defer b.Unlock()

	if n > len(b.buf) {
		return nil, errors.Errorf("cannot gen %d random bytes, internal buffer is only"+
			" %d bytes large", n, len(b.buf))
	}

	if len(b.buf)-b.offset < n {
		if err := b.refresh(); err != nil {
			return nil, err
		}
	}

	out := make([]byte, n)
	copy(out, b.buf[b.offset:b.offset+n])
	b.offset += n

	return out, nil
}

func (b *bufferedRandomGen) refresh() error {
	_, err := rand.Read(b.buf)
	if err != nil {
		return err
	}

	b.offset = 0

	return nil
}

func newBufferedRandomGen(size int) (*bufferedRandomGen, error) {
	b := &bufferedRandomGen{
		buf: make([]byte, size),
	}

	err := b.refresh()
	return b, err
}
