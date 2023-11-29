package storobj

import "sync"

func newBufferPool(initialSize int) *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() any {
				return make([]byte, initialSize)
			},
		},
	}
}

type bufferPool struct {
	pool sync.Pool
}

func (b *bufferPool) Get() []byte {
	buf := b.pool.Get().([]byte)

	return buf
}

func (b *bufferPool) Put(buf []byte) {
	// make sure the length is reset before putting it back into the pool. This
	// way all buffers will always have len=0, either because they are brand-new
	// or because they have been reset.
	buf = buf[:0]
	b.pool.Put(buf)
}
