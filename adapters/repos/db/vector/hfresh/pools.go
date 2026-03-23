//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hfresh

import "sync"

// list of pools shared between all HFresh instances
var (
	bufferPool = newBufferPool(1024)
)

// BufferPool is a simple wrapper around sync.Pool to manage byte slices of varying sizes.
type BufferPool struct {
	pool sync.Pool
}

func newBufferPool(size int) *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, size)
				return &b
			},
		},
	}
}

// Get returns a handle to a byte slice of the requested length and capacity.
// The caller must eventually call Put(handle) to return the buffer to the pool.
func (p *BufferPool) Get(length, capacity int) *[]byte {
	b := p.pool.Get().(*[]byte)
	if cap(*b) < capacity {
		*b = make([]byte, length, capacity)
	} else {
		*b = (*b)[:length]
	}
	return b
}

// Put returns a handle obtained from Get back to the pool.
// The handle must not be used after calling Put.
func (p *BufferPool) Put(b *[]byte) {
	if b == nil || cap(*b) == 0 {
		return
	}
	p.pool.Put(b)
}

// PutBytes returns a byte slice to the pool when the original handle from Get
// is unavailable (e.g. the slice was received as a plain []byte from a caller).
// This is less efficient than Put as it allocates a new pointer wrapper.
func (p *BufferPool) PutBytes(b []byte) {
	if cap(b) == 0 {
		return
	}
	p.pool.Put(&b)
}
