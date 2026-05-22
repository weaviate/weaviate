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

func (p *BufferPool) Get(length, capacity int) []byte {
	b := p.pool.Get().(*[]byte)
	if cap(*b) < capacity {
		*b = make([]byte, length, capacity)
	} else {
		*b = (*b)[:length]
	}
	return *b
}

func (p *BufferPool) Put(b []byte) {
	if cap(b) == 0 {
		return
	}

	p.pool.Put(&b)
}
