//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package storobj

import "sync"

func newBufferPool(initialSize int) *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() any {
				// initialize with len=0 to make sure we get a consistent result
				// whether it's a new or used buffer. Every buffer will always have
				// len=0 and cap>=initialSize
				return make([]byte, 0, initialSize)
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

	//nolint:staticcheck // I disagree with the linter, this doesn't need to be a
	//pointer. Even if we copy the slicestruct header, the backing array is
	//what's allocation-heavy and that will be reused. The profiles in the PR
	//description prove this.
	b.pool.Put(buf)
}
