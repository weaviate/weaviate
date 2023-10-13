//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package flat

import (
	"sync"
)

type pools struct {
	byteSlicePool   *byteSlicePool
	uint64SlicePool *uint64SlicePool
}

func newPools() *pools {
	return &pools{
		byteSlicePool:   newByteSlicePool(),
		uint64SlicePool: newUint64licePool(),
	}
}

type byteSlicePool struct {
	pool *sync.Pool
}

func newByteSlicePool() *byteSlicePool {
	return &byteSlicePool{
		pool: &sync.Pool{
			New: func() interface{} {
				newSlice := make([]byte, 0)
				return &newSlice
			},
		},
	}
}

func (p *byteSlicePool) Get(capacity int) *[]byte {
	slice := p.pool.Get().(*[]byte)
	if len(*slice) != capacity {
		newSlice := make([]byte, capacity)
		slice = &newSlice
	}
	return slice
}

func (p *byteSlicePool) Put(slice *[]byte) {
	p.pool.Put(slice)
}

type uint64SlicePool struct {
	pool *sync.Pool
}

func newUint64licePool() *uint64SlicePool {
	return &uint64SlicePool{
		pool: &sync.Pool{
			New: func() interface{} {
				newSlice := make([]uint64, 0)
				return &newSlice
			},
		},
	}
}

func (p *uint64SlicePool) Get(capacity int) *[]uint64 {
	slice := p.pool.Get().(*[]uint64)
	if len(*slice) != capacity {
		newSlice := make([]uint64, capacity)
		slice = &newSlice
	}
	return slice
}

func (p *uint64SlicePool) Put(slice *[]uint64) {
	p.pool.Put(slice)
}
