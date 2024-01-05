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

package flat

import (
	"sync"
)

const defaultSize = 200

type pools struct {
	byteSlicePool    *slicePool[byte]
	uint64SlicePool  *slicePool[uint64]
	float32SlicePool *slicePool[float32]
}

func newPools() *pools {
	return &pools{
		byteSlicePool:    newSlicePool[byte](),
		uint64SlicePool:  newSlicePool[uint64](),
		float32SlicePool: newSlicePool[float32](),
	}
}

type slicePool[T any] struct {
	pool *sync.Pool
}

type SliceStruct[T any] struct {
	slice []T
}

func newSlicePool[T any]() *slicePool[T] {
	return &slicePool[T]{
		pool: &sync.Pool{
			New: func() interface{} {
				return &SliceStruct[T]{
					slice: make([]T, defaultSize),
				}
			},
		},
	}
}

func (p *slicePool[T]) Get(capacity int) *SliceStruct[T] {
	t := p.pool.Get().(*SliceStruct[T])
	if cap(t.slice) < capacity {
		t.slice = make([]T, capacity)
	}
	t.slice = t.slice[:capacity]
	return t
}

func (p *slicePool[T]) Put(t *SliceStruct[T]) {
	p.pool.Put(t)
}
