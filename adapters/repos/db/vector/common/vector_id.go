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

package common

import (
	"context"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

type VectorSlice struct {
	Slice []float32
	Mem   []float32
	Buff8 []byte
	Buff  []byte
}

type VectorUint64Slice struct {
	Slice []uint64
}

type (
	VectorForID[T float32 | byte | uint64]         func(ctx context.Context, id uint64) ([]T, error)
	MultipleVectorForID[T float32 | byte | uint64] func(ctx context.Context, id uint64) ([]T, error)
	TempVectorForID                                func(ctx context.Context, id uint64, container *VectorSlice) ([]float32, error)
	MultiVectorForID                               func(ctx context.Context, ids []uint64) ([][]float32, []error)
)

type TargetVectorForID[T float32 | byte | uint64] struct {
	TargetVector     string
	VectorForIDThunk func(ctx context.Context, id uint64, targetVector string) ([]T, error)
}

func (t TargetVectorForID[T]) VectorForID(ctx context.Context, id uint64) ([]T, error) {
	return t.VectorForIDThunk(ctx, id, t.TargetVector)
}

type TargetMultipleVectorForID[T float32 | byte | uint64] struct {
	TargetVector             string
	MultipleVectorForIDThunk func(ctx context.Context, id uint64, targetVector string) ([]T, error)
}

func (t TargetMultipleVectorForID[T]) MultipleVectorForID(ctx context.Context, id uint64) ([]T, error) {
	return t.MultipleVectorForIDThunk(ctx, id, t.TargetVector)
}

type TargetTempVectorForID struct {
	TargetVector         string
	TempVectorForIDThunk func(ctx context.Context, id uint64, container *VectorSlice, targetVector string) ([]float32, error)
}

func (t TargetTempVectorForID) TempVectorForID(ctx context.Context, id uint64, container *VectorSlice) ([]float32, error) {
	return t.TempVectorForIDThunk(ctx, id, container, t.TargetVector)
}

type TempVectorUint64Pool struct {
	pool *sync.Pool
}

func NewTempUint64VectorsPool() *TempVectorUint64Pool {
	return &TempVectorUint64Pool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &VectorUint64Slice{
					Slice: nil,
				}
			},
		},
	}
}

func (pool *TempVectorUint64Pool) Get(capacity int) *VectorUint64Slice {
	container := pool.pool.Get().(*VectorUint64Slice)
	if cap(container.Slice) >= capacity {
		container.Slice = container.Slice[:capacity]
	} else {
		container.Slice = make([]uint64, capacity)
	}
	return container
}

func (pool *TempVectorUint64Pool) Put(container *VectorUint64Slice) {
	pool.pool.Put(container)
}

type TempVectorsPool struct {
	pool *sync.Pool
}

func NewTempVectorsPool() *TempVectorsPool {
	return &TempVectorsPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &VectorSlice{
					Mem:   nil,
					Buff8: make([]byte, 8),
					Buff:  nil,
					Slice: nil,
				}
			},
		},
	}
}

func (pool *TempVectorsPool) Get(capacity int) *VectorSlice {
	container := pool.pool.Get().(*VectorSlice)
	if len(container.Slice) >= capacity {
		container.Slice = container.Mem[:capacity]
	} else {
		container.Mem = make([]float32, capacity)
		container.Slice = container.Mem[:capacity]
	}
	return container
}

func (pool *TempVectorsPool) Put(container *VectorSlice) {
	pool.pool.Put(container)
}

type PqMaxPool struct {
	pool *sync.Pool
}

func NewPqMaxPool(defaultCap int) *PqMaxPool {
	return &PqMaxPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return priorityqueue.NewMax[any](defaultCap)
			},
		},
	}
}

func (pqh *PqMaxPool) GetMax(capacity int) *priorityqueue.Queue[any] {
	pq := pqh.pool.Get().(*priorityqueue.Queue[any])
	if pq.Cap() < capacity {
		pq.ResetCap(capacity)
	} else {
		pq.Reset()
	}

	return pq
}

func (pqh *PqMaxPool) Put(pq *priorityqueue.Queue[any]) {
	pqh.pool.Put(pq)
}
