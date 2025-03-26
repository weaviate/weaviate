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
	"errors"
	"fmt"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/dto"
)

type VectorIndex interface {
	AddBatch(ctx context.Context, ids []uint64, vector [][]float32) error
	AddMultiBatch(ctx context.Context, docIds []uint64, vectors [][][]float32) error

	ValidateBeforeInsert(vector []float32) error
	ValidateMultiBeforeInsert(vector [][]float32) error
}

type VectorRecord interface {
	Len() int
	Validate(vectorIndex VectorIndex) error
}

func AddVectorsToIndex(ctx context.Context, vectors []VectorRecord, vectorIndex VectorIndex) error {
	// ensure the vector is not empty
	if len(vectors) == 0 {
		return errors.New("empty vectors")
	}
	switch vectors[0].(type) {
	case *Vector[[]float32]:
		ids := make([]uint64, len(vectors))
		vecs := make([][]float32, len(vectors))
		for i, v := range vectors {
			ids[i] = v.(*Vector[[]float32]).ID
			vecs[i] = v.(*Vector[[]float32]).Vector
		}
		return vectorIndex.AddBatch(ctx, ids, vecs)
	case *Vector[[][]float32]:
		ids := make([]uint64, len(vectors))
		vecs := make([][][]float32, len(vectors))
		for i, v := range vectors {
			ids[i] = v.(*Vector[[][]float32]).ID
			vecs[i] = v.(*Vector[[][]float32]).Vector
		}
		return vectorIndex.AddMultiBatch(ctx, ids, vecs)
	default:
		return fmt.Errorf("unexpected vector type %T", vectors[0])
	}
}

type Vector[T dto.Embedding] struct {
	ID     uint64
	Vector T
}

func (v *Vector[T]) Len() int {
	switch any(v.Vector).(type) {
	case []float32:
		return len(v.Vector)
	case [][]float32:
		vec := any(v.Vector).([][]float32)
		if len(vec) > 0 {
			return len(vec[0])
		}
		return 0
	default:
		return 0
	}
}

func (v *Vector[T]) Validate(vectorIndex VectorIndex) error {
	// ensure the vector is not empty
	if len(v.Vector) == 0 {
		return errors.New("empty vector")
	}
	// delegate the validation to the index
	switch any(v.Vector).(type) {
	case []float32:
		return vectorIndex.ValidateBeforeInsert(any(v.Vector).([]float32))
	case [][]float32:
		return vectorIndex.ValidateMultiBeforeInsert(any(v.Vector).([][]float32))
	default:
		return fmt.Errorf("unexpected vector type %T", v.Vector)
	}
}

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
	VectorForID[T []float32 | []uint64 | float32 | byte | uint64] func(ctx context.Context, id uint64) ([]T, error)
	MultipleVectorForID[T float32 | uint64 | byte]                func(ctx context.Context, id uint64, relativeID uint64) ([]T, error)
	TempVectorForID[T []float32 | float32]                        func(ctx context.Context, id uint64, container *VectorSlice) ([]T, error)
	MultiVectorForID                                              func(ctx context.Context, ids []uint64) ([][]float32, []error)
)

type TargetVectorForID[T []float32 | float32 | byte | uint64] struct {
	TargetVector     string
	VectorForIDThunk func(ctx context.Context, id uint64, targetVector string) ([]T, error)
}

func (t TargetVectorForID[T]) VectorForID(ctx context.Context, id uint64) ([]T, error) {
	return t.VectorForIDThunk(ctx, id, t.TargetVector)
}

type TargetTempVectorForID[T []float32 | float32] struct {
	TargetVector         string
	TempVectorForIDThunk func(ctx context.Context, id uint64, container *VectorSlice, targetVector string) ([]T, error)
}

func (t TargetTempVectorForID[T]) TempVectorForID(ctx context.Context, id uint64, container *VectorSlice) ([]T, error) {
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
