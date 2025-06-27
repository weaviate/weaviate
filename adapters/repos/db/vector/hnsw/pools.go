//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

type pools struct {
	visitedListsLock *sync.RWMutex
	visitedLists     *visitedPool

	pqItemSlice  *sync.Pool
	pqHeuristic  *pqMinWithIndexPool
	pqResults    *common.PqMaxPool
	pqCandidates *pqMinPool

	tempVectors       *common.TempVectorsPool
	tempVectorsUint64 *common.TempVectorUint64Pool
}

func newPools(maxConnectionsLayerZero int) *pools {
	return &pools{
		visitedLists:     newVisitedPool(0),
		visitedListsLock: &sync.RWMutex{},
		pqItemSlice: &sync.Pool{
			New: func() interface{} {
				return make([]priorityqueue.Item[uint64], 0, maxConnectionsLayerZero)
			},
		},
		pqHeuristic:       newPqMinWithIndexPool(maxConnectionsLayerZero),
		pqResults:         common.NewPqMaxPool(maxConnectionsLayerZero),
		pqCandidates:      newPqMinPool(maxConnectionsLayerZero),
		tempVectors:       common.NewTempVectorsPool(),
		tempVectorsUint64: common.NewTempUint64VectorsPool(),
	}
}

type visitedPool struct {
	pool *sync.Pool
}

func newVisitedPool(initialSize int) *visitedPool {
	if initialSize <= 0 {
		initialSize = 1_000_000 // Default size if not specified
	}
	return &visitedPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return visited.NewSparseSet(initialSize, 8192)
			},
		},
	}
}

func (vp *visitedPool) Get() *visited.SparseSet {
	return vp.pool.Get().(*visited.SparseSet)
}

func (vp *visitedPool) Put(v *visited.SparseSet) {
	v.Reset()
	vp.pool.Put(v)
}

type pqMinPool struct {
	pool *sync.Pool
}

func newPqMinPool(defaultCap int) *pqMinPool {
	return &pqMinPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return priorityqueue.NewMin[any](defaultCap)
			},
		},
	}
}

func (pqh *pqMinPool) GetMin(capacity int) *priorityqueue.Queue[any] {
	pq := pqh.pool.Get().(*priorityqueue.Queue[any])
	if pq.Cap() < capacity {
		pq.ResetCap(capacity)
	} else {
		pq.Reset()
	}

	return pq
}

func (pqh *pqMinPool) Put(pq *priorityqueue.Queue[any]) {
	pqh.pool.Put(pq)
}

type pqMinWithIndexPool struct {
	pool *sync.Pool
}

func newPqMinWithIndexPool(defaultCap int) *pqMinWithIndexPool {
	return &pqMinWithIndexPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return priorityqueue.NewMin[uint64](defaultCap)
			},
		},
	}
}

func (pqh *pqMinWithIndexPool) GetMin(capacity int) *priorityqueue.Queue[uint64] {
	pq := pqh.pool.Get().(*priorityqueue.Queue[uint64])
	if pq.Cap() < capacity {
		pq.ResetCap(capacity)
	} else {
		pq.Reset()
	}

	return pq
}

func (pqh *pqMinWithIndexPool) Put(pq *priorityqueue.Queue[uint64]) {
	pqh.pool.Put(pq)
}
