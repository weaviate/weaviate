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

package hnsw

import (
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

type pools struct {
	visitedLists     *visited.Pool
	visitedListsLock *sync.Mutex

	pqItemSlice  *sync.Pool
	pqHeuristic  *pqMinWithIndexPool
	pqResults    *common.PqMaxPool
	pqCandidates *pqMinPool

	tempVectors *common.TempVectorsPool
}

func newPools(maxConnectionsLayerZero int) *pools {
	return &pools{
		visitedLists:     visited.NewPool(1, cache.InitialSize+500),
		visitedListsLock: &sync.Mutex{},
		pqItemSlice: &sync.Pool{
			New: func() interface{} {
				return make([]priorityqueue.Item[uint64], 0, maxConnectionsLayerZero)
			},
		},
		pqHeuristic:  newPqMinWithIndexPool(maxConnectionsLayerZero),
		pqResults:    common.NewPqMaxPool(maxConnectionsLayerZero),
		pqCandidates: newPqMinPool(maxConnectionsLayerZero),
		tempVectors:  common.NewTempVectorsPool(),
	}
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
