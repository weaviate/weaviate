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
	visitedListsLock *sync.RWMutex

	// visitedFastSets is used for tombstone cleanup where visits are sparse
	// relative to total graph size. FastSet uses O(visited) memory vs O(maxNodeID)
	// for ListSet, using an open-addressed hash table with linear probing.
	visitedFastSets     *visited.FastPool
	visitedFastSetsLock *sync.RWMutex

	pqItemSlice  *sync.Pool
	pqHeuristic  *pqMinWithIndexPool
	pqResults    *common.PqMaxPool
	pqCandidates *pqMinPool

	tempVectors       *common.TempVectorsPool
	tempVectorsUint64 *common.TempVectorUint64Pool
}

func newPools(maxConnectionsLayerZero int, initialVisitedListPoolSize int) *pools {
	return &pools{
		visitedLists:     visited.NewPool(1, cache.InitialSize+500, initialVisitedListPoolSize),
		visitedListsLock: &sync.RWMutex{},
		// FastPool for tombstone cleanup: capacity of 512 is enough for typical
		// efConstruction traversals, pool size matches list pool. FastSet uses
		// open-addressed hashing which is ~2.7x faster than Go's built-in map.
		visitedFastSets:     visited.NewFastPool(100, 512, initialVisitedListPoolSize),
		visitedFastSetsLock: &sync.RWMutex{},
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
