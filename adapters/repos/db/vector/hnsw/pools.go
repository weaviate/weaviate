//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"sync"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/visited"
)

type pools struct {
	visitedLists *visited.Pool
	pqItemSlice  *sync.Pool
	pqHeuristic  *pqMinPool
	pqResults    *pqMaxPool
	pqCandidates *pqMinPool
}

func newPools(maxConnectionsLayerZero int) *pools {
	return &pools{
		visitedLists: visited.NewPool(1, initialSize+500),
		pqItemSlice: &sync.Pool{
			New: func() interface{} {
				return make([]priorityqueue.Item, 0, maxConnectionsLayerZero)
			},
		},
		pqHeuristic:  newPqMinPool(maxConnectionsLayerZero),
		pqResults:    newPqMaxPool(maxConnectionsLayerZero),
		pqCandidates: newPqMinPool(maxConnectionsLayerZero),
	}
}

type pqMinPool struct {
	pool *sync.Pool
}

func newPqMinPool(defaultCap int) *pqMinPool {
	return &pqMinPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return priorityqueue.NewMin(defaultCap)
			},
		},
	}
}

func (pqh *pqMinPool) GetMin(capacity int) *priorityqueue.Queue {
	pq := pqh.pool.Get().(*priorityqueue.Queue)
	if pq.Cap() < capacity {
		pq.ResetCap(capacity)
	} else {
		pq.Reset()
	}

	return pq
}

func (pqh *pqMinPool) Put(pq *priorityqueue.Queue) {
	pqh.pool.Put(pq)
}

type pqMaxPool struct {
	pool *sync.Pool
}

func newPqMaxPool(defaultCap int) *pqMaxPool {
	return &pqMaxPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return priorityqueue.NewMax(defaultCap)
			},
		},
	}
}

func (pqh *pqMaxPool) GetMax(capacity int) *priorityqueue.Queue {
	pq := pqh.pool.Get().(*priorityqueue.Queue)
	if pq.Cap() < capacity {
		pq.ResetCap(capacity)
	} else {
		pq.Reset()
	}

	return pq
}

func (pqh *pqMaxPool) Put(pq *priorityqueue.Queue) {
	pqh.pool.Put(pq)
}
