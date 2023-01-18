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

package hnsw

import (
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

type pools struct {
	visitedLists     *visited.Pool
	visitedListsLock *sync.Mutex

	pqItemSlice  *sync.Pool
	pqHeuristic  *pqMinWithIndexPool
	pqResults    *pqMaxPool
	pqCandidates *pqMinPool
	connList     *connList
}

func newPools(maxConnectionsLayerZero int) *pools {
	return &pools{
		visitedLists:     visited.NewPool(1, initialSize+500),
		visitedListsLock: &sync.Mutex{},
		pqItemSlice: &sync.Pool{
			New: func() interface{} {
				return make([]priorityqueue.ItemWithIndex, 0, maxConnectionsLayerZero)
			},
		},
		pqHeuristic:  newPqMinWithIndexPool(maxConnectionsLayerZero),
		pqResults:    newPqMaxPool(maxConnectionsLayerZero),
		pqCandidates: newPqMinPool(maxConnectionsLayerZero),
		connList:     newConnList(maxConnectionsLayerZero),
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

type pqMinWithIndexPool struct {
	pool *sync.Pool
}

func newPqMinWithIndexPool(defaultCap int) *pqMinWithIndexPool {
	return &pqMinWithIndexPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return priorityqueue.NewMinWithIndex(defaultCap)
			},
		},
	}
}

func (pqh *pqMinWithIndexPool) GetMin(capacity int) *priorityqueue.QueueWithIndex {
	pq := pqh.pool.Get().(*priorityqueue.QueueWithIndex)
	if pq.Cap() < capacity {
		pq.ResetCap(capacity)
	} else {
		pq.Reset()
	}

	return pq
}

func (pqh *pqMinWithIndexPool) Put(pq *priorityqueue.QueueWithIndex) {
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

func newConnList(capacity int) *connList {
	return &connList{
		pool: &sync.Pool{
			New: func() interface{} {
				l := make([]uint64, 0, capacity)
				return &l
			},
		},
	}
}

type connList struct {
	pool *sync.Pool
}

func (cl *connList) Get(length int) *[]uint64 {
	list := cl.pool.Get().(*[]uint64)
	*list = (*list)[:length]
	return list
}

func (cl *connList) Put(list *[]uint64) {
	cl.pool.Put(list)
}
