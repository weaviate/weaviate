package hnsw

import (
	"sync"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/visited"
)

type pools struct {
	visitedLists *visited.Pool
	pqItemSlice  *sync.Pool
}

func newPools(maxConnectionsLayerZero int) *pools {
	return &pools{
		visitedLists: visited.NewPool(1, initialSize+500),
		pqItemSlice: &sync.Pool{
			New: func() interface{} {
				return make([]priorityqueue.Item, 0, maxConnectionsLayerZero)
			},
		},
	}
}
