package graph

import (
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	growthRate = 1.25
)

type Nodes struct {
	sync.RWMutex                        // protects the list
	locks        *common.ShardedRWLocks // protects individual nodes
	list         []*Vertex
}

func NewNodes(initialSize int) *Nodes {
	return &Nodes{
		list:  make([]*Vertex, initialSize),
		locks: common.NewDefaultShardedRWLocks(),
	}
}

func NewNodesWith(list []*Vertex) *Nodes {
	return &Nodes{
		list:  list,
		locks: common.NewDefaultShardedRWLocks(),
	}
}

func (n *Nodes) IsEmpty(entryPointID uint64) bool {
	n.RLock()
	defer n.RUnlock()

	return n.list[entryPointID] == nil
}

func (n *Nodes) Len() int {
	n.RLock()
	defer n.RUnlock()

	return len(n.list)
}

func (n *Nodes) Get(id uint64) *Vertex {
	n.RLock()
	defer n.RUnlock()

	if id >= uint64(len(n.list)) {
		return nil
	}

	n.locks.RLock(id)
	v := n.list[id]
	n.locks.RUnlock(id)
	return v
}

func (n *Nodes) Set(id uint64, node *Vertex) {
	n.RLock()
	defer n.RUnlock()

	n.locks.Lock(id)
	n.list[id] = node
	n.locks.Unlock(id)
}

// TODO: switch to Golang iterators once Go 1.23 is the
// minimum version
func (n *Nodes) IterLockedNonNil(fn func(*Vertex)) {
	n.RLock()
	list := n.list
	n.RUnlock()

	for _, node := range list {
		if node != nil {
			n.locks.Lock(node.id)
			fn(node)
			n.locks.Unlock(node.id)
		}
	}
}

func (n *Nodes) Grow(id uint64) (previousSize, newSize int) {
	if l := n.Len(); int(id) < l {
		return l, l
	}

	n.Lock()
	defer n.Unlock()

	// lock n.nodes' individual elements to avoid race between writing to elements
	// and copying entire slice
	n.locks.LockAll()
	defer n.locks.UnlockAll()

	previousSize = len(n.list)
	if int(id) < previousSize {
		return previousSize, previousSize
	}

	if (growthRate-1)*float64(previousSize) < float64(cache.MinimumIndexGrowthDelta) {
		// typically grow the index by the delta
		newSize = previousSize + cache.MinimumIndexGrowthDelta
	} else {
		newSize = int(float64(previousSize) * growthRate)
	}

	if newSize <= int(id) {
		// There are situations were docIDs are not in order. For example, if  the
		// default size is 10k and the default delta is 10k. Imagine the user
		// imports 21 objects, then deletes the first 20,500. When rebuilding the
		// index from disk the first id to be imported would be 20,501, however the
		// index default size and default delta would only reach up to 20,000.
		newSize = int(id) + cache.MinimumIndexGrowthDelta
	}

	newIndex := make([]*Vertex, newSize)
	copy(newIndex, n.list)
	n.list = newIndex

	return
}
