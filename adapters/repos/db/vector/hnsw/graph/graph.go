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

func (n *Nodes) SetNodes(nodes []*Vertex) {
	n.Lock()
	n.locks.LockAll()

	n.list = nodes

	n.locks.UnlockAll()
	n.Unlock()
}

func (n *Nodes) Reset(size int) {
	n.Lock()
	n.locks.LockAll()

	n.list = make([]*Vertex, size)

	n.locks.UnlockAll()
	n.Unlock()
}

func (n *Nodes) IsEmpty(entryPointID uint64) bool {
	n.RLock()
	n.locks.RLock(entryPointID)

	empty := n.list[entryPointID] == nil

	n.locks.RUnlock(entryPointID)
	n.RUnlock()

	return empty
}

func (n *Nodes) Len() int {
	n.RLock()

	l := len(n.list)

	n.RUnlock()

	return l
}

func (n *Nodes) Get(id uint64) *Vertex {
	n.RLock()
	defer n.RUnlock()

	if id >= uint64(len(n.list)) {
		// See https://github.com/weaviate/weaviate/issues/1838 for details.
		// This could be after a crash recovery when the object store is "further
		// ahead" than the hnsw index and we receive a delete request
		return nil
	}

	n.locks.RLock(id)
	v := n.list[id]
	n.locks.RUnlock(id)
	return v
}

func (n *Nodes) Set(node *Vertex) {
	n.RLock()

	id := node.ID()
	n.locks.Lock(id)
	n.list[id] = node
	n.locks.Unlock(id)

	n.RUnlock()
}

// TODO: switch to Golang iterators once Go 1.23 is the
// minimum version
func (n *Nodes) Iter(fn func(id uint64, node *Vertex) bool) {
	n.RLock()
	list := n.list
	n.RUnlock()

	for id := 0; id < len(list); id++ {
		id := uint64(id)
		n.locks.RLock(id)
		node := list[id]
		n.locks.RUnlock(id)

		if node != nil {
			ok := fn(id, node)
			if !ok {
				return // stop iteration
			}
		}
	}
}

// TODO: switch to Golang iterators once Go 1.23 is the
// minimum version
func (n *Nodes) IterReverse(fn func(id uint64, node *Vertex) bool) {
	n.RLock()
	list := n.list
	n.RUnlock()

	for id := len(list) - 1; id >= 0; id-- {
		id := uint64(id)
		n.locks.RLock(id)
		node := list[id]
		n.locks.RUnlock(id)

		if node != nil {
			ok := fn(id, node)
			if !ok {
				return // stop iteration
			}
		}
	}
}

// TODO: switch to Golang iterators once Go 1.23 is the
// minimum version
func (n *Nodes) IterE(fn func(id uint64, node *Vertex) error) error {
	n.RLock()
	list := n.list
	n.RUnlock()

	for id := 0; id < len(list); id++ {
		id := uint64(id)
		n.locks.RLock(id)
		node := list[id]
		n.locks.RUnlock(id)

		if node != nil {
			err := fn(id, node)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *Nodes) Delete(id uint64) {
	n.RLock()

	n.locks.Lock(id)
	n.list[id] = nil
	n.locks.Unlock(id)

	n.RUnlock()
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
