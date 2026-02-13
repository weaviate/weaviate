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

package visited

import (
	"math"
	"sync"
)

// FastPool is a thread-safe pool of FastSet instances.
type FastPool struct {
	sync.Mutex
	capacity   int
	sets       []FastSet
	maxStorage int
}

// NewFastPool creates a new pool with the specified initial size.
// poolSize: number of sets to pre-allocate (if <1, no pre-allocation)
// setCapacity: initial capacity for each set
// maxStorage: maximum number of sets to keep in the pool. If <1, unlimited storage is used.
func NewFastPool(poolSize, setCapacity, maxStorage int) *FastPool {
	if poolSize < 0 {
		poolSize = 0
	}

	if maxStorage < 1 {
		maxStorage = math.MaxInt
	}

	if poolSize > maxStorage {
		maxStorage = poolSize
	}

	sets := make([]FastSet, poolSize)
	for i := range sets {
		sets[i] = NewFastSet(setCapacity)
	}
	return &FastPool{
		capacity:   setCapacity,
		sets:       sets,
		maxStorage: maxStorage,
	}
}

// Borrow returns a FastSet from the pool, creating a new one if the pool is empty.
// The returned set is guaranteed to be reset and ready for use.
func (p *FastPool) Borrow() FastSet {
	p.Lock()
	defer p.Unlock()

	if len(p.sets) == 0 {
		return NewFastSet(p.capacity)
	}

	// Pop from the end (most recently returned, likely in cache)
	set := p.sets[len(p.sets)-1]
	p.sets = p.sets[:len(p.sets)-1]
	return set
}

// Return puts a FastSet back into the pool for reuse.
// The set is automatically reset before being returned to the pool.
func (p *FastPool) Return(set FastSet) {
	set.Reset()

	p.Lock()
	defer p.Unlock()

	if len(p.sets) >= p.maxStorage {
		// Pool is full, let GC collect this one
		return
	}

	p.sets = append(p.sets, set)
}

// Len returns the current number of sets in the pool.
func (p *FastPool) Len() int {
	p.Lock()
	defer p.Unlock()
	return len(p.sets)
}
