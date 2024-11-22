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

package visited

import (
	"math"
	"sync"
)

type Pool struct {
	sync.Mutex
	listSetSize int
	listSets    []ListSet
	maxStorage  int
}

// NewPool creates a new pool with specified size.
// listSetSize specifies the size of a list at creation time point
// maxStorage specifies the maximum number of lists that can be stored in the
// pool, the pool can still generate infinite lists, but if more than
// maxStorage are returned to the pool, some lists will be thrown away.
func NewPool(initialSize int, listSetSize int, maxStorage int) *Pool {
	if maxStorage < 1 {
		maxStorage = math.MaxInt
	}

	if initialSize > maxStorage {
		maxStorage = initialSize
	}

	p := &Pool{
		listSetSize: listSetSize,
		listSets:    make([]ListSet, initialSize), // make enough room
		maxStorage:  maxStorage,
	}

	for i := 0; i < initialSize; i++ {
		p.listSets[i] = NewList(listSetSize)
	}

	return p
}

// Borrow a list from the pool. If the pool is empty, a new list is craeted. If
// an old list is used, it is guaranteed to be reset – as that was performed on
// return.
func (p *Pool) Borrow() ListSet {
	p.Lock()

	if n := len(p.listSets); n > 0 {
		l := p.listSets[n-1]
		p.listSets = p.listSets[:n-1]
		p.Unlock()

		return l
	}
	p.Unlock()
	return NewList(p.listSetSize)
}

// Return list l to the pool
// The list l might be thrown if l.Len() > listSetSize*1.10
// or if the pool is full.
func (p *Pool) Return(l ListSet) {
	n := l.Len()
	if n < p.listSetSize || n > p.listSetSize*11/10 { // 11/10 could be tuned
		return
	}
	l.Reset()

	p.Lock()
	defer p.Unlock()

	if len(p.listSets) >= p.maxStorage {
		return
	}

	p.listSets = append(p.listSets, l)
}

// Destroy and empty pool
func (p *Pool) Destroy() {
	p.Lock()
	defer p.Unlock()
	for i := range p.listSets {
		p.listSets[i].free()
	}

	p.listSets = nil
}

// Len returns the number of lists currently in the pool
func (p *Pool) Len() int {
	p.Lock()
	defer p.Unlock()
	return len(p.listSets)
}
