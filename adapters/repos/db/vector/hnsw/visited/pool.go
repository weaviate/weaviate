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
	"sync"
	"sync/atomic"
)

type Pool struct {
	outLock     sync.Mutex
	inLock      sync.Mutex
	listSetSize int
	listSets    [][]ListSet
	switcher    atomic.Int32
}

// NewPool creates a new pool with specified size.
// listSetSize specifies the size of a list at creation time point
func NewPool(size int, listSetSize int) *Pool {
	p := &Pool{
		listSetSize: listSetSize,
		listSets:    make([][]ListSet, 2), // make enough room
		switcher:    atomic.Int32{},
	}

	p.listSets[0] = make([]ListSet, size)
	p.listSets[1] = make([]ListSet, 0, size)

	for i := 0; i < size; i++ {
		p.listSets[0][i] = NewList(listSetSize)
	}

	return p
}

func (p *Pool) Size() int {
	if len(p.listSets[0]) > len(p.listSets[1]) {
		return len(p.listSets[0])
	}
	return len(p.listSets[1])
}

// Borrow return a free list
func (p *Pool) Borrow() ListSet {
	p.outLock.Lock()
	readListIndex := p.switcher.Load()
	readList := p.listSets[readListIndex]
	if n := len(p.listSets[readListIndex]); n > 0 {
		l := readList[n-1]
		p.listSets[readListIndex][n-1].free() // prevent memory leak
		p.listSets[readListIndex] = p.listSets[readListIndex][:n-1]
		p.outLock.Unlock()

		return l
	}
	// check if we can switch
	p.inLock.Lock()
	writeListIndex := (readListIndex + 1) % 2
	writeList := p.listSets[writeListIndex]
	if len(writeList) > 0 {
		p.switcher.Store(writeListIndex)
	}

	p.inLock.Unlock()
	p.outLock.Unlock()
	return NewList(p.listSetSize)
}

// Return list l to the pool
// The list l might be thrown if l.Len() > listSetSize*1.10
func (p *Pool) Return(l ListSet) {
	n := l.Len()
	if n < p.listSetSize || n > p.listSetSize*11/10 { // 11/10 could be tuned
		return
	}
	l.Reset()

	p.inLock.Lock()
	writeListIndex := (p.switcher.Load() + 1) % 2
	p.listSets[writeListIndex] = append(p.listSets[writeListIndex], l)
	p.inLock.Unlock()
}

// Destroy and empty pool
func (p *Pool) Destroy() {
	p.inLock.Lock()
	p.outLock.Lock()
	defer p.inLock.Unlock()
	defer p.outLock.Unlock()
	for i := range p.listSets[0] {
		p.listSets[0][i].free()
	}
	for i := range p.listSets[1] {
		p.listSets[1][i].free()
	}
	p.listSets[0] = nil
	p.listSets[1] = nil
}
