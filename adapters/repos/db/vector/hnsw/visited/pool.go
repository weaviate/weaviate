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
)

type Pool struct {
	pool *sync.Pool
}

// NewPool creates a new pool with specified size.
// listSetSize specifies the size of a list at creation time point
func NewPool(size int) *Pool {
	return &Pool{
		pool: &sync.Pool{
			New: func() interface{} {
				return NewList(size)
			},
		},
	}
}

// Borrow return a free list
func (p *Pool) Borrow(capacity int) *ListSet {
	l := p.pool.Get().(*ListSet)
	if l.Len() < capacity {
		l.ResetCap(capacity)
	} else {
		l.Reset()
	}

	return l
}

// Return list l to the pool
// The list l might be thrown if l.Len() > listSetSize*1.10
func (p *Pool) Return(l *ListSet) {
	p.pool.Put(l)
}
