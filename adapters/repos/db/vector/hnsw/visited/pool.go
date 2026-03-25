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

import "sync"

type Pool struct {
	pool sync.Pool
}

func NewPool(listSetSize int) *Pool {
	p := &Pool{}
	p.pool.New = func() interface{} {
		return NewFastSet(listSetSize)
	}
	return p
}

func (p *Pool) Borrow() *FastSet {
	return p.pool.Get().(*FastSet)
}

func (p *Pool) Return(l *FastSet) {
	if l == nil {
		return
	}
	l.Reset()
	p.pool.Put(l)
}
