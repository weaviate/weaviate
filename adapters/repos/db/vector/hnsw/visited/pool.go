//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package visited

import (
	"sync"
)

type Pool struct {
	sync.Mutex
	listSize int
	lists    []*List
}

func NewPool(poolSize int, listSize int) *Pool {
	p := &Pool{
		listSize: listSize,
		lists:    make([]*List, poolSize),
	}

	for i := 0; i < poolSize; i++ {
		p.lists[i] = NewList(listSize)
	}

	return p
}

func (p *Pool) Borrow() *List {
	p.Lock()
	defer p.Unlock()

	if len(p.lists) > 0 {
		l := p.lists[0]
		p.lists = p.lists[1:]
		return l
	}

	return NewList(p.listSize)
}

func (p *Pool) Return(l *List) {
	p.Lock()
	defer p.Unlock()

	if len(l.store) != p.listSize {
		// discard this list, it does not match our current criteria
		l = nil
		return
	}

	l.Reset()
	p.lists = append(p.lists, l)
}

func (p *Pool) Destroy() {
	for i := range p.lists {
		p.lists[i] = nil
	}

	p.lists = nil
}
