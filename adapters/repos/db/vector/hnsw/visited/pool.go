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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type Stats struct {
	ReturnTooLarge atomic.Int64
	ReturnStored   atomic.Int64
	BorrowReused   atomic.Int64
	BorrowNew      atomic.Int64
}

type Pool struct {
	sync.Mutex
	listSetSize int
	listSets    []ListSet
	stats       Stats
	closeStats  chan struct{}
}

// NewPool creates a new pool with specified size.
// listSetSize specifies the size of a list at creation time point
func NewPool(size int, listSetSize int) *Pool {
	p := &Pool{
		listSetSize: listSetSize,
		listSets:    make([]ListSet, size), // make enough room
	}

	for i := 0; i < size; i++ {
		p.listSets[i] = NewList(listSetSize)
	}

	d, err := time.ParseDuration(os.Getenv("VISITED_POOL_LOG_INTERVAL"))
	if err != nil {
		d = 10 * time.Second
	}

	logger := logrus.New().WithField("component", "visited_pool")
	p.closeStats = make(chan struct{})

	go func() {
		t := time.NewTicker(d)
		defer t.Stop()

		for {
			select {
			case <-p.closeStats:
				return
			case <-t.C:
				logger.WithFields(logrus.Fields{
					"return_too_large": p.stats.ReturnTooLarge.Load(),
					"return_stored":    p.stats.ReturnStored.Load(),
					"borrow_reused":    p.stats.BorrowReused.Load(),
					"borrow_new":       p.stats.BorrowNew.Load(),
				}).Info("visited pool stats")
			}
		}
	}()

	return p
}

// Borrow return a free list
func (p *Pool) Borrow() ListSet {
	p.Lock()

	if n := len(p.listSets); n > 0 {
		l := p.listSets[n-1]
		p.listSets[n-1].free() // prevent memory leak
		p.listSets = p.listSets[:n-1]
		p.Unlock()

		p.stats.BorrowReused.Add(1)
		return l
	}
	p.Unlock()

	p.stats.BorrowNew.Add(1)
	return NewList(p.listSetSize)
}

// Return list l to the pool
// The list l might be thrown if l.Len() > listSetSize*1.10
func (p *Pool) Return(l ListSet) {
	n := l.Len()
	if n < p.listSetSize || n > p.listSetSize*11/10 { // 11/10 could be tuned
		p.stats.ReturnTooLarge.Add(1)
		return
	}
	l.Reset()

	p.Lock()
	defer p.Unlock()

	p.stats.ReturnStored.Add(1)
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

	close(p.closeStats)
}
