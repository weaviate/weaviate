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

package roaringset

import "sync"

// PendingMerges is a concurrency-safe queue of immutable items (flushed
// memtables) waiting to be merged into a larger structure by a single
// background drain.
type PendingMerges[T any] struct {
	lock    sync.Mutex
	pending []T
}

func NewPendingMerges[T any]() *PendingMerges[T] {
	return &PendingMerges[T]{pending: make([]T, 0, 8)}
}

// Add appends item and reports whether it landed on an empty queue. Only then
// must the caller start ONE background drain (via errors.GoWrapper, so the
// goroutine stays in the caller); items added while a drain is running are
// picked up by that drain.
func (p *PendingMerges[T]) Add(item T) (startDrain bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.pending = append(p.pending, item)
	return len(p.pending) == 1
}

// Drain merges every queued item in order, including items added mid-drain,
// and resets the queue once caught up. It does no locking of its own beyond
// the queue mutex: the caller MUST hold its own structure lock for the whole
// Drain so that merge and queue reset stay atomic vs readers. Lock order is
// always caller-lock -> queue mutex, never reversed.
func (p *PendingMerges[T]) Drain(merge func(item T)) {
	i := 0
	for {
		p.lock.Lock()
		if i == len(p.pending) {
			p.pending = p.pending[:0]
			p.lock.Unlock()
			return
		}
		item := p.pending[i]
		i++
		p.lock.Unlock()

		merge(item)
	}
}

// Snapshot returns a copy of the items still pending, nil when the queue is
// empty. Items are immutable, so the copy can be read after locks are
// released.
func (p *PendingMerges[T]) Snapshot() []T {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.pending) == 0 {
		return nil
	}
	out := make([]T, len(p.pending))
	copy(out, p.pending)
	return out
}

func (p *PendingMerges[T]) Len() int {
	p.lock.Lock()
	defer p.lock.Unlock()

	return len(p.pending)
}
