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

package dynsemaphore

import (
	"context"
	"fmt"
	"sync"
)

// DynamicWeighted matches semaphore.Weighted API exactly,
// except the maximum size is provided dynamically.
type DynamicWeighted struct {
	mu      sync.Mutex
	cur     int64
	waiters []waiter
	limitFn func() int64

	parent *DynamicWeighted
}

type waiter struct {
	n     int64
	ready chan struct{}
}

func NewDynamicWeighted(limitFn func() int64) *DynamicWeighted {
	return NewDynamicWeightedWithParent(nil, limitFn)
}

func NewDynamicWeightedWithParent(parent *DynamicWeighted, limitFn func() int64) *DynamicWeighted {
	return &DynamicWeighted{
		parent:  parent,
		limitFn: limitFn,
	}
}

func (s *DynamicWeighted) Acquire(ctx context.Context, n int64) error {
	// 1. Acquire parent first (global limiter)
	if s.parent != nil {
		if err := s.parent.Acquire(ctx, n); err != nil {
			return err
		}
	}

	// 2. Try to acquire local semaphore
	if err := s.acquireLocal(ctx, n); err != nil {
		// rollback parent if local acquisition fails
		if s.parent != nil {
			s.parent.Release(n)
		}
		return err
	}

	return nil
}

func (s *DynamicWeighted) acquireLocal(ctx context.Context, n int64) error {
	s.mu.Lock()

	limit := s.limitFn()
	if n > limit {
		s.mu.Unlock()
		return fmt.Errorf(
			"dynsemaphore: requested %d exceeds current limit %d",
			n, limit,
		)
	}

	// Fast path
	if len(s.waiters) == 0 && s.cur+n <= limit {
		s.cur += n
		s.mu.Unlock()
		return nil
	}

	// Slow path
	w := waiter{
		n:     n,
		ready: make(chan struct{}),
	}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		s.mu.Lock()

		// Re-check: was the waiter already granted?
		select {
		case <-w.ready:
			// Grant won the race; acquisition succeeded
			s.mu.Unlock()
			return nil
		default:
		}

		// Still waiting → remove from waiters
		for i, ww := range s.waiters {
			if ww.ready == w.ready {
				copy(s.waiters[i:], s.waiters[i+1:])
				s.waiters = s.waiters[:len(s.waiters)-1]
				break
			}
		}

		s.mu.Unlock()
		return ctx.Err()
	}
}

func (s *DynamicWeighted) TryAcquire(n int64) bool {
	// Parent must succeed first
	if s.parent != nil && !s.parent.TryAcquire(n) {
		return false
	}

	s.mu.Lock()
	limit := s.limitFn()

	if n > limit || len(s.waiters) > 0 || s.cur+n > limit {
		s.mu.Unlock()
		if s.parent != nil {
			s.parent.Release(n)
		}
		return false
	}

	s.cur += n
	s.mu.Unlock()
	return true
}

func (s *DynamicWeighted) Release(n int64) {
	// Release local first
	s.releaseLocal(n)

	// Then parent
	if s.parent != nil {
		s.parent.Release(n)
	}
}

func (s *DynamicWeighted) releaseLocal(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cur -= n
	if s.cur < 0 {
		panic("dynsemaphore: released more than held")
	}

	limit := s.limitFn()

	for len(s.waiters) > 0 {
		w := s.waiters[0]
		if s.cur+w.n > limit {
			return
		}

		s.cur += w.n
		close(w.ready)
		s.waiters = s.waiters[1:]
	}
}
