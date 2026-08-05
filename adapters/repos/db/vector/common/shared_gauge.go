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

package common

import (
	"context"
	"sync"
)

// SharedGauge is a thread-safe gauge that can be shared between multiple goroutines.
// It is used to track the number of running tasks, and allows to wait until all tasks are done.
type SharedGauge struct {
	mu    sync.Mutex
	count int64
	done  chan struct{} // lazily created by Wait while count>0; closed and cleared when count returns to 0
}

func NewSharedGauge() *SharedGauge {
	return &SharedGauge{}
}

// Incr increments the gauge and returns the new count.
func (sc *SharedGauge) Incr() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.count++

	return sc.count
}

// Decr decrements the gauge and returns the new count.
func (sc *SharedGauge) Decr() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.count == 0 {
		panic("illegal gauge state: count cannot be negative")
	}

	sc.count--

	if sc.count == 0 && sc.done != nil {
		// Wake any waiters and drop the channel so the next waiter creates a fresh one.
		close(sc.done)
		sc.done = nil
	}

	return sc.count
}

func (sc *SharedGauge) Count() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.count
}

// Wait blocks until the count reaches zero or the context is cancelled.
// Returns ctx.Err() if the context was cancelled before the count reached zero.
func (sc *SharedGauge) Wait(ctx context.Context) error {
	for {
		sc.mu.Lock()
		if sc.count == 0 {
			sc.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			sc.mu.Unlock()
			return err
		}
		if sc.done == nil {
			sc.done = make(chan struct{})
		}
		done := sc.done
		sc.mu.Unlock()

		select {
		case <-done:
			// count reached 0 at some point; loop to confirm it's still 0
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
