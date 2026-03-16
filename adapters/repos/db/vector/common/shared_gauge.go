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
	done  chan struct{} // closed when count reaches 0; replaced when count rises from 0
}

func NewSharedGauge() *SharedGauge {
	// Start at zero: done channel is already closed.
	done := make(chan struct{})
	close(done)
	return &SharedGauge{done: done}
}

func (sc *SharedGauge) Incr() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.count == 0 {
		// Open a fresh channel; waiters will block on it until count drops to 0 again.
		sc.done = make(chan struct{})
	}
	sc.count++
}

func (sc *SharedGauge) Decr() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.count == 0 {
		panic("illegal gauge state: count cannot be negative")
	}

	sc.count--

	if sc.count == 0 {
		close(sc.done)
	}
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
