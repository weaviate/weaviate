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

package distributedtask

import "context"

// ConcurrencyLimiter is a channel-based semaphore that limits the number of
// concurrent operations. Acquire blocks until a slot is available or the
// context is cancelled. Release returns a slot to the pool.
type ConcurrencyLimiter struct {
	sem chan struct{}
}

// NewConcurrencyLimiter creates a limiter that allows up to maxConcurrency
// concurrent operations. Panics if maxConcurrency < 1.
func NewConcurrencyLimiter(maxConcurrency int) *ConcurrencyLimiter {
	if maxConcurrency < 1 {
		panic("maxConcurrency must be >= 1")
	}
	return &ConcurrencyLimiter{sem: make(chan struct{}, maxConcurrency)}
}

// Acquire blocks until a slot is available or ctx is cancelled.
func (l *ConcurrencyLimiter) Acquire(ctx context.Context) error {
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns a slot to the pool. Must be called exactly once for each
// successful Acquire.
func (l *ConcurrencyLimiter) Release() {
	<-l.sem
}
