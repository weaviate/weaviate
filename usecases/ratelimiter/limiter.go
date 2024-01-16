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

package ratelimiter

import "sync/atomic"

// Limiter is a thread-safe counter that can be used for rate-limiting requests
type Limiter struct {
	max     int64
	current int64
}

// New creates a [Limiter] with the specified maximum concurrent requests
func New(maxRequests int) *Limiter {
	return &Limiter{
		max: int64(maxRequests),
	}
}

// If there is still room, TryInc, increases the counter and returns true. If
// there are too many concurrent requests it does not increase the counter and
// returns false
func (l *Limiter) TryInc() bool {
	if l.max <= 0 {
		return true
	}

	new := atomic.AddInt64(&l.current, 1)

	if new <= l.max {
		return true
	}

	// undo unsuccessful increment
	atomic.AddInt64(&l.current, -1)
	return false
}

func (l *Limiter) Dec() {
	if l.max <= 0 {
		return
	}

	new := atomic.AddInt64(&l.current, -1)
	if new < 0 {
		// Should not happen unless some client called Dec multiple times.
		// Try to reset current to 0. It's ok if swap doesn't happen, since
		// someone else must've succeeded at fixing current value.
		atomic.CompareAndSwapInt64(&l.current, new, 0)
	}
}
