//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package ratelimiter

import "sync"

// Limiter is a thread-safe counter that can be used for rate-limiting requests
type Limiter struct {
	lock    sync.Mutex
	max     int
	current int
}

// New creates a [Limiter] with the specified maximum concurrent requests
func New(maxRequests int) *Limiter {
	return &Limiter{
		max: maxRequests,
	}
}

// If there is still room, TryInc, increases the counter and returns true. If
// there are too many concurrent requests it does not increase the counter and
// returns false
func (l *Limiter) TryInc() bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.max < 0 {
		return true
	}

	if l.current < l.max {
		l.current++
		return true

	}

	return false
}

func (l *Limiter) Dec() {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.max < 0 {
		return
	}

	l.current--
}
