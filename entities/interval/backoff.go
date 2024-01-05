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

package interval

import (
	"sort"
	"time"
)

var defaultBackoffs = []time.Duration{
	time.Duration(0),
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
	1 * time.Hour,
	12 * time.Hour,
}

// BackoffTimer tracks a given range of intervals with increasing duration
type BackoffTimer struct {
	backoffLevel int
	backoffs     []time.Duration
	lastInterval time.Time
}

// NewBackoffTimer constructs and returns a *BackoffTimer instance
// If no backoffs are provided, defaultBackoffs is used. When the
// last backoff duration has elapsed, the timer will use the final
// duration for the remainder of the BackoffTimer's lifetime
func NewBackoffTimer(backoffs ...time.Duration) *BackoffTimer {
	boff := &BackoffTimer{backoffs: backoffs}
	if len(backoffs) == 0 {
		boff.backoffs = defaultBackoffs
	} else {
		sort.Slice(backoffs, func(i, j int) bool {
			return backoffs[i] < backoffs[j]
		})
	}
	return boff
}

// IncreaseInterval bumps the duration of the interval up to the next given value
func (b *BackoffTimer) IncreaseInterval() {
	b.lastInterval = time.Now()
	if b.backoffLevel < len(b.backoffs) {
		b.backoffLevel += 1
	}
}

// IntervalElapsed returns if the current interval has elapsed
func (b *BackoffTimer) IntervalElapsed() bool {
	return time.Since(b.lastInterval) > b.calculateInterval()
}

// Reset returns BackoffTimer to its original empty state
func (b *BackoffTimer) Reset() {
	b.lastInterval = time.Time{}
	b.backoffLevel = 0
}

func (b *BackoffTimer) calculateInterval() time.Duration {
	if b.backoffLevel >= len(b.backoffs) {
		return b.backoffs[len(b.backoffs)-1]
	}

	interval := b.backoffs[b.backoffLevel]

	return interval
}
