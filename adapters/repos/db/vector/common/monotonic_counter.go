//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"runtime"
	"sync/atomic"
)

// MonotonicCounter is a thread-safe counter that increments monotonically.
// It is a simple wrapper to avoid accidental resets, decrements, overflow,
// or other operations that could break the monotonicity.
// The zero value is a valid counter that starts at 0.
type MonotonicCounter struct {
	value atomic.Uint64
}

// NewMonotonicCounter creates a new MonotonicCounter with the given starting value.
// Next call to Next() will return the starting value + 1.
// For a counter that starts at 0, you can use the zero value directly.
func NewMonotonicCounter(start uint64) *MonotonicCounter {
	var c MonotonicCounter
	c.value.Store(start)
	return &c
}

// Next returns the next value of the counter.
// If the counter has hit its maximum value, it panics.
func (c *MonotonicCounter) Next() uint64 {
	next, ok := c.TryNext()
	if !ok {
		panic("MonotonicCounter overflow: reached maximum value")
	}
	return next
}

// TryNext returns (next, ok). When ok == false
// the counter has hit its maximum and next == zero(T).
func (c *MonotonicCounter) TryNext() (uint64, bool) {
	var attempts int

	for {
		current := c.value.Load()
		next := current + 1
		if next < current { // Check for overflow
			return next, false
		}
		if c.value.CompareAndSwap(current, next) {
			return next, true
		}

		attempts++
		if attempts < 4 {
			continue // Spin
		}

		runtime.Gosched() // Yield to prevent busy-waiting
	}
}

// NextN returns the next n values as a range: [start, end].
// If n is 0, it returns (0, 0).
// Panics if the range would overflow.
func (c *MonotonicCounter) NextN(n uint64) (start uint64, end uint64) {
	var ok bool
	start, end, ok = c.TryNextN(n)
	if !ok {
		panic("MonotonicCounter overflow: reached maximum value")
	}
	return start, end
}

// TryNextN returns the next n values as a range [start, end].
// If n is 0, it returns (0, 0, true).
// If the counter has hit its maximum value or the range would overflow,
// it returns (0, 0, false).
func (c *MonotonicCounter) TryNextN(n uint64) (start, end uint64, ok bool) {
	if n == 0 {
		return 0, 0, true
	}

	var attempts int
	for {
		current := c.value.Load()
		end = current + n
		if end < current { // Check for overflow
			return 0, 0, false
		}
		if c.value.CompareAndSwap(current, end) {
			start = current + 1
			return start, end, true
		}

		attempts++
		if attempts < 4 {
			continue // Spin
		}

		runtime.Gosched() // Yield to prevent busy-waiting
	}
}
