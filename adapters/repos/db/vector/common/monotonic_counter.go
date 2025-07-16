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

import "sync/atomic"

type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// MonotonicCounter is a thread-safe counter that increments monotonically.
// It is a simple wrapper to avoid accidental resets, decrements, overflow,
// or other operations that could break the monotonicity.
type MonotonicCounter[T Unsigned] struct {
	value AtomicCounter[T]
}

// NewUint64Counter creates a new Uint64 MonotonicCounter with the given starting value.
// Next call to Next() will return the starting value + 1.
func NewUint64Counter(start uint64) *MonotonicCounter[uint64] {
	var c MonotonicCounter[uint64]
	var value atomic.Uint64
	value.Store(start)
	c.value = &value
	return &c
}

// NewUint32Counter creates a new Uint32 MonotonicCounter with the given starting value.
// Next call to Next() will return the starting value + 1.
func NewUint32Counter(start uint32) *MonotonicCounter[uint32] {
	var c MonotonicCounter[uint32]
	var value atomic.Uint32
	value.Store(start)
	c.value = &value
	return &c
}

// Next returns the next value of the counter.
// If the counter has hit its maximum value, it panics.
func (c *MonotonicCounter[T]) Next() T {
	next, ok := c.TryNext()
	if !ok {
		panic("MonotonicCounter overflow: reached maximum value")
	}
	return next
}

// TryNext returns (next, ok). When ok == false
// the counter has hit its maximum and next == zero(T).
func (c *MonotonicCounter[T]) TryNext() (T, bool) {
	for {
		current := c.value.Load()
		next := current + 1
		if next < current { // Check for overflow
			return next, false
		}
		if c.value.CompareAndSwap(current, next) {
			return next, true
		}
	}
}

// NextN returns the next n values as a range: [start, end].
// Panics if n is 0 or the range would overflow.
func (c *MonotonicCounter[T]) NextN(n T) (start T, end T) {
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
func (c *MonotonicCounter[T]) TryNextN(n T) (start, end T, ok bool) {
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
	}
}

type AtomicCounter[T any] interface {
	Add(delta T) T
	CompareAndSwap(old T, new T) (swapped bool)
	Load() T
}
