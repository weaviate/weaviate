package common

import "sync"

type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// MonotonicCounter is a thread-safe counter that increments monotonically.
// It is a simple wrapper to avoid accidental resets, decrements, overflow,
// or other operations that could break the monotonicity.
// The zero value of the MonotonicCounter is a valid counter with a starting value of zero.
type MonotonicCounter[T Unsigned] struct {
	m     sync.Mutex
	value T
}

// NewMonotonicCounter creates a new MonotonicCounter with the given starting value.
// Next call to Next() will return the starting value + 1.
func NewMonotonicCounter[T Unsigned](start T) *MonotonicCounter[T] {
	return &MonotonicCounter[T]{
		value: start,
	}
}

// Next returns the next value of the counter.
// If the counter has hit its maximum value, it panics.
func (c *MonotonicCounter[T]) Next() T {
	next, ok := c.TryNext()
	if !ok {
		panic("monotonic counter has hit its maximum value")
	}
	return next
}

// TryNext returns (next, ok). When ok == false
// the counter has hit its maximum and next == zero(T).
func (c *MonotonicCounter[T]) TryNext() (T, bool) {
	c.m.Lock()
	defer c.m.Unlock()

	max := ^T(0)
	if c.value == max {
		var zero T
		return zero, false
	}
	c.value++
	return c.value, true
}

// NextN returns the next n values as a range: [start, end].
// Panics if n is 0 or the range would overflow.
func (c *MonotonicCounter[T]) NextN(n T) (start, end T) {
	start, end, ok := c.TryNextN(n)
	if !ok {
		panic("monotonic: counter overflow in NextN")
	}
	return start, end
}

// TryNextN returns the next n values as a range [start, end].
// If n is 0, it returns (0, 0, true).
// If the counter has hit its maximum value or the range would overflow,
// it returns (0, 0, false).
func (c *MonotonicCounter[T]) TryNextN(n T) (start, end T, ok bool) {
	if n == 0 {
		return
	}

	c.m.Lock()
	defer c.m.Unlock()

	max := ^T(0)
	if max-c.value < n {
		return
	}

	start = c.value + 1
	end = c.value + n
	c.value = end
	ok = true
	return
}
