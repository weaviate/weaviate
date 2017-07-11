// Semaphore implementation that adds so necessary synchronization
// primitive into Go language. It uses built-in channel with empty struct
// so it doesn't utilize a lot of memory to buffer acquired elements.

package nsync

import "time"

// Semaphore implementation uses built in channel using 0 size struct values.
type Semaphore struct {
	sch chan struct{}
}

// NewSemaphore returns an instance of a semaphore.
func NewSemaphore(value int) *Semaphore {
	return &Semaphore{
		sch: make(chan struct{}, value),
	}
}

// Acquire tries to acquire semaphore lock. If no luck it will block.
func (s *Semaphore) Acquire() {
	s.sch <- struct{}{}
}

// Release releases acquired semaphore. If semaphore is not acquired it will panic.
func (s *Semaphore) Release() {
	select {
	case <-s.sch:
	default:
		panic("No semaphore locks!")
	}
}

// TryAcquire tries to acquire semaphore. Returns true/false if success/failure accordingly.
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.sch <- struct{}{}:
		return true
	default:
		return false
	}
}

// TryAcquireTimeout tries to acquire semaphore for a specified time interval.
// Returns true/false if success/failure accordingly.
func (s *Semaphore) TryAcquireTimeout(d time.Duration) bool {
	select {
	case s.sch <- struct{}{}:
		return true
	case <-time.After(d):
		return false
	}
}

// Value returns the number of currently acquired semaphores.
func (s *Semaphore) Value() int {
	return len(s.sch)
}
