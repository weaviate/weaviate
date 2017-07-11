package nsync

import "time"

// TryMutex is another implementation to the lock primitives
// providing additional way to acquire locks.
type TryMutex struct {
	c chan struct{}
}

// NewTryMutex makes a new TryMutex instance.
func NewTryMutex() *TryMutex {
	return &TryMutex{
		c: make(chan struct{}, 1),
	}
}

// Lock acquires the lock.
func (tm TryMutex) Lock() {
	tm.c <- struct{}{}
}

// TryLock tries to acquires the lock returning true on success.
func (tm TryMutex) TryLock() bool {
	select {
	case tm.c <- struct{}{}:
		return true
	default:
		return false
	}
}

// TryLockTimeout tries to acquires the lock returning true on success.
// Attempt to acquire the lock will timeout after the caller defined interval.
func (tm TryMutex) TryLockTimeout(timeout time.Duration) bool {
	select {
	case tm.c <- struct{}{}:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Unlock releases the lock. If lock hasn't been acquired
// function will panic.
func (tm TryMutex) Unlock() {
	select {
	case <-tm.c:
	default:
		panic("Attemt to release not acquired mutex")
	}
}
