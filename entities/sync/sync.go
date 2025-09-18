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

package sync

import (
	"context"
	"fmt"
	"sync"
)

// KeyLocker it is a thread safe wrapper of sync.Map
// Usage: it's used in order to lock specific key in a map
// to synchronize concurrent access to a code block.
//
//	locker.Lock(id)
//	defer locker.Unlock(id)
type KeyLocker struct {
	m sync.Map
}

// NewKeyLocker creates Keylocker
func NewKeyLocker() *KeyLocker {
	return &KeyLocker{
		m: sync.Map{},
	}
}

// Lock it locks a specific bucket by it's ID
// to hold ant concurrent access to that specific item
//
//	do not forget calling Unlock() after locking it.
func (s *KeyLocker) Lock(ID string) {
	iLock := &sync.Mutex{}
	iLocks, _ := s.m.LoadOrStore(ID, iLock)

	iLock = iLocks.(*sync.Mutex)
	iLock.Lock()
}

// Unlock it unlocks a specific item by it's ID
func (s *KeyLocker) Unlock(ID string) {
	iLocks, _ := s.m.Load(ID)
	iLock := iLocks.(*sync.Mutex)
	iLock.Unlock()
}

// KeyRWLocker it is a thread safe wrapper of sync.Map
// Usage: it's used in order to lock/rlock specific key in a map
// to synchronize concurrent access to a code block.
//
//	locker.Lock(id)
//	defer locker.Unlock(id)
//
// or
//
//	locker.RLock(id)
//	defer locker.RUnlock(id)
type KeyRWLocker struct {
	m sync.Map
}

// NewKeyLocker creates Keylocker
func NewKeyRWLocker() *KeyRWLocker {
	return &KeyRWLocker{
		m: sync.Map{},
	}
}

// Lock it locks a specific bucket by it's ID
// to hold ant concurrent access to that specific item
//
//	do not forget calling Unlock() after locking it.
func (s *KeyRWLocker) Lock(ID string) {
	iLock := &sync.RWMutex{}
	iLocks, _ := s.m.LoadOrStore(ID, iLock)

	iLock = iLocks.(*sync.RWMutex)
	iLock.Lock()
}

// Unlock it unlocks a specific item by it's ID
func (s *KeyRWLocker) Unlock(ID string) {
	iLocks, _ := s.m.Load(ID)
	iLock := iLocks.(*sync.RWMutex)
	iLock.Unlock()
}

// RLock it rlocks a specific bucket by it's ID
// to hold ant concurrent access to that specific item
//
//	do not forget calling RUnlock() after rlocking it.
func (s *KeyRWLocker) RLock(ID string) {
	iLock := &sync.RWMutex{}
	iLocks, _ := s.m.LoadOrStore(ID, iLock)

	iLock = iLocks.(*sync.RWMutex)
	iLock.RLock()
}

// RUnlock it runlocks a specific item by it's ID
func (s *KeyRWLocker) RUnlock(ID string) {
	iLocks, _ := s.m.Load(ID)
	iLock := iLocks.(*sync.RWMutex)
	iLock.RUnlock()
}

// KeyLockerContext is a thread safe wrapper of sync.Map
// Usage: it's used in order to lock specific key in a map
// to synchronize concurrent access to a code block.
// It supports locking with a context.
// Note KeyLockerContext has almost identical code to KeyLocker
// but wasn't combined for performance reasons (eg don't want to
// slow down KeyLocker by adding context support). Feel free to
// explore DRYing them up if you want to. I looked into using
// generics for the underlying mutex type, but the performance
// hit was ~20% in benchmarks.
type KeyLockerContext struct {
	m sync.Map
}

// NewKeyLockerContext creates KeyLockerContext
func NewKeyLockerContext() *KeyLockerContext {
	return &KeyLockerContext{
		m: sync.Map{},
	}
}

// Lock locks a specific bucket by it's ID
// to hold any concurrent access to that specific item
//
//	do not forget to call Unlock() after locking it.
func (s *KeyLockerContext) Lock(ID string) {
	iLock := newContextMutex()
	iLocks, _ := s.m.LoadOrStore(ID, iLock)

	iLock = iLocks.(*contextMutex)
	iLock.Lock()
}

// TryLockWithContext tries to lock the mutex with a context.
// If the context is canceled while waiting for the lock, the lock attempt is aborted.
func (s *KeyLockerContext) TryLockWithContext(ID string, ctx context.Context) bool {
	iLock := newContextMutex()
	iLocks, _ := s.m.LoadOrStore(ID, iLock)
	iLock = iLocks.(*contextMutex)
	return iLock.TryLockWithContext(ctx)
}

// Unlock unlocks a specific item by it's ID
func (s *KeyLockerContext) Unlock(ID string) {
	iLocks, _ := s.m.Load(ID)
	if iLocks == nil {
		panic(fmt.Sprintf("unlock on non-existent ID: %s", ID))
	}
	iLock := iLocks.(*contextMutex)
	iLock.Unlock()
}

// contextMutex is a mutex that can be locked with a context.
// If the context is canceled while waiting for the lock, the lock attempt is aborted.
type contextMutex struct {
	// The underlying mutex.
	mu sync.Mutex
	// A channel to signal that the mutex has been released.
	// The channel is buffered with a capacity of 1, so that the sender (Unlock)
	// does not block if the receiver is not ready.
	ch chan struct{}
}

// newContextMutex creates a new contextMutex.
func newContextMutex() *contextMutex {
	return &contextMutex{
		// Initialize the channel with a buffer size of 1.
		// This is important for the Unlock method to not block.
		// When a goroutine unlocks the mutex, it sends a value to this channel.
		// If another goroutine is waiting in LockContext, it will receive the value
		// and acquire the lock. If no goroutine is waiting, the value is stored
		// in the buffer and the next goroutine that calls LockContext will
		// immediately acquire the lock.
		ch: make(chan struct{}, 1),
	}
}

// Lock locks the mutex. If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *contextMutex) Lock() {
	m.mu.Lock()
}

// Unlock unlocks the mutex. It is a run-time error if m is not locked on entry to Unlock.
// A locked Mutex is not associated with a particular goroutine. It is allowed for
// one goroutine to lock a Mutex and then arrange for another goroutine to unlock it.
func (m *contextMutex) Unlock() {
	// To unlock the mutex, we first need to acquire the underlying mutex.
	// This ensures that we don't have a race condition where two goroutines
	// try to unlock the mutex at the same time.
	m.mu.Unlock()

	// After unlocking the mutex, we need to signal to any waiting goroutines
	// that the mutex is now available. We do this by sending a value to the
	// channel. We use a select statement with a default case to prevent
	// the Unlock method from blocking if the channel buffer is full. This can
	// happen if the mutex is unlocked multiple times without being locked.
	select {
	case m.ch <- struct{}{}:
	default:
		// Do nothing if the channel is already full.
		// This can happen if Unlock is called multiple times without a corresponding Lock.
	}
}

func (m *contextMutex) TryLock() bool {
	return m.mu.TryLock()
}

// LockContext locks the mutex. If the lock is already in use, the calling
// goroutine blocks until the mutex is available or the context is canceled.
// If the context is Done/Err before the lock aquisition is attempted,
// the lock is not acquired.
func (m *contextMutex) TryLockWithContext(ctx context.Context) bool {
	// First, try to acquire the lock immediately.
	// This is a fast path that avoids the overhead of the select statement.
	if ctx.Err() == nil && m.mu.TryLock() {
		return true
	}

	// If the lock is not available, we wait for either the context to be
	// canceled or the mutex to be released.
	for {
		select {
		case <-ctx.Done():
			// The context was canceled, so we return an error.
			return false
		case <-m.ch:
			// make sure the lock is still available, don't block if someone else
			// took it already
			if m.mu.TryLock() {
				return true
			}
		}
	}
}
