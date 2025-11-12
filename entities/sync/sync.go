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

// Unlock unlocks a specific item by it's ID.
// It panics if the item exists but is not locked.
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
	// ch has a message on it when the mutex is locked, empty when unlocked.
	ch chan struct{}
}

// newContextMutex creates a new contextMutex.
func newContextMutex() *contextMutex {
	return &contextMutex{
		ch: make(chan struct{}, 1),
	}
}

// Lock locks the mutex. If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *contextMutex) Lock() {
	m.ch <- struct{}{}
}

// Unlock unlocks the mutex. It panics if m is not locked on entry to Unlock.
func (m *contextMutex) Unlock() {
	select {
	case <-m.ch:
	default:
		panic("unlock of unlocked contextMutex")
	}
}

// TryLockWithContext locks the mutex. If the lock is already in use, the
// calling goroutine blocks until the mutex is available or the context is done.
// If the context is done before the lock acquisition is attempted,  the lock
// is not acquired.
// Importantly, TryLockWithContext does not immediately return false if the
// lock is not currently available, rather it will block until the lock is
// available or the context is done. This behavior differs from sync.Mutex.TryLock.
func (m *contextMutex) TryLockWithContext(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case m.ch <- struct{}{}:
		return true
	}
}
