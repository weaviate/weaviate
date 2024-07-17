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

package sync

import (
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
