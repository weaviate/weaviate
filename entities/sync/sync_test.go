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
	"testing"

	"github.com/stretchr/testify/require"
)

func mutexLocked(m *sync.Mutex) bool {
	rlocked := m.TryLock()
	if rlocked {
		defer m.Unlock()
	}
	return !rlocked
}

func rwMutexLocked(m *sync.RWMutex) bool {
	// can not RLock
	rlocked := m.TryRLock()
	if rlocked {
		defer m.RUnlock()
	}
	return !rlocked
}

func rwMutexRLocked(m *sync.RWMutex) bool {
	// can not Lock, but can RLock
	locked := m.TryLock()
	if locked {
		defer m.Unlock()
		return false
	}
	rlocked := m.TryRLock()
	if rlocked {
		defer m.RUnlock()
	}
	return rlocked
}

func TestKeyLockerLockUnlock(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker()

	s.Lock("t1")
	lock, _ := s.m.Load("t1")
	r.True(mutexLocked(lock.(*sync.Mutex)))

	s.Unlock("t1")
	lock, _ = s.m.Load("t1")
	r.False(mutexLocked(lock.(*sync.Mutex)))

	s.Lock("t2")
	lock, _ = s.m.Load("t2")
	r.True(mutexLocked(lock.(*sync.Mutex)))

	s.Unlock("t2")
	lock, _ = s.m.Load("t2")
	r.False(mutexLocked(lock.(*sync.Mutex)))
}

func TestKeyRWLockerLockUnlock(t *testing.T) {
	r := require.New(t)
	s := NewKeyRWLocker()

	s.Lock("t1")
	lock, _ := s.m.Load("t1")
	r.True(rwMutexLocked(lock.(*sync.RWMutex)))
	r.False(rwMutexRLocked(lock.(*sync.RWMutex)))

	s.Unlock("t1")
	lock, _ = s.m.Load("t1")
	r.False(rwMutexLocked(lock.(*sync.RWMutex)))
	r.False(rwMutexRLocked(lock.(*sync.RWMutex)))

	s.Lock("t2")
	lock, _ = s.m.Load("t2")
	r.True(rwMutexLocked(lock.(*sync.RWMutex)))
	r.False(rwMutexRLocked(lock.(*sync.RWMutex)))

	s.Unlock("t2")
	lock, _ = s.m.Load("t2")
	r.False(rwMutexLocked(lock.(*sync.RWMutex)))
	r.False(rwMutexRLocked(lock.(*sync.RWMutex)))

	s.RLock("t1")
	lock, _ = s.m.Load("t1")
	r.False(rwMutexLocked(lock.(*sync.RWMutex)))
	r.True(rwMutexRLocked(lock.(*sync.RWMutex)))

	s.RUnlock("t1")
	lock, _ = s.m.Load("t1")
	r.False(rwMutexLocked(lock.(*sync.RWMutex)))
	r.False(rwMutexRLocked(lock.(*sync.RWMutex)))

	s.RLock("t2")
	lock, _ = s.m.Load("t2")
	r.False(rwMutexLocked(lock.(*sync.RWMutex)))
	r.True(rwMutexRLocked(lock.(*sync.RWMutex)))

	s.RUnlock("t2")
	lock, _ = s.m.Load("t2")
	r.False(rwMutexLocked(lock.(*sync.RWMutex)))
	r.False(rwMutexRLocked(lock.(*sync.RWMutex)))
}
