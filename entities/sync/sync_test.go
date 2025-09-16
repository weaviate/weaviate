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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mutexLocked(m *sync.Mutex) bool {
	rlocked := m.TryLock()
	if rlocked {
		defer m.Unlock()
	}
	return !rlocked
}

func contextMutexLocked(m *contextMutex) bool {
	l := m.mu.TryLock()
	if l {
		defer m.mu.Unlock()
	}
	return !l
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

func TestKeyLockerContextMutexLockUnlock(t *testing.T) {
	r := require.New(t)
	s := NewKeyLockerContext()

	s.Lock("t1")
	lock, _ := s.m.Load("t1")
	r.True(contextMutexLocked(lock.(*contextMutex)))

	s.Unlock("t1")
	lock, _ = s.m.Load("t1")
	r.False(contextMutexLocked(lock.(*contextMutex)))

	s.TryLockWithContext("t2", context.Background())
	lock, _ = s.m.Load("t2")
	r.True(contextMutexLocked(lock.(*contextMutex)))

	s.Unlock("t2")
	lock, _ = s.m.Load("t2")
	r.False(contextMutexLocked(lock.(*contextMutex)))
}

// Lock a key, then try to lock it concurrently from multiple goroutines with a context that gets cancelled
// all should give up after cancellation and return false from TryLockWithContext
func TestKeyLockerContextMutexLockConcurrentCancel(t *testing.T) {
	s := NewKeyLockerContext()
	s.Lock("t1")
	defer s.Unlock("t1")

	numGoroutines := 10
	ctx, cancel := context.WithCancel(context.Background())
	wgOuter := sync.WaitGroup{}
	wgOuter.Add(numGoroutines)
	wgInner := sync.WaitGroup{}
	wgInner.Add(numGoroutines)
	counter := atomic.Int32{}
	// try to lock concurrently, should all wait
	for i := 0; i < numGoroutines; i++ {
		go func() {
			done := s.TryLockWithContext("t1", ctx)
			if !done {
				counter.Add(1)
			}
			wgInner.Done()
		}()
		wgOuter.Done()
	}
	wgOuter.Wait() // make sure all goroutines are started
	cancel()       // cancel context to stop trying to lock

	// now all goroutines should have given up getting the lock
	wgInner.Wait()
	require.Equal(t, int32(numGoroutines), counter.Load())
}

func TestKeyLockerContextMutexLockConcurrentUnlock(t *testing.T) {
	s := NewKeyLockerContext()
	s.Lock("t1")

	numGoroutines := 10
	wgOuter := sync.WaitGroup{}
	wgOuter.Add(numGoroutines)
	wgInner := sync.WaitGroup{}
	wgInner.Add(numGoroutines)
	counter := atomic.Int32{}
	// try to lock concurrently, should all wait
	for i := 0; i < numGoroutines; i++ {
		go func() {
			done := s.TryLockWithContext("t1", context.Background())
			if !done {
				counter.Add(1)
			} else {
				counter.Add(-1)
				s.Unlock("t1")
			}
			wgInner.Done()
		}()
		wgOuter.Done()
	}
	wgOuter.Wait() // make sure all goroutines are started
	s.Unlock("t1") // unlock so that one of the goroutines can acquire the lock

	wgInner.Wait() // wait for all goroutines to be done
	require.Equal(t, -int32(numGoroutines), counter.Load())
}

func TestKeyLockerContextMultipleContext(t *testing.T) {
	s := NewKeyLockerContext()
	s.Lock("t1")

	numGoroutines := 10

	contexts := make([]struct {
		context context.Context
		cancel1 context.CancelFunc
	}, numGoroutines)
	for i := range contexts {
		contexts[i].context, contexts[i].cancel1 = context.WithCancel(context.Background())
	}

	wgOuter := sync.WaitGroup{}
	wgOuter.Add(numGoroutines)
	wgInner := sync.WaitGroup{}
	wgInner.Add(numGoroutines)
	counterCancelled := atomic.Int32{}
	counterSucceded := atomic.Int32{}
	// try to lock concurrently, should all wait
	for i := 0; i < numGoroutines; i++ {
		go func() {
			done := s.TryLockWithContext("t1", contexts[i].context)
			if !done {
				counterCancelled.Add(1)
			} else {
				counterSucceded.Add(1)
				s.Unlock("t1")
			}
			wgInner.Done()
		}()
		wgOuter.Done()
	}
	wgOuter.Wait() // make sure all goroutines are started

	// cancel some contexts
	for i := range contexts[:5] {
		contexts[i].cancel1()
	}
	time.Sleep(100 * time.Millisecond) // wait a little to give goroutines time to react to cancellation
	require.Equal(t, int32(0), counterSucceded.Load())
	require.Equal(t, int32(5), counterCancelled.Load())

	// unlock original lock so remaining goroutines can acquire the lock
	s.Unlock("t1")

	wgInner.Wait()
	require.Equal(t, int32(5), counterSucceded.Load())
	require.Equal(t, int32(5), counterCancelled.Load())
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
