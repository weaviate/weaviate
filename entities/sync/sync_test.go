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
	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)
	counter := atomic.Int32{}
	// try to lock concurrently, should all wait
	for i := 0; i < numGoroutines; i++ {
		go func() {
			done := s.TryLockWithContext("t1", ctx)
			if !done {
				counter.Add(1)
			}
			wg.Done()
		}()
		wgOuter.Done()
	}
	wgOuter.Wait() // make sure all goroutines are started
	cancel()       // cancel context to stop trying to lock

	// now all goroutines should have given up getting the lock
	wg.Wait()
	require.Equal(t, int32(numGoroutines), counter.Load())
}

func TestKeyLockerContextMutexLockConcurrentUnlock(t *testing.T) {
	s := NewKeyLockerContext()
	s.Lock("t1")

	numGoroutines := 10
	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)
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
			wg.Done()
		}()
	}
	s.Unlock("t1") // unlock so that one of the goroutines can acquire the lock

	wg.Wait() // wait for all goroutines to be done
	require.Equal(t, -int32(numGoroutines), counter.Load())
}

func TestKeyLockerContextMultipleContext(t *testing.T) {
	s := NewKeyLockerContext()
	s.Lock("t1")

	numGoroutines := 10

	contexts := make([]struct {
		context context.Context
		cancel  context.CancelFunc
	}, numGoroutines)
	for i := range contexts {
		contexts[i].context, contexts[i].cancel = context.WithCancel(context.Background())
	}

	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)

	cancelNum := numGoroutines / 2
	wgCancel := sync.WaitGroup{}
	wgCancel.Add(cancelNum)
	counterCancelled := atomic.Int32{}
	counterSucceeded := atomic.Int32{}
	// try to lock concurrently, should all wait
	for i := 0; i < numGoroutines; i++ {
		go func() {
			done := s.TryLockWithContext("t1", contexts[i].context)
			if !done {
				counterCancelled.Add(1)
				wgCancel.Done()
			} else {
				counterSucceeded.Add(1)
				s.Unlock("t1")
			}
			wg.Done()
		}()
	}

	// cancel some contexts
	for i := range contexts[:cancelNum] {
		contexts[i].cancel()
	}

	wgCancel.Wait()

	require.Equal(t, int32(0), counterSucceeded.Load())
	require.Equal(t, int32(cancelNum), counterCancelled.Load())

	// unlock original lock so remaining goroutines can acquire the lock
	s.Unlock("t1")

	wg.Wait()
	require.Equal(t, int32(numGoroutines-cancelNum), counterSucceeded.Load())
	require.Equal(t, int32(cancelNum), counterCancelled.Load())
}

func TestKeyLockerContextWithNormalLock(t *testing.T) {
	s := NewKeyLockerContext()
	s.Lock("t1")

	numGoroutines := 10
	wg := sync.WaitGroup{}
	wg.Add(numGoroutines * 2)
	counterCtx := atomic.Int32{}
	counterNoCtx := atomic.Int32{}

	// try to lock concurrently, should all wait
	for i := 0; i < numGoroutines; i++ {
		go func() {
			done := s.TryLockWithContext("t1", context.Background())
			if !done {
				counterCtx.Add(1)
			} else {
				counterCtx.Add(-1)
				s.Unlock("t1")
			}
			wg.Done()
		}()

		go func() {
			s.Lock("t1")
			counterNoCtx.Add(1)
			defer s.Unlock("t1")
			wg.Done()
		}()

	}
	s.Unlock("t1") // unlock so that one of the goroutines can acquire the lock

	wg.Wait() // wait for all goroutines to be done
	require.Equal(t, int32(-numGoroutines), counterCtx.Load())
	require.Equal(t, int32(numGoroutines), counterNoCtx.Load())
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
