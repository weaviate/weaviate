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

func TestContextMutex(t *testing.T) {
	m := newContextMutex()
	require.False(t, contextMutexLocked(m))
	m.Lock()
	require.True(t, contextMutexLocked(m))
	m.Unlock()
	require.False(t, contextMutexLocked(m))
	m.TryLockWithContext(context.Background())
	require.True(t, contextMutexLocked(m))
	m.Unlock()
	require.False(t, contextMutexLocked(m))
}

// verify that the critical sections are not accessed concurrently
// by ensuring a counter matches an atomic counter and that
// we don't see a "concurrent map writes" error inside critical sections
func TestContextMutexCriticalSection(t *testing.T) {
	m := newContextMutex()
	raceDetector := map[int]int{}
	atomicCounter := atomic.Int64{}
	var counter int64 = 0
	numWorkers := 100
	numIterations := 100000
	numIterationsPerWorker := numIterations / numWorkers
	wg := sync.WaitGroup{}
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		i := i
		go func(workerNum int) {
			defer wg.Done()
			workerContext, workerCancel := context.WithCancel(t.Context())
			for j := 0; j < numIterations; j++ {
				if j == workerNum*numIterationsPerWorker {
					timer := time.NewTimer(time.Millisecond)
					<-timer.C
					workerCancel()
					return
				}
				m.Lock()
				counter++
				raceDetector[workerNum]++
				m.Unlock()
				atomicCounter.Add(1)

				if m.TryLockWithContext(workerContext) {
					counter++
					raceDetector[workerNum]++
					m.Unlock()
					atomicCounter.Add(1)
				}

				if m.TryLock() {
					counter++
					raceDetector[workerNum]++
					m.Unlock()
					atomicCounter.Add(1)
				}
			}
			workerCancel()
		}(i)
	}
	wg.Wait()
	require.Equal(t, atomicCounter.Load(), counter)
}

// TestContextMutexConcurrentAccess tests concurrent access to the same mutex
func TestContextMutexConcurrentAccess(t *testing.T) {
	m := newContextMutex()
	counter := 0
	numGoroutines := 100
	numIterations := 1000

	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				m.Lock()
				counter++
				m.Unlock()
			}
		}()
	}

	wg.Wait()
	require.Equal(t, numGoroutines*numIterations, counter)
}

// TestContextMutexTryLockWithContextTimeout tests timeout behavior
func TestContextMutexTryLockWithContextTimeout(t *testing.T) {
	m := newContextMutex()

	// Lock the mutex
	m.Lock()

	// Try to lock with a short timeout
	ctx, cancel := context.WithTimeout(t.Context(), 51*time.Millisecond)
	defer cancel()

	start := time.Now()
	success := m.TryLockWithContext(ctx)
	duration := time.Since(start)

	require.False(t, success)
	require.True(t, duration >= 50*time.Millisecond)
	require.True(t, duration < 100*time.Millisecond)

	m.Unlock()
}

// TestContextMutexTryLockWithContextAlreadyCanceled tests already canceled context behavior
func TestContextMutexTryLockWithContextAlreadyCanceled(t *testing.T) {
	m := newContextMutex()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	success := m.TryLockWithContext(ctx)
	require.False(t, success)
}

// TestContextMutexTryLock tests the TryLock method
func TestContextMutexTryLock(t *testing.T) {
	m := newContextMutex()

	// Should succeed when not locked
	success := m.TryLock()
	require.True(t, success)

	// Should fail when already locked
	success = m.TryLock()
	require.False(t, success)

	m.Unlock()

	// Should succeed again after unlock
	success = m.TryLock()
	require.True(t, success)
	m.Unlock()
}

// TestContextMutexMixedOperations tests mixing different lock types
func TestContextMutexMixedOperations(t *testing.T) {
	m := newContextMutex()

	// Mix Lock, TryLock, and TryLockWithContext
	m.Lock()
	require.True(t, contextMutexLocked(m))
	m.Unlock()

	success := m.TryLock()
	require.True(t, success)
	m.Unlock()

	ctx := context.Background()
	success = m.TryLockWithContext(ctx)
	require.True(t, success)
	m.Unlock()

	// Test when already locked
	m.Lock()
	success = m.TryLock()
	require.False(t, success)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	success = m.TryLockWithContext(ctx)
	require.False(t, success)

	m.Unlock()
}
