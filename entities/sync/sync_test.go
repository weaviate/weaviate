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
	"fmt"
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

func rwMutexLocked(m *sync.RWMutex) bool {
	rlocked := m.TryRLock()
	if rlocked {
		defer m.RUnlock()
	}
	return !rlocked
}

func rwMutexRLocked(m *sync.RWMutex) bool {
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

// func TestKeyLocker(t *testing.T) {
// 	t.Run("basic locking", func(t *testing.T) {
// 		locker := NewKeyLocker()

// 		// Lock should work
// 		locker.Lock("test")
// 		locker.Unlock("test")

// 		// Multiple locks should work
// 		locker.Lock("test")
// 		locker.Lock("test2")
// 		locker.Unlock("test")
// 		locker.Unlock("test2")
// 	})

// 	t.Run("concurrent access", func(t *testing.T) {
// 		locker := NewKeyLocker()

// 		var counter int32
// 		var wg sync.WaitGroup

// 		for i := 0; i < 100; i++ {
// 			wg.Add(1)
// 			go func() {
// 				defer wg.Done()
// 				locker.Lock("test")
// 				atomic.AddInt32(&counter, 1)
// 				locker.Unlock("test")
// 			}()
// 		}

// 		wg.Wait()
// 		assert.Equal(t, int32(100), counter)
// 	})

// 	t.Run("panic on invalid unlock", func(t *testing.T) {
// 		locker := NewKeyLocker()

// 		assert.Panics(t, func() {
// 			locker.Unlock("nonexistent")
// 		})
// 	})
// }

// func TestKeyRWLocker(t *testing.T) {
// 	t.Run("basic locking", func(t *testing.T) {
// 		locker := NewKeyRWLocker()

// 		// Write lock should work
// 		locker.Lock("test")
// 		locker.Unlock("test")

// 		// Read lock should work
// 		locker.RLock("test")
// 		locker.RUnlock("test")

// 		// Multiple read locks should work concurrently
// 		locker.RLock("test")
// 		locker.RLock("test")
// 		locker.RUnlock("test")
// 		locker.RUnlock("test")
// 	})

// 	t.Run("concurrent read access", func(t *testing.T) {
// 		locker := NewKeyRWLocker()

// 		var counter int32
// 		var wg sync.WaitGroup

// 		for i := 0; i < 100; i++ {
// 			wg.Add(1)
// 			go func() {
// 				defer wg.Done()
// 				locker.RLock("test")
// 				atomic.AddInt32(&counter, 1)
// 				locker.RUnlock("test")
// 			}()
// 		}

// 		wg.Wait()
// 		assert.Equal(t, int32(100), counter)
// 	})

// 	t.Run("panic on invalid unlock", func(t *testing.T) {
// 		locker := NewKeyRWLocker()

// 		assert.Panics(t, func() {
// 			locker.Unlock("nonexistent")
// 		})
// 		assert.Panics(t, func() {
// 			locker.RUnlock("nonexistent")
// 		})
// 	})
// }

func TestKeyLockerLockUnlock(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker()

	s.Lock("t1")
	v, _ := s.m.Load("t1")
	info := v.(*lockInfo)
	r.True(mutexLocked(info.mutex))

	s.Unlock("t1")
	v, ok := s.m.Load("t1")
	r.True(ok, "Lock should remain in the cache after unlock")
	info = v.(*lockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")

	s.Lock("t2")
	v, _ = s.m.Load("t2")
	info = v.(*lockInfo)
	r.True(mutexLocked(info.mutex))

	s.Unlock("t2")
	v, ok = s.m.Load("t2")
	r.True(ok, "Lock should remain in the cache after unlock")
	info = v.(*lockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")
}

func TestKeyRWLockerLockUnlock(t *testing.T) {
	r := require.New(t)
	s := NewKeyRWLocker()

	s.Lock("t1")
	v, _ := s.m.Load("t1")
	info := v.(*rwLockInfo)
	r.True(rwMutexLocked(info.mutex))
	r.False(rwMutexRLocked(info.mutex))

	s.Unlock("t1")
	v, ok := s.m.Load("t1")
	r.True(ok, "Lock should remain in the cache after unlock")
	info = v.(*rwLockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")

	s.Lock("t2")
	v, _ = s.m.Load("t2")
	info = v.(*rwLockInfo)
	r.True(rwMutexLocked(info.mutex))
	r.False(rwMutexRLocked(info.mutex))

	s.Unlock("t2")
	v, ok = s.m.Load("t2")
	r.True(ok, "Lock should remain in the cache after unlock")
	info = v.(*rwLockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")

	s.RLock("t1")
	v, _ = s.m.Load("t1")
	info = v.(*rwLockInfo)
	r.False(rwMutexLocked(info.mutex))
	r.True(rwMutexRLocked(info.mutex))

	s.RUnlock("t1")
	v, ok = s.m.Load("t1")
	r.True(ok, "Lock should remain in the cache after unlock")
	info = v.(*rwLockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")

	s.RLock("t2")
	v, _ = s.m.Load("t2")
	info = v.(*rwLockInfo)
	r.False(rwMutexLocked(info.mutex))
	r.True(rwMutexRLocked(info.mutex))

	s.RUnlock("t2")
	v, ok = s.m.Load("t2")
	r.True(ok, "Lock should remain in the cache after unlock")
	info = v.(*rwLockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")
}

func TestKeyLockerRefCounting(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker()

	// Test that lock is kept in map while in use
	s.Lock("ref1")
	v, ok := s.m.Load("ref1")
	r.True(ok)
	info := v.(*lockInfo)
	r.Equal(int32(1), atomic.LoadInt32(&info.refs))
	s.Unlock("ref1")

	v, ok = s.m.Load("ref1")
	r.True(ok, "Lock should remain in the cache after unlock")
	info = v.(*lockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")

	// Test ref counting increases before lock acquisition
	var wg sync.WaitGroup
	wg.Add(3) // one locker, two waiters

	var holdLock sync.WaitGroup
	holdLock.Add(1)

	// First goroutine - holds the lock
	go func() {
		defer wg.Done()
		s.Lock("ref1")
		v, _ := s.m.Load("ref1")
		info := v.(*lockInfo)
		r.Equal(int32(1), atomic.LoadInt32(&info.refs))

		holdLock.Done()
		time.Sleep(time.Millisecond * 50)

		v, _ = s.m.Load("ref1")
		info = v.(*lockInfo)
		refs := atomic.LoadInt32(&info.refs)
		r.Equal(int32(3), refs, "Refs should be 3 (1 holder + 2 waiters)")
		s.Unlock("ref1")
	}()

	// Two goroutines that will wait for the lock
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			holdLock.Wait()
			s.Lock("ref1")
			s.Unlock("ref1")
		}()
	}

	wg.Wait()

	v, ok = s.m.Load("ref1")
	r.True(ok, "Lock should remain in the cache after all operations")
	info = v.(*lockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")
}

func TestKeyRWLockerRefCounting(t *testing.T) {
	r := require.New(t)
	s := NewKeyRWLocker()

	// Test single write lock ref counting
	s.Lock("ref1")
	v, ok := s.m.Load("ref1")
	r.True(ok)
	info := v.(*rwLockInfo)
	r.Equal(int32(1), atomic.LoadInt32(&info.refs))
	s.Unlock("ref1")

	v, ok = s.m.Load("ref1")
	r.True(ok, "Lock should remain in the cache after unlock")
	info = v.(*rwLockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")

	// Test single read lock ref counting
	s.RLock("ref2")
	v, ok = s.m.Load("ref2")
	r.True(ok)
	info = v.(*rwLockInfo)
	r.Equal(int32(1), atomic.LoadInt32(&info.refs))
	s.RUnlock("ref2")

	v, ok = s.m.Load("ref2")
	r.True(ok, "Lock should remain in the cache after unlock")
	info = v.(*rwLockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")

	// Test concurrent read locks
	var wg sync.WaitGroup
	const numReaders = 3
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			s.RLock("ref3")
			v, ok := s.m.Load("ref3")
			r.True(ok)
			info := v.(*rwLockInfo)
			refs := atomic.LoadInt32(&info.refs)
			r.True(refs > 0 && refs <= numReaders, "Refs should be between 1 and numReaders")
			s.RUnlock("ref3")
		}()
	}

	wg.Wait()

	v, ok = s.m.Load("ref3")
	r.True(ok, "Lock should remain in the cache after all operations")
	info = v.(*rwLockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")
}

func TestKeyLockerConcurrentAccess(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker()

	const numGoroutines = 10
	const iterations = 1000

	counter := int32(0)
	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)

	var start sync.WaitGroup
	start.Add(1)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			start.Wait()

			for j := 0; j < iterations; j++ {
				s.Lock("shared")
				current := atomic.LoadInt32(&counter)
				time.Sleep(time.Microsecond)
				atomic.StoreInt32(&counter, current+1)
				s.Unlock("shared")
			}
		}()
	}

	start.Done()
	wg.Wait()

	expected := int32(numGoroutines * iterations)
	actual := atomic.LoadInt32(&counter)
	r.Equal(expected, actual, "Expected counter to be %d but got %d", expected, actual)

	v, ok := s.m.Load("shared")
	r.True(ok, "Lock should remain in the cache after all operations")
	info := v.(*lockInfo)
	r.Equal(int32(0), atomic.LoadInt32(&info.refs), "Reference count should be 0")
}

// TestKeyLockerExpiry tests the time-based expiry functionality of KeyLocker
func TestKeyLockerExpiry(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker()

	// Create and release some locks
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		s.Lock(key)
		s.Unlock(key)
	}

	// Verify locks are in the cache
	var count int
	s.m.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	r.Equal(5, count, "All locks should be in the cache initially")

	// Manually set the lastAccess time to be in the past
	s.m.Range(func(key, value interface{}) bool {
		info := value.(*lockInfo)
		// Set last access to 11 minutes ago
		atomic.StoreInt64(&info.lastAccess, time.Now().Add(-11*time.Minute).UnixNano())
		return true
	})

	// Create an active lock that should not be cleaned up
	s.Lock("active")
	v, ok := s.m.Load("active")
	r.True(ok, "Lock should be in the cache")
	info := v.(*lockInfo)
	// Set last access to 11 minutes ago
	atomic.StoreInt64(&info.lastAccess, time.Now().Add(-11*time.Minute).UnixNano())

	// Trigger cleanup directly
	s.cleanup(time.Now().Add(-10 * time.Minute).UnixNano())

	// Verify that expired locks are removed and active lock remains
	count = 0
	s.m.Range(func(key, value interface{}) bool {
		count++
		r.Equal("active", key, "Only active lock should remain")
		return true
	})
	r.Equal(1, count, "Only active lock should remain in the cache")

	// Release the active lock
	s.Unlock("active")

	// Set its last access time to the past again
	v, ok = s.m.Load("active")
	r.True(ok)
	info = v.(*lockInfo)
	atomic.StoreInt64(&info.lastAccess, time.Now().Add(-11*time.Minute).UnixNano())

	// Trigger cleanup again
	s.cleanup(time.Now().Add(-10 * time.Minute).UnixNano())

	// Verify all locks are now removed
	count = 0
	s.m.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	r.Equal(0, count, "All locks should be removed after cleanup")
}

// TestKeyRWLockerExpiry tests the time-based expiry functionality of KeyRWLocker
func TestKeyRWLockerExpiry(t *testing.T) {
	r := require.New(t)
	s := NewKeyRWLocker()

	// Create and release some locks
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		if i%2 == 0 {
			s.Lock(key)
			s.Unlock(key)
		} else {
			s.RLock(key)
			s.RUnlock(key)
		}
	}

	// Verify locks are in the cache
	var count int
	s.m.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	r.Equal(5, count, "All locks should be in the cache initially")

	// Manually set the lastAccess time to be in the past
	s.m.Range(func(key, value interface{}) bool {
		info := value.(*rwLockInfo)
		// Set last access to 11 minutes ago
		atomic.StoreInt64(&info.lastAccess, time.Now().Add(-11*time.Minute).UnixNano())
		return true
	})

	// Create active locks that should not be cleaned up
	s.Lock("active-write")
	s.RLock("active-read")

	// Set their last access times to the past
	v, ok := s.m.Load("active-write")
	r.True(ok)
	infoWrite := v.(*rwLockInfo)
	atomic.StoreInt64(&infoWrite.lastAccess, time.Now().Add(-11*time.Minute).UnixNano())

	v, ok = s.m.Load("active-read")
	r.True(ok)
	infoRead := v.(*rwLockInfo)
	atomic.StoreInt64(&infoRead.lastAccess, time.Now().Add(-11*time.Minute).UnixNano())

	// Trigger cleanup directly
	s.cleanup(time.Now().Add(-10 * time.Minute).UnixNano())

	// Verify that expired locks are removed and active locks remain
	count = 0
	activeKeys := make(map[string]bool)
	s.m.Range(func(key, _ interface{}) bool {
		count++
		activeKeys[key.(string)] = true
		return true
	})
	r.Equal(2, count, "Only active locks should remain in the cache")
	r.True(activeKeys["active-write"], "Write lock should still be present")
	r.True(activeKeys["active-read"], "Read lock should still be present")

	// Release the active locks
	s.Unlock("active-write")
	s.RUnlock("active-read")

	// Set their last access times to the past again
	v, ok = s.m.Load("active-write")
	r.True(ok)
	infoWrite = v.(*rwLockInfo)
	atomic.StoreInt64(&infoWrite.lastAccess, time.Now().Add(-11*time.Minute).UnixNano())

	v, ok = s.m.Load("active-read")
	r.True(ok)
	infoRead = v.(*rwLockInfo)
	atomic.StoreInt64(&infoRead.lastAccess, time.Now().Add(-11*time.Minute).UnixNano())

	// Trigger cleanup again
	s.cleanup(time.Now().Add(-10 * time.Minute).UnixNano())

	// Verify all locks are now removed
	count = 0
	s.m.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	r.Equal(0, count, "All locks should be removed after cleanup")
}
