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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func mutexLocked(m *sync.Mutex) bool {
	rlocked := m.TryLock()
	if rlocked {
		defer m.Unlock()
	}
	return !rlocked
}

func TestKeyLockerLockUnlock(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())

	s.Lock("t1")
	value, ok := s.locks.Load("t1")
	r.True(ok)
	entry := value.(*lockEntry)
	r.True(mutexLocked(&entry.lock))

	s.Unlock("t1")
	value, ok = s.locks.Load("t1")
	r.True(ok)
	entry = value.(*lockEntry)
	r.False(mutexLocked(&entry.lock))

	s.Lock("t2")
	value, ok = s.locks.Load("t2")
	r.True(ok)
	entry = value.(*lockEntry)
	r.True(mutexLocked(&entry.lock))

	s.Unlock("t2")
	value, ok = s.locks.Load("t2")
	r.True(ok)
	entry = value.(*lockEntry)
	r.False(mutexLocked(&entry.lock))
}

func TestKeyLockerSizeLimit(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())
	s.maxSize = 2 // Set small size for testing

	// Add items up to size limit
	s.Lock("k1")
	s.Unlock("k1")
	s.Lock("k2")
	s.Unlock("k2")

	r.Equal(int32(2), s.count.Load())

	// Add one more to trigger cleanup
	s.Lock("k3")
	s.Unlock("k3")

	// Count should still be within limit
	r.LessOrEqual(s.count.Load(), s.maxSize)
}

func TestKeyLockerCleanup(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())
	s.maxAge = time.Millisecond * 100 // Set short maxAge for testing

	// Add some locks
	s.Lock("k1")
	s.Unlock("k1")
	s.Lock("k2")
	s.Unlock("k2")

	initialCount := s.count.Load()
	r.Equal(int32(2), initialCount)

	// Wait for locks to expire
	time.Sleep(time.Millisecond * 200)

	// Run cleanup
	s.Cleanup()

	// Count should be 0 after cleanup
	r.Equal(int32(0), s.count.Load())

	// Verify locks are removed
	_, ok := s.locks.Load("k1")
	r.False(ok)
	_, ok = s.locks.Load("k2")
	r.False(ok)
}

func TestKeyLockerLastUsedUpdate(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())
	s.maxAge = time.Millisecond * 200 // Set short maxAge for testing

	// Add a lock
	s.Lock("k1")
	s.Unlock("k1")

	// Get initial last used time
	value, ok := s.locks.Load("k1")
	r.True(ok)
	entry := value.(*lockEntry)
	initialLastUsed := entry.lastUsed.Load()

	// Wait a bit, then use the lock again
	time.Sleep(time.Millisecond * 100)
	s.Lock("k1")
	s.Unlock("k1")

	// Verify last used time was updated
	value, ok = s.locks.Load("k1")
	r.True(ok)
	entry = value.(*lockEntry)
	r.Greater(entry.lastUsed.Load(), initialLastUsed)

	// Wait for lock to expire
	time.Sleep(time.Millisecond * 300)
	s.Cleanup()

	// Verify lock was removed
	_, ok = s.locks.Load("k1")
	r.False(ok)
}

func TestKeyLockerConcurrent(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())
	numGoroutines := 10
	numIterations := 100

	done := make(chan bool)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < numIterations; j++ {
				key := "key"
				s.Lock(key)
				// Verify the lock is held
				value, ok := s.locks.Load(key)
				r.True(ok)
				entry := value.(*lockEntry)
				r.True(mutexLocked(&entry.lock))
				s.Unlock(key)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify final state
	r.Equal(int32(1), s.count.Load()) // Only one key was used
}

func TestKeyLockerMaxSizeAndEviction(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())
	s.maxSize = 3

	// Fill up to max size
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key%d", i)
		s.Lock(key)
		s.Unlock(key)
	}
	r.Equal(int32(3), s.count.Load())

	// Sleep briefly to ensure different timestamps
	time.Sleep(time.Millisecond)

	// Lock one key again to make it most recently used
	s.Lock("key1")
	s.Unlock("key1")

	// Add new key, should evict oldest
	s.Lock("key3")
	s.Unlock("key3")

	// Should still be at max size
	r.Equal(int32(3), s.count.Load())

	// key0 should be evicted (oldest), key1 should still exist (recently used)
	_, ok := s.locks.Load("key0")
	r.False(ok, "oldest key should be evicted")
	_, ok = s.locks.Load("key1")
	r.True(ok, "recently used key should still exist")
}

func TestKeyLockerLockedEntryNotEvicted(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())
	s.maxSize = 2

	s.Lock("key1")

	s.Lock("key2")
	s.Unlock("key2")

	s.Lock("key3")
	s.Unlock("key3")

	// Should still have key1 (locked) and one of key2/key3
	_, ok := s.locks.Load("key1")
	r.True(ok, "locked key should not be evicted")

	s.Unlock("key1")
}

func TestKeyLockerMaxAgeExpiration(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())
	s.maxAge = 100 * time.Millisecond

	// Add some keys
	s.Lock("key1")
	s.Unlock("key1")
	s.Lock("key2")
	s.Unlock("key2")

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	s.Cleanup()

	// All keys should be removed
	r.Equal(int32(0), s.count.Load())
	_, ok := s.locks.Load("key1")
	r.False(ok)
	_, ok = s.locks.Load("key2")
	r.False(ok)
}

func TestKeyLockerConcurrentLockUnlock(t *testing.T) {
	s := NewKeyLocker(logrus.New())
	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := make(chan struct{})
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			<-start // Wait for start signal

			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key%d", j%5) // Use 5 different keys
				s.Lock(key)
				// Small sleep to increase contention
				time.Sleep(time.Microsecond)
				s.Unlock(key)
			}
		}(i)
	}

	close(start)
	wg.Wait() // Wait for all goroutines to finish
}

func TestKeyLockerPanicOnUnlockNonexistent(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())

	r.Panics(func() {
		s.Unlock("nonexistent")
	}, "Unlock on nonexistent key should panic")
}

func TestKeyLockerCleanupLockedEntries(t *testing.T) {
	r := require.New(t)
	s := NewKeyLocker(logrus.New())
	s.maxAge = 100 * time.Millisecond

	// Lock a key and keep it locked
	s.Lock("key1")

	// Wait for expiration time
	time.Sleep(200 * time.Millisecond)

	// Run cleanup
	s.Cleanup()

	// Key should still exist because it's locked
	_, ok := s.locks.Load("key1")
	r.True(ok, "locked key should not be cleaned up even if expired")

	// Cleanup
	s.Unlock("key1")
}
