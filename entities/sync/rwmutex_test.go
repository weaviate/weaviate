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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadPreferringRWMutex(t *testing.T) {
	t.Run("read locks", func(t *testing.T) {
		mutex := NewReadPreferringRWMutex()

		for range 3 {
			mutex.RLock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RLock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)
		}
	})

	t.Run("write locks", func(t *testing.T) {
		mutex := NewReadPreferringRWMutex()

		for range 3 {
			mutex.Lock()
			assertLocked(t, mutex)
			assertRUnlocked(t, mutex)

			mutex.Unlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)
		}
	})

	t.Run("try read locks", func(t *testing.T) {
		mutex := NewReadPreferringRWMutex()

		for range 3 {
			assert.True(t, mutex.TryRLock())
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			assert.True(t, mutex.TryRLock())
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)
		}

		for range 3 {
			assert.True(t, mutex.TryRLock())
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RLock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)
		}

		for range 3 {
			mutex.RLock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			assert.True(t, mutex.TryRLock())
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)
		}
	})

	t.Run("try write locks", func(t *testing.T) {
		mutex := NewReadPreferringRWMutex()

		for range 3 {
			assert.True(t, mutex.TryLock())
			assertLocked(t, mutex)
			assertRUnlocked(t, mutex)

			assert.False(t, mutex.TryLock())
			assertLocked(t, mutex)
			assertRUnlocked(t, mutex)

			mutex.Unlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)
		}

		for range 3 {
			mutex.Lock()
			assertLocked(t, mutex)
			assertRUnlocked(t, mutex)

			assert.False(t, mutex.TryLock())
			assertLocked(t, mutex)
			assertRUnlocked(t, mutex)

			mutex.Unlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)
		}
	})

	t.Run("mixed locks", func(t *testing.T) {
		mutex := NewReadPreferringRWMutex()

		for range 3 {
			// rlocked 1x
			assert.True(t, mutex.TryRLock())
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			// no lock
			assert.False(t, mutex.TryLock())
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			// rlocked 2x
			mutex.RLock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			// no lock, already rlocked
			assert.False(t, mutex.TryLock())
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			// rlocked 1x
			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			// no lock, already locked
			assert.False(t, mutex.TryLock())
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			// runlocked, unlocked
			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)

			// locked
			mutex.Lock()
			assertLocked(t, mutex)
			assertRUnlocked(t, mutex)

			// no lock, already locked
			assert.False(t, mutex.TryLock())
			assertLocked(t, mutex)
			assertRUnlocked(t, mutex)

			// no rlock, already locked
			assert.False(t, mutex.TryRLock())
			assertLocked(t, mutex)
			assertRUnlocked(t, mutex)

			// runlocked, unlocked
			mutex.Unlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)

			// rlocked
			assert.True(t, mutex.TryRLock())
			assertUnlocked(t, mutex)
			assertRLocked(t, mutex)

			// runlocked, unlocked
			mutex.RUnlock()
			assertUnlocked(t, mutex)
			assertRUnlocked(t, mutex)
		}
	})

	t.Run("parallel read locks", func(t *testing.T) {
		count := 5

		mutex := NewReadPreferringRWMutex()

		wg := new(sync.WaitGroup)
		wg.Add(count * 2)
		for range count {
			go func() {
				defer wg.Done()
				mutex.RLock()
			}()
		}
		for range count {
			go func() {
				defer wg.Done()
				assert.True(t, mutex.TryRLock())
			}()
		}

		// nothing to assert. all rlocks must have been acquired by then
		wg.Wait()
		for range count * 2 {
			mutex.RUnlock()
		}
		assertUnlocked(t, mutex)
		assertRUnlocked(t, mutex)
	})

	t.Run("parallel write locks", func(t *testing.T) {
		count := 5

		inUse := 0
		maxInUse := 0
		lock := new(sync.Mutex)
		execute := func(callback func()) {
			lock.Lock()
			inUse++
			maxInUse = max(maxInUse, inUse)
			lock.Unlock()

			callback()

			lock.Lock()
			inUse--
			lock.Unlock()
		}

		mutex := NewReadPreferringRWMutex()

		wg := new(sync.WaitGroup)
		wg.Add(count * 2)
		for range count {
			go func() {
				defer wg.Done()

				mutex.Lock()
				defer mutex.Unlock()
				execute(func() { time.Sleep(time.Millisecond) })
			}()
		}
		for range count {
			go func() {
				defer wg.Done()

				if mutex.TryLock() {
					defer mutex.Unlock()
					execute(func() { time.Sleep(time.Millisecond) })
				}
			}()
		}

		wg.Wait()
		assert.Equal(t, 0, inUse)
		assert.Equal(t, 1, maxInUse)
		assertUnlocked(t, mutex)
		assertRUnlocked(t, mutex)
	})

	t.Run("acquire read lock waiting for write lock", func(t *testing.T) {
		beforeLockCh := make(chan struct{}, 1)
		afterLockCh := make(chan struct{}, 1)

		mutex := NewReadPreferringRWMutex()
		mutex.RLock()

		go func() {
			beforeLockCh <- struct{}{}
			mutex.Lock()
			afterLockCh <- struct{}{}
		}()

		// wait for goroutine to start
		<-beforeLockCh

		assertUnlocked(t, mutex)
		assertRLocked(t, mutex)

		assert.True(t, mutex.TryRLock())
		assert.False(t, mutex.TryLock())

		assertUnlocked(t, mutex)
		assertRLocked(t, mutex)

		mutex.RUnlock()
		mutex.RUnlock()

		assertRUnlocked(t, mutex)

		<-afterLockCh
		assertLocked(t, mutex)
		assertRUnlocked(t, mutex)

		mutex.Unlock()
		assertUnlocked(t, mutex)
		assertRUnlocked(t, mutex)
	})

	t.Run("parallel acquire read lock waiting for write lock", func(t *testing.T) {
		count := 5

		var lastRUnlock time.Time
		var firstLock time.Time
		lock := new(sync.Mutex)

		mutex := NewReadPreferringRWMutex()
		mutex.RLock()

		wgStarted := new(sync.WaitGroup)
		wgStarted.Add(count * 2)
		wgExecuted := new(sync.WaitGroup)
		wgExecuted.Add(count * 2)
		for range count {
			go func() {
				wgStarted.Done()
				defer wgExecuted.Done()

				mutex.RLock()
				defer mutex.RUnlock()

				time.Sleep(time.Millisecond)

				lock.Lock()
				lastRUnlock = time.Now()
				lock.Unlock()
			}()
		}
		for range count {
			go func() {
				wgStarted.Done()
				defer wgExecuted.Done()

				mutex.Lock()
				defer mutex.Unlock()

				lock.Lock()
				if firstLock.IsZero() {
					firstLock = time.Now()
				}
				lock.Unlock()

				time.Sleep(time.Millisecond)
			}()
		}
		wgStarted.Wait()
		mutex.RUnlock()
		wgExecuted.Wait()

		assertUnlocked(t, mutex)
		assertRUnlocked(t, mutex)
		assert.False(t, lastRUnlock.After(firstLock))
	})
}

func assertLocked(t *testing.T, m *ReadPreferringRWMutex) {
	t.Helper()
	assert.True(t, rwMutexLocked(m.lock), "mutex should be locked")
}

func assertUnlocked(t *testing.T, m *ReadPreferringRWMutex) {
	t.Helper()
	assert.False(t, rwMutexLocked(m.lock), "mutex should be unlocked")
}

func assertRLocked(t *testing.T, m *ReadPreferringRWMutex) {
	t.Helper()
	assert.True(t, rwMutexRLocked(m.lock), "mutex should be rlocked")
}

func assertRUnlocked(t *testing.T, m *ReadPreferringRWMutex) {
	t.Helper()
	assert.False(t, rwMutexRLocked(m.lock), "mutex should be runlocked")
}
