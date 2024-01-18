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

package vector

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShardedLocks_ParallelLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel LockAll does not fall into deadlock
	count := 10
	sl := NewDefaultShardedLocks()

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			sl.LockAll()
			sl.UnlockAll()
		}()
	}
	wg.Wait()
}

func TestShardedLocks_ParallelRLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel RLockAll does not fall into deadlock
	count := 10
	sl := NewDefaultShardedLocks()

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			sl.RLockAll()
			sl.RUnlockAll()
		}()
	}
	wg.Wait()
}

func TestShardedLocks_ParallelLocksAllAndRLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel LockAll + RLockAll does not fall into deadlock
	count := 50
	sl := NewDefaultShardedLocks()

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				sl.LockAll()
				sl.UnlockAll()
			} else {
				sl.RLockAll()
				sl.RUnlockAll()
			}
		}(i)
	}
	wg.Wait()
}

func TestShardedLocks_MixedLocks(t *testing.T) {
	// no asserts
	// ensures parallel LockAll + RLockAll + Lock + RLock does not fall into deadlock
	count := 1000
	sl := NewShardedLocks(10)

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			id := uint64(i)
			if i%5 == 0 {
				if i%2 == 0 {
					sl.LockAll()
					sl.UnlockAll()
				} else {
					sl.RLockAll()
					sl.RUnlockAll()
				}
			} else {
				if i%2 == 0 {
					sl.Lock(id)
					sl.Unlock(id)
				} else {
					sl.RLock(id)
					sl.RUnlock(id)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestShardedLocks(t *testing.T) {
	t.Run("RLock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.RLock(1)
		m.RLock(1)

		m.RUnlock(1)
		m.RUnlock(1)
	})

	t.Run("Lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.Lock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(time.Millisecond)
			m.Unlock(1)

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("RLock blocks Lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.RLock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.RUnlock(1)

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("Lock blocks RLock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.Lock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(100 * time.Millisecond)
			m.Unlock(1)

			close(ch)
		}()

		m.RLock(1)

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.RUnlock(1)
	})

	t.Run("Lock blocks LockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.Lock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.Unlock(1)

			close(ch)
		}()

		m.LockAll()

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.UnlockAll()
	})

	t.Run("LockAll blocks Lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.LockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.UnlockAll()

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("LockAll blocks RLock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.LockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.UnlockAll()

			close(ch)
		}()

		m.RLock(1)

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.RUnlock(1)
	})

	t.Run("LockAll blocks LockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.LockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.UnlockAll()

			close(ch)
		}()

		m.LockAll()

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.UnlockAll()
	})

	t.Run("UnlockAll releases all locks", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.LockAll()
		m.UnlockAll()

		m.Lock(1)
		m.Unlock(1)

		m.RLock(1)
		m.RUnlock(1)
	})

	t.Run("RLockAll blocks Lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.RLockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.RUnlockAll()

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("RLockAll doesn't block/unblock RLock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.RLockAll()
		m.RLock(1)

		m.RUnlockAll()
		m.RUnlock(1)
	})

	t.Run("RLockAll blocks LockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.RLockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.RUnlockAll()

			close(ch)
		}()

		m.LockAll()

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.UnlockAll()
	})

	t.Run("RLockAll doesn't block RLockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.RLockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.RUnlockAll()

			close(ch)
		}()

		m.RLockAll()

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		m.RUnlockAll()
	})

	t.Run("unlock should wake up next waiting lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(2)

		m.RLock(1)

		ch1 := make(chan struct{})
		ch2 := make(chan struct{})

		go func() {
			defer close(ch1)

			m.Lock(1)
		}()

		go func() {
			defer close(ch2)

			time.Sleep(100 * time.Millisecond)
			m.Lock(1)
		}()

		time.Sleep(10 * time.Millisecond)
		m.RUnlock(1)

		<-ch1

		m.Unlock(1)

		<-ch2
	})
}
