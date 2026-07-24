//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"math/rand"
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
				sl.LockAll()
				sl.UnlockAll()
			} else {
				sl.Lock(id)
				sl.Unlock(id)
			}
		}(i)
	}
	wg.Wait()
}

func TestShardedLocks(t *testing.T) {
	t.Run("Lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.Lock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.Unlock(1)

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("Lock blocks LockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.Lock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.Unlock(1)

			close(ch)
		}()

		m.LockAll()

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
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
			time.Sleep(50 * time.Millisecond)
			m.UnlockAll()

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("LockAll blocks LockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(5)

		m.LockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.UnlockAll()

			close(ch)
		}()

		m.LockAll()

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
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
	})

	t.Run("unlock should wake up next waiting lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedLocks(2)

		m.Lock(1)

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
		m.Unlock(1)

		<-ch1

		m.Unlock(1)

		<-ch2

		m.Unlock(1)
	})
}

func TestShardedRWLocks_ParallelLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel LockAll does not fall into deadlock
	count := 10
	sl := NewDefaultShardedRWLocks()

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

func TestShardedRWLocks_ParallelRLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel RLockAll does not fall into deadlock
	count := 10
	sl := NewDefaultShardedRWLocks()

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

func TestShardedRWLocks_ParallelLocksAllAndRLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel LockAll + RLockAll does not fall into deadlock
	count := 50
	sl := NewDefaultShardedRWLocks()

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

func TestShardedRWLocks_MixedLocks(t *testing.T) {
	// no asserts
	// ensures parallel LockAll + RLockAll + Lock + RLock does not fall into deadlock
	count := 1000
	sl := NewShardedRWLocks(10)

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

func TestShardedRWLocks(t *testing.T) {
	t.Run("RLock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.RLock(1)
		m.RLock(1)

		m.RUnlock(1)
		m.RUnlock(1)
	})

	t.Run("Lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.Lock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.Unlock(1)

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("RLock blocks Lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.RLock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.RUnlock(1)

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("Lock blocks RLock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.Lock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.Unlock(1)

			close(ch)
		}()

		m.RLock(1)

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.RUnlock(1)
	})

	t.Run("Lock blocks LockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.Lock(1)

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.Unlock(1)

			close(ch)
		}()

		m.LockAll()

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.UnlockAll()
	})

	t.Run("LockAll blocks Lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.LockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.UnlockAll()

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("LockAll blocks RLock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.LockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.UnlockAll()

			close(ch)
		}()

		m.RLock(1)

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.RUnlock(1)
	})

	t.Run("LockAll blocks LockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.LockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.UnlockAll()

			close(ch)
		}()

		m.LockAll()

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.UnlockAll()
	})

	t.Run("UnlockAll releases all locks", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.LockAll()
		m.UnlockAll()

		m.Lock(1)
		m.Unlock(1)

		m.RLock(1)
		m.RUnlock(1)
	})

	t.Run("RLockAll blocks Lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.RLockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.RUnlockAll()

			close(ch)
		}()

		m.Lock(1)

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.Unlock(1)
	})

	t.Run("RLockAll doesn't block/unblock RLock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.RLockAll()
		m.RLock(1)

		m.RUnlockAll()
		m.RUnlock(1)
	})

	t.Run("RLockAll blocks LockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.RLockAll()

		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			m.RUnlockAll()

			close(ch)
		}()

		m.LockAll()

		select {
		case <-ch:
		case <-time.After(1 * time.Second):
			require.Fail(t, "should be unlocked")
		}

		m.UnlockAll()
	})

	t.Run("RLockAll doesn't block RLockAll", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(5)

		m.RLockAll()
		m.RLockAll()

		m.RUnlockAll()
		m.RUnlockAll()
	})

	t.Run("unlock should wake up next waiting lock", func(t *testing.T) {
		t.Parallel()
		m := NewShardedRWLocks(2)

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

		m.Unlock(1)
	})
}

type lockable interface {
	Lock(id uint64)
	Unlock(id uint64)
	LockAll()
	UnlockAll()
}

func BenchmarkLocksHighContention(b *testing.B) {
	const (
		numGoroutines = 4096 // increase goroutines to scale up contention
		numOps        = 1000 // keep ops per goroutine constant
		hotKeySpace   = 4096 // touch more locks (¼ of 32k)
		hotFraction   = 0.8  // 80% of keys are clustered
		hotRange      = 64   // 80% of keys in this tight cluster
	)

	keys := make([]uint64, numGoroutines)
	for i := range keys {
		if rand.Float64() < hotFraction {
			keys[i] = uint64(rand.Intn(hotRange)) // hot cluster
		} else {
			keys[i] = uint64(rand.Intn(hotKeySpace)) + 10000 // spread cold keys
		}
	}

	run := func(b *testing.B, l lockable) {
		b.Helper()

		results := make([]int, numGoroutines*10)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			for j := 0; j < numGoroutines; j++ {
				id := keys[j]
				go func() {
					for k := 0; k < numOps; k++ {
						l.Lock(id)
						// Simulate some real work under the lock
						x := 0
						for i := 0; i < 1000; i++ {
							x += i * int(id%10)
						}
						results[id] = x
						l.Unlock(id)
					}
					wg.Done()
				}()
			}

			wg.Wait()
		}
	}

	b.Run("ShardedLocks_512", func(b *testing.B) {
		run(b, NewShardedLocks(512))
	})

	b.Run("ShardedRWLocks_512", func(b *testing.B) {
		run(b, NewShardedRWLocks(512))
	})
}

func TestLazyShardedRWLocks_LazyAllocation(t *testing.T) {
	l := NewLazyShardedRWLocks(8, 4)

	require.EqualValues(t, 0, l.Count(), "no stripes must be allocated before first use")
	require.EqualValues(t, 4, l.PageSize)

	l.Lock(1)
	l.Unlock(1)
	require.EqualValues(t, 8, l.Count(), "first use must allocate the initial stripe count")

	l.RLock(2)
	l.RUnlock(2)
	l.LockAll()
	l.UnlockAll()
	require.EqualValues(t, 8, l.Count(), "regular use must not change the stripe count")
}

func TestLazyShardedRWLocks_EnsureCount(t *testing.T) {
	t.Run("on a fresh instance", func(t *testing.T) {
		l := NewLazyShardedRWLocks(8, 1)
		l.EnsureCount(512)
		require.EqualValues(t, 512, l.Count())
	})

	t.Run("grows but never shrinks", func(t *testing.T) {
		l := NewLazyShardedRWLocks(8, 1)
		l.Lock(1)
		l.Unlock(1)
		require.EqualValues(t, 8, l.Count())

		l.EnsureCount(64)
		require.EqualValues(t, 64, l.Count())

		l.EnsureCount(16)
		require.EqualValues(t, 64, l.Count(), "EnsureCount must never shrink")
	})

	t.Run("never allocates below the initial count", func(t *testing.T) {
		// the initial layout must not depend on whether EnsureCount or a
		// lock operation touches the instance first
		l := NewLazyShardedRWLocks(8, 1)
		l.EnsureCount(2)
		require.EqualValues(t, 8, l.Count())
	})
}

func TestLazyShardedRWLocks_MutualExclusionAcrossRestripe(t *testing.T) {
	// counters guarded by per-id locks while another goroutine keeps growing
	// the stripe count: no increment may be lost across the stripe swaps
	const (
		goroutines = 8
		increments = 2_000
		ids        = 16
	)

	l := NewLazyShardedRWLocks(2, 1)
	counters := make([]int, ids)

	wg := new(sync.WaitGroup)
	wg.Add(goroutines + 1)

	go func() {
		defer wg.Done()
		for c := uint64(4); c <= 512; c *= 2 {
			l.EnsureCount(c)
			time.Sleep(time.Millisecond)
		}
	}()

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				id := uint64((g + i) % ids)
				l.Lock(id)
				counters[id]++
				l.Unlock(id)
			}
		}(g)
	}
	wg.Wait()

	total := 0
	for _, c := range counters {
		total += c
	}
	require.Equal(t, goroutines*increments, total, "increments were lost across re-striping")
	require.EqualValues(t, 512, l.Count())
}

func TestLazyShardedRWLocks_MixedLocks(t *testing.T) {
	// no asserts
	// ensures parallel LockAll + Lock + RLock + EnsureCount does not deadlock
	count := 1000
	l := NewLazyShardedRWLocks(2, 1)

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			id := uint64(i)
			switch i % 7 {
			case 0:
				l.LockAll()
				l.UnlockAll()
			case 1:
				l.EnsureCount(uint64(i))
			default:
				if i%2 == 0 {
					l.Lock(id)
					l.Unlock(id)
				} else {
					l.RLock(id)
					l.RUnlock(id)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestLazyShardedRWLocks_LockBlocks(t *testing.T) {
	l := NewLazyShardedRWLocks(4, 1)

	l.Lock(1)

	ch := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		l.Unlock(1)

		close(ch)
	}()

	l.Lock(1)

	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		require.Fail(t, "should be unlocked")
	}

	l.Unlock(1)
}
