package lockmanager

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func lockCount(m *LockManager) int {
	var count int
	for i := range m.locks {
		m.locks[i].m.Range(func(key uint64, lock *atomic.Int32) bool {
			count++
			return true
		})
	}

	return count
}

func TestLockManager(t *testing.T) {
	t.Run("RLock", func(t *testing.T) {
		t.Parallel()
		m := New()

		m.RLock(1)
		m.RLock(1)

		require.Equal(t, 1, lockCount(m))

		m.RUnlock(1)
		m.RUnlock(1)
	})

	t.Run("Lock", func(t *testing.T) {
		t.Parallel()
		m := New()

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
		m := New()

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
		m := New()

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
		m := New()

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
		m := New()

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
		m := New()

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
		m := New()

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
		m := New()

		m.LockAll()
		m.UnlockAll()

		m.Lock(1)
		m.Unlock(1)

		m.RLock(1)
		m.RUnlock(1)
	})

	t.Run("RLockAll blocks Lock", func(t *testing.T) {
		t.Parallel()
		m := New()

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
		m := New()

		m.RLockAll()
		m.RLock(1)

		m.RUnlockAll()
		m.RUnlock(1)
	})

	t.Run("RLockAll blocks LockAll", func(t *testing.T) {
		t.Parallel()
		m := New()

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
		m := New()

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

	t.Run("unknown lock", func(t *testing.T) {
		t.Parallel()
		m := New()

		require.Panics(t, func() {
			m.Unlock(1)
		})
	})

	t.Run("unlock should wake up next waiting lock", func(t *testing.T) {
		t.Parallel()
		m := NewWith(1)

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

	t.Run("Vaccum", func(t *testing.T) {
		t.Parallel()
		m := NewWith(2)

		m.RLock(0)
		m.Lock(1)
		m.Lock(2)
		require.Equal(t, 3, lockCount(m))

		m.RUnlock(0)
		m.Unlock(1)

		require.Equal(t, 3, lockCount(m))

		ch := make(chan struct{})
		go func() {
			defer close(ch)

			time.Sleep(100 * time.Millisecond)
			m.Unlock(2)
		}()

		m.Vaccum()

		select {
		case <-ch:
		default:
			require.Fail(t, "should be unlocked")
		}

		require.Equal(t, 0, lockCount(m))
	})
}

func TestShardLocks_ParallelLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel LockAll does not fall into deadlock
	count := 10
	sl := New()

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

func TestShardLocks_ParallelRLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel RLockAll does not fall into deadlock
	count := 10
	sl := New()

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

func TestShardLocks_ParallelLocksAllAndRLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel LockAll + RLockAll does not fall into deadlock
	count := 50
	sl := New()

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

func TestShardLocks_MixedLocks(t *testing.T) {
	// no asserts
	// ensures parallel LockAll + RLockAll + Lock + RLock does not fall into deadlock
	count := 1000
	sl := New()

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

func BenchmarkLock(b *testing.B) {
	m := NewWith(1)

	var wg sync.WaitGroup
	wg.Add(10)
	ch := make(chan uint64)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			for id := range ch {
				m.Lock(id)
				m.Unlock(id)
			}
		}()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			ch <- uint64(j) + 1
		}
	}
	close(ch)
	wg.Wait()
}

func BenchmarkSpeedOfLight(b *testing.B) {
	ms := make([]sync.Mutex, 10000)

	var wg sync.WaitGroup
	wg.Add(10)
	ch := make(chan uint64)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			for id := range ch {
				ms[id].Lock()
				ms[id].Unlock()
			}
		}()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			ch <- uint64(j)
		}
	}
	close(ch)
	wg.Wait()
}

func BenchmarkRLock(b *testing.B) {
	m := NewWith(10)

	var wg sync.WaitGroup
	wg.Add(10)
	ch := make(chan uint64)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			for id := range ch {
				m.RLock(id)
				m.RUnlock(id)

				if id%100 == 0 {
					m.Vaccum()
				}
			}
		}()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			ch <- uint64(j) + 1
		}
	}
	close(ch)
	wg.Wait()
}

func BenchmarkRSpeedOfLight(b *testing.B) {
	ms := make([]sync.RWMutex, 10000)

	var wg sync.WaitGroup
	wg.Add(10)
	ch := make(chan uint64)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			for id := range ch {
				ms[id].RLock()
				ms[id].RUnlock()
			}
		}()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			ch <- uint64(j)
		}
	}
	close(ch)
	wg.Wait()
}
