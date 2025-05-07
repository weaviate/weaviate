package ctxlock

import (
	"sync"
	"testing"
	"time"
)

func TestMeteredRWMutex_BasicLockUnlock(t *testing.T) {
	m := NewMeteredRWMutex("test-metered")

	m.Lock()
	m.Unlock()

	m.RLock()
	m.RUnlock()
}

func TestMeteredRWMutex_TryLock_Succeeds(t *testing.T) {
	m := NewMeteredRWMutex("test-metered")

	ok := m.TryLock()
	if !ok {
		t.Fatal("expected TryLock to succeed")
	}
	m.Unlock()
}

func TestMeteredRWMutex_TryLock_FailsWhenBusy(t *testing.T) {
	m := NewMeteredRWMutex("test-metered")

	m.Lock()
	defer m.Unlock()

	ok := m.TryLock()
	if ok {
		t.Fatal("expected TryLock to fail while locked")
	}
}

func TestMeteredRWMutex_TryRLock_Succeeds(t *testing.T) {
	m := NewMeteredRWMutex("test-metered")

	ok := m.TryRLock()
	if !ok {
		t.Fatal("expected TryRLock to succeed")
	}
	m.RUnlock()
}

func TestMeteredRWMutex_TryRLock_FailsWhenWriteLocked(t *testing.T) {
	m := NewMeteredRWMutex("test-metered")

	m.Lock()
	defer m.Unlock()

	ok := m.TryRLock()
	if ok {
		t.Fatal("expected TryRLock to fail when write locked")
	}
}

func TestMeteredRWMutex_ParallelReaders(t *testing.T) {
	m := NewMeteredRWMutex("test-metered")

	const numReaders = 5
	var wg sync.WaitGroup

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.RLock()
			time.Sleep(20 * time.Millisecond)
			m.RUnlock()
		}()
	}

	wg.Wait()
}

func TestMeteredRWMutex_WriterBlocksReaders(t *testing.T) {
	m := NewMeteredRWMutex("test-metered")

	m.Lock()

	start := time.Now()
	ok := m.TryRLock()
	if ok {
		t.Fatal("expected TryRLock to fail while write locked")
	}
	elapsed := time.Since(start)
	if elapsed > 50*time.Millisecond {
		t.Errorf("TryRLock took too long to fail: %v", elapsed)
	}

	m.Unlock()
}

func TestMeteredRWMutex_RTLockAlias(t *testing.T) {
	m := NewMeteredRWMutex("test-metered")

	ok := m.RTryLock()
	if !ok {
		t.Fatal("expected RTryLock to succeed")
	}
	m.RUnlock()
}