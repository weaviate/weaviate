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

package ctxlock

import (
	"sync"
	"testing"
	"time"
)

func newTestMaxRWMutex(limit int) *MaxRWMutex {
	m := &MaxRWMutex{
		location:   "test",
		maxWaiting: limit,
		waiting:    make(chan struct{}, limit),
	}
	return m
}

func TestMaxRWMutex_BasicLockUnlock(t *testing.T) {
	m := newTestMaxRWMutex(2)

	m.Lock()
	m.Unlock()

	m.RLock()
	m.RUnlock()
}

func TestMaxRWMutex_TryLock_Succeeds(t *testing.T) {
	m := newTestMaxRWMutex(1)

	ok := m.TryLock()
	if !ok {
		t.Fatal("expected TryLock to succeed")
	}
	m.Unlock()
}

func TestMaxRWMutex_TryLock_FailsWhenBusy(t *testing.T) {
	m := newTestMaxRWMutex(1)

	m.Lock()
	defer m.Unlock()

	ok := m.TryLock()
	if ok {
		t.Fatal("expected TryLock to fail while locked")
	}
}

func TestMaxRWMutex_TryRLock_Succeeds(t *testing.T) {
	m := newTestMaxRWMutex(1)

	ok := m.TryRLock()
	if !ok {
		t.Fatal("expected TryRLock to succeed")
	}
	m.RUnlock()
}

func TestMaxRWMutex_TryRLock_FailsWhenBusy(t *testing.T) {
	m := newTestMaxRWMutex(1)

	m.Lock() // block writers and readers
	defer m.Unlock()

	ok := m.TryRLock()
	if ok {
		t.Fatal("expected TryRLock to fail while write locked")
	}
}

func TestMaxRWMutex_MaxWaitingLimits(t *testing.T) {
	m := newTestMaxRWMutex(2) // only 2 allowed waiters

	var wg sync.WaitGroup

	// First goroutine: grab the lock
	m.Lock()

	// Two goroutines: waiters that should be allowed
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.RLock()
			time.Sleep(50 * time.Millisecond) // hold briefly
			m.RUnlock()
		}()
		time.Sleep(10 * time.Millisecond) // stagger slightly
	}

	time.Sleep(20 * time.Millisecond)

	// One more waiter should exceed the limit
	start := time.Now()
	ok := m.TryLock()
	if ok {
		t.Fatal("expected TryLock to fail due to max waiting limit exceeded")
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("TryLock took too long to fail: %v", elapsed)
	}

	m.Unlock()
	wg.Wait()
}

func TestMaxRWMutex_WriterBlocksReaders(t *testing.T) {
	m := newTestMaxRWMutex(2)

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

func TestMaxRWMutex_ParallelReaders(t *testing.T) {
	m := newTestMaxRWMutex(10)

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

func TestMaxRWMutex_TooManyWaiters(t *testing.T) {
	m := newTestMaxRWMutex(1)

	var wg sync.WaitGroup

	// Take lock
	m.Lock()

	// First waiter — allowed
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.RLock()
		time.Sleep(50 * time.Millisecond)
		m.RUnlock()
	}()

	time.Sleep(10 * time.Millisecond)

	// Second waiter — should be rejected
	ok := m.TryRLock()
	if ok {
		t.Fatal("expected TryRLock to fail due to too many waiters")
	}

	m.Unlock()
	wg.Wait()
}

func TestMaxRWMutex_RTLockAlias(t *testing.T) {
	m := newTestMaxRWMutex(1)

	ok := m.RTryLock()
	if !ok {
		t.Fatal("expected RTryLock to succeed")
	}
	m.RUnlock()
}
