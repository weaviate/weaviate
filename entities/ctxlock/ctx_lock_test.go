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
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCtxRWMutex_LockUnlock(t *testing.T) {
	m := NewCtxRWMutex("")

	m.Lock()
	m.Unlock()

	m.Lock()
	m.Unlock()
}

func TestCtxRWMutex_RLockRUnlock(t *testing.T) {
	m := NewCtxRWMutex("")

	m.RLock()
	m.RUnlock()

	m.RLock()
	m.RUnlock()
}

func TestCtxRWMutex_TryLock(t *testing.T) {
	m := NewCtxRWMutex("")

	ok := m.TryLock()
	if !ok {
		t.Fatal("expected TryLock to succeed")
	}
	defer m.Unlock()

	ok = m.TryLock()
	if ok {
		t.Fatal("expected second TryLock to fail while locked")
	}
}

func TestCtxRWMutex_TryRLock(t *testing.T) {
	m := NewCtxRWMutex("")

	ok := m.TryRLock()
	if !ok {
		t.Fatal("expected TryRLock to succeed")
	}
	defer m.RUnlock()

	ok = m.TryRLock()
	if !ok {
		t.Fatal("expected second TryRLock to succeed while locked")
	}

	m.RUnlock()
}

func TestCtxRWMutex_LockContext_Timeout(t *testing.T) {
	m := NewCtxRWMutex("")
	m.Lock()
	defer m.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := m.LockContext(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected context timeout wait, got %v", elapsed)
	}
}

func TestCtxRWMutex_RLockContext_Timeout(t *testing.T) {
	m := NewCtxRWMutex("")
	m.Lock()
	defer m.Unlock()

	ctx, _ := context.WithTimeout(context.Background(), 50*time.Millisecond)

	start := time.Now()
	err := m.RLockContext(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected context timeout wait, got %v", elapsed)
	}
}

func TestCtxRWMutex_ParallelReaders(t *testing.T) {
	m := NewCtxRWMutex("")
	var wg sync.WaitGroup
	numReaders := 5
	started := make(chan struct{}, numReaders)

	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()

			if err := m.RLockContext(context.Background()); err != nil {
				t.Errorf("reader %d failed to acquire lock: %v", id, err)
				return
			}
			started <- struct{}{}             // signal this reader got the lock
			time.Sleep(50 * time.Millisecond) // simulate work
			m.RUnlock()
		}(i)
	}

	// Wait for all readers to start
	for i := 0; i < numReaders; i++ {
		select {
		case <-started:
			// good
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("reader %d did not start in time", i)
		}
	}

	wg.Wait()
}

func TestCtxRWMutex_WriterBlocksReader(t *testing.T) {
	m := NewCtxRWMutex("")
	m.Lock()

	var got string
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		if err := m.RLockContext(ctx); err != nil {
			got = err.Error()
		} else {
			got = "acquired"
			m.RUnlock()
		}
	}()

	time.Sleep(150 * time.Millisecond)
	m.Unlock()
	<-done

	if got == "acquired" {
		t.Fatal("reader should have timed out while writer held the lock")
	}
}

func TestCtxRWMutex_WritersBlockEachOther(t *testing.T) {
	m := NewCtxRWMutex("")
	m.Lock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := m.LockContext(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected deadline exceeded, got: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	m.Unlock()
	<-done
}
