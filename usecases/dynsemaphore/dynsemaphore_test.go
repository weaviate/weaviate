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

package dynsemaphore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func eventually(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestAcquireReleaseBasic(t *testing.T) {
	sem := NewDynamicWeighted(func() int64 { return 2 })

	ctx := context.Background()

	if err := sem.Acquire(ctx, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sem.TryAcquire(1) {
		t.Fatal("expected TryAcquire to succeed")
	}

	sem.Release(2)

	if !sem.TryAcquire(2) {
		t.Fatal("expected full capacity after release")
	}
}

func TestAcquireFailsWhenRequestExceedsLimit(t *testing.T) {
	var limit int64 = 1
	sem := NewDynamicWeighted(func() int64 {
		return atomic.LoadInt64(&limit)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		// Request more than the limit → must block until ctx is canceled
		done <- sem.Acquire(ctx, 2)
	}()

	// Give goroutine time to reach the blocking path
	time.Sleep(10 * time.Millisecond)

	// Cancel the context to unblock Acquire
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected context error when n > limit")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not unblock after context cancellation")
	}

	// Ensure semaphore is still usable
	if err := sem.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("semaphore corrupted after oversized acquire: %v", err)
	}
	sem.Release(1)
}

func TestTryAcquireBlockedByWaiter(t *testing.T) {
	sem := NewDynamicWeighted(func() int64 { return 1 })
	ctx := context.Background()

	if err := sem.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	waiting := make(chan struct{})
	go func() {
		close(waiting)
		_ = sem.Acquire(ctx, 1)
	}()

	<-waiting

	if sem.TryAcquire(1) {
		t.Fatal("TryAcquire must fail when waiters exist")
	}

	sem.Release(1)
}

func TestAcquireContextCancel(t *testing.T) {
	sem := NewDynamicWeighted(func() int64 { return 1 })
	ctx := context.Background()

	if err := sem.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	ctx2, cancel := context.WithCancel(context.Background())
	done := make(chan error)

	go func() {
		done <- sem.Acquire(ctx2, 1)
	}()

	cancel()

	err := <-done
	if err == nil {
		t.Fatal("expected context cancellation error")
	}

	// After cancellation, semaphore must still work
	sem.Release(1)

	if err := sem.Acquire(ctx, 1); err != nil {
		t.Fatalf("unexpected error after cancellation: %v", err)
	}
}

func TestFIFOOrdering(t *testing.T) {
	sem := NewDynamicWeighted(func() int64 { return 1 })
	ctx := context.Background()

	if err := sem.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	var order []int
	var mu sync.Mutex

	enqueued := make(chan struct{}, 3)
	release := make(chan struct{})

	for i := 0; i < 3; i++ {
		i := i
		go func() {
			enqueued <- struct{}{} // signal intent
			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			<-release
			sem.Release(1)
		}()
		<-enqueued // enforce enqueue order
	}

	// Allow first waiter to proceed
	sem.Release(1)

	for i := 0; i < 3; i++ {
		release <- struct{}{}
	}

	eventually(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	})

	for i := 0; i < 3; i++ {
		if order[i] != i {
			t.Fatalf("FIFO violated: got %v", order)
		}
	}
}

func TestDynamicLimitIncrease(t *testing.T) {
	var limit int64 = 1
	sem := NewDynamicWeighted(func() int64 {
		return atomic.LoadInt64(&limit)
	})

	ctx := context.Background()

	if err := sem.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	acquired := make(chan struct{})
	go func() {
		_ = sem.Acquire(ctx, 1)
		close(acquired)
	}()

	time.Sleep(10 * time.Millisecond)

	select {
	case <-acquired:
		t.Fatal("should not acquire before limit increase")
	default:
	}

	atomic.StoreInt64(&limit, 2)
	sem.Release(1)

	eventually(t, time.Second, func() bool {
		select {
		case <-acquired:
			return true
		default:
			return false
		}
	})
}

func TestDynamicLimitDecrease(t *testing.T) {
	var limit int64 = 2
	sem := NewDynamicWeighted(func() int64 {
		return atomic.LoadInt64(&limit)
	})

	ctx := context.Background()

	if err := sem.Acquire(ctx, 2); err != nil {
		t.Fatal(err)
	}

	atomic.StoreInt64(&limit, 1)

	done := make(chan struct{})
	go func() {
		_ = sem.Acquire(ctx, 1)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("acquire should block after limit decrease")
	default:
	}

	sem.Release(2)

	eventually(t, time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

func TestReleasePanicsOnOverRelease(t *testing.T) {
	sem := NewDynamicWeighted(func() int64 { return 1 })

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on over-release")
		}
	}()

	sem.Release(1)
}

func TestConcurrentStress(t *testing.T) {
	var limit int64 = 5
	sem := NewDynamicWeighted(func() int64 {
		return atomic.LoadInt64(&limit)
	})

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
			sem.Release(1)
		}()
	}

	wg.Wait()
}

func TestParentLimitsChild(t *testing.T) {
	parent := NewDynamicWeighted(func() int64 { return 1 })
	child := NewDynamicWeightedWithParent(parent, func() int64 { return 1 })

	ctx := context.Background()

	if err := child.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		_ = child.Acquire(ctx, 1)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("child acquire should block due to parent limit")
	default:
	}

	child.Release(1)

	eventually(t, time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

func TestGlobalLimitAcrossMultipleChildren(t *testing.T) {
	parent := NewDynamicWeighted(func() int64 { return 2 })

	childA := NewDynamicWeightedWithParent(parent, func() int64 { return 2 })
	childB := NewDynamicWeightedWithParent(parent, func() int64 { return 2 })

	ctx := context.Background()

	if err := childA.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := childB.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		_ = childA.Acquire(ctx, 1)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("acquire should block due to global limit")
	default:
	}

	childB.Release(1)

	eventually(t, time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

func TestReleaseReleasesParent(t *testing.T) {
	parent := NewDynamicWeighted(func() int64 { return 1 })
	child := NewDynamicWeightedWithParent(parent, func() int64 { return 1 })

	ctx := context.Background()

	if err := child.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	child.Release(1)

	if !parent.TryAcquire(1) {
		t.Fatal("parent capacity not released")
	}

	parent.Release(1)
}

func TestParentRollbackOnChildAcquireFailure(t *testing.T) {
	parent := NewDynamicWeighted(func() int64 { return 1 })

	// child limit always zero
	child := NewDynamicWeightedWithParent(parent, func() int64 { return 0 })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error)
	go func() {
		done <- child.Acquire(ctx, 1)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	err := <-done
	if err == nil {
		t.Fatal("expected acquire failure")
	}

	// parent must not be leaked
	if !parent.TryAcquire(1) {
		t.Fatal("parent capacity leaked after failed child acquire")
	}
	parent.Release(1)
}
