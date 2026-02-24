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
		done <- sem.Acquire(ctx, 2)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error when n > limit")
		}
		if !errors.Is(err, ErrRequestExceedsLimit) {
			t.Fatalf("expected ErrRequestExceedsLimit, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire unexpectedly blocked")
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

	release := make(chan struct{})

	for i := 0; i < 3; i++ {
		i := i
		go func() {
			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			<-release
			sem.Release(1)
		}()
		// Wait until this goroutine is actually in the waiters list
		// before starting the next one, to guarantee FIFO order.
		eventually(t, time.Second, func() bool {
			sem.mu.Lock()
			defer sem.mu.Unlock()
			return len(sem.waiters) == i+1
		})
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

func TestParentLimitDecreaseWhileChildHoldsResource(t *testing.T) {
	var parentLimit int64 = 5
	parent := NewDynamicWeighted(func() int64 {
		return atomic.LoadInt64(&parentLimit)
	})
	child := NewDynamicWeightedWithParent(parent, func() int64 { return 10 })

	ctx := context.Background()

	// Child acquires 3 units when parent limit is 5
	if err := child.Acquire(ctx, 3); err != nil {
		t.Fatalf("unexpected error acquiring: %v", err)
	}

	// Parent limit decreases to 2 (below what child holds)
	atomic.StoreInt64(&parentLimit, 2)

	// Child should still be able to release correctly
	child.Release(3)

	// Verify parent was released correctly
	if !parent.TryAcquire(2) {
		t.Fatal("parent capacity not properly released after limit decrease")
	}
	parent.Release(2)
}

func TestParentLimitIncreaseWhileChildHoldsResource(t *testing.T) {
	var parentLimit int64 = 2
	parent := NewDynamicWeighted(func() int64 {
		return atomic.LoadInt64(&parentLimit)
	})
	child := NewDynamicWeightedWithParent(parent, func() int64 { return 10 })

	ctx := context.Background()

	// Child acquires 2 units when parent limit is 2
	if err := child.Acquire(ctx, 2); err != nil {
		t.Fatalf("unexpected error acquiring: %v", err)
	}

	// Parent limit increases to 10
	atomic.StoreInt64(&parentLimit, 10)

	// Child should still be able to release correctly
	child.Release(2)

	// Verify parent was released correctly and can now accept more
	if !parent.TryAcquire(10) {
		t.Fatal("parent capacity not properly released after limit increase")
	}
	parent.Release(10)
}

func TestParentLimitChangeWithMultipleChildren(t *testing.T) {
	var parentLimit int64 = 5
	parent := NewDynamicWeighted(func() int64 {
		return atomic.LoadInt64(&parentLimit)
	})
	childA := NewDynamicWeightedWithParent(parent, func() int64 { return 10 })
	childB := NewDynamicWeightedWithParent(parent, func() int64 { return 10 })

	ctx := context.Background()

	// Both children acquire resources when parent limit is 5
	if err := childA.Acquire(ctx, 2); err != nil {
		t.Fatalf("unexpected error acquiring childA: %v", err)
	}
	if err := childB.Acquire(ctx, 3); err != nil {
		t.Fatalf("unexpected error acquiring childB: %v", err)
	}

	// Parent limit changes to 3 (below total held: 2+3=5)
	atomic.StoreInt64(&parentLimit, 3)

	// Both children should still be able to release correctly
	childA.Release(2)
	childB.Release(3)

	// Verify parent was released correctly
	if !parent.TryAcquire(3) {
		t.Fatal("parent capacity not properly released after limit change with multiple children")
	}
	parent.Release(3)
}

func TestParentLimitChangeWhileChildWaiting(t *testing.T) {
	var parentLimit int64 = 1
	parent := NewDynamicWeighted(func() int64 {
		return atomic.LoadInt64(&parentLimit)
	})
	child := NewDynamicWeightedWithParent(parent, func() int64 { return 2 })

	ctx := context.Background()

	// First acquire uses up parent limit
	if err := child.Acquire(ctx, 1); err != nil {
		t.Fatalf("unexpected error acquiring: %v", err)
	}

	// Second acquire will block waiting for parent
	done := make(chan struct{})
	go func() {
		_ = child.Acquire(ctx, 1)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)

	// Verify it's blocked
	select {
	case <-done:
		t.Fatal("acquire should be blocked")
	default:
	}

	// Increase parent limit while child is waiting
	atomic.StoreInt64(&parentLimit, 2)

	// Release first acquire - should unblock waiting acquire
	child.Release(1)

	eventually(t, time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})

	// Clean up
	child.Release(1)
}

func TestParentLimitDecreaseWhileChildWaiting(t *testing.T) {
	var parentLimit int64 = 3
	parent := NewDynamicWeighted(func() int64 {
		return atomic.LoadInt64(&parentLimit)
	})
	child := NewDynamicWeightedWithParent(parent, func() int64 { return 5 })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Acquire 2 units, leaving 1 in parent
	if err := child.Acquire(ctx, 2); err != nil {
		t.Fatalf("unexpected error acquiring: %v", err)
	}

	// Try to acquire 2 more (would need 2 from parent, but only 1 available)
	acquireErr := make(chan error, 1)
	go func() {
		acquireErr <- child.Acquire(ctx, 2)
	}()

	time.Sleep(10 * time.Millisecond)

	// Verify it's blocked
	select {
	case err := <-acquireErr:
		t.Fatalf("acquire should be blocked, got: %v", err)
	default:
	}

	// Decrease parent limit to 1 (total held is 2, so limit is now below held)
	atomic.StoreInt64(&parentLimit, 1)

	// Release should still work correctly even though limit decreased
	// This is the key test: release should not panic or corrupt state
	child.Release(2)

	// Cancel the waiting acquire's context to clean up
	cancel()

	// Wait for the waiting acquire to finish (it will fail due to context cancellation)
	err := <-acquireErr
	if err == nil {
		t.Fatal("expected acquire to fail due to context cancellation")
	}

	// Verify parent state is correct by trying to acquire what should be available
	// (parent limit is 1, and we released 2, so cur should be 0, allowing acquire of 1)
	ctx2 := context.Background()
	if err := parent.Acquire(ctx2, 1); err != nil {
		t.Fatalf("parent should allow acquire after release, got error: %v", err)
	}
	parent.Release(1)
}

func TestTryAcquireParentFails(t *testing.T) {
	// Test TryAcquire when parent.TryAcquire fails (lines 129-131)
	parent := NewDynamicWeighted(func() int64 { return 1 })
	child := NewDynamicWeightedWithParent(parent, func() int64 { return 10 })

	ctx := context.Background()

	// Acquire from parent directly, leaving no capacity
	if err := parent.Acquire(ctx, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Child TryAcquire should fail because parent has no capacity
	if child.TryAcquire(1) {
		t.Fatal("TryAcquire should fail when parent has no capacity")
	}

	// Verify parent state is unchanged
	parent.Release(1)
}

func TestTryAcquireExceedsLimit(t *testing.T) {
	// Test TryAcquire when n > limit (line 136, first condition)
	sem := NewDynamicWeighted(func() int64 { return 1 })

	if sem.TryAcquire(2) {
		t.Fatal("TryAcquire should fail when n > limit")
	}
}

func TestTryAcquireExceedsCurrentCapacity(t *testing.T) {
	// Test TryAcquire when s.cur+n > limit (line 136, third condition)
	sem := NewDynamicWeighted(func() int64 { return 2 })

	ctx := context.Background()

	// Acquire 1 unit, leaving 1 available
	if err := sem.Acquire(ctx, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to acquire 2 more (would exceed limit of 2)
	if sem.TryAcquire(2) {
		t.Fatal("TryAcquire should fail when cur+n > limit")
	}

	sem.Release(1)
}

func TestTryAcquireWithParentRollbackOnLimitExceeded(t *testing.T) {
	// Test TryAcquire rollback when local acquisition fails due to n > limit
	parent := NewDynamicWeighted(func() int64 { return 10 })
	var childLimit int64 = 1
	child := NewDynamicWeightedWithParent(parent, func() int64 {
		return atomic.LoadInt64(&childLimit)
	})

	// Acquire from parent directly to track its state
	if !parent.TryAcquire(1) {
		t.Fatal("parent should have capacity")
	}

	// TryAcquire from child with n > limit - should fail and rollback parent
	// First, parent.TryAcquire(2) will succeed (parent now has 3 used: 1 + 2)
	// Then child.TryAcquire will fail because 2 > 1 (limit)
	// This should trigger parent.Release(2) rollback (parent now has 1 used)
	if child.TryAcquire(2) {
		t.Fatal("TryAcquire should fail when n > limit")
	}

	// Verify parent was rolled back - should have 9 available (1 used, 9 free)
	if !parent.TryAcquire(9) {
		t.Fatal("parent should have 9 units available after TryAcquire rollback")
	}
	parent.Release(10) // Release all (1 + 9)
}

func TestTryAcquireWithParentRollbackOnWaiters(t *testing.T) {
	// Test TryAcquire rollback when local acquisition fails due to waiters (lines 138-140)
	parent := NewDynamicWeighted(func() int64 { return 10 })
	child := NewDynamicWeightedWithParent(parent, func() int64 { return 1 })

	ctx := context.Background()

	// Acquire from child, leaving no child capacity
	if err := child.Acquire(ctx, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check parent has 1 unit used
	if !parent.TryAcquire(9) {
		t.Fatal("parent should have 9 units available")
	}
	parent.Release(9)

	// Create a waiter - this will acquire from parent and then wait on child
	waiting := make(chan struct{})
	go func() {
		close(waiting)
		_ = child.Acquire(ctx, 1)
	}()

	<-waiting
	time.Sleep(10 * time.Millisecond)

	// Now parent should have 2 units used (1 from first acquire, 1 from waiting acquire)
	// TryAcquire should fail due to waiters, and if it acquired from parent, it should rollback
	if child.TryAcquire(1) {
		t.Fatal("TryAcquire should fail when waiters exist")
	}

	// Verify parent state - should have 2 units used (from the two Acquire calls)
	// The TryAcquire rollback should have released any parent capacity it acquired
	if parent.TryAcquire(10) {
		t.Fatal("parent should have 2 units used")
	}
	if !parent.TryAcquire(8) {
		t.Fatal("parent should have 8 units available")
	}
	parent.Release(8)

	// Clean up
	child.Release(1)
	<-waiting // Wait for waiter to complete
	child.Release(1)
}
