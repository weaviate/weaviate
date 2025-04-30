package ctxlock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCtxLock_LockUnlock(t *testing.T) {
	lock := NewCtxLock(0)

	if err := lock.Lock(context.Background()); err != nil {
		t.Fatalf("unexpected error acquiring lock: %v", err)
	}
	lock.Unlock()

	// Try again after unlock
	if err := lock.Lock(context.Background()); err != nil {
		t.Fatalf("unexpected error on re-acquire: %v", err)
	}
	lock.Unlock()
}

func TestCtxLock_ContextTimeout(t *testing.T) {
	lock := NewCtxLock(0)

	// Acquire lock to block the next goroutine
	if err := lock.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := lock.Lock(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got: %v", err)
	}

	lock.Unlock()
}

func TestCtxLock_DefaultTimeout(t *testing.T) {
	lock := NewCtxLock(100) // 100ms default timeout

	// First goroutine locks and holds
	if err := lock.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	err := lock.Lock(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error due to timeout, got none")
	}
	if elapsed < 90*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Fatalf("expected ~100ms wait, got %v", elapsed)
	}

	lock.Unlock()
}

func TestCtxLock_UpdateTimeoutThreadSafe(t *testing.T) {
	lock := NewCtxLock(50)

	// Acquire lock so next goroutine has to wait
	if err := lock.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}

	ready := make(chan struct{})
	done := make(chan struct{})

	go func() {
		<-ready // wait until main signals to proceed
		err := lock.Lock(context.Background())
		if err == nil {
			t.Error("expected timeout error but got none")
		}
		close(done)
	}()

	// Increase timeout *before* the goroutine starts waiting
	lock.SetTimeout(500)
	if newTimeout := lock.Timeout(); newTimeout != 500*time.Millisecond {
		t.Errorf("expected timeout to be 500ms, got %v", newTimeout)
	}

	close(ready) // allow goroutine to try locking (after timeout is updated)

	// Sleep past original timeout but not past new one
	time.Sleep(100 * time.Millisecond)
	lock.Unlock() // allow other goroutine to try

	<-done
}

func TestCtxLock_ContextOverridesDefaultTimeout(t *testing.T) {
	lock := NewCtxLock(500) // Should be overridden by context

	lock.Lock(context.Background()) // Block lock

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := lock.Lock(ctx)
	elapsed := time.Since(start)

	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got: %v", err)
	}
	if elapsed < 90*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Fatalf("context deadline not respected, waited: %v", elapsed)
	}

	lock.Unlock()
}