package ctxlock

import (
	"context"
	"sync"
	"time"
)

type CtxLock struct {
	ch           chan struct{}
	timeoutMu    sync.RWMutex
	timeout      time.Duration
}

func NewCtxLock(timeoutMillis int) *CtxLock {
	return &CtxLock{
		ch:      make(chan struct{}, 1),
		timeout: time.Duration(timeoutMillis) * time.Millisecond,
	}
}

// Lock attempts to acquire the lock with both context and internal timeout
func (l *CtxLock) Lock(ctx context.Context) error {
	l.timeoutMu.RLock()
	timeout := l.timeout
	l.timeoutMu.RUnlock()

	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	select {
	case l.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unlock releases the lock
func (l *CtxLock) Unlock() {
	select {
	case <-l.ch:
	default:
		panic("unlock of unlocked CtxLock")
	}
}

// SetTimeout updates the default timeout (in milliseconds) for the lock
func (l *CtxLock) SetTimeout(timeoutMillis int) {
	l.timeoutMu.Lock()
	defer l.timeoutMu.Unlock()
	l.timeout = time.Duration(timeoutMillis) * time.Millisecond
}

// Timeout returns the current default timeout
func (l *CtxLock) Timeout() time.Duration {
	l.timeoutMu.RLock()
	defer l.timeoutMu.RUnlock()
	return l.timeout
}