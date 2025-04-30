package ctxlock

import (
	"context"
	"errors"
	"time"
)

var ErrCtxTimeout = errors.New("ctxsync: lock acquisition timed out")

type CtxRWMutex struct {
	writeCh   chan struct{}
	readersCh chan struct{}
	readers   int
	writer    bool
}

// NewCtxRWMutex creates a new context-aware read/write mutex
func NewCtxRWMutex() *CtxRWMutex {
	return &CtxRWMutex{
		writeCh:   make(chan struct{}, 1),
		readersCh: make(chan struct{}, 1),
	}
}

// LockContext acquires the write lock or returns an error on timeout/cancel
func (m *CtxRWMutex) LockContext(ctx context.Context) error {
	select {
	case m.writeCh <- struct{}{}:
		// Wait for all readers to exit
		for {
			select {
			case <-m.readersCh:
				m.readers--
				if m.readers == 0 {
					m.writer = true
					return nil
				}
			default:
				if m.readers == 0 {
					m.writer = true
					return nil
				}
				time.Sleep(1 * time.Millisecond)
			}
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *CtxRWMutex) Lock() {
	_ = m.LockContext(context.Background())
}

func (m *CtxRWMutex) TryLock() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	return m.LockContext(ctx) == nil
}

func (m *CtxRWMutex) Unlock() {
	if !m.writer {
		panic("ctxsync: unlock called without writer lock")
	}
	m.writer = false
	select {
	case <-m.writeCh:
	default:
		panic("ctxsync: inconsistent writeCh")
	}
}

// RLockContext acquires the read lock or returns on context cancel/timeout
func (m *CtxRWMutex) RLockContext(ctx context.Context) error {
	for {
		if m.writer || len(m.writeCh) > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				time.Sleep(1 * time.Millisecond)
			}
		} else {
			m.readers++
			m.readersCh <- struct{}{}
			return nil
		}
	}
}

func (m *CtxRWMutex) RLock() {
	_ = m.RLockContext(context.Background())
}

func (m *CtxRWMutex) TryRLock() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	return m.RLockContext(ctx) == nil
}

func (m *CtxRWMutex) RUnlock() {
	if m.readers <= 0 {
		panic("ctxsync: RUnlock without RLock")
	}
	m.readers--
	select {
	case <-m.readersCh:
	default:
	}
}