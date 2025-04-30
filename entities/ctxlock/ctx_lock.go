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
	"time"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

var ErrCtxTimeout = errors.New("ctxsync: lock acquisition timed out")

type CtxRWMutex struct {
	rwlock   sync.RWMutex
	location string
}

// NewCtxRWMutex creates a new context-aware read/write mutex
func NewCtxRWMutex(location string) *CtxRWMutex {
	return &CtxRWMutex{
		rwlock:   sync.RWMutex{},
		location: location,
	}
}

func (m *CtxRWMutex) CtxRWLocation(location string) {
	m.location = location
}

// LockContext acquires the write lock or returns an error on timeout/cancel
func (m *CtxRWMutex) LockContext(ctx context.Context) error {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	defer monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	done := make(chan bool, 1)
	go func() {
		defer close(done)
		m.rwlock.Lock()
		done <- true
		time.Sleep(1000 * time.Millisecond)
	}()

	select {
	case <-done:
		monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
		return nil // Lock acquired successfully
	case <-ctx.Done():
		return context.DeadlineExceeded // Timeout or cancellation occurred
	}
}

func (m *CtxRWMutex) LockContextWithTimeout(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	go func() {
		time.Sleep(timeout)
		cancel()
	}()
	return m.LockContext(ctx)
}

func (m *CtxRWMutex) Lock() {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	m.rwlock.Lock()
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
}

func (m *CtxRWMutex) TryLock() bool {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	ret := m.rwlock.TryLock()
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
	return ret
}

func (m *CtxRWMutex) Unlock() {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	m.rwlock.Unlock()
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Dec()
}

// RLockContext acquires the read lock or returns on context cancel/timeout
func (m *CtxRWMutex) RLockContext(ctx context.Context) error {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	defer monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	done := make(chan bool, 1)
	go func() {
		defer close(done)
		m.rwlock.RLock()
		done <- true
		time.Sleep(1000 * time.Millisecond)
	}()

	select {
	case <-done:
		monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
		return nil // Lock acquired successfully
	case <-ctx.Done():
		return context.DeadlineExceeded // Timeout or cancellation occurred
	}
}

func (m *CtxRWMutex) RLockContextWithTimeout(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	go func() {
		time.Sleep(timeout)
		cancel()
	}()
	return m.RLockContext(ctx)
}

func (m *CtxRWMutex) RLock() {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	defer monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	_ = m.RLockContext(context.Background())
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
}

func (m *CtxRWMutex) TryRLock() bool {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	defer monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	ret := m.rwlock.TryRLock()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
	return ret
}

func (m *CtxRWMutex) RUnlock() {
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Dec()
	m.rwlock.RUnlock()
}
