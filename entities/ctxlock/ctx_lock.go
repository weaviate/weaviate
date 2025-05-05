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

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	caslock "github.com/viney-shih/go-lock"
)

var ErrCtxTimeout = errors.New("ctxsync: lock acquisition timed out")

// CtxRWMutex is a context-aware read/write mutex
type CtxRWMutex struct {
	rwlock        *caslock.CASMutex // The underlying RWMutex
	location      string       // The location of the mutex, used for monitoring
	enforceTimout bool         // Whether to enforce timeout on lock acquisition

	doneMu    sync.Mutex
	doneCache []chan bool
}

const maxCachedDoneChans = 10

func (m *CtxRWMutex) getDoneChan() chan bool {
	m.doneMu.Lock()
	defer m.doneMu.Unlock()
	n := len(m.doneCache)
	if n == 0 {
		return make(chan bool, 1)
	}
	ch := m.doneCache[n-1]
	m.doneCache = m.doneCache[:n-1]
	return ch
}

func (m *CtxRWMutex) releaseDoneChan(ch chan bool) {
	// Drain in case the channel wasn't read
	select {
	case <-ch:
	default:
	}

	m.doneMu.Lock()
	defer m.doneMu.Unlock()
	if len(m.doneCache) < maxCachedDoneChans {
		m.doneCache = append(m.doneCache, ch)
	}
}

// NewCtxRWMutex creates a new context-aware read/write mutex
func NewCtxRWMutex(location string) *CtxRWMutex {
	return &CtxRWMutex{
		rwlock:   caslock.NewCASMutex(),
		location: location,
	}
}

// CtxRWLocation sets the location for the mutex, used for monitoring
func (m *CtxRWMutex) CtxRWLocation(location string) {
	m.location = location
}

// LockContext acquires the write lock or returns an error on timeout/cancel
func (m *CtxRWMutex) LockContext(ctx context.Context) error {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	defer monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	if m.enforceTimout {

		acquired := make(chan struct{})
		go func() {
			m.rwlock.Lock()
			close(acquired)
		}()

		select {
		case <-acquired:
			monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
			return nil
		case <-ctx.Done():
			// Spawn a cleaner to unlock once the goroutine does lock
			go func() {
				<-acquired // wait until lock is acquired
				m.rwlock.Unlock()
			}()
			return context.DeadlineExceeded
		}
	} else {
		m.rwlock.Lock()
		monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
		return nil
	}
}

// LockContextWithTimeout acquires the write lock or returns an error on timeout/cancel
func (m *CtxRWMutex) LockWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(),timeout)
	enterrors.GoWrapper(func() {
		time.Sleep(timeout)
		cancel()
	}, nil)
	return m.LockContext(ctx)
}

// Lock acquires the write lock
func (m *CtxRWMutex) Lock() {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	m.rwlock.Lock()
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
}

// TryLock attempts to acquire the write lock without blocking
func (m *CtxRWMutex) TryLock() bool {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	ret := m.rwlock.TryLock()
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
	return ret
}

// Unlock releases the write lock
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

	if m.enforceTimout {
		acquired := make(chan struct{})

		go func() {
			m.rwlock.RLock()
			close(acquired)
		}()

		select {
		case <-acquired:
			monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
			return nil
		case <-ctx.Done():
			go func() {
				<-acquired
				m.rwlock.RUnlock()
			}()
			return context.DeadlineExceeded
		}
	} else {
		m.rwlock.RLock()
		monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
		return nil
	}
}

// RLockContextWithTimeout acquires the read lock or returns on context cancel/timeout
func (m *CtxRWMutex) RLockWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	go func() {
		time.Sleep(timeout)
		cancel()
	}()
	return m.RLockContext(ctx)
}

// RLock acquires the read lock
func (m *CtxRWMutex) RLock() {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	defer monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	_ = m.RLockContext(context.Background())
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
}

// TryRLock attempts to acquire the read lock without blocking
func (m *CtxRWMutex) TryRLock() bool {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	defer monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	ret := m.rwlock.RTryLock()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
	return ret
}

// TryRLock attempts to acquire the read lock without blocking
func (m *CtxRWMutex) RTryLock() bool {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	defer monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	ret := m.rwlock.RTryLock()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
	return ret
}

// RUnlock releases the read lock
func (m *CtxRWMutex) RUnlock() {
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Dec()
	m.rwlock.RUnlock()
}
