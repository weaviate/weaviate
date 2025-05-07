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

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// MeteredRWMutex is a read/write mutex with metering for lock usage.  It provides insight into how many threads are waiting on each lock, and how many are currently holding the lock (or read lock).
type MeteredRWMutex struct {
	rwlock   sync.RWMutex
	location string
}

func NewMeteredRWMutex(location string) *MeteredRWMutex {
	return &MeteredRWMutex{
		location: location,
	}
}

func (m *MeteredRWMutex) CtxRWLocation(location string) {
	m.location = location
}

func (m *MeteredRWMutex) Lock() {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	m.rwlock.Lock()
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
}

func (m *MeteredRWMutex) TryLock() bool {
	ok := m.rwlock.TryLock()
	if ok {
		monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
	}
	return ok
}

func (m *MeteredRWMutex) Unlock() {
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Dec()
	m.rwlock.Unlock()
}

func (m *MeteredRWMutex) RLock() {
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Inc()
	m.rwlock.RLock()
	monitoring.GetMetrics().LocksWaiting.WithLabelValues(m.location).Dec()
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
}

func (m *MeteredRWMutex) TryRLock() bool {
	ok := m.rwlock.TryRLock()
	if ok {
		monitoring.GetMetrics().Locks.WithLabelValues(m.location).Inc()
	}
	return ok
}

func (m *MeteredRWMutex) RTryLock() bool {
	return m.TryRLock()
}

func (m *MeteredRWMutex) RUnlock() {
	monitoring.GetMetrics().Locks.WithLabelValues(m.location).Dec()
	m.rwlock.RUnlock()
}
