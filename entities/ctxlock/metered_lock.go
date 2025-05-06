package ctxlock

import (
	"sync"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// MeteredRWMutex is a read/write mutex with metering for lock usage
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
