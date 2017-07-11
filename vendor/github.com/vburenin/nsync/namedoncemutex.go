package nsync

import "sync"

// OnceMutex is a mutex that can be locked only once.
// Lock operation returns true if mutex has been successfully locked.
// Any other concurrent attempts will block until mutex is unlocked.
// However, any other attempts to grab a lock will return false.
type OnceMutex struct {
	mu   sync.Mutex
	used bool
}

// NewOnceMutex returns an instance of OnсeMutex.
func NewOnceMutex() *OnceMutex {
	return &OnceMutex{}
}

// Lock tries to acquire lock.
func (om *OnceMutex) Lock() bool {
	om.mu.Lock()
	if om.used {
		om.mu.Unlock()
		return false
	}
	return true
}

// Unlock tries to release a lock.
func (om *OnceMutex) Unlock() {
	om.used = true
	om.mu.Unlock()
}

// NamedOnceMutex is a map of dynamically created mutexes by provided id.
// First attempt to lock by id will create a new mutex and acquire a lock.
// All other concurrent attempts will block waiting mutex to be unlocked for the same id.
// Once mutex unlocked, all other lock attempts will return false for the same instance of mutex.
// Unlocked mutex is discarded. Next attempt to acquire a lock for the same id will succeed.
// Such behaviour may be used to refresh a local cache of data identified by some key avoiding
// concurrent request to receive a refreshed value for the same key.
type NamedOnceMutex struct {
	lockMap map[interface{}]*OnceMutex
	mutex   sync.Mutex
}

// NewNamedOnceMutex returns an instance of NamedOnceMutex.
func NewNamedOnceMutex() *NamedOnceMutex {
	return &NamedOnceMutex{
		lockMap: make(map[interface{}]*OnceMutex),
	}
}

// Lock try to acquire a lock for provided id. If attempt is successful, true is returned
// If lock is already acquired by something else it will block until mutex is unlocked returning false.
func (nom *NamedOnceMutex) Lock(useMutexKey interface{}) bool {
	nom.mutex.Lock()
	m, ok := nom.lockMap[useMutexKey]
	if ok {
		nom.mutex.Unlock()
		return m.Lock()
	}

	m = &OnceMutex{}
	m.Lock()
	nom.lockMap[useMutexKey] = m
	nom.mutex.Unlock()
	return true
}

// Unlock unlocks the locked mutex. Used mutex will be discarded.
func (nom *NamedOnceMutex) Unlock(useMutexKey interface{}) {
	nom.mutex.Lock()
	m, ok := nom.lockMap[useMutexKey]
	if ok {
		delete(nom.lockMap, useMutexKey)
		nom.mutex.Unlock()
		m.Unlock()
	} else {
		nom.mutex.Unlock()
	}
}
