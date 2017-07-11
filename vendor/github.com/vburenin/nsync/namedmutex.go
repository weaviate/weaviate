// Mutex that can be acquired by a given name. Makes overall life much easier during handling map key specific updates.
// Warning: Created named mutexes are never removed.

package nsync

import (
	"sync"
	"time"
)

// NamedMutex acquires a lock based on a user defined name.
// Can be used in factories which produce singleton objects depending
// on the name. For example: queue name, user name, etc.
// Locks are based on channels, so an additional TryLock has been introduced.
type NamedMutex struct {
	mutexMap   map[string]chan struct{}
	localMutex sync.Mutex
}

// NewNamedMutex makes a new named mutex instance.
func NewNamedMutex() *NamedMutex {
	return &NamedMutex{
		mutexMap: make(map[string]chan struct{}),
	}
}

// Lock acquires the lock. On the first lock attempt
// a new channel is automatically created.
func (nm *NamedMutex) Lock(name string) {
	nm.localMutex.Lock()
	mc, ok := nm.mutexMap[name]
	if !ok {
		mc = make(chan struct{}, 1)
		nm.mutexMap[name] = mc
	}
	nm.localMutex.Unlock()
	mc <- struct{}{}
}

// TryLock tries to acquires the lock returning true on success.
// On the first lock attempt a new channel is automatically created.
func (nm *NamedMutex) TryLock(name string) bool {
	nm.localMutex.Lock()
	mc, ok := nm.mutexMap[name]
	if !ok {
		mc = make(chan struct{}, 1)
		nm.mutexMap[name] = mc
	}
	nm.localMutex.Unlock()
	select {
	case mc <- struct{}{}:
		return true
	default:
		return false
	}
}

// TryLockTimeout tries to acquires the lock returning true on success.
// Attempt to acquire the lock will timeout after the caller defined interval.
// On the first lock attempt a new channel is automatically created.
func (nm *NamedMutex) TryLockTimeout(name string, timeout time.Duration) bool {
	nm.localMutex.Lock()
	mc, ok := nm.mutexMap[name]
	if !ok {
		mc = make(chan struct{}, 1)
		nm.mutexMap[name] = mc
	}
	nm.localMutex.Unlock()
	select {
	case mc <- struct{}{}:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Unlock releases the lock. If lock hasn't been acquired
// function will panic.
func (nm *NamedMutex) Unlock(name string) {
	nm.localMutex.Lock()
	defer nm.localMutex.Unlock()
	mc := nm.mutexMap[name]
	if len(mc) == 0 {
		panic("No named mutex acquired: " + name)
	}
	<-mc
}
