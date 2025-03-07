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

// Package sync provides string-keyed synchronization primitives with automatic cleanup.
//
// The package implements two main types:
//   - KeyLocker: A mutex-based synchronization mechanism using string keys
//   - KeyRWLocker: A read-write mutex-based synchronization mechanism using string keys
//
// Both implementations maintain an internal cache of mutexes that are automatically
// created on first use and cleaned up when no longer needed. This is achieved through
// reference counting: each Lock/RLock operation increments a counter, and each
// Unlock/RUnlock decrements it. When the counter reaches zero, the mutex is removed
// from the cache.
//
// Example usage of KeyLocker:
//
//	locker := sync.NewKeyLocker()
//
//	// Exclusive access
//	locker.Lock("user-123")
//	defer locker.Unlock("user-123")
//	// Critical section here...
//
// Example usage of KeyRWLocker:
//
//	rwlocker := sync.NewKeyRWLocker()
//
//	// Shared access (multiple readers)
//	rwlocker.RLock("doc-456")
//	defer rwlocker.RUnlock("doc-456")
//	// Read-only section...
//
//	// Exclusive access (single writer)
//	rwlocker.Lock("doc-456")
//	defer rwlocker.Unlock("doc-456")
//	// Write section...
//
// Features:
//   - Automatic mutex creation and cleanup
//   - Reference counting for proper resource management
//   - Thread-safe mutex cache using sync.Map
//   - Panic behavior matching standard sync.Mutex/RWMutex
//   - Zero configuration required
//
// Note: The implementation maintains mutexes in memory only while they are in use.
// Once all locks are released, the mutex is automatically removed from the cache.
package sync

import (
	"sync"
	"sync/atomic"
	"time"
)

// lockInfo holds a mutex and its reference count for the KeyLocker implementation.
type lockInfo struct {
	mutex      *sync.Mutex
	refs       int32 // atomic reference counter
	lastAccess int64 // atomic timestamp of last access
}

// KeyLocker provides mutex-based synchronization using string keys.
// It maintains a cache of mutexes in a sync.Map for reuse.
type KeyLocker struct {
	m             sync.Map // map[string]*lockInfo
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

// NewKeyLocker creates a new KeyLocker instance.
func NewKeyLocker() *KeyLocker {
	locker := &KeyLocker{
		stopCleanup: make(chan struct{}),
	}

	// Start a cleanup ticker for this instance
	locker.cleanupTicker = time.NewTicker(5 * time.Minute) // TODO : this can be configurable

	go func() {
		for {
			select {
			case <-locker.cleanupTicker.C:
				// Get current time minus 10 minutes for expiry check
				cutoff := time.Now().Add(-10 * time.Minute).UnixNano()
				locker.cleanup(cutoff)
			case <-locker.stopCleanup:
				locker.cleanupTicker.Stop()
				return
			}
		}
	}()

	return locker
}

// Lock acquires a mutex for the given ID.
func (s *KeyLocker) Lock(ID string) {
	if v, ok := s.m.Load(ID); ok {
		info := v.(*lockInfo)
		atomic.AddInt32(&info.refs, 1)
		atomic.StoreInt64(&info.lastAccess, time.Now().UnixNano())
		info.mutex.Lock()
		return
	}

	info := &lockInfo{
		mutex:      &sync.Mutex{},
		refs:       1,
		lastAccess: time.Now().UnixNano(),
	}

	actual, loaded := s.m.LoadOrStore(ID, info)
	if !loaded {
		info.mutex.Lock()
		return
	}

	// Someone else created the mutex, use that one
	existingInfo := actual.(*lockInfo)
	atomic.AddInt32(&existingInfo.refs, 1)
	atomic.StoreInt64(&existingInfo.lastAccess, time.Now().UnixNano())
	existingInfo.mutex.Lock()
}

// Unlock releases the mutex for the given ID.
func (s *KeyLocker) Unlock(ID string) {
	v, ok := s.m.Load(ID)
	if !ok {
		// If no mutex exists, we're unlocking an unlocked mutex
		var m sync.Mutex
		m.Unlock() // This will panic appropriately
		return
	}

	info := v.(*lockInfo)
	info.mutex.Unlock()

	atomic.StoreInt64(&info.lastAccess, time.Now().UnixNano())
	atomic.AddInt32(&info.refs, -1)
}

// cleanup removes expired mutexes from the cache
func (s *KeyLocker) cleanup(cutoff int64) {
	var keysToCheck []string
	var infosToCheck []*lockInfo

	s.m.Range(func(key, value interface{}) bool {
		keysToCheck = append(keysToCheck, key.(string))
		infosToCheck = append(infosToCheck, value.(*lockInfo))
		return true
	})

	for i, key := range keysToCheck {
		info := infosToCheck[i]
		lastAccess := atomic.LoadInt64(&info.lastAccess)

		if lastAccess < cutoff && atomic.LoadInt32(&info.refs) == 0 {
			if info.mutex.TryLock() {
				s.m.Delete(key)
				info.mutex.Unlock()
			}
		}
	}
}

// rwLockInfo holds a read-write mutex and its reference count for the KeyRWLocker implementation.
type rwLockInfo struct {
	mutex      *sync.RWMutex
	refs       int32 // atomic reference counter
	lastAccess int64 // atomic timestamp of last access
}

// KeyRWLocker provides read-write mutex synchronization using string keys.
// It maintains a cache of RWMutexes in a sync.Map for reuse.
type KeyRWLocker struct {
	m             sync.Map // map[string]*rwLockInfo
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

// NewKeyRWLocker creates a new KeyRWLocker instance.
func NewKeyRWLocker() *KeyRWLocker {
	locker := &KeyRWLocker{
		stopCleanup: make(chan struct{}),
	}

	locker.cleanupTicker = time.NewTicker(5 * time.Minute)

	go func() {
		for {
			select {
			case <-locker.cleanupTicker.C:
				// Get current time minus 10 minutes for expiry check
				cutoff := time.Now().Add(-10 * time.Minute).UnixNano()
				locker.cleanup(cutoff)
			case <-locker.stopCleanup:
				locker.cleanupTicker.Stop()
				return
			}
		}
	}()

	return locker
}

// Lock acquires an exclusive (write) lock for the given ID.
func (s *KeyRWLocker) Lock(ID string) {
	if v, ok := s.m.Load(ID); ok {
		info := v.(*rwLockInfo)
		atomic.AddInt32(&info.refs, 1)
		atomic.StoreInt64(&info.lastAccess, time.Now().UnixNano())
		info.mutex.Lock()
		return
	}

	// Create new mutex
	info := &rwLockInfo{
		mutex:      &sync.RWMutex{},
		refs:       1,
		lastAccess: time.Now().UnixNano(),
	}

	// Try to store new mutex
	actual, loaded := s.m.LoadOrStore(ID, info)
	if !loaded {
		// We created a new mutex, acquire it
		info.mutex.Lock()
		return
	}

	// Someone else created the mutex, use that one
	existingInfo := actual.(*rwLockInfo)
	atomic.AddInt32(&existingInfo.refs, 1)
	atomic.StoreInt64(&existingInfo.lastAccess, time.Now().UnixNano())
	existingInfo.mutex.Lock()
}

// RLock acquires a shared (read) lock for the given ID.
func (s *KeyRWLocker) RLock(ID string) {
	if v, ok := s.m.Load(ID); ok {
		info := v.(*rwLockInfo)

		atomic.AddInt32(&info.refs, 1)

		atomic.StoreInt64(&info.lastAccess, time.Now().UnixNano())
		info.mutex.RLock()
		return
	}

	info := &rwLockInfo{
		mutex:      &sync.RWMutex{},
		refs:       1,
		lastAccess: time.Now().UnixNano(),
	}

	actual, loaded := s.m.LoadOrStore(ID, info)
	if !loaded {
		info.mutex.RLock()
		return
	}

	// Someone else created the mutex, use that one
	existingInfo := actual.(*rwLockInfo)
	atomic.AddInt32(&existingInfo.refs, 1)
	atomic.StoreInt64(&existingInfo.lastAccess, time.Now().UnixNano())
	existingInfo.mutex.RLock()
}

// Unlock releases an exclusive (write) lock for the given ID.
func (s *KeyRWLocker) Unlock(ID string) {
	v, ok := s.m.Load(ID)
	if !ok {
		// If no mutex exists, we're unlocking an unlocked mutex
		var m sync.RWMutex
		m.Unlock() // This will panic appropriately
		return
	}

	info := v.(*rwLockInfo)
	info.mutex.Unlock()

	atomic.StoreInt64(&info.lastAccess, time.Now().UnixNano())
	atomic.AddInt32(&info.refs, -1)
}

// RUnlock releases a shared (read) lock for the given ID.
func (s *KeyRWLocker) RUnlock(ID string) {
	v, ok := s.m.Load(ID)
	if !ok {
		// If no mutex exists, we're unlocking an unlocked mutex
		var m sync.RWMutex
		m.RUnlock() // This will panic appropriately
		return
	}

	info := v.(*rwLockInfo)
	info.mutex.RUnlock()

	atomic.StoreInt64(&info.lastAccess, time.Now().UnixNano())
	atomic.AddInt32(&info.refs, -1)
}

// cleanup removes expired mutexes from the cache
func (s *KeyRWLocker) cleanup(cutoff int64) {
	var keysToCheck []string
	var infosToCheck []*rwLockInfo

	s.m.Range(func(key, value interface{}) bool {
		keysToCheck = append(keysToCheck, key.(string))
		infosToCheck = append(infosToCheck, value.(*rwLockInfo))
		return true
	})

	for i, key := range keysToCheck {
		info := infosToCheck[i]
		lastAccess := atomic.LoadInt64(&info.lastAccess)

		if lastAccess < cutoff && atomic.LoadInt32(&info.refs) == 0 {
			if info.mutex.TryLock() {
				s.m.Delete(key)
				info.mutex.Unlock()
			}
		}
	}
}
