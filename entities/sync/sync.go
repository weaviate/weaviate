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

// Package sync provides thread-safe synchronization primitives for key-based locking.
// It implements a memory-efficient key-based locking mechanism with automatic cleanup
// of unused locks to prevent memory leaks. The package is particularly useful when
// you need to synchronize access to resources identified by string keys.
package sync

import (
	"sync"
	"sync/atomic"
	"time"
)

type lockEntry struct {
	lock     sync.Mutex
	lastUsed atomic.Int64 // Unix nano for atomic access
}

// KeyLocker provides thread-safe locking by key with automatic resource cleanup.
// It maintains a bounded number of locks and automatically removes unused ones
// based on both size limits and time-based expiration.
//
// Features:
//   - Thread-safe key-based locking
//   - Automatic cleanup of unused locks
//   - Size-based eviction (LRU)
//   - Time-based expiration
//   - Memory leak prevention
type KeyLocker struct {
	locks   sync.Map
	maxAge  time.Duration // maximum age of unused locks
	count   atomic.Int32  // current count of locks
	maxSize int32         // maximum number of locks
}

// NewKeyLocker creates a new KeyLocker with default settings.
// It initializes a KeyLocker with:
//   - Maximum size of 1000 locks
//   - Maximum age of 24 hours for unused locks
//   - Automatic cleanup every 5 minutes
//
// The created KeyLocker automatically starts a background goroutine
// for periodic cleanup of expired locks.
func NewKeyLocker() *KeyLocker {
	kl := &KeyLocker{
		maxSize: 1000,           // default max size
		maxAge:  24 * time.Hour, // default max age
	}
	go kl.periodicCleanup(5 * time.Minute) // cleanup every 5 minutes
	return kl
}

// Lock acquires a lock for the given key. If the key doesn't exist,
// a new lock is created. If the maximum size is reached, the oldest
// unused lock is evicted before creating a new one.
//
// The lock is guaranteed to be held when this method returns. The caller
// must ensure they call Unlock when they're done with the lock.
//
// If the key already exists, its last-used timestamp is updated,
// preventing it from being cleaned up while in use.
func (kl *KeyLocker) Lock(key string) {
	now := time.Now().UnixNano()

	// Check if we already have this lock
	if value, ok := kl.locks.Load(key); ok {
		entry := value.(*lockEntry)
		entry.lastUsed.Store(now)
		entry.lock.Lock()
		return
	}

	// If we're at capacity, try to remove the oldest entry
	for kl.count.Load() >= kl.maxSize {
		var oldestKey interface{}
		var oldestTime int64 = now + 1 // Initialize to future time

		// Find the oldest entry
		kl.locks.Range(func(k, v interface{}) bool {
			entry := v.(*lockEntry)
			lastUsed := entry.lastUsed.Load()
			if lastUsed < oldestTime && !isLocked(&entry.lock) {
				oldestTime = lastUsed
				oldestKey = k
			}
			return true
		})

		// Break if we couldn't find any unlocked entries
		if oldestKey == nil {
			break
		}

		// Try to remove the oldest entry
		kl.locks.Delete(oldestKey)
		kl.count.Add(-1)
	}

	// Create and store new entry
	entry := &lockEntry{}
	entry.lastUsed.Store(now)

	actual, loaded := kl.locks.LoadOrStore(key, entry)
	if loaded {
		entry = actual.(*lockEntry)
		entry.lastUsed.Store(now)
	} else {
		kl.count.Add(1)
	}
	entry.lock.Lock()
}

// Unlock releases the lock for the given key.
// It must be called exactly once for each successful Lock call.
// Calling Unlock on an unlocked key or a key that doesn't exist
// may result in undefined behavior.
func (kl *KeyLocker) Unlock(key string) {
	value, _ := kl.locks.Load(key)
	entry := value.(*lockEntry)
	entry.lock.Unlock()
}

// Cleanup removes all locks that haven't been used for longer than maxAge.
// This method is called automatically by the periodic cleanup goroutine,
// but can also be called manually if immediate cleanup is desired.
//
// It is safe to call this method while other goroutines are using the KeyLocker.
// Only unused and expired locks will be removed.
func (kl *KeyLocker) Cleanup() {
	threshold := time.Now().Add(-kl.maxAge).UnixNano()
	removed := int32(0)

	kl.locks.Range(func(key, value interface{}) bool {
		entry := value.(*lockEntry)
		if entry.lastUsed.Load() < threshold && !isLocked(&entry.lock) {
			kl.locks.Delete(key)
			removed++
		}
		return true
	})

	if removed > 0 {
		kl.count.Add(-removed)
	}
}

func (kl *KeyLocker) periodicCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		kl.Cleanup()
	}
}

// Helper function to check if mutex is locked
func isLocked(m *sync.Mutex) bool {
	locked := !m.TryLock()
	if !locked {
		m.Unlock()
	}
	return locked
}
