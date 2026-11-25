//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import "sync"

// evictingKeyRWLocker is a read-write lock per key whose entry is dropped once no caller holds or waits on it.
type evictingKeyRWLocker struct {
	mu      sync.Mutex
	entries map[string]*keyRWEntry
}

type keyRWEntry struct {
	sync.RWMutex
	refs int
}

func newEvictingKeyRWLocker() *evictingKeyRWLocker {
	return &evictingKeyRWLocker{entries: map[string]*keyRWEntry{}}
}

func (l *evictingKeyRWLocker) Lock(key string)    { l.acquire(key).Lock() }
func (l *evictingKeyRWLocker) RLock(key string)   { l.acquire(key).RLock() }
func (l *evictingKeyRWLocker) Unlock(key string)  { l.release(key, (*sync.RWMutex).Unlock) }
func (l *evictingKeyRWLocker) RUnlock(key string) { l.release(key, (*sync.RWMutex).RUnlock) }

func (l *evictingKeyRWLocker) acquire(key string) *keyRWEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.entries[key]
	if e == nil {
		e = &keyRWEntry{}
		l.entries[key] = e
	}
	e.refs++
	return e
}

func (l *evictingKeyRWLocker) release(key string, unlock func(*sync.RWMutex)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.entries[key]
	unlock(&e.RWMutex)
	if e.refs--; e.refs == 0 {
		delete(l.entries, key)
	}
}
