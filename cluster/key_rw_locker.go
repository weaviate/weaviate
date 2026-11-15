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

// keyRWLocker is a read-write lock per key whose entry is dropped once no caller holds or waits on it.
type keyRWLocker struct {
	mu      sync.Mutex
	entries map[string]*keyRWEntry
}

type keyRWEntry struct {
	sync.RWMutex
	refs int
}

func newKeyRWLocker() *keyRWLocker {
	return &keyRWLocker{entries: map[string]*keyRWEntry{}}
}

func (l *keyRWLocker) Lock(key string)    { l.acquire(key).Lock() }
func (l *keyRWLocker) RLock(key string)   { l.acquire(key).RLock() }
func (l *keyRWLocker) Unlock(key string)  { l.release(key, (*sync.RWMutex).Unlock) }
func (l *keyRWLocker) RUnlock(key string) { l.release(key, (*sync.RWMutex).RUnlock) }

func (l *keyRWLocker) acquire(key string) *keyRWEntry {
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

func (l *keyRWLocker) release(key string, unlock func(*sync.RWMutex)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.entries[key]
	unlock(&e.RWMutex)
	if e.refs--; e.refs == 0 {
		delete(l.entries, key)
	}
}
