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

// evictingKeyLocker is a lock per key whose entry is dropped once no caller holds or waits on it.
type evictingKeyLocker struct {
	mu      sync.Mutex
	entries map[string]*keyEntry
}

type keyEntry struct {
	sync.Mutex
	refs int
}

func newEvictingKeyLocker() *evictingKeyLocker {
	return &evictingKeyLocker{entries: map[string]*keyEntry{}}
}

func (l *evictingKeyLocker) Lock(key string) { l.acquire(key).Lock() }

func (l *evictingKeyLocker) acquire(key string) *keyEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.entries[key]
	if e == nil {
		e = &keyEntry{}
		l.entries[key] = e
	}
	e.refs++
	return e
}

func (l *evictingKeyLocker) Unlock(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.entries[key]
	e.Unlock()
	if e.refs--; e.refs == 0 {
		delete(l.entries, key)
	}
}
