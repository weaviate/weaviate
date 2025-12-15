//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sync

import "sync"

type ReadPreferringRWMutex struct {
	mainlock *sync.RWMutex
	trylock  *sync.Mutex
	ch       chan struct{}
}

func NewReadPreferringRWMutex() *ReadPreferringRWMutex {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}

	return &ReadPreferringRWMutex{
		mainlock: new(sync.RWMutex),
		trylock:  new(sync.Mutex),
		ch:       ch,
	}
}

func (m *ReadPreferringRWMutex) Lock() {
	for {
		<-m.ch
		// If multiple TryLocks calls are made in parallel, it may happen
		// that none of them will acquire the lock despite lock being free.
		// For that reason aux lock was added to prevent concurrent TryLock executions.
		m.trylock.Lock()
		if m.mainlock.TryLock() {
			m.trylock.Unlock()
			return
		}
		m.trylock.Unlock()
	}
}

func (m *ReadPreferringRWMutex) Unlock() {
	m.mainlock.Unlock()
	m.notify()
}

func (m *ReadPreferringRWMutex) TryLock() bool {
	return m.mainlock.TryLock()
}

func (m *ReadPreferringRWMutex) RLock() {
	m.mainlock.RLock()
}

func (m *ReadPreferringRWMutex) RUnlock() {
	m.mainlock.RUnlock()
	m.notify()
}

func (m *ReadPreferringRWMutex) TryRLock() bool {
	return m.mainlock.TryRLock()
}

func (m *ReadPreferringRWMutex) notify() {
	select {
	case m.ch <- struct{}{}:
	default:
	}
}
