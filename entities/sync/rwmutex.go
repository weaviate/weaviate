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

package sync

import "sync"

type ReadPreferringRWMutex struct {
	lock *sync.RWMutex
	ch   chan struct{}
}

func NewReadPreferringRWMutex() *ReadPreferringRWMutex {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}

	return &ReadPreferringRWMutex{
		lock: new(sync.RWMutex),
		ch:   ch,
	}
}

func (m *ReadPreferringRWMutex) Lock() {
	for {
		<-m.ch
		if m.lock.TryLock() {
			return
		}
	}
}

func (m *ReadPreferringRWMutex) Unlock() {
	m.lock.Unlock()
	m.notify()
}

func (m *ReadPreferringRWMutex) TryLock() bool {
	return m.lock.TryLock()
}

func (m *ReadPreferringRWMutex) RLock() {
	m.lock.RLock()
}

func (m *ReadPreferringRWMutex) RUnlock() {
	m.lock.RUnlock()
	m.notify()
}

func (m *ReadPreferringRWMutex) TryRLock() bool {
	return m.lock.TryRLock()
}

func (m *ReadPreferringRWMutex) notify() {
	select {
	case m.ch <- struct{}{}:
	default:
	}
}
