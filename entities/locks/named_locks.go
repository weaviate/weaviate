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

package locks

import "sync"

type NamedLocks struct {
	main  *sync.Mutex
	locks map[string]*sync.Mutex
}

func NewNamedLocks() *NamedLocks {
	return &NamedLocks{
		main:  new(sync.Mutex),
		locks: make(map[string]*sync.Mutex),
	}
}

func (l *NamedLocks) Lock(name string) {
	l.lock(name).Lock()
}

func (l *NamedLocks) Unlock(name string) {
	l.lock(name).Unlock()
}

func (l *NamedLocks) Locked(name string, callback func()) {
	l.Lock(name)
	defer l.Unlock(name)

	callback()
}

func (l *NamedLocks) lock(name string) *sync.Mutex {
	l.main.Lock()
	defer l.main.Unlock()

	if _, ok := l.locks[name]; !ok {
		l.locks[name] = new(sync.Mutex)
	}
	return l.locks[name]
}

type NamedRWLocks struct {
	main  *sync.Mutex
	locks map[string]*sync.RWMutex
}

func NewNamedRWLocks() *NamedRWLocks {
	return &NamedRWLocks{
		main:  new(sync.Mutex),
		locks: make(map[string]*sync.RWMutex),
	}
}

func (l *NamedRWLocks) Lock(name string) {
	l.lock(name).Lock()
}

func (l *NamedRWLocks) Unlock(name string) {
	l.lock(name).Unlock()
}

func (l *NamedRWLocks) Locked(name string, callback func()) {
	l.Lock(name)
	defer l.Unlock(name)

	callback()
}

func (l *NamedRWLocks) RLock(name string) {
	l.lock(name).RLock()
}

func (l *NamedRWLocks) RUnlock(name string) {
	l.lock(name).RUnlock()
}

func (l *NamedRWLocks) RLocked(name string, callback func()) {
	l.RLock(name)
	defer l.RUnlock(name)

	callback()
}

func (l *NamedRWLocks) lock(name string) *sync.RWMutex {
	l.main.Lock()
	defer l.main.Unlock()

	if _, ok := l.locks[name]; !ok {
		l.locks[name] = new(sync.RWMutex)
	}
	return l.locks[name]
}
