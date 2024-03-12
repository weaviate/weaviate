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

import (
	"sync"

	"github.com/sirupsen/logrus"
)

// KeyLocker it is a thread safe wrapper of sync.Map
// Usage: it's used in order to lock specific key in a map
// to synchronizes concurrent access to a code block.
// locker.Lock(id)
// defer locker.Unlock(id)
type KeyLocker struct {
	m      sync.Map
	logger logrus.FieldLogger
}

// New creates Keylocker
func New(l logrus.FieldLogger) *KeyLocker {
	return &KeyLocker{
		m:      sync.Map{},
		logger: l,
	}
}

// Lock it locks a specific bucket by it's ID
// to hold ant concurrent access to that specific item
//
//	do not forget calling Unlock() after locking it.
func (s *KeyLocker) Lock(ID string) {
	iLock := &sync.Mutex{}
	iLocks, _ := s.m.LoadOrStore(ID, iLock)
	var ok bool
	iLock, ok = iLocks.(*sync.Mutex)
	if !ok {
		// shouldn't happen
		s.logger.Error("lock of non existing lock for item with ID=%s", ID)
		return
	}

	iLock.Lock()
}

// Unlock it unlocks a specific item by it's ID
// and it will delete it from the shared locks map
func (s *KeyLocker) Unlock(ID string) {
	iLocks, ok := s.m.Load(ID)
	if !ok {
		s.logger.Error("unlock of non existing lock for item with ID=%s", ID)
		return
	}
	iLock, ok := iLocks.(*sync.Mutex)
	if !ok {
		// shouldn't happen
		s.logger.Error("lock of non existing lock for item with ID=%s", ID)
		return
	}
	iLock.Unlock()
}
