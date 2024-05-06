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
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func mutexLocked(m *sync.Mutex) bool {
	const mLocked = 1
	state := reflect.ValueOf(m).Elem().FieldByName("state")
	return state.Int()&mLocked == mLocked
}

func TestSyncLockUnlock(t *testing.T) {
	r := require.New(t)
	s := New()

	s.Lock("t1")
	lock, _ := s.m.Load("t1")
	r.True(mutexLocked(lock.(*sync.Mutex)))

	s.Unlock("t1")
	lock, _ = s.m.Load("t1")
	r.False(mutexLocked(lock.(*sync.Mutex)))

	s.Lock("t2")
	lock, _ = s.m.Load("t2")
	r.True(mutexLocked(lock.(*sync.Mutex)))

	s.Unlock("t2")
	lock, _ = s.m.Load("t2")
	r.False(mutexLocked(lock.(*sync.Mutex)))
}
