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

package global

import (
	"sync"
	"sync/atomic"
	"time"
)

var (
	State     *StateType
	StateOnce sync.Once
)

func Manager() *StateType {
	StateOnce.Do(func() {
		State = &StateType{}
	})
	return State
}

type StateType struct {
	rejectRequests     atomic.Bool
	shutdownInProgress atomic.Bool
}

// Sets the shutdown in progress flag to true.  There will be a delay of 5 seconds,
func (s *StateType) StartShutdown() {
	s.rejectRequests.Store(true)
	go func() {
		time.Sleep(5*time.Second)
		s.shutdownInProgress.Store(true)
	}()
}

func (s *StateType) IsShutdownInProgress() bool {
	return s.shutdownInProgress.Load()
}

func (s *StateType) RejectRequests() bool {
	return s.rejectRequests.Load()
}
