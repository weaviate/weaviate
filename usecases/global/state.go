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
	"sync/atomic"
	"time"
	"fmt"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

var (
	state     *StateType = &StateType{}
	ErrServerShuttingDown = fmt.Errorf("server is shutting down")
)

func Manager() *StateType {
	return state
}

type StateType struct {
	rejectRequests     atomic.Bool
	shutdownInProgress atomic.Bool
}

// Sets the shutdown in progress flag to true.  There will be a delay of 5 seconds,
func (s *StateType) StartShutdown() {
	s.rejectRequests.Store(true)
	enterrors.GoWrapper(func() {
		time.Sleep(5 * time.Second)
		s.shutdownInProgress.Store(true)
	}, nil)
}

// This becomes true 5 seconds after shutdown is called.  This is typically checked before write operations, allowing weaviate to cancel running requests without file corruption.  Batches will be cancelled halfway through, etc.
func (s *StateType) IsShutdownInProgress() bool {
	return s.shutdownInProgress.Load()
}

// Immediately when shutdown is called, the server will stop accepting new requests.  The rest of the server continues running normally, allowing current requests to finish.  If weaviate does not shutdown within 5 seconds, it will start cancelling all running requests(see shutdownInProgress).
//
// This is typically checked at the "top level" of the server, before any processing is started.
func (s *StateType) ShouldRejectRequests() bool {
	return s.rejectRequests.Load()
}
