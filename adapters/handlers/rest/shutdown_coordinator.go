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

package rest

import (
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

// ShutdownCoordinator provides coordinated shutdown state management for HTTP and gRPC servers.
//
// This coordinator solves the race condition problem where APIs continue serving requests
// during shutdown because HTTP servers, gRPC servers, and health checks shut down without
// proper coordination. It provides a single source of truth for shutdown state that can be
// queried by readiness probes, middleware, and other components.
//
// The coordinator uses atomic operations for thread-safe state management, ensuring that
// shutdown state changes are immediately visible across all goroutines without locks.
type ShutdownCoordinator struct {
	shuttingDown int32 // 0 = running, 1 = shutting down (accessed atomically)
	logger       *logrus.Logger
}

// NewShutdownCoordinator creates a new shutdown coordinator with the specified logger.
//
// The coordinator starts in a "running" state and will only transition to "shutting down"
// when NotifyShutdown is called. Once in the shutdown state, it cannot transition back to running.
//
// Parameters:
//
//	logger: Used for logging shutdown state transitions and debugging information
//
// Returns:
//
//	A new ShutdownCoordinator ready for use
func NewShutdownCoordinator(logger *logrus.Logger) *ShutdownCoordinator {
	return &ShutdownCoordinator{
		logger: logger,
	}
}

// IsShuttingDown reports whether a shutdown has been initiated.
//
// This method implements the state.ShutdownTracker interface and is the primary
// way for components to check shutdown state. It uses atomic operations for
// thread-safe access without locks.
//
// Returns:
//
//	true if NotifyShutdown has been called, false otherwise
func (sc *ShutdownCoordinator) IsShuttingDown() bool {
	return atomic.LoadInt32(&sc.shuttingDown) == 1
}

// NotifyShutdown transitions the coordinator to the shutdown state.
//
// This method should be called once during the shutdown initiation phase, typically
// in the PreServerShutdown hook. It atomically sets the shutdown state and logs
// the transition. Subsequent calls are safe but have no effect.
//
// The state transition is permanent - once shutdown is activated, the coordinator
// will always report IsShuttingDown() as true for the lifetime of the instance.
func (sc *ShutdownCoordinator) NotifyShutdown() {
	if atomic.CompareAndSwapInt32(&sc.shuttingDown, 0, 1) {
		sc.logger.Debug("notify shutdown")
	}
}
