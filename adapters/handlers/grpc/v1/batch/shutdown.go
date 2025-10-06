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

package batch

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

type Shutdown struct {
	HandlersCtx    context.Context
	HandlersCancel context.CancelFunc
	RecvWg         *sync.WaitGroup
	SendWg         *sync.WaitGroup
	WorkersWg      *sync.WaitGroup
}

func NewShutdown(ctx context.Context) *Shutdown {
	var recvWg sync.WaitGroup
	var sendWg sync.WaitGroup
	var workersWg sync.WaitGroup

	hCtx, hCancel := context.WithCancel(ctx)

	return &Shutdown{
		HandlersCtx:    hCtx,
		HandlersCancel: hCancel,
		RecvWg:         &recvWg,
		SendWg:         &sendWg,
		WorkersWg:      &workersWg,
	}
}

// Drain handles the graceful shutdown of all batch processing components.
//
// The order of operations needs to be as follows to ensure that there are no missed objects/references in any of the
// write queues nor any missed errors in the read queues:
//
// 1. Stop accepting new requests in the handlers
//   - This prevents new requests from being added to the system while we are shutting down
//
// 2. Wait for all in-flight Send requests to finish
//   - This ensures that the write queues are no longer being written to
//
// 3. Stop the scheduler loop and drain the write queues
//   - This ensures that all currently waiting write objects/references are added to the internal queues
//
// 4. Stop the worker loops and drain the internal queue
//   - This ensures that all currently waiting batch requests in the internal queue are processed
//
// 5. Signal shutdown complete and wait for all streams to communicate this to clients
//   - This ensures that all clients have acknowledged shutdown so that they can successfully reconnect to another node
//
// The gRPC shutdown is then considered complete as every queue has been drained successfully so the server
// can move onto switching off the HTTP handlers and shutting itself down completely.
func (s *Shutdown) Drain(logger logrus.FieldLogger) {
	log := logger.WithField("action", "shutdown_drain")
	// stop handlers first
	s.HandlersCancel()
	log.Info("wait for all receivers to finish adding to processing queue")
	s.RecvWg.Wait()
	log.Info("all receivers finished")
	// wait for all workers to finish
	log.Info("waiting for all workers to finish processing internal queue")
	// wait for all the objects to be processed from the internal queue
	s.WorkersWg.Wait()
	log.Info("finished draining the internal queues")
	log.Info("waiting for all sends to exit")
	// wait for all streams to exit, i.e. be hungup by their clients
	s.SendWg.Wait()
	log.Info("all senders exited, shutdown complete")
}
