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

// Drain handles the graceful shutdown of all batch processing components.
//
// The order of operations needs to be as follows to ensure that there are no missed objects/references in any of the
// processing queue nor any missed errors in the reporting queues:
//
// 1. Stop accepting creation of new streams in the handlers
//   - This prevents new streams from being added to the system while we are shutting down
//
// 2. Wait for all in-process streams to be drained (i.e. all h.recv goroutines to complete)
//   - This ensures that the processing queue is no longer being written to
//
// 3. Stop the worker loops and drain the processing queue
//   - This ensures that all currently waiting batch requests in the processing queue are processed and any errors added to the reporting queues
//
// 4. Signal shutdown complete and wait for all streams to communicate this to clients
//   - This ensures that all clients have acknowledged shutdown so that they can successfully reconnect to another node
//
// The batching shutdown is then considered complete as every queue has been drained successfully so the server
// can move onto switching off the HTTP handlers and shutting itself down completely.
func drain(triggerShuttingDown context.CancelFunc, recvWg *sync.WaitGroup, processingQueue processingQueue, workersWg *sync.WaitGroup, sendWg *sync.WaitGroup, logger logrus.FieldLogger) {
	log := logger.WithField("action", "shutdown_drain")
	// stop handlers first
	triggerShuttingDown()
	log.Info("waiting for all receivers to finish")
	// wait for all currently open h.recv goroutines to complete thereby ensuring the processing queue is no longer being written to
	recvWg.Wait()
	log.Info("all receivers finished, closing processing queue")
	// close the processing queue to signal to workers that no more requests will be coming and that they can exit
	close(processingQueue)
	log.Info("waiting for all workers to drain the processing queue")
	// wait for all the objects to be processed from the internal queue
	workersWg.Wait()
	log.Info("all workers finished, waiting for all senders to finish")
	// wait for all streams to exit, i.e. be hungup by their clients
	sendWg.Wait()
	log.Info("all senders exited, shutdown complete")
}
