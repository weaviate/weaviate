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
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

type Drain func()

// Start initializes the batch processing system by setting up the necessary components.
//
// It creates a stream handler for managing incoming batch requests, starts a specified number of
// worker goroutines for processing these requests, and returns both the stream handler and a drain
// function to gracefully shut down the system when needed.
//
// The drain function ensures that all ongoing processes are completed before the system is fully shut down,
// preventing data loss or corruption, and should be called in the server.PreShutdown hook before the HTTP/gRPC
// servers have been gracefully stopped themselves.
func Start(
	authenticator authenticator,
	authorizer authorization.Authorizer,
	batchHandler batcher,
	reg prometheus.Registerer,
	numWorkers int,
	logger logrus.FieldLogger,
) (*StreamHandler, Drain) {
	recvWg := sync.WaitGroup{}
	sendWg := sync.WaitGroup{}
	workersWg := sync.WaitGroup{}

	shuttingDownCtx, triggerShuttingDown := context.WithCancel(context.Background())
	reportingQueues := NewReportingQueues()
	processingQueue := NewProcessingQueue(numWorkers)

	enqueuedObjectsCounter := atomic.Int32{}
	metrics := NewBatchStreamingMetrics(reg)
	StartBatchWorkers(&workersWg, numWorkers, processingQueue, reportingQueues, batchHandler, &enqueuedObjectsCounter, metrics, logger)
	handler := NewStreamHandler(
		authenticator,
		authorizer,
		shuttingDownCtx,
		&recvWg,
		&sendWg,
		reportingQueues,
		processingQueue,
		&enqueuedObjectsCounter,
		metrics,
		logger,
	)

	drain := func() {
		drain(
			triggerShuttingDown,
			&recvWg,
			processingQueue,
			&workersWg,
			&sendWg,
			logger,
		)
	}

	return handler, drain
}
