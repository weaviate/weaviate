//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"context"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type Drain func()

type options struct {
	admissionChecker admissionChecker
	backpressure     config.BatchStream
}

type Option func(*options)

// WithAdmissionChecker replaces the admission checker that decides whether a batch
// message is accepted. The same checker reports the live heap ratio that sets
// the ack delay.
func WithAdmissionChecker(c admissionChecker) Option {
	return func(o *options) {
		o.admissionChecker = c
	}
}

// WithBackpressure replaces the backpressure configuration the receiver applies
// to incoming messages. Zero fields take their defaults.
func WithBackpressure(cfg config.BatchStream) Option {
	return func(o *options) {
		o.backpressure = cfg
	}
}

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
	schemaManager schemaManager,
	reg prometheus.Registerer,
	numWorkers int,
	logger logrus.FieldLogger,
	namespacesEnabled bool,
	opts ...Option,
) (*StreamHandler, Drain) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	backpressure := o.backpressure.WithDefaults()
	// While a receiver holds for memory, drain is stuck waiting on recvWg. The
	// hold must therefore not outlast the grace period drain allows.
	if maxHold := int(SHUTDOWN_GRACE_PERIOD / time.Second); backpressure.HoldSeconds > maxHold {
		backpressure.HoldSeconds = maxHold
	}
	if o.admissionChecker == nil {
		// The batch stream gets its own memory monitor with a lower threshold than
		// the global one (0.9 vs 0.97 of GOMEMLIMIT by default). Imports should slow
		// down before the rest of the process runs short of memory, because accepted
		// batches sit in memory until the workers drain them.
		o.admissionChecker = memwatch.NewMonitor(memwatch.LiveHeapReader, debug.SetMemoryLimit, backpressure.GateRatio)
	}

	recvWg := sync.WaitGroup{}
	sendWg := sync.WaitGroup{}
	workersWg := sync.WaitGroup{}

	shuttingDownCtx, triggerShuttingDown := context.WithCancel(context.Background())
	reportingQueues := NewReportingQueues()
	processingQueue := NewProcessingQueue()

	metrics := NewBatchStreamingMetrics(reg)
	StartBatchWorkers(&workersWg, numWorkers, processingQueue, reportingQueues, batchHandler, logger)
	handler := NewStreamHandler(
		authenticator,
		authorizer,
		shuttingDownCtx,
		triggerShuttingDown,
		&recvWg,
		&sendWg,
		reportingQueues,
		processingQueue,
		metrics,
		logger,
		schemaManager,
		namespacesEnabled,
		o.admissionChecker,
		backpressure,
	)

	drain := func() {
		drain(
			handler.stopAccepting,
			&recvWg,
			processingQueue,
			&workersWg,
			&sendWg,
			logger,
		)
	}

	return handler, drain
}
