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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

type Drain func()

func Start(
	authenticator authenticator,
	authorizer authorization.Authorizer,
	batchHandler Batcher,
	reg prometheus.Registerer,
	numWorkers int,
	logger logrus.FieldLogger,
) (*StreamHandler, Drain) {
	recvWg := sync.WaitGroup{}
	sendWg := sync.WaitGroup{}
	workersWg := sync.WaitGroup{}

	shuttingDownCtx, triggerShuttingDown := context.WithCancel(context.Background())
	reportingQueues := NewReportingQueues()
	// buffer size of 10x workers helping to ensure minimal overhead between recv and workers while not using too much memory
	// nor delaying shutdown too much by requiring a long drain period
	processingQueue := NewProcessingQueue(numWorkers * 10)

	StartBatchWorkers(&workersWg, numWorkers, processingQueue, reportingQueues, batchHandler, logger)

	handler := NewStreamHandler(
		authenticator,
		authorizer,
		shuttingDownCtx,
		&recvWg,
		&sendWg,
		reportingQueues,
		processingQueue,
		NewBatchStreamingMetrics(reg),
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
