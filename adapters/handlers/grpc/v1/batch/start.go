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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/auth"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func Start(
	authenticator *auth.Handler,
	authorization authorization.Authorizer,
	batchHandler Batcher,
	reg prometheus.Registerer,
	shutdown *Shutdown,
	numWorkers int,
	logger logrus.FieldLogger,
) (*StreamHandler, *BatchStreamingCallbacks) {
	metrics := NewBatchStreamingCallbacks(reg)

	internalBuffer := 10 * numWorkers // Higher numbers make shutdown slower but lower numbers make throughput worse
	processingQueue := NewBatchProcessingQueue(internalBuffer)
	reportingQueue := NewBatchReportingQueue(internalBuffer)
	writeQueues := NewBatchWriteQueues(numWorkers)
	readQueues := NewBatchReadQueues()

	streamHandler := NewStreamHandler(shutdown.HandlersCtx, shutdown.RecvWg, shutdown.SendWg, writeQueues, readQueues, metrics, logger)
	StartScheduler(shutdown.SchedulerCtx, shutdown.SchedulerWg, writeQueues, readQueues, processingQueue, reportingQueue, metrics, logger)
	StartBatchWorkers(shutdown.WorkersCtx, shutdown.WorkersWg, numWorkers, processingQueue, reportingQueue, readQueues, batchHandler, logger)

	return streamHandler, metrics
}
