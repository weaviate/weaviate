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
	processingQueue processingQueue,
	logger logrus.FieldLogger,
) (*StreamHandler, *BatchStreamingCallbacks) {
	metrics := NewBatchStreamingCallbacks(reg)

	reportingQueues := NewReportingQueues()
	streamHandler := NewStreamHandler(shutdown.HandlersCtx, shutdown.RecvWg, shutdown.SendWg, reportingQueues, processingQueue, metrics, logger)
	// batch workers set their own per-process timeout and should not be cancelled on shutdown
	StartBatchWorkers(context.Background(), shutdown.WorkersWg, numWorkers, processingQueue, reportingQueues, batchHandler, logger)

	return streamHandler, metrics
}
