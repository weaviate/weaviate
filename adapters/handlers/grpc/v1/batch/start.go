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
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func Start(
	authenticator authenticator,
	authorizer authorization.Authorizer,
	batchHandler Batcher,
	reg prometheus.Registerer,
	shutdown *Shutdown,
	numWorkers int,
	processingQueue processingQueue,
	logger logrus.FieldLogger,
) *StreamHandler {
	reportingQueues := NewReportingQueues()
	StartBatchWorkers(shutdown.WorkersWg, numWorkers, processingQueue, reportingQueues, batchHandler, logger)
	return NewStreamHandler(authenticator, authorizer, shutdown.HandlersCtx, shutdown.RecvWg, shutdown.SendWg, reportingQueues, processingQueue, NewBatchStreamingMetrics(reg), logger)
}
