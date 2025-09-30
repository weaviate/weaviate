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
	batchMetrics := NewBatchStreamingCallbacks(reg)

	processingQueue := NewBatchProcessingQueue(numWorkers)
	reportingQueue := NewBatchReportingQueue(numWorkers)
	batchWriteQueues := NewBatchWriteQueues(numWorkers)
	batchReadQueues := NewBatchReadQueues()

	batchStreamHandler := NewStreamHandler(shutdown.HandlersCtx, shutdown.RecvWg, shutdown.SendWg, batchWriteQueues, batchReadQueues, batchMetrics, logger)

	StartBatchWorkers(shutdown.WorkersCtx, shutdown.WorkersWg, numWorkers, processingQueue, reportingQueue, batchReadQueues, batchHandler, logger)
	StartScheduler(shutdown.SchedulerCtx, shutdown.SchedulerWg, batchWriteQueues, processingQueue, reportingQueue, batchMetrics, logger)

	return batchStreamHandler, batchMetrics
}
