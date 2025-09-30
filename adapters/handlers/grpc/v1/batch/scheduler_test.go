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

package batch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestScheduler(t *testing.T) {
	ctx := context.Background()

	logger := logrus.New()

	t.Run("dynamic", func(t *testing.T) {
		shutdownCtx, shutdownCancel := context.WithCancel(ctx)
		defer shutdownCancel()

		writeQueues := batch.NewBatchWriteQueues(1)
		processingQueue := batch.NewBatchProcessingQueue(1)
		reportingQueue := batch.NewBatchReportingQueue(1)

		writeQueues.Make("test-stream", nil)
		var wg sync.WaitGroup
		batch.StartScheduler(shutdownCtx, &wg, writeQueues, processingQueue, reportingQueue, nil, logger)

		queue, ok := writeQueues.GetQueue("test-stream")
		require.True(t, ok, "Expected write queue to exist")

		obj := &pb.BatchObject{}
		queue <- batch.NewWriteObject(obj)

		require.Eventually(t, func() bool {
			select {
			case receivedObj := <-processingQueue:
				return receivedObj.Objects.Values[0] == obj
			default:
				return false
			}
		}, 1*time.Second, 10*time.Millisecond, "Expected object to be sent to processing queue")

		shutdownCancel() // Trigger shutdown
		close(queue)     // Close the write queue as part of shutdown
		wg.Wait()        // Wait for the scheduler to finish

		// Assert that stop was sent to processing queue on shutdown
		require.Eventually(t, func() bool {
			select {
			case receivedObj := <-processingQueue:
				return receivedObj.Stop
			default:
				return false
			}
		}, 1*time.Second, 10*time.Millisecond, "Expected stop to be sent to processing queue")

		require.Empty(t, processingQueue, "Expected processing queue to be empty after shutdown")
		require.Equal(t, context.Canceled, shutdownCtx.Err(), "Expected context to be canceled")
	})
}
