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

func TestSchedulerLoop(t *testing.T) {
	ctx := context.Background()

	logger := logrus.New()

	t.Run("dynamic", func(t *testing.T) {
		shutdownCtx, shutdownCancel := context.WithCancel(ctx)
		defer shutdownCancel()

		writeQueues := batch.NewBatchWriteQueues(1)
		readQueues := batch.NewBatchReadQueues()
		processingQueue := batch.NewBatchProcessingQueue(1)
		reportingQueue := batch.NewBatchReportingQueue(1)

		writeQueues.Make(StreamId, nil)
		readQueues.Make(StreamId)
		var wg sync.WaitGroup
		batch.StartScheduler(shutdownCtx, &wg, writeQueues, readQueues, processingQueue, reportingQueue, nil, logger)

		wq, ok := writeQueues.GetQueue(StreamId)
		require.True(t, ok, "Expected write queue to exist")
		rq, ok := readQueues.Get(StreamId)
		require.True(t, ok, "Expected read queue to exist")

		obj := &pb.BatchObject{}
		wq <- batch.NewWriteObject(obj)

		require.Eventually(t, func() bool {
			select {
			case receivedObj := <-processingQueue:
				return receivedObj.Objects.Values[0] == obj
			default:
				return false
			}
		}, 1*time.Second, 10*time.Millisecond, "Expected object to be sent to processing queue")

		// Simulate worker reporting back to scheduler
		reportingQueue <- batch.NewWorkersStats(100*time.Millisecond, StreamId)

		shutdownCancel() // Trigger shutdown
		close(wq)        // Close the write queue as part of shutdown
		wg.Wait()        // Wait for the scheduler to finish

		require.Empty(t, processingQueue, "Expected processing queue to be empty after shutdown; %d items left", len(processingQueue))
		_, ok = <-processingQueue
		require.False(t, ok, "Expected processing queue to be closed")
		select {
		case _, ok = <-rq:
			require.False(t, ok, "Expected read queue to be closed")
		default:
			t.Fatal("Expected read queue to be closed, but it is not")
		}
		require.False(t, ok, "Expected read queue to be closed")
		require.Equal(t, context.Canceled, shutdownCtx.Err(), "Expected context to be canceled")
	})
}
