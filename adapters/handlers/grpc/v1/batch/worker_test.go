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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/replica"
)

var StreamId string = "329c306b-c912-4ec7-9b1d-55e5e0ca8dea"

func TestWorkerLoop(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	logger := logrus.New()

	t.Run("should process from the queue and send data without error", func(t *testing.T) {
		mockBatcher := mocks.NewMockBatcher(t)

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		reportingQueues := batch.NewBatchReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := batch.NewBatchProcessingQueue(1)
		listeningQueue := batch.NewListeningQueue()

		mockBatcher.EXPECT().BatchObjects(ctx, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		mockBatcher.EXPECT().BatchReferences(ctx, mock.Anything).Return(&pb.BatchReferencesReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		var wg sync.WaitGroup
		batch.StartBatchWorkers(ctx, &wg, 1, processingQueue, reportingQueues, listeningQueue, mockBatcher, logger)

		go func() {
			for range listeningQueue {
			}
		}()

		// Send data
		processingQueue <- batch.NewProcessRequest(
			[]*pb.BatchObject{{}},
			nil,
			StreamId,
			nil,
		)
		processingQueue <- batch.NewProcessRequest(
			nil,
			[]*pb.BatchReference{{}},
			StreamId,
			nil,
		)
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		close(listeningQueue)
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})

	t.Run("should process from the queue and send data returning partial error", func(t *testing.T) {
		mockBatcher := mocks.NewMockBatcher(t)

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		reportingQueues := batch.NewBatchReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := batch.NewBatchProcessingQueue(1)
		listeningQueue := batch.NewListeningQueue()

		errorsObj := []*pb.BatchObjectsReply_BatchError{
			{
				Error: replica.ErrReplicas.Error(),
				Index: 0,
			},
			{
				Error: "objs error",
				Index: 1,
			},
		}
		errorsRefs := []*pb.BatchReferencesReply_BatchError{
			{
				Error: "refs error",
				Index: 0,
			},
		}
		// Return one retriable error and one regular error for objects
		mockBatcher.EXPECT().BatchObjects(ctx, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: errorsObj,
		}, nil).Times(1)
		// Verify that the retriable error is sent again and no error is returned this time
		mockBatcher.EXPECT().BatchObjects(ctx, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		// Return one regular error for references
		mockBatcher.EXPECT().BatchReferences(ctx, mock.Anything).Return(&pb.BatchReferencesReply{
			Took:   float32(1),
			Errors: errorsRefs,
		}, nil).Times(1)
		var wg sync.WaitGroup
		batch.StartBatchWorkers(ctx, &wg, 1, processingQueue, reportingQueues, listeningQueue, mockBatcher, logger)

		// Send data
		obj := &pb.BatchObject{}
		ref := &pb.BatchReference{}
		// must use goroutine to avoid deadlock due to one worker sending error over read stream
		// while next send to processing queue is blocked by there only being one worker
		go func() {
			processingQueue <- batch.NewProcessRequest(
				[]*pb.BatchObject{obj, obj, obj},
				[]*pb.BatchReference{ref, ref},
				StreamId,
				nil,
			)
			for range listeningQueue {
			}
		}()

		rq, ok := reportingQueues.Get(StreamId)
		require.True(t, ok, "Expected reporting queue to exist and to contain message")

		// Read first report from worker
		report := <-rq
		require.NotNil(t, report.Errors, "Expected errors to be returned")
		require.NotNil(t, report.Stats, "Expected stats to be returned")
		require.Len(t, report.Errors, 2, "Expected two errors to be returned")
		require.Equal(t, "objs error", report.Errors[0].Error, "Expected error message to match")
		require.Equal(t, obj, report.Errors[0].GetObject(), "Expected object to match")
		require.Equal(t, "refs error", report.Errors[1].Error, "Expected error message to match")
		require.Equal(t, ref, report.Errors[1].GetReference(), "Expected reference to match")

		require.Empty(t, rq, "Expected reporting queue to be empty after reading all messages")
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		close(listeningQueue)
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})
}
