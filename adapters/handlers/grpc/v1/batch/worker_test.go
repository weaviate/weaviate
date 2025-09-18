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

		readQueues := batch.NewBatchReadQueues()
		readQueues.Make(StreamId)
		processingQueue := batch.NewBatchProcessingQueue(1)
		reportingQueue := batch.NewBatchReportingQueue(100) // don't care about blocking here

		mockBatcher.EXPECT().BatchObjects(ctx, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		mockBatcher.EXPECT().BatchReferences(ctx, mock.Anything).Return(&pb.BatchReferencesReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		var wg sync.WaitGroup
		batch.StartBatchWorkers(ctx, &wg, 1, processingQueue, reportingQueue, readQueues, mockBatcher, logger)

		// Send data
		processingQueue <- batch.NewProcessRequest(
			[]*pb.BatchObject{{}},
			nil,
			StreamId,
			false,
			nil,
		)
		processingQueue <- batch.NewProcessRequest(
			nil,
			[]*pb.BatchReference{{}},
			StreamId,
			false,
			nil,
		)

		// Send sentinel
		processingQueue <- batch.NewProcessRequest(
			nil,
			nil,
			StreamId,
			true,
			nil,
		)

		// Accept the stop message
		ch, ok := readQueues.Get(StreamId)
		require.True(t, ok, "Expected read queue to exist and to contain message")
		_, ok = <-ch
		require.False(t, ok, "Expected read queue to be closed")

		cancel()               // Cancel the context to stop the worker loop
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
		require.Empty(t, ch, "Expected read queue to be empty after processing")
		require.Equal(t, ctx.Err(), context.Canceled, "Expected context to be canceled")
	})

	t.Run("should process from the queue during shutdown", func(t *testing.T) {
		mockBatcher := mocks.NewMockBatcher(t)

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		readQueues := batch.NewBatchReadQueues()
		readQueues.Make(StreamId)
		processingQueue := batch.NewBatchProcessingQueue(1)
		reportingQueue := batch.NewBatchReportingQueue(100) // don't care about blocking here

		mockBatcher.EXPECT().BatchObjects(ctx, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		mockBatcher.EXPECT().BatchReferences(ctx, mock.Anything).Return(&pb.BatchReferencesReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		var wg sync.WaitGroup
		batch.StartBatchWorkers(ctx, &wg, 1, processingQueue, reportingQueue, readQueues, mockBatcher, logger)

		cancel() // Cancel the context to simulate shutdown
		// Send data after context cancellation to ensure that the worker processes it
		// in its shutdown select-case
		processingQueue <- batch.NewProcessRequest(
			[]*pb.BatchObject{{}},
			nil,
			StreamId,
			false,
			nil,
		)
		processingQueue <- batch.NewProcessRequest(
			nil,
			[]*pb.BatchReference{{}},
			StreamId,
			false,
			nil,
		)
		// Send sentinel
		processingQueue <- batch.NewProcessRequest(
			nil,
			nil,
			StreamId,
			true,
			nil,
		)
		close(processingQueue) // Close the internal queue to stop processing as part of the shutdown

		// Accept the stop message
		ch, ok := readQueues.Get(StreamId)
		require.True(t, ok, "Expected read queue to exist and to contain message")
		_, ok = <-ch
		require.False(t, ok, "Expected read queue to be closed")

		wg.Wait() // Wait for the worker to finish processing
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
		require.Empty(t, ch, "Expected read queue to be empty after processing")
		require.Equal(t, ctx.Err(), context.Canceled, "Expected context to be canceled")
	})

	t.Run("should process from the queue and send data returning partial error", func(t *testing.T) {
		mockBatcher := mocks.NewMockBatcher(t)

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		readQueues := batch.NewBatchReadQueues()
		readQueues.Make(StreamId)
		processingQueue := batch.NewBatchProcessingQueue(1)
		reportingQueue := batch.NewBatchReportingQueue(100) // don't care about blocking here

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
		batch.StartBatchWorkers(ctx, &wg, 1, processingQueue, reportingQueue, readQueues, mockBatcher, logger)

		// Send data
		obj := &pb.BatchObject{}
		// must use goroutine to avoid deadlock due to one worker sending error over read stream
		// while next send to processing queue is blocked by there only being one worker
		go func() {
			processingQueue <- batch.NewProcessRequest(
				[]*pb.BatchObject{obj, obj, obj},
				nil,
				StreamId,
				false,
				nil,
			)
			ref := &pb.BatchReference{}
			processingQueue <- batch.NewProcessRequest(
				nil,
				[]*pb.BatchReference{ref, ref},
				StreamId,
				false,
				nil,
			)

			// Send sentinel
			processingQueue <- batch.NewProcessRequest(
				nil,
				nil,
				StreamId,
				true,
				nil,
			)
		}()

		ch, ok := readQueues.Get(StreamId)
		require.True(t, ok, "Expected read queue to exist and to contain message")

		// Read first error
		errs := <-ch
		require.NotNil(t, errs.Errors, "Expected errors to be returned")
		require.Len(t, errs.Errors, 1, "Expected one error to be returned")
		require.Equal(t, "objs error", errs.Errors[0].Error, "Expected error message to match")
		require.Equal(t, obj, errs.Errors[0].GetObject(), "Expected object to match")

		// Read second error
		errs = <-ch
		require.NotNil(t, errs.Errors, "Expected errors to be returned")
		require.Len(t, errs.Errors, 1, "Expected one error to be returned")
		require.Equal(t, "refs error", errs.Errors[0].Error, "Expected error message to match")

		// Read sentinel
		_, ok = <-ch
		require.False(t, ok, "Expected read queue to be closed")

		cancel()               // Cancel the context to stop the worker loop
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
		require.Empty(t, ch, "Expected read queue to be empty after processing")
		require.Equal(t, ctx.Err(), context.Canceled, "Expected context to be canceled")
	})
}
