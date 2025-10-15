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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
		mockBatcher := mocks.NewMockbatcher(t)

		reportingQueues := NewReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := NewProcessingQueue(1)

		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		mockBatcher.EXPECT().BatchReferences(mock.Anything, mock.Anything).Return(&pb.BatchReferencesReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		var wg sync.WaitGroup
		StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, mockBatcher, logger)

		// Send data
		wg.Add(2)
		processingQueue <- &processRequest{
			objects:          []*pb.BatchObject{{}},
			references:       nil,
			streamId:         StreamId,
			consistencyLevel: nil,
			wg:               &wg,
			streamCtx:        ctx,
		}
		processingQueue <- &processRequest{
			objects:          nil,
			references:       []*pb.BatchReference{{}},
			streamId:         StreamId,
			consistencyLevel: nil,
			wg:               &wg,
			streamCtx:        ctx,
		}
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})

	t.Run("should process from the queue and send data returning partial error", func(t *testing.T) {
		mockBatcher := mocks.NewMockbatcher(t)

		reportingQueues := NewReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := NewProcessingQueue(1)

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
		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: errorsObj,
		}, nil).Times(1)
		// Verify that the retriable error is sent again and no error is returned this time
		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		// Return one regular error for references
		mockBatcher.EXPECT().BatchReferences(mock.Anything, mock.Anything).Return(&pb.BatchReferencesReply{
			Took:   float32(1),
			Errors: errorsRefs,
		}, nil).Times(1)
		var wg sync.WaitGroup
		StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, mockBatcher, logger)

		// Send data
		obj := &pb.BatchObject{}
		ref := &pb.BatchReference{}
		// must use goroutine to avoid deadlock due to one worker sending error over read stream
		// while next send to processing queue is blocked by there only being one worker
		wg.Add(1)
		go func() {
			processingQueue <- &processRequest{
				objects:          []*pb.BatchObject{obj, obj, obj},
				references:       []*pb.BatchReference{ref, ref},
				streamId:         StreamId,
				consistencyLevel: nil,
				wg:               &wg,
				streamCtx:        ctx,
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
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})
}
