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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

var StreamId string = "329c306b-c912-4ec7-9b1d-55e5e0ca8dea"

func TestWorkerLoop(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	logger := logrus.New()

	t.Run("should process separate objs & refs requests from the queue and send data without error", func(t *testing.T) {
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
		StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, mockBatcher, &atomic.Int32{}, nil, logger)

		UUID0 := uuid.New().String()
		ref1 := &pb.BatchReference{
			FromUuid:       UUID0,
			ToUuid:         uuid.New().String(),
			Name:           "ref",
			FromCollection: "Class",
		}

		// Send data
		wg.Add(2)
		processingQueue <- &processRequest{
			objects:          []*pb.BatchObject{{Uuid: UUID0}},
			references:       nil,
			streamId:         StreamId,
			consistencyLevel: nil,
			wg:               &wg,
			streamCtx:        ctx,
		}
		processingQueue <- &processRequest{
			objects:          nil,
			references:       []*pb.BatchReference{ref1},
			streamId:         StreamId,
			consistencyLevel: nil,
			wg:               &wg,
			streamCtx:        ctx,
		}

		rq, ok := reportingQueues.Get(StreamId)
		require.True(t, ok, "Expected reporting queue to exist and to contain message")

		// Read first report from worker
		report := <-rq

		require.NotNil(t, report.Successes, "Expected successes to be returned")
		require.Equal(t, 0, len(report.Errors), "Expected no errors to be returned")
		require.NotNil(t, report.Stats, "Expected stats to be returned")
		require.Len(t, report.Successes, 1, "Expected one result to be returned")

		require.Equal(t, UUID0, report.Successes[0].GetUuid(), "Expected first result's UUID to match")

		// Read second report from worker
		report = <-rq

		require.NotNil(t, report.Successes, "Expected successes to be returned")
		require.Equal(t, 0, len(report.Errors), "Expected no errors to be returned")
		require.NotNil(t, report.Stats, "Expected stats to be returned")
		require.Len(t, report.Successes, 1, "Expected one result to be returned")

		require.Equal(t, toBeacon(ref1), report.Successes[0].GetBeacon(), "Expected second result's beacon to match")

		require.Empty(t, rq, "Expected reporting queue to be empty after reading all messages")
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})

	t.Run("should process combined objs & refs request from the queue and send data returning errors", func(t *testing.T) {
		mockBatcher := mocks.NewMockbatcher(t)

		reportingQueues := NewReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := NewProcessingQueue(1)

		errorsObj := []*pb.BatchObjectsReply_BatchError{
			{
				Error: replicaerrors.NewReplicasError(nil).Error(),
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
		StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, mockBatcher, &atomic.Int32{}, nil, logger)

		// Send data
		UUID0 := uuid.New().String()
		UUID1 := uuid.New().String()
		UUID2 := uuid.New().String()
		ref1 := &pb.BatchReference{
			FromUuid:       UUID0,
			ToUuid:         UUID1,
			Name:           "ref",
			FromCollection: "Class",
		}
		ref2 := &pb.BatchReference{
			FromUuid:       UUID1,
			ToUuid:         UUID2,
			Name:           "ref",
			FromCollection: "Class",
		}
		// must use goroutine to avoid deadlock due to one worker sending error over read stream
		// while next send to processing queue is blocked by there only being one worker
		wg.Add(1)
		go func() {
			processingQueue <- &processRequest{
				objects:          []*pb.BatchObject{{Uuid: UUID0}, {Uuid: UUID1}, {Uuid: UUID2}},
				references:       []*pb.BatchReference{ref1, ref2},
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
		require.NotNil(t, report.Successes, "Expected successes to be returned")
		require.NotNil(t, report.Errors, "Expected errors to be returned")
		require.NotNil(t, report.Stats, "Expected stats to be returned")
		require.Len(t, report.Successes, 3, "Expected three successes to be returned")
		require.Len(t, report.Errors, 2, "Expected two errors to be returned")

		require.Equal(t, UUID0, report.Successes[0].GetUuid(), "Expected first success' UUID to match")
		require.Equal(t, UUID2, report.Successes[1].GetUuid(), "Expected second success' UUID to match")
		require.Equal(t, toBeacon(ref2), report.Successes[2].GetBeacon(), "Expected third success' beacon to match")

		require.Equal(t, "objs error", report.Errors[0].GetError(), "Expected first error to be first non-retriable object error")
		require.Equal(t, UUID1, report.Errors[0].GetUuid(), "Expected first error's UUID to match")
		require.Equal(t, "refs error", report.Errors[1].GetError(), "Expected second error to be first non-retriable reference error")
		require.Equal(t, toBeacon(ref1), report.Errors[1].GetBeacon(), "Expected second error's beacon to match")

		require.Empty(t, rq, "Expected reporting queue to be empty after reading all messages")
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})
}
