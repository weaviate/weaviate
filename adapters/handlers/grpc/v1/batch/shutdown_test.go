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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestShutdownLogic(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockBatcher(t)

	readQueues := batch.NewBatchReadQueues()
	readQueues.Make(StreamId)
	writeQueues := batch.NewBatchWriteQueues()
	writeQueues.Make(StreamId, nil)
	wq, ok := writeQueues.GetQueue(StreamId)
	require.Equal(t, true, ok, "write queue should exist")
	internalQueue := batch.NewBatchInternalQueue()

	howManyObjs := 5000
	// 5000 objs will be sent five times in batches of 1000
	// 100 of the each batch will error
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
		time.Sleep(1 * time.Second)
		errors := make([]*pb.BatchObjectsReply_BatchError, 0, 100)
		for i := 0; i < 100; i++ {
			errors = append(errors, &pb.BatchObjectsReply_BatchError{
				Error: "some error",
				Index: int32(i),
			})
		}
		return &pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: errors,
		}, nil
	}).Times(5)

	for i := 0; i < howManyObjs; i++ {
		wq <- batch.NewWriteObject(&pb.BatchObject{})
	}

	done := make(chan struct{})
	stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
	stream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetError().Error == "some error" &&
			msg.GetError().GetObject() != nil
	})).Return(nil).Times(500)
	stream.EXPECT().Send(&pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShuttingDown_{
			ShuttingDown: &pb.BatchStreamReply_ShuttingDown{},
		},
	}).Return(nil).Once()
	stream.EXPECT().Send(&pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Shutdown_{
			Shutdown: &pb.BatchStreamReply_Shutdown{},
		},
	}).RunAndReturn(func(*pb.BatchStreamReply) error {
		// Ensure handler cancel call comes after this message has been emitted to avoid races
		close(done)
		return nil
	}).Once()

	shutdown := batch.NewShutdown(ctx)
	handler := batch.NewQueuesHandler(shutdown.HandlersCtx, shutdown.SendWg, shutdown.StreamWg, shutdown.ShutdownFinished, writeQueues, readQueues, logger)
	batch.StartScheduler(shutdown.SchedulerCtx, shutdown.SchedulerWg, writeQueues, internalQueue, logger)
	batch.StartBatchWorkers(shutdown.WorkersCtx, shutdown.WorkersWg, 1, internalQueue, readQueues, mockBatcher, logger)

	go func() {
		err := handler.StreamSend(ctx, StreamId, stream, done)
		require.NoError(t, err, "Expected no error when streaming")
	}()
	shutdown.Drain(logger)
}
