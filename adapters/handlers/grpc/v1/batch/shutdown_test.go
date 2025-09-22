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
	processingQueue := batch.NewBatchProcessingQueue(1)
	reportingQueue := batch.NewBatchReportingQueue(1)

	howManyObjs := 5000
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
	}).Maybe()

	for i := 0; i < howManyObjs; i++ {
		wq <- batch.NewWriteObject(&pb.BatchObject{})
	}

	stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
	stream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetError().GetError() == "some error" &&
			msg.GetError().GetObject() != nil
	})).Return(nil).Maybe()
	stream.EXPECT().Send(&pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShutdownTriggered_{
			ShutdownTriggered: &pb.BatchStreamReply_ShutdownTriggered{},
		},
	}).Return(nil).Once()
	stream.EXPECT().Send(&pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShutdownInProgress_{
			ShutdownInProgress: &pb.BatchStreamReply_ShutdownInProgress{},
		},
	}).Return(nil).Maybe()
	stream.EXPECT().Send(&pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShutdownFinished_{
			ShutdownFinished: &pb.BatchStreamReply_ShutdownFinished{},
		},
	}).Return(nil).Once()
	stream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		time.Sleep(10 * time.Second)
		return &pb.BatchStreamRequest{}, nil
	}).Maybe()

	shutdown := batch.NewShutdown(ctx)
	handler := batch.NewQueuesHandler(shutdown.HandlersCtx, shutdown.RecvWg, shutdown.SendWg, shutdown.ShutdownFinished, writeQueues, readQueues, logger)
	batch.StartScheduler(shutdown.SchedulerCtx, shutdown.SchedulerWg, writeQueues, processingQueue, reportingQueue, logger)
	batch.StartBatchWorkers(shutdown.WorkersCtx, shutdown.WorkersWg, 1, processingQueue, reportingQueue, readQueues, mockBatcher, logger)

	go func() {
		err := handler.StreamSend(ctx, StreamId, stream)
		require.NoError(t, err, "Expected no error when running stream send")
	}()
	go func() {
		err := handler.StreamRecv(ctx, StreamId, stream)
		require.NoError(t, err, "Expected no error when running stream recv")
	}()
	shutdown.Drain(logger)
}
