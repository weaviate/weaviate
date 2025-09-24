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
	"errors"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestShutdownLogicHappyPath(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockBatcher(t)

	readQueues := batch.NewBatchReadQueues()
	readQueues.Make(StreamId)
	writeQueues := batch.NewBatchWriteQueues(1)
	writeQueues.Make(StreamId, nil)
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

	objs := make([]*pb.BatchObject, 0, howManyObjs)
	for i := 0; i < howManyObjs; i++ {
		objs = append(objs, &pb.BatchObject{})
	}

	stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
	stream.EXPECT().Recv().Return(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Objects_{
			Objects: &pb.BatchStreamRequest_Objects{
				Values: objs,
			},
		},
	}, nil).Once()
	stream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		// simulate ending the stream from the client-side gracefully after receiving the stop
		<-ctx.Done()
		return nil, io.EOF
	}).Once()
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
		Message: &pb.BatchStreamReply_Backoff_{
			Backoff: &pb.BatchStreamReply_Backoff{},
		},
	}).Return(nil).Maybe()
	stream.EXPECT().Send(&pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShutdownFinished_{
			ShutdownFinished: &pb.BatchStreamReply_ShutdownFinished{},
		},
	}).Return(nil).Once()

	shutdown := batch.NewShutdown(ctx)
	handler := batch.NewQueuesHandler(shutdown.HandlersCtx, shutdown.RecvWg, shutdown.SendWg, shutdown.ShutdownFinished, writeQueues, readQueues, nil, logger)
	batch.StartScheduler(shutdown.SchedulerCtx, shutdown.SchedulerWg, writeQueues, processingQueue, reportingQueue, nil, logger)
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

func TestShutdownLogicAfterBrokenStream(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockBatcher(t)

	readQueues := batch.NewBatchReadQueues()
	readQueues.Make(StreamId)
	writeQueues := batch.NewBatchWriteQueues(1)
	writeQueues.Make(StreamId, nil)
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

	objs := make([]*pb.BatchObject, 0, howManyObjs)
	for i := 0; i < howManyObjs; i++ {
		objs = append(objs, &pb.BatchObject{})
	}

	stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
	var count int
	stream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		count++
		switch count {
		case 1:
			return &pb.BatchStreamRequest{
				Message: &pb.BatchStreamRequest_Objects_{
					Objects: &pb.BatchStreamRequest_Objects{
						Values: objs,
					},
				},
			}, nil
		case 2:
			// simulate ending the stream from the client-side ungracefully
			return nil, errors.New("some network error")
		}
		panic("should not be called more than twice")
	}).Times(2)

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
		Message: &pb.BatchStreamReply_Backoff_{
			Backoff: &pb.BatchStreamReply_Backoff{},
		},
	}).Return(nil).Maybe()
	stream.EXPECT().Send(&pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShutdownFinished_{
			ShutdownFinished: &pb.BatchStreamReply_ShutdownFinished{},
		},
	}).Return(nil).Once()

	shutdown := batch.NewShutdown(ctx)
	handler := batch.NewQueuesHandler(shutdown.HandlersCtx, shutdown.RecvWg, shutdown.SendWg, shutdown.ShutdownFinished, writeQueues, readQueues, nil, logger)
	batch.StartScheduler(shutdown.SchedulerCtx, shutdown.SchedulerWg, writeQueues, processingQueue, reportingQueue, nil, logger)
	batch.StartBatchWorkers(shutdown.WorkersCtx, shutdown.WorkersWg, 1, processingQueue, reportingQueue, readQueues, mockBatcher, logger)

	go func() {
		err := handler.StreamSend(ctx, StreamId, stream)
		require.NoError(t, err, "Expected no error when running stream send")
	}()
	go func() {
		err := handler.StreamRecv(ctx, StreamId, stream)
		require.Equal(t, err.Error(), "some network error", "Expected network error when running stream recv")
	}()
	shutdown.Drain(logger)
}
