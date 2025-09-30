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

func TestShutdownHappyPath(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockBatcher(t)

	howManyObjs := 5000
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
		time.Sleep(100 * time.Millisecond)
		numErrs := int(len(req.Objects) / 10)
		errors := make([]*pb.BatchObjectsReply_BatchError, 0, numErrs)
		for i := 0; i < numErrs; i++ {
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

	stream := newMockStream(ctx, t)
	var count int
	stream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		count++
		switch count {
		case 1:
			return newBatchStreamStartRequest(), nil
		case 2:
			return newBatchStreamObjsRequest(objs), nil
		case 3:
			return nil, io.EOF
		}
		panic("should not be called more than thrice")
	}).Times(3)
	stream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetError().GetError() == "some error" &&
			msg.GetError().GetObject() != nil
	})).Return(nil).Maybe()
	stream.EXPECT().Send(newBatchStreamShutdownTriggeredReply()).Return(nil).Once()
	stream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetBackoff() != nil
	})).Return(nil).Maybe()
	stream.EXPECT().Send(newBatchStreamShutdownFinishedReply()).Return(nil).Once()

	numWorkers := 1
	shutdown := batch.NewShutdown(ctx)
	handler, _ := batch.Start(nil, nil, mockBatcher, nil, shutdown, numWorkers, logger)
	go func() {
		time.Sleep(1 * time.Second)
		shutdown.Drain(logger)
	}()
	err := handler.Handle(stream)
	require.NoError(t, err, "handler should shut down gracefully")
}

func TestShutdownAfterBrokenStream(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockBatcher(t)

	howManyObjs := 5000
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
		time.Sleep(100 * time.Millisecond)
		numErrs := int(len(req.Objects) / 10)
		errors := make([]*pb.BatchObjectsReply_BatchError, 0, numErrs)
		for i := 0; i < numErrs; i++ {
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

	stream := newMockStream(ctx, t)
	var count int
	stream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		count++
		switch count {
		case 1:
			return newBatchStreamStartRequest(), nil
		case 2:
			return newBatchStreamObjsRequest(objs), nil
		case 3:
			// simulate ending the stream from the client-side ungracefully
			return nil, errors.New("some network error")
		}
		panic("should not be called more than thrice")
	}).Times(3)

	stream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetError().GetError() == "some error" &&
			msg.GetError().GetObject() != nil
	})).Return(nil).Maybe()
	stream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetBackoff() != nil
	})).Return(nil).Maybe()
	// whenever the server is cancelled, it will the client to stop despite the broken stream
	// in this specific case, the client will never receive this message
	stream.EXPECT().Send(newBatchStreamStopReply()).Return(nil).Once()

	numWorkers := 1
	shutdown := batch.NewShutdown(ctx)
	handler, _ := batch.Start(nil, nil, mockBatcher, nil, shutdown, numWorkers, logger)
	err := handler.Handle(stream)
	require.Error(t, err, "some network error")
	shutdown.Drain(logger)
}

func newBatchStreamShutdownTriggeredReply() *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShutdownTriggered_{
			ShutdownTriggered: &pb.BatchStreamReply_ShutdownTriggered{},
		},
	}
}

func newBatchStreamShutdownFinishedReply() *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShutdownFinished_{
			ShutdownFinished: &pb.BatchStreamReply_ShutdownFinished{},
		},
	}
}
