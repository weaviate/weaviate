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
	"fmt"
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

func TestStreamHandler(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	t.Run("start and stop due to cancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockBatcher(t)
		mockStream := newMockStream(ctx, t)

		recvCount := 0
		mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
			recvCount++
			switch recvCount {
			case 1:
				return newBatchStreamStartRequest(), nil // Send start message
			case 2:
				return nil, io.EOF // End the stream
			default:
				panic(fmt.Sprintf("should not be called more than twice, was called %d times", recvCount))
			}
		}).Times(2)

		mockStream.EXPECT().Send(&pb.BatchStreamReply{
			Message: &pb.BatchStreamReply_Stop_{
				Stop: &pb.BatchStreamReply_Stop{},
			},
		}).Return(nil).Once()

		numWorkers := 1
		shutdown := batch.NewShutdown(context.Background())
		handler, _ := batch.Start(nil, nil, mockBatcher, nil, shutdown, numWorkers, logger)
		err := handler.Handle(mockStream)
		require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
	})

	t.Run("start and stop due to sentinel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockBatcher(t)
		mockStream := newMockStream(ctx, t)

		recvCount := 0
		mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
			recvCount++
			switch recvCount {
			case 1:
				return newBatchStreamStartRequest(), nil // Send start message
			case 2:
				return newBatchStreamStopRequest(), nil // Send stop message
			case 3:
				return nil, io.EOF // End the stream
			default:
				panic(fmt.Sprintf("should not be called more than twice, was called %d times", recvCount))
			}
		}).Times(3)

		mockStream.EXPECT().Send(newBatchStreamStopReply()).Return(nil).Once()

		numWorkers := 1
		shutdown := batch.NewShutdown(context.Background())
		handler, _ := batch.Start(nil, nil, mockBatcher, nil, shutdown, numWorkers, logger)
		err := handler.Handle(mockStream)
		require.NoError(t, err, "Expected no error when streaming")
	})

	t.Run("start error and stop due to cancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockBatcher(t)
		mockStream := newMockStream(ctx, t)

		obj := &pb.BatchObject{
			Collection: "TestClass",
		}
		mockBatcher.EXPECT().
			BatchObjects(mock.Anything, &pb.BatchObjectsRequest{Objects: []*pb.BatchObject{obj}}).
			Return(&pb.BatchObjectsReply{Errors: []*pb.BatchObjectsReply_BatchError{{Error: "batcher error"}}}, nil).
			Once()

		recvCount := 0
		mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
			recvCount++
			switch recvCount {
			case 1:
				return newBatchStreamStartRequest(), nil // Send start message
			case 2:
				return newBatchStreamObjsRequest([]*pb.BatchObject{obj}), nil // Send 1 object
			case 3:
				return nil, io.EOF // End the stream
			default:
				panic(fmt.Sprintf("should not be called more than thrice, was called %d times", recvCount))
			}
		}).Times(3)
		mockStream.EXPECT().Send(newBatchStreamErrReply("batcher error", obj)).Return(nil).Once()
		mockStream.EXPECT().Send(newBatchStreamStopReply()).Return(nil).Once()

		numWorkers := 1
		shutdown := batch.NewShutdown(context.Background())
		handler, _ := batch.Start(nil, nil, mockBatcher, nil, shutdown, numWorkers, logger)
		err := handler.Handle(mockStream)
		require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
	})

	t.Run("start error and stop due to sentinel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockBatcher(t)
		mockStream := newMockStream(ctx, t)

		obj := &pb.BatchObject{
			Collection: "TestClass",
		}
		mockBatcher.EXPECT().
			BatchObjects(mock.Anything, &pb.BatchObjectsRequest{Objects: []*pb.BatchObject{obj}}).
			Return(&pb.BatchObjectsReply{Errors: []*pb.BatchObjectsReply_BatchError{{Error: "batcher error"}}}, nil).
			Once()

		recvCount := 0
		mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
			recvCount++
			switch recvCount {
			case 1:
				return newBatchStreamStartRequest(), nil // Send start message
			case 2:
				return newBatchStreamObjsRequest([]*pb.BatchObject{obj}), nil // Send 1 object
			case 3:
				return newBatchStreamStopRequest(), nil // Send stop message
			case 4:
				return nil, io.EOF // End the stream
			default:
				panic(fmt.Sprintf("should not be called more than four times, was called %d times", recvCount))
			}
		}).Times(4)

		mockStream.EXPECT().Send(newBatchStreamErrReply("batcher error", obj)).Return(nil).Once()
		mockStream.EXPECT().Send(newBatchStreamStopReply()).Return(nil).Once()

		numWorkers := 1
		shutdown := batch.NewShutdown(context.Background())
		handler, _ := batch.Start(nil, nil, mockBatcher, nil, shutdown, numWorkers, logger)
		err := handler.Handle(mockStream)
		require.NoError(t, err, "Expected no error when streaming")
	})

	t.Run("end to end receiving objects", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		logger := logrus.New()

		mockBatcher := mocks.NewMockBatcher(t)
		mockStream := newMockStream(ctx, t)

		numObjs := 10000
		objsCh := make(chan *pb.BatchObject, numObjs)
		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
			start := time.Now()
			for _, obj := range req.Objects {
				objsCh <- obj
			}
			return &pb.BatchObjectsReply{
				Took:   float32(time.Since(start).Seconds()),
				Errors: nil,
			}, nil
		}).Maybe()

		objs := make([]*pb.BatchObject, 0, numObjs)
		for i := 0; i < numObjs; i++ {
			objs = append(objs, &pb.BatchObject{Collection: "TestClass"})
		}

		recvCount := 0
		mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
			recvCount++
			switch recvCount {
			case 1:
				return newBatchStreamStartRequest(), nil // Send start message
			case 2:
				return newBatchStreamObjsRequest(objs), nil // Send 10000 objects
			case 3:
				return newBatchStreamStopRequest(), nil // Send stop message
			case 4:
				return nil, io.EOF // End the stream
			default:
				panic(fmt.Sprintf("should not be called more than 4 times, was called %d times", recvCount))
			}
		}).Times(4)

		mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
			return msg.GetBackoff() != nil
		})).Return(nil).Maybe()
		mockStream.EXPECT().Send(newBatchStreamStopReply()).Return(nil).Once()

		numWorkers := 1
		shutdown := batch.NewShutdown(context.Background())
		handler, _ := batch.Start(nil, nil, mockBatcher, nil, shutdown, numWorkers, logger)
		err := handler.Handle(mockStream)
		require.NoError(t, err, "Expected no error when handling stream")
		require.Len(t, objsCh, numObjs, "Expected all objects to be processed into mock channel")
	})
}

func newBatchStreamStartRequest() *pb.BatchStreamRequest {
	return &pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Start_{
			Start: &pb.BatchStreamRequest_Start{},
		},
	}
}

func newBatchStreamObjsRequest(objs []*pb.BatchObject) *pb.BatchStreamRequest {
	return &pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Objects_{
			Objects: &pb.BatchStreamRequest_Objects{
				Values: objs,
			},
		},
	}
}

func newBatchStreamStopRequest() *pb.BatchStreamRequest {
	return &pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Stop_{
			Stop: &pb.BatchStreamRequest_Stop{},
		},
	}
}

func newBatchStreamStopReply() *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Stop_{
			Stop: &pb.BatchStreamReply_Stop{},
		},
	}
}

func newBatchStreamErrReply(err string, obj *pb.BatchObject) *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Error_{
			Error: &pb.BatchStreamReply_Error{
				Error:  err,
				Detail: &pb.BatchStreamReply_Error_Object{Object: obj},
			},
		},
	}
}

func newMockStream(ctx context.Context, t *testing.T) *mocks.MockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply] {
	stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
	stream.EXPECT().Context().Return(ctx).Once()
	return stream
}
