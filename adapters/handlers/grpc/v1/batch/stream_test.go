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
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestStreamHandler(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	t.Run("start and stop ungracefully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockbatcher(t)
		mockStream := newMockStream(t)
		mockStream.EXPECT().Context().Return(ctx).Once()
		mockAuthenticator := mocks.NewMockauthenticator(t)
		mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

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
		mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()

		numWorkers := 1
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
		err := handler.Handle(mockStream)
		require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
	})

	t.Run("start and stop gracefully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockbatcher(t)
		mockStream := newMockStream(t)
		mockStream.EXPECT().Context().Return(ctx).Once()
		mockAuthenticator := mocks.NewMockauthenticator(t)
		mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

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
		mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()

		numWorkers := 1
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
		err := handler.Handle(mockStream)
		require.NoError(t, err, "Expected no error when streaming")
	})

	t.Run("start error and stop ungracefully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockbatcher(t)
		mockStream := newMockStream(t)
		mockStream.EXPECT().Context().Return(ctx).Twice()
		mockAuthenticator := mocks.NewMockauthenticator(t)
		mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

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
		mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
			errs := msg.GetResults().GetErrors()
			return len(errs) > 0 && errs[0].Error == "batcher error" && errs[0].GetUuid() == obj.Uuid
		})).Return(nil).Once()
		mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()

		numWorkers := 1
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
		err := handler.Handle(mockStream)
		require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
	})

	t.Run("start error and stop gracefully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockbatcher(t)
		mockStream := newMockStream(t)
		mockStream.EXPECT().Context().Return(ctx).Twice()
		mockAuthenticator := mocks.NewMockauthenticator(t)
		mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

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
		mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
			errs := msg.GetResults().GetErrors()
			return len(errs) > 0 && errs[0].Error == "batcher error" && errs[0].GetUuid() == obj.Uuid
		})).Return(nil).Once()
		mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()

		numWorkers := 1
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
		err := handler.Handle(mockStream)
		require.NoError(t, err, "Expected no error when streaming")
	})

	t.Run("end to end receiving objects", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		logger := logrus.New()

		mockBatcher := mocks.NewMockbatcher(t)
		mockStream := newMockStream(t)
		mockStream.EXPECT().Context().Return(ctx).Maybe()
		mockAuthenticator := mocks.NewMockauthenticator(t)
		mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

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
				return nil, io.EOF // End the stream
			default:
				panic(fmt.Sprintf("should not be called more than thrice, was called %d times", recvCount))
			}
		}).Times(3)

		mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
		mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
			return msg.GetResults() != nil
		})).Return(nil).Maybe()

		numWorkers := 1
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
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
		Message: &pb.BatchStreamRequest_Data_{
			Data: &pb.BatchStreamRequest_Data{
				Objects: &pb.BatchStreamRequest_Data_Objects{
					Values: objs,
				},
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

func newMockStream(t *testing.T) *mocks.MockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply] {
	stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
	return stream
}
