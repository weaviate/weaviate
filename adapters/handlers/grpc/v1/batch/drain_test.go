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
	"sync"
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

func TestDrainOfInProgressBatch(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockbatcher(t)
	mockStream := newMockStream(t)
	mockStream.EXPECT().Context().Return(ctx).Maybe()
	mockAuthenticator := mocks.NewMockauthenticator(t)
	mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

	howManyObjs := 5000
	objsCh := make(chan *pb.BatchObject, howManyObjs)
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
		for _, obj := range req.Objects {
			objsCh <- obj
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

	var count int
	shouldDrain := make(chan struct{})
	mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		count++
		switch count {
		case 1:
			return newBatchStreamStartRequest(), nil
		case 2:
			close(shouldDrain)
			return newBatchStreamObjsRequest(objs), nil
		case 3:
			return nil, io.EOF // End the stream
		}
		panic("should not be called more than thrice")
	}).Times(3)
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetResults() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
	mockStream.EXPECT().Send(newBatchStreamShuttingDownReply()).Return(nil).Once()
	mockStream.EXPECT().Send(newBatchStreamShutdownReply()).Return(nil).Once()

	numWorkers := 1
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-shouldDrain
		drain()
	}()
	err := handler.Handle(mockStream)
	require.Nil(t, err, "handler should not return an error got: %s", err)
	require.Len(t, objsCh, howManyObjs, "all objects should have been processed")
	wg.Wait()
}

func TestDrainOfFinishedBatch(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockbatcher(t)
	mockStream := newMockStream(t)
	mockStream.EXPECT().Context().Return(ctx).Maybe()
	mockAuthenticator := mocks.NewMockauthenticator(t)
	mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

	howManyObjs := 5000
	objsCh := make(chan *pb.BatchObject, howManyObjs)
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
		for _, obj := range req.Objects {
			objsCh <- obj
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

	var count int
	shouldDrain := make(chan struct{})
	mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		count++
		switch count {
		case 1:
			return newBatchStreamStartRequest(), nil
		case 2:
			return newBatchStreamObjsRequest(objs), nil
		case 3:
			return newBatchStreamStopRequest(), nil
		case 4:
			defer close(shouldDrain)
			return nil, io.EOF // End the stream
		}
		panic("should not be called more than four times")
	}).Times(4)
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetResults() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
	// depending on timings, may or may not be emitted
	mockStream.EXPECT().Send(newBatchStreamShuttingDownReply()).Return(nil).Maybe()

	numWorkers := 1
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-shouldDrain
		drain()
	}()
	err := handler.Handle(mockStream)
	require.Nil(t, err, "handler should not return an error")
	require.Len(t, objsCh, howManyObjs, "all objects should have been processed")
	wg.Wait()
}

func TestDrainAfterBrokenStream(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockbatcher(t)
	mockAuthenticator := mocks.NewMockauthenticator(t)
	mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

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

	mockStream := newMockStream(t)
	mockStream.EXPECT().Context().Return(ctx).Maybe()
	var count int
	networkErr := errors.New("some network error")
	mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		count++
		switch count {
		case 1:
			return newBatchStreamStartRequest(), nil
		case 2:
			return newBatchStreamObjsRequest(objs), nil
		case 3:
			// simulate ending the stream from the client-side ungracefully
			return nil, networkErr
		}
		panic("should not be called more than thrice")
	}).Times(3)

	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetResults() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()

	numWorkers := 1
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
	err := handler.Handle(mockStream)
	require.NotNil(t, err, "handler should return an error")
	require.ErrorAs(t, err, &networkErr, "handler should return network error")
	drain()
}

func TestDrainWithHangingClient(t *testing.T) {
	testDuration := 5 * time.Minute
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testDuration)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockbatcher(t)
	mockAuthenticator := mocks.NewMockauthenticator(t)
	mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

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

	mockStream := newMockStream(t)
	mockStream.EXPECT().Context().Return(ctx).Maybe()
	var count int
	shouldDrain := make(chan struct{})
	mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		count++
		switch count {
		case 1:
			return newBatchStreamStartRequest(), nil
		case 2:
			return newBatchStreamObjsRequest(objs), nil
		case 3:
			close(shouldDrain)
			// simulate a client that does not close the stream correctly
			time.Sleep(testDuration)
			return nil, io.EOF
		}
		panic("should not be called more than thrice")
	}).Times(3)

	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetResults() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetBackoff() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
	mockStream.EXPECT().Send(newBatchStreamShuttingDownReply()).Return(nil).Once()

	numWorkers := 1
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-shouldDrain
		drain()
	}()
	err := handler.Handle(mockStream)
	wg.Wait()
	require.NotNil(t, err, "handler should return error shutting down")
	require.ErrorAs(t, err, &context.Canceled, "handler should return context.Canceled error")
}

func TestDrainWithMisbehavingClient(t *testing.T) {
	testDuration := 5 * time.Minute
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testDuration)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockbatcher(t)
	mockAuthenticator := mocks.NewMockauthenticator(t)
	mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

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

	mockStream := newMockStream(t)
	mockStream.EXPECT().Context().Return(ctx).Maybe()
	var count int
	shouldDrain := make(chan struct{})
	mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		count++
		switch count {
		case 1:
			return newBatchStreamStartRequest(), nil
		case 2:
			close(shouldDrain)
			return newBatchStreamObjsRequest(objs), nil
		default:
			// just keep sending data, the client is misbehaving
			return newBatchStreamObjsRequest(objs), nil
		}
	}).Maybe()

	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetResults() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetBackoff() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
	mockStream.EXPECT().Send(newBatchStreamShuttingDownReply()).Return(nil).Once()
	// Will not emit shutdown message since client never stops sending messages, it gets hung up on instead

	numWorkers := 1
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, nil, numWorkers, logger)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-shouldDrain
		drain()
	}()
	err := handler.Handle(mockStream)
	wg.Wait()
	require.NotNil(t, err, "handler should return error shutting down")
	require.ErrorAs(t, err, &context.Canceled, "handler should return context.Canceled error")
}

func newBatchStreamShuttingDownReply() *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShuttingDown_{
			ShuttingDown: &pb.BatchStreamReply_ShuttingDown{},
		},
	}
}

func newBatchStreamShutdownReply() *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Shutdown_{
			Shutdown: &pb.BatchStreamReply_Shutdown{},
		},
	}
}

func newBatchStreamStartedReply() *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Started_{
			Started: &pb.BatchStreamReply_Started{},
		},
	}
}
