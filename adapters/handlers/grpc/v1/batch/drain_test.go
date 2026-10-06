//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestDrainOfInProgressBatch(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockBatcher(t)
	mockSchemaManager := mocks.NewMockschemaManager(t)
	mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
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

	collection := "TestClass"
	mockSchemaManager.EXPECT().
		GetCachedClassNoAuth(mock.Anything, collection).
		Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).
		Once()
	objs := make([]*pb.BatchObject, 0, howManyObjs)
	for i := 0; i < howManyObjs; i++ {
		objs = append(objs, &pb.BatchObject{Collection: collection})
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
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetAcks() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
	mockStream.EXPECT().Send(newBatchStreamShuttingDownReply()).Return(nil).Once()

	numWorkers := 1
	// The client finishes before the cut, as a well-behaved one does.
	clientCallsCtx, cancelClientCalls := context.WithCancel(context.Background())
	t.Cleanup(cancelClientCalls)
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false,
		batch.WithClientCallsCtx(clientCallsCtx))
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

	mockBatcher := mocks.NewMockBatcher(t)
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

	collection := "TestClass"
	mockSchemaManager := mocks.NewMockschemaManager(t)
	mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
	mockSchemaManager.EXPECT().
		GetCachedClassNoAuth(mock.Anything, collection).
		Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).
		Once()
	objs := make([]*pb.BatchObject, 0, howManyObjs)
	for i := 0; i < howManyObjs; i++ {
		objs = append(objs, &pb.BatchObject{Collection: collection})
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
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetAcks() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
	// depending on timings, may or may not be emitted
	mockStream.EXPECT().Send(newBatchStreamShuttingDownReply()).Return(nil).Maybe()

	numWorkers := 1
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
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

	mockBatcher := mocks.NewMockBatcher(t)
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

	collection := "TestClass"
	mockSchemaManager := mocks.NewMockschemaManager(t)
	mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
	mockSchemaManager.EXPECT().
		GetCachedClassNoAuth(mock.Anything, collection).
		Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).
		Once()
	objs := make([]*pb.BatchObject, 0, howManyObjs)
	for i := 0; i < howManyObjs; i++ {
		objs = append(objs, &pb.BatchObject{Collection: collection})
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
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetAcks() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()

	numWorkers := 1
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
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

	mockBatcher := mocks.NewMockBatcher(t)
	mockSchemaManager := mocks.NewMockschemaManager(t)
	mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
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

	collection := "TestClass"
	mockSchemaManager.EXPECT().
		GetCachedClassNoAuth(mock.Anything, collection).
		Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).
		Once()

	objs := make([]*pb.BatchObject, 0, howManyObjs)
	for i := 0; i < howManyObjs; i++ {
		objs = append(objs, &pb.BatchObject{Collection: collection})
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
		return msg.GetAcks() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetBackoff() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
	mockStream.EXPECT().Send(newBatchStreamShuttingDownReply()).Return(nil).Once()

	numWorkers := 1
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
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

	mockBatcher := mocks.NewMockBatcher(t)
	mockSchemaManager := mocks.NewMockschemaManager(t)
	mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
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
	collection := "TestClass"
	mockSchemaManager.EXPECT().
		GetCachedClassNoAuth(mock.Anything, collection).
		Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).
		Maybe()
	objs := make([]*pb.BatchObject, 0, howManyObjs)
	for i := 0; i < howManyObjs; i++ {
		objs = append(objs, &pb.BatchObject{Collection: collection})
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
		return msg.GetAcks() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetBackoff() != nil
	})).Return(nil).Maybe()
	mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
	mockStream.EXPECT().Send(newBatchStreamShuttingDownReply()).Return(nil).Once()
	// Will not emit shutdown message since client never stops sending messages, it gets hung up on instead

	numWorkers := 1
	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
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

// A stream still receiving after the shutdown message is cut when client calls
// are cancelled, long before SHUTDOWN_GRACE_PERIOD.
func TestDrainClosesStreamsWhenClientCallsCancelled(t *testing.T) {
	const waitLimit = 5 * time.Second
	collection := "TestClass"

	cases := []struct {
		name string
		// keepSending makes the client send data after the shutdown message instead of going silent.
		keepSending bool
		// batchWaitsForCtx makes the batcher hold the first batch until its ctx ends,
		// so it is still unwritten when client calls are cancelled.
		batchWaitsForCtx bool
	}{
		{name: "client keeps sending", keepSending: true},
		{name: "client goes silent"},
		{name: "batch not yet written is abandoned", batchWaitsForCtx: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			clientCallsCtx, cancelClientCalls := context.WithCancel(context.Background())
			t.Cleanup(cancelClientCalls)

			var written atomic.Int32
			batchEntered := make(chan struct{}, 1)
			batcher := mocks.NewMockBatcher(t)
			batcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
					select {
					case batchEntered <- struct{}{}:
					default:
					}
					if tc.batchWaitsForCtx {
						<-ctx.Done()
					}
					// the LSM write is skipped once ctx is done
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					written.Add(int32(len(req.Objects)))
					return &pb.BatchObjectsReply{}, nil
				}).Maybe()
			schemaManager := mocks.NewMockschemaManager(t)
			schemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
			schemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, collection).
				Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).Maybe()
			authenticator := mocks.NewMockauthenticator(t)
			authenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

			stream := newMockStream(t)
			stream.EXPECT().Context().Return(ctx).Maybe()
			shutdownSent := make(chan struct{})
			stream.EXPECT().Send(newBatchStreamShuttingDownReply()).RunAndReturn(func(*pb.BatchStreamReply) error {
				close(shutdownSent)
				return nil
			}).Once()
			stream.EXPECT().Send(mock.Anything).Return(nil).Maybe()
			objs := []*pb.BatchObject{{Collection: collection, Uuid: "5f8e0d34-1c6a-4a1e-9f0c-6f9b6e0f0a11"}}
			recvCount := 0
			stream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
				recvCount++
				switch {
				case recvCount == 1:
					return newBatchStreamStartRequest(), nil
				case recvCount == 2 || tc.keepSending:
					return newBatchStreamObjsRequest(objs), nil
				default:
					<-ctx.Done()
					return nil, io.EOF
				}
			}).Maybe()

			handler, drain := batch.Start(authenticator, nil, batcher, schemaManager, nil, 1, logrus.New(), false,
				batch.WithClientCallsCtx(clientCallsCtx))
			handled := make(chan error, 1)
			go func() { handled <- handler.Handle(stream) }()

			select {
			case <-batchEntered:
			case <-time.After(waitLimit):
				require.FailNow(t, "first batch never reached the batcher")
			}
			drained := make(chan struct{})
			go func() {
				drain()
				close(drained)
			}()
			select {
			case <-shutdownSent:
			case <-time.After(waitLimit):
				require.FailNow(t, "shutdown message never sent")
			}
			cancelClientCalls()

			select {
			case err := <-handled:
				require.ErrorIs(t, err, context.Canceled)
				require.ErrorContains(t, err, "recv stream closed as client calls were cancelled")
			case <-time.After(waitLimit):
				require.FailNow(t, "stream not closed after client calls were cancelled")
			}
			select {
			case <-drained:
			case <-time.After(waitLimit):
				require.FailNow(t, "drain did not finish after the stream was closed")
			}
			if tc.batchWaitsForCtx {
				require.Zero(t, written.Load(), "the batch unwritten at the cut must stay unwritten")
			}
		})
	}
}

func newBatchStreamShuttingDownReply() *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShuttingDown_{
			ShuttingDown: &pb.BatchStreamReply_ShuttingDown{},
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
