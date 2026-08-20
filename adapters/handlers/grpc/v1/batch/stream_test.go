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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestStreamHandler(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	t.Run("start and stop ungracefully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockbatcher(t)
		mockSchemaManager := mocks.NewMockschemaManager(t)
		mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
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
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
		err := handler.Handle(mockStream)
		require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
	})

	t.Run("start and stop gracefully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockbatcher(t)
		mockSchemaManager := mocks.NewMockschemaManager(t)
		mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
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
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
		err := handler.Handle(mockStream)
		require.NoError(t, err, "Expected no error when streaming")
	})

	t.Run("start error and stop ungracefully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockbatcher(t)
		mockSchemaManager := mocks.NewMockschemaManager(t)
		mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
		mockStream := newMockStream(t)
		mockStream.EXPECT().Context().Return(ctx).Once()
		mockAuthenticator := mocks.NewMockauthenticator(t)
		mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

		collection := "TestClass"
		obj := &pb.BatchObject{
			Collection: collection,
		}
		mockBatcher.EXPECT().
			BatchObjects(mock.Anything, &pb.BatchObjectsRequest{Objects: []*pb.BatchObject{obj}}).
			Return(&pb.BatchObjectsReply{Errors: []*pb.BatchObjectsReply_BatchError{{Error: "batcher error"}}}, nil).
			Once()
		mockSchemaManager.EXPECT().
			GetCachedClassNoAuth(mock.Anything, collection).
			Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).
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
		mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
			return msg.GetAcks() != nil
		})).Return(nil).Once()
		mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()

		numWorkers := 1
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
		err := handler.Handle(mockStream)
		require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
	})

	t.Run("start error and stop gracefully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		collection := "TestClass"
		mockBatcher := mocks.NewMockbatcher(t)
		mockSchemaManager := mocks.NewMockschemaManager(t)
		mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
		mockSchemaManager.EXPECT().
			GetCachedClassNoAuth(mock.Anything, collection).
			Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).
			Once()

		mockStream := newMockStream(t)
		mockStream.EXPECT().Context().Return(ctx).Once()
		mockAuthenticator := mocks.NewMockauthenticator(t)
		mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

		obj := &pb.BatchObject{
			Collection: collection,
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
		mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
			return msg.GetAcks() != nil
		})).Return(nil).Once()
		mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()

		numWorkers := 1
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
		err := handler.Handle(mockStream)
		require.NoError(t, err, "Expected no error when streaming")
	})

	t.Run("end to end receiving objects", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		logger := logrus.New()

		mockBatcher := mocks.NewMockbatcher(t)
		mockSchemaManager := mocks.NewMockschemaManager(t)
		mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
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
		collection := "TestClass"
		mockSchemaManager.EXPECT().
			GetCachedClassNoAuth(mock.Anything, collection).
			Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).
			Maybe()

		objs := make([]*pb.BatchObject, 0, numObjs)
		for i := 0; i < numObjs; i++ {
			objs = append(objs, &pb.BatchObject{Collection: collection})
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
		mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
			return msg.GetAcks() != nil
		})).Return(nil).Once()

		numWorkers := 1
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
		err := handler.Handle(mockStream)
		require.NoError(t, err, "Expected no error when handling stream")
		require.Len(t, objsCh, numObjs, "Expected all objects to be processed into mock channel")
	})

	t.Run("receiver and sender Send calls are mutually exclusive", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockbatcher(t)
		mockSchemaManager := mocks.NewMockschemaManager(t)
		mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
		mockStream := newMockStream(t)
		mockStream.EXPECT().Context().Return(ctx).Maybe()
		mockAuthenticator := mocks.NewMockauthenticator(t)
		mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

		const numBatches = 100
		collection := "TestClass"

		var inFlight atomic.Int32
		var maxObserved atomic.Int32
		mockStream.EXPECT().Send(mock.Anything).RunAndReturn(func(msg *pb.BatchStreamReply) error {
			n := inFlight.Add(1)
			for {
				prev := maxObserved.Load()
				if n <= prev || maxObserved.CompareAndSwap(prev, n) {
					break
				}
			}
			if n > 1 {
				t.Errorf("concurrent Send detected: %d goroutines in flight", n)
			}
			time.Sleep(time.Microsecond) // widen the race window
			inFlight.Add(-1)
			return nil
		}).Maybe()

		recvCount := 0
		mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
			recvCount++
			switch {
			case recvCount == 1:
				return newBatchStreamStartRequest(), nil
			case recvCount <= numBatches+1:
				return newBatchStreamObjsRequest([]*pb.BatchObject{
					{Collection: collection, Uuid: uuid.New().String()},
				}), nil
			default:
				return nil, io.EOF
			}
		}).Times(numBatches + 2)

		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
			return &pb.BatchObjectsReply{Took: 1}, nil
		}).Maybe()
		mockSchemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, collection).
			Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).Maybe()

		// numWorkers > 1 so worker Results overlap with receiver Acks in time.
		numWorkers := 4
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
		err := handler.Handle(mockStream)
		require.NoError(t, err)

		require.Equal(t, int32(0), inFlight.Load())
		require.LessOrEqual(t, maxObserved.Load(), int32(1), "stream.Send must never have more than one goroutine in flight")
	})

	t.Run("results not lost when stream Send is slow", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		mockBatcher := mocks.NewMockbatcher(t)
		mockSchemaManager := mocks.NewMockschemaManager(t)
		mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
		mockStream := newMockStream(t)
		mockStream.EXPECT().Context().Return(ctx).Maybe()
		mockAuthenticator := mocks.NewMockauthenticator(t)
		mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

		const numBatches = 20
		collection := "TestClass"

		expected := make([]string, 0, numBatches)
		for range numBatches {
			expected = append(expected, uuid.New().String())
		}

		var slowdownsRemaining atomic.Int32
		slowdownsRemaining.Store(3)

		// Mutex-guarded so the test holds even if the concurrent-Send guard regresses.
		var seenMu sync.Mutex
		seen := make(map[string]struct{}, numBatches)

		mockStream.EXPECT().Send(mock.Anything).RunAndReturn(func(msg *pb.BatchStreamReply) error {
			if results := msg.GetResults(); results != nil {
				if slowdownsRemaining.Add(-1) >= 0 {
					time.Sleep(1200 * time.Millisecond)
				}
				seenMu.Lock()
				for _, s := range results.GetSuccesses() {
					if u := s.GetUuid(); u != "" {
						seen[u] = struct{}{}
					}
				}
				seenMu.Unlock()
			}
			return nil
		}).Maybe()

		recvCount := 0
		mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
			recvCount++
			switch {
			case recvCount == 1:
				return newBatchStreamStartRequest(), nil
			case recvCount <= numBatches+1:
				return newBatchStreamObjsRequest([]*pb.BatchObject{
					{Collection: collection, Uuid: expected[recvCount-2]},
				}), nil
			default:
				return nil, io.EOF
			}
		}).Times(numBatches + 2)

		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
			return &pb.BatchObjectsReply{Took: 1}, nil
		}).Maybe()
		mockSchemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, collection).
			Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).Maybe()

		numWorkers := 4
		handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, false)
		err := handler.Handle(mockStream)
		require.NoError(t, err)

		seenMu.Lock()
		got := make([]string, 0, len(seen))
		for u := range seen {
			got = append(got, u)
		}
		seenMu.Unlock()
		require.ElementsMatch(t, expected, got)
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

func newBatchStreamObjsAndRefsRequest(objs []*pb.BatchObject, refs []*pb.BatchReference) *pb.BatchStreamRequest {
	return &pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Data_{
			Data: &pb.BatchStreamRequest_Data{
				Objects:    &pb.BatchStreamRequest_Data_Objects{Values: objs},
				References: &pb.BatchStreamRequest_Data_References{Values: refs},
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

// mockBatchStream fails any test whose handler runs two Sends on one stream at
// once, which gRPC forbids.
type mockBatchStream struct {
	*mocks.MockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply]
	t        *testing.T
	inFlight atomic.Int32
}

// t.Errorf rather than t.Fatal, because the handler sends from goroutines other
// than the test's.
func (s *mockBatchStream) Send(msg *pb.BatchStreamReply) error {
	if n := s.inFlight.Add(1); n > 1 {
		s.t.Errorf("concurrent Send on a single stream: %d goroutines in flight", n)
	}
	defer s.inFlight.Add(-1)
	return s.MockWeaviate_BatchStreamServer.Send(msg)
}

func newMockStream(t *testing.T) *mockBatchStream {
	return &mockBatchStream{
		MockWeaviate_BatchStreamServer: mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t),
		t:                              t,
	}
}

// TestStreamHandlerCollectionResolution verifies that the receiver resolves the
// raw obj.Collection to a namespace-qualified / alias-resolved / uppercased name
// before the vectorisation-hint schema lookup. Without this resolution the
// schema lookup misses on NS-enabled clusters, for alias callers, and for
// lowercased class input — which silently disables the 10x fan-out optimization
// in worker.sendObjects for vectoriser-backed collections.
func TestStreamHandlerCollectionResolution(t *testing.T) {
	logger := logrus.New()

	cases := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		rawCollection     string // value sent by the client in obj.Collection
		resolvedAs        string // expected argument to GetCachedClassNoAuth
		aliasTarget       string // non-empty if ResolveAlias should resolve to this target
	}{
		{
			name:              "namespaced principal qualifies short class",
			namespacesEnabled: true,
			principal:         &models.Principal{Namespace: "customer1"},
			rawCollection:     "Movies",
			resolvedAs:        "Movies",
		},
		{
			name:              "alias is resolved to its target class",
			namespacesEnabled: false,
			principal:         &models.Principal{},
			rawCollection:     "MyAlias",
			resolvedAs:        "Movies",
			aliasTarget:       "Movies",
		},
		{
			name:              "lowercased class is uppercased before lookup",
			namespacesEnabled: false,
			principal:         &models.Principal{},
			rawCollection:     "movies",
			resolvedAs:        "Movies",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			mockBatcher := mocks.NewMockbatcher(t)
			mockSchemaManager := mocks.NewMockschemaManager(t)
			mockStream := newMockStream(t)
			mockStream.EXPECT().Context().Return(ctx).Maybe()
			mockAuthenticator := mocks.NewMockauthenticator(t)
			mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(tc.principal, nil).Once()

			expectedLookup := tc.resolvedAs
			if tc.namespacesEnabled && tc.principal.Namespace != "" {
				expectedLookup = tc.principal.Namespace + ":" + tc.resolvedAs
			}

			// First ResolveAlias is called on the qualified input; in alias cases
			// it returns the target, otherwise "".
			if tc.aliasTarget != "" {
				mockSchemaManager.EXPECT().ResolveAlias(tc.rawCollection).Return(tc.aliasTarget).Once()
			} else {
				mockSchemaManager.EXPECT().ResolveAlias(expectedLookup).Return("").Once()
			}

			// The schema lookup must use the resolved name, not the raw client input.
			mockSchemaManager.EXPECT().
				GetCachedClassNoAuth(mock.Anything, expectedLookup).
				Return(map[string]versioned.Class{
					expectedLookup: {Class: &models.Class{Class: expectedLookup}},
				}, nil).
				Once()

			mockBatcher.EXPECT().
				BatchObjects(mock.Anything, mock.Anything).
				Return(&pb.BatchObjectsReply{}, nil).
				Maybe()

			recvCount := 0
			mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
				recvCount++
				switch recvCount {
				case 1:
					return newBatchStreamStartRequest(), nil
				case 2:
					return newBatchStreamObjsRequest([]*pb.BatchObject{
						{Collection: tc.rawCollection, Uuid: uuid.New().String()},
					}), nil
				default:
					return nil, io.EOF
				}
			}).Times(3)
			mockStream.EXPECT().Send(newBatchStreamStartedReply()).Return(nil).Once()
			mockStream.EXPECT().Send(mock.Anything).Return(nil).Maybe()

			numWorkers := 1
			handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, numWorkers, logger, tc.namespacesEnabled)
			err := handler.Handle(mockStream)
			require.NoError(t, err)
		})
	}
}

// The client must learn which objects were dropped when a message is rejected
// before it reaches the queue.
func TestStreamHandlerReportsSchemaResolutionFailures(t *testing.T) {
	logger := logrus.New()

	cases := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		collection        string
		getClassErr       error
	}{
		{
			name:              "namespace resolution failure",
			namespacesEnabled: true,
			principal:         &models.Principal{Namespace: "customer1"},
			collection:        "customer2:TestClass",
		},
		{
			// the schema error names the namespace-qualified class, which a
			// namespaced principal must never see
			name:              "class fetch failure",
			namespacesEnabled: true,
			principal:         &models.Principal{Namespace: "customer1"},
			collection:        "TestClass",
			getClassErr:       fmt.Errorf("class %q not found", "customer1:TestClass"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			objs := []*pb.BatchObject{
				{Collection: tc.collection, Uuid: uuid.New().String()},
				{Collection: tc.collection, Uuid: uuid.New().String()},
			}
			refs := []*pb.BatchReference{
				{FromCollection: tc.collection, FromUuid: uuid.New().String(), ToUuid: uuid.New().String(), Name: "ref"},
			}

			mockBatcher := mocks.NewMockbatcher(t)
			mockSchemaManager := mocks.NewMockschemaManager(t)
			mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
			mockSchemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, mock.Anything).
				Return(nil, tc.getClassErr).Maybe()

			mockAuthenticator := mocks.NewMockauthenticator(t)
			mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(tc.principal, nil).Once()

			mockStream := newMockStream(t)
			mockStream.EXPECT().Context().Return(ctx).Maybe()

			// collect here on the handler's goroutine; assert only after Handle returns
			var sentMu sync.Mutex
			var reportedUuids, reportedBeacons, reportedErrors []string
			mockStream.EXPECT().Send(mock.Anything).RunAndReturn(func(msg *pb.BatchStreamReply) error {
				results := msg.GetResults()
				if results == nil {
					return nil
				}
				sentMu.Lock()
				defer sentMu.Unlock()
				for _, e := range results.GetErrors() {
					reportedErrors = append(reportedErrors, e.GetError())
					if u := e.GetUuid(); u != "" {
						reportedUuids = append(reportedUuids, u)
					}
					if b := e.GetBeacon(); b != "" {
						reportedBeacons = append(reportedBeacons, b)
					}
				}
				return nil
			}).Maybe()

			recvCount := 0
			mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
				recvCount++
				switch recvCount {
				case 1:
					return newBatchStreamStartRequest(), nil
				case 2:
					return newBatchStreamObjsAndRefsRequest(objs, refs), nil
				default:
					return nil, io.EOF
				}
			}).Maybe()

			handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, 1, logger, tc.namespacesEnabled)
			err := handler.Handle(mockStream)
			require.Error(t, err, "the stream still ends with the rejection error")

			sentMu.Lock()
			defer sentMu.Unlock()
			require.ElementsMatch(t, []string{objs[0].GetUuid(), objs[1].GetUuid()}, reportedUuids,
				"every object of the rejected message must be reported to the client")
			expectedBeacon := batch.BEACON_START + refs[0].GetFromCollection() + "/" + refs[0].GetFromUuid() + "/" + refs[0].GetName()
			require.ElementsMatch(t, []string{expectedBeacon}, reportedBeacons,
				"every reference of the rejected message must be reported to the client")
			require.Len(t, reportedErrors, 3, "one error per object and per reference")
			for _, e := range reportedErrors {
				require.NotEmpty(t, e)
				require.NotContains(t, e, "customer1:", "namespace information must be stripped for a confined principal")
			}
		})
	}
}

// A stream blocked in its first Recv while drain completes is invisible to
// drain's wait groups. It must be rejected, or it would push onto the closed
// queue.
func TestHandleRejectsStreamsThatStartDuringDrain(t *testing.T) {
	logger := logrus.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collection := "TestClass"

	mockBatcher := mocks.NewMockbatcher(t)
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).
		Return(&pb.BatchObjectsReply{}, nil).Maybe()
	mockSchemaManager := mocks.NewMockschemaManager(t)
	mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
	mockSchemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, collection).
		Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).Maybe()

	mockAuthenticator := mocks.NewMockauthenticator(t)
	mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

	mockStream := newMockStream(t)
	mockStream.EXPECT().Context().Return(ctx).Maybe()
	mockStream.EXPECT().Send(mock.Anything).Return(nil).Maybe()

	recvEntered := make(chan struct{})
	drained := make(chan struct{})
	recvCount := 0
	// All expectations are Maybe() because once the stream is rejected, no further
	// message is ever read.
	mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		recvCount++
		switch recvCount {
		case 1:
			close(recvEntered)
			<-drained
			return newBatchStreamStartRequest(), nil
		case 2:
			return newBatchStreamObjsRequest([]*pb.BatchObject{
				{Collection: collection, Uuid: uuid.New().String()},
			}), nil
		default:
			return nil, io.EOF
		}
	}).Maybe()

	handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, 1, logger, false)

	handled := make(chan error, 1)
	go func() {
		handled <- handler.Handle(mockStream)
	}()

	<-recvEntered
	drain()
	close(drained)

	select {
	case err := <-handled:
		require.Error(t, err, "a stream that starts during drain must be rejected")
		require.Contains(t, err.Error(), "not accepting new streams")
	case <-time.After(10 * time.Second):
		t.Fatal("Handle did not return after the stream was drained out from under it")
	}
}

// A receiver panic must reach the client as an error, not as a clean close that
// makes a dropped batch look like success.
func TestReceiverPanicEndsStreamWithError(t *testing.T) {
	logger := logrus.New()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	collection := "TestClass"

	mockBatcher := mocks.NewMockbatcher(t)
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).
		Return(&pb.BatchObjectsReply{}, nil).Maybe()
	mockSchemaManager := mocks.NewMockschemaManager(t)
	mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
	mockSchemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, collection).
		Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).Maybe()

	mockAuthenticator := mocks.NewMockauthenticator(t)
	mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

	mockStream := newMockStream(t)
	mockStream.EXPECT().Context().Return(ctx).Maybe()
	mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
		return msg.GetAcks() != nil
	})).RunAndReturn(func(*pb.BatchStreamReply) error {
		panic("send acks blew up")
	}).Maybe()
	mockStream.EXPECT().Send(mock.Anything).Return(nil).Maybe()

	recvCount := 0
	mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		recvCount++
		switch recvCount {
		case 1:
			return newBatchStreamStartRequest(), nil
		case 2:
			return newBatchStreamObjsRequest([]*pb.BatchObject{
				{Collection: collection, Uuid: uuid.New().String()},
			}), nil
		default:
			return nil, io.EOF
		}
	}).Maybe()

	handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, 1, logger, false)

	handled := make(chan error, 1)
	go func() {
		handled <- handler.Handle(mockStream)
	}()

	select {
	case err := <-handled:
		require.Error(t, err)
		require.Contains(t, err.Error(), "receiver panicked")
	case <-time.After(10 * time.Second):
		t.Fatal("Handle did not return after the receiver panicked")
	}
}

// Each row makes the receiver exit early while the recv goroutine holds a decoded
// request. Without the ctx check on recv's channel sends, both the goroutine and
// the request leak for the process lifetime. The two grace-period exits are not
// rows because they call cancel() before returning, which is the same release
// these rows exercise.
func TestStreamHandlerRecvGoroutineDoesNotLeakOnEarlyExit(t *testing.T) {
	logger := logrus.New()

	cases := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		collection        string
		checkAllocErr     error
		getClassErr       error
		acksSendErr       error
		invalidRequest    bool
	}{
		{
			name:          "out of memory",
			principal:     &models.Principal{},
			collection:    "TestClass",
			checkAllocErr: enterrors.ErrNotEnoughMemory,
		},
		{
			// a namespaced principal is not allowed to prefix a class name with a
			// namespace itself, so namespacing.Resolve rejects this before the schema
			// manager is touched
			name:              "schema resolve failure",
			namespacesEnabled: true,
			principal:         &models.Principal{Namespace: "customer1"},
			collection:        "customer2:TestClass",
		},
		{
			name:        "class fetch failure",
			principal:   &models.Principal{},
			collection:  "TestClass",
			getClassErr: errors.New("schema unavailable"),
		},
		{
			name:           "invalid request",
			principal:      &models.Principal{},
			collection:     "TestClass",
			invalidRequest: true,
		},
		{
			name:        "ack send failure",
			principal:   &models.Principal{},
			collection:  "TestClass",
			acksSendErr: errors.New("client gone"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			mockBatcher := mocks.NewMockbatcher(t)
			mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).
				Return(&pb.BatchObjectsReply{}, nil).Maybe()

			mockSchemaManager := mocks.NewMockschemaManager(t)
			mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
			mockSchemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, names ...string) (map[string]versioned.Class, error) {
					if tc.getClassErr != nil {
						return nil, tc.getClassErr
					}
					classes := make(map[string]versioned.Class, len(names))
					for _, name := range names {
						classes[name] = versioned.Class{Class: &models.Class{Class: name}}
					}
					return classes, nil
				}).Maybe()

			mockAllocChecker := mocks.NewMockAllocChecker(t)
			mockAllocChecker.EXPECT().Refresh(mock.Anything).Return().Maybe()
			mockAllocChecker.EXPECT().CheckAlloc(mock.Anything).Return(tc.checkAllocErr).Maybe()

			mockAuthenticator := mocks.NewMockauthenticator(t)
			mockAuthenticator.EXPECT().PrincipalFromContext(ctx).Return(tc.principal, nil).Once()

			mockStream := newMockStream(t)
			mockStream.EXPECT().Context().Return(ctx).Maybe()

			dataRequest := func() *pb.BatchStreamRequest {
				return newBatchStreamObjsRequest([]*pb.BatchObject{
					{Collection: tc.collection, Uuid: uuid.New().String()},
				})
			}

			recvCount := 0
			mockStream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
				recvCount++
				switch recvCount {
				case 1:
					return newBatchStreamStartRequest(), nil
				case 2:
					if tc.invalidRequest {
						// neither Data nor Stop
						return &pb.BatchStreamRequest{}, nil
					}
					return dataRequest(), nil
				default:
					return dataRequest(), nil
				}
			}).Maybe()

			mockStream.EXPECT().Send(mock.MatchedBy(func(msg *pb.BatchStreamReply) bool {
				return msg.GetAcks() != nil
			})).Return(tc.acksSendErr).Maybe()
			mockStream.EXPECT().Send(mock.Anything).Return(nil).Maybe()

			handler, drain := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, 1, logger, tc.namespacesEnabled, batch.WithAllocChecker(mockAllocChecker))
			// Defers run last-in first-out: drain retires the workers first, then the
			// leak check runs with only the stream's own goroutines left to account for.
			defer leaktest.Check(t)()
			defer drain()

			_ = handler.Handle(mockStream)
		})
	}
}

// One stalled client must not hold up other streams. Send blocks while that
// client's flow-control window is full, so a send lock shared across streams
// would freeze every stream's acks and receive loops.
func TestSendNotSerialisedAcrossStreams(t *testing.T) {
	logger := logrus.New()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	collection := "TestClass"

	mockBatcher := mocks.NewMockbatcher(t)
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).
		Return(&pb.BatchObjectsReply{}, nil).Maybe()
	mockSchemaManager := mocks.NewMockschemaManager(t)
	mockSchemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
	mockSchemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, collection).
		Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).Maybe()

	mockAuthenticator := mocks.NewMockauthenticator(t)
	mockAuthenticator.EXPECT().PrincipalFromContext(mock.Anything).Return(&models.Principal{}, nil).Times(2)

	stalledEntered := make(chan struct{})
	releaseStalled := make(chan struct{})
	acked := make(chan struct{})
	finish := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseStalled) })
	stop := sync.OnceFunc(func() { close(finish) })
	// Defers run last-in first-out: unblock the stalled Send first, then let both
	// clients hang up.
	defer stop()
	defer release()

	dataRequest := func() *pb.BatchStreamRequest {
		return newBatchStreamObjsRequest([]*pb.BatchObject{
			{Collection: collection, Uuid: uuid.New().String()},
		})
	}
	// streams stay open until after the assertion, so neither can end early
	recvSequence := func() func() (*pb.BatchStreamRequest, error) {
		count := 0
		return func() (*pb.BatchStreamRequest, error) {
			count++
			switch count {
			case 1:
				return newBatchStreamStartRequest(), nil
			case 2:
				return dataRequest(), nil
			default:
				<-finish
				return nil, io.EOF
			}
		}
	}

	// The stalled client blocks on its very first reply, so it holds whatever lock
	// Send takes for the whole time the assertion runs.
	stalledStream := newMockStream(t)
	stalledStream.EXPECT().Context().Return(ctx).Maybe()
	stalledStream.EXPECT().Recv().RunAndReturn(recvSequence()).Maybe()
	var stalledOnce sync.Once
	stalledStream.EXPECT().Send(mock.Anything).RunAndReturn(func(*pb.BatchStreamReply) error {
		stalledOnce.Do(func() {
			close(stalledEntered)
			<-releaseStalled
		})
		return nil
	}).Maybe()

	healthyStream := newMockStream(t)
	healthyStream.EXPECT().Context().Return(ctx).Maybe()
	healthyStream.EXPECT().Recv().RunAndReturn(recvSequence()).Maybe()
	var ackedOnce sync.Once
	healthyStream.EXPECT().Send(mock.Anything).RunAndReturn(func(msg *pb.BatchStreamReply) error {
		if msg.GetAcks() != nil {
			ackedOnce.Do(func() { close(acked) })
		}
		return nil
	}).Maybe()

	// more workers than streams, so that a worker stuck on the stalled stream's
	// undeliverable report does not starve the healthy stream
	handler, _ := batch.Start(mockAuthenticator, nil, mockBatcher, mockSchemaManager, nil, 4, logger, false)

	stalledHandled := make(chan error, 1)
	go func() {
		stalledHandled <- handler.Handle(stalledStream)
	}()
	healthyHandled := make(chan error, 1)
	go func() {
		healthyHandled <- handler.Handle(healthyStream)
	}()

	<-stalledEntered

	select {
	case <-acked:
	case <-time.After(15 * time.Second):
		t.Fatal("a healthy stream never got its acks while another stream was stalled inside Send")
	}

	release()
	stop()

	for _, handled := range []chan error{stalledHandled, healthyHandled} {
		select {
		case err := <-handled:
			require.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("Handle did not return after both clients hung up")
		}
	}
}
